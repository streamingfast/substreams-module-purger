package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"substream-module-purger/datastore"
	"sync"
	"time"

	smp "substream-module-purger"

	"cloud.google.com/go/storage"
	"github.com/charmbracelet/huh"
	"github.com/drone/envsubst"
	"github.com/googleapis/gax-go/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "purger",
	Short: "Substreams module cache purger",
}

var poisonedCmd = &cobra.Command{
	Use:   "prune-poisoned-data <bucket>",
	Short: "remove cache based on various conditions",
	RunE:  runPrunePoisoned,
	Args:  cobra.ExactArgs(2),
}

var oldCmd = &cobra.Command{
	Use:   "prune-old-data",
	Short: "remove cache based on age (uses the database)",
	RunE:  runPruneOld,
}

var backoff = gax.Backoff{
	Initial:    100 * time.Millisecond,
	Multiplier: 2,
	Max:        30 * time.Second,
}

func init() {
	rootCmd.AddCommand(oldCmd)
	rootCmd.AddCommand(poisonedCmd)
	rootCmd.PersistentFlags().String("project", "dfuseio-global", "requester-pay project name")
	rootCmd.PersistentFlags().String("network", "sol-mainnet", "specify a network")
	rootCmd.PersistentFlags().Bool("force", false, "force pruning (skip confirmation)")

	oldCmd.Flags().String("database-dsn", "postgres://localhost:5432/postgres?enable_incremental_sort=off&sslmode=disable", "Database DSN")
	oldCmd.Flags().Uint64("max-age-days", 31, "max age of module caches to keep, in days")

	poisonedCmd.Flags().String("path", "{network}", "Narrow down pruning to this path (accepts '{network}')")
	poisonedCmd.Flags().Uint64("lowest-poisoned-block", 0, "Only caches containing blocks above this will be targeted for pruning")
	poisonedCmd.Flags().Uint64("highest-poisoned-block", 0, "If non-zero, only caches containing blocks below this will be targeted for pruning")
	poisonedCmd.Flags().Uint64("exclude-after-unixtimestamp", 0, "If non-zero, only caches created before that time will be targeted for pruning")

	poisonedCmd.Flags().StringSlice("module-types", []string{"output", "state", "index"}, "Only modules of these types will be targeted for pruning")
}

func getGlobalParams(cmd *cobra.Command) (project, network string, force bool, err error) {
	force = sflags.MustGetBool(cmd, "force")
	network = sflags.MustGetString(cmd, "network")
	if network == "" {
		return "", "", false, fmt.Errorf("network is required (ex: eth-mainnet)")
	}
	project = sflags.MustGetString(cmd, "project")
	return
}

func runPrunePoisoned(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	project, network, force, err := getGlobalParams(cmd)
	if err != nil {
		return err
	}
	//Search order
	//- GOOGLE_APPLICATION_CREDENTIALS environment variable
	//- User credentials set up by using the Google Cloud CLI
	//- The attached service account, returned by the metadata server
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println(cli.WarningStyle.Render("make sure, you have google authorization credentials..."))
		return fmt.Errorf("creating storage client: %w", err)
	}

	bucket := client.Bucket(args[0]).UserProject(project)
	lookupPath := sflags.MustGetString(cmd, "path")
	lookupPath = strings.ReplaceAll(lookupPath, "{network}", network)

	excludeAfter := int64(sflags.MustGetUint64(cmd, "exclude-after-unixtimestamp"))
	highest := sflags.MustGetUint64(cmd, "highest-poisoned-block")
	lowest := sflags.MustGetUint64(cmd, "lowest-poisoned-block")
	targetTypes := make(map[datastore.FileType]bool)
	for _, t := range sflags.MustGetStringSlice(cmd, "module-types") {
		switch t {
		case "output":
			targetTypes[datastore.Output] = true
		case "state":
			targetTypes[datastore.State] = true
		case "index":
			targetTypes[datastore.Index] = true
		default:
			return fmt.Errorf("invalid type")
		}
	}
	if len(targetTypes) == 0 {
		return fmt.Errorf("need at least one module type to target")
	}

	jobs := make(chan job, 1000)
	var wg sync.WaitGroup

	for w := 1; w <= 250; w++ {
		wg.Add(1)
		go worker(ctx, &wg, jobs)
	}

	count := 0
	pathCount := 0
	lastPath := ""
	err = listFiles(ctx, lookupPath, bucket, func(filePath string, createdAt time.Time, fileSize int64) {
		if path.Base(filePath) == "substreams.partial.spkg.zst" {
			return // skip those silently
		}
		low, high, ft, err := datastore.FileInfo(filePath)
		if err != nil {
			zlog.Debug("skipping invalid file", zap.String("path", filePath))
			return
		}
		if !targetTypes[ft] {
			return
		}
		if high <= lowest {
			return
		}
		if highest != 0 && low > highest {
			return
		}
		if excludeAfter != 0 && createdAt.Unix() > excludeAfter {
			return
		}
		if p := path.Dir(filePath); p != lastPath {
			zlog.Info("going to next folder", zap.Int("previous_deleted_count", pathCount), zap.String("next folder", p))
			lastPath = p
			pathCount = 0
		}

		if !force {
			confirm, all, err := runConfirmFormWithAll(fmt.Sprintf("Delete file %q ?", filePath))
			if err != nil {
				zlog.Error(fmt.Sprintf("running confirm form: %s", err))
				return
			}
			if !confirm {
				return
			}
			if all {
				force = true
			}
		}

		jobs <- job{
			filePath: filePath,
			bucket:   bucket,
		}
		count++
		pathCount++

	}, smp.Unlimited)
	if err != nil && err != io.EOF {
		return fmt.Errorf("listing files to delete: %w", err)
	}
	close(jobs)

	zlog.Info("Total deleted files", zap.Int("count", count))

	return nil
}

func runPruneOld(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	project, network, force, err := getGlobalParams(cmd)
	if err != nil {
		return err
	}

	databaseDSN := sflags.MustGetString(cmd, "database-dsn")
	databaseDSN, err = envsubst.Eval(databaseDSN, os.Getenv)
	if err != nil {
		return fmt.Errorf("expending dsn: %w", err)
	}

	db, err := sqlx.Open("postgres", databaseDSN)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}

	maxAgeDays := sflags.MustGetUint64(cmd, "max-age-days")
	if maxAgeDays == 0 {
		return fmt.Errorf("max-age-days is required to be greater than 0")
	}

	zlog.Info("getting modules to purge... (this will take a few minutes)", zap.String("network", network), zap.Uint64("max_age_days", maxAgeDays))
	modulesCache, err := datastore.ModulesToPurge(db, network, maxAgeDays)
	if err != nil {
		return fmt.Errorf("loading modules cache to purge: %w", err)
	}

	zlog.Info("about to purge", zap.Int("modules_count", len(modulesCache)))

	// Google cloud Auth Search order
	//- GOOGLE_APPLICATION_CREDENTIALS environment variable
	//- User credentials set up by using the Google Cloud CLI
	//- The attached service account, returned by the metadata server
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println(cli.WarningStyle.Render("make sure, you have google authorization credentials..."))
		return fmt.Errorf("creating storage client: %w", err)
	}
	youngestDate := time.Now().Add(-time.Duration(maxAgeDays) * time.Hour * 24)

	for _, m := range modulesCache {
		bucket := client.Bucket(m.Bucket).Retryer(storage.WithBackoff(backoff))
		if project != "" {
			bucket = bucket.UserProject(project)
		}
		relpath := fmt.Sprintf("%s/%s", m.Network, m.Subfolder)

		fileCount := 0
		filesToPurge := make([]string, 0)
		var totalFileSize int64
		err = listFiles(ctx, relpath, bucket, func(filePath string, createdAt time.Time, fileSize int64) {
			//validate all the date because we are not trusting the database data yet
			if createdAt.After(youngestDate) && !strings.HasSuffix(filePath, ".partial.zst") {
				panic(fmt.Sprintf("file %q (%q) is newer than module %q (%q)", filePath, createdAt, m, m.YoungestFileCreationDate))
			}
			filesToPurge = append(filesToPurge, filePath)
			totalFileSize += fileSize
		}, smp.Unlimited)
		if err != nil && err != io.EOF {
			return fmt.Errorf("listing files: %w", err)
		}

		if !force {
			fmt.Printf("%s:", cli.PurpleStyle.Render("List of files to purge"))
			for _, filePath := range filesToPurge {
				fmt.Printf("- %s\n", filePath)
			}
			fmt.Printf("\n%s: %d bytes (%.2f MB)\n",
				cli.HeaderStyle.Render("Total potential savings"),
				totalFileSize,
				float64(totalFileSize)/(1024*1024))

			confirm, all, err := runConfirmFormWithAll("Do you want to confirm the purge?")
			if err != nil {
				return fmt.Errorf("running confirm form: %w", err)
			}

			if !confirm {
				return nil
			}
			if all {
				force = true
			}
		}
		zlog.Info("pruning files", zap.String("folder", path.Dir(filesToPurge[0])), zap.Int("count", len(filesToPurge)), zap.Float64("size_mib", float64(totalFileSize)/(1024*1024)))

		jobs := make(chan job, 1000)
		var wg sync.WaitGroup

		start := time.Now()
		for w := 1; w <= 250; w++ {
			wg.Add(1)
			go worker(ctx, &wg, jobs)
		}

		zlog.Info("start purging files...", zap.String("folder", path.Dir(filesToPurge[0])))
		for _, filePath := range filesToPurge {
			fileCount++
			jobs <- job{
				filePath: filePath,
				bucket:   bucket,
			}
			if fileCount%1000 == 0 {
				zlog.Info("progress", zap.Int("processed", fileCount))
			}
		}
		close(jobs)

		zlog.Info("waiting for all files to be deleted", zap.Int("files_deleted", fileCount))
		wg.Wait()
		zlog.Info("module cache purging done", zap.String("path", relpath), zap.Duration("elapsed", time.Since(start)), zap.String("storage saved", fmt.Sprintf("%.2f MB",
			float64(totalFileSize)/(1024*1024))))
	}

	return nil
}

func runConfirmFormWithAll(title string) (bool, bool, error) {
	confirmOverwrite := "No"

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Options(huh.NewOptions("Yes", "No", "All")...).
				Title(title).
				Value(&confirmOverwrite),
		),
	)

	if err := form.Run(); err != nil {
		return false, false, fmt.Errorf("error running form: %w", err)
	}

	switch confirmOverwrite {
	case "No":
		return false, false, nil
	case "Yes":
		return true, false, nil
	case "All":
		return true, true, nil
	default:
		return false, false, fmt.Errorf("invalid")
	}
}
