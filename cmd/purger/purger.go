package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"substream-module-purger/datastore"
	"sync"
	"time"

	smp "substream-module-purger"

	"cloud.google.com/go/storage"
	"github.com/drone/envsubst"
	"github.com/googleapis/gax-go/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/cli/utils"
	"go.uber.org/zap"
)

var purgerCmd = &cobra.Command{
	Use:   "runPurger",
	Short: "Substreams module data runPurger",
	RunE:  runPurger,
}

func init() {
	purgerCmd.Flags().String("database-dsn", "postgres://localhost:5432/postgres?enable_incremental_sort=off&sslmode=disable", "Database DSN")
	purgerCmd.Flags().String("interval", "1 month", "max age of module caches to keep in SQL language")
	purgerCmd.Flags().String("network", "sol-mainnet", "specify a network")
	purgerCmd.Flags().String("project", "dfuseio-global", "requester-pay project name")
	purgerCmd.Flags().Bool("force", false, "force purge")
}

func runPurger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	databaseDSN := sflags.MustGetString(cmd, "database-dsn")
	databaseDSN, err := envsubst.Eval(databaseDSN, os.Getenv)
	if err != nil {
		return fmt.Errorf("expending dsn: %w", err)
	}

	db, err := sqlx.Open("postgres", databaseDSN)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}

	network := sflags.MustGetString(cmd, "network")
	if network == "" {
		return fmt.Errorf("network is required (ex: eth-mainnet)")
	}

	interval := sflags.MustGetString(cmd, "interval")
	if interval == "" {
		return fmt.Errorf("interval is required (ex: `1 month`)")
	}

	zlog.Info("getting modules to purge... (this will take a few minutes)")
	modulesCache, err := datastore.ModulesToPurge(db, network, interval)
	if err != nil {
		return fmt.Errorf("loading modules cache to purge: %w", err)
	}
	//modulesCache := []datastore.ModuleCache{
	//{
	//Bucket:                   "dfuseio-global-substreams-uscentral",
	//Network:                  "sol-mainnet",
	//Subfolder:                "substreams-states/v5/5a44b90f7a7a75de44a32bf6a6aa75de62c24ffb/outputs",
	//YoungestFileCreationDate: time.Now().Add(time.Hour * -24 * 30),
	//},
	//}

	zlog.Info("about to purge", zap.Int("modules_count", len(modulesCache)))

	//Search order
	//- GOOGLE_APPLICATION_CREDENTIALS environment variable
	//- User credentials set up by using the Google Cloud CLI
	//- The attached service account, returned by the metadata server
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println(cli.WarningStyle.Render("make sure, you have google authorization credentials..."))
		return fmt.Errorf("creating storage client: %w", err)
	}

	for _, m := range modulesCache {
		bucket := client.Bucket(m.Bucket).Retryer(storage.WithBackoff(gax.Backoff{
			Initial: 100 * time.Millisecond,
			Max:     10 * time.Second,
		}))
		if project := sflags.MustGetString(cmd, "project"); project != "" {
			bucket = bucket.UserProject(project)
		}
		path := fmt.Sprintf("%s/%s", m.Network, m.Subfolder)

		fileCount := 0
		filesToPurge := make([]string, 0)
		var totalFileSize int64
		err = listFiles(ctx, path, bucket, func(filePath string, createdAt time.Time, fileSize int64) {
			//validate all the date because we are not trusting the database data yet
			if createdAt.After(m.YoungestFileCreationDate) && !strings.HasSuffix(filePath, ".partial.zst") {
				panic(fmt.Sprintf("file %q (%q) is newer than module %q (%q)", filePath, createdAt, m, m.YoungestFileCreationDate))
			}
			filesToPurge = append(filesToPurge, filePath)
			totalFileSize += fileSize
		}, smp.Unlimited)
		if err != nil && err != io.EOF {
			return fmt.Errorf("listing files: %w", err)
		}

		fmt.Printf("%s:", cli.PurpleStyle.Render("List of files to purge"))
		for _, filePath := range filesToPurge {
			fmt.Printf("- %s\n", filePath)
		}
		fmt.Printf("\n%s: %d bytes (%.2f MB)\n",
			cli.HeaderStyle.Render("Total potential savings"),
			totalFileSize,
			float64(totalFileSize)/(1024*1024))

		force := sflags.MustGetBool(cmd, "force")
		if !force {
			confirm, err := utils.RunConfirmForm("Do you want to confirm the purge?")
			if err != nil {
				return fmt.Errorf("running confirm form: %w", err)
			}

			if !confirm {
				return nil
			}
		}

		jobs := make(chan job, 1000)
		var wg sync.WaitGroup

		start := time.Now()
		for w := 1; w <= 250; w++ {
			wg.Add(1)
			go worker(ctx, w, &wg, jobs)
		}

		zlog.Info("start purging files...")
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
		zlog.Info("module cache purging done", zap.String("path", path), zap.Duration("elapsed", time.Since(start)), zap.String("storage saved", fmt.Sprintf("%.2f MB",
			float64(totalFileSize)/(1024*1024))))
		fmt.Println()
	}

	return nil
}
