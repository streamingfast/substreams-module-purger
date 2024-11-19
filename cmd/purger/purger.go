package main

import (
	"fmt"
	"os"
	"strings"
	"substream-module-purger/datastore"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/drone/envsubst"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/cli/utils"
	"go.uber.org/zap"
	smp "substream-module-purger"
)

var purgerCmd = &cobra.Command{
	Use:   "runPurger [project_id]",
	Short: "Substreams module data runPurger",
	RunE:  runPurger,
	Args:  cobra.ExactArgs(1),
}

func init() {
	purgerCmd.Flags().String("database-dsn", "postgres://localhost:5432/postgres?enable_incremental_sort=off&sslmode=disable", "Database DSN")
	purgerCmd.Flags().String("subfolder", "", "Specify a subfolder to limit purging to modules within it.")
	purgerCmd.Flags().Bool("force", false, "Force purge")
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

	subfolder := sflags.MustGetString(cmd, "subfolder")
	if subfolder == "" {
		return fmt.Errorf("subfolder is required")
	}

	modulesCache, err := datastore.ModulesToPurge(db, subfolder)
	if err != nil {
		return fmt.Errorf("loading modulesCache to purge: %w", err)
	}

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
		bucket := client.Bucket(m.Bucket)
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
		if err != nil {
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
		defer close(jobs)
		var wg sync.WaitGroup

		start := time.Now()
		for w := 1; w <= 250; w++ {
			wg.Add(1)
			go worker(ctx, w, &wg, jobs)
		}

		zlog.Info("start purging files...")
		for _, filePath := range filesToPurge {
			fileCount++
			jobs <- job{filePath: filePath}
			if fileCount%1000 == 0 {
				zlog.Info("progress", zap.Int("processed", fileCount))
			}
		}

		zlog.Info("waiting for all files to be deleted", zap.Int("files_deleted", fileCount))
		wg.Wait()
		zlog.Info("module cache purging done", zap.String("path", path), zap.Duration("elapsed", time.Since(start)), zap.String("storage saved", fmt.Sprintf("%.2f MB",
			float64(totalFileSize)/(1024*1024))))
		fmt.Println()
	}

	return nil
}
