package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/drone/envsubst"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/substream-module-purger"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

var purgerCmd = &cobra.Command{
	Use:   "purger [project_id]",
	Short: "Substreams module data purger",
	RunE:  purger,
	Args:  cobra.ExactArgs(1),
}

func init() {
	purgerCmd.Flags().String("database-dsn", "postgres://localhost:5432/postgres?enable_incremental_sort=off&sslmode=disable", "Database DSN")
}

func purger(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	databaseDSN := sflags.MustGetString(cmd, "database-dsn")
	databaseDSN, err := envsubst.Eval(databaseDSN, os.Getenv)
	if err != nil {
		return fmt.Errorf("expending dsn: %w", err)
	}

	db, err := sqlx.Open("postgres", databaseDSN)
	if err != nil {
		return fmt.Errorf("openning database: %w", err)
	}

	modules, err := moduleToPurge(db)
	if err != nil {
		return fmt.Errorf("loading modules to purge: %w", err)
	}

	zlog.Info("about to purge", zap.Int("modules_count", len(modules)))

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("creating storage client: %w", err)
	}

	for _, m := range modules {
		zlog.Info("fetching file for module", zap.Stringer("module", m))
		bucket := client.Bucket(m.Bucket)
		path := fmt.Sprintf("%s/%s", m.Network, m.Subfolder)

		start := time.Now()
		zlog.Info("starting deletion ....")

		jobs := make(chan job, 1000)
		var wg sync.WaitGroup

		for w := 1; w <= 250; w++ {
			wg.Add(1)
			go worker(ctx, w, &wg, jobs)
		}

		fileCount := 0

		cli.NoError(err, "Unable to list files")

		filesToPurge := []string{}
		err = listFiles(ctx, path, bucket, func(f string, createdAt time.Time) {
			//validate all the date because we are not trusting the database data yet
			if createdAt.After(m.YoungestFileCreationDate) && !strings.HasSuffix(f, ".partial.zst") {
				panic(fmt.Sprintf("file %q (%q) is newer than module %q (%q)", f, createdAt, m, m.YoungestFileCreationDate))
			}
			filesToPurge = append(filesToPurge, f)
		}, Unlimited)

		for _, f := range filesToPurge {
			fileCount++
			jobs <- job{file: f}
			if fileCount%1000 == 0 {
				zlog.Info("progress", zap.Int("processed", fileCount))
			}
		}
		close(jobs)

		if err != nil {
			return fmt.Errorf("listing files: %w", err)
		}

		zlog.Info("waiting for all files to be deleted", zap.Int("files_deleted", fileCount))
		wg.Wait()
		zlog.Info("module purging done", zap.String("path", path), zap.Duration("elapsed", time.Since(start)))
		fmt.Println()
	}

	return nil
}

var purgeableModuleQuery = `
with youngest_file as (
    select bucket, network, subfolder, max(created_at) as youngest_file_creation_date
    from cost_estimator.files
    where deleted_at is null
    and filetype != 1
	and subfolder = 'substreams-states/v4/efb6f81bcc9affb7f134265fb27d607bf4826fc1/states'
    group by bucket, network, subfolder
    order by youngest_file_creation_date
)
select * from youngest_file where youngest_file_creation_date < now() - interval '1 month';
`

type module struct {
	Bucket                   string    `db:"bucket"`
	Network                  string    `db:"network"`
	Subfolder                string    `db:"subfolder"`
	YoungestFileCreationDate time.Time `db:"youngest_file_creation_date"`
}

func (m *module) String() string {
	return fmt.Sprintf("%s (%s) %s, %s", m.Bucket, m.Network, m.Subfolder, m.YoungestFileCreationDate)
}

func moduleToPurge(db *sqlx.DB) ([]*module, error) {
	fmt.Println("Querying module to purge")
	out := []*module{}

	err := db.Select(&out, purgeableModuleQuery)
	if err != nil {
		return nil, fmt.Errorf("querying module: %w", err)
	}

	return out, nil
}

func listFiles(ctx context.Context, prefix string, bucket *storage.BucketHandle, f func(file string, createdAt time.Time), limit int) error {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()

	zlog.Info("Listing files from bucket", zap.String("prefix", prefix))
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	count := 0
	for {
		if limit != -1 && count > limit {
			return nil
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("listing files for prefix %q: %w", prefix, err)
		}

		count++
		f(attrs.Name, attrs.Created)
	}

	return nil
}

type job struct {
	file   string
	bucket *storage.BucketHandle
}

func worker(ctx context.Context, id int, wg *sync.WaitGroup, jobs <-chan job) {
	defer wg.Done()
	for j := range jobs {
		err := deleteFile(ctx, j.file, j.bucket)
		if err != nil {
			zlog.Info("retrying file", zap.String("file", j.file))
			err = deleteFile(ctx, j.file, j.bucket)
			if err != nil {
				zlog.Info("skipping failed file", zap.String("file", j.file))
			}
		}
	}
}

func deleteFile(ctx context.Context, filePath string, bucket *storage.BucketHandle) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	//o := bucket.Object(filePath)
	//if err := o.Delete(ctx); err != nil {
	//	return fmt.Errorf("Object(%q).Delete: %v", filePath, err)
	//}

	return nil
}
