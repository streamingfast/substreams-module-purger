package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

type job struct {
	filePath string
	bucket   *storage.BucketHandle
}

func listFiles(ctx context.Context, prefix string, bucket *storage.BucketHandle, f func(filePath string, createdAt time.Time, fileSize int64), limit int) error {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()

	zlog.Info("Listing files from bucket", zap.String("prefix", prefix))
	q := &storage.Query{
		Prefix:     prefix,
		Projection: storage.ProjectionNoACL,
	}
	q.SetAttrSelection([]string{"Name", "Created", "Size"})
	it := bucket.Objects(ctx, q)

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
		f(attrs.Name, attrs.Created, attrs.Size)
	}

	return nil
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan job) {
	defer wg.Done()
	for j := range jobs {
		err := deleteFile(ctx, j.filePath, j.bucket)
		if err != nil {
			zlog.Info("skipping failed file", zap.String("file", j.filePath))
		}
	}
}

func deleteFile(ctx context.Context, filePath string, bucket *storage.BucketHandle) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	o := bucket.Object(filePath).Retryer(storage.WithBackoff(backoff))
	if err := o.Delete(ctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %v", filePath, err)
	}

	return nil
}
