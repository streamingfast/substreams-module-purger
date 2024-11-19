package main

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"sync"
	"time"
)

type job struct {
	filePath string
	bucket   *storage.BucketHandle
}

func listFiles(ctx context.Context, prefix string, bucket *storage.BucketHandle, f func(filePath string, createdAt time.Time, fileSize int64), limit int) error {
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
		f(attrs.Name, attrs.Created, attrs.Size)
	}

	return nil
}

func worker(ctx context.Context, id int, wg *sync.WaitGroup, jobs <-chan job) {
	defer wg.Done()
	for j := range jobs {
		err := deleteFile(ctx, j.filePath, j.bucket)
		if err != nil {
			zlog.Info("retrying file", zap.String("file", j.filePath))
			err = deleteFile(ctx, j.filePath, j.bucket)
			if err != nil {
				zlog.Info("skipping failed file", zap.String("file", j.filePath))
			}
		}
	}
}

func deleteFile(ctx context.Context, filePath string, bucket *storage.BucketHandle) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	o := bucket.Object(filePath)
	if err := o.Delete(ctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %v", filePath, err)
	}

	return nil
}
