package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/util"
)

// boltStore provides the common lifecycle for bbolt-backed stores:
// database open/close, bucket creation, background cleanup, and health checks.
type boltStore struct {
	db        *bolt.DB
	bucket    []byte
	maxAge    time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	closeOnce *sync.Once
	closeErr  error
}

// boltStoreConfig configures a boltStore.
type boltStoreConfig struct {
	DBPath     string
	BucketName []byte
	MaxAge     time.Duration
	Label      string // for log/error messages (e.g. "callback", "diagnostics")
}

// openBoltStore opens a bbolt database, creates the bucket, and returns the
// base store. Call startCleanup after construction if MaxAge > 0.
func openBoltStore(cfg boltStoreConfig) (*boltStore, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("%s db path is required", cfg.Label)
	}

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s db: %w", cfg.Label, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(cfg.BucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create %s bucket: %w", cfg.Label, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &boltStore{
		db:        db,
		bucket:    cfg.BucketName,
		maxAge:    cfg.MaxAge,
		ctx:       ctx,
		cancel:    cancel,
		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},
	}, nil
}

// startCleanup runs an initial cleanup and starts a background loop.
// removeExpired is the store-specific function that deletes old entries.
// onPanic (may be nil) is invoked if a cleanup iteration panics; callers
// typically inject a metrics-increment closure here. Kept as a parameter
// rather than a package import so internal/backend/shared stays free of
// the internal/metrics dependency.
func (s *boltStore) startCleanup(label string, cleanupInterval time.Duration, removeExpired func(time.Duration) (int, error), onPanic util.PanicHandler) {
	// Initial cleanup to clear stale entries from a previous run.
	if removed, err := removeExpired(s.maxAge); err != nil {
		slog.Warn("initial "+label+" cleanup failed", "error", err)
	} else if removed > 0 {
		slog.Info("removed expired "+label+" on startup", "count", removed, "max_age", s.maxAge)
	}

	interval := cleanupInterval
	if interval <= 0 {
		interval = s.maxAge
	}
	s.wg.Go(func() {
		util.StartCleanupLoop(s.ctx, interval, func() error {
			removed, err := removeExpired(s.maxAge)
			if err != nil {
				return err
			}
			if removed > 0 {
				slog.Debug("cleaned up expired "+label, "count", removed)
			}
			return nil
		}, label, onPanic)
	})
}

// removeOlderThan is a generic cleanup helper for bbolt stores that store
// JSON-encoded entries with a timestamp field. It iterates all entries in the
// bucket, unmarshals each as T, extracts the timestamp using getTime, and
// deletes entries older than maxAge. Malformed entries are also removed.
func removeOlderThan[T any](db *bolt.DB, bucket []byte, maxAge time.Duration, getTime func(*T) time.Time) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket %q not found", string(bucket))
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry T
			if err := json.Unmarshal(v, &entry); err != nil {
				slog.Warn("removing malformed entry",
					"bucket", string(bucket), "key", string(k), "error", err)
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
				continue
			}
			if getTime(&entry).Before(cutoff) {
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
			}
		}
		return nil
	})

	return removed, err
}

// Healthy checks that the bbolt database is accessible and the bucket exists.
func (s *boltStore) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(s.bucket) == nil {
			return errors.New("bucket missing")
		}
		return nil
	})
}

// Close shuts down the store gracefully. It is idempotent: the first call
// closes the database and captures any error; subsequent calls return the same error.
func (s *boltStore) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
