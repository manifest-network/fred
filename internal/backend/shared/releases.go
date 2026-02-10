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

var releasesBucketName = []byte("releases")

// Release represents a single release (deployment) of a lease.
type Release struct {
	Version   int       `json:"version"`
	Manifest  []byte    `json:"manifest"`
	Image     string    `json:"image"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error,omitempty"`
}

// ReleaseStore persists release history in bbolt so it survives backend restarts.
type ReleaseStore struct {
	db     *bolt.DB
	maxAge time.Duration

	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	closeOnce *sync.Once
	closeErr  error
}

// ReleaseStoreConfig configures the release store.
type ReleaseStoreConfig struct {
	DBPath          string
	MaxAge          time.Duration
	CleanupInterval time.Duration
}

// NewReleaseStore opens or creates a bbolt database for release persistence.
func NewReleaseStore(cfg ReleaseStoreConfig) (*ReleaseStore, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("releases db path is required")
	}

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open releases db: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(releasesBucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create releases bucket: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &ReleaseStore{
		db:        db,
		maxAge:    cfg.MaxAge,
		cancel:    cancel,
		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},
	}

	if cfg.MaxAge > 0 {
		if removed, cleanupErr := s.RemoveOlderThan(cfg.MaxAge); cleanupErr != nil {
			slog.Warn("initial releases cleanup failed", "error", cleanupErr)
		} else if removed > 0 {
			slog.Info("removed expired releases on startup", "count", removed, "max_age", cfg.MaxAge)
		}

		interval := cfg.CleanupInterval
		if interval <= 0 {
			interval = cfg.MaxAge
		}
		s.wg.Go(func() {
			util.StartCleanupLoop(ctx, interval, s.cleanup, "releases")
		})
	}

	return s, nil
}

func (s *ReleaseStore) cleanup() error {
	removed, err := s.RemoveOlderThan(s.maxAge)
	if err != nil {
		return err
	}
	if removed > 0 {
		slog.Debug("cleaned up expired releases", "count", removed)
	}
	return nil
}

// Healthy checks if the bbolt database is accessible.
func (s *ReleaseStore) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(releasesBucketName) == nil {
			return errors.New("releases bucket missing")
		}
		return nil
	})
}

// Append adds a new release for a lease, auto-assigning Version.
func (s *ReleaseStore) Append(leaseUUID string, r Release) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		var releases []Release

		data := b.Get([]byte(leaseUUID))
		if data != nil {
			if err := json.Unmarshal(data, &releases); err != nil {
				releases = nil
			}
		}

		r.Version = len(releases) + 1
		releases = append(releases, r)

		encoded, err := json.Marshal(releases)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

// List returns all releases for a lease. Returns nil, nil when not found.
func (s *ReleaseStore) List(leaseUUID string) ([]Release, error) {
	var releases []Release

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &releases)
	})

	return releases, err
}

// Latest returns the most recent release for a lease. Returns nil, nil when not found.
func (s *ReleaseStore) Latest(leaseUUID string) (*Release, error) {
	releases, err := s.List(leaseUUID)
	if err != nil || len(releases) == 0 {
		return nil, err
	}
	return &releases[len(releases)-1], nil
}

// UpdateLatestStatus updates the status (and optionally error) of the most recent release.
func (s *ReleaseStore) UpdateLatestStatus(leaseUUID, status, errMsg string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}

		var releases []Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return fmt.Errorf("failed to unmarshal releases: %w", err)
		}
		if len(releases) == 0 {
			return nil
		}

		releases[len(releases)-1].Status = status
		releases[len(releases)-1].Error = errMsg

		encoded, err := json.Marshal(releases)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

// Delete removes all releases for a lease. No-op if not found.
func (s *ReleaseStore) Delete(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// RemoveOlderThan deletes release entries where all releases are older than maxAge.
func (s *ReleaseStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var releases []Release
			if err := json.Unmarshal(v, &releases); err != nil {
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
				continue
			}

			// Remove entry if all releases are older than cutoff
			allOld := true
			for _, r := range releases {
				if !r.CreatedAt.Before(cutoff) {
					allOld = false
					break
				}
			}
			if allOld {
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

// Close shuts down the release store gracefully.
func (s *ReleaseStore) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
