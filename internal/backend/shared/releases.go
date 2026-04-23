package shared

import (
	"encoding/json"
	"fmt"
	"log/slog"
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
	*boltStore
}

// ReleaseStoreConfig configures the release store.
type ReleaseStoreConfig struct {
	DBPath          string
	MaxAge          time.Duration
	CleanupInterval time.Duration
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic.
}

// NewReleaseStore opens or creates a bbolt database for release persistence.
func NewReleaseStore(cfg ReleaseStoreConfig) (*ReleaseStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: releasesBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "releases",
	})
	if err != nil {
		return nil, err
	}

	s := &ReleaseStore{boltStore: base}

	if cfg.MaxAge > 0 {
		base.startCleanup("releases", cfg.CleanupInterval, s.RemoveOlderThan, cfg.OnCleanupPanic)
	}

	return s, nil
}

// Append adds a new release for a lease, auto-assigning Version.
func (s *ReleaseStore) Append(leaseUUID string, r Release) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		var releases []Release

		data := b.Get([]byte(leaseUUID))
		if data != nil {
			if err := json.Unmarshal(data, &releases); err != nil {
				return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
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

// LatestActive returns the most recent release with "active" status for a lease.
// Returns nil, nil when no active release is found.
func (s *ReleaseStore) LatestActive(leaseUUID string) (*Release, error) {
	releases, err := s.List(leaseUUID)
	if err != nil || len(releases) == 0 {
		return nil, err
	}
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i].Status == "active" {
			return &releases[i], nil
		}
	}
	return nil, nil
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

// ActivateLatest marks the most recent release as "active" and all previous
// "active" releases as "superseded" in a single transaction.
func (s *ReleaseStore) ActivateLatest(leaseUUID string) error {
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

		for i := range len(releases) - 1 {
			if releases[i].Status == "active" {
				releases[i].Status = "superseded"
			}
		}
		releases[len(releases)-1].Status = "active"
		releases[len(releases)-1].Error = ""

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
				slog.Warn("deleting corrupted release entry during cleanup",
					"lease_uuid", string(k),
					"error", err,
				)
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
