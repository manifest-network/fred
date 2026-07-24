package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/util"
)

var releasesBucketName = []byte("releases")

// Release represents a single release (deployment) of a lease.
type Release struct {
	Version   int            `json:"version"`
	Manifest  []byte         `json:"manifest"`
	Image     string         `json:"image"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	Error     string         `json:"error,omitempty"`
	Reason    backend.Reason `json:"reason,omitempty"`
	Message   string         `json:"message,omitempty"`
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

// maxVersion returns the highest Version among releases, or 0 for an empty slice.
// Append derives the next version from this (not len) so that within-key pruning by
// RemoveOlderThan's keep-latest guard can never cause a version to be reused: the
// pruning always retains the index-latest entry, which holds the maximum version, so
// maxVersion(remaining)+1 stays strictly greater than every version ever issued for
// the lease (ENG-440). Uses the Go 1.21 max builtin (do not name the accumulator
// `max`, which would shadow it).
func maxVersion(releases []Release) int {
	highest := 0
	for _, r := range releases {
		highest = max(highest, r.Version)
	}
	return highest
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

		r.Version = maxVersion(releases) + 1
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

// UpdateLatestStatus updates the status and curated (reason, message) of the
// most recent release. The verbose per-failure detail is intentionally NOT
// stored here — callers log it and surface only the curated pair to tenants.
func (s *ReleaseStore) UpdateLatestStatus(leaseUUID, status string, reason backend.Reason, message string) error {
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
		releases[len(releases)-1].Reason = reason
		releases[len(releases)-1].Message = message

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
		releases[len(releases)-1].Reason = ""
		releases[len(releases)-1].Message = ""

		encoded, err := json.Marshal(releases)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

// RecordMigration appends an "active" release entry for a recover-time
// migration so the lease's release history captures the wrap-and-rename
// step. Idempotent: if the most-recent active entry already carries the
// same wrapped manifest bytes (byte-equal), the call is a no-op. This
// lets the migration pipeline re-run on the next startup if it was
// interrupted between rename and recover-loop completion without
// inflating release history.
//
// Used exclusively by the docker backend's migrate.go (Task 9).
func (s *ReleaseStore) RecordMigration(leaseUUID string, manifest []byte) error {
	if latest, _ := s.LatestActive(leaseUUID); latest != nil && bytes.Equal(latest.Manifest, manifest) {
		return nil
	}
	return s.Append(leaseUUID, Release{
		Manifest:  manifest,
		Image:     "stack",
		Status:    "active",
		CreatedAt: time.Now(),
	})
}

// Delete removes all releases for a lease. No-op if not found.
func (s *ReleaseStore) Delete(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// RemoveOlderThan prunes release history older than maxAge while preserving each
// lease's load-bearing records. For every lease it ALWAYS retains its index-latest
// entry — the entry the runtime mutators append to / activate, and (because Append
// assigns the next version to the tail) the holder of the lease's maximum version,
// which keeps Append's max+1 derivation collision-free — AND, when a distinct one
// exists, the most-recent "active" release that recoverState rehydrates the
// StackManifest from (see LatestActive). So it protects one entry when the newest
// entry is itself the active release or no active release exists, two otherwise. Only
// entries that are BOTH older than the cutoff AND not protected are pruned, so a
// lease's record is never emptied and its live manifest is never removed (ENG-440).
// Corrupt or empty values are removed whole-key. Returns the number of release ENTRIES
// removed (corrupt/empty keys count as one).
func (s *ReleaseStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)

		// Decide all changes during a read-only cursor pass, then apply them after the
		// loop. Mutating the bucket (Put/Delete) while the cursor is live can split/
		// rebalance pages and invalidate the cursor. Cursor keys alias tx-lifetime
		// pages reused across iterations, so any retained key must be copied.
		type change struct {
			key     []byte
			value   []byte // nil => delete the whole key
			removed int
		}
		var changes []change

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var releases []Release
			if err := json.Unmarshal(v, &releases); err != nil {
				slog.Warn("deleting corrupted release entry during cleanup",
					"lease_uuid", string(k), "error", err)
				changes = append(changes, change{key: append([]byte(nil), k...), removed: 1})
				continue
			}
			if len(releases) == 0 {
				// Empty/null value: nothing to protect; treat like a corrupt key.
				changes = append(changes, change{key: append([]byte(nil), k...), removed: 1})
				continue
			}

			// Protected indices (never pruned): the index-latest entry (holds the max
			// version) and the most-recent "active" entry (the manifest-rehydration
			// source). At most two, so plain ints — no per-key map allocation.
			keepLatest := len(releases) - 1
			keepActive := -1
			for i := len(releases) - 1; i >= 0; i-- {
				if releases[i].Status == "active" {
					keepActive = i
					break
				}
			}

			kept := make([]Release, 0, len(releases))
			prunedHere := 0
			for i, r := range releases {
				if i != keepLatest && i != keepActive && r.CreatedAt.Before(cutoff) {
					prunedHere++
					continue
				}
				kept = append(kept, r)
			}
			if prunedHere == 0 {
				continue // unchanged: don't rewrite an untouched key
			}

			encoded, err := json.Marshal(kept)
			if err != nil {
				return fmt.Errorf("failed to marshal pruned releases for %s: %w", string(k), err)
			}
			changes = append(changes, change{key: append([]byte(nil), k...), value: encoded, removed: prunedHere})
		}

		for _, ch := range changes {
			if ch.value == nil {
				if err := b.Delete(ch.key); err != nil {
					return err
				}
			} else {
				if err := b.Put(ch.key, ch.value); err != nil {
					return err
				}
			}
			removed += ch.removed
		}
		return nil
	})

	return removed, err
}
