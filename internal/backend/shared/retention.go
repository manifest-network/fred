package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

var retentionBucketName = []byte("retention")

// RetentionStatusActive is the status of an active (held) retention entry.
const RetentionStatusActive = "active"

// RetentionStatusRestoring is the status of an entry currently being restored.
const RetentionStatusRestoring = "restoring"

var (
	// ErrNoRetention is returned when no retained data exists for a given lease UUID.
	ErrNoRetention = errors.New("no retained data for lease")
	// ErrNotRestorable is returned when a retained lease is not in a restorable state.
	ErrNotRestorable = errors.New("retained lease not in a restorable state")
)

// RetentionEntry records the data needed to restore a soft-deleted lease.
type RetentionEntry struct {
	OriginalLeaseUUID   string                  `json:"original_lease_uuid"`
	Tenant              string                  `json:"tenant"`
	ProviderUUID        string                  `json:"provider_uuid"`
	Items               []backend.LeaseItem     `json:"items"`
	StackManifest       *manifest.StackManifest `json:"stack_manifest"`
	CallbackURL         string                  `json:"callback_url"`
	RetainedVolumeNames []string                `json:"retained_volume_names"`
	Status              string                  `json:"status"`
	NewLeaseUUID        string                  `json:"new_lease_uuid,omitempty"`
	Generation          int                     `json:"generation"`
	CreatedAt           time.Time               `json:"created_at"`
	RestoringSince      time.Time               `json:"restoring_since,omitempty"`
}

// RetentionStoreConfig configures the retention store.
type RetentionStoreConfig struct {
	DBPath string
}

// RetentionStore persists soft-deleted lease data in bbolt so volumes and
// manifests survive lease closure and are available for restore.
type RetentionStore struct {
	*boltStore
}

// NewRetentionStore opens or creates a bbolt database for retention persistence.
// No background cleanup loop is started; the docker backend drives reaping
// explicitly via ReapIfExpired / ListExpired.
func NewRetentionStore(cfg RetentionStoreConfig) (*RetentionStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: retentionBucketName,
		Label:      "retention",
	})
	if err != nil {
		return nil, err
	}
	return &RetentionStore{boltStore: base}, nil
}

// Put persists a RetentionEntry, upserting by OriginalLeaseUUID.
func (s *RetentionStore) Put(e RetentionEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal retention entry: %w", err)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		return bkt.Put([]byte(e.OriginalLeaseUUID), data)
	})
}

// Get retrieves a RetentionEntry by original lease UUID.
// Returns nil, nil when absent.
func (s *RetentionStore) Get(orig string) (*RetentionEntry, error) {
	var entry *RetentionEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		entry = &RetentionEntry{}
		if err := json.Unmarshal(raw, entry); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		return nil
	})
	return entry, err
}

// Delete removes a RetentionEntry by original lease UUID. It is idempotent:
// no error is returned when the entry is absent.
func (s *RetentionStore) Delete(orig string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		return bkt.Delete([]byte(orig))
	})
}

// List returns all RetentionEntry records in the store.
func (s *RetentionStore) List() ([]RetentionEntry, error) {
	return s.filter(func(_ *RetentionEntry) bool { return true })
}

// ListExpired returns active entries whose CreatedAt is older than maxAge.
func (s *RetentionStore) ListExpired(maxAge time.Duration) ([]RetentionEntry, error) {
	cutoff := time.Now().Add(-maxAge)
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusActive && e.CreatedAt.Before(cutoff)
	})
}

// ListByTenant returns all entries for the given tenant.
func (s *RetentionStore) ListByTenant(tenant string) ([]RetentionEntry, error) {
	return s.filter(func(e *RetentionEntry) bool {
		return e.Tenant == tenant
	})
}

// ListRestoring returns all entries currently in the restoring state.
func (s *RetentionStore) ListRestoring() ([]RetentionEntry, error) {
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusRestoring
	})
}

// filter iterates all bucket entries and returns those for which keep returns true.
func (s *RetentionStore) filter(keep func(*RetentionEntry) bool) ([]RetentionEntry, error) {
	var results []RetentionEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		return bkt.ForEach(func(_, v []byte) error {
			var e RetentionEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("failed to unmarshal retention entry: %w", err)
			}
			if keep(&e) {
				results = append(results, e)
			}
			return nil
		})
	})
	return results, err
}

// ClaimForRestore atomically transitions an ACTIVE, non-expired record to
// restoring. Returns ErrNoRetention when absent or expired, ErrNotRestorable
// when not in active state.
func (s *RetentionStore) ClaimForRestore(orig, newLease string, maxAge time.Duration) (*RetentionEntry, error) {
	var out *RetentionEntry
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return ErrNoRetention
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusActive {
			return ErrNotRestorable
		}
		if maxAge > 0 && time.Since(e.CreatedAt) >= maxAge {
			return ErrNoRetention // about to be reaped
		}
		e.Status = RetentionStatusRestoring
		e.NewLeaseUUID = newLease
		e.RestoringSince = time.Now()
		e.Generation++
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		out = &e
		return bkt.Put([]byte(orig), data)
	})
	return out, err
}

// ReapIfExpired deletes the record only if it is STILL active AND expired.
// Returns the retained volume names for the caller to destroy AFTER the
// transaction commits. Returns nil, nil when the record is absent, not active,
// or not yet expired.
func (s *RetentionStore) ReapIfExpired(orig string, maxAge time.Duration) ([]string, error) {
	if maxAge <= 0 {
		return nil, nil
	}
	var names []string
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		if time.Since(e.CreatedAt) < maxAge {
			return nil
		}
		names = e.RetainedVolumeNames
		return bkt.Delete([]byte(orig))
	})
	return names, err
}

// DeleteIfActive atomically removes a record ONLY if it is still ACTIVE,
// returning its retained volume names for the caller to destroy AFTER the
// txn commits. deleted=false (nil names) when absent or not active (e.g.
// concurrently claimed for restore), so cap-eviction never races a restore.
func (s *RetentionStore) DeleteIfActive(orig string) ([]string, bool, error) {
	var (
		names   []string
		deleted bool
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		names = e.RetainedVolumeNames
		deleted = true
		return bkt.Delete([]byte(orig))
	})
	return names, deleted, err
}

// RevertToActive transitions a restoring record back to active, using a
// compare-and-swap on Generation. Returns (true, nil) on success, (false, nil)
// when the record is absent, not in restoring state, or the generation does not
// match. On success the Generation is bumped and NewLeaseUUID/RestoringSince
// are cleared.
func (s *RetentionStore) RevertToActive(orig string, expectGen int) (bool, error) {
	var swapped bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusRestoring {
			return nil
		}
		if e.Generation != expectGen {
			return nil
		}
		e.Status = RetentionStatusActive
		e.Generation++
		e.NewLeaseUUID = ""
		e.RestoringSince = time.Time{}
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		swapped = true
		return bkt.Put([]byte(orig), data)
	})
	return swapped, err
}
