package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// RetentionStatusReaping marks a record whose volumes are pending physical
// destruction: the bytes are still on disk (so the footprint must keep counting
// in the admission projection) but the record is NOT restore-claimable. It is a
// finalizer tombstone — kept until every volume is confirmed destroyed, then
// Delete()d. See ENG-376.
const RetentionStatusReaping = "reaping"

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
	ReapingSince        time.Time               `json:"reaping_since,omitempty"`
}

// RetentionStoreConfig configures the retention store.
type RetentionStoreConfig struct {
	DBPath string
	// OnReindex (nil-safe) fires after each index build with record count, duration, and
	// trigger ("open"|"manual"). A callback (not a metrics import) so this package stays
	// free of internal/metrics — mirrors boltStore.startCleanup's onPanic seam.
	OnReindex func(count int, dur time.Duration, trigger string)
}

// RetentionStore persists soft-deleted lease data in bbolt. The `retention` bucket is
// the single source of truth; byTenant/byStatus are a DERIVED in-memory index rebuilt
// from the bucket on open (never persisted, cannot drift across a restart). The bucket
// is INDEX-COUPLED: it may be mutated ONLY through this type's wrapped methods (each
// maintains the index under s.mu). Never wire boltStore.startCleanup / removeOlderThan
// to this store — they cursor.Delete directly on the bucket and would bypass the index.
type RetentionStore struct {
	*boltStore
	mu        sync.RWMutex
	byTenant  map[string]map[string]struct{} // tenant -> set of OriginalLeaseUUID
	byStatus  map[string]map[string]struct{} // status -> set of OriginalLeaseUUID
	onReindex func(count int, dur time.Duration, trigger string)
}

// NewRetentionStore opens or creates a bbolt database for retention persistence.
// No background cleanup loop is started; the docker backend drives reaping and
// eviction explicitly via the MarkReaping* / ListReaping / ListExpired methods
// (reapExpiredRetentions, evictRetentionsToCap, and the retryReapingRecords sweep
// in restore.go), plus PutReaping for deprovision give-up tombstones.
func NewRetentionStore(cfg RetentionStoreConfig) (*RetentionStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: retentionBucketName,
		Label:      "retention",
	})
	if err != nil {
		return nil, err
	}
	s := &RetentionStore{boltStore: base, onReindex: cfg.OnReindex}
	// Derived index: rebuilt from the primary bucket on open, before the store is
	// published to any other goroutine (so no lock needed). Fail-closed on a malformed
	// record — a corrupt retention record is a data-integrity event, not something to
	// silently skip.
	start := time.Now()
	byTenant, byStatus, count, err := s.scanIndex()
	if err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to build retention index: %w", err)
	}
	s.byTenant, s.byStatus = byTenant, byStatus
	s.fireReindex(count, time.Since(start), "open")
	return s, nil
}

func (s *RetentionStore) fireReindex(count int, dur time.Duration, trigger string) {
	if s.onReindex != nil {
		s.onReindex(count, dur, trigger)
	}
}

// ReIndex rebuilds the in-memory index from the primary bucket and atomically swaps it in.
// Safe on a live store: builds fresh maps, then assigns under s.mu so readers see either the
// whole old or whole new index. The self-heal/recovery seam (the index is never the source of truth).
func (s *RetentionStore) ReIndex() error {
	start := time.Now()
	byTenant, byStatus, count, err := s.scanIndex()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.byTenant, s.byStatus = byTenant, byStatus
	s.mu.Unlock()
	s.fireReindex(count, time.Since(start), "manual")
	return nil
}

// scanIndex builds fresh tenant/status index maps from one pass over the primary bucket,
// decoding only the three indexed fields (skips the heavy Items/StackManifest allocation;
// encoding/json still scans every byte). Returns the maps + record count. Fails on a
// malformed record (fail-closed).
func (s *RetentionStore) scanIndex() (byTenant, byStatus map[string]map[string]struct{}, count int, err error) {
	byTenant = map[string]map[string]struct{}{}
	byStatus = map[string]map[string]struct{}{}
	err = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(k, v []byte) error {
			var e struct {
				OriginalLeaseUUID string `json:"original_lease_uuid"`
				Tenant            string `json:"tenant"`
				Status            string `json:"status"`
			}
			if uerr := json.Unmarshal(v, &e); uerr != nil {
				// Use the bucket key (the OriginalLeaseUUID by convention) — a
				// totally-malformed record has an empty e.OriginalLeaseUUID, so the
				// operator-facing store-open failure must name the key to be lookup-able.
				return fmt.Errorf("malformed retention record %q: %w", string(k), uerr)
			}
			idxAdd(byTenant, e.Tenant, e.OriginalLeaseUUID)
			idxAdd(byStatus, e.Status, e.OriginalLeaseUUID)
			count++
			return nil
		})
	})
	return byTenant, byStatus, count, err
}

// idxAdd / idxDel maintain a set-valued index map. Caller holds s.mu (or maps not yet
// published). delete on a nil/absent map or missing key is a Go no-op.
func idxAdd(m map[string]map[string]struct{}, key, uuid string) {
	set := m[key]
	if set == nil {
		set = map[string]struct{}{}
		m[key] = set
	}
	set[uuid] = struct{}{}
}

func idxDel(m map[string]map[string]struct{}, key, uuid string) {
	set := m[key]
	if set == nil {
		return
	}
	delete(set, uuid)
	if len(set) == 0 {
		delete(m, key)
	}
}

// indexApply reconciles the index for a record transition. Caller MUST hold s.mu.
// oldE=nil → insert; newE=nil → delete; both set → move. Always removes oldE.Tenant and
// adds newE.Tenant (tenant immutability is observed, not assumed/optimized).
func (s *RetentionStore) indexApply(oldE, newE *RetentionEntry) {
	if oldE != nil {
		idxDel(s.byTenant, oldE.Tenant, oldE.OriginalLeaseUUID)
		idxDel(s.byStatus, oldE.Status, oldE.OriginalLeaseUUID)
	}
	if newE != nil {
		idxAdd(s.byTenant, newE.Tenant, newE.OriginalLeaseUUID)
		idxAdd(s.byStatus, newE.Status, newE.OriginalLeaseUUID)
	}
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

// PutActiveMerged atomically upserts the soft-delete record for a closing lease,
// merging mergeVolumes into any existing record's RetainedVolumeNames. Single txn,
// so it is safe against a concurrent ClaimForRestore (no Get→Put TOCTOU):
//   - absent: writes `base` fresh (caller sets CreatedAt=now, Generation=0, Status=active).
//   - existing ACTIVE: PRESERVES the stored CreatedAt and Generation, writes the
//     UNION of stored RetainedVolumeNames and base.RetainedVolumeNames (dedup), and
//     KEEPS the stored StackManifest when base's is nil (a close retry must never
//     clobber a restorable manifest with a nil one); other fields come from `base`.
//   - existing NON-active (restoring): writes NOTHING, returns ok=false — a restore owns
//     the record; a blind write would corrupt the CAS. Caller defers (keeps lease Failed).
//
// Returns (ok bool, err error): ok=false + nil err means "deferred, record is restoring".
func (s *RetentionStore) PutActiveMerged(base RetentionEntry) (bool, error) {
	var ok bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("failed to unmarshal retention entry: %w", err)
			}
			if stored.Status != RetentionStatusActive {
				return nil // restoring (or otherwise non-active): refuse, ok stays false
			}
			// Existing ACTIVE: preserve the grace clock + CAS generation, union the names.
			base.CreatedAt = stored.CreatedAt
			base.Generation = stored.Generation
			base.RetainedVolumeNames = dedupUnion(stored.RetainedVolumeNames, base.RetainedVolumeNames)
			// Never let a nil base manifest clobber a previously-persisted one. A
			// close retry can recompute base.StackManifest == nil (e.g. a transient
			// release-store hydration failure, or the release reaped between
			// attempts), and Restore rejects nil manifests — clobbering would make
			// an otherwise-restorable lease permanently un-restorable. The manifest
			// is the only hydrated (retry-variable) field, so it is the only one
			// needing this guard.
			if base.StackManifest == nil {
				base.StackManifest = stored.StackManifest
			}
		}
		data, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(base.OriginalLeaseUUID), data)
	})
	return ok, err
}

// PutReaping writes a reaping tombstone for an ABANDONED on-disk footprint (a
// deprovision give-up). It is idempotent and never clobbers a still-counted record:
//   - absent: writes a fresh reaping record (stamps ReapingSince=now).
//   - existing reaping: unions RetainedVolumeNames and PRESERVES ReapingSince (aging).
//   - existing active/restoring: writes NOTHING, returns ok=false — that record
//     already counts the footprint (or owns it for restore); a blind reaping write
//     would corrupt accounting/CAS. Caller treats ok=false as "already tracked".
//
// Single txn, so it is safe against a concurrent ClaimForRestore. (ENG-376)
func (s *RetentionStore) PutReaping(base RetentionEntry) (bool, error) {
	base.Status = RetentionStatusReaping
	base.ReapingSince = time.Now()
	var ok bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("failed to unmarshal retention entry: %w", err)
			}
			switch stored.Status {
			case RetentionStatusActive, RetentionStatusRestoring:
				return nil // already counted/owned — refuse, ok stays false
			case RetentionStatusReaping:
				// Re-leak of a lease that already has a reaping tombstone: preserve the
				// stored entry's accounting/identity fields (Items/Tenant/ProviderUUID/
				// CreatedAt/ReapingSince) WHOLESALE and only union any newly discovered
				// volume names, so a future caller passing partial `base` data can never
				// clobber a still-counted footprint (mirrors PutActiveMerged's
				// preserve-stored idiom; honors this method's "never clobbers" contract).
				stored.RetainedVolumeNames = dedupUnion(stored.RetainedVolumeNames, base.RetainedVolumeNames)
				base = stored
			}
		}
		data, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(base.OriginalLeaseUUID), data)
	})
	return ok, err
}

// dedupUnion returns the order-preserving deduplicated union of a and b
// (a's entries first, then b's not already present).
func dedupUnion(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
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

// ListReaping returns all entries currently in the reaping (pending-destroy) state.
func (s *RetentionStore) ListReaping() ([]RetentionEntry, error) {
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusReaping
	})
}

// DeleteIfActive atomically removes a record ONLY if it is still ACTIVE. Returns
// (names, deleted, err); deleted=false (nil names) when absent or not active (e.g.
// concurrently claimed for restore). Used by reconcileOrphanedRetentions (ENG-370)
// to prune an orphaned active record whose backing volumes have already vanished
// out-of-band — the ACTIVE-only CAS guarantees a concurrent restore (active→restoring)
// is never clobbered. The returned names are unused there: the volumes are already
// gone, so there is nothing to destroy.
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

// MarkReapingIfActive atomically transitions an ACTIVE record to reaping and
// returns its volume names for the caller to destroy AFTER the txn commits.
// ok=false (nil names) when absent or not active (e.g. concurrently claimed for
// restore). The record is NOT deleted — it is the finalizer tombstone that keeps
// the footprint counted until the volumes are confirmed gone. (ENG-376)
func (s *RetentionStore) MarkReapingIfActive(orig string) ([]string, bool, error) {
	var (
		names []string
		ok    bool
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
		e.Status = RetentionStatusReaping
		e.ReapingSince = time.Now()
		names = e.RetainedVolumeNames
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	return names, ok, err
}

// MarkReapingIfExpired atomically transitions an ACTIVE, expired record to
// reaping and returns its volume names for the
// caller to destroy AFTER the txn commits. The record is NOT deleted — it stays a
// counted tombstone until the volumes are confirmed gone. Returns ok=false when
// absent, not active, or not yet expired, and a no-op when maxAge<=0. (ENG-376)
func (s *RetentionStore) MarkReapingIfExpired(orig string, maxAge time.Duration) ([]string, bool, error) {
	if maxAge <= 0 {
		return nil, false, nil
	}
	var (
		names []string
		ok    bool
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
		if time.Since(e.CreatedAt) < maxAge {
			return nil
		}
		e.Status = RetentionStatusReaping
		e.ReapingSince = time.Now()
		names = e.RetainedVolumeNames
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	return names, ok, err
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
