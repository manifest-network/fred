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
	byTenant  map[string]map[string]struct{} // tenant -> set of bbolt bucket keys (the authoritative lease UUID; see scanIndex/indexApply)
	byStatus  map[string]map[string]struct{} // status -> set of bbolt bucket keys (the authoritative lease UUID; see scanIndex/indexApply)
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

// ReIndex rebuilds the in-memory index from the primary bucket and swaps it in.
// Safe on a live store: it holds s.mu across the WHOLE scan+swap, which serializes it with
// mutators (each holds s.mu across its {db.Update + indexApply}). Holding the lock only for the
// swap would be unsafe — a mutator could commit a write and update the live index between the
// scan returning and the swap, and the swap would then overwrite that write with a pre-scan
// snapshot (lost update → drift until the next rebuild). fireReindex runs outside the lock.
// The self-heal/recovery seam (the index is never the source of truth).
func (s *RetentionStore) ReIndex() error {
	start := time.Now()
	s.mu.Lock()
	byTenant, byStatus, count, err := s.scanIndex()
	if err != nil {
		s.mu.Unlock()
		return err
	}
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
			// Index on the bucket KEY, not e.OriginalLeaseUUID: the key is the authoritative
			// UUID that getAll resolves via Get(uuid), so keying on it keeps the index→Get
			// round-trip correct even if a record's value UUID is empty/mismatched (partial
			// corruption / older format / manual edit). For store-written records key == value.
			uuid := string(k)
			idxAdd(byTenant, e.Tenant, uuid)
			idxAdd(byStatus, e.Status, uuid)
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
// oldE=nil → insert; newE=nil → delete; both set → move. The set member is the caller-supplied
// uuid — the bbolt bucket KEY, which is authoritative and is what getAll resolves via Get; it
// must NOT be derived from oldE/newE.OriginalLeaseUUID, so a record whose stored value UUID is
// empty/mismatched (corruption / old format / manual edit) is still indexed under its real key,
// consistent with scanIndex. Partitions come from the entries (tenant immutability is observed,
// not assumed/optimized).
func (s *RetentionStore) indexApply(uuid string, oldE, newE *RetentionEntry) {
	if oldE != nil {
		idxDel(s.byTenant, oldE.Tenant, uuid)
		idxDel(s.byStatus, oldE.Status, uuid)
	}
	if newE != nil {
		idxAdd(s.byTenant, newE.Tenant, uuid)
		idxAdd(s.byStatus, newE.Status, uuid)
	}
}

// Put persists a RetentionEntry, upserting by OriginalLeaseUUID. It reads the
// pre-image in-txn so the index can drop the stale partition membership of any
// record being overwritten (a status/tenant change must not leave a phantom).
func (s *RetentionStore) Put(e RetentionEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal retention entry: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldE *RetentionEntry
	err = s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(e.OriginalLeaseUUID)); raw != nil {
			oldE = &RetentionEntry{}
			if uerr := json.Unmarshal(raw, oldE); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", e.OriginalLeaseUUID, uerr)
			}
		}
		return bkt.Put([]byte(e.OriginalLeaseUUID), data)
	})
	if err != nil {
		return err
	}
	s.indexApply(e.OriginalLeaseUUID, oldE, &e)
	return nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		ok   bool
		oldE *RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", base.OriginalLeaseUUID, err)
			}
			oldE = &stored
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
	if err != nil {
		return false, err
	}
	if ok {
		s.indexApply(base.OriginalLeaseUUID, oldE, &base)
	}
	return ok, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		ok   bool
		oldE *RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", base.OriginalLeaseUUID, err)
			}
			// Capture the pre-image as a value copy BEFORE the reaping-branch
			// `base = stored` assignment below aliases stored into base.
			preImage := stored
			oldE = &preImage
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
	if err != nil {
		return false, err
	}
	if ok {
		s.indexApply(base.OriginalLeaseUUID, oldE, &base)
	}
	return ok, nil
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
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		return nil
	})
	return entry, err
}

// Delete removes a RetentionEntry by original lease UUID. It is idempotent:
// no error is returned when the entry is absent. It reads the pre-image in-txn
// so the index can drop the deleted record's partition membership.
func (s *RetentionStore) Delete(orig string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldE *RetentionEntry
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(orig)); raw != nil {
			oldE = &RetentionEntry{}
			if uerr := json.Unmarshal(raw, oldE); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", orig, uerr)
			}
		}
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return err
	}
	s.indexApply(orig, oldE, nil) // oldE=nil when absent → no-op
	return nil
}

// List returns all RetentionEntry records in the store.
func (s *RetentionStore) List() ([]RetentionEntry, error) {
	return s.filter(func(_ *RetentionEntry) bool { return true })
}

// Keys returns every retained lease UUID (the bbolt key) without unmarshalling the heavy
// record value (skips the dominant per-record json.Unmarshal). string(k) copies, so no
// cursor bytes escape the transaction.
func (s *RetentionStore) Keys() ([]string, error) {
	var out []string
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(k, _ []byte) error {
			out = append(out, string(k))
			return nil
		})
	})
	return out, err
}

// KeysPage returns one keyset page of retained lease UUIDs (the bbolt keys) in
// ascending key order, containing the keys strictly greater than `after`. It
// uses a bbolt cursor Seek so the read is O(limit), not a full-bucket scan, and
// (like Keys) skips the heavy per-record value unmarshal.
//
//   - limit <= 0 -> returns all keys (unpaginated passthrough), next "".
//   - limit  > 0 -> returns up to limit keys; next is the last returned key iff a
//     full page was returned AND more keys remain, otherwise "".
//
// The returned slice is always non-nil so callers serialize it as [] not null.
// Precondition: keys are canonical lease UUIDs — bbolt stores keys byte-sorted,
// which matches the client's keyset cursor order (canonical-lowercase UUID).
func (s *RetentionStore) KeysPage(after string, limit int) (keys []string, next string, err error) {
	keys = []string{}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(retentionBucketName)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		var k []byte
		if after == "" {
			k, _ = c.First()
		} else {
			// Seek lands on the first key >= after; advance past an exact match so
			// the page starts strictly after the cursor (keyset semantics).
			k, _ = c.Seek([]byte(after))
			if k != nil && string(k) == after {
				k, _ = c.Next()
			}
		}

		for ; k != nil; k, _ = c.Next() {
			if limit > 0 && len(keys) == limit {
				next = keys[len(keys)-1] // full page + at least one more key remains
				return nil
			}
			keys = append(keys, string(k)) // string(k) copies; no cursor bytes escape the txn
		}
		return nil
	})
	return keys, next, err
}

// ListExpired returns active entries whose CreatedAt is older than maxAge.
func (s *RetentionStore) ListExpired(maxAge time.Duration) ([]RetentionEntry, error) {
	cutoff := time.Now().Add(-maxAge)
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusActive && e.CreatedAt.Before(cutoff)
	})
}

// keysOf copies a set's keys (caller holds the lock).
func keysOf(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for u := range set {
		out = append(out, u)
	}
	return out
}

// getAll Gets each UUID from the primary in ONE db.View (so the per-record reads share a
// single MVCC snapshot and N txns collapse to 1), skipping nil (concurrently deleted) and
// re-applying keep on the fetched entry (it may have left the partition since the index
// snapshot). Eventually-consistent: the index snapshot and the View are separate
// observations; callers re-validate via CAS before destructive action (see doc comments).
func (s *RetentionStore) getAll(uuids []string, keep func(*RetentionEntry) bool) ([]RetentionEntry, error) {
	out := make([]RetentionEntry, 0, len(uuids))
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		for _, u := range uuids {
			raw := bkt.Get([]byte(u))
			if raw == nil {
				continue
			}
			var e RetentionEntry
			if uerr := json.Unmarshal(raw, &e); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", u, uerr)
			}
			if keep(&e) {
				out = append(out, e)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListByTenant returns all entries for the given tenant (eventually-consistent; callers
// must re-validate via CAS before mutating — see type doc). Served from the index.
func (s *RetentionStore) ListByTenant(tenant string) ([]RetentionEntry, error) {
	s.mu.RLock()
	uuids := keysOf(s.byTenant[tenant])
	s.mu.RUnlock()
	return s.getAll(uuids, func(e *RetentionEntry) bool { return e.Tenant == tenant })
}

// ListRestoring returns all entries currently restoring (eventually-consistent). Served from the index.
func (s *RetentionStore) ListRestoring() ([]RetentionEntry, error) {
	s.mu.RLock()
	uuids := keysOf(s.byStatus[RetentionStatusRestoring])
	s.mu.RUnlock()
	return s.getAll(uuids, func(e *RetentionEntry) bool { return e.Status == RetentionStatusRestoring })
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names   []string
		deleted bool
		oldE    RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		oldE = e
		names = e.RetainedVolumeNames
		deleted = true
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return nil, false, err
	}
	if deleted {
		s.indexApply(orig, &oldE, nil)
	}
	return names, deleted, nil
}

// filter iterates all bucket entries and returns those for which keep returns true.
func (s *RetentionStore) filter(keep func(*RetentionEntry) bool) ([]RetentionEntry, error) {
	var results []RetentionEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		return bkt.ForEach(func(k, v []byte) error {
			var e RetentionEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(k), err)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		out  *RetentionEntry
		oldE RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return ErrNoRetention
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if e.Status != RetentionStatusActive {
			return ErrNotRestorable
		}
		if maxAge > 0 && time.Since(e.CreatedAt) >= maxAge {
			return ErrNoRetention // about to be reaped
		}
		oldE = e // value copy of the ACTIVE pre-image, before mutation
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
	if err != nil {
		return nil, err
	}
	if out != nil {
		s.indexApply(orig, &oldE, out)
	}
	return out, nil
}

// MarkReapingIfActive atomically transitions an ACTIVE record to reaping and
// returns its volume names for the caller to destroy AFTER the txn commits.
// ok=false (nil names) when absent or not active (e.g. concurrently claimed for
// restore). The record is NOT deleted — it is the finalizer tombstone that keeps
// the footprint counted until the volumes are confirmed gone. (ENG-376)
func (s *RetentionStore) MarkReapingIfActive(orig string) ([]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names []string
		ok    bool
		oldE  RetentionEntry
		newE  RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusActive {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusReaping
		newE.ReapingSince = time.Now()
		names = newE.RetainedVolumeNames
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return nil, false, err
	}
	if ok {
		s.indexApply(orig, &oldE, &newE)
	}
	return names, ok, nil
}

// MarkReapingIfExpired atomically transitions an ACTIVE, expired record to
// reaping and returns its volume names for the caller to destroy AFTER the txn
// commits. The record is NOT deleted — it stays a
// counted tombstone until the volumes are confirmed gone. Returns ok=false when
// absent, not active, or not yet expired, and a no-op when maxAge<=0. (ENG-376)
func (s *RetentionStore) MarkReapingIfExpired(orig string, maxAge time.Duration) ([]string, bool, error) {
	if maxAge <= 0 {
		return nil, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names []string
		ok    bool
		oldE  RetentionEntry
		newE  RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusActive {
			return nil
		}
		if time.Since(oldE.CreatedAt) < maxAge {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusReaping
		newE.ReapingSince = time.Now()
		names = newE.RetainedVolumeNames
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return nil, false, err
	}
	if ok {
		s.indexApply(orig, &oldE, &newE)
	}
	return names, ok, nil
}

// RevertToActive transitions a restoring record back to active, using a
// compare-and-swap on Generation. Returns (true, nil) on success, (false, nil)
// when the record is absent, not in restoring state, or the generation does not
// match. On success the Generation is bumped and NewLeaseUUID/RestoringSince
// are cleared.
func (s *RetentionStore) RevertToActive(orig string, expectGen int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		swapped bool
		oldE    RetentionEntry
		newE    RetentionEntry
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusRestoring {
			return nil
		}
		if oldE.Generation != expectGen {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusActive
		newE.Generation++
		newE.NewLeaseUUID = ""
		newE.RestoringSince = time.Time{}
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		swapped = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return false, err
	}
	if swapped {
		s.indexApply(orig, &oldE, &newE)
	}
	return swapped, nil
}
