package shared

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// callbackBucketName is the v0.13 callback queue. Keep it readable and
	// otherwise untouched by new writes so a rollback binary never sees a key
	// it cannot remove (v0.13 deletes entries by lease UUID).
	callbackBucketName = []byte("pending_callbacks")

	// callbackV2BucketName stores one entry per delivery UUID. Separating the
	// schema from the legacy bucket makes multiple callbacks for one lease safe
	// while keeping rollback binaries from replaying v2 entries forever.
	callbackV2BucketName = []byte("pending_callbacks_v2")
)

type callbackStorageVersion uint8

const (
	callbackStorageUnknown callbackStorageVersion = iota
	callbackStorageLegacy
	callbackStorageV2
)

// CallbackDeliveryKind identifies whether a durable callback settles a
// requested operation or reports an autonomous lifecycle observation. The
// distinction is persisted so enqueue can safely coalesce stale lifecycle
// observations without ever deleting an undelivered operation completion for
// the same lease.
type CallbackDeliveryKind string

const (
	// CallbackDeliveryKindOperation is an exact requested-operation
	// completion. Operation completions are independent durable deliveries and
	// are never removed by enqueue of another callback.
	CallbackDeliveryKindOperation CallbackDeliveryKind = "operation"

	// CallbackDeliveryKindLifecycle is an observation-only lifecycle event.
	// Enqueueing a newer lifecycle observation atomically replaces older typed
	// lifecycle observations for the lease.
	CallbackDeliveryKindLifecycle CallbackDeliveryKind = "lifecycle"
)

// CallbackEntry represents a pending callback to be delivered.
//
// Success is retained for backwards compatibility with entries persisted by
// binaries that predate the Status field. New writers populate Success AND
// Status (and Backend). Readers prefer Status when non-empty and fall back to
// Success otherwise; see callback_sender.ReplayPendingCallbacks.
type CallbackEntry struct {
	// DeliveryID uniquely identifies this delivery attempt in the durable v2
	// queue. Legacy v0.13 entries have no ID and remain supported on replay.
	DeliveryID  string `json:"delivery_id,omitempty"`
	LeaseUUID   string `json:"lease_uuid"`
	CallbackURL string `json:"callback_url"`
	// DeliveryKind is empty on legacy and pre-kind v2 records. Readers treat an
	// empty or unknown kind conservatively as non-supersedable.
	DeliveryKind CallbackDeliveryKind `json:"delivery_kind,omitempty"`
	// Sequence is allocated from bbolt in the same transaction as enqueue and
	// lifecycle coalescing. Legacy and pre-sequence records have zero and sort
	// before every newly sequenced record.
	Sequence uint64                 `json:"sequence,omitempty"`
	Success  bool                   `json:"success"`
	Status   backend.CallbackStatus `json:"status,omitempty"`
	Backend  string                 `json:"backend,omitempty"`
	Error    string                 `json:"error,omitempty"`
	// Retained persists the best-effort deprovision retain-success flag so a
	// restart-replayed callback keeps it. Legacy entries default to false.
	Retained  bool      `json:"retained,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// storageVersion/storageKey are populated by StoreEntry/ListPending and
	// deliberately excluded from JSON. They let RemoveEntry delete exactly the
	// record that was delivered, including a legacy record without DeliveryID.
	storageVersion callbackStorageVersion
	storageKey     string
	storageDigest  [sha256.Size]byte
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	*boltStore

	// deliveryLocks are shared with every CallbackSender constructed over this
	// store. The store's independently-owned TTL loop therefore joins the exact
	// same per-lease critical section as enqueue/replay/delivery instead of
	// deleting records underneath an in-memory drain snapshot.
	deliveryLocksMu *sync.Mutex
	deliveryLocks   map[string]*callbackLeaseLock
}

// CallbackStoreConfig configures the callback store.
type CallbackStoreConfig struct {
	DBPath          string            // Path to bbolt database file
	MaxAge          time.Duration     // Max age before entries are cleaned up (0 = no expiry)
	CleanupInterval time.Duration     // How often to run cleanup (defaults to MaxAge)
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic (e.g., bump a metric)
}

// NewCallbackStore opens or creates a bbolt database for callback persistence.
// If MaxAge > 0, a background cleanup loop removes expired entries periodically
// and an initial cleanup runs immediately to clear stale entries from previous runs.
func NewCallbackStore(cfg CallbackStoreConfig) (*CallbackStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: callbackBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "callback",
	})
	if err != nil {
		return nil, err
	}

	s := &CallbackStore{
		boltStore:       base,
		deliveryLocksMu: &sync.Mutex{},
		deliveryLocks:   make(map[string]*callbackLeaseLock),
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists(callbackV2BucketName)
		return createErr
	}); err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to create callback v2 bucket: %w", err)
	}

	if cfg.MaxAge > 0 {
		base.startCleanup("callback", cfg.CleanupInterval, s.RemoveOlderThan, cfg.OnCleanupPanic)
	}

	return s, nil
}

// Store persists a callback entry before attempting delivery.
func (s *CallbackStore) Store(entry CallbackEntry) error {
	_, err := s.StoreEntry(entry)
	return err
}

// StoreEntry persists a typed callback and returns its durable delivery
// identity and sequence. Lifecycle enqueue atomically coalesces older typed,
// sequenced lifecycle observations for the same lease; operation completions
// and protected legacy/unknown records remain independent. Callers that remove
// a successfully delivered callback must pass the returned value to
// RemoveEntry; lease-wide removal would discard unrelated exact completions.
func (s *CallbackStore) StoreEntry(entry CallbackEntry) (CallbackEntry, error) {
	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	return s.storeEntryLocked(entry)
}

// storeEntryLocked is StoreEntry's mutation primitive. A CallbackSender calls
// it only while holding its keyed lease lock; the exported store API acquires
// the same lock above so cleanup and test/compatibility callers cannot race a
// live drain.
func (s *CallbackStore) storeEntryLocked(entry CallbackEntry) (CallbackEntry, error) {
	if err := validateCallbackDeliveryKind(entry.DeliveryKind); err != nil {
		return CallbackEntry{}, err
	}
	if entry.Sequence != 0 {
		return CallbackEntry{}, fmt.Errorf("callback sequence is store-assigned")
	}
	if entry.DeliveryID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("failed to allocate callback delivery ID: %w", err)
		}
		entry.DeliveryID = id.String()
	} else if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
		return CallbackEntry{}, err
	}

	var data []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackV2BucketName)
		if b.Get([]byte(entry.DeliveryID)) != nil {
			return fmt.Errorf("callback delivery ID already exists: %s", entry.DeliveryID)
		}
		sequence, seqErr := b.NextSequence()
		if seqErr != nil {
			return fmt.Errorf("failed to allocate callback sequence: %w", seqErr)
		}
		if sequence == 0 {
			return fmt.Errorf("callback sequence exhausted")
		}
		entry.Sequence = sequence
		data, seqErr = json.Marshal(entry)
		if seqErr != nil {
			return fmt.Errorf("failed to marshal callback entry: %w", seqErr)
		}
		if putErr := b.Put([]byte(entry.DeliveryID), data); putErr != nil {
			return putErr
		}

		if entry.DeliveryKind != CallbackDeliveryKindLifecycle {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if string(k) == entry.DeliveryID {
				continue
			}
			var candidate CallbackEntry
			if unmarshalErr := json.Unmarshal(v, &candidate); unmarshalErr != nil ||
				candidate.LeaseUUID != entry.LeaseUUID ||
				candidate.DeliveryKind != CallbackDeliveryKindLifecycle ||
				candidate.Sequence == 0 || candidate.Sequence >= entry.Sequence ||
				candidate.DeliveryID != string(k) ||
				validateCallbackDeliveryID(candidate.DeliveryID) != nil {
				continue
			}
			if deleteErr := c.Delete(); deleteErr != nil {
				return deleteErr
			}
		}
		return nil
	})
	if err != nil {
		return CallbackEntry{}, err
	}

	entry.storageVersion = callbackStorageV2
	entry.storageKey = entry.DeliveryID
	entry.storageDigest = sha256.Sum256(data)
	return entry, nil
}

// Remove deletes every pending callback for a lease. It intentionally remains
// lease-wide for deprovision cleanup and compatibility with legacy callers.
// Delivery success paths must use RemoveEntry instead.
func (s *CallbackStore) Remove(leaseUUID string) error {
	unlock := s.lockDeliveryLease(leaseUUID)
	defer unlock()
	return s.removeLeaseLocked(leaseUUID)
}

func (s *CallbackStore) removeLeaseLocked(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, source := range []struct {
			bucket []byte
			legacy bool
		}{
			{callbackBucketName, true},
			{callbackV2BucketName, false},
		} {
			b := tx.Bucket(source.bucket)
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				// The direct key comparison preserves removal of malformed legacy
				// records whose value can no longer be decoded.
				if source.legacy && string(k) == leaseUUID {
					if err := c.Delete(); err != nil {
						return err
					}
					continue
				}

				var entry CallbackEntry
				if err := json.Unmarshal(v, &entry); err != nil || entry.LeaseUUID != leaseUUID {
					continue
				}
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RemoveEntry deletes exactly one delivered callback. The entry must come
// from StoreEntry or ListPending so its durable bucket/key identity is known.
func (s *CallbackStore) RemoveEntry(entry CallbackEntry) error {
	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	return s.removeEntryLocked(entry)
}

func (s *CallbackStore) removeEntryLocked(entry CallbackEntry) error {
	bucketName := callbackV2BucketName
	key := entry.DeliveryID
	isV2 := true
	switch entry.storageVersion {
	case callbackStorageLegacy:
		bucketName = callbackBucketName
		key = entry.storageKey
		isV2 = false
	case callbackStorageV2:
		key = entry.storageKey
		if entry.DeliveryID != key {
			return fmt.Errorf("callback delivery ID %q does not match durable key %q", entry.DeliveryID, key)
		}
	case callbackStorageUnknown:
		return fmt.Errorf("callback entry has no durable storage capability")
	default:
		return fmt.Errorf("unknown callback storage version %d", entry.storageVersion)
	}
	if key == "" {
		return fmt.Errorf("callback entry has empty durable storage key")
	}
	if entry.storageDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("callback entry has no durable value capability")
	}
	if isV2 {
		if err := validateCallbackDeliveryID(key); err != nil {
			return err
		}
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		current := b.Get([]byte(key))
		if current == nil {
			return nil
		}
		if sha256.Sum256(current) != entry.storageDigest {
			return fmt.Errorf("callback entry changed before precise removal")
		}
		return b.Delete([]byte(key))
	})
}

// ListPending returns all pending callback entries for durable outbox replay.
func (s *CallbackStore) ListPending() ([]CallbackEntry, error) {
	return s.listPending("")
}

// listPending returns the durable FIFO for one lease, or every lease when
// leaseUUID is empty. Callers use the all-leases form only for discovery;
// delivery re-lists a lease while holding its keyed drain lock.
func (s *CallbackStore) listPending(leaseUUID string) ([]CallbackEntry, error) {
	var entries []CallbackEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		for _, source := range []struct {
			bucket  []byte
			version callbackStorageVersion
		}{
			{callbackBucketName, callbackStorageLegacy},
			{callbackV2BucketName, callbackStorageV2},
		} {
			b := tx.Bucket(source.bucket)
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var entry CallbackEntry
				if err := json.Unmarshal(v, &entry); err != nil {
					return fmt.Errorf("failed to decode callback entry in bucket %q at key %q: %w",
						string(source.bucket), string(k), err)
				}
				if entry.LeaseUUID == "" {
					return fmt.Errorf("callback entry in bucket %q at key %q has empty lease identity",
						string(source.bucket), string(k))
				}
				if source.version == callbackStorageLegacy && entry.LeaseUUID != string(k) {
					return fmt.Errorf("legacy callback lease identity mismatch: key %q contains %q",
						string(k), entry.LeaseUUID)
				}
				if source.version == callbackStorageV2 {
					if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
						return fmt.Errorf("invalid callback delivery identity in bucket %q at key %q: %w",
							string(source.bucket), string(k), err)
					}
					if entry.DeliveryID != string(k) {
						return fmt.Errorf("callback delivery identity mismatch in bucket %q: key %q contains %q",
							string(source.bucket), string(k), entry.DeliveryID)
					}
				}
				if leaseUUID != "" && entry.LeaseUUID != leaseUUID {
					continue
				}
				entry.storageVersion = source.version
				entry.storageKey = string(k)
				entry.storageDigest = sha256.Sum256(v)
				entries = append(entries, entry)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Protected unknown records sort before all typed sequenced records. This
	// deliberately favors duplicate delivery over allowing a new lifecycle
	// observation to overtake a legacy or pre-kind exact completion.
	sort.SliceStable(entries, func(i, j int) bool {
		iSequenced := entries[i].sequenced()
		jSequenced := entries[j].sequenced()
		if iSequenced != jSequenced {
			return !iSequenced
		}
		if iSequenced {
			if entries[i].Sequence != entries[j].Sequence {
				return entries[i].Sequence < entries[j].Sequence
			}
			return entries[i].storageKey < entries[j].storageKey
		}
		if entries[i].storageVersion != entries[j].storageVersion {
			return entries[i].storageVersion < entries[j].storageVersion
		}
		return entries[i].storageKey < entries[j].storageKey
	})

	return entries, nil
}

// RemoveOlderThan expires callback deliveries without creating a hole in a
// lease FIFO. A typed queue is deleted only when every record for that lease is
// expired; retaining a fresh suffix while deleting an older exact completion
// would let that suffix overtake the completion. Legacy, pre-kind, and
// pre-sequence records are protected indefinitely because their operation-vs-
// lifecycle meaning is unknowable. Malformed data fails the whole pass closed
// and remains available for operator recovery instead of being silently erased.
func (s *CallbackStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	leaseUUIDs, err := s.callbackLeaseUUIDs()
	if err != nil {
		return 0, err
	}

	removed := 0
	lockedLeases := make(map[string]struct{}, len(leaseUUIDs))
	unlockers := make([]func(), 0, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		unlock, acquired := s.tryLockDeliveryLease(leaseUUID)
		if !acquired {
			// Delivery owns this lease. Skipping one TTL pass is safer than
			// waiting behind its HTTP retry budget or mutating its drain snapshot.
			continue
		}
		lockedLeases[leaseUUID] = struct{}{}
		unlockers = append(unlockers, unlock)
	}
	defer func() {
		for _, unlock := range unlockers {
			unlock()
		}
	}()
	if len(lockedLeases) == 0 {
		return 0, nil
	}

	removed, err = s.removeExpiredLeasesLocked(lockedLeases, maxAge)
	if err != nil {
		return 0, err
	}
	return removed, nil
}

type callbackExpiryRef struct {
	bucket  []byte
	key     string
	entry   CallbackEntry
	unknown bool
}

// callbackLeaseUUIDs validates the complete durable queue before cleanup makes
// its first mutation. Sorting makes lock acquisition and tests deterministic.
func (s *CallbackStore) callbackLeaseUUIDs() ([]string, error) {
	leases := make(map[string]struct{})
	err := s.db.View(func(tx *bolt.Tx) error {
		return walkCallbackEntries(tx, func(ref callbackExpiryRef) error {
			leases[ref.entry.LeaseUUID] = struct{}{}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	leaseUUIDs := make([]string, 0, len(leases))
	for leaseUUID := range leases {
		leaseUUIDs = append(leaseUUIDs, leaseUUID)
	}
	sort.Strings(leaseUUIDs)
	return leaseUUIDs, nil
}

func (s *CallbackStore) removeExpiredLeasesLocked(leaseUUIDs map[string]struct{}, maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		refsByLease := make(map[string][]callbackExpiryRef, len(leaseUUIDs))
		if err := walkCallbackEntries(tx, func(ref callbackExpiryRef) error {
			if _, locked := leaseUUIDs[ref.entry.LeaseUUID]; locked {
				refsByLease[ref.entry.LeaseUUID] = append(refsByLease[ref.entry.LeaseUUID], ref)
			}
			return nil
		}); err != nil {
			return err
		}

		for _, refs := range refsByLease {
			eligible := len(refs) != 0
			for _, ref := range refs {
				if ref.unknown || !ref.entry.CreatedAt.Before(cutoff) {
					eligible = false
					break
				}
			}
			if !eligible {
				continue
			}
			for _, ref := range refs {
				b := tx.Bucket(ref.bucket)
				if b == nil {
					return fmt.Errorf("callback bucket %q not found", string(ref.bucket))
				}
				if err := b.Delete([]byte(ref.key)); err != nil {
					return err
				}
				removed++
			}
		}
		return nil
	})
	return removed, err
}

// walkCallbackEntries validates identities and exposes immutable references
// while its caller's bbolt transaction is active. Unknown-but-decodable schema
// is marked protected; malformed data returns an error so the transaction rolls
// back and health can surface the poison barrier.
func walkCallbackEntries(tx *bolt.Tx, visit func(callbackExpiryRef) error) error {
	for _, source := range []struct {
		bucket  []byte
		version callbackStorageVersion
	}{
		{callbackBucketName, callbackStorageLegacy},
		{callbackV2BucketName, callbackStorageV2},
	} {
		b := tx.Bucket(source.bucket)
		if b == nil {
			return fmt.Errorf("callback bucket %q not found", string(source.bucket))
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry CallbackEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return fmt.Errorf("failed to decode callback entry in bucket %q at key %q: %w",
					string(source.bucket), string(k), err)
			}
			if entry.LeaseUUID == "" {
				return fmt.Errorf("callback entry in bucket %q at key %q has empty lease identity",
					string(source.bucket), string(k))
			}
			if source.version == callbackStorageLegacy && entry.LeaseUUID != string(k) {
				return fmt.Errorf("legacy callback lease identity mismatch: key %q contains %q",
					string(k), entry.LeaseUUID)
			}
			if source.version == callbackStorageV2 {
				if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
					return fmt.Errorf("invalid callback delivery identity in bucket %q at key %q: %w",
						string(source.bucket), string(k), err)
				}
				if entry.DeliveryID != string(k) {
					return fmt.Errorf("callback delivery identity mismatch in bucket %q: key %q contains %q",
						string(source.bucket), string(k), entry.DeliveryID)
				}
			}
			if err := visit(callbackExpiryRef{
				bucket:  source.bucket,
				key:     string(k),
				entry:   entry,
				unknown: source.version != callbackStorageV2 || entry.Sequence == 0 || !entry.DeliveryKind.known(),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *CallbackStore) lockDeliveryLease(leaseUUID string) func() {
	return lockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackStore) tryLockDeliveryLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

// Healthy checks both the legacy compatibility queue and the v2 delivery-ID
// queue. The embedded boltStore health check only knows its legacy bucket.
func (s *CallbackStore) Healthy() error {
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(callbackBucketName) == nil {
			return fmt.Errorf("legacy callback bucket missing")
		}
		if tx.Bucket(callbackV2BucketName) == nil {
			return fmt.Errorf("callback v2 bucket missing")
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := s.ListPending(); err != nil {
		return fmt.Errorf("callback queue unhealthy: %w", err)
	}
	return nil
}

func validateCallbackDeliveryID(value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id.String() != value || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return fmt.Errorf("callback delivery ID must be a canonical UUIDv4: %q", value)
	}
	return nil
}

func validateCallbackDeliveryKind(kind CallbackDeliveryKind) error {
	if kind.known() {
		return nil
	}
	return fmt.Errorf("invalid callback delivery kind: %q", kind)
}

func (k CallbackDeliveryKind) known() bool {
	return k == CallbackDeliveryKindOperation || k == CallbackDeliveryKindLifecycle
}

func (e CallbackEntry) sequenced() bool {
	return e.storageVersion == callbackStorageV2 && e.Sequence > 0 && e.DeliveryKind.known()
}
