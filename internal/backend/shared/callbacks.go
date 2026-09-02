package shared

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// callbackBucketName is the v0.13 callback queue. Keep it readable and
	// otherwise untouched by new writes so a rollback binary never sees a key
	// it cannot remove (v0.13 deletes entries by lease UUID).
	callbackBucketName = []byte("pending_callbacks")

	// callbackV2BucketName contains one nested bucket per lease, with monotonic
	// big-endian sequence keys inside that lease bucket. Separating the schema
	// from the legacy bucket makes multiple callbacks for one lease safe while
	// keeping rollback binaries from replaying v2 entries forever. The per-lease
	// level is also the corruption and traversal boundary: one malformed delivery
	// can stop only its identifiable lease instead of poisoning the backend outbox.
	callbackV2BucketName = []byte("pending_callbacks_v2")

	// callbackOperationIntentBucketName is the write-ahead journal for accepted
	// asynchronous provision and restore operations. An intent is replaced by
	// its exact operation callback in one bbolt transaction, closing the crash
	// window between a substrate mutation and durable callback enqueue.
	callbackOperationIntentBucketName = []byte("pending_callback_operation_intents")

	// callbackMaintenanceIntentBucketName is the write-ahead journal for
	// restart, update, and custom-domain replacements. Maintenance has its own
	// typed authority because it settles through the lifecycle route and is
	// fenced to an exact source/target release pair, unlike provision/restore.
	callbackMaintenanceIntentBucketName = []byte("pending_callback_maintenance_intents")

	// callbackCloseIntentBucketName is the write-ahead finalizer for accepted
	// deprovisions. It is separate from operation intents because a close owns
	// destructive cleanup and an observation-only lifecycle completion, not an
	// asynchronous provision/restore result.
	callbackCloseIntentBucketName = []byte("pending_callback_close_intents")
)

type callbackStorageVersion uint8

const (
	callbackStorageUnknown callbackStorageVersion = iota
	callbackStorageLegacy
	callbackStorageV2
)

const (
	// callbackCreatedAtFutureSkew admits a modest wall-clock difference for new
	// caller-supplied callback entries. Durable rows are validated independently
	// of the current wall clock: sequence and typed IDs, never time, are their
	// ordering and authority. This distinction keeps an RTC/NTP rollback from
	// quarantining already-accepted work during restart recovery.
	callbackCreatedAtFutureSkew = 5 * time.Minute

	// maxCallbackEntryBytes is the shared write/read ceiling. A writer must never
	// commit a row that replay and health validation would later reject.
	// Callback errors are already intended to be concise; one MiB leaves ample
	// compatibility headroom while keeping health and replay work bounded.
	maxCallbackEntryBytes = 1 << 20
)

var (
	errTerminalLifecyclePending       = errors.New("terminal lifecycle callback already pending")
	errLegacyCallbackOutboxNotDrained = errors.New(
		"pending v0.13 callback rows remain; drain the legacy callback outbox before upgrade",
	)
	// ErrCallbackIntentRequired means a caller tried to enqueue exact causal
	// evidence without atomically consuming the durable intent that authorized
	// it. Only observation-only lifecycle callbacks may use Store or StoreEntry.
	ErrCallbackIntentRequired = errors.New("exact callback requires durable intent settlement")
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

	// CallbackDeliveryKindMaintenance is an exact maintenance-derived fact
	// delivered through the lifecycle callback route. It covers the requested
	// restart/update/custom-domain completion and, when a committed target is
	// already lost, its paired runtime-failure fact. Both are causal barriers,
	// never coalescible observations, so later lifecycle activity cannot erase or
	// overtake either one.
	CallbackDeliveryKindMaintenance CallbackDeliveryKind = "maintenance"

	// CallbackDeliveryKindLifecycle is an observation-only lifecycle event.
	// Enqueueing a newer lifecycle observation atomically replaces older typed
	// lifecycle observations for the lease.
	CallbackDeliveryKindLifecycle CallbackDeliveryKind = "lifecycle"
)

// CallbackEntry represents a pending callback to be delivered.
//
// Success is retained for schema compatibility with entries persisted by
// binaries that predate the Status field and for rollback readers of current
// rows. New writers populate Success AND Status (and Backend). A current sender
// does not deliver a pre-identity v0.13 row; inspection and age cleanup still
// decode its old Success-only shape without rewriting it.
type CallbackEntry struct {
	// DeliveryID identifies this delivery inside its lease's durable v2 queue.
	// Writers allocate a random UUIDv4; precise storage authority is the lease,
	// delivery ID, and value digest together. Legacy v0.13 entries have no ID;
	// current startup requires their separate bucket to be empty.
	DeliveryID  string `json:"delivery_id,omitempty"`
	LeaseUUID   string `json:"lease_uuid"`
	CallbackURL string `json:"callback_url"`
	// DeliveryKind is empty on legacy records. Every v2 row is typed.
	DeliveryKind CallbackDeliveryKind `json:"delivery_kind,omitempty"`
	// Sequence is allocated from bbolt in the same transaction as enqueue and
	// lifecycle coalescing. Legacy records have zero; every v2 row has a positive
	// sequence encoded in its durable key.
	Sequence uint64                 `json:"sequence,omitempty"`
	Success  bool                   `json:"success"`
	Status   backend.CallbackStatus `json:"status,omitempty"`
	Backend  string                 `json:"backend,omitempty"`
	// BackendStorageID is captured when a new callback is enqueued. Replays
	// preserve it exactly instead of restamping a queued observation with the
	// backend's current identity. Only v0.13 rows may omit it; the mandatory
	// stopped upgrade drains those rows, and a current sender refuses to deliver
	// one rather than inventing lineage at replay time.
	BackendStorageID string `json:"backend_storage_id,omitempty"`
	Error            string `json:"error,omitempty"`
	// Retained persists the best-effort deprovision retain-success flag so a
	// restart-replayed callback keeps it. Legacy entries default to false.
	Retained  bool      `json:"retained,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// storageVersion/storageLease/storageDeliveryID/storageKey are populated by
	// StoreEntry and durable reads, and deliberately excluded from JSON. Together
	// with the digest they let RemoveEntry delete exactly the record that was
	// delivered, including a legacy record without DeliveryID.
	storageVersion    callbackStorageVersion
	storageLease      string
	storageDeliveryID string
	storageKey        string
	storageDigest     [sha256.Size]byte
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	*boltStore

	// deliveryLocks are shared with every CallbackSender constructed over this
	// store. They serialize the short journal mutations that allocate FIFO
	// sequence numbers, settle intents, and precisely remove delivered rows.
	// HTTP delivery deliberately does not hold this lock.
	deliveryLocksMu *sync.Mutex
	deliveryLocks   map[string]*callbackLeaseLock
	// drainLocks elect exactly one HTTP drainer per lease across every sender
	// constructed over this store. A drainer retains ownership across HTTP while
	// releasing deliveryLocks between journal mutations, so live settlement can
	// append promptly without permitting duplicate or out-of-order wire sends.
	drainLocksMu *sync.Mutex
	drainLocks   map[string]*callbackLeaseLock
	// replaySubscribers receive a coalescing hint after every transaction that
	// appends an outbox row and after an owner releases a drain in a state where
	// an earlier edge could have been consumed. Keeping these signals at durable
	// commit/ownership boundaries means a direct intent settlement cannot wait
	// for the periodic sweep merely because its caller forgot to notify a
	// CallbackSender. The bbolt row remains the level-triggered authority.
	replaySubscribersMu sync.Mutex
	replaySubscribers   map[chan struct{}]struct{}
	cleanupInterval     time.Duration
	onCleanupPanic      util.PanicHandler
	cleanupOnce         sync.Once
}

// CallbackStoreConfig configures the callback store.
type CallbackStoreConfig struct {
	DBPath          string            // Path to bbolt database file
	MaxAge          time.Duration     // Max age for legacy/lifecycle entries; exact operation/maintenance completions never expire (0 = no expiry)
	CleanupInterval time.Duration     // How often to run cleanup (defaults to MaxAge)
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic (e.g., bump a metric)
}

// CallbackStoreInspection is read-only schema/evidence used by explicit
// storage-lineage initialization.
type CallbackStoreInspection struct {
	Exists         bool
	IdentityBound  bool
	LegacySchema   bool
	UpgradedSchema bool
	Pending        int
}

// InspectCallbackStoreReadOnly inspects durable v0.13 and v2 outbox rows
// without creating a database, bucket, cleanup goroutine, or write
// transaction. Storage-lineage initialization requires zero: an old queued
// callback has no immutable backend storage ID and must be drained by the old
// backend before a new lineage can be sealed.
func InspectCallbackStoreReadOnly(dbPath string) (CallbackStoreInspection, error) {
	return inspectCallbackStoreReadOnlyFile(pathnameAuthoritativeStoreFile(dbPath))
}

// InspectBoundCallbackStoreReadOnly is the descriptor-relative form used by
// storage-lineage initialization after the journal's physical parent has been
// retained.
func InspectBoundCallbackStoreReadOnly(
	path *BoundAuthoritativeStorePath,
) (CallbackStoreInspection, error) {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return CallbackStoreInspection{}, err
	}
	return inspectCallbackStoreReadOnlyFile(file)
}

func inspectCallbackStoreReadOnlyFile(
	file authoritativeStoreFile,
) (CallbackStoreInspection, error) {
	if _, err := file.Lstat(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CallbackStoreInspection{}, nil
		}
		return CallbackStoreInspection{}, fmt.Errorf("stat callback database: %w", err)
	}
	db, _, err := openExistingBoltDBFile(file, true, false)
	if err != nil {
		return CallbackStoreInspection{}, fmt.Errorf("open callback database read-only: %w", err)
	}
	defer func() { _ = db.Close() }()
	inspection := CallbackStoreInspection{Exists: true}
	budget := newStoppedAuthoritativeInspectionBudget()
	err = db.View(func(tx *bolt.Tx) error {
		inspection.IdentityBound = tx.Bucket(storeIdentityBucketName) != nil
		legacy := tx.Bucket(callbackBucketName)
		if legacy == nil {
			return errors.New("legacy callback bucket missing")
		}
		inspection.LegacySchema = true
		if err := legacy.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("legacy callback bucket contains nested bucket %q", key)
			}
			inspection.Pending++
			return nil
		}); err != nil {
			return err
		}
		v2 := tx.Bucket(callbackV2BucketName)
		intents := tx.Bucket(callbackOperationIntentBucketName)
		maintenanceIntents := tx.Bucket(callbackMaintenanceIntentBucketName)
		closeIntents := tx.Bucket(callbackCloseIntentBucketName)
		currentBuckets := []*bolt.Bucket{v2, intents, maintenanceIntents, closeIntents}
		present := 0
		for _, bucket := range currentBuckets {
			if bucket != nil {
				present++
			}
		}
		if present == 0 {
			// A stopped v0.13 backend has none of the current buckets. Its
			// legacy bucket is the complete outbox, so this is expected during
			// adoption.
			return nil
		}
		if present != len(currentBuckets) {
			return fmt.Errorf("callback journal contains a partial upgraded schema (%d of %d buckets)",
				present, len(currentBuckets))
		}
		inspection.UpgradedSchema = true
		if err := v2.ForEach(func(leaseKey, value []byte) error {
			if err := budget.observe(leaseKey, value); err != nil {
				return err
			}
			if value != nil {
				return fmt.Errorf("callback v2 lease %q is not a nested bucket", leaseKey)
			}
			leaseBucket := v2.Bucket(leaseKey)
			if leaseBucket == nil {
				return fmt.Errorf("callback v2 lease %q is unreadable", leaseKey)
			}
			return leaseBucket.ForEach(func(deliveryKey, deliveryValue []byte) error {
				if err := budget.observe(deliveryKey, deliveryValue); err != nil {
					return err
				}
				if deliveryValue == nil {
					return fmt.Errorf("callback v2 lease %q contains nested delivery bucket %q", leaseKey, deliveryKey)
				}
				inspection.Pending++
				return nil
			})
		}); err != nil {
			return err
		}
		if err := intents.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("callback operation intent %q is a nested bucket", key)
			}
			inspection.Pending++
			return nil
		}); err != nil {
			return err
		}
		if err := maintenanceIntents.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("callback maintenance intent %q is a nested bucket", key)
			}
			inspection.Pending++
			return nil
		}); err != nil {
			return err
		}
		return closeIntents.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("callback close intent %q is a nested bucket", key)
			}
			inspection.Pending++
			return nil
		})
	})
	if err != nil {
		return CallbackStoreInspection{}, fmt.Errorf("inspect pending callbacks: %w", err)
	}
	return inspection, nil
}

// NewCallbackStore opens or creates a bbolt database for callback persistence.
// If MaxAge > 0, a background cleanup loop removes expired legacy/lifecycle
// entries periodically and an initial cleanup runs immediately. Exact operation
// completions are permanent causal barriers and never expire by age.
//
// Deprecated: this compatibility-only constructor creates an unbound journal.
// Application composition roots are repository-guarded to use
// OpenIdentityBoundCallbackStore and cannot obtain authority from this value.
func NewCallbackStore(cfg CallbackStoreConfig) (*CallbackStore, error) {
	s, err := newCallbackStore(cfg, backendidentity.VerifiedStorage{}, nil)
	if err != nil {
		return nil, err
	}
	s.StartMaintenance()
	return s, nil
}

// OpenIdentityBoundCallbackStore opens an initialized authoritative journal
// without creating its file or binding. Cleanup is deferred until
// StartMaintenance so the whole backend store set can open first.
func OpenIdentityBoundCallbackStore(
	cfg CallbackStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*CallbackStore, error) {
	if !storage.Valid() {
		return nil, errors.New("verified backend storage authority is required")
	}
	if gate == nil || !gate.Valid() {
		return nil, errors.New("backend storage authority gate is required")
	}
	return newCallbackStore(cfg, storage, gate)
}

func newCallbackStore(
	cfg CallbackStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*CallbackStore, error) {
	storeCfg := boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: callbackBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "callback",
	}
	var base *boltStore
	var err error
	if storage.Valid() {
		base, err = openIdentityBoundBoltStore(storeCfg, authoritativeStoreCallbacks, storage, gate)
	} else {
		base, err = openBoltStore(storeCfg)
	}
	if err != nil {
		return nil, err
	}

	s := &CallbackStore{
		boltStore:         base,
		deliveryLocksMu:   &sync.Mutex{},
		deliveryLocks:     make(map[string]*callbackLeaseLock),
		drainLocksMu:      &sync.Mutex{},
		drainLocks:        make(map[string]*callbackLeaseLock),
		replaySubscribers: make(map[chan struct{}]struct{}),
		cleanupInterval:   cfg.CleanupInterval,
		onCleanupPanic:    cfg.OnCleanupPanic,
	}
	initializeSchema := func(tx *bolt.Tx) error {
		if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
			return err
		}
		for _, bucketName := range callbackCurrentSchemaBuckets() {
			if _, createErr := tx.CreateBucketIfNotExists(bucketName); createErr != nil {
				return createErr
			}
		}
		return nil
	}
	var schemaErr error
	if storage.Valid() {
		schemaErr = s.view(requireCompleteCallbackSchema)
	} else {
		schemaErr = s.update(initializeSchema)
	}
	if schemaErr != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to verify callback journal schema: %w", schemaErr)
	}

	return s, nil
}

// StartMaintenance runs the initial expiry pass and starts the periodic loop
// exactly once.
func (s *CallbackStore) StartMaintenance() {
	if s == nil || s.maxAge <= 0 {
		return
	}
	s.cleanupOnce.Do(func() {
		s.startCleanup("callback", s.cleanupInterval, s.RemoveOlderThan, s.onCleanupPanic)
	})
}

// PrepareBoundCallbackStoreStorage binds a callback journal through a retained
// physical-parent capability and a pending authority minted only after the
// marker-pair anchor is durable.
func PrepareBoundCallbackStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
	profile backendidentity.InitializationProfile,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	allowCreate, err := allowAuthoritativeStoreCreation(profile)
	if err != nil {
		return err
	}
	return initializeIdentityBoundBoltStoreBound(
		path, callbackBucketName, "callback", authoritativeStoreCallbacks, storage.ID(), allowCreate,
		validateCallbackStoreBeforeBinding,
	)
}

func validateCallbackStoreBeforeBinding(tx *bolt.Tx) error {
	legacy := tx.Bucket(callbackBucketName)
	if legacy == nil {
		return errors.New("callback bucket is missing")
	}
	pending := 0
	if err := legacy.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("callback record with key length %d is a nested bucket", len(key))
		}
		pending++
		return nil
	}); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("legacy callback journal is not drained (%d pending)", pending)
	}
	current := callbackCurrentSchemaBuckets()
	present := 0
	for _, bucket := range current {
		if tx.Bucket(bucket) != nil {
			present++
		}
	}
	if present != 0 && present != len(current) {
		return errors.New("callback journal contains a partial upgraded schema")
	}
	if present == 0 {
		for _, bucket := range current {
			if _, err := tx.CreateBucket(bucket); err != nil {
				return fmt.Errorf("create callback journal schema bucket: %w", err)
			}
		}
	}
	for _, bucket := range current {
		if err := requireEmptyBucket(tx.Bucket(bucket)); err != nil {
			return fmt.Errorf("callback journal schema is not empty: %w", err)
		}
	}
	return nil
}

func callbackCurrentSchemaBuckets() [][]byte {
	return [][]byte{
		callbackV2BucketName,
		callbackOperationIntentBucketName,
		callbackMaintenanceIntentBucketName,
		callbackCloseIntentBucketName,
	}
}

func requireCompleteCallbackSchema(tx *bolt.Tx) error {
	if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
		return err
	}
	for _, bucketName := range callbackCurrentSchemaBuckets() {
		if tx.Bucket(bucketName) == nil {
			return fmt.Errorf("required callback journal bucket %q is missing", bucketName)
		}
	}
	return nil
}

func requireEmptyBucket(bucket *bolt.Bucket) error {
	if bucket == nil {
		return errors.New("bucket is missing")
	}
	key, _ := bucket.Cursor().First()
	if key != nil {
		return errors.New("bucket contains durable records")
	}
	return nil
}

// CheckBoundCallbackStoreStorage verifies a prepared callback journal through
// its retained physical-parent capability.
func CheckBoundCallbackStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, callbackBucketName, "callback", authoritativeStoreCallbacks, storage.ID(),
		requireCompleteCallbackSchema,
	)
}

// VerifyBoundCallbackStoreStorage is the capability-bound committed-store
// verifier used before marker finalization.
func VerifyBoundCallbackStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.VerifiedStorage,
) error {
	if !storage.Valid() {
		return errors.New("verified backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, callbackBucketName, "callback", authoritativeStoreCallbacks, storage.ID(),
		requireCompleteCallbackSchema,
	)
}

func VerifyCallbackStoreStorage(dbPath string, storage backendidentity.VerifiedStorage) error {
	return verifyIdentityBoundBoltStore(
		dbPath, callbackBucketName, "callback", authoritativeStoreCallbacks, storage,
		requireCompleteCallbackSchema,
	)
}

// Store persists a callback entry before attempting delivery.
func (s *CallbackStore) Store(entry CallbackEntry) error {
	_, err := s.StoreEntry(entry)
	return err
}

// StoreEntry persists an observation-only lifecycle callback and returns its
// durable delivery identity and sequence. Lifecycle enqueue atomically
// coalesces older typed, sequenced lifecycle observations for the same lease.
// Exact operation and maintenance completions must instead atomically consume
// their matching intent. Callers that remove a successfully delivered callback
// must pass the returned value to RemoveEntry; lease-wide removal would discard
// unrelated exact completions.
func (s *CallbackStore) StoreEntry(entry CallbackEntry) (CallbackEntry, error) {
	if entry.LeaseUUID == "" {
		return CallbackEntry{}, fmt.Errorf("callback lease identity is required")
	}
	if entry.DeliveryKind == CallbackDeliveryKindOperation ||
		entry.DeliveryKind == CallbackDeliveryKindMaintenance {
		return CallbackEntry{}, fmt.Errorf(
			"%w: %s callback", ErrCallbackIntentRequired, entry.DeliveryKind,
		)
	}
	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	return s.storeEntryLocked(entry)
}

// storeEntryLocked is StoreEntry's mutation primitive. A CallbackSender calls
// it only while holding its keyed lease lock. Exact operation and maintenance
// completions reach the transaction primitive through their typed intent
// resolvers instead; the exported store API cannot manufacture them without
// their write-ahead authority.
func (s *CallbackStore) storeEntryLocked(entry CallbackEntry) (CallbackEntry, error) {
	if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
		return CallbackEntry{}, err
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
	err := s.update(func(tx *bolt.Tx) error {
		if entry.DeliveryKind == CallbackDeliveryKindOperation {
			if err := rejectOperationWhileClosingTx(tx, entry.LeaseUUID); err != nil {
				return err
			}
			if err := rejectOperationWhileMaintainingTx(tx, entry.LeaseUUID); err != nil {
				return err
			}
		}
		var putErr error
		entry, data, putErr = putCallbackEntryTx(tx, entry)
		return putErr
	})
	if err != nil {
		return CallbackEntry{}, err
	}

	entry.storageVersion = callbackStorageV2
	entry.storageLease = entry.LeaseUUID
	entry.storageDeliveryID = entry.DeliveryID
	entry.storageKey = string(callbackSequenceKey(entry.Sequence))
	entry.storageDigest = sha256.Sum256(data)
	s.notifyReplaySubscribers()
	return entry, nil
}

// putCallbackEntryTx inserts one validated v2 callback in the caller's write
// transaction. Keeping this load-bearing write in one primitive lets normal
// enqueue and intent settlement share identical sequence/coalescing rules.
func putCallbackEntryTx(tx *bolt.Tx, entry CallbackEntry) (CallbackEntry, []byte, error) {
	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		return CallbackEntry{}, nil, fmt.Errorf("callback v2 bucket missing")
	}

	leaseBucket := root.Bucket([]byte(entry.LeaseUUID))
	var existingEntries []CallbackEntry
	if root.Get([]byte(entry.LeaseUUID)) != nil {
		return CallbackEntry{}, nil, fmt.Errorf("callback v2 lease %q is not a nested bucket", entry.LeaseUUID)
	}
	if leaseBucket != nil {
		var err error
		existingEntries, err = readV2CallbackEntries(leaseBucket, entry.LeaseUUID)
		if err != nil {
			return CallbackEntry{}, nil, err
		}
	} else {
		var err error
		leaseBucket, err = root.CreateBucket([]byte(entry.LeaseUUID))
		if err != nil {
			return CallbackEntry{}, nil, fmt.Errorf("create callback lease bucket %q: %w", entry.LeaseUUID, err)
		}
	}
	for _, existing := range existingEntries {
		if existing.DeliveryID == entry.DeliveryID {
			return CallbackEntry{}, nil, fmt.Errorf("callback delivery ID already exists for lease %q: %s",
				entry.LeaseUUID, entry.DeliveryID)
		}
	}
	if entry.DeliveryKind == CallbackDeliveryKindLifecycle &&
		entry.Status != backend.CallbackStatusDeprovisioned {
		for _, existing := range existingEntries {
			if existing.DeliveryKind == CallbackDeliveryKindLifecycle &&
				existing.Status == backend.CallbackStatusDeprovisioned {
				return CallbackEntry{}, nil, fmt.Errorf("%w for lease %q", errTerminalLifecyclePending, entry.LeaseUUID)
			}
		}
		legacy := tx.Bucket(callbackBucketName)
		if legacy == nil {
			return CallbackEntry{}, nil, fmt.Errorf("legacy callback bucket missing")
		}
		if value := legacy.Get([]byte(entry.LeaseUUID)); value != nil {
			existing, err := decodeLegacyCallbackEntry(entry.LeaseUUID, value)
			if err != nil {
				return CallbackEntry{}, nil, err
			}
			if existing.Status == backend.CallbackStatusDeprovisioned {
				return CallbackEntry{}, nil, fmt.Errorf("%w for lease %q", errTerminalLifecyclePending, entry.LeaseUUID)
			}
		}
	}

	sequence, err := root.NextSequence()
	if err != nil {
		return CallbackEntry{}, nil, fmt.Errorf("failed to allocate callback sequence: %w", err)
	}
	if sequence == 0 {
		return CallbackEntry{}, nil, fmt.Errorf("callback sequence exhausted")
	}
	if len(existingEntries) > 0 && existingEntries[len(existingEntries)-1].Sequence >= sequence {
		return CallbackEntry{}, nil, fmt.Errorf("callback sequence %d does not advance lease %q FIFO after %d",
			sequence, entry.LeaseUUID, existingEntries[len(existingEntries)-1].Sequence)
	}
	entry.Sequence = sequence
	if err := validateStoredV2CallbackEntry(entry, entry.LeaseUUID); err != nil {
		return CallbackEntry{}, nil, err
	}
	sequenceKey := callbackSequenceKey(sequence)
	if leaseBucket.Get(sequenceKey) != nil || leaseBucket.Bucket(sequenceKey) != nil {
		return CallbackEntry{}, nil, fmt.Errorf("callback sequence already exists for lease %q: %d",
			entry.LeaseUUID, sequence)
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return CallbackEntry{}, nil, fmt.Errorf("failed to marshal callback entry: %w", err)
	}
	if len(data) > maxCallbackEntryBytes {
		return CallbackEntry{}, nil, fmt.Errorf("callback entry exceeds %d bytes", maxCallbackEntryBytes)
	}
	if err := leaseBucket.Put(sequenceKey, data); err != nil {
		return CallbackEntry{}, nil, err
	}
	if entry.DeliveryKind == CallbackDeliveryKindLifecycle {
		for _, candidate := range existingEntries {
			if candidate.DeliveryKind != CallbackDeliveryKindLifecycle {
				continue
			}
			if err := leaseBucket.Delete([]byte(candidate.storageKey)); err != nil {
				return CallbackEntry{}, nil, err
			}
		}
	}
	return entry, data, nil
}

// RemoveEntry deletes exactly one delivered callback. The entry must come
// from StoreEntry or ListPending so its durable bucket/key identity is known.
func (s *CallbackStore) RemoveEntry(entry CallbackEntry) error {
	if entry.storageLease == "" {
		return fmt.Errorf("callback entry has no durable lease capability")
	}
	if entry.LeaseUUID != entry.storageLease {
		return fmt.Errorf("callback lease identity %q does not match durable lease %q",
			entry.LeaseUUID, entry.storageLease)
	}
	unlock := s.lockDeliveryLease(entry.storageLease)
	defer unlock()
	return s.removeEntryLocked(entry)
}

func (s *CallbackStore) removeEntryLocked(entry CallbackEntry) error {
	var key string
	switch entry.storageVersion {
	case callbackStorageLegacy:
		key = entry.storageKey
	case callbackStorageV2:
		key = entry.storageKey
		if entry.Sequence == 0 || key != string(callbackSequenceKey(entry.Sequence)) {
			return fmt.Errorf("callback sequence %d does not match durable key", entry.Sequence)
		}
		if entry.DeliveryID != entry.storageDeliveryID {
			return fmt.Errorf("callback delivery ID %q does not match durable identity %q",
				entry.DeliveryID, entry.storageDeliveryID)
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
	if entry.storageLease == "" {
		return fmt.Errorf("callback entry has no durable lease capability")
	}
	if entry.LeaseUUID != entry.storageLease {
		return fmt.Errorf("callback lease identity %q does not match durable lease %q",
			entry.LeaseUUID, entry.storageLease)
	}
	if entry.storageVersion == callbackStorageV2 {
		if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
			return err
		}
	}

	return s.update(func(tx *bolt.Tx) error {
		return removeCallbackEntryTx(tx, entry)
	})
}

// ListPending returns all pending callback entries for durable outbox replay.
func (s *CallbackStore) ListPending() ([]CallbackEntry, error) {
	var entries []CallbackEntry
	err := s.view(func(tx *bolt.Tx) error {
		leaseUUIDs, discoveryErr := callbackLeaseUUIDsTx(tx)
		if discoveryErr != nil {
			return discoveryErr
		}
		for _, leaseUUID := range leaseUUIDs {
			leaseEntries, readErr := listPendingCallbackEntriesTx(tx, leaseUUID)
			if readErr != nil {
				return readErr
			}
			entries = append(entries, leaseEntries...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortCallbackEntries(entries)
	return entries, nil
}

// listPending returns one lease's durable FIFO. The legacy row is a direct
// lookup by lease key and v2 deliveries live in that lease's nested bucket, so
// this path is O(k) in the number of callbacks for the requested lease rather
// than O(N) in the backend outbox. A malformed row blocks only this lease;
// Healthy still walks and reports corruption across the complete store.
func (s *CallbackStore) listPending(leaseUUID string) ([]CallbackEntry, error) {
	if leaseUUID == "" {
		return nil, fmt.Errorf("callback lease identity is required")
	}
	var entries []CallbackEntry
	err := s.view(func(tx *bolt.Tx) error {
		var readErr error
		entries, readErr = listPendingCallbackEntriesTx(tx, leaseUUID)
		return readErr
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func sortCallbackEntries(entries []CallbackEntry) {
	// Protected legacy records sort before all typed sequenced records. This
	// deliberately favors duplicate delivery over allowing a new lifecycle
	// observation to overtake a legacy exact completion.
	slices.SortStableFunc(entries, func(left, right CallbackEntry) int {
		iSequenced := left.sequenced()
		jSequenced := right.sequenced()
		if iSequenced != jSequenced {
			if !iSequenced {
				return -1
			}
			return 1
		}
		if iSequenced {
			if left.Sequence != right.Sequence {
				return cmp.Compare(left.Sequence, right.Sequence)
			}
			return cmp.Compare(left.storageKey, right.storageKey)
		}
		if left.storageVersion != right.storageVersion {
			return cmp.Compare(left.storageVersion, right.storageVersion)
		}
		if left.storageLease != right.storageLease {
			return cmp.Compare(left.storageLease, right.storageLease)
		}
		return cmp.Compare(left.storageKey, right.storageKey)
	})
}

// RemoveOlderThan expires only each lease FIFO's contiguous, expired prefix.
// A fresh head is an ordering barrier even when a later row has an older wall
// clock, so clock rollback cannot delete a delivery from behind live work.
// A typed operation or maintenance completion is also a permanent barrier: it
// may be the only durable evidence capable of settling Fred's write-ahead
// placement or replacement intent, so elapsed wall time can never make it safe
// to discard. Legacy records and lifecycle observations can expire normally.
// Each lease is locked and transacted independently: a busy or malformed lease
// cannot hold locks for, roll back, or otherwise block cleanup of an unrelated
// lease.
func (s *CallbackStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	leaseUUIDs, discoveryErr := s.callbackLeaseUUIDs()
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	problems := []error{discoveryErr}
	for _, leaseUUID := range leaseUUIDs {
		unlockDrain, acquired := s.tryLockDrainLease(leaseUUID)
		if !acquired {
			// A wire drainer owns this lease. Skipping one TTL pass is safer
			// than deleting the row whose HTTP outcome is still pending.
			continue
		}
		// Drain ownership prevents HTTP delivery from starting while cleanup
		// runs. Join the short mutation lock as well so enqueue, settlement,
		// and precise removal cannot race the expiry transaction.
		unlockMutation := s.lockDeliveryLease(leaseUUID)
		leaseRemoved, cleanupErr := runCallbackLeaseCleanup(
			unlockMutation,
			unlockDrain,
			func() (int, error) { return s.removeExpiredLeaseLocked(leaseUUID, cutoff) },
			// A callback commit can notify replay while cleanup owns the drain
			// election and waits for the mutation lock. That replay pass then skips
			// this lease, while cleanup must preserve a fresh/exact row. Re-publish a
			// conservative edge after both locks are released so the next pass can
			// acquire drain ownership immediately. The handoff also runs on panic.
			s.notifyReplaySubscribers,
		)
		removed += leaseRemoved
		if cleanupErr != nil {
			problems = append(problems, fmt.Errorf("callback lease %q: %w", leaseUUID, cleanupErr))
		}
	}
	return removed, errors.Join(problems...)
}

// runCallbackLeaseCleanup makes both per-lease locks panic-safe. Drain
// ownership is released while the mutation lock still excludes enqueue: a
// callback appended immediately after that handoff therefore emits a wake only
// after a new drainer can acquire ownership. The cleanup scheduler recovers a
// panic at its goroutine boundary, so both unlock defers are required here.
// afterUnlock runs last and closes the notification handoff on every exit.
func runCallbackLeaseCleanup(
	unlockMutation func(),
	unlockDrain func(),
	cleanup func() (int, error),
	afterUnlock func(),
) (int, error) {
	if afterUnlock != nil {
		// Registered first so LIFO execution runs this only after both ownership
		// releases, including while a cleanup panic unwinds.
		defer afterUnlock()
	}
	defer unlockMutation()
	defer unlockDrain()
	return cleanup()
}

// callbackLeaseUUIDs discovers lease identities from durable keys without
// decoding callback values. Both the v0.13 row key and the v2 nested-bucket key
// identify the affected lease even when its value is corrupt, so replay and TTL
// can quarantine that lease while continuing with unrelated work. Structural
// errors are returned for health/metrics but do not discard discovered leases.
func (s *CallbackStore) callbackLeaseUUIDs() ([]string, error) {
	var leaseUUIDs []string
	var discoveryErr error
	err := s.view(func(tx *bolt.Tx) error {
		leaseUUIDs, discoveryErr = callbackLeaseUUIDsTx(tx)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leaseUUIDs, discoveryErr
}

func (s *CallbackStore) removeExpiredLeaseLocked(leaseUUID string, cutoff time.Time) (int, error) {
	removed := 0
	txErr := s.update(func(tx *bolt.Tx) error {
		entries, readErr := listPendingCallbackEntriesTx(tx, leaseUUID)
		if readErr != nil {
			return readErr
		}
		sortCallbackEntries(entries)
		for _, entry := range entries {
			if entry.storageVersion == callbackStorageV2 &&
				(entry.DeliveryKind == CallbackDeliveryKindOperation ||
					entry.DeliveryKind == CallbackDeliveryKindMaintenance) {
				break
			}
			if !entry.CreatedAt.Before(cutoff) {
				break
			}
			if deleteErr := removeCallbackEntryTx(tx, entry); deleteErr != nil {
				return deleteErr
			}
			removed++
		}
		return nil
	})
	if txErr != nil {
		return 0, txErr
	}
	return removed, nil
}

func callbackLeaseUUIDsTx(tx *bolt.Tx) ([]string, error) {
	leases := make(map[string]struct{})
	var problems []error

	legacy := tx.Bucket(callbackBucketName)
	if legacy == nil {
		problems = append(problems, fmt.Errorf("legacy callback bucket missing"))
	} else {
		cursor := legacy.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			leaseUUID := string(key)
			if leaseUUID == "" {
				problems = append(problems, fmt.Errorf("legacy callback bucket contains an empty lease key"))
				continue
			}
			leases[leaseUUID] = struct{}{}
			if value == nil {
				problems = append(problems,
					fmt.Errorf("legacy callback lease %q is a nested bucket", leaseUUID))
			}
		}
	}

	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		problems = append(problems, fmt.Errorf("callback v2 bucket missing"))
	} else {
		cursor := root.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			leaseUUID := string(key)
			if leaseUUID == "" {
				problems = append(problems, fmt.Errorf("callback v2 bucket contains an empty lease key"))
				continue
			}
			leases[leaseUUID] = struct{}{}
			if value != nil || root.Bucket(key) == nil {
				problems = append(problems,
					fmt.Errorf("callback v2 lease %q is not a nested bucket", leaseUUID))
			}
		}
	}

	return slices.Sorted(maps.Keys(leases)), errors.Join(problems...)
}

func listPendingCallbackEntriesTx(tx *bolt.Tx, leaseUUID string) ([]CallbackEntry, error) {
	legacy := tx.Bucket(callbackBucketName)
	if legacy == nil {
		return nil, fmt.Errorf("legacy callback bucket missing")
	}
	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		return nil, fmt.Errorf("callback v2 bucket missing")
	}

	var entries []CallbackEntry
	leaseKey := []byte(leaseUUID)
	if legacy.Bucket(leaseKey) != nil {
		return nil, fmt.Errorf("legacy callback lease %q is a nested bucket", leaseUUID)
	}
	if value := legacy.Get(leaseKey); value != nil {
		entry, err := decodeLegacyCallbackEntry(leaseUUID, value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if root.Get(leaseKey) != nil {
		return nil, fmt.Errorf("callback v2 lease %q is not a nested bucket", leaseUUID)
	}
	if leaseBucket := root.Bucket(leaseKey); leaseBucket != nil {
		v2Entries, err := readV2CallbackEntries(leaseBucket, leaseUUID)
		if err != nil {
			return nil, err
		}
		entries = append(entries, v2Entries...)
	}
	return entries, nil
}

func decodeLegacyCallbackEntry(leaseUUID string, value []byte) (CallbackEntry, error) {
	if err := validateUniqueCallbackJSONObject(value); err != nil {
		return CallbackEntry{}, fmt.Errorf("failed to decode callback entry in bucket %q at key %q: %w",
			string(callbackBucketName), leaseUUID, err)
	}
	var entry CallbackEntry
	if err := json.Unmarshal(value, &entry); err != nil {
		return CallbackEntry{}, fmt.Errorf("failed to decode callback entry in bucket %q at key %q: %w",
			string(callbackBucketName), leaseUUID, err)
	}
	if err := validateLegacyCallbackEntry(entry, leaseUUID); err != nil {
		return CallbackEntry{}, fmt.Errorf("invalid legacy callback entry in bucket %q at key %q: %w",
			string(callbackBucketName), leaseUUID, err)
	}
	entry.storageVersion = callbackStorageLegacy
	entry.storageLease = leaseUUID
	entry.storageKey = leaseUUID
	entry.storageDigest = sha256.Sum256(value)
	return entry, nil
}

func readV2CallbackEntries(leaseBucket *bolt.Bucket, leaseUUID string) ([]CallbackEntry, error) {
	var entries []CallbackEntry
	cursor := leaseBucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if value == nil {
			return nil, fmt.Errorf("callback v2 lease %q contains nested delivery bucket %q",
				leaseUUID, string(key))
		}
		if err := validateUniqueCallbackJSONObject(value); err != nil {
			return nil, fmt.Errorf("failed to decode callback entry for lease %q at key %q: %w",
				leaseUUID, string(key), err)
		}
		var entry CallbackEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return nil, fmt.Errorf("failed to decode callback entry for lease %q at key %q: %w",
				leaseUUID, string(key), err)
		}
		if err := validateStoredV2CallbackEntry(entry, leaseUUID); err != nil {
			return nil, fmt.Errorf("invalid callback delivery for lease %q at key %q: %w",
				leaseUUID, string(key), err)
		}
		sequence, err := callbackSequenceFromKey(key)
		if err != nil {
			return nil, fmt.Errorf("invalid callback sequence key for lease %q delivery %q: %w",
				leaseUUID, entry.DeliveryID, err)
		}
		if entry.Sequence != sequence {
			return nil, fmt.Errorf("callback sequence mismatch for lease %q delivery %q: key %d contains %d",
				leaseUUID, entry.DeliveryID, sequence, entry.Sequence)
		}
		entry.storageVersion = callbackStorageV2
		entry.storageLease = leaseUUID
		entry.storageDeliveryID = entry.DeliveryID
		entry.storageKey = string(key)
		entry.storageDigest = sha256.Sum256(value)
		entries = append(entries, entry)
	}
	return entries, nil
}

func callbackSequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func callbackSequenceFromKey(key []byte) (uint64, error) {
	if len(key) != 8 {
		return 0, fmt.Errorf("must be 8 bytes, got %d", len(key))
	}
	sequence := binary.BigEndian.Uint64(key)
	if sequence == 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return sequence, nil
}

func removeCallbackEntryTx(tx *bolt.Tx, entry CallbackEntry) error {
	key := []byte(entry.storageKey)
	var bucket *bolt.Bucket
	switch entry.storageVersion {
	case callbackStorageLegacy:
		bucket = tx.Bucket(callbackBucketName)
		if bucket == nil {
			return fmt.Errorf("legacy callback bucket missing")
		}
	case callbackStorageV2:
		root := tx.Bucket(callbackV2BucketName)
		if root == nil {
			return fmt.Errorf("callback v2 bucket missing")
		}
		leaseKey := []byte(entry.storageLease)
		if root.Get(leaseKey) != nil {
			return fmt.Errorf("callback v2 lease %q is not a nested bucket", entry.storageLease)
		}
		bucket = root.Bucket(leaseKey)
		if bucket == nil {
			return nil
		}
	default:
		return fmt.Errorf("callback entry has no durable storage capability")
	}

	current := bucket.Get(key)
	if current == nil {
		return nil
	}
	if sha256.Sum256(current) != entry.storageDigest {
		return fmt.Errorf("callback entry changed before precise removal")
	}
	if err := bucket.Delete(key); err != nil {
		return err
	}
	if entry.storageVersion == callbackStorageV2 {
		firstKey, _ := bucket.Cursor().First()
		if firstKey == nil {
			return tx.Bucket(callbackV2BucketName).DeleteBucket([]byte(entry.storageLease))
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

func (s *CallbackStore) tryLockDrainLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.drainLocksMu, s.drainLocks, leaseUUID)
}

// subscribeReplayWake registers one running replay loop for commit
// notifications. Registration happens before the loop's initial drain, which
// closes the otherwise possible lost-wakeup window between startup replay and
// entering the select loop.
func (s *CallbackStore) subscribeReplayWake(wake chan struct{}) func() {
	if s == nil || wake == nil {
		return func() {}
	}
	s.replaySubscribersMu.Lock()
	s.replaySubscribers[wake] = struct{}{}
	s.replaySubscribersMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.replaySubscribersMu.Lock()
			delete(s.replaySubscribers, wake)
			s.replaySubscribersMu.Unlock()
		})
	}
}

// notifyReplaySubscribers publishes a best-effort edge after an outbox commit.
// Signals are non-blocking and coalescing because replay always re-lists the
// durable queue; dropping a redundant edge cannot lose work.
func (s *CallbackStore) notifyReplaySubscribers() {
	if s == nil {
		return
	}
	s.replaySubscribersMu.Lock()
	defer s.replaySubscribersMu.Unlock()
	for wake := range s.replaySubscribers {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// Healthy checks both queue buckets and validates every durable row. Delivery
// quarantine is per identifiable lease, but any corruption keeps health red so
// an operator cannot miss preserved poison evidence. The embedded boltStore
// health check only knows its legacy bucket and cannot enforce that contract.
func (s *CallbackStore) Healthy() error {
	if err := s.view(func(tx *bolt.Tx) error {
		if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
			return err
		}
		if tx.Bucket(callbackV2BucketName) == nil {
			return fmt.Errorf("callback v2 bucket missing")
		}
		operations := tx.Bucket(callbackOperationIntentBucketName)
		if operations == nil {
			return fmt.Errorf("callback operation intent bucket missing")
		}
		maintenance := tx.Bucket(callbackMaintenanceIntentBucketName)
		if maintenance == nil {
			return fmt.Errorf("callback maintenance intent bucket missing")
		}
		closes := tx.Bucket(callbackCloseIntentBucketName)
		if closes == nil {
			return fmt.Errorf("callback close intent bucket missing")
		}

		// Check the cross-journal invariant from one bbolt snapshot. Reading the
		// two journals in separate transactions can observe an operation before
		// BeginCloseIntent and its replacement close afterward, falsely reporting
		// an overlap even though the transition itself is atomic.
		if err := operations.ForEach(func(key, value []byte) error {
			if value == nil {
				return nil
			}
			switch {
			case maintenance.Get(key) != nil:
				return fmt.Errorf(
					"callback intent journals unhealthy: lease %q has simultaneous operation and maintenance intents",
					key,
				)
			case closes.Get(key) != nil:
				return fmt.Errorf(
					"callback intent journals unhealthy: lease %q has simultaneous operation and close intents",
					key,
				)
			}
			return nil
		}); err != nil {
			return err
		}
		return maintenance.ForEach(func(key, value []byte) error {
			if value == nil || closes.Get(key) == nil {
				return nil
			}
			return fmt.Errorf(
				"callback intent journals unhealthy: lease %q has simultaneous maintenance and close intents",
				key,
			)
		})
	}); err != nil {
		return err
	}
	if _, err := s.ListPending(); err != nil {
		return fmt.Errorf("callback queue unhealthy: %w", err)
	}
	if _, err := s.ListOperationIntents(); err != nil {
		return fmt.Errorf("callback operation intent journal unhealthy: %w", err)
	}
	if _, err := s.ListMaintenanceIntents(); err != nil {
		return fmt.Errorf("callback maintenance intent journal unhealthy: %w", err)
	}
	if _, err := s.ListCloseIntents(); err != nil {
		return fmt.Errorf("callback close intent journal unhealthy: %w", err)
	}
	return nil
}

// requireDrainedLegacyCallbackBucket enforces the stopped-and-drained v0.13
// upgrade boundary. Legacy rows predate durable backend storage identity, so a
// current process cannot authenticate or attribute them without inventing
// lineage. The read-only inspector remains available before startup; a current
// store opens only after the old outbox is empty.
func requireDrainedLegacyCallbackBucket(tx *bolt.Tx) error {
	legacy := tx.Bucket(callbackBucketName)
	if legacy == nil {
		return fmt.Errorf("legacy callback bucket missing")
	}
	key, _ := legacy.Cursor().First()
	if key != nil {
		return errLegacyCallbackOutboxNotDrained
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

func validateNewCallbackEntry(entry CallbackEntry, now time.Time) error {
	if err := validateCanonicalLeaseUUID(entry.LeaseUUID); err != nil {
		return err
	}
	if err := validateCallbackDeliveryKind(entry.DeliveryKind); err != nil {
		return err
	}
	if entry.Sequence != 0 {
		return fmt.Errorf("callback sequence is store-assigned")
	}
	if entry.DeliveryID != "" {
		if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
			return err
		}
	}
	if err := validateCallbackEntrySemantics(entry); err != nil {
		return err
	}
	return validateNewCallbackCreatedAt(entry.CreatedAt, now)
}

func validateStoredV2CallbackEntry(entry CallbackEntry, leaseUUID string) error {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return fmt.Errorf("invalid durable callback lease key: %w", err)
	}
	if err := validateCanonicalLeaseUUID(entry.LeaseUUID); err != nil {
		return err
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("callback lease identity mismatch: bucket %q contains %q",
			leaseUUID, entry.LeaseUUID)
	}
	if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
		return err
	}
	if err := validateCallbackDeliveryKind(entry.DeliveryKind); err != nil {
		return err
	}
	if entry.Sequence == 0 {
		return fmt.Errorf("callback sequence must be positive")
	}
	return validateCallbackEntrySemantics(entry)
}

// validateLegacyCallbackEntry deliberately does not impose the current UUID or
// typed-route schema while inspecting, cleaning up, or quarantining a v0.13
// row. The supported stopped upgrade requires this bucket to be empty before a
// current store opens. The old writer used the caller's lease key verbatim and
// emitted no v2 identity fields; accepting old URL shapes here does not grant a
// current sender authority to deliver the row.
func validateLegacyCallbackEntry(entry CallbackEntry, leaseUUID string) error {
	if leaseUUID == "" || entry.LeaseUUID == "" {
		return fmt.Errorf("callback lease identity is required")
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("legacy callback lease identity mismatch: key %q contains %q",
			leaseUUID, entry.LeaseUUID)
	}
	if entry.DeliveryID != "" || entry.DeliveryKind != "" || entry.Sequence != 0 {
		return fmt.Errorf("legacy callback contains v2 delivery identity")
	}
	if err := validateCallbackDestination(entry.CallbackURL); err != nil {
		return err
	}
	operationErr := backend.ValidateOperationCallbackURL(entry.CallbackURL)
	lifecycleErr := backend.ValidateLifecycleCallbackURL(entry.CallbackURL)
	if operationErr != nil && lifecycleErr != nil {
		return fmt.Errorf("legacy callback URL has invalid authority: %w",
			errors.Join(operationErr, lifecycleErr))
	}
	if entry.Status == "" {
		if entry.Retained {
			return fmt.Errorf("legacy pre-status callback cannot be retained")
		}
	} else if err := validateCallbackStatus(entry); err != nil {
		return err
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func validateCallbackEntrySemantics(entry CallbackEntry) error {
	if entry.BackendStorageID == "" {
		return fmt.Errorf("callback backend storage identity is required")
	}
	if _, err := backendidentity.Parse(entry.BackendStorageID); err != nil {
		return fmt.Errorf("invalid callback backend storage identity: %w", err)
	}
	if err := validateCallbackDestination(entry.CallbackURL); err != nil {
		return err
	}
	// DeliveryKind is the backend's causal ordering intent. A stopped upgrade
	// can preserve a tokenless callback URL already embedded in a migrated v0.13
	// workload; a current backend may later enqueue an identity-bound v2
	// lifecycle observation for it. The URL validators therefore allow no
	// selector for that workload compatibility, but reject malformed, duplicate,
	// mixed, or opposite-class selectors. No pending pre-identity callback row
	// itself crosses the documented cutover.
	switch entry.DeliveryKind {
	case CallbackDeliveryKindOperation:
		if err := backend.ValidateOperationCallbackURL(entry.CallbackURL); err != nil {
			return fmt.Errorf("operation delivery has invalid callback URL: %w", err)
		}
	case CallbackDeliveryKindMaintenance:
		if err := backend.ValidateLifecycleCallbackURL(entry.CallbackURL); err != nil {
			return fmt.Errorf("maintenance delivery has invalid callback URL: %w", err)
		}
	case CallbackDeliveryKindLifecycle:
		if err := backend.ValidateLifecycleCallbackURL(entry.CallbackURL); err != nil {
			return fmt.Errorf("lifecycle delivery has invalid callback URL: %w", err)
		}
	default:
		return validateCallbackDeliveryKind(entry.DeliveryKind)
	}
	if err := validateCallbackStatus(entry); err != nil {
		return err
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func validateCallbackStatus(entry CallbackEntry) error {
	switch entry.DeliveryKind {
	case CallbackDeliveryKindOperation:
		if entry.Status != backend.CallbackStatusSuccess && entry.Status != backend.CallbackStatusFailed {
			return fmt.Errorf("operation callback has invalid status %q", entry.Status)
		}
		if entry.Retained {
			return fmt.Errorf("operation callback cannot be retained")
		}
	case CallbackDeliveryKindMaintenance:
		if entry.Status != backend.CallbackStatusSuccess && entry.Status != backend.CallbackStatusFailed {
			return fmt.Errorf("maintenance callback has invalid status %q", entry.Status)
		}
		if entry.Retained {
			return fmt.Errorf("maintenance callback cannot be retained")
		}
	case CallbackDeliveryKindLifecycle:
		if entry.Status != backend.CallbackStatusSuccess &&
			entry.Status != backend.CallbackStatusFailed &&
			entry.Status != backend.CallbackStatusDeprovisioned {
			return fmt.Errorf("lifecycle callback has invalid status %q", entry.Status)
		}
		if entry.Retained && entry.Status != backend.CallbackStatusDeprovisioned {
			return fmt.Errorf("retained flag requires deprovisioned lifecycle status")
		}
	case "":
		// A status-bearing v0.13 row has no delivery kind. It may contain any
		// status that the old sender emitted, but retained remains terminal-only.
		if entry.Status != backend.CallbackStatusSuccess &&
			entry.Status != backend.CallbackStatusFailed &&
			entry.Status != backend.CallbackStatusDeprovisioned {
			return fmt.Errorf("legacy callback has invalid status %q", entry.Status)
		}
		if entry.Retained && entry.Status != backend.CallbackStatusDeprovisioned {
			return fmt.Errorf("retained flag requires deprovisioned legacy status")
		}
	default:
		return validateCallbackDeliveryKind(entry.DeliveryKind)
	}
	wantSuccess := entry.Status != backend.CallbackStatusFailed
	if entry.Success != wantSuccess {
		return fmt.Errorf("callback success flag %t conflicts with status %q", entry.Success, entry.Status)
	}
	return nil
}

func validateCanonicalLeaseUUID(value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != value {
		return fmt.Errorf("callback lease identity must be a canonical non-nil UUID: %q", value)
	}
	return nil
}

func validateStoredCallbackCreatedAt(createdAt time.Time) error {
	if createdAt.IsZero() || createdAt.Before(time.Unix(0, 0)) {
		return fmt.Errorf("callback created_at must be on or after the Unix epoch")
	}
	return nil
}

func validateNewCallbackCreatedAt(createdAt, now time.Time) error {
	if err := validateStoredCallbackCreatedAt(createdAt); err != nil {
		return err
	}
	if createdAt.After(now.Add(callbackCreatedAtFutureSkew)) {
		return fmt.Errorf("callback created_at exceeds the %s future clock-skew allowance",
			callbackCreatedAtFutureSkew)
	}
	return nil
}

func validateCallbackDestination(callbackURL string) error {
	endpoint, err := callbackurl.ParseEndpoint(callbackURL)
	if err != nil {
		return fmt.Errorf("invalid callback destination: %w", err)
	}
	hostname := strings.TrimSuffix(endpoint.Hostname(), ".")
	if address, _, found := strings.Cut(hostname, "%"); found {
		hostname = address
	}
	if ip := net.ParseIP(hostname); ip != nil &&
		!ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() {
		return fmt.Errorf("callback destination IP is not a routable unicast address")
	}
	return nil
}

// validateUniqueCallbackJSONObject rejects ambiguous duplicate field names at
// every object depth before encoding/json can silently apply last-value-wins
// semantics. Unknown fields remain accepted for forward-compatible readers.
func validateUniqueCallbackJSONObject(value []byte) error {
	return validateUniqueJSONObject(value, maxCallbackEntryBytes)
}

func validateUniqueJSONObject(value []byte, maxBytes int) error {
	if len(value) > maxBytes {
		return fmt.Errorf("JSON entry exceeds %d bytes", maxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return fmt.Errorf("expected JSON object")
	}
	if err := validateUniqueJSONObjectBody(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected data after JSON object")
	}
	return nil
}

// validateUniqueJSONObjectBody consumes an object after its opening brace.
// Values are walked recursively so duplicate authoritative fields nested in
// arrays (for example items[0].sku) cannot be accepted by last-value-wins JSON
// decoding. Unknown values are validated structurally but otherwise ignored.
func validateUniqueJSONObjectBody(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := nameToken.(string)
		if !ok {
			return fmt.Errorf("object field name is not a string")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate field %q", name)
		}
		seen[name] = struct{}{}
		if err := validateUniqueJSONValue(decoder); err != nil {
			return fmt.Errorf("decode field %q: %w", name, err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return fmt.Errorf("unterminated JSON object")
	}
	return nil
}

func validateUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		return validateUniqueJSONObjectBody(decoder)
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := validateUniqueJSONValue(decoder); err != nil {
				return fmt.Errorf("decode array element %d: %w", index, err)
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closeDelimiter, ok := closing.(json.Delim); !ok || closeDelimiter != ']' {
			return fmt.Errorf("unterminated JSON array")
		}
		return nil
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func validateCallbackDeliveryKind(kind CallbackDeliveryKind) error {
	if kind.known() {
		return nil
	}
	return fmt.Errorf("invalid callback delivery kind: %q", kind)
}

func (k CallbackDeliveryKind) known() bool {
	return k == CallbackDeliveryKindOperation ||
		k == CallbackDeliveryKindMaintenance ||
		k == CallbackDeliveryKindLifecycle
}

func (e CallbackEntry) sequenced() bool {
	return e.storageVersion == callbackStorageV2 && e.Sequence > 0 && e.DeliveryKind.known()
}
