package shared

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

	// These bucket names belonged to an unshipped v0.14 multi-head design. They
	// are retained only so startup can recognize and reject that unsafe schema;
	// no current code creates, reads, or migrates them.
	callbackOperationIntentBucketName   = []byte("pending_callback_operation_intents")
	callbackMaintenanceIntentBucketName = []byte("pending_callback_maintenance_intents")
	callbackCloseIntentBucketName       = []byte("pending_callback_close_intents")
	callbackClosedLeaseBucketName       = []byte("closed_callback_leases")
)

type callbackStorageVersion uint8

const (
	callbackStorageUnknown callbackStorageVersion = iota
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

	// callbackV2EntryVersion makes each current outbox row self-describing. A
	// reader must reject both older/incomplete rows and future rows rather than
	// silently projecting fields it does not understand into CallbackEntry.
	callbackV2EntryVersion uint8 = 1

	// callbackCleanupLeaseBatchSize is the cooperative work budget between
	// cancellation checks in the asynchronous expiry pass. Each lease remains a
	// separate bbolt transaction and lock domain, so shutdown waits for at most
	// the current short transaction rather than a fleet-sized sweep.
	callbackCleanupLeaseBatchSize = 64
)

var (
	errTerminalLifecyclePending       = errors.New("terminal lifecycle callback already pending")
	errLegacyCallbackOutboxNotDrained = errors.New(
		"pending v0.13 callback rows remain; drain the legacy callback outbox before upgrade",
	)
	// ErrCallbackIntentRequired means a caller tried to enqueue exact causal
	// evidence without atomically transitioning the durable intent that
	// authorized it. Semantic publication must cross CallbackPublisher; the raw
	// journal write is package-private.
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

// CallbackEntry represents a pending callback to be delivered. Status is the
// sole outcome representation: the v2 outbox is isolated from v0.13 readers,
// so persisting a second derived success bit would create contradictory states
// without providing rollback compatibility.
type CallbackEntry struct {
	// DeliveryID identifies this delivery inside its lease's durable v2 queue.
	// Writers allocate a random UUIDv4; precise storage authority is the lease,
	// delivery ID, and value digest together.
	DeliveryID  string `json:"delivery_id,omitempty"`
	LeaseUUID   string `json:"lease_uuid"`
	CallbackURL string `json:"callback_url"`
	// DeliveryKind is present on every runtime row.
	DeliveryKind CallbackDeliveryKind `json:"delivery_kind,omitempty"`
	// Sequence is allocated from bbolt in the same transaction as enqueue and
	// lifecycle coalescing. Every runtime row has a positive sequence encoded in
	// its durable key.
	Sequence uint64                 `json:"sequence,omitempty"`
	Status   backend.CallbackStatus `json:"status,omitempty"`
	Backend  string                 `json:"backend,omitempty"`
	// BackendStorageID is captured when a new callback is enqueued. Replays
	// preserve it exactly instead of restamping a queued observation with the
	// backend's current identity.
	BackendStorageID string `json:"backend_storage_id,omitempty"`
	Error            string `json:"error,omitempty"`
	// Retained persists the best-effort deprovision retain-success flag so a
	// restart-replayed callback keeps it.
	Retained  bool      `json:"retained,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// storageVersion/storageLease/storageDeliveryID/storageKey are populated by
	// durable enqueue/read paths and deliberately excluded from JSON. Together
	// with the digest they let the sender remove exactly the record it delivered.
	storageVersion    callbackStorageVersion
	storageLease      string
	storageDeliveryID string
	storageKey        string
	storageDigest     [sha256.Size]byte
}

// storedV2CallbackEntry is the wire shape for the current outbox. Keep the
// version outside CallbackEntry so unsupported future rows fail closed.
type storedV2CallbackEntry struct {
	Version uint8 `json:"version"`
	CallbackEntry
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
	// replaySubscribers receive lease-identified, coalescing hints after every
	// transaction that appends an outbox row and after an owner releases a drain
	// in a state where an earlier edge could have been consumed. The typed mailbox
	// retains every affected lease while coalescing repeated hints; a fleet-wide
	// edge cannot falsely restart unrelated failed deliveries. The bbolt row
	// remains the level-triggered authority.
	replaySubscribersMu sync.Mutex
	replaySubscribers   map[*callbackReplayMailbox]struct{}
	cleanupInterval     time.Duration
	onCleanupPanic      util.PanicHandler
	cleanupOnce         sync.Once
}

// journalBackendIdentity derives durable lineage from the marker capability
// that opened an authoritative store. The name fallback exists only for the
// deprecated unbound constructor used by isolated compatibility tests; an
// unbound journal can never manufacture storage authority. Production callers
// therefore cannot restamp a bound journal with independently selected fields.
func (s *CallbackStore) journalBackendIdentity(fallbackName string) (string, backendidentity.ID) {
	if s != nil && s.boltStore != nil && s.binding != nil {
		return s.binding.backendName, s.binding.storageID
	}
	return fallbackName, backendidentity.ID{}
}

// CallbackStoreConfig configures the callback store.
type CallbackStoreConfig struct {
	DBPath          string            // Path to bbolt database file
	MaxAge          time.Duration     // Max age for lifecycle observations; exact operation/maintenance completions never expire (0 = no expiry)
	CleanupInterval time.Duration     // How often to run cleanup (defaults to MaxAge)
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic (e.g., bump a metric)
}

// CallbackStoreInspection is read-only schema/evidence used by explicit
// storage-lineage initialization.
type CallbackStoreInspection struct {
	Exists                          bool
	IdentityBound                   bool
	LegacySchema                    bool
	UpgradedSchema                  bool
	Pending                         int
	LeaseMutationUUIDSlots          uint64
	LeaseMutationUUIDSlotLimit      uint64
	CallbackReceiptReservations     uint64
	CallbackReceiptReservationLimit uint64
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
		if err := validateCallbackRootBuckets(tx); err != nil {
			return err
		}
		present := countCallbackSchemaBuckets(tx)
		if present == 0 {
			// A stopped v0.13 backend has none of the current buckets. Its
			// legacy bucket is the complete outbox, so this is expected during
			// adoption.
			return nil
		}
		if present != len(callbackCurrentSchemaBuckets()) {
			return fmt.Errorf("callback journal contains a partial aggregate schema: has %d of %d buckets",
				present, len(callbackCurrentSchemaBuckets()))
		}
		inspection.UpgradedSchema = true
		v2 := tx.Bucket(callbackV2BucketName)
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
		heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if err := heads.ForEach(func(key, value []byte) error {
			if err := budget.observe(key, value); err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
			}
			head, err := decodeLeaseMutationHead(key, value)
			if err != nil {
				return err
			}
			switch state := head.(type) {
			case operationLeaseMutationHead:
				if state.claim.entry.State == operationIntentPending {
					inspection.Pending++
				}
			case maintenanceLeaseMutationHead, closeLeaseMutationHead:
				inspection.Pending++
			case closedLeaseMutationHead:
			default:
				return fmt.Errorf("callback lease mutation head %q decoded as %T", key, head)
			}
			return nil
		}); err != nil {
			return err
		}
		// Slot validation has its own hard global row cap and fixed-size values,
		// so do not double-charge these mirror keys against the generic inspection
		// row budget already charged for their aggregate heads.
		if err := validateLeaseMutationUUIDSlotsTx(tx); err != nil {
			return err
		}
		capacity, err := leaseMutationUUIDCapacityTx(tx)
		if err != nil {
			return err
		}
		inspection.LeaseMutationUUIDSlots = capacity.Reserved
		inspection.LeaseMutationUUIDSlotLimit = capacity.Limit
		operationHistory := tx.Bucket(callbackOperationHistoryBucketName)
		if err := operationHistory.ForEach(func(leaseKey, value []byte) error {
			if err := budget.observe(leaseKey, value); err != nil {
				return err
			}
			if value != nil {
				return fmt.Errorf("completed operation history %q is not a nested bucket", leaseKey)
			}
			leaseBucket := operationHistory.Bucket(leaseKey)
			if leaseBucket == nil {
				return fmt.Errorf("completed operation history %q is unreadable", leaseKey)
			}
			return leaseBucket.ForEach(budget.observe)
		}); err != nil {
			return err
		}
		maintenanceHistory := tx.Bucket(callbackMaintenanceHistoryBucketName)
		if err := maintenanceHistory.ForEach(func(leaseKey, value []byte) error {
			if err := budget.observe(leaseKey, value); err != nil {
				return err
			}
			if value != nil {
				return fmt.Errorf("completed maintenance history %q is not a nested bucket", leaseKey)
			}
			leaseBucket := maintenanceHistory.Bucket(leaseKey)
			if leaseBucket == nil {
				return fmt.Errorf("completed maintenance history %q is unreadable", leaseKey)
			}
			return leaseBucket.ForEach(budget.observe)
		}); err != nil {
			return err
		}
		if err := validateCallbackReceiptStateTx(tx); err != nil {
			return err
		}
		receiptCapacity, err := callbackReceiptCapacityTx(tx)
		if err != nil {
			return err
		}
		inspection.CallbackReceiptReservations = receiptCapacity.Reserved
		inspection.CallbackReceiptReservationLimit = receiptCapacity.Limit
		return nil
	})
	if err != nil {
		return CallbackStoreInspection{}, fmt.Errorf("inspect pending callbacks: %w", err)
	}
	return inspection, nil
}

// OpenIdentityBoundCallbackStore opens an initialized authoritative journal
// without creating its file or binding. It may transactionally add empty,
// backward-compatible schema buckets after verifying the pre-existing core
// schema and storage identity. Cleanup is deferred until StartMaintenance so
// the whole backend store set can open first.
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
	if !storage.Valid() {
		return nil, errors.New("verified backend storage authority is required")
	}
	if gate == nil || !gate.Valid() {
		return nil, errors.New("backend storage authority gate is required")
	}
	storeCfg := boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: callbackBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "callback",
	}
	base, err := openIdentityBoundBoltStore(
		storeCfg, authoritativeStoreCallbacks, storage, gate,
	)
	if err != nil {
		return nil, err
	}
	return finishCallbackStoreOpen(cfg, base, upgradeIdentityBoundCallbackSchema)
}

func finishCallbackStoreOpen(
	cfg CallbackStoreConfig,
	base *boltStore,
	initializeSchema func(*bolt.Tx) error,
) (*CallbackStore, error) {
	if base == nil || initializeSchema == nil {
		return nil, errors.New("callback store base and schema initializer are required")
	}
	s := &CallbackStore{
		boltStore:         base,
		deliveryLocksMu:   &sync.Mutex{},
		deliveryLocks:     make(map[string]*callbackLeaseLock),
		drainLocksMu:      &sync.Mutex{},
		drainLocks:        make(map[string]*callbackLeaseLock),
		replaySubscribers: make(map[*callbackReplayMailbox]struct{}),
		cleanupInterval:   cfg.CleanupInterval,
		onCleanupPanic:    cfg.OnCleanupPanic,
	}
	if schemaErr := s.update(initializeSchema); schemaErr != nil {
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
		s.startCleanupAsync(
			"callback",
			s.cleanupInterval,
			func(maxAge time.Duration) (int, error) {
				return s.removeOlderThanContext(s.ctx, maxAge)
			},
			s.onCleanupPanic,
		)
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
	if err := validateCallbackRootBuckets(tx); err != nil {
		return err
	}
	current := callbackCurrentSchemaBuckets()
	present := countCallbackSchemaBuckets(tx)
	if present != 0 && present != len(current) {
		return errors.New("callback journal contains a partial aggregate schema")
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
		callbackLeaseMutationHeadBucketName,
		callbackLeaseMutationUUIDSlotBucketName,
		callbackOperationHistoryBucketName,
		callbackMaintenanceHistoryBucketName,
	}
}

func callbackUnshippedSchemaBuckets() [][]byte {
	return [][]byte{
		callbackOperationIntentBucketName,
		callbackMaintenanceIntentBucketName,
		callbackCloseIntentBucketName,
		callbackClosedLeaseBucketName,
	}
}

func rejectUnshippedCallbackSchema(tx *bolt.Tx) error {
	for _, bucketName := range callbackUnshippedSchemaBuckets() {
		if tx.Bucket(bucketName) != nil {
			return fmt.Errorf(
				"callback journal contains unsupported unshipped multi-bucket schema %q; restore the drained v0.13 journal",
				bucketName,
			)
		}
	}
	return nil
}

// validateCallbackRootBuckets is the journal-wide downgrade fence. bbolt's
// Tx.ForEach visits only top-level application buckets, so nested per-lease and
// journal buckets remain governed by their owning schema. The allow-list keeps
// the one v0.13 bucket readable, admits the shared storage-identity bucket, and
// makes an older binary refuse a future authoritative root it cannot interpret.
func validateCallbackRootBuckets(tx *bolt.Tx) error {
	if err := rejectUnshippedCallbackSchema(tx); err != nil {
		return err
	}
	allowed := map[string]struct{}{
		string(callbackBucketName):      {},
		string(storeIdentityBucketName): {},
	}
	for _, bucketName := range callbackCurrentSchemaBuckets() {
		allowed[string(bucketName)] = struct{}{}
	}
	return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if _, ok := allowed[string(name)]; ok {
			return nil
		}
		return fmt.Errorf("callback journal contains unsupported top-level bucket %q", name)
	})
}

func countCallbackSchemaBuckets(tx *bolt.Tx) int {
	present := 0
	for _, bucketName := range callbackCurrentSchemaBuckets() {
		if tx.Bucket(bucketName) != nil {
			present++
		}
	}
	return present
}

func upgradeIdentityBoundCallbackSchema(tx *bolt.Tx) error {
	if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
		return err
	}
	if err := validateCallbackRootBuckets(tx); err != nil {
		return err
	}
	present := countCallbackSchemaBuckets(tx)
	if present != 0 && present != len(callbackCurrentSchemaBuckets()) {
		return errors.New("callback journal contains a partial aggregate schema")
	}
	if present == 0 {
		for _, bucketName := range callbackCurrentSchemaBuckets() {
			if _, err := tx.CreateBucket(bucketName); err != nil {
				return fmt.Errorf("create callback aggregate schema bucket %q: %w", bucketName, err)
			}
		}
	}
	return requireCompleteCallbackSchema(tx)
}

func requireCompleteCallbackSchema(tx *bolt.Tx) error {
	if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
		return err
	}
	if err := validateCallbackRootBuckets(tx); err != nil {
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

// store persists an observation-only callback in package-local compatibility
// and corruption tests. Production publication crosses CallbackPublisher.
func (s *CallbackStore) store(entry CallbackEntry) error {
	_, err := s.storeEntry(entry)
	return err
}

// storeEntry persists an observation-only lifecycle callback and returns its
// durable delivery identity and sequence. Lifecycle enqueue atomically
// coalesces older typed, sequenced lifecycle observations for the same lease.
// Exact operation and maintenance completions must instead atomically consume
// their matching intent. The transport removes a successfully delivered row by
// passing this returned value to removeEntry; lease-wide removal would discard
// unrelated exact completions.
func (s *CallbackStore) storeEntry(entry CallbackEntry) (CallbackEntry, error) {
	backendName, storageID := s.journalBackendIdentity(entry.Backend)
	if storageID.Valid() {
		entry.Backend = backendName
		entry.BackendStorageID = storageID.String()
	}
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

// storeEntryLocked is storeEntry's mutation primitive. CallbackPublisher calls
// it only while holding its keyed lease lock. Exact operation and maintenance
// completions reach the transaction primitive through their typed intent
// resolvers instead; no sibling package can manufacture them without their
// write-ahead authority.
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
	s.notifyReplayCommit(entry.LeaseUUID)
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
	data, err := marshalV2CallbackEntry(entry)
	if err != nil {
		return CallbackEntry{}, nil, err
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

// removeEntry deletes exactly one delivered callback in package-local tests.
// Production removal belongs exclusively to CallbackSender's precise drain.
func (s *CallbackStore) removeEntry(entry CallbackEntry) error {
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
	if entry.storageVersion == callbackStorageUnknown {
		return fmt.Errorf("callback entry has no durable storage capability")
	}
	if entry.storageVersion != callbackStorageV2 {
		return fmt.Errorf("unknown callback storage version %d", entry.storageVersion)
	}
	key := entry.storageKey
	if entry.Sequence == 0 || key != string(callbackSequenceKey(entry.Sequence)) {
		return fmt.Errorf("callback sequence %d does not match durable key", entry.Sequence)
	}
	if entry.DeliveryID != entry.storageDeliveryID {
		return fmt.Errorf("callback delivery ID %q does not match durable identity %q",
			entry.DeliveryID, entry.storageDeliveryID)
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
	if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
		return err
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

// listPending returns one lease's durable FIFO from its nested bucket, so this
// path is O(k) in the number of callbacks for the requested lease rather than
// O(N) in the backend outbox. A malformed row blocks only this lease; Healthy
// still walks and reports corruption across the complete store.
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
	slices.SortStableFunc(entries, func(left, right CallbackEntry) int {
		if left.Sequence != right.Sequence {
			return cmp.Compare(left.Sequence, right.Sequence)
		}
		if left.storageLease != right.storageLease {
			return cmp.Compare(left.storageLease, right.storageLease)
		}
		return cmp.Compare(left.storageKey, right.storageKey)
	})
}

// removeOlderThan expires only each lease FIFO's contiguous, expired prefix.
// A fresh head is an ordering barrier even when a later row has an older wall
// clock, so clock rollback cannot delete a delivery from behind live work.
// A typed operation or maintenance completion is also a permanent barrier: it
// may be the only durable evidence capable of settling Fred's write-ahead
// placement or replacement intent, so elapsed wall time can never make it safe
// to discard. Lifecycle observations can expire normally.
// Each lease is locked and transacted independently: a busy or malformed lease
// cannot hold locks for, roll back, or otherwise block cleanup of an unrelated
// lease.
func (s *CallbackStore) removeOlderThan(maxAge time.Duration) (int, error) {
	return s.removeOlderThanContext(context.Background(), maxAge)
}

func (s *CallbackStore) removeOlderThanContext(
	ctx context.Context,
	maxAge time.Duration,
) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	if ctx == nil {
		return 0, errors.New("callback cleanup context is required")
	}
	select {
	case <-ctx.Done():
		return 0, nil
	default:
	}
	leaseUUIDs, discoveryErr := s.callbackLeaseUUIDs()
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	problems := []error{discoveryErr}
	for batchStart := 0; batchStart < len(leaseUUIDs); batchStart += callbackCleanupLeaseBatchSize {
		if ctx.Err() != nil {
			break
		}
		batchEnd := min(batchStart+callbackCleanupLeaseBatchSize, len(leaseUUIDs))
		for _, leaseUUID := range leaseUUIDs[batchStart:batchEnd] {
			if ctx.Err() != nil {
				return removed, errors.Join(problems...)
			}
			unlockDrain, acquired := s.tryLockDrainLease(leaseUUID)
			if !acquired {
				// A wire drainer owns this lease. Skipping one TTL pass is safer
				// than deleting the row whose HTTP outcome is still pending.
				continue
			}
			// Cleanup is optional maintenance, so it must never queue behind a
			// publisher that owns the mutation gate. Release drain ownership and
			// re-publish the level-triggered wake instead; the next pass can retry.
			unlockMutation, mutationAcquired := s.tryLockDeliveryLease(leaseUUID)
			if !mutationAcquired {
				unlockDrain()
				s.notifyReplayHandoff(leaseUUID)
				continue
			}
			leaseRemoved, cleanupErr := runCallbackLeaseCleanup(
				unlockMutation,
				unlockDrain,
				func() (int, error) {
					select {
					case <-ctx.Done():
						return 0, nil
					default:
					}
					return s.removeExpiredLeaseLocked(leaseUUID, cutoff)
				},
				// A callback commit can notify replay while cleanup owns the drain
				// election. Re-publish a conservative edge after both locks are
				// released so the next pass can acquire drain ownership immediately.
				// The handoff also runs on panic.
				func() { s.notifyReplayHandoff(leaseUUID) },
			)
			removed += leaseRemoved
			if cleanupErr != nil {
				problems = append(problems, fmt.Errorf("callback lease %q: %w", leaseUUID, cleanupErr))
			}
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
// decoding callback values. The v2 nested-bucket key identifies the affected
// lease even when its value is corrupt, so replay and TTL can quarantine that
// lease while continuing with unrelated work. Structural errors are returned
// for health/metrics but do not discard discovered leases. A legacy row is a
// journal-wide upgrade-boundary violation, never runtime work.
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

	if err := requireDrainedLegacyCallbackBucket(tx); err != nil {
		problems = append(problems, err)
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
	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		return nil, fmt.Errorf("callback v2 bucket missing")
	}

	leaseKey := []byte(leaseUUID)
	if root.Get(leaseKey) != nil {
		return nil, fmt.Errorf("callback v2 lease %q is not a nested bucket", leaseUUID)
	}
	if leaseBucket := root.Bucket(leaseKey); leaseBucket != nil {
		return readV2CallbackEntries(leaseBucket, leaseUUID)
	}
	return nil, nil
}

func readV2CallbackEntries(leaseBucket *bolt.Bucket, leaseUUID string) ([]CallbackEntry, error) {
	var entries []CallbackEntry
	err := walkV2CallbackEntries(leaseBucket, leaseUUID, func(entry CallbackEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// walkV2CallbackEntries validates one lease FIFO while retaining only the
// current row. Replay supplies a visitor that collects the small per-lease
// queue it needs; health supplies nil and therefore never materializes the
// fleet-wide outbox or its potentially large callback payloads.
func walkV2CallbackEntries(
	leaseBucket *bolt.Bucket,
	leaseUUID string,
	visit func(CallbackEntry) error,
) error {
	cursor := leaseBucket.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		if value == nil {
			return fmt.Errorf("callback v2 lease %q contains nested delivery bucket %q",
				leaseUUID, string(key))
		}
		entry, err := decodeV2CallbackEntry(value)
		if err != nil {
			return fmt.Errorf("failed to decode callback entry for lease %q at key %q: %w",
				leaseUUID, string(key), err)
		}
		if err := validateStoredV2CallbackEntry(entry, leaseUUID); err != nil {
			return fmt.Errorf("invalid callback delivery for lease %q at key %q: %w",
				leaseUUID, string(key), err)
		}
		sequence, err := callbackSequenceFromKey(key)
		if err != nil {
			return fmt.Errorf("invalid callback sequence key for lease %q delivery %q: %w",
				leaseUUID, entry.DeliveryID, err)
		}
		if entry.Sequence != sequence {
			return fmt.Errorf("callback sequence mismatch for lease %q delivery %q: key %d contains %d",
				leaseUUID, entry.DeliveryID, sequence, entry.Sequence)
		}
		entry.storageVersion = callbackStorageV2
		entry.storageLease = leaseUUID
		entry.storageDeliveryID = entry.DeliveryID
		entry.storageKey = string(key)
		entry.storageDigest = sha256.Sum256(value)
		if visit != nil {
			if err := visit(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCallbackQueueTx(tx *bolt.Tx) error {
	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		return fmt.Errorf("callback v2 bucket missing")
	}
	return root.ForEach(func(leaseKey, value []byte) error {
		leaseUUID := string(leaseKey)
		if value != nil {
			return fmt.Errorf("callback v2 lease %q is not a nested bucket", leaseUUID)
		}
		if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
			return fmt.Errorf("callback v2 bucket has invalid lease key: %w", err)
		}
		leaseBucket := root.Bucket(leaseKey)
		if leaseBucket == nil {
			return fmt.Errorf("callback v2 lease %q is unreadable", leaseUUID)
		}
		return walkV2CallbackEntries(leaseBucket, leaseUUID, nil)
	})
}

func marshalV2CallbackEntry(entry CallbackEntry) ([]byte, error) {
	data, err := json.Marshal(storedV2CallbackEntry{
		Version:       callbackV2EntryVersion,
		CallbackEntry: entry,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal callback entry: %w", err)
	}
	return data, nil
}

func decodeV2CallbackEntry(value []byte) (CallbackEntry, error) {
	var stored storedV2CallbackEntry
	if err := decodeStrictAuthoritativeObject(value, maxCallbackEntryBytes, &stored); err != nil {
		return CallbackEntry{}, err
	}
	if stored.Version != callbackV2EntryVersion {
		return CallbackEntry{}, fmt.Errorf("unsupported callback entry version %d", stored.Version)
	}
	return stored.CallbackEntry, nil
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
	if entry.storageVersion != callbackStorageV2 {
		return fmt.Errorf("callback entry has no current durable storage capability")
	}
	key := []byte(entry.storageKey)
	root := tx.Bucket(callbackV2BucketName)
	if root == nil {
		return fmt.Errorf("callback v2 bucket missing")
	}
	leaseKey := []byte(entry.storageLease)
	if root.Get(leaseKey) != nil {
		return fmt.Errorf("callback v2 lease %q is not a nested bucket", entry.storageLease)
	}
	bucket := root.Bucket(leaseKey)
	if bucket == nil {
		return nil
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
	firstKey, _ := bucket.Cursor().First()
	if firstKey == nil {
		return root.DeleteBucket([]byte(entry.storageLease))
	}
	return nil
}

func (s *CallbackStore) lockDeliveryLease(leaseUUID string) func() {
	return lockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackStore) lockDeliveryLeaseContext(
	ctx context.Context,
	leaseUUID string,
) (func(), error) {
	return lockCallbackLeaseContext(
		ctx, s.deliveryLocksMu, s.deliveryLocks, leaseUUID,
	)
}

func (s *CallbackStore) tryLockDeliveryLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackStore) tryLockDrainLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.drainLocksMu, s.drainLocks, leaseUUID)
}

// subscribeReplayWake registers one running replay loop for typed commit and
// drain-handoff notifications. Registration happens before the loop's initial
// drain, which closes the otherwise possible lost-wakeup window between startup
// replay and entering the select loop.
func (s *CallbackStore) subscribeReplayWake(mailbox *callbackReplayMailbox) func() {
	if s == nil || mailbox == nil {
		return func() {}
	}
	s.replaySubscribersMu.Lock()
	s.replaySubscribers[mailbox] = struct{}{}
	s.replaySubscribersMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.replaySubscribersMu.Lock()
			delete(s.replaySubscribers, mailbox)
			s.replaySubscribersMu.Unlock()
		})
	}
}

// notifyReplayCommit publishes the exact lease affected by a durable outbox
// append. A normal commit does not restart a previously failed durable head:
// that head remains an ordering barrier until explicit or periodic retry.
func (s *CallbackStore) notifyReplayCommit(leaseUUID string) {
	s.notifyReplaySubscribers(newCallbackReplayCommitWake(leaseUUID))
}

// notifyReplayHandoff publishes the exact lease whose drain ownership was
// released while work may remain. Unlike a commit, handoff is allowed to wake a
// dormant peer because the earlier delivery failure may have been cancellation
// of the retiring owner rather than refusal by the callback destination.
func (s *CallbackStore) notifyReplayHandoff(leaseUUID string) {
	s.notifyReplaySubscribers(newCallbackReplayHandoffWake(leaseUUID))
}

func (s *CallbackStore) notifyReplaySubscribers(wake callbackReplayWake) {
	if s == nil || !wake.valid() {
		return
	}
	s.replaySubscribersMu.Lock()
	defer s.replaySubscribersMu.Unlock()
	for mailbox := range s.replaySubscribers {
		mailbox.publish(wake)
	}
}

// Healthy checks both queue buckets and validates every durable row. Delivery
// quarantine is per identifiable lease, but any corruption keeps health red so
// an operator cannot miss preserved poison evidence. The embedded boltStore
// health check only knows its legacy bucket and cannot enforce that contract.
func (s *CallbackStore) Healthy() error {
	return s.view(func(tx *bolt.Tx) error {
		if err := requireCompleteCallbackSchema(tx); err != nil {
			return err
		}
		heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if err := heads.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
			}
			_, err := decodeLeaseMutationHead(key, value)
			return err
		}); err != nil {
			return err
		}
		if err := validateLeaseMutationUUIDSlotsTx(tx); err != nil {
			return err
		}
		if err := validateCallbackReceiptStateTx(tx); err != nil {
			return err
		}
		if err := validateCallbackQueueTx(tx); err != nil {
			return fmt.Errorf("callback queue unhealthy: %w", err)
		}
		return nil
	})
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
	default:
		return validateCallbackDeliveryKind(entry.DeliveryKind)
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
