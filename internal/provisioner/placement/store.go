package placement

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var bucketName = []byte("placements")

// recordIssuerSequence provides process-local identities for opaque record
// capabilities. RecordRevision is never serialized, so reopening a Store
// deliberately mints a different issuer.
var recordIssuerSequence atomic.Uint64

// State describes what is durably known about one lease placement.
type State uint8

const (
	// StateAbsent means no record exists for the lease.
	StateAbsent State = iota
	// StateAttempting means a backend call may have happened but has not yet
	// been confirmed or authoritatively disproved.
	StateAttempting
	// StateConfirmed means Backend is the last positively observed owner. A
	// confirmed placement may also carry an unresolved Attempt.
	StateConfirmed
	// StateUnusable means a record exists but cannot safely be interpreted.
	StateUnusable
)

// String returns the stable log representation of a placement state.
func (s State) String() string {
	switch s {
	case StateAbsent:
		return "absent"
	case StateAttempting:
		return "attempting"
	case StateConfirmed:
		return "confirmed"
	case StateUnusable:
		return "unusable"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

var (
	// ErrInvalidPlacement means a required lease or backend identifier is empty.
	ErrInvalidPlacement = errors.New("invalid placement")
	// ErrAttemptConflict means an unresolved attempt already exists. Retrying
	// even the same target is unsafe because the first backend outcome is unknown.
	ErrAttemptConflict = errors.New("placement attempt already exists")
	// ErrBackendConflict means an operation targeted a backend other than the
	// lease's confirmed owner.
	ErrBackendConflict = errors.New("placement backend conflicts with target")
	// ErrUnusablePlacement means an on-disk record exists but cannot safely be
	// used as either affinity or an attempt gate.
	ErrUnusablePlacement = errors.New("placement record is unusable")
	// ErrAttemptMismatch means an attempt settlement did not name the current
	// unresolved attempt.
	ErrAttemptMismatch = errors.New("placement attempt does not match target")
	// ErrInvalidAttemptToken means an attempt settlement capability was not
	// issued by this store or does not contain all required placement evidence.
	ErrInvalidAttemptToken = errors.New("invalid placement attempt token")
	// ErrInvalidInventoryFence means a typed inventory fence was not issued by
	// this store or was invalidated after it was issued.
	ErrInvalidInventoryFence = errors.New("invalid placement inventory fence")
	// ErrInvalidRecordRevision means a typed record mutation was attempted with
	// the invalid zero revision, including an unupgraded legacy record.
	ErrInvalidRecordRevision = errors.New("invalid placement record revision")
	// ErrInvalidRestoreClaim means a restore settlement capability was not
	// issued by this store or is missing facts needed for exact settlement.
	ErrInvalidRestoreClaim = errors.New("invalid placement restore claim")
	// ErrRestoreSourceNotFound means no durable placement identifies the
	// retained source requested by a restore.
	ErrRestoreSourceNotFound = errors.New("restore source placement not found")
	// ErrRestoreSourceUnavailable means the requested source is not one
	// unambiguous, confirmed, revisioned placement with no unresolved attempt.
	ErrRestoreSourceUnavailable = errors.New("restore source placement is unavailable")
	// ErrRestoreSourceClaimed means another synchronous restore dispatch already
	// holds the exclusive process-local claim for this source.
	ErrRestoreSourceClaimed = errors.New("restore source placement is claimed")
	// ErrRestoreTargetUnavailable means the target already has durable placement
	// evidence. Restore admission requires a truly absent target and never
	// adopts, retries, or overwrites an existing placement.
	ErrRestoreTargetUnavailable = errors.New("restore target placement is unavailable")
)

// RecordRevision is the opaque identity of one durable placement record
// version. Its private fields bind the capability to the issuing store, lease,
// and exact revision. The zero value is invalid. It deliberately has no
// numeric or lease accessor: callers can only present it back to its issuer.
type RecordRevision struct {
	issuer    uint64
	leaseUUID string
	value     uint64
}

// Valid reports whether the revision identifies a durably written record.
func (revision RecordRevision) Valid() bool {
	return revision.issuer != 0 && revision.leaseUUID != "" && revision.value != 0
}

func (s *Store) newRecordRevision(leaseUUID string, value uint64) RecordRevision {
	if s == nil || s.recordIssuer == 0 || leaseUUID == "" || value == 0 {
		return RecordRevision{}
	}
	return RecordRevision{issuer: s.recordIssuer, leaseUUID: leaseUUID, value: value}
}

// InventoryFence is a causal placement-store boundary. Unlike RecordRevision,
// an explicitly issued fence may represent revision zero. The private issuer
// and authority epoch prevent mixing stores or reusing evidence after a newer
// inventory session supersedes the collection that minted it.
type InventoryFence struct {
	issuer   *Store
	revision uint64
	epoch    uint64
}

// Valid reports whether the fence was explicitly issued by a placement store.
// A store still revalidates the issuer and authority epoch when consuming it.
func (fence InventoryFence) Valid() bool {
	return fence.issuer != nil && fence.epoch != 0
}

// InventoryProjection is one fleet observation to be applied at a single
// causal boundary. Complete must be true only when every backend in the
// configured topology was authoritatively observed. A complete projection
// establishes the durable admission baseline in the same transaction as its
// placement mutations. Placements contains unique positive owners. A positive
// observation from the exact attempted backend may confirm that attempt.
// Conflicts contains leases reported by at least two backends. Inventory
// silence is deliberately not representable: it cannot clear an attempt or a
// durable conflict because an earlier backend request may commit after the
// inventory response.
//
// ProjectInventory defensively copies all maps and slices before use.
type InventoryProjection struct {
	Complete   bool
	Placements map[string]string
	Conflicts  map[string][]string
}

// ProjectionResult reports leases left unchanged because their durable
// evidence was newer than the input inventory or was source-claimed by a
// restore.
type ProjectionResult struct {
	// Fenced contains the lease UUIDs whose submitted observations were not
	// applied at this inventory boundary.
	Fenced map[string]struct{}
}

// AttemptToken is the exclusive capability for settling one durable
// write-ahead attempt. Its private fields bind the settlement to the issuing
// store, lease, backend, operation, and exact record revision. The zero value
// is invalid.
type AttemptToken struct {
	issuer      *Store
	leaseUUID   string
	backendName string
	operationID operation.OperationID
	revision    RecordRevision
}

// Valid reports whether the token contains every fact required to identify a
// typed write-ahead attempt. The issuing store still performs an exact CAS.
func (token AttemptToken) Valid() bool {
	return token.issuer != nil && token.leaseUUID != "" && token.backendName != "" &&
		token.operationID.Valid() && token.revision.Valid() &&
		token.revision.issuer == token.issuer.recordIssuer &&
		token.revision.leaseUUID == token.leaseUUID
}

// RestoreClaim is the exclusive capability for settling one synchronous
// restore dispatch. Its private fields bind it to the issuing store, exact
// source record, target attempt, backend, operation, and process-local nonce.
// The zero value is invalid.
//
// The source claim is intentionally process-local and short-lived: it fences
// source mutation only until the backend synchronously accepts, refuses, or
// returns an ambiguous outcome. The target attempt remains durable across
// process restarts and is the authority after dispatch returns.
type RestoreClaim struct {
	issuer          *Store
	sourceLeaseUUID string
	targetLeaseUUID string
	backendName     string
	operationID     operation.OperationID
	sourceRevision  RecordRevision
	targetRevision  RecordRevision
	nonce           uint64
}

// Valid reports whether the claim carries every fact required to identify an
// exact restore dispatch. The issuing store still checks its live reservation
// before consuming the claim.
func (claim RestoreClaim) Valid() bool {
	return claim.issuer != nil && claim.sourceLeaseUUID != "" &&
		claim.targetLeaseUUID != "" && claim.sourceLeaseUUID != claim.targetLeaseUUID &&
		claim.backendName != "" && claim.operationID.Valid() &&
		claim.sourceRevision.Valid() && claim.targetRevision.Valid() && claim.nonce != 0 &&
		claim.sourceRevision.issuer == claim.issuer.recordIssuer &&
		claim.sourceRevision.leaseUUID == claim.sourceLeaseUUID &&
		claim.targetRevision.issuer == claim.issuer.recordIssuer &&
		claim.targetRevision.leaseUUID == claim.targetLeaseUUID
}

// Backend returns the confirmed source owner bound to the claim. An invalid
// claim returns an empty backend.
func (claim RestoreClaim) Backend() string {
	if !claim.Valid() {
		return ""
	}
	return claim.backendName
}

// Placement is an immutable snapshot of one cached placement record. Backend
// is the last positively observed owner. Attempt is a write-ahead record of a
// backend call whose outcome is not yet known. SetAt is the record's first-seen
// time and gates orphan pruning.
//
// State and revision deliberately remain opaque. Callers use State and
// Revision to make decisions without being able to mutate store-owned metadata.
type Placement struct {
	Backend string
	Attempt string
	SetAt   time.Time
	// Conflict is a durable quarantine marker: more than one backend reported
	// positive ownership, so no individual backend/status is authoritative.
	Conflict bool
	// ConflictBackends is the sorted durable set of backends that have been
	// positively identified as possible owners while Conflict is true. Keeping
	// the names is essential across restart/reconfiguration: a backend removed
	// from the current router must remain an unresolved deprovision candidate,
	// rather than disappearing from the evidence simply because it is offline.
	ConflictBackends []string
	// ConflictOwnersUnknown marks a legacy or malformed conflict whose complete
	// candidate set was not persisted. Such a record remains fail-closed until an
	// operator resolves it; a complete view of only the current configuration
	// cannot prove that a former owner is gone.
	ConflictOwnersUnknown bool

	unusable bool
	revision uint64
	// recordRevision is attached only to immutable snapshots returned by a Store.
	// It is neither persisted nor trusted independently by another store.
	recordRevision RecordRevision
	// attemptOperationID is present only for attempts begun through the typed
	// API. Legacy records remain readable with an invalid operation identity.
	attemptOperationID operation.OperationID
}

// State returns the placement's derived state. The zero Placement is Absent.
func (p Placement) State() State {
	if p.unusable || p.Conflict {
		return StateUnusable
	}
	if p.Backend != "" {
		return StateConfirmed
	}
	if p.Attempt != "" {
		return StateAttempting
	}
	// Only the completely empty public snapshot is Absent. A timestamp without
	// either placement fact is structurally present but cannot be interpreted.
	if !p.SetAt.IsZero() || p.revision != 0 {
		return StateUnusable
	}
	return StateAbsent
}

// Revision returns the opaque per-record revision used by conditional writes.
func (p Placement) Revision() uint64 { return p.revision }

// RecordRevision returns the opaque identity of this exact durable snapshot.
// The zero Placement and legacy unrevisioned records return an invalid value.
func (p Placement) RecordRevision() RecordRevision {
	if !p.recordRevision.Valid() || p.recordRevision.value != p.revision {
		return RecordRevision{}
	}
	return p.recordRevision
}

// AttemptOperationID returns the operation identity associated with the
// unresolved attempt. Legacy attempts return an invalid ID.
func (p Placement) AttemptOperationID() operation.OperationID {
	if p.Attempt == "" {
		return operation.OperationID{}
	}
	return p.attemptOperationID
}

// record is the version-tolerant bbolt representation. The backend and set_at
// field names retain compatibility with the ENG-335 JSON format. Before that
// change, values were raw backend names; decodeRecord still accepts them.
type record struct {
	Backend               string    `json:"backend"`
	Attempt               string    `json:"attempt,omitempty"`
	OperationID           string    `json:"operation_id,omitempty"`
	SetAt                 time.Time `json:"set_at"`
	Revision              uint64    `json:"revision,omitempty"`
	Conflict              bool      `json:"conflict,omitempty"`
	ConflictBackends      []string  `json:"conflict_backends,omitempty"`
	ConflictOwnersUnknown bool      `json:"conflict_owners_unknown,omitempty"`
}

// Store is a bbolt-backed placement store with an in-memory read cache. All
// writes commit to bbolt before the cache or revision clock is changed.
type Store struct {
	db             *bolt.DB
	cache          map[string]Placement
	lifecycleCache map[string]lifecycleCapability
	// deleteRevisions fences stale inventory from recreating an exact key that
	// was deleted after its snapshot began. Entries exist only while at least one
	// registered inventory snapshot could still need them, so unrelated keys do
	// not share a global fence and tombstones do not grow for the process lifetime.
	deleteRevisions map[string]uint64
	// activeSnapshots counts registered inventory cutoffs. More than one caller
	// may begin at the same revision, so a refcount is required before tombstones
	// at that cutoff can be pruned.
	activeSnapshots map[uint64]uint64
	now             func() time.Time
	revision        uint64
	authorityEpoch  uint64
	// Backend topology and its admission baseline are loaded from the metadata
	// bucket. The baseline remains durable across process restart, but is usable
	// only while it exactly matches the current topology identity.
	backendTopology     []string
	backendTopologySet  map[string]struct{}
	knownBackendNames   map[string]struct{}
	topologyFingerprint string
	topologyID          uint64
	baselineFingerprint string
	baselineTopologyID  uint64
	// restoreClaims are process-local source reservations held only across a
	// synchronous backend Restore call. The durable target attempt is the
	// authority after dispatch returns or this process restarts.
	restoreClaims map[string]RestoreClaim
	restoreNonce  uint64
	recordIssuer  uint64
	mu            sync.RWMutex
	closeOnce     sync.Once
	closeErr      error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithClock injects the clock used to stamp SetAt. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// NewStore opens or creates a bbolt database, atomically migrates unambiguous
// revision-zero confirmed owners, and loads all placement records into memory.
// Corrupt records remain present as StateUnusable so a missing placement can
// never be inferred from unreadable durable state.
func NewStore(dbPath string, opts ...Option) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open placement db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		initializationEpoch, err := captureLifecycleInitializationEpoch(tx)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return err
		}
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		if err := initializeLifecycleCapabilities(tx, initializationEpoch); err != nil {
			return err
		}
		if err := migrateLegacyConfirmedRevisions(tx); err != nil {
			return err
		}
		return pruneDetachedRetiredLifecycleCapabilities(tx)
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize placement store: %w", err)
	}

	cache := make(map[string]Placement)
	var lifecycleCache map[string]lifecycleCapability
	var revision uint64
	var metadata topologyMetadata
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("placements bucket missing")
		}
		if err := b.ForEach(func(k, v []byte) error {
			p := decodeRecord(string(k), v)
			cache[string(k)] = p
			if p.revision > revision {
				revision = p.revision
			}
			return nil
		}); err != nil {
			return err
		}
		var err error
		lifecycleCache, err = loadLifecycleCapabilities(tx)
		if err != nil {
			return err
		}
		quarantineLifecycleBindings(cache, lifecycleCache)
		metadata, err = loadTopologyMetadata(tx)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placement store: %w", err)
	}

	recordIssuer := recordIssuerSequence.Add(1)
	if recordIssuer == 0 {
		_ = db.Close()
		return nil, errors.New("placement record capability issuer exhausted")
	}
	s := &Store{
		db:              db,
		cache:           cache,
		lifecycleCache:  lifecycleCache,
		deleteRevisions: make(map[string]uint64),
		activeSnapshots: make(map[uint64]uint64),
		now:             time.Now,
		revision:        revision,
		authorityEpoch:  1,
		backendTopology: slices.Clone(metadata.Topology),
		backendTopologySet: func() map[string]struct{} {
			set := make(map[string]struct{}, len(metadata.Topology))
			for _, backendName := range metadata.Topology {
				set[backendName] = struct{}{}
			}
			return set
		}(),
		knownBackendNames: func() map[string]struct{} {
			set := make(map[string]struct{}, len(metadata.KnownBackends))
			for _, backendName := range metadata.KnownBackends {
				set[backendName] = struct{}{}
			}
			return set
		}(),
		topologyFingerprint: metadata.TopologyFingerprint,
		topologyID:          metadata.TopologyID,
		baselineFingerprint: metadata.BaselineFingerprint,
		baselineTopologyID:  metadata.BaselineTopologyID,
		restoreClaims:       make(map[string]RestoreClaim),
		recordIssuer:        recordIssuer,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// migrateLegacyConfirmedRevisions upgrades the unambiguous confirmed-owner
// records written by v0.13 and earlier onto the revisioned schema. All records
// are examined before any write so new revisions start strictly above every
// existing durable revision. The caller's bbolt update transaction makes the
// entire migration atomic, including revision exhaustion or a failed Put.
//
// Attempts, conflicts, and unusable records deliberately remain byte-for-byte
// untouched. None of them is safe to turn into an exact confirmed-owner
// capability merely because it predates record revisions.
func migrateLegacyConfirmedRevisions(tx *bolt.Tx) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return errors.New("placements bucket missing")
	}

	type migration struct {
		leaseUUID string
		placement Placement
	}
	var migrations []migration
	var revision uint64
	if err := b.ForEach(func(k, v []byte) error {
		leaseUUID := string(k)
		p := decodeRecord(leaseUUID, v)
		if p.revision > revision {
			revision = p.revision
		}
		if leaseUUID != "" && p.revision == 0 && p.State() == StateConfirmed &&
			p.Attempt == "" && !p.Conflict {
			migrations = append(migrations, migration{
				leaseUUID: leaseUUID,
				placement: p,
			})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan legacy placement revisions: %w", err)
	}

	for _, migration := range migrations {
		if revision == math.MaxUint64 {
			return errors.New("placement revision exhausted during legacy migration")
		}
		revision++
		migration.placement.revision = revision
		encoded, err := encodePlacement(migration.placement)
		if err != nil {
			return fmt.Errorf("encode migrated placement %q: %w", migration.leaseUUID, err)
		}
		if err := b.Put([]byte(migration.leaseUUID), encoded); err != nil {
			return fmt.Errorf("write migrated placement %q: %w", migration.leaseUUID, err)
		}
	}
	return nil
}

// decodeRecord parses the current JSON representation or the pre-ENG-335 raw
// backend-name representation. An empty raw value, malformed JSON object, or
// JSON object with neither Backend nor Attempt remains present but Unusable.
func decodeRecord(leaseUUID string, v []byte) Placement {
	if len(v) > 0 && v[0] != '{' {
		if !validLegacyBackendName(v) {
			return unusableRecord(leaseUUID, errors.New("legacy backend name is not printable UTF-8"))
		}
		return Placement{Backend: string(v)}
	}

	var r record
	if len(v) == 0 {
		return unusableRecord(leaseUUID, errors.New("empty value"))
	}
	if err := json.Unmarshal(v, &r); err != nil {
		return unusableRecord(leaseUUID, err)
	}

	operationID, operationErr := decodeOperationID(r.OperationID)
	p := Placement{
		Backend:               r.Backend,
		Attempt:               r.Attempt,
		SetAt:                 r.SetAt,
		Conflict:              r.Conflict,
		ConflictBackends:      normalizeBackendNames(r.ConflictBackends),
		ConflictOwnersUnknown: r.ConflictOwnersUnknown,
		revision:              r.Revision,
		attemptOperationID:    operationID,
	}
	if operationErr != nil {
		return unusableRecord(leaseUUID, operationErr)
	}
	if p.Conflict {
		// Records written before conflict candidates were introduced contain only
		// conflict=true. Preserve them, but never mistake the missing owner set for
		// proof that the current router represents the whole historical fleet.
		if len(p.ConflictBackends) < 2 {
			p.ConflictOwnersUnknown = true
		}
	} else {
		// Candidate metadata has meaning only while the quarantine is active.
		p.ConflictBackends = nil
		p.ConflictOwnersUnknown = false
	}
	if p.Attempt == "" && p.attemptOperationID.Valid() {
		return unusableRecord(leaseUUID, errors.New("operation identity exists without an attempt"))
	}
	if p.Backend == "" && p.Attempt == "" && !p.Conflict {
		p.unusable = true
		slog.Warn("placement: loaded record with no backend or attempt",
			"lease_uuid", leaseUUID)
	}
	return p
}

func decodeOperationID(value string) (operation.OperationID, error) {
	if value == "" {
		return operation.OperationID{}, nil
	}
	id, err := operation.ParseID(value)
	if err != nil {
		return operation.OperationID{}, fmt.Errorf("invalid persisted operation identity: %w", err)
	}
	return id, nil
}

func validLegacyBackendName(v []byte) bool {
	if !utf8.Valid(v) {
		return false
	}
	for _, r := range string(v) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func unusableRecord(leaseUUID string, err error) Placement {
	slog.Warn("placement: loaded unparseable record",
		"lease_uuid", leaseUUID, "error", err)
	return Placement{unusable: true}
}

func encodePlacement(p Placement) ([]byte, error) {
	operationID := ""
	if p.attemptOperationID.Valid() {
		operationID = p.attemptOperationID.String()
	}
	return json.Marshal(record{
		Backend:               p.Backend,
		Attempt:               p.Attempt,
		OperationID:           operationID,
		SetAt:                 p.SetAt,
		Revision:              p.revision,
		Conflict:              p.Conflict,
		ConflictBackends:      normalizeBackendNames(p.ConflictBackends),
		ConflictOwnersUnknown: p.ConflictOwnersUnknown,
	})
}

func normalizeBackendNames(names []string) []string {
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name != "" {
			unique[name] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(unique))
}

func equalPlacementIgnoringRevision(a, b Placement) bool {
	return a.Backend == b.Backend &&
		a.Attempt == b.Attempt &&
		a.SetAt.Equal(b.SetAt) &&
		a.Conflict == b.Conflict &&
		slices.Equal(a.ConflictBackends, b.ConflictBackends) &&
		a.ConflictOwnersUnknown == b.ConflictOwnersUnknown &&
		a.attemptOperationID == b.attemptOperationID &&
		a.unusable == b.unusable
}

// Lookup returns an immutable placement snapshot. A missing key returns the
// zero Placement (StateAbsent).
func (s *Store) Lookup(leaseUUID string) Placement {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := s.cache[leaseUUID]
	p.ConflictBackends = slices.Clone(p.ConflictBackends)
	p.recordRevision = s.newRecordRevision(leaseUUID, p.revision)
	return p
}

// List returns a point-in-time copy of every cached placement, including
// StateUnusable records. Mutating the returned map cannot affect the store.
func (s *Store) List() map[string]Placement {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := maps.Clone(s.cache)
	for leaseUUID, p := range out {
		p.ConflictBackends = slices.Clone(p.ConflictBackends)
		p.recordRevision = s.newRecordRevision(leaseUUID, p.revision)
		out[leaseUUID] = p
	}
	return out
}

// BeginInventorySession registers and returns a typed inventory boundary.
// Callers must pair it with EndInventorySession even when collection fails.
func (s *Store) BeginInventorySession() InventoryFence {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A new collection supersedes every process-local inventory fence issued
	// from an older collection. The durable admission baseline is deliberately
	// independent and remains usable while the topology is unchanged.
	s.advanceAuthorityEpochLocked()
	fence := s.inventoryFenceLocked()
	s.activeSnapshots[fence.revision]++
	return fence
}

// EndInventorySession releases a typed boundary returned by
// BeginInventorySession. Invalid or foreign fences are harmless no-ops. An
// authority-invalidated fence still releases its registered snapshot.
func (s *Store) EndInventorySession(fence InventoryFence) {
	if !fence.Valid() || fence.issuer != s {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endInventorySnapshotLocked(fence.revision)
}

// InventoryBootstrapped reports whether a complete fleet inventory was
// durably committed for the currently configured backend topology. It remains
// true across process restart and ordinary inventory sessions. A topology
// change makes the prior baseline inapplicable until a complete projection
// commits for the new topology.
func (s *Store) InventoryBootstrapped() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasCurrentAdmissionBaselineLocked()
}

// Caller holds s.mu.
func (s *Store) advanceAuthorityEpochLocked() {
	s.authorityEpoch++
	if s.authorityEpoch == 0 {
		// Epoch wrap is not operationally reachable, but zero is reserved for an
		// invalid fence and must never be issued.
		s.authorityEpoch = 1
	}
}

// Caller holds at least s.mu.RLock.
func (s *Store) inventoryFenceLocked() InventoryFence {
	return InventoryFence{issuer: s, revision: s.revision, epoch: s.authorityEpoch}
}

// Caller holds s.mu.
func (s *Store) endInventorySnapshotLocked(revision uint64) {

	count := s.activeSnapshots[revision]
	switch count {
	case 0:
		return
	case 1:
		delete(s.activeSnapshots, revision)
	default:
		s.activeSnapshots[revision] = count - 1
	}
	s.pruneDeleteRevisionsLocked()
}

func validateTypedAttempt(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) error {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return err
	}
	if !operationID.Valid() {
		return operation.ErrInvalidID
	}
	return nil
}

// Caller holds at least s.mu.RLock.
func (s *Store) newAttemptToken(
	leaseUUID, backendName string,
	operationID operation.OperationID,
	revision uint64,
) AttemptToken {
	token := AttemptToken{
		issuer:      s,
		leaseUUID:   leaseUUID,
		backendName: backendName,
		operationID: operationID,
		revision:    s.newRecordRevision(leaseUUID, revision),
	}
	if !token.Valid() {
		return AttemptToken{}
	}
	return token
}

// BeginRestore atomically claims one confirmed source for synchronous restore
// dispatch and durably records the target attempt on that same source backend.
// No claim becomes visible unless the target write commits. The source and
// target must be different leases, the admission baseline must match the
// current durable topology, the source must be one revisioned confirmed owner
// with no unresolved attempt, and the target must have no placement record at
// all.
func (s *Store) BeginRestore(
	baseline AdmissionBaseline,
	sourceLeaseUUID, targetLeaseUUID string,
	operationID operation.OperationID,
) (RestoreClaim, error) {
	if sourceLeaseUUID == "" {
		return RestoreClaim{}, fmt.Errorf("%w: source lease UUID is required", ErrInvalidPlacement)
	}
	if targetLeaseUUID == "" {
		return RestoreClaim{}, fmt.Errorf("%w: target lease UUID is required", ErrInvalidPlacement)
	}
	if sourceLeaseUUID == targetLeaseUUID {
		return RestoreClaim{}, fmt.Errorf("%w: source and target lease UUIDs must differ", ErrInvalidPlacement)
	}
	if !operationID.Valid() {
		return RestoreClaim{}, operation.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateAdmissionBaselineLocked(baseline); err != nil {
		return RestoreClaim{}, err
	}
	if s.restoreSourceClaimedLocked(sourceLeaseUUID) {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, sourceLeaseUUID)
	}
	// A source-reserved lease cannot simultaneously become another restore's
	// target. This is the restore counterpart to the typed attempt-admission
	// fence and keeps the source immutable for the holder of the first claim.
	if s.restoreSourceClaimedLocked(targetLeaseUUID) {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, targetLeaseUUID)
	}

	source, exists := s.cache[sourceLeaseUUID]
	if !exists || source.State() == StateAbsent {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceNotFound, sourceLeaseUUID)
	}
	sourceRevision := s.newRecordRevision(sourceLeaseUUID, source.revision)
	if source.State() != StateConfirmed || source.Attempt != "" ||
		!sourceRevision.Valid() {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceUnavailable, sourceLeaseUUID)
	}
	if err := s.validateConfiguredBackendLocked(source.Backend); err != nil {
		return RestoreClaim{}, err
	}
	target, targetExists := s.cache[targetLeaseUUID]
	if targetExists || target.State() != StateAbsent {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q is %s",
			ErrRestoreTargetUnavailable, targetLeaseUUID, target.State())
	}
	if s.restoreNonce == math.MaxUint64 {
		return RestoreClaim{}, errors.New("placement restore nonce exhausted")
	}
	nonce := s.restoreNonce + 1

	targetRevision, err := s.setAttemptingLocked(targetLeaseUUID, source.Backend, operationID)
	if err != nil {
		return RestoreClaim{}, err
	}

	claim := RestoreClaim{
		issuer:          s,
		sourceLeaseUUID: sourceLeaseUUID,
		targetLeaseUUID: targetLeaseUUID,
		backendName:     source.Backend,
		operationID:     operationID,
		sourceRevision:  sourceRevision,
		targetRevision:  s.newRecordRevision(targetLeaseUUID, targetRevision),
		nonce:           nonce,
	}
	if !claim.Valid() {
		// This is unreachable after the validation above. Returning an error is
		// still safer than exposing a durable attempt with no settlement claim.
		return RestoreClaim{}, ErrInvalidRestoreClaim
	}
	s.restoreNonce = nonce
	s.restoreClaims[sourceLeaseUUID] = claim
	return claim, nil
}

// ConfirmRestore consumes the exact live source claim and promotes the target
// attempt after synchronous backend acceptance. If an exact fast callback has
// already settled the target, it leaves that result untouched and still
// consumes the source claim successfully.
func (s *Store) ConfirmRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementConfirm)
}

// RefuseRestore consumes the exact live source claim and clears the target
// attempt after a definitive synchronous refusal. An exact fast callback wins:
// its already-settled target is never deleted or rewritten.
func (s *Store) RefuseRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementRefuse)
}

// AbandonRestore consumes the exact live source claim without changing the
// durable target attempt. Call it for an ambiguous synchronous outcome: the
// backend (or later complete inventory) owns settlement after dispatch.
func (s *Store) AbandonRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementAbandon)
}

type restoreSettlement uint8

const (
	restoreSettlementConfirm restoreSettlement = iota + 1
	restoreSettlementRefuse
	restoreSettlementAbandon
)

func (s *Store) settleRestore(claim RestoreClaim, settlement restoreSettlement) (bool, error) {
	if !claim.Valid() || claim.issuer != s {
		return false, ErrInvalidRestoreClaim
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.restoreClaims[claim.sourceLeaseUUID]
	if !exists || current != claim {
		return false, nil
	}
	// Source reservations must never leak beyond synchronous dispatch, even if
	// the target settlement write itself fails. The unresolved durable target
	// attempt is the conservative recovery state in that case.
	defer delete(s.restoreClaims, claim.sourceLeaseUUID)

	if settlement == restoreSettlementAbandon {
		return true, nil
	}

	targetToken := AttemptToken{
		issuer:      s,
		leaseUUID:   claim.targetLeaseUUID,
		backendName: claim.backendName,
		operationID: claim.operationID,
		revision:    claim.targetRevision,
	}
	target, matches := s.matchAttemptTokenLocked(targetToken)
	if !matches {
		// Exact callback settlement (or a later authoritative mutation) wins. A
		// synchronous response must never recreate or delete that result.
		return true, nil
	}
	if target.State() == StateUnusable {
		return true, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, claim.targetLeaseUUID)
	}

	switch settlement {
	case restoreSettlementConfirm:
		if target.Backend != "" && target.Backend != claim.backendName {
			return true, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
				ErrBackendConflict, claim.targetLeaseUUID, target.Backend, claim.backendName)
		}
		next, err := s.nextRevision()
		if err != nil {
			return true, err
		}
		target.Backend = claim.backendName
		target.Attempt = ""
		target.attemptOperationID = operation.OperationID{}
		target.revision = next
		capability := promoteAttemptLifecycle(
			claim.backendName, claim.operationID,
		)
		if err := s.putPlacementWithLifecycleLocked(
			claim.targetLeaseUUID, target, capability, "confirm restore placement",
		); err != nil {
			return true, err
		}
		s.revision = next
		return true, nil

	case restoreSettlementRefuse:
		if target.Backend == "" {
			if err := s.deleteLocked(claim.targetLeaseUUID, "refuse restore placement"); err != nil {
				return true, err
			}
			return true, nil
		}
		next, err := s.nextRevision()
		if err != nil {
			return true, err
		}
		target.Attempt = ""
		target.attemptOperationID = operation.OperationID{}
		target.revision = next
		capability := clearAttemptLifecycle(
			s.lifecycleCache[claim.targetLeaseUUID], claim.backendName, claim.operationID,
		)
		if err := s.putPlacementWithLifecycleLocked(
			claim.targetLeaseUUID, target, capability, "refuse restore placement",
		); err != nil {
			return true, err
		}
		s.revision = next
		return true, nil

	default:
		return true, ErrInvalidRestoreClaim
	}
}

// Caller holds at least s.mu.RLock.
func (s *Store) restoreSourceClaimedLocked(leaseUUID string) bool {
	_, claimed := s.restoreClaims[leaseUUID]
	return claimed
}

// Caller holds s.mu.
func (s *Store) setAttemptingLocked(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (uint64, error) {
	existing, exists := s.cache[leaseUUID]
	if exists && existing.State() == StateUnusable {
		return 0, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if existing.Attempt != "" {
		return 0, fmt.Errorf("%w: lease %q targets %q", ErrAttemptConflict, leaseUUID, existing.Attempt)
	}
	if existing.Backend != "" && existing.Backend != backendName {
		return 0, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, existing.Backend, backendName)
	}

	revision, err := s.nextRevision()
	if err != nil {
		return 0, err
	}
	p := existing
	if !exists {
		p.SetAt = s.now().UTC()
	}
	capability, err := s.lifecycleWithAttemptLocked(leaseUUID, backendName, operationID)
	if err != nil {
		return 0, err
	}
	p.Attempt = backendName
	p.attemptOperationID = operationID
	p.revision = revision
	if err := s.putPlacementWithLifecycleLocked(
		leaseUUID, p, capability, "set attempting placement",
	); err != nil {
		return 0, err
	}
	s.revision = revision
	return revision, nil
}

// ConfirmAttempt promotes only the exact typed write-ahead attempt represented
// by token. A stale token returns false without writing; it can never confirm a
// later attempt even when that attempt targets the same backend.
func (s *Store) ConfirmAttempt(token AttemptToken) (bool, error) {
	if err := s.validateAttemptToken(token); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, matches := s.matchAttemptTokenLocked(token)
	if !matches {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, token.leaseUUID)
	}
	if p.Backend != "" && p.Backend != token.backendName {
		return false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, token.leaseUUID, p.Backend, token.backendName)
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Backend = token.backendName
	p.Attempt = ""
	p.attemptOperationID = operation.OperationID{}
	p.revision = next
	capability := promoteAttemptLifecycle(
		token.backendName, token.operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		token.leaseUUID, p, capability, "confirm typed placement attempt",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// RefuseAttempt clears only the exact typed write-ahead attempt represented by
// token after a definitive synchronous refusal. Ambiguous outcomes must not
// call this method. A stale token returns false without writing.
func (s *Store) RefuseAttempt(token AttemptToken) (bool, error) {
	if err := s.validateAttemptToken(token); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, matches := s.matchAttemptTokenLocked(token)
	if !matches {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, token.leaseUUID)
	}

	if p.Backend == "" {
		if err := s.deleteLocked(token.leaseUUID, "refuse typed placement attempt"); err != nil {
			return false, err
		}
		return true, nil
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Attempt = ""
	p.attemptOperationID = operation.OperationID{}
	p.revision = next
	capability := clearAttemptLifecycle(
		s.lifecycleCache[token.leaseUUID], token.backendName, token.operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		token.leaseUUID, p, capability, "refuse typed placement attempt",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// ConfirmOperation promotes a typed attempt identified by durable operation
// identity. It is the callback-safe counterpart to ConfirmAttempt: callbacks
// need not retain the process-local AttemptToken, but still cannot settle a
// legacy, mismatched, or newer same-backend operation. An already-confirmed
// same-backend record with no attempt is an idempotent success and is not
// mutated.
func (s *Store) ConfirmOperation(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.cache[leaseUUID]
	if !exists {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Attempt == "" {
		if p.Backend != backendName {
			return false, nil
		}
		capability, err := rotateMaintenanceLifecycle(
			s.lifecycleCache[leaseUUID], backendName, operationID,
		)
		if err != nil {
			return false, err
		}
		current := s.lifecycleCache[leaseUUID]
		if current == capability {
			return true, nil
		}
		if err := s.putLifecycleLocked(
			leaseUUID, capability, "rotate confirmed lifecycle capability",
		); err != nil {
			return false, err
		}
		return true, nil
	}
	if p.Attempt != backendName || p.attemptOperationID != operationID {
		return false, nil
	}
	if p.Backend != "" && p.Backend != backendName {
		return false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, p.Backend, backendName)
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Backend = backendName
	p.Attempt = ""
	p.attemptOperationID = operation.OperationID{}
	p.revision = next
	capability := promoteAttemptLifecycle(
		backendName, operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		leaseUUID, p, capability, "confirm placement operation",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// RefuseOperation clears only the typed attempt whose persisted operation
// identity exactly matches. It never removes an already-confirmed owner.
func (s *Store) RefuseOperation(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.cache[leaseUUID]
	if !exists || p.Attempt != backendName || p.attemptOperationID != operationID {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Backend == "" {
		if err := s.deleteLocked(leaseUUID, "refuse placement operation"); err != nil {
			return false, err
		}
		return true, nil
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Attempt = ""
	p.attemptOperationID = operation.OperationID{}
	p.revision = next
	capability := clearAttemptLifecycle(
		s.lifecycleCache[leaseUUID], backendName, operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		leaseUUID, p, capability, "refuse placement operation",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

func (s *Store) validateAttemptToken(token AttemptToken) error {
	if !token.Valid() || token.issuer != s {
		return ErrInvalidAttemptToken
	}
	return nil
}

// matchAttemptTokenLocked checks every durable token component in one critical
// section. Caller holds s.mu.
func (s *Store) matchAttemptTokenLocked(token AttemptToken) (Placement, bool) {
	p, exists := s.cache[token.leaseUUID]
	if !exists || p.revision != token.revision.value ||
		p.Attempt != token.backendName || p.attemptOperationID != token.operationID {
		return Placement{}, false
	}
	return p, true
}

// DeleteRecord removes only the exact store- and lease-bound placement record
// represented by revision. The target is derived from the capability itself;
// callers cannot transplant a numerically equal revision to another lease or
// store. Invalid and foreign revisions are rejected.
func (s *Store) DeleteRecord(revision RecordRevision) (bool, error) {
	if !revision.Valid() || revision.issuer != s.recordIssuer {
		return false, ErrInvalidRecordRevision
	}
	leaseUUID := revision.leaseUUID

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreSourceClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, leaseUUID)
	}
	p, exists := s.cache[leaseUUID]
	if !exists || p.revision != revision.value {
		return false, nil
	}
	if err := s.deleteLocked(leaseUUID, "delete typed placement record"); err != nil {
		return false, err
	}
	return true, nil
}

type projectionMutation struct {
	placement Placement
	encoded   []byte
	revision  uint64
}

type projectionLifecycleMutation struct {
	capability lifecycleCapability
	encoded    []byte
	persist    bool
}

// ProjectInventory computes a placement projection against fence and persists
// every material write in one bbolt transaction. When Complete is true, that
// same transaction also establishes the durable admission baseline for the
// configured topology, including for empty or idempotent projections. No
// cache, baseline, or revision-clock change is visible unless the transaction
// commits. A partial projection never erases an existing matching baseline.
func (s *Store) ProjectInventory(
	fence InventoryFence,
	input InventoryProjection,
) (ProjectionResult, error) {
	projection, err := normalizeInventoryProjection(input)
	if err != nil {
		return ProjectionResult{}, err
	}
	if !fence.Valid() || fence.issuer != s {
		return ProjectionResult{}, ErrInvalidInventoryFence
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if fence.epoch != s.authorityEpoch {
		return ProjectionResult{}, ErrInvalidInventoryFence
	}
	if err := s.validateProjectionBackendsLocked(projection); err != nil {
		return ProjectionResult{}, err
	}

	result := ProjectionResult{Fenced: make(map[string]struct{})}
	keySet := make(map[string]struct{}, len(projection.Placements)+len(projection.Conflicts))
	for leaseUUID := range projection.Placements {
		keySet[leaseUUID] = struct{}{}
	}
	for leaseUUID := range projection.Conflicts {
		keySet[leaseUUID] = struct{}{}
	}
	keys := slices.Sorted(maps.Keys(keySet))

	now := s.now().UTC()
	nextRevision := s.revision
	mutations := make(map[string]projectionMutation, len(keys))
	lifecycleMutations := make(map[string]projectionLifecycleMutation, len(keys))
	for _, leaseUUID := range keys {
		if s.restoreSourceClaimedLocked(leaseUUID) {
			result.markFenced(leaseUUID)
			continue
		}
		if s.mutationRevisionLocked(leaseUUID) > fence.revision {
			result.markFenced(leaseUUID)
			continue
		}

		existing, exists := s.cache[leaseUUID]
		var candidate Placement
		var mutate bool
		switch {
		case projection.Conflicts[leaseUUID] != nil:
			candidate = projectConflict(existing, exists, projection.Conflicts[leaseUUID], now)
			mutate = !exists || existing.revision == 0 ||
				!equalPlacementIgnoringRevision(candidate, existing)

		case projection.Placements[leaseUUID] != "":
			candidate = projectPositivePlacement(
				existing,
				exists,
				projection.Placements[leaseUUID],
				now,
			)
			mutate = !exists || existing.revision == 0 ||
				!equalPlacementIgnoringRevision(candidate, existing)
		}
		if !mutate {
			continue
		}
		if nextRevision == math.MaxUint64 {
			return ProjectionResult{}, fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		candidate.revision = nextRevision
		encoded, err := encodePlacement(candidate)
		if err != nil {
			return ProjectionResult{}, mutationFailure("encode inventory projection", err)
		}
		if backendName := projection.Placements[leaseUUID]; backendName != "" {
			currentCapability, capabilityExists := s.lifecycleCache[leaseUUID]
			capability, persist := projectPositiveLifecycle(
				currentCapability, capabilityExists, existing, exists, backendName,
			)
			var capabilityEncoded []byte
			if persist {
				var capabilityErr error
				capabilityEncoded, capabilityErr = encodeLifecycleCapability(capability)
				if capabilityErr != nil {
					return ProjectionResult{}, mutationFailure(
						"encode inventory lifecycle projection", capabilityErr,
					)
				}
			}
			lifecycleMutations[leaseUUID] = projectionLifecycleMutation{
				capability: capability,
				encoded:    capabilityEncoded,
				persist:    persist,
			}
		}
		mutations[leaseUUID] = projectionMutation{
			placement: candidate,
			encoded:   encoded,
			revision:  nextRevision,
		}
	}

	if len(mutations) == 0 && !projection.Complete {
		if err := s.verifyBucket(); err != nil {
			return ProjectionResult{}, mutationFailure("verify inventory projection", err)
		}
		return result, nil
	}

	mutationKeys := slices.Sorted(maps.Keys(mutations))
	nextMetadata := s.topologyMetadataLocked()
	if projection.Complete {
		nextMetadata.BaselineFingerprint = s.topologyFingerprint
		nextMetadata.BaselineTopologyID = s.topologyID
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if b == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		for _, leaseUUID := range mutationKeys {
			mutation := mutations[leaseUUID]
			if err := b.Put([]byte(leaseUUID), mutation.encoded); err != nil {
				return err
			}
			if lifecycleMutation, ok := lifecycleMutations[leaseUUID]; ok &&
				lifecycleMutation.persist {
				if err := capabilities.Put(
					[]byte(leaseUUID), lifecycleMutation.encoded,
				); err != nil {
					return err
				}
			}
		}
		if projection.Complete {
			if err := putTopologyMetadata(tx, nextMetadata); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return ProjectionResult{}, mutationFailure("project placement inventory", err)
	}

	for _, leaseUUID := range mutationKeys {
		mutation := mutations[leaseUUID]
		s.cache[leaseUUID] = mutation.placement
		if lifecycleMutation, ok := lifecycleMutations[leaseUUID]; ok {
			s.lifecycleCache[leaseUUID] = lifecycleMutation.capability
		}
		delete(s.deleteRevisions, leaseUUID)
	}
	s.revision = nextRevision
	if projection.Complete {
		s.baselineFingerprint = nextMetadata.BaselineFingerprint
		s.baselineTopologyID = nextMetadata.BaselineTopologyID
	}
	return result, nil
}

func (result *ProjectionResult) markFenced(leaseUUID string) {
	result.Fenced[leaseUUID] = struct{}{}
}

func normalizeInventoryProjection(input InventoryProjection) (InventoryProjection, error) {
	projection := InventoryProjection{
		Complete:   input.Complete,
		Placements: maps.Clone(input.Placements),
		Conflicts:  make(map[string][]string, len(input.Conflicts)),
	}
	for leaseUUID, backendName := range projection.Placements {
		if err := validateIDs(leaseUUID, backendName); err != nil {
			return InventoryProjection{}, err
		}
	}
	for leaseUUID, backendNames := range input.Conflicts {
		if leaseUUID == "" {
			return InventoryProjection{}, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
		normalized := normalizeBackendNames(backendNames)
		if len(normalized) < 2 {
			return InventoryProjection{}, fmt.Errorf(
				"%w: conflict for lease %q requires at least two backends",
				ErrInvalidPlacement, leaseUUID,
			)
		}
		projection.Conflicts[leaseUUID] = normalized
	}
	for leaseUUID := range projection.Conflicts {
		if _, overlaps := projection.Placements[leaseUUID]; overlaps {
			return InventoryProjection{}, projectionOverlapError(leaseUUID)
		}
	}
	return projection, nil
}

func projectionOverlapError(leaseUUID string) error {
	return fmt.Errorf("%w: projection contains contradictory outcomes for lease %q",
		ErrInvalidPlacement, leaseUUID)
}

func projectConflict(
	existing Placement,
	exists bool,
	backendNames []string,
	now time.Time,
) Placement {
	setAt := existing.SetAt
	if setAt.IsZero() {
		setAt = now
	}
	candidateSet := make(map[string]struct{}, len(backendNames)+len(existing.ConflictBackends)+2)
	for _, backendName := range backendNames {
		candidateSet[backendName] = struct{}{}
	}
	for _, backendName := range existing.ConflictBackends {
		candidateSet[backendName] = struct{}{}
	}
	if existing.Backend != "" {
		candidateSet[existing.Backend] = struct{}{}
	}
	if existing.Attempt != "" {
		candidateSet[existing.Attempt] = struct{}{}
	}
	unknownOwners := existing.ConflictOwnersUnknown ||
		(existing.Conflict && len(existing.ConflictBackends) < 2) ||
		(exists && existing.State() == StateUnusable && !existing.Conflict)
	return Placement{
		Backend:               existing.Backend,
		Attempt:               existing.Attempt,
		SetAt:                 setAt,
		Conflict:              true,
		ConflictBackends:      slices.Sorted(maps.Keys(candidateSet)),
		ConflictOwnersUnknown: unknownOwners,
		attemptOperationID:    existing.attemptOperationID,
	}
}

func projectPositivePlacement(
	existing Placement,
	exists bool,
	backendName string,
	now time.Time,
) Placement {
	p := existing
	if !exists || p.unusable {
		p = Placement{SetAt: now}
	}
	p.Backend = backendName
	if p.Attempt == backendName {
		p.Attempt = ""
		p.attemptOperationID = operation.OperationID{}
	}
	p.Conflict = false
	p.ConflictBackends = nil
	p.ConflictOwnersUnknown = false
	p.unusable = false
	return p
}

// Healthy checks that the bbolt database and placement authority buckets are accessible.
func (s *Store) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		if tx.Bucket(lifecycleCapabilityBucketName) == nil {
			return errors.New("placement lifecycle capability bucket missing")
		}
		return nil
	})
}

// Close closes the bbolt database. It is safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func validateIDs(leaseUUID, backendName string) error {
	if leaseUUID == "" {
		return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}
	if backendName == "" {
		return fmt.Errorf("%w: backend name is required", ErrInvalidPlacement)
	}
	return nil
}

func (s *Store) nextRevision() (uint64, error) {
	if s.revision == math.MaxUint64 {
		return 0, fmt.Errorf("placement revision exhausted")
	}
	return s.revision + 1, nil
}

// mutationRevisionLocked returns the per-record revision for a present key or
// the exact key's deletion revision while an older registered inventory remains
// active. Unrelated deletions never fence this lease.
// Caller holds at least s.mu.RLock.
func (s *Store) mutationRevisionLocked(leaseUUID string) uint64 {
	if p, exists := s.cache[leaseUUID]; exists {
		return p.revision
	}
	return s.deleteRevisions[leaseUUID]
}

// verifyBucket proves the durable store is readable even when an idempotent or
// fully fenced synchronization has no mutation to commit.
// Caller holds at least s.mu.RLock.
func (s *Store) verifyBucket() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		if tx.Bucket(lifecycleCapabilityBucketName) == nil {
			return errors.New("placement lifecycle capability bucket missing")
		}
		return nil
	})
}

// deleteLocked durably removes one key and retains its deletion revision only
// while a registered inventory snapshot could otherwise recreate it.
// Caller holds s.mu.
func (s *Store) deleteLocked(leaseUUID, operation string) error {
	revision, err := s.nextRevision()
	if err != nil {
		return err
	}
	if err := s.deleteDurable(leaseUUID, operation); err != nil {
		return err
	}
	delete(s.cache, leaseUUID)
	if len(s.activeSnapshots) > 0 {
		s.deleteRevisions[leaseUUID] = revision
	}
	s.revision = revision
	return nil
}

// pruneDeleteRevisionsLocked drops tombstones that are no newer than every
// active inventory cutoff. Such inventories already observed the key absent.
// Caller holds s.mu.
func (s *Store) pruneDeleteRevisionsLocked() {
	if len(s.activeSnapshots) == 0 {
		clear(s.deleteRevisions)
		return
	}
	oldest := uint64(math.MaxUint64)
	for revision := range s.activeSnapshots {
		if revision < oldest {
			oldest = revision
		}
	}
	for leaseUUID, revision := range s.deleteRevisions {
		if revision <= oldest {
			delete(s.deleteRevisions, leaseUUID)
		}
	}
}

// deleteDurable removes placement while retaining its current lifecycle
// capability for delayed teardown callbacks. Any unresolved attempt marker is
// cleared in the same transaction. Caller holds s.mu and performs the placement
// cache delete after this succeeds.
func (s *Store) deleteDurable(leaseUUID, operation string) error {
	capability, capabilityExists := s.lifecycleCache[leaseUUID]
	capability, retainCapability := lifecycleAfterPlacementDelete(
		capability, s.cache[leaseUUID],
	)
	var capabilityEncoded []byte
	var err error
	if capabilityExists && retainCapability {
		capabilityEncoded, err = encodeLifecycleCapability(capability)
		if err != nil {
			return mutationFailure("encode lifecycle capability for "+operation, err)
		}
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		if err := placements.Delete([]byte(leaseUUID)); err != nil {
			return err
		}
		if capabilityExists && retainCapability {
			return capabilities.Put([]byte(leaseUUID), capabilityEncoded)
		}
		return capabilities.Delete([]byte(leaseUUID))
	}); err != nil {
		return mutationFailure(operation, err)
	}
	if capabilityExists && retainCapability {
		s.lifecycleCache[leaseUUID] = capability
	} else {
		delete(s.lifecycleCache, leaseUUID)
	}
	return nil
}

func mutationFailure(operation string, err error) error {
	metrics.PlacementWriteFailuresTotal.Inc()
	return fmt.Errorf("failed to %s: %w", operation, err)
}
