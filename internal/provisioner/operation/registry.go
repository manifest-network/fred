package operation

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
)

// Kind distinguishes a fresh provision from a restore.
type Kind uint8

const (
	KindInvalid Kind = iota
	KindProvision
	KindRestore
)

func (kind Kind) valid() bool {
	switch kind {
	case KindProvision, KindRestore:
		return true
	case KindInvalid:
		return false
	default:
		return false
	}
}

// TrackSpec contains the immutable facts recorded when an operation starts.
// StartedAt defaults to time.Now when it is zero. Its zero value is invalid.
type TrackSpec struct {
	LeaseUUID     string
	Tenant        string
	Items         []backend.LeaseItem
	Backend       string
	StartedAt     time.Time
	Kind          Kind
	TokenRequired bool
}

// Valid reports whether spec contains the minimum identity needed by the
// registry. Tenant, backend, and items are metadata and may legitimately be
// empty while an operation is being recovered.
func (spec TrackSpec) Valid() bool {
	return spec.LeaseUUID != "" && spec.Kind.valid()
}

// Record is an immutable snapshot of one tracked operation. Mutating the Items
// slice returned by Registry methods cannot mutate registry state.
type Record struct {
	LeaseUUID     string
	Tenant        string
	Items         []backend.LeaseItem
	Backend       string
	ID            OperationID
	TokenRequired bool
	StartedAt     time.Time
	Kind          Kind
	Phase         Phase
	Settlement    SettlementKind
}

// Valid reports whether record represents a tracked operation.
func (record Record) Valid() bool {
	return record.LeaseUUID != "" && record.ID.Valid() && record.Kind.valid() && record.Phase.valid()
}

// RoutingSKU returns the first SKU for backend routing decisions.
func (record Record) RoutingSKU() string {
	if len(record.Items) == 0 {
		return ""
	}
	return record.Items[0].SKU
}

func (record Record) clone() Record {
	record.Items = slices.Clone(record.Items)
	return record
}

// TrackOutcome is the exhaustive result of attempting to track an operation.
// The zero value is conservative and never authorizes a backend side effect.
type TrackOutcome uint8

const (
	TrackInvalid TrackOutcome = iota
	TrackStarted
	TrackBusy
	TrackSnapshotStale
)

// TrackResult contains a start outcome and, only when Started returns true, the
// capability that owns the tracked operation.
type TrackResult struct {
	token   Token
	outcome TrackOutcome
}

// Phase describes where a typed operation is relative to its synchronous
// backend call. The zero value is invalid and therefore authorizes no actor.
type Phase uint8

const (
	PhaseInvalid Phase = iota
	PhasePreparing
	PhaseCalling
	PhaseActive
)

func (phase Phase) valid() bool {
	switch phase {
	case PhasePreparing, PhaseCalling, PhaseActive:
		return true
	case PhaseInvalid:
		return false
	default:
		return false
	}
}

// InitiationResult contains the outcome of registering a phase-aware backend
// operation and, only on success, its opaque initiation capability.
type InitiationResult struct {
	initiation Initiation
	outcome    TrackOutcome
}

// Outcome returns why initiation did or did not start.
func (result InitiationResult) Outcome() TrackOutcome { return result.outcome }

// Started reports whether a preparing operation was registered.
func (result InitiationResult) Started() bool {
	return result.outcome == TrackStarted && result.initiation.Valid()
}

// Capability returns the initiation capability on success and an invalid
// capability for every other outcome.
func (result InitiationResult) Capability() Initiation {
	if !result.Started() {
		return Initiation{}
	}
	return result.initiation
}

// InitiationCompletion is the exhaustive result of completing an initiation.
// The zero value is an invalid/stale capability and is never success.
type InitiationCompletion uint8

const (
	InitiationInvalid InitiationCompletion = iota
	InitiationActivated
	InitiationAborted
	InitiationSettling
	InitiationFinished
)

// Outcome returns the reason tracking did or did not start.
func (result TrackResult) Outcome() TrackOutcome {
	return result.outcome
}

// Started reports whether the operation was registered.
func (result TrackResult) Started() bool {
	return result.outcome == TrackStarted && result.token.Valid()
}

// Token returns the operation capability on success and an invalid token for
// every non-started outcome.
func (result TrackResult) Token() Token {
	if !result.Started() {
		return Token{}
	}
	return result.token
}

// LeaseClaimOutcome is the exhaustive result of attempting to claim a lease.
// The invalid zero value is conservative and does not authorize work.
type LeaseClaimOutcome uint8

const (
	LeaseClaimInvalid LeaseClaimOutcome = iota
	LeaseClaimAcquired
	LeaseClaimBusy
	LeaseClaimSnapshotStale
)

// LeaseClaimResult contains a lease-claim outcome and its capability.
type LeaseClaimResult struct {
	claim   LeaseClaim
	outcome LeaseClaimOutcome
}

// Outcome returns the reason the lease was or was not claimed.
func (result LeaseClaimResult) Outcome() LeaseClaimOutcome {
	return result.outcome
}

// Acquired reports whether the exclusive lease claim was acquired.
func (result LeaseClaimResult) Acquired() bool {
	return result.outcome == LeaseClaimAcquired && result.claim.Valid()
}

// Claim returns the capability on success and an invalid claim otherwise.
func (result LeaseClaimResult) Claim() LeaseClaim {
	if !result.Acquired() {
		return LeaseClaim{}
	}
	return result.claim
}

// SettlementOutcome is the exhaustive result of attempting to claim terminal
// work for an operation. Its zero value is conservative.
type SettlementOutcome uint8

const (
	SettlementInvalid SettlementOutcome = iota
	SettlementClaimed
	SettlementNotFound
	SettlementOperationMismatch
	SettlementBusy
)

// SettlementResult contains an immutable operation snapshot and the exclusive
// capability needed to release or finish its settlement.
type SettlementResult struct {
	record  Record
	claim   SettlementClaim
	outcome SettlementOutcome
}

// Outcome returns the reason settlement was or was not claimed.
func (result SettlementResult) Outcome() SettlementOutcome {
	return result.outcome
}

// Claimed reports whether the settlement claim was acquired.
func (result SettlementResult) Claimed() bool {
	return result.outcome == SettlementClaimed && result.record.Valid() && result.claim.Valid()
}

// Record returns the claimed operation snapshot on success and an invalid
// record otherwise.
func (result SettlementResult) Record() Record {
	if !result.Claimed() {
		return Record{}
	}
	return result.record.clone()
}

// Claim returns the settlement capability on success and an invalid claim
// otherwise.
func (result SettlementResult) Claim() SettlementClaim {
	if !result.Claimed() {
		return SettlementClaim{}
	}
	return result.claim
}

type trackedOperation struct {
	record           Record
	token            Token
	initiation       Initiation
	claim            SettlementClaim
	terminalFinished bool
}

// Registry is the sole owner of process-local lifecycle-operation state and
// claims. All exported methods are safe for concurrent use.
type Registry struct {
	operations           map[string]trackedOperation
	leaseClaims          map[string]LeaseClaim
	lastMutation         map[string]uint64
	issuedOperationIDs   map[OperationID]struct{}
	countObserver        func(int)
	operationIDSource    func() (OperationID, error)
	identity             registryIdentity
	nextOperationID      uint64
	nextClaimNonce       uint64
	mutationRevision     uint64
	lastSnapshotRevision uint64
	mu                   sync.RWMutex
}

// NewRegistry constructs an empty operation registry with process-unique ID
// and capability seeds.
func NewRegistry() *Registry {
	return newRandomRegistryWithObserver(
		randomNonZeroUint64(), randomNonZeroUint64(), nil,
	)
}

// NewRegistryWithCountObserver constructs a registry that synchronously calls
// observer after each operation-count mutation. The observer must not call back
// into the registry. This hook keeps metrics outside the coordination package
// while ensuring typed and compatibility callers observe the same count.
func NewRegistryWithCountObserver(observer func(int)) *Registry {
	return newRandomRegistryWithObserver(
		randomNonZeroUint64(), randomNonZeroUint64(), observer,
	)
}

func newRandomRegistryWithObserver(
	identity, claimSeed uint64,
	observer func(int),
) *Registry {
	registry := newRegistryBase(identity, claimSeed, observer)
	registry.operationIDSource = randomOperationID
	return registry
}

func newRegistry(identity, operationSeed, claimSeed uint64) *Registry {
	return newRegistryWithObserver(identity, operationSeed, claimSeed, nil)
}

func newRegistryWithObserver(
	identity, operationSeed, claimSeed uint64,
	observer func(int),
) *Registry {
	registry := newRegistryBase(identity, claimSeed, observer)
	registry.nextOperationID = operationSeed
	// Deterministic allocation exists only for package tests. Production
	// constructors install randomOperationID above so an ID observed by one
	// backend reveals nothing about another backend's current or future ID.
	registry.operationIDSource = func() (OperationID, error) {
		registry.nextOperationID++
		if registry.nextOperationID == 0 {
			registry.nextOperationID++
		}
		return deterministicOperationID(registry.nextOperationID), nil
	}
	return registry
}

func newRegistryBase(
	identity, claimSeed uint64,
	observer func(int),
) *Registry {
	if identity == 0 {
		identity = 1
	}
	return &Registry{
		operations:         make(map[string]trackedOperation),
		leaseClaims:        make(map[string]LeaseClaim),
		lastMutation:       make(map[string]uint64),
		issuedOperationIDs: make(map[OperationID]struct{}),
		countObserver:      observer,
		identity:           newRegistryIdentity(identity),
		nextClaimNonce:     claimSeed,
	}
}

func randomOperationID() (OperationID, error) {
	value, err := uuid.NewRandom()
	if err != nil {
		// Operation identity is a wire capability shared across mutually
		// untrusted backends. A predictable fallback would turn RNG failure into
		// cross-backend callback authority, so allocation fails closed.
		return OperationID{}, err
	}
	return newOperationID(value), nil
}

// deterministicOperationID is used only by package-private deterministic test
// construction. Production constructors always install randomOperationID.
func deterministicOperationID(sequence uint64) OperationID {
	var input [8]byte
	binary.BigEndian.PutUint64(input[:], sequence)
	digest := sha256.Sum256(input[:])
	var value uuid.UUID
	copy(value[:], digest[:len(value)])
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return newOperationID(value)
}

func randomNonZeroUint64() uint64 {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err == nil {
		if value := binary.BigEndian.Uint64(raw[:]); value != 0 {
			return value
		}
	}
	value := uint64(time.Now().UnixNano())
	if value == 0 {
		return 1
	}
	return value
}

// TryTrack atomically starts tracking spec when neither an operation nor a
// lease claim already owns its lease.
func (registry *Registry) TryTrack(spec TrackSpec) TrackResult {
	if !spec.Valid() {
		return TrackResult{outcome: TrackInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, claimed := registry.leaseClaims[spec.LeaseUUID]; claimed {
		return TrackResult{outcome: TrackBusy}
	}
	if _, exists := registry.operations[spec.LeaseUUID]; exists {
		return TrackResult{outcome: TrackBusy}
	}
	return registry.trackLocked(spec)
}

// TryTrackClaimed starts tracking under the exact lease capability returned by
// TryClaimLease or TryClaimLeaseNow. A claim from another registry, a stale
// claim, or a spec for another lease is invalid.
func (registry *Registry) TryTrackClaimed(claim LeaseClaim, spec TrackSpec) TrackResult {
	if !spec.Valid() || !claim.Valid() || claim.leaseUUID != spec.LeaseUUID {
		return TrackResult{outcome: TrackInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if claim.registry != registry.identity || registry.leaseClaims[spec.LeaseUUID] != claim {
		return TrackResult{outcome: TrackInvalid}
	}
	if _, exists := registry.operations[spec.LeaseUUID]; exists {
		return TrackResult{outcome: TrackBusy}
	}
	return registry.trackLocked(spec)
}

func (registry *Registry) trackLocked(spec TrackSpec) TrackResult {
	token := registry.installLocked(spec, PhaseActive, Initiation{})
	if !token.Valid() {
		return TrackResult{outcome: TrackInvalid}
	}
	return TrackResult{token: token, outcome: TrackStarted}
}

// TryInitiate registers spec in the preparing phase when neither an operation
// nor a lease claim owns the lease. Production backend-call paths use this
// instead of TryTrack so close, timeout, and callback actors can distinguish
// local preparation from accepted asynchronous work.
func (registry *Registry) TryInitiate(spec TrackSpec) InitiationResult {
	if !spec.Valid() {
		return InitiationResult{outcome: TrackInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, claimed := registry.leaseClaims[spec.LeaseUUID]; claimed {
		return InitiationResult{outcome: TrackBusy}
	}
	if _, exists := registry.operations[spec.LeaseUUID]; exists {
		return InitiationResult{outcome: TrackBusy}
	}
	return registry.initiateLocked(spec)
}

// TryInitiateClaimed registers a preparing operation under the exact lease
// action capability held by reconciliation.
func (registry *Registry) TryInitiateClaimed(
	claim LeaseClaim,
	spec TrackSpec,
) InitiationResult {
	if !spec.Valid() || !claim.Valid() || claim.leaseUUID != spec.LeaseUUID {
		return InitiationResult{outcome: TrackInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if claim.registry != registry.identity || registry.leaseClaims[spec.LeaseUUID] != claim {
		return InitiationResult{outcome: TrackInvalid}
	}
	if _, exists := registry.operations[spec.LeaseUUID]; exists {
		return InitiationResult{outcome: TrackBusy}
	}
	return registry.initiateLocked(spec)
}

func (registry *Registry) initiateLocked(spec TrackSpec) InitiationResult {
	// installLocked needs the capability stored with the record, while the
	// capability itself contains the token allocated there. Install explicitly
	// so both values are born under the same registry lock.
	id := registry.allocateOperationIDLocked()
	if !id.Valid() {
		return InitiationResult{outcome: TrackInvalid}
	}
	token := newToken(registry.identity, spec.LeaseUUID, id)
	initiation := newInitiation(token)
	registry.installRecordLocked(spec, token, PhasePreparing, initiation)
	return InitiationResult{initiation: initiation, outcome: TrackStarted}
}

func (registry *Registry) installLocked(
	spec TrackSpec,
	phase Phase,
	initiation Initiation,
) Token {
	id := registry.allocateOperationIDLocked()
	if !id.Valid() {
		return Token{}
	}
	token := newToken(registry.identity, spec.LeaseUUID, id)
	registry.installRecordLocked(spec, token, phase, initiation)
	return token
}

func (registry *Registry) installRecordLocked(
	spec TrackSpec,
	token Token,
	phase Phase,
	initiation Initiation,
) {
	startedAt := spec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	record := Record{
		LeaseUUID:     spec.LeaseUUID,
		Tenant:        spec.Tenant,
		Items:         slices.Clone(spec.Items),
		Backend:       spec.Backend,
		ID:            token.ID(),
		TokenRequired: spec.TokenRequired,
		StartedAt:     startedAt,
		Kind:          spec.Kind,
		Phase:         phase,
		Settlement:    SettlementUnclaimed,
	}
	registry.operations[spec.LeaseUUID] = trackedOperation{
		record:     record,
		token:      token,
		initiation: initiation,
	}
	registry.markMutationLocked(spec.LeaseUUID)
	registry.notifyCountLocked()
}

// BindBackend binds the exact preparing initiation to the authoritative
// backend discovered after the operation was registered. Restore uses this to
// avoid trusting a pre-claim source lookup: the operation begins with no
// backend, placement.BeginRestore atomically selects the source owner, and only
// this initiation capability can bind that owner before the call starts.
//
// Binding is one-shot. Invalid, foreign, stale, already-bound, claimed, or
// non-preparing operations return false without mutation.
func (registry *Registry) BindBackend(initiation Initiation, backendName string) bool {
	if !initiation.Valid() || initiation.token.registry != registry.identity || backendName == "" {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[initiation.token.leaseUUID]
	if !exists || tracked.initiation != initiation || tracked.token != initiation.token ||
		tracked.record.Phase != PhasePreparing || tracked.record.Backend != "" ||
		tracked.claim.Valid() || tracked.terminalFinished {
		return false
	}
	tracked.record.Backend = backendName
	registry.operations[initiation.token.leaseUUID] = tracked
	return true
}

// BeginCall advances the exact preparing operation to calling immediately
// before invoking the synchronous backend method.
func (registry *Registry) BeginCall(initiation Initiation) bool {
	if !initiation.Valid() || initiation.token.registry != registry.identity {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[initiation.token.leaseUUID]
	if !exists || tracked.initiation != initiation || tracked.token != initiation.token ||
		tracked.record.Phase != PhasePreparing || tracked.record.Backend == "" ||
		tracked.claim.Valid() || tracked.terminalFinished {
		return false
	}
	tracked.record.Phase = PhaseCalling
	registry.operations[initiation.token.leaseUUID] = tracked
	return true
}

// Activate completes an accepted synchronous backend return. If an inline
// callback is settling, it retains ownership; if it already finished, this
// call only retires the call barrier left behind for the initiator.
func (registry *Registry) Activate(initiation Initiation) InitiationCompletion {
	return registry.completeInitiation(initiation, true)
}

// AbortInitiation completes a local preflight failure or a synchronous backend
// result that was not accepted. A callback that already claimed or finished
// the exact operation wins, because it is stronger evidence than the caller's
// cleanup path.
func (registry *Registry) AbortInitiation(initiation Initiation) InitiationCompletion {
	return registry.completeInitiation(initiation, false)
}

func (registry *Registry) completeInitiation(
	initiation Initiation,
	accepted bool,
) InitiationCompletion {
	if !initiation.Valid() || initiation.token.registry != registry.identity {
		return InitiationInvalid
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[initiation.token.leaseUUID]
	if !exists {
		// Phase-aware settlement is the only production path that can finish the
		// exact operation before its caller returns. Missing is therefore an
		// idempotent finished result for a still-valid registry capability.
		return InitiationFinished
	}
	if tracked.initiation != initiation || tracked.token != initiation.token {
		return InitiationInvalid
	}
	if accepted {
		if tracked.record.Phase != PhaseCalling {
			return InitiationInvalid
		}
	} else if tracked.record.Phase != PhasePreparing && tracked.record.Phase != PhaseCalling {
		return InitiationInvalid
	}

	if tracked.terminalFinished {
		registry.removeOperationLocked(initiation.token.leaseUUID)
		return InitiationFinished
	}
	if tracked.claim.Valid() {
		// The synchronous call has returned, so the call barrier may become
		// active. The settlement capability still excludes close and timeout
		// until the callback releases or finishes it.
		tracked.record.Phase = PhaseActive
		tracked.initiation = Initiation{}
		registry.operations[initiation.token.leaseUUID] = tracked
		return InitiationSettling
	}
	if accepted {
		tracked.record.Phase = PhaseActive
		tracked.initiation = Initiation{}
		registry.operations[initiation.token.leaseUUID] = tracked
		return InitiationActivated
	}

	registry.removeOperationLocked(initiation.token.leaseUUID)
	return InitiationAborted
}

// Snapshot returns a causal operation boundary. Even a new registry with
// revision zero returns an explicitly valid snapshot.
func (registry *Registry) Snapshot() TrackerSnapshot {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.pruneMutationsLocked()
	return newTrackerSnapshot(registry.identity, registry.mutationRevision)
}

func (registry *Registry) pruneMutationsLocked() {
	for leaseUUID, revision := range registry.lastMutation {
		if revision <= registry.lastSnapshotRevision {
			delete(registry.lastMutation, leaseUUID)
		}
	}
	registry.lastSnapshotRevision = registry.mutationRevision
}

// TryClaimLease acquires an exclusive lease-action claim only when the lease
// has not mutated after snapshot.
func (registry *Registry) TryClaimLease(leaseUUID string, snapshot TrackerSnapshot) LeaseClaimResult {
	if leaseUUID == "" || !snapshot.Valid() || snapshot.registry != registry.identity {
		return LeaseClaimResult{outcome: LeaseClaimInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	// Snapshot tombstones older than the prior boundary may already have been
	// compacted. Consequently, a boundary remains consumable only while it is
	// the registry's latest causal boundary. A second Snapshot at the same
	// revision is equivalent; any later revision makes the old capability stale
	// globally before lease-local evidence is consulted.
	if snapshot.revision != registry.lastSnapshotRevision {
		return LeaseClaimResult{outcome: LeaseClaimSnapshotStale}
	}
	return registry.tryClaimLeaseLocked(leaseUUID, snapshot.revision, false)
}

// TryClaimLeaseNow is the event-path variant. Acquiring the claim itself marks
// a mutation so an older reconciliation snapshot remains fenced even if the
// claim is acquired and released between its inventory and action phases.
func (registry *Registry) TryClaimLeaseNow(leaseUUID string) LeaseClaimResult {
	if leaseUUID == "" {
		return LeaseClaimResult{outcome: LeaseClaimInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.tryClaimLeaseLocked(leaseUUID, 0, true)
}

func (registry *Registry) tryClaimLeaseLocked(
	leaseUUID string,
	maxRevision uint64,
	markAcquisition bool,
) LeaseClaimResult {
	if !markAcquisition && registry.lastMutation[leaseUUID] > maxRevision {
		return LeaseClaimResult{outcome: LeaseClaimSnapshotStale}
	}
	if _, exists := registry.operations[leaseUUID]; exists {
		return LeaseClaimResult{outcome: LeaseClaimBusy}
	}
	if _, exists := registry.leaseClaims[leaseUUID]; exists {
		return LeaseClaimResult{outcome: LeaseClaimBusy}
	}

	claim := newLeaseClaim(registry.identity, leaseUUID, registry.allocateClaimNonceLocked())
	registry.leaseClaims[leaseUUID] = claim
	if markAcquisition {
		registry.markMutationLocked(leaseUUID)
	}
	return LeaseClaimResult{claim: claim, outcome: LeaseClaimAcquired}
}

// ReleaseLease releases only the exact lease claim supplied by its owner.
func (registry *Registry) ReleaseLease(claim LeaseClaim) bool {
	if !claim.Valid() || claim.registry != registry.identity {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.leaseClaims[claim.leaseUUID] != claim {
		return false
	}
	delete(registry.leaseClaims, claim.leaseUUID)
	registry.markMutationLocked(claim.leaseUUID)
	return true
}

// Abort removes only token's still-unclaimed operation.
func (registry *Registry) Abort(token Token) bool {
	if !token.Valid() || token.registry != registry.identity {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[token.leaseUUID]
	if !exists || tracked.token != token || tracked.claim.Valid() {
		return false
	}
	registry.removeOperationLocked(token.leaseUUID)
	return true
}

// TryClaimCallback acquires terminal callback ownership for the exact
// operation ID. A callback may settle while the backend call is executing, but
// not while its callback URL and durable intent are still being prepared.
func (registry *Registry) TryClaimCallback(leaseUUID string, id OperationID) SettlementResult {
	return registry.tryClaimSettlement(leaseUUID, id, SettlementTerminal, settlementCallback)
}

// TryClaimTimeout acquires timeout ownership only after the synchronous
// backend call has returned accepted and the operation is active.
func (registry *Registry) TryClaimTimeout(leaseUUID string, id OperationID) SettlementResult {
	return registry.tryClaimSettlement(leaseUUID, id, SettlementTerminal, settlementTimeout)
}

// TryClaimDeprovision acquires deprovision ownership for the exact operation
// ID.
func (registry *Registry) TryClaimDeprovision(leaseUUID string, id OperationID) SettlementResult {
	return registry.tryClaimSettlement(leaseUUID, id, SettlementDeprovision, settlementDeprovision)
}

type settlementActor uint8

const (
	settlementActorInvalid settlementActor = iota
	settlementCallback
	settlementTimeout
	settlementDeprovision
)

func (registry *Registry) tryClaimSettlement(
	leaseUUID string,
	id OperationID,
	kind SettlementKind,
	actor settlementActor,
) SettlementResult {
	if leaseUUID == "" || !id.Valid() || !kind.validClaimKind() || actor == settlementActorInvalid {
		return SettlementResult{outcome: SettlementInvalid}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[leaseUUID]
	if !exists {
		return SettlementResult{outcome: SettlementNotFound}
	}
	if tracked.record.ID != id {
		return SettlementResult{outcome: SettlementOperationMismatch}
	}
	if tracked.terminalFinished {
		if actor == settlementCallback {
			return SettlementResult{outcome: SettlementNotFound}
		}
		return SettlementResult{outcome: SettlementBusy}
	}
	switch actor {
	case settlementCallback:
		if tracked.record.Phase == PhasePreparing {
			return SettlementResult{outcome: SettlementBusy}
		}
	case settlementTimeout, settlementDeprovision:
		if tracked.record.Phase != PhaseActive {
			return SettlementResult{outcome: SettlementBusy}
		}
	default:
		return SettlementResult{outcome: SettlementInvalid}
	}
	if tracked.claim.Valid() {
		return SettlementResult{outcome: SettlementBusy}
	}

	claim := newSettlementClaim(tracked.token, registry.allocateClaimNonceLocked(), kind)
	tracked.claim = claim
	tracked.record.Settlement = kind
	registry.operations[leaseUUID] = tracked
	return SettlementResult{
		record:  tracked.record.clone(),
		claim:   claim,
		outcome: SettlementClaimed,
	}
}

// ReleaseSettlement releases only the exact settlement claim. A claim released
// and reacquired for the same operation has a new nonce, so a stale owner cannot
// release the replacement claim.
func (registry *Registry) ReleaseSettlement(claim SettlementClaim) bool {
	if !claim.Valid() || claim.token.registry != registry.identity {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[claim.token.leaseUUID]
	if !exists || tracked.claim != claim {
		return false
	}
	tracked.claim = SettlementClaim{}
	tracked.record.Settlement = SettlementUnclaimed
	registry.operations[claim.token.leaseUUID] = tracked
	return true
}

// FinishSettlement removes only the operation owned by the exact settlement
// claim.
func (registry *Registry) FinishSettlement(claim SettlementClaim) bool {
	if !claim.Valid() || claim.token.registry != registry.identity {
		return false
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	tracked, exists := registry.operations[claim.token.leaseUUID]
	if !exists || tracked.claim != claim {
		return false
	}
	if tracked.record.Phase == PhaseCalling && claim.kind == SettlementTerminal {
		// An inline callback may finish while Provision/Restore is still on the
		// stack. Retain a claim-free terminal marker until Activate or
		// AbortInitiation observes the synchronous return; otherwise a racing close
		// could see no operation and deprovision before the call completes.
		tracked.claim = SettlementClaim{}
		tracked.terminalFinished = true
		tracked.record.Settlement = SettlementTerminal
		registry.operations[claim.token.leaseUUID] = tracked
		registry.markMutationLocked(claim.token.leaseUUID)
		return true
	}
	registry.removeOperationLocked(claim.token.leaseUUID)
	return true
}

// Lookup returns an immutable snapshot of the tracked operation.
func (registry *Registry) Lookup(leaseUUID string) (Record, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	tracked, exists := registry.operations[leaseUUID]
	if !exists {
		return Record{}, false
	}
	return tracked.record.clone(), true
}

// Contains reports whether leaseUUID has a tracked operation.
func (registry *Registry) Contains(leaseUUID string) bool {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	_, exists := registry.operations[leaseUUID]
	return exists
}

// Count returns the number of tracked operations.
func (registry *Registry) Count() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.operations)
}

// CountsByBackend returns a detached snapshot of operation counts per backend.
func (registry *Registry) CountsByBackend() map[string]int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	counts := make(map[string]int, len(registry.operations))
	for _, tracked := range registry.operations {
		counts[tracked.record.Backend]++
	}
	return counts
}

// LeaseUUIDs returns a detached snapshot of tracked lease UUIDs.
func (registry *Registry) LeaseUUIDs() []string {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return slices.Collect(maps.Keys(registry.operations))
}

// TimedOut returns detached snapshots of operations older than timeout.
func (registry *Registry) TimedOut(timeout time.Duration) []Record {
	now := time.Now()
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	var timedOut []Record
	for _, tracked := range registry.operations {
		if tracked.record.Phase == PhaseActive && now.Sub(tracked.record.StartedAt) > timeout {
			timedOut = append(timedOut, tracked.record.clone())
		}
	}
	return timedOut
}

func (registry *Registry) allocateOperationIDLocked() OperationID {
	if registry.operationIDSource == nil {
		return OperationID{}
	}
	// A collision with any ID issued by this process is an ABA risk: a delayed
	// signed callback could otherwise match an unrelated replacement operation.
	// Remember issued IDs for the process lifetime. Any source error, invalid
	// value, or collision fails the operation immediately; retrying could hide a
	// broken or adversarial entropy source.
	candidate, err := registry.operationIDSource()
	if err != nil || !candidate.Valid() {
		return OperationID{}
	}
	if _, exists := registry.issuedOperationIDs[candidate]; exists {
		return OperationID{}
	}
	registry.issuedOperationIDs[candidate] = struct{}{}
	return candidate
}

func (registry *Registry) allocateClaimNonceLocked() uint64 {
	registry.nextClaimNonce++
	if registry.nextClaimNonce == 0 {
		registry.nextClaimNonce++
	}
	return registry.nextClaimNonce
}

func (registry *Registry) markMutationLocked(leaseUUID string) {
	registry.mutationRevision++
	if registry.mutationRevision == 0 {
		registry.mutationRevision++
	}
	registry.lastMutation[leaseUUID] = registry.mutationRevision
}

func (registry *Registry) removeOperationLocked(leaseUUID string) {
	delete(registry.operations, leaseUUID)
	registry.markMutationLocked(leaseUUID)
	registry.notifyCountLocked()
}

func (registry *Registry) notifyCountLocked() {
	if registry.countObserver != nil {
		registry.countObserver(len(registry.operations))
	}
}
