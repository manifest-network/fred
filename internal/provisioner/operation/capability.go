package operation

// registryMarker is deliberately non-zero-sized. Go permits pointers to
// distinct zero-sized variables to compare equal, which would weaken the
// process-local capability boundary.
type registryMarker struct{ issued byte }

// registryIdentity distinguishes capabilities issued by different in-process
// registries. Pointer identity is unique by construction for the lifetime of
// each registry and needs neither randomness nor a collision-prone fallback.
// Keeping the marker and constructor private prevents callers from minting
// capabilities outside the registry.
type registryIdentity struct {
	marker *registryMarker
}

func newRegistryIdentity() registryIdentity {
	return registryIdentity{marker: &registryMarker{issued: 1}}
}

func (id registryIdentity) valid() bool {
	return id.marker != nil
}

// TrackerSnapshot is a causal boundary issued by an operation registry. A
// registry accepts it only when it was issued by that same registry.
//
// The zero value is invalid. A freshly constructed registry still issues an
// explicitly valid snapshot whose internal mutation revision may be zero.
type TrackerSnapshot struct {
	registry registryIdentity
	revision uint64
}

// Valid reports whether the snapshot was explicitly issued by a registry.
func (snapshot TrackerSnapshot) Valid() bool {
	return snapshot.registry.valid()
}

func newTrackerSnapshot(registry registryIdentity, revision uint64) TrackerSnapshot {
	if !registry.valid() {
		return TrackerSnapshot{}
	}
	return TrackerSnapshot{registry: registry, revision: revision}
}

// ReconciliationBoundary is one atomic view of the Registry at the beginning
// of an inventory sweep. It keeps the causal mutation revision and the set of
// operations that were active at that revision inseparable. Callers can ask
// observational questions, but only the Registry authority that minted the
// boundary can consume it to acquire a lease claim.
//
// The zero value is invalid. Its private maps are detached at construction and
// cannot be spliced with a snapshot from another Registry or sweep.
type ReconciliationBoundary struct {
	snapshot TrackerSnapshot
	inFlight map[string]struct{}
}

// Valid reports whether the boundary was atomically issued by a Registry.
func (boundary ReconciliationBoundary) Valid() bool {
	return boundary.snapshot.Valid() && boundary.inFlight != nil
}

// WasInFlight reports whether leaseUUID had a tracked operation at the exact
// boundary. It is observational only and grants no claim or settlement power.
func (boundary ReconciliationBoundary) WasInFlight(leaseUUID string) bool {
	if !boundary.Valid() || leaseUUID == "" {
		return false
	}
	_, present := boundary.inFlight[leaseUUID]
	return present
}

func newReconciliationBoundary(
	registry registryIdentity,
	revision uint64,
	inFlight map[string]struct{},
) ReconciliationBoundary {
	if !registry.valid() || inFlight == nil {
		return ReconciliationBoundary{}
	}
	return ReconciliationBoundary{
		snapshot: newTrackerSnapshot(registry, revision),
		inFlight: inFlight,
	}
}

// operationToken is the registry-private identity shared by its public,
// purpose-specific initiation and settlement capabilities. Keeping this token
// private prevents callers from bypassing the phase-aware transition APIs.
type operationToken struct {
	registry  registryIdentity
	leaseUUID string
	id        OperationID
}

func (token operationToken) valid() bool {
	return token.registry.valid() && token.leaseUUID != "" && token.id.Valid()
}

func (token operationToken) operationID() OperationID {
	if !token.valid() {
		return OperationID{}
	}
	return token.id
}

func newOperationToken(registry registryIdentity, leaseUUID string, id OperationID) operationToken {
	token := operationToken{registry: registry, leaseUUID: leaseUUID, id: id}
	if !token.valid() {
		return operationToken{}
	}
	return token
}

// Initiation is the exclusive capability to advance one newly registered
// backend operation from local preparation through the synchronous call
// boundary. It deliberately exposes only the wire identity: callers cannot
// obtain the underlying registry token and bypass phase-aware completion.
// The zero value is invalid.
type Initiation struct {
	token operationToken
}

// Valid reports whether this capability was issued for a tracked operation.
func (initiation Initiation) Valid() bool {
	return initiation.token.valid()
}

// ID returns the operation identity carried in the callback URL and durable
// placement attempt. An invalid initiation returns an invalid OperationID.
func (initiation Initiation) ID() OperationID {
	if !initiation.Valid() {
		return OperationID{}
	}
	return initiation.token.operationID()
}

func newInitiation(token operationToken) Initiation {
	initiation := Initiation{token: token}
	if !initiation.Valid() {
		return Initiation{}
	}
	return initiation
}

// LeaseClaim is the exclusive capability to perform a non-overlapping
// lifecycle action for one lease. Its private nonce distinguishes consecutive
// claims for the same lease, preventing a stale release from releasing a newer
// claim. The zero value is invalid.
type LeaseClaim struct {
	registry  registryIdentity
	leaseUUID string
	nonce     uint64
}

// Valid reports whether claim was explicitly issued by a registry.
func (claim LeaseClaim) Valid() bool {
	return claim.registry.valid() && claim.leaseUUID != "" && claim.nonce != 0
}

func newLeaseClaim(registry registryIdentity, leaseUUID string, nonce uint64) LeaseClaim {
	claim := LeaseClaim{registry: registry, leaseUUID: leaseUUID, nonce: nonce}
	if !claim.Valid() {
		return LeaseClaim{}
	}
	return claim
}

// SettlementKind describes the actor that owns terminal work for a tracked
// operation. It is observable on Record snapshots; mutation still requires the
// corresponding opaque SettlementClaim.
type SettlementKind uint8

const (
	SettlementUnclaimed SettlementKind = iota
	SettlementTerminal
	SettlementDeprovision
)

func (kind SettlementKind) validClaimKind() bool {
	switch kind {
	case SettlementTerminal, SettlementDeprovision:
		return true
	case SettlementUnclaimed:
		return false
	default:
		return false
	}
}

// SettlementClaim is the exclusive capability to settle one tracked
// operation. It binds the operation token, settlement purpose, and a fresh
// claim nonce. The nonce prevents a stale actor from finishing a claim that was
// released and subsequently reacquired for the same operation. The zero value
// is invalid.
type SettlementClaim struct {
	token operationToken
	nonce uint64
	kind  SettlementKind
	actor settlementActor
}

// Valid reports whether claim was explicitly issued by a registry.
func (claim SettlementClaim) Valid() bool {
	return claim.token.valid() && claim.nonce != 0 && claim.kind.validClaimKind() &&
		claim.actor.valid() &&
		((claim.actor == settlementDeprovision) == (claim.kind == SettlementDeprovision))
}

func newSettlementClaim(
	token operationToken,
	nonce uint64,
	kind SettlementKind,
	actor settlementActor,
) SettlementClaim {
	claim := SettlementClaim{token: token, nonce: nonce, kind: kind, actor: actor}
	if !claim.Valid() {
		return SettlementClaim{}
	}
	return claim
}
