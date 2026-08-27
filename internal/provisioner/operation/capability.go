package operation

// registryIdentity distinguishes capabilities issued by different in-process
// registries. A registry generates one non-zero identity at construction time.
// Keeping the type and constructor private prevents callers from minting
// capabilities outside the registry.
type registryIdentity struct {
	value uint64
}

func newRegistryIdentity(value uint64) registryIdentity {
	if value == 0 {
		return registryIdentity{}
	}
	return registryIdentity{value: value}
}

func (id registryIdentity) valid() bool {
	return id.value != 0
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

// Token is the capability returned when a registry starts tracking an
// operation. It binds the operation identity to both its lease and the issuing
// registry, so conditional cleanup cannot accidentally target another lease or
// another registry. The zero value is invalid.
type Token struct {
	registry  registryIdentity
	leaseUUID string
	id        OperationID
}

// Valid reports whether token was issued for a non-empty lease and valid
// operation identity.
func (token Token) Valid() bool {
	return token.registry.valid() && token.leaseUUID != "" && token.id.Valid()
}

// ID returns the operation identity carried to the backend wire. An invalid
// token returns an invalid OperationID.
func (token Token) ID() OperationID {
	if !token.Valid() {
		return OperationID{}
	}
	return token.id
}

func newToken(registry registryIdentity, leaseUUID string, id OperationID) Token {
	token := Token{registry: registry, leaseUUID: leaseUUID, id: id}
	if !token.Valid() {
		return Token{}
	}
	return token
}

// Initiation is the exclusive capability to advance one newly registered
// backend operation from local preparation through the synchronous call
// boundary. It deliberately exposes only the wire identity: callers cannot
// obtain the underlying Token and bypass phase-aware completion with Abort.
// The zero value is invalid.
type Initiation struct {
	token Token
}

// Valid reports whether this capability was issued for a tracked operation.
func (initiation Initiation) Valid() bool {
	return initiation.token.Valid()
}

// ID returns the operation identity carried in the callback URL and durable
// placement attempt. An invalid initiation returns an invalid OperationID.
func (initiation Initiation) ID() OperationID {
	if !initiation.Valid() {
		return OperationID{}
	}
	return initiation.token.ID()
}

func newInitiation(token Token) Initiation {
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
	token Token
	nonce uint64
	kind  SettlementKind
}

// Valid reports whether claim was explicitly issued by a registry.
func (claim SettlementClaim) Valid() bool {
	return claim.token.Valid() && claim.nonce != 0 && claim.kind.validClaimKind()
}

func newSettlementClaim(token Token, nonce uint64, kind SettlementKind) SettlementClaim {
	claim := SettlementClaim{token: token, nonce: nonce, kind: kind}
	if !claim.Valid() {
		return SettlementClaim{}
	}
	return claim
}
