package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilityZeroValuesAreInvalid(t *testing.T) {
	assert.Equal(t, registryIdentity{}, newRegistryIdentity(0))
	assert.False(t, (TrackerSnapshot{}).Valid())
	assert.False(t, (Token{}).Valid())
	assert.False(t, (Token{}).ID().Valid())
	assert.False(t, (LeaseClaim{}).Valid())
	assert.False(t, (SettlementClaim{}).Valid())
}

func TestTrackerSnapshotRequiresExplicitRegistryIdentity(t *testing.T) {
	registry := newRegistryIdentity(11)

	initial := newTrackerSnapshot(registry, 0)
	assert.True(t, initial.Valid(), "revision zero is valid only when explicitly issued")
	assert.Equal(t, uint64(0), initial.revision)

	later := newTrackerSnapshot(registry, 42)
	assert.True(t, later.Valid())
	assert.Equal(t, uint64(42), later.revision)

	invalid := newTrackerSnapshot(registryIdentity{}, 42)
	assert.False(t, invalid.Valid())
}

func TestTokenRequiresCompleteIssuanceSpec(t *testing.T) {
	registry := newRegistryIdentity(11)
	id := deterministicOperationID(42)

	token := newToken(registry, "lease-1", id)
	require.True(t, token.Valid())
	assert.Equal(t, id, token.ID())

	tests := []struct {
		name     string
		registry registryIdentity
		lease    string
		id       OperationID
	}{
		{name: "invalid registry", registry: registryIdentity{}, lease: "lease-1", id: id},
		{name: "empty lease", registry: registry, lease: "", id: id},
		{name: "invalid operation ID", registry: registry, lease: "lease-1", id: OperationID{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newToken(tt.registry, tt.lease, tt.id)
			assert.False(t, got.Valid())
			assert.False(t, got.ID().Valid())
		})
	}
}

func TestLeaseClaimRequiresRegistryLeaseAndNonce(t *testing.T) {
	registry := newRegistryIdentity(11)

	claim := newLeaseClaim(registry, "lease-1", 9)
	assert.True(t, claim.Valid())

	tests := []struct {
		name     string
		registry registryIdentity
		lease    string
		nonce    uint64
	}{
		{name: "invalid registry", registry: registryIdentity{}, lease: "lease-1", nonce: 9},
		{name: "empty lease", registry: registry, lease: "", nonce: 9},
		{name: "zero nonce", registry: registry, lease: "lease-1", nonce: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, newLeaseClaim(tt.registry, tt.lease, tt.nonce).Valid())
		})
	}
}

func TestSettlementClaimRequiresTokenNonceAndPurpose(t *testing.T) {
	registry := newRegistryIdentity(11)
	token := newToken(registry, "lease-1", deterministicOperationID(42))

	for _, kind := range []SettlementKind{
		SettlementTerminal,
		SettlementDeprovision,
	} {
		claim := newSettlementClaim(token, 9, kind)
		assert.True(t, claim.Valid())
	}

	tests := []struct {
		name  string
		token Token
		nonce uint64
		kind  SettlementKind
	}{
		{name: "invalid token", token: Token{}, nonce: 9, kind: SettlementTerminal},
		{name: "zero nonce", token: token, nonce: 0, kind: SettlementTerminal},
		{name: "unclaimed kind", token: token, nonce: 9, kind: SettlementUnclaimed},
		{name: "unknown kind", token: token, nonce: 9, kind: SettlementKind(255)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, newSettlementClaim(tt.token, tt.nonce, tt.kind).Valid())
		})
	}
}

func TestCapabilitiesAreBoundToRegistryAndNonce(t *testing.T) {
	registryA := newRegistryIdentity(1)
	registryB := newRegistryIdentity(2)
	id := deterministicOperationID(42)

	tokenA := newToken(registryA, "lease-1", id)
	tokenB := newToken(registryB, "lease-1", id)
	assert.NotEqual(t, tokenA, tokenB)

	leaseClaimA := newLeaseClaim(registryA, "lease-1", 1)
	leaseClaimB := newLeaseClaim(registryA, "lease-1", 2)
	assert.NotEqual(t, leaseClaimA, leaseClaimB)

	settlementClaimA := newSettlementClaim(tokenA, 1, SettlementTerminal)
	settlementClaimB := newSettlementClaim(tokenA, 2, SettlementTerminal)
	assert.NotEqual(t, settlementClaimA, settlementClaimB)

	terminalClaim := newSettlementClaim(tokenA, 1, SettlementTerminal)
	deprovisionClaim := newSettlementClaim(tokenA, 1, SettlementDeprovision)
	assert.NotEqual(t, terminalClaim, deprovisionClaim)
}

func TestCapabilitiesAreComparable(t *testing.T) {
	registry := newRegistryIdentity(1)
	snapshot := newTrackerSnapshot(registry, 0)
	token := newToken(registry, "lease-1", deterministicOperationID(1))
	leaseClaim := newLeaseClaim(registry, "lease-1", 1)
	settlementClaim := newSettlementClaim(token, 1, SettlementTerminal)

	assert.Contains(t, map[TrackerSnapshot]struct{}{snapshot: {}}, snapshot)
	assert.Contains(t, map[Token]struct{}{token: {}}, token)
	assert.Contains(t, map[LeaseClaim]struct{}{leaseClaim: {}}, leaseClaim)
	assert.Contains(t, map[SettlementClaim]struct{}{settlementClaim: {}}, settlementClaim)
}
