package operation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSettlementAuthorityBindsRegistryOnceAndRejectsForeignClaims(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	_, err = registry.BindSettlementAuthority()
	require.Error(t, err)

	foreign := newTestRegistry()
	foreignAuthority, err := foreign.BindSettlementAuthority()
	require.NoError(t, err)

	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))
	claimed := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, claimed.Claimed())
	require.False(t, foreignAuthority.ReleaseCallback(claimed.Claim()))
	require.True(t, authority.ReleaseCallback(claimed.Claim()))
}

func TestSettlementAuthorityCallbackMetadataAndInlineFinishRemainExact(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))

	result := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, result.Claimed())
	claim := result.Claim()
	metadata := claim.Metadata()
	require.Equal(t, "lease-1", metadata.LeaseUUID())
	require.Equal(t, "backend-a", metadata.Backend())
	require.Equal(t, initiation.ID(), metadata.ID())
	require.Equal(t, PhaseCalling, metadata.Phase())
	require.Equal(t, SettlementTerminal, metadata.Settlement())

	require.True(t, authority.FinishCallback(claim))
	require.Equal(t, InitiationFinished, registry.activate(initiation))
	require.False(t, authority.RuntimeController().Contains("lease-1"))
}

func TestSettlementAuthorityTimeoutCandidateCannotCrossRegistry(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testOperationSpec{
		LeaseUUID: "lease-1", Tenant: "tenant-a",
		Items: validInitiationItems(), Backend: "backend-a", Kind: KindProvision,
		StartedAt: time.Now().Add(-time.Hour),
	})
	require.True(t, registry.beginCall(initiation))
	require.Equal(t, InitiationActivated, registry.activate(initiation))
	candidates := authority.TimedOut(time.Minute)
	require.Len(t, candidates, 1)

	foreign := newTestRegistry()
	foreignAuthority, err := foreign.BindSettlementAuthority()
	require.NoError(t, err)
	require.Equal(t, SettlementInvalid, foreignAuthority.TryClaimTimeout(candidates[0]).Outcome())
	claimed := authority.TryClaimTimeout(candidates[0])
	require.True(t, claimed.Claimed())
	require.True(t, authority.ReleaseTimeout(claimed.Claim()))
}

func TestSettlementAuthorityDeprovisionDerivesOperationIdentity(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))
	require.Equal(t, InitiationActivated, registry.activate(initiation))

	claimed := authority.TryClaimDeprovision("lease-1")
	require.True(t, claimed.Claimed())
	require.Equal(t, initiation.ID(), claimed.Claim().Metadata().ID())
	require.True(t, authority.ReleaseDeprovision(claimed.Claim()))
}
