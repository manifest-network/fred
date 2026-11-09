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

func TestCallbackSuccessExcludesStaleTimeoutButAllowsSettlement(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))
	require.Equal(t, InitiationActivated, registry.activate(initiation))
	candidates := authority.TimedOut(0)
	require.Len(t, candidates, 1)

	callback := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, callback.Claimed())
	require.True(t, authority.ObserveCallbackSuccess(callback.Claim()))
	require.True(t, authority.ObserveCallbackSuccess(callback.Claim()), "observation is idempotent")
	require.True(t, authority.ReleaseCallback(callback.Claim()))
	require.Empty(t, authority.TimedOut(0))
	require.Equal(t, SettlementBusy, authority.TryClaimTimeout(candidates[0]).Outcome())

	// Positive evidence only excludes timeout: a subsequent callback or an
	// explicit deprovision may still settle the operation.
	retry := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, retry.Claimed())
	require.True(t, authority.ReleaseCallback(retry.Claim()))
	deprovision := authority.TryClaimDeprovision("lease-1")
	require.True(t, deprovision.Claimed())
	require.True(t, authority.FinishDeprovision(deprovision.Claim()))

	// Success belongs to the operation, not the lease UUID forever.
	replacement := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(replacement))
	require.Equal(t, InitiationActivated, registry.activate(replacement))
	require.False(t, authority.ObserveCallbackSuccess(callback.Claim()))
	require.Equal(t, SettlementOperationMismatch, authority.TryClaimTimeout(candidates[0]).Outcome())
	candidates = authority.TimedOut(0)
	require.Len(t, candidates, 1)
	require.True(t, authority.TryClaimTimeout(candidates[0]).Claimed())
}

func TestCallbackSuccessRequiresExactLiveClaim(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	foreign := newTestRegistry()
	foreignAuthority, err := foreign.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))
	require.Equal(t, InitiationActivated, registry.activate(initiation))

	callback := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, callback.Claimed())
	require.False(t, authority.ObserveCallbackSuccess(CallbackClaim{}))
	require.False(t, foreignAuthority.ObserveCallbackSuccess(callback.Claim()))
	require.False(t, (SettlementAuthority{}).ObserveCallbackSuccess(callback.Claim()))
	require.True(t, authority.ReleaseCallback(callback.Claim()))
	retry := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, retry.Claimed())
	require.False(t, authority.ObserveCallbackSuccess(callback.Claim()), "a released nonce cannot change a reacquired claim")
	require.True(t, authority.ReleaseCallback(retry.Claim()))
	candidates := authority.TimedOut(0)
	require.Len(t, candidates, 1)
	require.True(t, authority.TryClaimTimeout(candidates[0]).Claimed())
}

func TestInlineCallbackSuccessSurvivesActivation(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))
	callback := authority.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, callback.Claimed())
	require.True(t, authority.ObserveCallbackSuccess(callback.Claim()))
	require.True(t, authority.ReleaseCallback(callback.Claim()))
	require.Equal(t, InitiationSettling, registry.activate(initiation))
	require.Empty(t, authority.TimedOut(0))
	// Exercise the live claim guard independently of candidate filtering.
	require.Equal(t, SettlementBusy, registry.tryClaimTimeout("lease-1", initiation.ID()).Outcome())
}

func TestInlineSuccessCannotBeErasedByAbortOrSameOperationRecovery(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	spec := testTrackSpec("lease-1")
	lease := authority.TryClaimLeaseNow(spec.LeaseUUID)
	require.True(t, lease.Acquired())
	initiated := registry.tryInitiateClaimed(lease.Claim(), spec.operationSpec())
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	dispatch, joined := authority.JoinDispatch(initiation)
	require.True(t, joined)
	require.False(t, authority.CallbackOwnsDispatch(dispatch))
	require.True(t, authority.BeginDispatchCall(dispatch))

	callback := authority.TryClaimCallback(spec.LeaseUUID, initiation.ID())
	require.True(t, callback.Claimed())
	require.True(t, authority.ObserveCallbackSuccess(callback.Claim()))
	require.True(t, authority.ReleaseCallback(callback.Claim()))
	require.True(t, authority.CallbackOwnsDispatch(dispatch))
	require.False(t, authority.CallbackOwnsDispatch(DispatchOperation{}))
	foreign, err := newTestRegistry().BindSettlementAuthority()
	require.NoError(t, err)
	require.False(t, foreign.CallbackOwnsDispatch(dispatch))

	// Abort consumes the call barrier, but success keeps this exact record
	// active. Even the original lease capability cannot overwrite it.
	require.Equal(t, InitiationSettling, authority.AbortDispatch(dispatch))
	require.Equal(t, RecoveryBusy, authority.RecoverClaimed(lease.Claim(), initiation.ID(), spec.recovered()))
	metadata, exists := authority.Lookup(spec.LeaseUUID)
	require.True(t, exists)
	require.Equal(t, initiation.ID(), metadata.ID())
	require.Equal(t, PhaseActive, metadata.Phase())
	require.Empty(t, authority.TimedOut(0))
	require.Equal(t, SettlementBusy, registry.tryClaimTimeout(spec.LeaseUUID, initiation.ID()).Outcome())
	require.True(t, authority.ReleaseLease(lease.Claim()))
	require.Equal(t, LeaseClaimBusy, authority.TryClaimLeaseNow(spec.LeaseUUID).Outcome(),
		"ordinary recovery must defer to the still-tracked operation")

	replay := authority.TryClaimCallback(spec.LeaseUUID, initiation.ID())
	require.True(t, replay.Claimed())
	require.True(t, authority.FinishCallback(replay.Claim()))
	// A later operation for the same lease cannot inherit the success.
	replacement := requireInitiated(t, registry, spec)
	require.False(t, authority.CallbackOwnsDispatch(dispatch))
	require.True(t, registry.beginCall(replacement))
	require.Equal(t, InitiationActivated, registry.activate(replacement))
	require.Len(t, authority.TimedOut(0), 1)
}
