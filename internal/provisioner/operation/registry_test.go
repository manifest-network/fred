package operation

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func newTestRegistry() *Registry {
	return newRegistry(11, 100, 200)
}

func testTrackSpec(leaseUUID string) TrackSpec {
	return TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Items: []backend.LeaseItem{
			{SKU: "sku-a", Quantity: 1},
		},
		Backend: "backend-a",
		Kind:    KindProvision,
	}
}

func requireStarted(t *testing.T, registry *Registry, spec TrackSpec) Token {
	t.Helper()
	result := registry.TryTrack(spec)
	require.Equal(t, TrackStarted, result.Outcome())
	require.True(t, result.Started())
	require.True(t, result.Token().Valid())
	return result.Token()
}

func TestRegistryZeroResultsAreConservative(t *testing.T) {
	var track TrackResult
	assert.Equal(t, TrackInvalid, track.Outcome())
	assert.False(t, track.Started())
	assert.False(t, track.Token().Valid())

	var initiation InitiationResult
	assert.Equal(t, TrackInvalid, initiation.Outcome())
	assert.False(t, initiation.Started())
	assert.False(t, initiation.Capability().Valid())

	var lease LeaseClaimResult
	assert.Equal(t, LeaseClaimInvalid, lease.Outcome())
	assert.False(t, lease.Acquired())
	assert.False(t, lease.Claim().Valid())

	var settlement SettlementResult
	assert.Equal(t, SettlementInvalid, settlement.Outcome())
	assert.False(t, settlement.Claimed())
	assert.False(t, settlement.Record().Valid())
	assert.False(t, settlement.Claim().Valid())

	assert.False(t, (TrackSpec{}).Valid())
	assert.False(t, (TrackSpec{LeaseUUID: "lease-1", Kind: KindInvalid}).Valid())
	assert.False(t, (Record{}).Valid())
	assert.False(t, (Initiation{}).Valid())
	assert.False(t, (Initiation{}).ID().Valid())
	assert.False(t, Kind(255).valid())
	assert.False(t, Phase(255).valid())
	assert.Empty(t, (Record{}).RoutingSKU())
}

func requireInitiated(t *testing.T, registry *Registry, spec TrackSpec) Initiation {
	t.Helper()
	result := registry.TryInitiate(spec)
	require.Equal(t, TrackStarted, result.Outcome())
	require.True(t, result.Started())
	require.True(t, result.Capability().Valid())
	return result.Capability()
}

func TestRegistryInitiationPhasesGateLifecycleActors(t *testing.T) {
	registry := newTestRegistry()
	spec := testTrackSpec("lease-1")
	initiation := requireInitiated(t, registry, spec)

	record, exists := registry.Lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhasePreparing, record.Phase)
	assert.Equal(t, SettlementBusy,
		registry.TryClaimCallback(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.TryClaimTimeout(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.TryClaimDeprovision(spec.LeaseUUID, initiation.ID()).Outcome())

	require.True(t, registry.BeginCall(initiation))
	assert.False(t, registry.BeginCall(initiation), "the call boundary is one-shot")
	record, exists = registry.Lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhaseCalling, record.Phase)
	assert.Equal(t, SettlementBusy,
		registry.TryClaimTimeout(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.TryClaimDeprovision(spec.LeaseUUID, initiation.ID()).Outcome())

	callback := registry.TryClaimCallback(spec.LeaseUUID, initiation.ID())
	require.True(t, callback.Claimed(), "an inline terminal callback may settle during the call")
	assert.Equal(t, InitiationSettling, registry.Activate(initiation))
	record, exists = registry.Lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhaseActive, record.Phase)
	assert.True(t, registry.FinishSettlement(callback.Claim()))
	assert.False(t, registry.Contains(spec.LeaseUUID))
}

func TestRegistryBindBackendIsExactPreparingAndOneShot(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(12, 100, 200)
	spec := testTrackSpec("lease-restore")
	spec.Backend = ""
	initiation := requireInitiated(t, registry, spec)
	foreignInitiation := requireInitiated(t, foreign, spec)

	assert.False(t, registry.BeginCall(initiation),
		"an unbound operation cannot cross the backend call boundary")
	assert.False(t, registry.BindBackend(Initiation{}, "backend-a"))
	assert.False(t, registry.BindBackend(foreignInitiation, "backend-a"))
	assert.False(t, registry.BindBackend(initiation, ""))
	assert.True(t, registry.BindBackend(initiation, "backend-a"))
	assert.False(t, registry.BindBackend(initiation, "backend-a"),
		"binding is one-shot even for the same backend")
	assert.False(t, registry.BindBackend(initiation, "backend-b"),
		"a preparing operation cannot be rebound to another backend")

	record, exists := registry.Lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, "backend-a", record.Backend)
	assert.Equal(t, PhasePreparing, record.Phase)
	require.True(t, registry.BeginCall(initiation))
	assert.False(t, registry.BindBackend(initiation, "backend-b"),
		"calling operations are immutable")
	assert.Equal(t, InitiationAborted, registry.AbortInitiation(initiation))
	assert.False(t, registry.BindBackend(initiation, "backend-a"),
		"a capability is stale after its operation is removed")
}

func TestRegistryInlineCallbackRetainsCallBarrierUntilInitiatorReturns(t *testing.T) {
	registry := newTestRegistry()
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.BeginCall(initiation))

	callback := registry.TryClaimCallback("lease-1", initiation.ID())
	require.True(t, callback.Claimed())
	require.True(t, registry.FinishSettlement(callback.Claim()))

	record, exists := registry.Lookup("lease-1")
	require.True(t, exists, "terminal settlement must retain the synchronous call barrier")
	assert.Equal(t, PhaseCalling, record.Phase)
	assert.Equal(t, SettlementTerminal, record.Settlement)
	assert.Equal(t, SettlementNotFound,
		registry.TryClaimCallback("lease-1", initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.TryClaimTimeout("lease-1", initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.TryClaimDeprovision("lease-1", initiation.ID()).Outcome())

	assert.Equal(t, InitiationFinished, registry.Activate(initiation))
	assert.False(t, registry.Contains("lease-1"))
}

func TestRegistryAbortInitiationIsExactAndPhaseAware(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(12, 100, 200)
	preparing := requireInitiated(t, registry, testTrackSpec("lease-preparing"))
	foreignInitiation := requireInitiated(t, foreign, testTrackSpec("lease-preparing"))

	assert.Equal(t, InitiationInvalid, registry.AbortInitiation(Initiation{}))
	assert.Equal(t, InitiationInvalid, registry.AbortInitiation(foreignInitiation))
	assert.Equal(t, InitiationAborted, registry.AbortInitiation(preparing))
	assert.Equal(t, InitiationFinished, registry.AbortInitiation(preparing),
		"completion is idempotent when the exact operation is already gone")

	calling := requireInitiated(t, registry, testTrackSpec("lease-calling"))
	require.True(t, registry.BeginCall(calling))
	assert.Equal(t, InitiationAborted, registry.AbortInitiation(calling))
	assert.False(t, registry.Contains("lease-calling"))
}

func TestRegistryTimedOutExcludesPreparingAndCalling(t *testing.T) {
	registry := newTestRegistry()
	old := testTrackSpec("lease-1")
	old.StartedAt = time.Now().Add(-time.Hour)
	initiation := requireInitiated(t, registry, old)
	assert.Empty(t, registry.TimedOut(time.Minute))

	require.True(t, registry.BeginCall(initiation))
	assert.Empty(t, registry.TimedOut(time.Minute))
	assert.Equal(t, InitiationActivated, registry.Activate(initiation))
	require.Len(t, registry.TimedOut(time.Minute), 1)
}

func TestRegistrySnapshotIsExplicitlyValidAtRevisionZero(t *testing.T) {
	registry := newTestRegistry()
	snapshot := registry.Snapshot()

	assert.True(t, snapshot.Valid())
	assert.Zero(t, snapshot.revision)
	assert.True(t, NewRegistry().Snapshot().Valid())
	assert.True(t, NewRegistryWithCountObserver(nil).Snapshot().Valid())
	assert.True(t, newRegistry(0, 0, 0).Snapshot().Valid())
}

func TestRegistryTryTrackRecordsDetachedOperation(t *testing.T) {
	registry := newTestRegistry()
	startedAt := time.Now().Add(-time.Minute).Round(0)
	spec := testTrackSpec("lease-1")
	spec.StartedAt = startedAt

	result := registry.TryTrack(spec)
	require.True(t, result.Started())
	assert.Equal(t, TrackStarted, result.Outcome())
	id := result.Token().ID()
	assert.Equal(t, deterministicOperationID(101), id)

	// The registry owns a clone, not the caller's mutable slice.
	spec.Items[0].SKU = "mutated-input"
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	require.True(t, record.Valid())
	assert.Equal(t, "lease-1", record.LeaseUUID)
	assert.Equal(t, "tenant-a", record.Tenant)
	assert.Equal(t, "backend-a", record.Backend)
	assert.Equal(t, "sku-a", record.Items[0].SKU)
	assert.Equal(t, "sku-a", record.RoutingSKU())
	assert.Equal(t, id, record.ID)
	assert.Equal(t, startedAt, record.StartedAt)
	assert.Equal(t, KindProvision, record.Kind)
	assert.Equal(t, SettlementUnclaimed, record.Settlement)

	// Lookup also returns a clone.
	record.Items[0].SKU = "mutated-output"
	again, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, "sku-a", again.Items[0].SKU)

	assert.True(t, registry.Contains("lease-1"))
	assert.Equal(t, 1, registry.Count())
}

func TestRegistryTryTrackDefaultsStartTimeAndRejectsInvalidOrBusy(t *testing.T) {
	registry := newTestRegistry()

	invalid := registry.TryTrack(TrackSpec{})
	assert.Equal(t, TrackInvalid, invalid.Outcome())
	assert.False(t, invalid.Started())

	before := time.Now()
	requireStarted(t, registry, testTrackSpec("lease-1"))
	after := time.Now()
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.False(t, record.StartedAt.Before(before))
	assert.False(t, record.StartedAt.After(after))

	busy := registry.TryTrack(testTrackSpec("lease-1"))
	assert.Equal(t, TrackBusy, busy.Outcome())
	assert.False(t, busy.Started())
	assert.False(t, busy.Token().Valid())
}

func TestRegistryTryTrackConcurrentHasSingleWinner(t *testing.T) {
	registry := newTestRegistry()
	const workers = 100
	var started atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if registry.TryTrack(testTrackSpec("lease-1")).Started() {
				started.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), started.Load())
	assert.Equal(t, 1, registry.Count())
}

func TestRegistryConcurrentLeaseClaimHasSingleWinner(t *testing.T) {
	registry := newTestRegistry()
	const workers = 100
	var acquired atomic.Int32
	var winningClaim LeaseClaim
	var winnerMu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result := registry.TryClaimLeaseNow("lease-1")
			if result.Acquired() {
				acquired.Add(1)
				winnerMu.Lock()
				winningClaim = result.Claim()
				winnerMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), acquired.Load())
	require.True(t, winningClaim.Valid())
	assert.True(t, registry.ReleaseLease(winningClaim))
}

func TestRegistryConcurrentSettlementHasSingleWinner(t *testing.T) {
	registry := newTestRegistry()
	token := requireStarted(t, registry, testTrackSpec("lease-1"))
	const workers = 100
	var acquired atomic.Int32
	var winningClaim SettlementClaim
	var winnerMu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result := registry.TryClaimCallback("lease-1", token.ID())
			if result.Claimed() {
				acquired.Add(1)
				winnerMu.Lock()
				winningClaim = result.Claim()
				winnerMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), acquired.Load())
	require.True(t, winningClaim.Valid())
	assert.True(t, registry.FinishSettlement(winningClaim))
	assert.Zero(t, registry.Count())
}

func TestRegistryDeterministicTestIDsRemainUniqueAcrossSequenceWrap(t *testing.T) {
	registry := newRegistry(11, math.MaxUint64-1, 200)
	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	second := requireStarted(t, registry, testTrackSpec("lease-2"))

	assert.Equal(t, deterministicOperationID(math.MaxUint64), first.ID())
	assert.Equal(t, deterministicOperationID(1), second.ID())
	assert.NotEqual(t, first.ID(), second.ID())
}

func TestRegistryProductionOperationIDsAreIndependentUUIDv4Values(t *testing.T) {
	registry := NewRegistry()
	const operations = 64
	ids := make(map[OperationID]struct{}, operations)
	for index := range operations {
		spec := testTrackSpec(fmt.Sprintf("lease-%d", index))
		spec.Backend = fmt.Sprintf("backend-%d", index%2)
		token := requireStarted(t, registry, spec)
		parsed, err := uuid.Parse(token.ID().String())
		require.NoError(t, err)
		assert.Equal(t, uuid.Version(4), parsed.Version())
		assert.Equal(t, token.ID().String(), parsed.String(), "wire form must be canonical")
		_, duplicate := ids[token.ID()]
		assert.False(t, duplicate, "independent operation IDs must not collide")
		ids[token.ID()] = struct{}{}
	}
	assert.Len(t, ids, operations)
}

func TestRegistryOperationIDAllocationRejectsReuseAndFailsClosed(t *testing.T) {
	t.Run("collision fails without installing or consuming another candidate", func(t *testing.T) {
		registry := newTestRegistry()
		candidates := []OperationID{
			deterministicOperationID(501),
			deterministicOperationID(501),
			deterministicOperationID(777),
		}
		registry.operationIDSource = func() (OperationID, error) {
			candidate := candidates[0]
			candidates = candidates[1:]
			return candidate, nil
		}

		first := requireStarted(t, registry, testTrackSpec("lease-a"))
		require.True(t, registry.Abort(first))
		second := registry.TryTrack(testTrackSpec("lease-b"))

		assert.Equal(t, deterministicOperationID(501), first.ID())
		assert.Equal(t, TrackInvalid, second.Outcome())
		assert.False(t, second.Started())
		assert.Len(t, candidates, 1, "collision must not be hidden by retrying the source")
		assert.Len(t, registry.issuedOperationIDs, 1)
		assert.Zero(t, registry.Count())
	})

	t.Run("entropy error installs no operation", func(t *testing.T) {
		registry := newTestRegistry()
		registry.operationIDSource = func() (OperationID, error) {
			return OperationID{}, errors.New("entropy unavailable")
		}

		tracked := registry.TryTrack(testTrackSpec("lease-a"))
		assert.Equal(t, TrackInvalid, tracked.Outcome())
		assert.False(t, tracked.Started())
		assert.Zero(t, registry.Count())

		initiated := registry.TryInitiate(testTrackSpec("lease-b"))
		assert.Equal(t, TrackInvalid, initiated.Outcome())
		assert.False(t, initiated.Started())
		assert.Zero(t, registry.Count())
	})

	t.Run("invalid source value installs no operation", func(t *testing.T) {
		registry := newTestRegistry()
		registry.operationIDSource = func() (OperationID, error) {
			return OperationID{}, nil
		}

		result := registry.TryTrack(testTrackSpec("lease-a"))
		assert.Equal(t, TrackInvalid, result.Outcome())
		assert.False(t, result.Started())
		assert.Zero(t, registry.Count())
	})
}

func TestRegistryLeaseClaimFencesSnapshotAndRequiresExactCapability(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(12, 100, 200)
	snapshot := registry.Snapshot()

	invalidLease := registry.TryClaimLease("", snapshot)
	assert.Equal(t, LeaseClaimInvalid, invalidLease.Outcome())
	assert.Equal(t, LeaseClaimInvalid, registry.TryClaimLeaseNow("").Outcome())
	foreignSnapshot := registry.TryClaimLease("lease-1", foreign.Snapshot())
	assert.Equal(t, LeaseClaimInvalid, foreignSnapshot.Outcome())

	result := registry.TryClaimLease("lease-1", snapshot)
	require.True(t, result.Acquired())
	claim := result.Claim()
	assert.True(t, claim.Valid())

	busy := registry.TryClaimLease("lease-1", snapshot)
	assert.Equal(t, LeaseClaimBusy, busy.Outcome())
	assert.Equal(t, TrackBusy, registry.TryTrack(testTrackSpec("lease-1")).Outcome())

	wrongLeaseSpec := testTrackSpec("lease-2")
	assert.Equal(t, TrackInvalid, registry.TryTrackClaimed(claim, wrongLeaseSpec).Outcome())
	assert.Equal(t, TrackInvalid, foreign.TryTrackClaimed(claim, testTrackSpec("lease-1")).Outcome())

	tracked := registry.TryTrackClaimed(claim, testTrackSpec("lease-1"))
	require.True(t, tracked.Started())
	assert.Equal(t, TrackBusy,
		registry.TryTrackClaimed(claim, testTrackSpec("lease-1")).Outcome())
	assert.True(t, registry.ReleaseLease(claim))
	assert.False(t, registry.ReleaseLease(claim))
	assert.False(t, registry.ReleaseLease(foreign.TryClaimLeaseNow("foreign").Claim()))
}

func TestRegistryLeaseClaimIsBusyWhenOperationAlreadyExists(t *testing.T) {
	registry := newTestRegistry()
	requireStarted(t, registry, testTrackSpec("lease-1"))
	snapshot := registry.Snapshot()

	result := registry.TryClaimLease("lease-1", snapshot)
	assert.Equal(t, LeaseClaimBusy, result.Outcome())
	assert.False(t, result.Acquired())
}

func TestRegistryLeaseClaimNoncePreventsStaleReleaseABA(t *testing.T) {
	registry := newTestRegistry()
	first := registry.TryClaimLeaseNow("lease-1")
	require.True(t, first.Acquired())
	require.True(t, registry.ReleaseLease(first.Claim()))

	second := registry.TryClaimLeaseNow("lease-1")
	require.True(t, second.Acquired())
	assert.NotEqual(t, first.Claim(), second.Claim())
	assert.False(t, registry.ReleaseLease(first.Claim()),
		"a stale capability must not release a reacquired lease claim")
	assert.True(t, registry.ReleaseLease(second.Claim()))
}

func TestRegistrySnapshotFencesCompletedAndStraddlingActions(t *testing.T) {
	t.Run("completed after snapshot", func(t *testing.T) {
		registry := newTestRegistry()
		snapshot := registry.Snapshot()
		claim := registry.TryClaimLeaseNow("lease-1")
		require.True(t, claim.Acquired())
		require.True(t, registry.ReleaseLease(claim.Claim()))

		result := registry.TryClaimLease("lease-1", snapshot)
		assert.Equal(t, LeaseClaimSnapshotStale, result.Outcome())
	})

	t.Run("straddles snapshot", func(t *testing.T) {
		registry := newTestRegistry()
		claim := registry.TryClaimLeaseNow("lease-1")
		require.True(t, claim.Acquired())
		snapshot := registry.Snapshot()
		require.True(t, registry.ReleaseLease(claim.Claim()))

		result := registry.TryClaimLease("lease-1", snapshot)
		assert.Equal(t, LeaseClaimSnapshotStale, result.Outcome())
	})
}

func TestRegistrySnapshotPrunesOnlyMutationsOlderThanPriorBoundary(t *testing.T) {
	registry := newTestRegistry()
	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.Abort(first))
	firstBoundary := registry.Snapshot()
	assert.Contains(t, registry.lastMutation, "lease-1")

	second := requireStarted(t, registry, testTrackSpec("lease-2"))
	require.True(t, registry.Abort(second))
	secondBoundary := registry.Snapshot()
	assert.NotContains(t, registry.lastMutation, "lease-1")
	assert.Contains(t, registry.lastMutation, "lease-2")

	assert.True(t, firstBoundary.Valid())
	assert.True(t, secondBoundary.Valid())
	assert.Greater(t, secondBoundary.revision, firstBoundary.revision)

	// lease-1's tombstone was compacted when secondBoundary was issued. The old
	// capability must therefore be rejected as a whole; treating the missing
	// tombstone as "unchanged" would let stale inventory authorize work after a
	// completed operation (an ABA across snapshot compaction).
	stale := registry.TryClaimLease("lease-1", firstBoundary)
	assert.Equal(t, LeaseClaimSnapshotStale, stale.Outcome())
	assert.False(t, stale.Acquired())

	current := registry.TryClaimLease("lease-1", secondBoundary)
	require.True(t, current.Acquired())
	assert.True(t, registry.ReleaseLease(current.Claim()))
}

func TestRegistryEquivalentSnapshotAtSameRevisionRemainsConsumable(t *testing.T) {
	registry := newTestRegistry()
	first := registry.Snapshot()
	second := registry.Snapshot()
	require.Equal(t, first.revision, second.revision)

	claim := registry.TryClaimLease("lease-1", first)
	require.True(t, claim.Acquired())
	assert.True(t, registry.ReleaseLease(claim.Claim()))
}

func TestRegistryAbortRequiresExactUnclaimedToken(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(12, 100, 200)
	first := requireStarted(t, registry, testTrackSpec("lease-1"))

	assert.False(t, registry.Abort(Token{}))
	foreignToken := requireStarted(t, foreign, testTrackSpec("lease-1"))
	assert.False(t, registry.Abort(foreignToken))

	settlement := registry.TryClaimCallback("lease-1", first.ID())
	require.True(t, settlement.Claimed())
	assert.False(t, registry.Abort(first), "a settlement claim owns terminal cleanup")
	require.True(t, registry.ReleaseSettlement(settlement.Claim()))
	require.True(t, registry.Abort(first))

	second := requireStarted(t, registry, testTrackSpec("lease-1"))
	assert.NotEqual(t, first, second)
	assert.False(t, registry.Abort(first), "a stale token must not remove its replacement")
	assert.True(t, registry.Contains("lease-1"))
}

func TestRegistrySettlementOutcomesAndRecordState(t *testing.T) {
	registry := newTestRegistry()
	token := requireStarted(t, registry, testTrackSpec("lease-1"))
	otherID := deterministicOperationID(999)

	assert.Equal(t, SettlementInvalid,
		registry.TryClaimCallback("", token.ID()).Outcome())
	assert.Equal(t, SettlementInvalid,
		registry.TryClaimCallback("lease-1", OperationID{}).Outcome())
	assert.Equal(t, SettlementNotFound,
		registry.TryClaimCallback("missing", token.ID()).Outcome())
	assert.Equal(t, SettlementOperationMismatch,
		registry.TryClaimCallback("lease-1", otherID).Outcome())

	result := registry.TryClaimCallback("lease-1", token.ID())
	require.True(t, result.Claimed())
	assert.Equal(t, SettlementClaimed, result.Outcome())
	assert.Equal(t, SettlementTerminal, result.Record().Settlement)
	assert.True(t, result.Claim().Valid())

	busy := registry.TryClaimDeprovision("lease-1", token.ID())
	assert.Equal(t, SettlementBusy, busy.Outcome())
	assert.False(t, busy.Claimed())

	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, SettlementTerminal, record.Settlement)
	assert.True(t, registry.ReleaseSettlement(result.Claim()))
	record, exists = registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, SettlementUnclaimed, record.Settlement)
	_, exists = registry.Lookup("missing")
	assert.False(t, exists)
}

func TestRegistrySettlementClaimNoncePreventsReleaseAndFinishABA(t *testing.T) {
	registry := newTestRegistry()
	token := requireStarted(t, registry, testTrackSpec("lease-1"))

	first := registry.TryClaimCallback("lease-1", token.ID())
	require.True(t, first.Claimed())
	require.True(t, registry.ReleaseSettlement(first.Claim()))

	second := registry.TryClaimDeprovision("lease-1", token.ID())
	require.True(t, second.Claimed())
	assert.Equal(t, SettlementDeprovision, second.Record().Settlement)
	assert.NotEqual(t, first.Claim(), second.Claim())
	assert.False(t, registry.ReleaseSettlement(first.Claim()))
	assert.False(t, registry.FinishSettlement(first.Claim()))
	assert.True(t, registry.FinishSettlement(second.Claim()))
	assert.False(t, registry.Contains("lease-1"))
	assert.False(t, registry.FinishSettlement(second.Claim()))
}

func TestRegistrySettlementRejectsForeignClaim(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(12, 100, 200)
	requireStarted(t, registry, testTrackSpec("lease-1"))
	foreignToken := requireStarted(t, foreign, testTrackSpec("lease-1"))
	foreignClaim := foreign.TryClaimCallback("lease-1", foreignToken.ID())
	require.True(t, foreignClaim.Claimed())

	assert.False(t, registry.ReleaseSettlement(foreignClaim.Claim()))
	assert.False(t, registry.FinishSettlement(foreignClaim.Claim()))
	assert.True(t, registry.Contains("lease-1"))
}

func TestRegistryObserverSnapshotsAreDetached(t *testing.T) {
	registry := newTestRegistry()
	first := testTrackSpec("lease-1")
	first.Backend = "backend-a"
	second := testTrackSpec("lease-2")
	second.Backend = "backend-a"
	third := testTrackSpec("lease-3")
	third.Backend = "backend-b"
	requireStarted(t, registry, first)
	requireStarted(t, registry, second)
	requireStarted(t, registry, third)

	counts := registry.CountsByBackend()
	assert.Equal(t, map[string]int{"backend-a": 2, "backend-b": 1}, counts)
	counts["backend-a"] = 99
	assert.Equal(t, 2, registry.CountsByBackend()["backend-a"])

	leases := registry.LeaseUUIDs()
	sort.Strings(leases)
	assert.Equal(t, []string{"lease-1", "lease-2", "lease-3"}, leases)
	leassesCopy := append([]string(nil), leases...)
	leassesCopy[0] = "mutated"
	fresh := registry.LeaseUUIDs()
	sort.Strings(fresh)
	assert.Equal(t, leases, fresh)
}

func TestRegistryCountObserverSeesTypedMutationsInOrder(t *testing.T) {
	var observed []int
	registry := newRegistryWithObserver(11, 100, 200, func(count int) {
		observed = append(observed, count)
	})

	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	second := requireStarted(t, registry, testTrackSpec("lease-2"))
	assert.False(t, registry.TryTrack(testTrackSpec("lease-2")).Started())
	require.True(t, registry.Abort(first))
	require.True(t, registry.Abort(second))
	replacement := requireStarted(t, registry, testTrackSpec("lease-2"))
	require.True(t, registry.Abort(replacement))

	assert.Equal(t, []int{1, 2, 1, 0, 1, 0}, observed)
}

func TestRegistryTimedOutReturnsDetachedRecords(t *testing.T) {
	registry := newTestRegistry()
	old := testTrackSpec("lease-old")
	old.StartedAt = time.Now().Add(-10 * time.Minute)
	fresh := testTrackSpec("lease-fresh")
	fresh.StartedAt = time.Now()
	requireStarted(t, registry, old)
	requireStarted(t, registry, fresh)

	timedOut := registry.TimedOut(5 * time.Minute)
	require.Len(t, timedOut, 1)
	assert.Equal(t, "lease-old", timedOut[0].LeaseUUID)
	timedOut[0].Items[0].SKU = "mutated"
	record, exists := registry.Lookup("lease-old")
	require.True(t, exists)
	assert.Equal(t, "sku-a", record.Items[0].SKU)
	assert.Equal(t, 2, registry.Count())
}

func TestRegistryClaimNonceAndMutationRevisionSkipZeroOnWrap(t *testing.T) {
	registry := newRegistry(11, 100, math.MaxUint64)
	claim := registry.TryClaimLeaseNow("lease-1")
	require.True(t, claim.Acquired())
	assert.Equal(t, uint64(1), claim.Claim().nonce)

	registry.mutationRevision = math.MaxUint64
	require.True(t, registry.ReleaseLease(claim.Claim()))
	assert.Equal(t, uint64(1), registry.mutationRevision)
	assert.Equal(t, uint64(1), registry.lastMutation["lease-1"])
}
