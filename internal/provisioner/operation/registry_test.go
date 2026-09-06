package operation

import (
	"context"
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
	return newRegistry(100, 200)
}

// testOperationSpec is a deliberately weak test-only fixture used to exercise the
// Registry's innermost fail-closed checks. Production has no corresponding
// public bag of fields; callers must use the purpose-specific constructors.
type testOperationSpec struct {
	LeaseUUID string
	Tenant    string
	Items     []backend.LeaseItem
	Backend   string
	StartedAt time.Time
	Kind      Kind
}

func (spec testOperationSpec) Valid() bool { return spec.operationSpec().valid() }

func (spec testOperationSpec) operationSpec() operationSpec {
	return operationSpec{
		leaseUUID: spec.LeaseUUID,
		tenant:    spec.Tenant,
		items:     spec.Items,
		backend:   spec.Backend,
		startedAt: spec.StartedAt,
		kind:      spec.Kind,
	}
}

func (spec testOperationSpec) recovered() RecoveredOperation {
	return RecoveredOperation{spec: spec.operationSpec()}
}

func testTrackSpec(leaseUUID string) testOperationSpec {
	return testOperationSpec{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Items: []backend.LeaseItem{
			{SKU: "sku-a", Quantity: 1},
		},
		Backend: "backend-a",
		Kind:    KindProvision,
	}
}

// tryInitiateForTest follows the same claim-before-initiation protocol as every
// production caller while preserving the compact result assertions in this
// package's Registry unit tests.
func tryInitiateForTest(registry *Registry, spec testOperationSpec) InitiationResult {
	claimResult := registry.tryClaimLeaseNow(spec.LeaseUUID)
	if !claimResult.Acquired() {
		if claimResult.Outcome() == LeaseClaimBusy {
			return InitiationResult{outcome: TrackBusy}
		}
		return InitiationResult{outcome: TrackInvalid}
	}
	claim := claimResult.Claim()
	defer registry.releaseLease(claim)
	return registry.tryInitiateClaimed(claim, spec.operationSpec())
}

func requireStarted(t *testing.T, registry *Registry, spec testOperationSpec) operationToken {
	t.Helper()
	initiation := requireInitiated(t, registry, spec)
	require.True(t, registry.beginCall(initiation))
	require.Equal(t, InitiationActivated, registry.activate(initiation))
	return initiation.token
}

func requireSettled(t *testing.T, registry *Registry, token operationToken) {
	t.Helper()
	result := registry.tryClaimCallback(token.leaseUUID, token.operationID())
	require.True(t, result.Claimed())
	require.True(t, registry.finishSettlement(result.Claim()))
}

func TestRegistryZeroResultsAreConservative(t *testing.T) {
	var recovery RecoveryResult
	assert.Equal(t, RecoveryInvalid, recovery)
	assert.False(t, recovery.Recovered())
	assert.Equal(t, "invalid", recovery.String())

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

	assert.False(t, (testOperationSpec{}).Valid())
	assert.False(t, (testOperationSpec{LeaseUUID: "lease-1", Kind: KindInvalid}).Valid())
	assert.False(t, (Record{}).Valid())
	assert.False(t, (Initiation{}).Valid())
	assert.False(t, (Initiation{}).ID().Valid())
	assert.False(t, Kind(255).valid())
	assert.False(t, Phase(255).valid())
	assert.Empty(t, (Record{}).RoutingSKU())
}

func TestKindDurableEncodingIsClosedAndStrict(t *testing.T) {
	for _, test := range []struct {
		kind Kind
		wire string
	}{
		{kind: KindProvision, wire: "provision"},
		{kind: KindRestore, wire: "restore"},
	} {
		assert.True(t, test.kind.Valid())
		assert.Equal(t, test.wire, test.kind.String())
		parsed, err := ParseKind(test.wire)
		require.NoError(t, err)
		assert.Equal(t, test.kind, parsed)
	}
	for _, wire := range []string{"", "Provision", "restore ", "restart", "invalid"} {
		parsed, err := ParseKind(wire)
		require.ErrorIs(t, err, ErrInvalidKind)
		assert.Equal(t, KindInvalid, parsed)
	}
	assert.False(t, KindInvalid.Valid())
	assert.False(t, Kind(255).Valid())
}

func requireInitiated(t *testing.T, registry *Registry, spec testOperationSpec) Initiation {
	t.Helper()
	result := tryInitiateForTest(registry, spec)
	require.Equal(t, TrackStarted, result.Outcome())
	require.True(t, result.Started())
	require.True(t, result.Capability().Valid())
	return result.Capability()
}

func TestRegistryInitiationPhasesGateLifecycleActors(t *testing.T) {
	registry := newTestRegistry()
	spec := testTrackSpec("lease-1")
	initiation := requireInitiated(t, registry, spec)

	record, exists := registry.lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhasePreparing, record.Phase)
	assert.Equal(t, SettlementBusy,
		registry.tryClaimCallback(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.tryClaimTimeout(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.tryClaimDeprovision(spec.LeaseUUID, initiation.ID()).Outcome())

	require.True(t, registry.beginCall(initiation))
	assert.False(t, registry.beginCall(initiation), "the call boundary is one-shot")
	record, exists = registry.lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhaseCalling, record.Phase)
	assert.Equal(t, SettlementBusy,
		registry.tryClaimTimeout(spec.LeaseUUID, initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.tryClaimDeprovision(spec.LeaseUUID, initiation.ID()).Outcome())

	callback := registry.tryClaimCallback(spec.LeaseUUID, initiation.ID())
	require.True(t, callback.Claimed(), "an inline terminal callback may settle during the call")
	assert.Equal(t, InitiationSettling, registry.activate(initiation))
	record, exists = registry.lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, PhaseActive, record.Phase)
	assert.True(t, registry.finishSettlement(callback.Claim()))
	assert.False(t, registry.contains(spec.LeaseUUID))
}

func TestRegistryBindBackendIsExactPreparingAndOneShot(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(100, 200)
	spec := testTrackSpec("lease-restore")
	spec.Backend = ""
	spec.Kind = KindRestore
	initiation := requireInitiated(t, registry, spec)
	foreignInitiation := requireInitiated(t, foreign, spec)

	assert.False(t, registry.beginCall(initiation),
		"an unbound operation cannot cross the backend call boundary")
	assert.False(t, registry.bindBackend(Initiation{}, "backend-a"))
	assert.False(t, registry.bindBackend(foreignInitiation, "backend-a"))
	assert.False(t, registry.bindBackend(initiation, ""))
	assert.True(t, registry.bindBackend(initiation, "backend-a"))
	assert.False(t, registry.bindBackend(initiation, "backend-a"),
		"binding is one-shot even for the same backend")
	assert.False(t, registry.bindBackend(initiation, "backend-b"),
		"a preparing operation cannot be rebound to another backend")

	record, exists := registry.lookup(spec.LeaseUUID)
	require.True(t, exists)
	assert.Equal(t, "backend-a", record.Backend)
	assert.Equal(t, PhasePreparing, record.Phase)
	require.True(t, registry.beginCall(initiation))
	assert.False(t, registry.bindBackend(initiation, "backend-b"),
		"calling operations are immutable")
	assert.Equal(t, InitiationAborted, registry.abortInitiation(initiation))
	assert.False(t, registry.bindBackend(initiation, "backend-a"),
		"a capability is stale after its operation is removed")
}

func TestRegistryInlineCallbackRetainsCallBarrierUntilInitiatorReturns(t *testing.T) {
	registry := newTestRegistry()
	initiation := requireInitiated(t, registry, testTrackSpec("lease-1"))
	require.True(t, registry.beginCall(initiation))

	callback := registry.tryClaimCallback("lease-1", initiation.ID())
	require.True(t, callback.Claimed())
	require.True(t, registry.finishSettlement(callback.Claim()))

	record, exists := registry.lookup("lease-1")
	require.True(t, exists, "terminal settlement must retain the synchronous call barrier")
	assert.Equal(t, PhaseCalling, record.Phase)
	assert.Equal(t, SettlementTerminal, record.Settlement)
	assert.Equal(t, SettlementNotFound,
		registry.tryClaimCallback("lease-1", initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.tryClaimTimeout("lease-1", initiation.ID()).Outcome())
	assert.Equal(t, SettlementBusy,
		registry.tryClaimDeprovision("lease-1", initiation.ID()).Outcome())

	assert.Equal(t, InitiationFinished, registry.activate(initiation))
	assert.False(t, registry.contains("lease-1"))
}

func TestRegistryAbortInitiationIsExactAndPhaseAware(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(100, 200)
	preparing := requireInitiated(t, registry, testTrackSpec("lease-preparing"))
	foreignInitiation := requireInitiated(t, foreign, testTrackSpec("lease-preparing"))

	assert.Equal(t, InitiationInvalid, registry.abortInitiation(Initiation{}))
	assert.Equal(t, InitiationInvalid, registry.abortInitiation(foreignInitiation))
	assert.Equal(t, InitiationAborted, registry.abortInitiation(preparing))
	assert.Equal(t, InitiationFinished, registry.abortInitiation(preparing),
		"completion is idempotent when the exact operation is already gone")

	calling := requireInitiated(t, registry, testTrackSpec("lease-calling"))
	require.True(t, registry.beginCall(calling))
	assert.Equal(t, InitiationAborted, registry.abortInitiation(calling))
	assert.False(t, registry.contains("lease-calling"))
}

func TestRegistryTimedOutExcludesPreparingAndCalling(t *testing.T) {
	registry := newTestRegistry()
	old := testTrackSpec("lease-1")
	old.StartedAt = time.Now().Add(-time.Hour)
	initiation := requireInitiated(t, registry, old)
	assert.Empty(t, registry.timedOut(time.Minute))

	require.True(t, registry.beginCall(initiation))
	assert.Empty(t, registry.timedOut(time.Minute))
	assert.Equal(t, InitiationActivated, registry.activate(initiation))
	require.Len(t, registry.timedOut(time.Minute), 1)
}

func TestRegistrySnapshotIsExplicitlyValidAtRevisionZero(t *testing.T) {
	registry := newTestRegistry()
	snapshot := registry.snapshot()

	assert.True(t, snapshot.Valid())
	assert.Zero(t, snapshot.revision)
	assert.True(t, NewRegistry().snapshot().Valid())
	assert.True(t, NewRegistryWithCountObserver(nil).snapshot().Valid())
	assert.True(t, newRegistry(0, 0).snapshot().Valid())
}

func TestRegistryActiveOperationSnapshotsAreDetached(t *testing.T) {
	registry := newTestRegistry()
	startedAt := time.Now().Add(-time.Minute).Round(0)
	spec := testTrackSpec("lease-1")
	spec.StartedAt = startedAt

	token := requireStarted(t, registry, spec)
	id := token.operationID()
	assert.Equal(t, deterministicOperationID(101), id)

	// The registry owns a clone, not the caller's mutable slice.
	spec.Items[0].SKU = "mutated-input"
	record, exists := registry.lookup("lease-1")
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
	again, exists := registry.lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, "sku-a", again.Items[0].SKU)

	assert.True(t, registry.contains("lease-1"))
	assert.Equal(t, 1, registry.count())
}

func TestRegistryTryInitiateDefaultsStartTimeAndRejectsInvalidOrBusy(t *testing.T) {
	registry := newTestRegistry()

	invalid := tryInitiateForTest(registry, testOperationSpec{})
	assert.Equal(t, TrackInvalid, invalid.Outcome())
	assert.False(t, invalid.Started())

	before := time.Now()
	requireStarted(t, registry, testTrackSpec("lease-1"))
	after := time.Now()
	record, exists := registry.lookup("lease-1")
	require.True(t, exists)
	assert.False(t, record.StartedAt.Before(before))
	assert.False(t, record.StartedAt.After(after))

	busy := tryInitiateForTest(registry, testTrackSpec("lease-1"))
	assert.Equal(t, TrackBusy, busy.Outcome())
	assert.False(t, busy.Started())
	assert.False(t, busy.Capability().Valid())
}

func TestRegistryTryInitiateConcurrentHasSingleWinner(t *testing.T) {
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
			if tryInitiateForTest(registry, testTrackSpec("lease-1")).Started() {
				started.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), started.Load())
	assert.Equal(t, 1, registry.count())
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
			result := registry.tryClaimLeaseNow("lease-1")
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
	assert.True(t, registry.releaseLease(winningClaim))
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
			result := registry.tryClaimCallback("lease-1", token.operationID())
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
	assert.True(t, registry.finishSettlement(winningClaim))
	assert.Zero(t, registry.count())
}

func TestRegistryDeterministicTestIDsFailClosedAtSequenceExhaustion(t *testing.T) {
	registry := newRegistry(math.MaxUint64-1, 200)
	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	assert.Equal(t, deterministicOperationID(math.MaxUint64), first.operationID())
	requireSettled(t, registry, first)

	for _, leaseUUID := range []string{"lease-2", "lease-3"} {
		result := tryInitiateForTest(registry, testTrackSpec(leaseUUID))
		assert.Equal(t, TrackInvalid, result.Outcome())
		assert.False(t, result.Started())
	}
	assert.Equal(t, uint64(math.MaxUint64), registry.nextOperationID,
		"exhaustion must never wrap or reuse the deterministic sequence")
	assert.Zero(t, registry.count())
}

func TestRegistryProductionOperationIDsHaveCanonicalUUIDv4WireShape(t *testing.T) {
	registry := NewRegistry()
	for index := range 8 {
		spec := testTrackSpec(fmt.Sprintf("lease-%d", index))
		spec.Backend = fmt.Sprintf("backend-%d", index%2)
		token := requireStarted(t, registry, spec)
		parsed, err := uuid.Parse(token.operationID().String())
		require.NoError(t, err)
		assert.Equal(t, uuid.Version(4), parsed.Version())
		assert.Equal(t, token.operationID().String(), parsed.String(), "wire form must be canonical")
	}
}

func TestRegistryRecoverClaimedInstallsExactActiveOperation(t *testing.T) {
	registry := newRegistry(0, 0)
	claimResult := registry.tryClaimLeaseNow("lease-1")
	require.True(t, claimResult.Acquired())
	claim := claimResult.Claim()
	id := deterministicOperationID(909)
	spec := testOperationSpec{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
		Items:     []backend.LeaseItem{{SKU: "sku-1", Quantity: 2}},
		Backend:   "backend-a",
		StartedAt: time.Unix(123, 0),
		Kind:      KindRestore,
	}

	recovered := registry.recoverClaimed(claim, id, spec.recovered())
	require.True(t, recovered.Recovered())
	assert.Equal(t, RecoveryInstalled, recovered)
	assert.Equal(t, RecoveryBusy, registry.recoverClaimed(claim, id, spec.recovered()),
		"the held lease claim must not replace the operation it just recovered")
	record, exists := registry.lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, id, record.ID)
	assert.Equal(t, PhaseActive, record.Phase)
	assert.Equal(t, KindRestore, record.Kind)
	assert.Equal(t, spec.Backend, record.Backend)
	assert.Equal(t, spec.Tenant, record.Tenant)
	assert.Equal(t, spec.Items, record.Items)
	assert.True(t, registry.releaseLease(claim),
		"the caller retains exclusion until durable promotion has completed")

	callback := registry.tryClaimCallback("lease-1", id)
	require.True(t, callback.Claimed())
	assert.True(t, registry.finishSettlement(callback.Claim()))
}

func TestRegistryRecoverClaimedFailsClosed(t *testing.T) {
	registry := newRegistry(0, 0)
	claimResult := registry.tryClaimLeaseNow("lease-1")
	require.True(t, claimResult.Acquired())
	claim := claimResult.Claim()
	validID := deterministicOperationID(910)
	validSpec := testOperationSpec{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
		Items:     validInitiationItems(),
		Backend:   "backend-a",
		Kind:      KindProvision,
	}

	tests := []struct {
		name  string
		claim LeaseClaim
		id    OperationID
		spec  testOperationSpec
	}{
		{name: "zero claim", id: validID, spec: validSpec},
		{name: "zero ID", claim: claim, spec: validSpec},
		{name: "empty tenant", claim: claim, id: validID, spec: testOperationSpec{
			LeaseUUID: "lease-1", Items: validInitiationItems(),
			Backend: "backend-a", Kind: KindProvision,
		}},
		{name: "empty backend", claim: claim, id: validID, spec: testOperationSpec{
			LeaseUUID: "lease-1", Tenant: "tenant-1",
			Items: validInitiationItems(), Kind: KindProvision,
		}},
		{name: "different lease", claim: claim, id: validID, spec: testOperationSpec{
			LeaseUUID: "lease-2", Tenant: "tenant-1", Items: validInitiationItems(),
			Backend: "backend-a", Kind: KindProvision,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, RecoveryInvalid,
				registry.recoverClaimed(test.claim, test.id, test.spec.recovered()))
		})
	}
	assert.False(t, registry.contains("lease-1"))
	assert.True(t, registry.releaseLease(claim))
}

func TestRegistryOperationIDAllocationFailsClosed(t *testing.T) {
	t.Run("entropy error installs no operation", func(t *testing.T) {
		registry := newTestRegistry()
		registry.operationIDSource = func() (OperationID, error) {
			return OperationID{}, errors.New("entropy unavailable")
		}

		initiated := tryInitiateForTest(registry, testTrackSpec("lease-a"))
		assert.Equal(t, TrackInvalid, initiated.Outcome())
		assert.False(t, initiated.Started())
		assert.Zero(t, registry.count())
	})

	t.Run("invalid source value installs no operation", func(t *testing.T) {
		registry := newTestRegistry()
		registry.operationIDSource = func() (OperationID, error) {
			return OperationID{}, nil
		}

		result := tryInitiateForTest(registry, testTrackSpec("lease-a"))
		assert.Equal(t, TrackInvalid, result.Outcome())
		assert.False(t, result.Started())
		assert.Zero(t, registry.count())
	})
}

func TestRegistryLeaseClaimFencesSnapshotAndRequiresExactCapability(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(100, 200)
	snapshot := registry.snapshot()

	invalidLease := registry.tryClaimLease("", snapshot)
	assert.Equal(t, LeaseClaimInvalid, invalidLease.Outcome())
	assert.Equal(t, LeaseClaimInvalid, registry.tryClaimLeaseNow("").Outcome())
	foreignSnapshot := registry.tryClaimLease("lease-1", foreign.snapshot())
	assert.Equal(t, LeaseClaimInvalid, foreignSnapshot.Outcome())

	result := registry.tryClaimLease("lease-1", snapshot)
	require.True(t, result.Acquired())
	claim := result.Claim()
	assert.True(t, claim.Valid())

	busy := registry.tryClaimLease("lease-1", snapshot)
	assert.Equal(t, LeaseClaimBusy, busy.Outcome())
	assert.Equal(t, TrackBusy, tryInitiateForTest(registry, testTrackSpec("lease-1")).Outcome())

	wrongLeaseSpec := testTrackSpec("lease-2")
	assert.Equal(t, TrackInvalid,
		registry.tryInitiateClaimed(claim, wrongLeaseSpec.operationSpec()).Outcome())
	assert.Equal(t, TrackInvalid,
		foreign.tryInitiateClaimed(claim, testTrackSpec("lease-1").operationSpec()).Outcome())

	tracked := registry.tryInitiateClaimed(claim, testTrackSpec("lease-1").operationSpec())
	require.True(t, tracked.Started())
	assert.Equal(t, TrackBusy,
		registry.tryInitiateClaimed(claim, testTrackSpec("lease-1").operationSpec()).Outcome())
	assert.True(t, registry.releaseLease(claim))
	assert.False(t, registry.releaseLease(claim))
	assert.False(t, registry.releaseLease(foreign.tryClaimLeaseNow("foreign").Claim()))
}

func TestRegistryLeaseClaimIsBusyWhenOperationAlreadyExists(t *testing.T) {
	registry := newTestRegistry()
	requireStarted(t, registry, testTrackSpec("lease-1"))
	snapshot := registry.snapshot()

	result := registry.tryClaimLease("lease-1", snapshot)
	assert.Equal(t, LeaseClaimBusy, result.Outcome())
	assert.False(t, result.Acquired())
}

func TestRegistryLeaseClaimNoncePreventsStaleReleaseABA(t *testing.T) {
	registry := newTestRegistry()
	first := registry.tryClaimLeaseNow("lease-1")
	require.True(t, first.Acquired())
	require.True(t, registry.releaseLease(first.Claim()))

	second := registry.tryClaimLeaseNow("lease-1")
	require.True(t, second.Acquired())
	assert.NotEqual(t, first.Claim(), second.Claim())
	assert.False(t, registry.releaseLease(first.Claim()),
		"a stale capability must not release a reacquired lease claim")
	assert.True(t, registry.releaseLease(second.Claim()))
}

func TestRegistrySnapshotFencesCompletedAndStraddlingActions(t *testing.T) {
	t.Run("completed after snapshot", func(t *testing.T) {
		registry := newTestRegistry()
		snapshot := registry.snapshot()
		claim := registry.tryClaimLeaseNow("lease-1")
		require.True(t, claim.Acquired())
		require.True(t, registry.releaseLease(claim.Claim()))

		result := registry.tryClaimLease("lease-1", snapshot)
		assert.Equal(t, LeaseClaimSnapshotStale, result.Outcome())
	})

	t.Run("straddles snapshot", func(t *testing.T) {
		registry := newTestRegistry()
		claim := registry.tryClaimLeaseNow("lease-1")
		require.True(t, claim.Acquired())
		snapshot := registry.snapshot()
		require.True(t, registry.releaseLease(claim.Claim()))

		result := registry.tryClaimLease("lease-1", snapshot)
		assert.Equal(t, LeaseClaimSnapshotStale, result.Outcome())
	})
}

func TestReconciliationBoundaryIncludesActiveLeaseClaims(t *testing.T) {
	registry := newTestRegistry()
	claim := registry.tryClaimLeaseNow("lease-claimed")
	require.True(t, claim.Acquired())
	requireStarted(t, registry, testTrackSpec("lease-operation"))

	boundary := registry.captureReconciliationBoundary()
	require.True(t, boundary.Valid())
	assert.True(t, boundary.WasInFlight("lease-claimed"))
	assert.True(t, boundary.WasInFlight("lease-operation"))
	assert.False(t, boundary.WasInFlight("lease-idle"))

	require.True(t, registry.releaseLease(claim.Claim()))
}

func TestRegistrySnapshotPrunesOnlyMutationsOlderThanPriorBoundary(t *testing.T) {
	registry := newTestRegistry()
	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	requireSettled(t, registry, first)
	firstBoundary := registry.snapshot()
	assert.Contains(t, registry.lastMutation, "lease-1")

	second := requireStarted(t, registry, testTrackSpec("lease-2"))
	requireSettled(t, registry, second)
	secondBoundary := registry.snapshot()
	assert.NotContains(t, registry.lastMutation, "lease-1")
	assert.Contains(t, registry.lastMutation, "lease-2")

	assert.True(t, firstBoundary.Valid())
	assert.True(t, secondBoundary.Valid())
	assert.Greater(t, secondBoundary.revision, firstBoundary.revision)

	// lease-1's tombstone was compacted when secondBoundary was issued. The old
	// capability must therefore be rejected as a whole; treating the missing
	// tombstone as "unchanged" would let stale inventory authorize work after a
	// completed operation (an ABA across snapshot compaction).
	stale := registry.tryClaimLease("lease-1", firstBoundary)
	assert.Equal(t, LeaseClaimSnapshotStale, stale.Outcome())
	assert.False(t, stale.Acquired())

	current := registry.tryClaimLease("lease-1", secondBoundary)
	require.True(t, current.Acquired())
	assert.True(t, registry.releaseLease(current.Claim()))
}

func TestRegistryEquivalentSnapshotAtSameRevisionRemainsConsumable(t *testing.T) {
	registry := newTestRegistry()
	first := registry.snapshot()
	second := registry.snapshot()
	require.Equal(t, first.revision, second.revision)

	claim := registry.tryClaimLease("lease-1", first)
	require.True(t, claim.Acquired())
	assert.True(t, registry.releaseLease(claim.Claim()))
}

func TestRegistrySettlementOutcomesAndRecordState(t *testing.T) {
	registry := newTestRegistry()
	token := requireStarted(t, registry, testTrackSpec("lease-1"))
	otherID := deterministicOperationID(999)

	assert.Equal(t, SettlementInvalid,
		registry.tryClaimCallback("", token.operationID()).Outcome())
	assert.Equal(t, SettlementInvalid,
		registry.tryClaimCallback("lease-1", OperationID{}).Outcome())
	assert.Equal(t, SettlementNotFound,
		registry.tryClaimCallback("missing", token.operationID()).Outcome())
	assert.Equal(t, SettlementOperationMismatch,
		registry.tryClaimCallback("lease-1", otherID).Outcome())

	result := registry.tryClaimCallback("lease-1", token.operationID())
	require.True(t, result.Claimed())
	assert.Equal(t, SettlementClaimed, result.Outcome())
	assert.Equal(t, SettlementTerminal, result.Record().Settlement)
	assert.True(t, result.Claim().Valid())

	busy := registry.tryClaimDeprovision("lease-1", token.operationID())
	assert.Equal(t, SettlementBusy, busy.Outcome())
	assert.False(t, busy.Claimed())

	record, exists := registry.lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, SettlementTerminal, record.Settlement)
	assert.True(t, registry.releaseSettlement(result.Claim()))
	record, exists = registry.lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, SettlementUnclaimed, record.Settlement)
	_, exists = registry.lookup("missing")
	assert.False(t, exists)
}

func TestRegistrySettlementClaimNoncePreventsReleaseAndFinishABA(t *testing.T) {
	registry := newTestRegistry()
	token := requireStarted(t, registry, testTrackSpec("lease-1"))

	first := registry.tryClaimCallback("lease-1", token.operationID())
	require.True(t, first.Claimed())
	require.True(t, registry.releaseSettlement(first.Claim()))

	second := registry.tryClaimDeprovision("lease-1", token.operationID())
	require.True(t, second.Claimed())
	assert.Equal(t, SettlementDeprovision, second.Record().Settlement)
	assert.NotEqual(t, first.Claim(), second.Claim())
	assert.False(t, registry.releaseSettlement(first.Claim()))
	assert.False(t, registry.finishSettlement(first.Claim()))
	assert.True(t, registry.finishSettlement(second.Claim()))
	assert.False(t, registry.contains("lease-1"))
	assert.False(t, registry.finishSettlement(second.Claim()))
}

func TestRegistrySettlementRejectsForeignClaim(t *testing.T) {
	registry := newTestRegistry()
	foreign := newRegistry(100, 200)
	requireStarted(t, registry, testTrackSpec("lease-1"))
	foreignToken := requireStarted(t, foreign, testTrackSpec("lease-1"))
	foreignClaim := foreign.tryClaimCallback("lease-1", foreignToken.operationID())
	require.True(t, foreignClaim.Claimed())

	assert.False(t, registry.releaseSettlement(foreignClaim.Claim()))
	assert.False(t, registry.finishSettlement(foreignClaim.Claim()))
	assert.True(t, registry.contains("lease-1"))
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

	counts := registry.countsByBackend()
	assert.Equal(t, map[string]int{"backend-a": 2, "backend-b": 1}, counts)
	counts["backend-a"] = 99
	assert.Equal(t, 2, registry.countsByBackend()["backend-a"])

	leases := registry.leaseUUIDs()
	sort.Strings(leases)
	assert.Equal(t, []string{"lease-1", "lease-2", "lease-3"}, leases)
	leassesCopy := append([]string(nil), leases...)
	leassesCopy[0] = "mutated"
	fresh := registry.leaseUUIDs()
	sort.Strings(fresh)
	assert.Equal(t, leases, fresh)
}

func TestRegistryCountObserverSeesTypedMutationsInOrder(t *testing.T) {
	var observed []int
	registry := newRegistryWithObserver(100, 200, func(count int) {
		observed = append(observed, count)
	})

	first := requireStarted(t, registry, testTrackSpec("lease-1"))
	second := requireStarted(t, registry, testTrackSpec("lease-2"))
	assert.False(t, tryInitiateForTest(registry, testTrackSpec("lease-2")).Started())
	requireSettled(t, registry, first)
	requireSettled(t, registry, second)
	replacement := requireStarted(t, registry, testTrackSpec("lease-2"))
	requireSettled(t, registry, replacement)

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

	timedOut := registry.timedOut(5 * time.Minute)
	require.Len(t, timedOut, 1)
	assert.Equal(t, "lease-old", timedOut[0].LeaseUUID)
	timedOut[0].Items[0].SKU = "mutated"
	record, exists := registry.lookup("lease-old")
	require.True(t, exists)
	assert.Equal(t, "sku-a", record.Items[0].SKU)
	assert.Equal(t, 2, registry.count())
}

func TestRuntimeControllerWaitForDrain(t *testing.T) {
	t.Run("already empty", func(t *testing.T) {
		registry := newTestRegistry()
		runtime := bindRuntimeForTest(t, registry)
		assert.Zero(t, runtime.WaitForDrain(context.Background(), time.Second))
	})

	t.Run("exact operation removal wakes waiter", func(t *testing.T) {
		registry := newTestRegistry()
		runtime := bindRuntimeForTest(t, registry)
		tracked := requireStarted(t, registry, testTrackSpec("lease-1"))
		result := make(chan int, 1)
		go func() {
			result <- runtime.WaitForDrain(context.Background(), time.Second)
		}()

		requireSettled(t, registry, tracked)
		assert.Zero(t, <-result)
	})

	t.Run("context and timeout return current count", func(t *testing.T) {
		registry := newTestRegistry()
		runtime := bindRuntimeForTest(t, registry)
		tracked := requireStarted(t, registry, testTrackSpec("lease-1"))
		t.Cleanup(func() { requireSettled(t, registry, tracked) })

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		assert.Equal(t, 1, runtime.WaitForDrain(ctx, time.Second))
		assert.Equal(t, 1, runtime.WaitForDrain(context.Background(), 0))
	})
}

func TestRuntimeControllerBeginDrainRejectsOrdinaryWorkAndWaitsForHeldActions(t *testing.T) {
	registry := newTestRegistry()
	runtime := bindRuntimeForTest(t, registry)
	tracked := requireStarted(t, registry, testTrackSpec("lease-operation"))
	held := registry.tryClaimLeaseNow("lease-action")
	require.True(t, held.Acquired())
	assert.Equal(t, 2, runtime.PendingWorkCount())

	runtime.BeginDrain()
	runtime.BeginDrain()

	assert.Equal(t, TrackInvalid, tryInitiateForTest(registry, testTrackSpec("lease-new")).Outcome())
	assert.Equal(t, LeaseClaimInvalid, registry.tryClaimLeaseNow("lease-new").Outcome())
	snapshot := registry.snapshot()
	assert.Equal(t, LeaseClaimInvalid, registry.tryClaimLease("lease-new", snapshot).Outcome())
	assert.Equal(t, TrackInvalid,
		registry.tryInitiateClaimed(
			held.Claim(), testTrackSpec("lease-action").operationSpec(),
		).Outcome(),
		"a claim acquired before shutdown must not start a new backend operation after the barrier")

	recovery := registry.tryClaimCallbackRecoveryLease("lease-recovery")
	require.True(t, recovery.Acquired(),
		"authenticated durable callback recovery remains admitted during drain")
	assert.Equal(t, 3, runtime.PendingWorkCount())

	settlement := registry.tryClaimCallback("lease-operation", tracked.operationID())
	require.True(t, settlement.Claimed(), "existing operations must remain settleable")
	require.True(t, registry.finishSettlement(settlement.Claim()))
	assert.Equal(t, 2, runtime.WaitForDrain(context.Background(), 0),
		"held ordinary and recovery claims are both shutdown work")

	drained := make(chan int, 1)
	go func() {
		drained <- runtime.WaitForDrain(context.Background(), time.Second)
	}()
	require.True(t, registry.releaseLease(held.Claim()))
	select {
	case result := <-drained:
		t.Fatalf("drain returned with callback recovery still active: %d", result)
	case <-time.After(10 * time.Millisecond):
	}
	require.True(t, registry.releaseLease(recovery.Claim()))
	assert.Zero(t, <-drained)
	assert.Zero(t, runtime.PendingWorkCount())
}

func TestRuntimeControllerWaitForDrainCountsLeaseClaimWithoutOperation(t *testing.T) {
	registry := newTestRegistry()
	runtime := bindRuntimeForTest(t, registry)
	claim := registry.tryClaimLeaseNow("lease-action")
	require.True(t, claim.Acquired())
	runtime.BeginDrain()

	assert.Equal(t, 1, runtime.WaitForDrain(context.Background(), 0))
	require.True(t, registry.releaseLease(claim.Claim()))
	assert.Zero(t, runtime.WaitForDrain(context.Background(), time.Second))
}

func TestRegistryDrainSignalRearmsForNextOperationGeneration(t *testing.T) {
	registry := newTestRegistry()
	first := requireStarted(t, registry, testTrackSpec("lease-first"))
	firstSignal := registry.drained
	select {
	case <-firstSignal:
		t.Fatal("a live operation must hold an open drain signal")
	default:
	}
	requireSettled(t, registry, first)
	select {
	case <-firstSignal:
	default:
		t.Fatal("removing the final operation must close its drain signal")
	}

	second := requireStarted(t, registry, testTrackSpec("lease-second"))
	secondSignal := registry.drained
	assert.NotEqual(t, firstSignal, secondSignal)
	select {
	case <-secondSignal:
		t.Fatal("a new operation generation must receive a fresh open signal")
	default:
	}
	requireSettled(t, registry, second)
}

func TestRegistryClaimNonceAndMutationRevisionSkipZeroOnWrap(t *testing.T) {
	registry := newRegistry(100, math.MaxUint64)
	claim := registry.tryClaimLeaseNow("lease-1")
	require.True(t, claim.Acquired())
	assert.Equal(t, uint64(1), claim.Claim().nonce)

	registry.mutationRevision = math.MaxUint64
	require.True(t, registry.releaseLease(claim.Claim()))
	assert.Equal(t, uint64(1), registry.mutationRevision)
	assert.Equal(t, uint64(1), registry.lastMutation["lease-1"])
}
