package provisioner

import (
	"context"
	"slices"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

type fencedFleetResolver map[string]backendidentity.ID

func (resolver fencedFleetResolver) ExpectedBackendStorageIdentity(name string) (backendidentity.ID, bool) {
	id, ok := resolver[name]
	return id, ok
}

// restartFenced restarts Fred with exactly the named backends fenced, as an
// operator would after editing backends[].fenced: their clients carry no
// connection and the placement store is told which backends are fenced. With
// no names it lifts every fence.
func (f *fleet) restartFenced(fenced ...string) {
	f.t.Helper()
	if f.liveRouterEntries == nil {
		f.liveRouterEntries = slices.Clone(f.routerEntries)
	}
	entries := slices.Clone(f.liveRouterEntries)
	for index, entry := range entries {
		name := entry.Backend.Name()
		if !slices.Contains(fenced, name) {
			continue
		}
		policy, err := backend.NewFencedConnectionPolicy(name)
		require.NoError(f.t, err)
		client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{},
			fencedFleetResolver{name: testBackendStorageID(name)})
		require.NoError(f.t, err)
		entries[index].Backend = client
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(f.t, err)
	f.router = router
	f.routerEntries = entries

	require.NoError(f.t, f.placement.Close())
	callbackRoutes, err := placement.NewCallbackRouteFactory("http://fred.invalid")
	require.NoError(f.t, err)
	store, err := placementstore.NewStore(
		f.placementPath,
		placement.WithClock(func() time.Time { return time.Now().Add(-f.placementAge) }),
		placement.WithCallbackRouteFactory(callbackRoutes),
		placement.WithFencedBackends(fenced),
	)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { _ = store.Close() })
	f.placement = store
	f.tracker.testOperationRegistry = newTestOperationRegistry()
	f.coordinator, err = f.tracker.bindPlacementStore(store)
	require.NoError(f.t, err)
	f.execution = bindTestBackendRuntime(f.t, f.coordinator, f.router)
	f.tracker.callbackStore = store
	f.reconcilerCfg.Coordinator = bindTestReconciliationCoordinator(
		f.t, f.placement, f.execution, f.chain, f.payloads, nil,
	)
	reconciler, err := newTestReconciler(
		f.t, f.reconcilerCfg, f.chain, f.acknowledger, f.router, f.tracker, store,
	)
	require.NoError(f.t, err)
	f.reconciler = reconciler
}

// Not parallel: reads process-global collectors.
func TestFleet_FencedBackendIsNeverAskedAndItsLeasesWait(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-live", billingtypes.LEASE_STATE_ACTIVE)
	f.addLease("lease-fenced", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(1).seedProvision(t, "lease-live", f.providerUUID, backend.ProvisionStatusReady)
	fencedServer := f.backendAt(2)
	fencedServer.seedProvision(t, "lease-fenced", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-fenced", fencedServer.name)

	f.restartFenced(fencedServer.name)
	fencedOutcome := metrics.ReconcilerBackendInventoryTotal.WithLabelValues(
		fencedServer.name, metrics.InventoryOutcomeFenced)
	unanswered := metrics.ReconcilerBackendInventoryTotal.WithLabelValues(
		fencedServer.name, metrics.InventoryOutcomeUnanswered)
	fencedFetch := metrics.ReconcilerBackendFetchTotal.WithLabelValues(
		fencedServer.name, metrics.FetchOutcomeFenced)
	fencedBefore, unansweredBefore, fetchBefore :=
		promtestutil.ToFloat64(fencedOutcome), promtestutil.ToFloat64(unanswered), promtestutil.ToFloat64(fencedFetch)
	fencedServer.mu.Lock()
	listsBefore, retentionsBefore := fencedServer.listCalls, fencedServer.retentionCalls
	fencedServer.mu.Unlock()

	require.NoError(t, f.sweepN(3))

	fencedServer.mu.Lock()
	assert.Equal(t, listsBefore, fencedServer.listCalls, "a fenced backend is never asked")
	assert.Equal(t, retentionsBefore, fencedServer.retentionCalls)
	fencedServer.mu.Unlock()
	assert.Equal(t, fencedBefore+3, promtestutil.ToFloat64(fencedOutcome))
	assert.Equal(t, unansweredBefore, promtestutil.ToFloat64(unanswered),
		"a fence is not reported as an outage")
	assert.Equal(t, fetchBefore+3, promtestutil.ToFloat64(fencedFetch))
	assert.InDelta(t, 0, promtestutil.ToFloat64(
		metrics.ReconcilerBackendInventoryAnswered.WithLabelValues(fencedServer.name)), 0)

	f.assertPlacementPinned("lease-fenced", fencedServer.name)
	_, rejected, closed := f.chainCalls()
	assert.NotContains(t, rejected, fleetLeaseUUID("lease-fenced"))
	assert.NotContains(t, closed, fleetLeaseUUID("lease-fenced"),
		"silence from a fenced backend is not evidence the lease is gone")
	for _, server := range []*fakeBackendServer{f.backendAt(1), f.backendAt(3)} {
		assert.Zero(t, server.provisionCount("lease-fenced"),
			"a fenced lease is never re-provisioned on another backend")
	}
	assert.Equal(t, placement.InventoryReady, f.placement.InventoryReadiness())
}

// A close whose only failed call was refused by a fenced backend can only
// finish after an operator decision, so the scheduler parks it instead of
// retrying every few seconds and reporting it overdue.
func TestFleet_CloseOnAFencedBackendIsParked(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-closing", billingtypes.LEASE_STATE_ACTIVE)
	fencedServer := f.backendAt(2)
	fencedServer.seedProvision(t, "lease-closing", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.restartFenced(fencedServer.name)
	require.NoError(t, f.sweep())

	closeAuthority, err := f.execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	result := closeAuthority.DeprovisionEvent(t.Context(), fleetLeaseUUID("lease-closing"))
	require.Equal(t, placement.DeprovisionEventDeferred, result.Disposition(), "%v", result.Err())
	require.Equal(t, placement.DeprovisionDeferredBackendFenced, result.Deferred().Reason())
	require.ErrorIs(t, result.Err(), backend.ErrBackendFenced)

	scheduler := newDeferredCloseScheduler()
	scheduler.start(t.Context())
	t.Cleanup(func() { scheduler.stop(); scheduler.wg.Wait() })
	parked := metrics.DeferredClosesTotal.WithLabelValues("parked", string(placement.DeprovisionDeferredBackendFenced))
	before := promtestutil.ToFloat64(parked)
	require.NoError(t, scheduler.enqueue(result.Deferred()))
	assert.Equal(t, before+1, promtestutil.ToFloat64(parked))
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	assert.Empty(t, scheduler.entries, "a fenced wait holds no retry slot")
	f.assertPlacementPinned("lease-closing", fencedServer.name)
}

// A close queued while recovery was pending parks once its retry finds that
// only the fence remains, releasing its retry slot.
func TestFleet_QueuedCloseParksOnceOnlyTheFenceRemains(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-queued", billingtypes.LEASE_STATE_ACTIVE)
	fencedServer := f.backendAt(2)
	fencedServer.seedProvision(t, "lease-queued", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())

	// Interrupt a sweep after backend-2 reported the lease, then restart with
	// backend-2 fenced: recovery is pending on a reporter Fred no longer asks.
	f.backendAt(3).setFault(faultHang)
	ctx, cancel := context.WithCancel(f.t.Context())
	defer cancel()
	stop := time.AfterFunc(200*time.Millisecond, cancel)
	defer stop.Stop()
	require.ErrorIs(t, f.reconciler.ReconcileAll(ctx), context.Canceled)
	f.backendAt(3).setFault(faultNone)
	f.restartFenced(fencedServer.name)
	require.Equal(t, placement.InventoryRecoveryPending, f.placement.InventoryReadiness())

	closeAuthority, err := f.execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	result := closeAuthority.DeprovisionEvent(t.Context(), fleetLeaseUUID("lease-queued"))
	require.Equal(t, placement.DeprovisionEventDeferred, result.Disposition(), "%v", result.Err())
	require.Equal(t, placement.DeprovisionDeferredInventory, result.Deferred().Reason())

	scheduler := newDeferredCloseScheduler()
	scheduler.start(t.Context())
	t.Cleanup(func() { scheduler.stop(); scheduler.wg.Wait() })
	parked := metrics.DeferredClosesTotal.WithLabelValues("parked", string(placement.DeprovisionDeferredBackendFenced))
	before := promtestutil.ToFloat64(parked)
	require.NoError(t, scheduler.enqueue(result.Deferred()))

	require.NoError(t, f.sweep())
	require.Equal(t, placement.InventoryFencedReporterUnaccounted, f.placement.InventoryReadiness(),
		"recovery clears without the fenced reporter, which stays unaccounted")
	require.Eventually(t, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return len(scheduler.entries) == 0
	}, 15*time.Second, 10*time.Millisecond, "the retry found only the fence and released its slot")
	assert.Equal(t, before+1, promtestutil.ToFloat64(parked))
	f.assertPlacementPinned("lease-queued", fencedServer.name)
}

// The reconciler routes among backends that answered, and a fenced backend
// never answers. A SKU only it serves must still wait, never reaching the
// default backend, where it could be rejected on chain or run on hardware the
// operator never assigned it.
func TestFleet_ReconcilerNeverSendsAFencedOnlySKUToTheDefault(t *testing.T) {
	f := newFleet(t, fleetOptions{backendSKUs: map[int][]string{2: {"sku-fenced-only"}}})
	require.NoError(t, f.sweep())
	f.restartFenced(f.backendAt(2).name)
	f.addLease("lease-fenced-sku", billingtypes.LEASE_STATE_PENDING, "sku-fenced-only")

	require.NoError(t, f.sweepN(2))

	for _, server := range f.servers {
		assert.Zero(t, server.provisionCount("lease-fenced-sku"), server.name)
	}
	_, rejected, _ := f.chainCalls()
	assert.NotContains(t, rejected, fleetLeaseUUID("lease-fenced-sku"))
	assert.Equal(t, placement.StateAbsent, f.placement.Lookup(fleetLeaseUUID("lease-fenced-sku")).State())
}

// Redelivering an attempt to a fenced backend is refused locally every time,
// so the reconciler leaves the attempt in place without counting a lease error.
func TestFleet_AttemptOnAFencedBackendWaitsQuietly(t *testing.T) {
	f := newFleet(t, fleetOptions{backendSKUs: map[int][]string{2: {"sku-attempt"}}})
	require.NoError(t, f.sweep())
	induceFleetAmbiguousProvision(t, f, "lease-attempt", "sku-attempt", 2)
	f.restartFenced(f.backendAt(2).name)
	leaseErrors := metrics.ReconciliationActions.WithLabelValues(metrics.ActionLeaseError)
	before := promtestutil.ToFloat64(leaseErrors)

	require.NoError(t, f.sweep())

	assert.Equal(t, before, promtestutil.ToFloat64(leaseErrors))
	p := f.placement.Lookup(fleetLeaseUUID("lease-attempt"))
	assert.Equal(t, placement.StateAttempting, p.State())
	assert.Equal(t, f.backendAt(2).name, p.Attempt, "the write-ahead attempt is preserved")
}

// interruptSweepAfterProvisions cancels a sweep once the fast backends have
// answered their provision inventories, leaving the marker pending with those
// reporters journaled.
func (f *fleet) interruptSweepAfterProvisions(hang *fakeBackendServer) {
	f.t.Helper()
	hang.setFault(faultHang)
	ctx, cancel := context.WithCancel(f.t.Context())
	defer cancel()
	stop := time.AfterFunc(200*time.Millisecond, cancel)
	defer stop.Stop()
	require.ErrorIs(f.t, f.reconciler.ReconcileAll(ctx), context.Canceled)
	hang.setFault(faultNone)
}

// Codex review of #245: a fenced reporter's lost positive may be a lease with
// no placement row. Clearing recovery must not let Fred admit that lease on a
// healthy peer while the original workload still runs on the fenced backend.
func TestFleet_FencedReporterLostPositiveIsNotAdmittedElsewhere(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	require.NoError(t, f.sweep(), "establish the admission baseline")
	f.addLease("lease-lost-positive", billingtypes.LEASE_STATE_PENDING)
	fencedServer := f.backendAt(2)
	fencedServer.seedProvision(t, "lease-lost-positive", f.providerUUID, backend.ProvisionStatusReady)

	f.interruptSweepAfterProvisions(f.backendAt(3))
	f.restartFenced(fencedServer.name)
	require.Equal(t, placement.InventoryRecoveryPending, f.placement.InventoryReadiness())
	leaseErrors := metrics.ReconciliationActions.WithLabelValues(metrics.ActionLeaseError)
	errorsBefore := promtestutil.ToFloat64(leaseErrors)

	require.NoError(t, f.sweepN(2))

	assert.Equal(t, placement.InventoryFencedReporterUnaccounted, f.placement.InventoryReadiness())
	assert.Equal(t, errorsBefore, promtestutil.ToFloat64(leaseErrors),
		"the reconciler defers the lease quietly instead of attempting a refused admission")
	for _, server := range []*fakeBackendServer{f.backendAt(1), f.backendAt(3)} {
		assert.Zero(t, server.provisionCount("lease-lost-positive"),
			"the lease may already run on the fenced backend; it is not admitted on %s", server.name)
	}
	_, rejected, _ := f.chainCalls()
	assert.NotContains(t, rejected, fleetLeaseUUID("lease-lost-positive"))

	// The fence lifts: the backend answers, its positive is projected, and
	// admission resumes without a second copy.
	f.restartFenced()
	require.NoError(t, f.sweepN(2))
	assert.Equal(t, placement.InventoryReady, f.placement.InventoryReadiness())
	f.assertPlacementPinned("lease-lost-positive", fencedServer.name)
	for _, server := range []*fakeBackendServer{f.backendAt(1), f.backendAt(3)} {
		assert.Zero(t, server.provisionCount("lease-lost-positive"), server.name)
	}
}

// Under -race this pins that the fenced branches never write the shared
// inventory maps while workers run. The fenced backend sorts last, so no
// later worker launch orders its write before the earlier workers' writes.
func TestFleet_FencedInventoryBranchDoesNotRaceItsWorkers(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-race", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(1).seedProvision(t, "lease-race", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.restartFenced(f.backendAt(3).name)
	require.NoError(t, f.sweepN(3))
	f.assertPlacementPinned("lease-race", f.backendAt(1).name)
}

// Codex re-review of #245: withholding admission of leases without a row must
// not stop recovery of a lease that has one. A confirmed owner that answers
// and has lost the workload still gets the lease back.
func TestFleet_FencedReporterDoesNotBlockConfirmedOwnerRecovery(t *testing.T) {
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-owned", billingtypes.LEASE_STATE_ACTIVE)
	owner := f.backendAt(1)
	owner.seedProvision(t, "lease-owned", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-owned", owner.name)

	f.addLease("lease-other", billingtypes.LEASE_STATE_PENDING)
	fencedServer := f.backendAt(2)
	fencedServer.seedProvision(t, "lease-other", f.providerUUID, backend.ProvisionStatusReady)
	f.interruptSweepAfterProvisions(f.backendAt(3))
	f.restartFenced(fencedServer.name)
	require.NoError(t, f.sweep())
	require.Equal(t, placement.InventoryFencedReporterUnaccounted, f.placement.InventoryReadiness())

	// The owner keeps answering but has lost the workload.
	require.NoError(t, owner.mock.Deprovision(t.Context(), fleetLeaseUUID("lease-owned")))
	require.NoError(t, f.sweepN(2))

	assert.Equal(t, 1, owner.provisionCount("lease-owned"),
		"the confirmed owner gets its lease back while the fenced reporter stays unaccounted")
	f.assertPlacementPinned("lease-owned", owner.name)
	for _, server := range []*fakeBackendServer{f.backendAt(1), f.backendAt(3)} {
		assert.Zero(t, server.provisionCount("lease-other"),
			"the lease the fenced backend may hold is still not admitted on %s", server.name)
	}
}

// classifyStopped closes the fleet's placement store and inspects the stopped
// database exactly as placement-repair -classify does, as an operator would
// before restarting with a backend fenced. A later restart reopens the store.
func (f *fleet) classifyStopped() placement.AuthorityReport {
	f.t.Helper()
	require.NoError(f.t, f.placement.Close())
	expectation, err := placement.NewAuthorityExpectation(f.providerUUID, backendTopologyNames(f.router))
	require.NoError(f.t, err)
	report, err := placement.InspectAuthorityFile(f.placementPath, expectation)
	require.NoError(f.t, err)
	return report
}

// ENG-1119: a graceful stop that lands while a backend is still answering lets
// the in-flight sweep finish its reads and commit its projection instead of
// abandoning it. Nothing is left pending, so restarting with the sweep's
// reporter fenced records nothing and new leases are still admitted.
// Not parallel: reads process-global collectors.
func TestFleet_GracefulStopDrainsInFlightSweep(t *testing.T) {
	f := newFleet(t, fleetOptions{shutdownSweepGrace: 5 * time.Second})
	require.NoError(t, f.sweep(), "establish the admission baseline")
	f.addLease("lease-drained", billingtypes.LEASE_STATE_ACTIVE)
	reporter := f.backendAt(2)
	reporter.seedProvision(t, "lease-drained", f.providerUUID, backend.ProvisionStatusReady)

	slow := f.backendAt(3)
	slow.setFault(faultSlowOK)
	ctx, cancel := context.WithCancel(f.t.Context())
	defer cancel()
	stop := time.AfterFunc(100*time.Millisecond, cancel)
	defer stop.Stop()
	started := time.Now()
	require.ErrorIs(t, f.reconciler.ReconcileAll(ctx), context.Canceled,
		"a stopped sweep still starts no lifecycle work")
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond,
		"the sweep kept reading the slow backend after the stop at 100ms")
	assert.Less(t, elapsed, 5*time.Second, "the sweep finished inside its grace")
	slow.setFault(faultNone)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.ReconcilerSweepProjectionCommitted),
		"the sweep finished its reads and committed its projection")
	assert.Equal(t, placement.InventoryReady, f.placement.InventoryReadiness(),
		"the committed projection cleared the pending marker")
	f.assertPlacementPinned("lease-drained", reporter.name)

	report := f.classifyStopped()
	assert.Zero(t, report.PendingInventorySweepID)
	assert.Empty(t, report.FenceRestartWouldRecord, "-classify shows nothing a fence would record")

	f.restartFenced(reporter.name)
	f.addLease("lease-new", billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweepN(2))
	assert.Equal(t, placement.InventoryReady, f.placement.InventoryReadiness(),
		"the fenced reporter was never recorded as unaccounted")
	assert.Equal(t, 1, f.backendAt(1).provisionCount("lease-new"),
		"a new lease is admitted on an unfenced backend")
	f.assertPlacementPinned("lease-drained", reporter.name)
}

// ENG-1119 G4: a stop whose grace expires while a backend still hangs abandons
// the sweep as before: nothing is projected and the marker stays pending, with
// the reporter journaled. -classify names that reporter in
// fence_restart_would_record, and a restart with it fenced records it and
// withholds new-lease admission.
// Not parallel: reads process-global collectors.
func TestFleet_GracefulStopPastGraceStillAbandonsSweep(t *testing.T) {
	f := newFleet(t, fleetOptions{shutdownSweepGrace: 100 * time.Millisecond})
	require.NoError(t, f.sweep(), "establish the admission baseline")
	f.addLease("lease-abandoned", billingtypes.LEASE_STATE_PENDING)
	reporter := f.backendAt(2)
	reporter.seedProvision(t, "lease-abandoned", f.providerUUID, backend.ProvisionStatusReady)

	f.interruptSweepAfterProvisions(f.backendAt(3))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.ReconcilerSweepProjectionCommitted),
		"a sweep abandoned past its grace commits nothing")
	assert.Equal(t, placement.InventoryRecoveryPending, f.placement.InventoryReadiness())

	report := f.classifyStopped()
	assert.NotZero(t, report.PendingInventorySweepID)
	assert.False(t, report.InventorySweepUntracked)
	assert.Equal(t, []string{reporter.name}, report.InventorySweepReporters)
	assert.Equal(t, []string{reporter.name}, report.FenceRestartWouldRecord,
		"-classify names the backend whose fence would hold admission")

	f.restartFenced(reporter.name)
	require.Equal(t, placement.InventoryRecoveryPending, f.placement.InventoryReadiness())
	require.NoError(t, f.sweepN(2))
	assert.Equal(t, placement.InventoryFencedReporterUnaccounted, f.placement.InventoryReadiness())
	f.addLease("lease-new", billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweepN(2))
	for _, server := range []*fakeBackendServer{f.backendAt(1), f.backendAt(3)} {
		assert.Zero(t, server.provisionCount("lease-new"),
			"new-lease admission is withheld on %s while the fenced reporter is unaccounted", server.name)
		assert.Zero(t, server.provisionCount("lease-abandoned"),
			"the lease the fenced backend may hold is not admitted on %s", server.name)
	}
}
