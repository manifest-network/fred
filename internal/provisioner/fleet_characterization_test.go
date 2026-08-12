package provisioner

// Characterization tests for reconciling a multi-backend fleet over the real
// HTTP transport.
//
// These pin the behaviour that must survive ENG-356, which removes the
// fleet-wide abort in fetchAllProvisions. They are written against the CURRENT
// code and pass on it: today a sweep that cannot reach every backend aborts, so
// the "nothing was destroyed" invariant holds trivially. After ENG-356 the same
// assertions must still hold, but for the interesting reason — the sweep
// proceeds and defers exactly the leases it cannot positively attribute. That
// equivalence is the point: any change to these assertions other than the ones
// ENG-356 explicitly inverts is a regression, not a test that needs updating.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// allFaults is every way a backend can fail to answer a sweep. Each reaches the
// reconciler through a different branch of backend.HTTPClient, so the matrix is
// over transport failure modes rather than over one injected error.
var allFaults = []faultKind{
	faultConnReset,
	faultHang,
	faultHTTP500,
	faultHTTP503,
	faultGarbage,
	faultOversize,
}

// --------------------------------------------------------------------------
// Healthy fleet: the complete-sweep path must be untouched by ENG-356
// --------------------------------------------------------------------------

func TestFleet_HealthyFleet_ProvisionsPendingLease(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-new", billingtypes.LEASE_STATE_PENDING)

	require.NoError(t, f.sweep())

	// No SKUs configured, so routing falls to the default backend.
	require.Equal(t, 1, f.backendAt(1).provisionCount("lease-new"))
	f.assertProvisionedExactlyOnce("lease-new")
}

func TestFleet_HealthyFleet_AcknowledgesReadyLease(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-ready", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(2).seedProvision(t, "lease-ready", f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())

	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, "lease-ready")
}

func TestFleet_HealthyFleet_DeprovisionsOrphan(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A provision with no chain lease behind it.
	f.backendAt(3).seedProvision(t, "lease-orphan", f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())

	require.Equal(t, 1, f.backendAt(3).deprovisionCount("lease-orphan"),
		"an orphan on an answering backend must still be reaped")
}

func TestFleet_HealthyFleet_LeavesActiveProvisionedLeaseAlone(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-healthy", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(t, "lease-healthy", f.providerUUID, backend.ProvisionStatusReady)

	// The first sweep legitimately populates the placement index, so settle it
	// before snapshotting: the invariant is about what a sweep destroys, not
	// about the additive sync.
	require.NoError(t, f.sweep())

	before := f.captureState([]string{"lease-healthy"})
	require.NoError(t, f.sweep())
	f.assertNothingDestroyed(before, []string{"lease-healthy"})
}

// --------------------------------------------------------------------------
// The false-positive guard
// --------------------------------------------------------------------------

// A backend that answers slowly, but within the timeout, is answering. If it
// were ever counted as unanswered, ENG-356 would defer leases on a perfectly
// healthy machine — converting a latency blip into a stalled lease.
func TestFleet_SlowButSuccessfulBackend_IsNotAFailure(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-slow", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(2).seedProvision(t, "lease-slow", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).setFault(faultSlowOK)

	require.NoError(t, f.sweep(), "a slow but successful backend must not fail the sweep")

	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, "lease-slow")
}

// --------------------------------------------------------------------------
// Degraded fleet: the invariant
// --------------------------------------------------------------------------

// The money case. A lease whose data lives on a backend that did not answer
// must never be re-provisioned onto a healthy peer — that is how an empty
// volume gets laid over live tenant data, unattended and reported as success.
func TestFleet_ActiveLeaseOnFaultedBackend_IsNeverReprovisioned(t *testing.T) {
	t.Parallel()

	for _, fault := range allFaults {
		t.Run(string(fault), func(t *testing.T) {
			t.Parallel()
			f := newFleet(t, fleetOptions{})

			// The lease is ACTIVE on chain and lives on backend-2.
			f.addLease("lease-pinned", billingtypes.LEASE_STATE_ACTIVE)
			f.backendAt(2).seedProvision(t, "lease-pinned", f.providerUUID, backend.ProvisionStatusReady)

			// Establish the placement record while the fleet is healthy.
			require.NoError(t, f.sweep())
			f.assertPlacementPinned("lease-pinned", "backend-2")

			// Now backend-2 goes quiet.
			f.backendAt(2).setFault(fault)

			before := f.captureState([]string{"lease-pinned"})
			_ = f.sweepN(2)
			f.assertNothingDestroyed(before, []string{"lease-pinned"})
			f.assertPlacementPinned("lease-pinned", "backend-2")
		})
	}
}

// The production data-loss shape, stated exactly. With no placement record to
// pin it, a lease whose backend went quiet looks unprovisioned, and the
// ACTIVE && !isProvisioned row hands it to least-loaded routing — which picks a
// DIFFERENT machine and lays a brand-new empty volume over live tenant data.
// Nothing about that is visible to the caller: the provision succeeds.
//
// The previous test pins the same rule with a placement record present (where
// the pin happens to route it back to its real owner). This one removes the
// pin, so a regression lands the provision on a peer — which is the failure
// that actually destroys data.
func TestFleet_UnplacedLeaseOnFaultedBackend_IsNotProvisionedOnAPeer(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{noPlacement: true})

	f.addLease("lease-unplaced", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(t, "lease-unplaced", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).setFault(faultConnReset)

	before := f.captureState([]string{"lease-unplaced"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-unplaced"})

	for _, srv := range f.servers {
		require.Zerof(t, srv.totalProvisionCalls(),
			"no lease may be provisioned anywhere while the fleet is incomplete (%s)", srv.name)
	}
}

// A backend that dies partway through a paginated listing is the subtlest
// failure mode here: the client has already collected real provisions when the
// next page fails. walkKeysetPages is complete-or-error precisely so those
// pages are discarded rather than returned as a short list — a short list would
// look like "these leases are gone" and is indistinguishable from truth.
func TestFleet_MidPaginationFailure_YieldsNoPartialList(t *testing.T) {
	t.Parallel()
	// pageLimit 1 forces a multi-page walk, so page 2 exists to fail.
	f := newFleet(t, fleetOptions{pageLimit: 1})

	// Real UUIDs, unlike the readable names elsewhere: the keyset cursor IS the
	// lease UUID, and ParsePageParams rejects a continue token that does not
	// parse as one. Any test that lowers pageLimit needs UUID lease IDs.
	const (
		leaseP1 = "11111111-1111-4111-8111-111111111111"
		leaseP2 = "22222222-2222-4222-8222-222222222222"
	)

	f.addLease(leaseP1, billingtypes.LEASE_STATE_ACTIVE)
	f.addLease(leaseP2, billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(t, leaseP1, f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).seedProvision(t, leaseP2, f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())

	f.backendAt(2).setFault(faultPage2)

	leases := []string{leaseP1, leaseP2}
	before := f.captureState(leases)
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, leases)
}

// faultConnReset drops an established connection; this kills the listener so
// the dial itself fails. Both are transport errors to the reconciler, but
// connection-refused is what an operator actually produces by stopping a
// backend, and it is the headline scenario in the ticket.
//
// Note what this test can and cannot see: a dead listener cannot count the
// calls made to it, so its teeth are the placement and chain assertions plus
// the call counters on the SURVIVING backends — which is what catches a
// provision being substituted onto a peer. Use the faultConnReset variant
// above when the assertion needs to observe calls to the faulted backend
// itself.
func TestFleet_KilledBackend_DestroysNothing(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-killed", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(t, "lease-killed", f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-killed", "backend-2")

	f.backendAt(2).kill()

	before := f.captureState([]string{"lease-killed"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-killed"})
	f.assertPlacementPinned("lease-killed", "backend-2")
}

// The whole fleet being unreachable is not a special case — it is the degenerate
// end of the same axis, and it must destroy nothing either.
func TestFleet_AllBackendsFaulted_DestroysNothing(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-a", billingtypes.LEASE_STATE_ACTIVE)
	f.addLease("lease-b", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(1).seedProvision(t, "lease-a", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).seedProvision(t, "lease-b", f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())

	for _, srv := range f.servers {
		srv.setFault(faultHTTP500)
	}

	leases := []string{"lease-a", "lease-b"}
	before := f.captureState(leases)
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, leases)
}

// A provision on a backend that did not answer must not be classified as an
// orphan. Today the abort makes this unreachable; after ENG-356 it is prevented
// because a deferred lease never enters the orphan set at all.
func TestFleet_OrphanOnFaultedBackend_IsNotDeprovisioned(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// Live on backend-2, with no chain lease — an orphan candidate, but only if
	// backend-2 is heard from.
	f.backendAt(2).seedProvision(t, "lease-ghost", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).setFault(faultConnReset)

	before := f.captureState([]string{"lease-ghost"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-ghost"})
}

// The payload store is the input to re-provisioning an ACTIVE lease. Deleting a
// live lease's payload during a degraded sweep would make the NEXT sweep see
// errPayloadNotAvailable, classify it as permanent, and close a healthy ACTIVE
// lease on chain. This guards the chainLeases-filtering trap directly.
func TestFleet_PayloadForLiveLease_SurvivesDegradedSweep(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-payload", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(t, "lease-payload", f.providerUUID, backend.ProvisionStatusReady)
	require.True(t, f.payloads.Store("lease-payload", []byte("manifest-bytes")))

	require.NoError(t, f.sweep())
	f.backendAt(2).setFault(faultHTTP500)

	before := f.captureState([]string{"lease-payload"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-payload"})

	has, err := f.payloads.Has("lease-payload")
	require.NoError(t, err)
	require.True(t, has, "a live lease's payload must survive a sweep that could not see the fleet")
}

// Retained leases pin a backend for restore affinity. A backend going quiet
// must not cost the placement record that points at the machine holding the
// retained data.
func TestFleet_RetainedLeasePlacement_SurvivesDegradedSweep(t *testing.T) {
	t.Parallel()
	// prunableFleet, not the default: with the default hour-long interval the
	// 2x-interval grace window would hold the record on its own and this would
	// assert nothing.
	f := prunableFleet(t)

	f.backendAt(3).seedRetention("lease-retained")
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-retained", "backend-3")

	f.backendAt(3).setFault(faultConnReset)

	before := f.captureState([]string{"lease-retained"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-retained"})
	f.assertPlacementPinned("lease-retained", "backend-3")
}

// prunableFleet builds a fleet in which the placement pruner can actually fire:
// a short interval shrinks the 2x-interval grace window, and back-dated
// placement records age past it deterministically. Without both, every pruner
// assertion below would pass for the wrong reason — the grace window alone
// would hold every record, and a test asserting "not pruned" would prove
// nothing. TestFleet_TerminalLease_PlacementIsPruned is the control that keeps
// the others honest.
func prunableFleet(t *testing.T) *fleet {
	t.Helper()
	return newFleet(t, fleetOptions{
		interval:     time.Second,
		placementAge: time.Hour,
	})
}

// Control: with a complete sweep and complete retentions, a chain-terminal
// lease that is gone from every backend DOES get its placement pruned. This is
// what proves the two "does not prune" tests below are meaningful.
func TestFleet_TerminalLease_PlacementIsPruned(t *testing.T) {
	t.Parallel()
	f := prunableFleet(t)

	f.backendAt(2).seedProvision(t, "lease-x", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-x", billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-x", "backend-2")

	// Lease closes on chain and its resources are gone from the backend.
	f.removeLease("lease-x")
	f.backendAt(2).mock.Clear()

	require.NoError(t, f.sweep())
	require.Empty(t, f.placement.Get("lease-x"),
		"a chain-terminal lease absent from every backend should be pruned on a complete sweep")
}

// Sweep completeness and retention completeness are independent signals. A
// backend serving /provisions but failing /retentions must still hold the
// pruner off, or a transient retention outage looks like "the data is gone".
func TestFleet_RetentionsFailureAlone_DoesNotPrunePlacement(t *testing.T) {
	t.Parallel()
	f := prunableFleet(t)

	f.backendAt(2).seedProvision(t, "lease-x", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-x", billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-x", "backend-2")

	// Exactly the shape the control prunes on — except /retentions is failing.
	f.removeLease("lease-x")
	f.backendAt(2).mock.Clear()
	f.backendAt(2).setFault(faultRetentionsOnly)

	_ = f.sweepN(2)
	f.assertPlacementPinned("lease-x", "backend-2")
}

// The same shape again, but with the backend unreachable entirely. Pruning here
// would delete the very record that tells a future sweep which machine holds
// the lease — converting a transient outage into a permanently unplaced lease.
func TestFleet_BackendUnreachable_DoesNotPrunePlacement(t *testing.T) {
	t.Parallel()
	f := prunableFleet(t)

	f.backendAt(2).seedProvision(t, "lease-x", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-x", billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-x", "backend-2")

	f.removeLease("lease-x")
	f.backendAt(2).setFault(faultConnReset)

	_ = f.sweepN(2)
	f.assertPlacementPinned("lease-x", "backend-2")
}

// --------------------------------------------------------------------------
// Recovery
// --------------------------------------------------------------------------

// After the fault clears, the deferred lease must reconcile — and must not have
// been provisioned a second time along the way.
func TestFleet_FaultThenRecover_ProvisionsExactlyOnce(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-recover", billingtypes.LEASE_STATE_PENDING)

	// backend-1 is the default target; fault it before the lease is ever placed.
	f.backendAt(1).setFault(faultHTTP500)
	_ = f.sweepN(2)

	f.backendAt(1).setFault(faultNone)
	require.NoError(t, f.sweepN(2))

	f.assertProvisionedExactlyOnce("lease-recover")
}

// --------------------------------------------------------------------------
// The behaviour ENG-356 replaces
// --------------------------------------------------------------------------

// This is the baseline being removed, pinned explicitly so the change is
// visible in the diff rather than implicit. ENG-356 rewrites this test: the
// sweep will stop returning an error, the healthy backends' leases will be
// reconciled, and only leases attributable to the silent backend will be
// skipped.
func TestFleet_DegradedSweep_CurrentlyAbortsForEveryLease(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A lease on a HEALTHY backend, ready to be acknowledged.
	f.addLease("lease-on-healthy", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(1).seedProvision(t, "lease-on-healthy", f.providerUUID, backend.ProvisionStatusReady)

	// One unrelated backend is unreachable.
	f.backendAt(3).setFault(faultConnReset)

	err := f.sweep()

	require.Error(t, err, "today any backend failure aborts the whole sweep")
	require.Contains(t, err.Error(), "incomplete backend data")

	acked, _, _ := f.chainCalls()
	require.NotContains(t, acked, "lease-on-healthy",
		"today a lease on a healthy backend is collateral damage of another backend's outage")
}

// The circuit breaker is part of the transport, so an unreachable backend stops
// being contacted at all once it trips. That matters for ENG-356: a tripped
// breaker must mark the backend unanswered without a retry, and this pins the
// observable — the client stops issuing HTTP requests entirely.
func TestFleet_RepeatedFailures_OpenCircuitStopsHTTPRequests(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-cb", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).setFault(faultHTTP500)

	// Default CBFailureThresh is 5 consecutive failures.
	_ = f.sweepN(5)
	callsAtTrip := f.backendAt(2).listCallCount()

	_ = f.sweepN(2)
	callsAfter := f.backendAt(2).listCallCount()

	require.Equal(t, callsAtTrip, callsAfter,
		"once the breaker is open the client must short-circuit instead of dialing the backend")
}
