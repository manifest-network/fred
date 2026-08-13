package provisioner

// Characterization tests for reconciling a multi-backend fleet over the real
// HTTP transport.
//
// These pin what a sweep must NOT destroy when it cannot see the whole fleet.
//
// They were written and merged BEFORE ENG-356 (#212), against the code that
// still aborted a degraded sweep outright — where the "nothing was destroyed"
// invariant held trivially. ENG-356 then removed that abort, and the same
// assertions still hold, now for the interesting reason: the sweep proceeds and
// defers exactly the leases it cannot positively attribute.
//
// That equivalence is the point, and it is why these tests are worth keeping in
// this shape. Exactly one scenario changed across that transition — the one
// written to pin the replaced behavior. If a future change turns any of the
// others red, the default assumption is a regression, not a test that needs
// updating.

import (
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
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
// The destructive tail is gated on a complete fleet view
// --------------------------------------------------------------------------

// Orphan classification is safe against partial BACKEND data — a silent backend
// contributes no candidates — but not against a stale CHAIN snapshot. The two
// chain queries are not atomic, so a lease created between them is invisible to
// both while the event path may already be provisioning it. Until ENG-356 the
// fleet-wide abort suppressed the orphan pass on exactly these sweeps; letting
// it run now would newly expose that hazard.
//
// So the orphan pass is skipped whole while the fleet view is incomplete, even
// for candidates on a backend that DID answer.
func TestFleet_DegradedSweep_SkipsOrphanDeprovisionEvenOnAnsweringBackend(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A textbook orphan on a HEALTHY backend: provisioned, no chain lease.
	f.backendAt(1).seedProvision(t, "lease-orphan", f.providerUUID, backend.ProvisionStatusReady)
	// An unrelated backend is unreachable.
	f.backendAt(3).setFault(faultConnReset)

	require.NoError(t, f.sweepN(2))

	require.Zero(t, f.backendAt(1).deprovisionCount("lease-orphan"),
		"orphan GC must not run while the fleet view is incomplete, even for a candidate on an answering backend")

	// Control: once the fleet is whole again, the same orphan IS reaped — so the
	// assertion above is a gate, not a permanent leak.
	f.backendAt(3).setFault(faultNone)
	require.NoError(t, f.sweep())
	require.Equal(t, 1, f.backendAt(1).deprovisionCount("lease-orphan"),
		"orphan GC must resume once every backend answers")
}

// cleanupOrphanedPayloads deletes the payloads of leases that are gone from the
// chain. It reads the same possibly-stale chain snapshot, and it too was
// suppressed by the abort, so it is gated the same way.
func TestFleet_DegradedSweep_SkipsPayloadCleanup(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A payload with no chain lease behind it — the shape the cleaner deletes.
	require.True(t, f.payloads.Store("lease-gone", []byte("manifest-bytes")))
	f.backendAt(2).setFault(faultConnReset)

	require.NoError(t, f.sweepN(2))

	has, err := f.payloads.Has("lease-gone")
	require.NoError(t, err)
	require.True(t, has,
		"payload cleanup must not run while the fleet view is incomplete")

	// Control: it IS cleaned once the fleet is whole, so the gate above is not
	// hiding a permanent leak.
	f.backendAt(2).setFault(faultNone)
	require.NoError(t, f.sweep())

	has, err = f.payloads.Has("lease-gone")
	require.NoError(t, err)
	require.False(t, has, "payload cleanup must resume once every backend answers")
}

// deferLease trusts exactly two inputs: membership in snapshot.provisions, and
// the placement record. The tests below pin the placement half; this one pins
// the other, at the source.
//
// The property is that snapshot.provisions can only ever name a backend that
// actually answered — every entry is inserted after that backend's
// ListProvisions succeeded, with BackendName overwritten by the answering
// backend's own name rather than trusted from the wire. If a non-answering
// backend could contribute even one entry, `isProvisioned` would short-circuit
// the guard into PROCEED on evidence that does not exist.
func TestFleet_FleetSnapshot_ExcludesNonAnsweringBackendAndStampsOwner(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.backendAt(1).seedProvision(t, "lease-a", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(2).seedProvision(t, "lease-b", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(3).seedProvision(t, "lease-c", f.providerUUID, backend.ProvisionStatusReady)

	f.backendAt(2).setFault(faultConnReset)

	snap := f.reconciler.fetchFleetSnapshot(t.Context())

	require.False(t, snap.complete, "one backend did not answer")
	require.Equal(t, map[string]bool{
		"backend-1": true,
		"backend-2": false,
		"backend-3": true,
	}, snap.answered, "answered must record every configured backend, false for the silent one")
	require.Equal(t, []string{"backend-2"}, snap.unansweredBackends())

	// The silent backend's lease must be absent — not present-with-stale-data.
	require.NotContains(t, snap.provisions, "lease-b",
		"a backend that did not answer must contribute nothing")

	// Answering backends' leases are present and attributed to them.
	require.Equal(t, "backend-1", snap.provisions["lease-a"].BackendName)
	require.Equal(t, "backend-3", snap.provisions["lease-c"].BackendName)
}

// The placement sync runs BEFORE the per-lease guard and is read back by it in
// the same sweep, so anything it writes becomes evidence immediately. A
// retention proves a past deprovision on a backend, not present ownership —
// deriving a placement from one during a degraded sweep would manufacture the
// guard's own input, flipping a lease that should DEFER into one that PROCEEDS
// and aiming it at the wrong backend. Durably, in bbolt, outliving the outage.
//
// It would also aim DEPROVISION at that backend, turning what should be a loud
// failure against the real holder into a resolved teardown on a machine that
// holds nothing — reported as success while the containers keep running.
func TestFleet_DegradedSweep_DoesNotManufacturePlacementFromRetention(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// backend-1 answers and reports a RETENTION for the lease.
	f.backendAt(1).seedRetention("lease-r")
	// backend-2 is the silent one.
	f.backendAt(2).setFault(faultConnReset)
	// The lease is ACTIVE on chain with no placement record — the state in which
	// a manufactured record does the damage.
	f.addLease("lease-r", billingtypes.LEASE_STATE_ACTIVE)

	require.NoError(t, f.sweepN(2))

	require.Empty(t, f.placement.Get("lease-r"),
		"a degraded sweep must not write a placement derived from retention data")

	// And the guard must still be deferring — otherwise the assertion above
	// could pass for the wrong reason (e.g. the lease being handled some other way).
	for _, srv := range f.servers {
		require.Zerof(t, srv.totalProvisionCalls(),
			"the lease must be deferred, not provisioned (%s)", srv.name)
	}
}

// The companion that keeps the fix from being over-broad: on a COMPLETE sweep
// the retention-derived backfill still runs, which is what keeps restore
// affinity working for a closed lease whose placement record was lost.
func TestFleet_CompleteSweep_StillBackfillsPlacementFromRetention(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.backendAt(1).seedRetention("lease-r")

	require.NoError(t, f.sweep())

	require.Equal(t, "backend-1", f.placement.Get("lease-r"),
		"a complete sweep must still backfill placement from retention data")
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
// The behavior ENG-356 introduces
// --------------------------------------------------------------------------

// The headline change, and the inverse of what this test asserted before
// ENG-356. Previously ANY backend failing aborted the whole sweep, so a lease
// sitting ready on a perfectly healthy backend was collateral damage of an
// unrelated backend's outage — and recovery for every lease degraded as p^n,
// getting worse as the fleet grew.
//
// Now the sweep proceeds, the healthy backends' leases are reconciled, and only
// leases attributable to the silent backend are skipped.
func TestFleet_DegradedSweep_ReconcilesLeasesOnHealthyBackends(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A lease on a HEALTHY backend, ready to be acknowledged.
	f.addLease("lease-on-healthy", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(1).seedProvision(t, "lease-on-healthy", f.providerUUID, backend.ProvisionStatusReady)

	// One unrelated backend is unreachable.
	f.backendAt(3).setFault(faultConnReset)

	require.NoError(t, f.sweep(),
		"a single unreachable backend must no longer fail the sweep")

	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, "lease-on-healthy",
		"a lease on a healthy backend must be reconciled despite another backend's outage")
}

// The other half of the same rule: acting on the healthy backends must not mean
// acting on the silent one's leases. This is the pairing that makes the change
// safe rather than merely less conservative — and note the two leases are
// reconciled in the SAME sweep, so the guard is per-lease, not per-sweep.
func TestFleet_DegradedSweep_ActsOnHealthyAndDefersSilentInOneSweep(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// Ready on a healthy backend → should be acknowledged.
	f.addLease("lease-healthy", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(1).seedProvision(t, "lease-healthy", f.providerUUID, backend.ProvisionStatusReady)

	// ACTIVE on the backend that is about to go silent → must be left alone.
	f.addLease("lease-silent", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(3).seedProvision(t, "lease-silent", f.providerUUID, backend.ProvisionStatusReady)

	// Establish placement while everything is healthy.
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-silent", "backend-3")

	f.backendAt(3).setFault(faultConnReset)

	before := f.captureState([]string{"lease-silent"})
	require.NoError(t, f.sweep())

	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, "lease-healthy", "the healthy backend's lease is reconciled")

	f.assertNothingDestroyed(before, []string{"lease-silent"})
	f.assertPlacementPinned("lease-silent", "backend-3")
}

// A degraded sweep must be visible. Before ENG-356 a fleet-wide outage was
// impossible to miss because reconciliation simply stopped; afterwards it
// costs only the affected leases, so without a distinct signal the failure
// becomes silent. outcome="degraded" is that signal, and the last-success
// timestamp must NOT advance — otherwise the staleness alert goes quiet during
// precisely the outage it exists to catch.
func TestFleet_DegradedSweep_EmitsDegradedOutcomeAndWithholdsSuccess(t *testing.T) {
	// Deliberately NOT parallel: these are process-global Prometheus collectors,
	// and every other fleet test drives sweeps that move the same counters. Go
	// runs sequential tests in isolation from parallel ones, which is what makes
	// the before/after deltas below meaningful rather than flaky.
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-x", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(1).seedProvision(t, "lease-x", f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())
	lastSuccessAfterClean := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)
	require.NotZero(t, lastSuccessAfterClean, "a clean sweep should record success")

	degradedBefore := promtestutil.ToFloat64(
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeDegraded))

	f.backendAt(2).setFault(faultConnReset)
	require.NoError(t, f.sweep())

	degradedAfter := promtestutil.ToFloat64(
		metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeDegraded))
	assert.Equal(t, degradedBefore+1, degradedAfter,
		"a sweep that could not see the whole fleet must be counted as degraded exactly once")

	assert.Equal(t, lastSuccessAfterClean,
		promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp),
		"a degraded sweep must not advance the last-success timestamp")

	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.ReconcilerSweepComplete),
		"sweep_complete must report 0 while the fleet view is incomplete")
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
