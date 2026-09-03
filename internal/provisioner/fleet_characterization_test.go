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
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
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

func TestFleet_V013UpgradeBackfillsExistingWorkloadsWithoutMovingThem(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// Model the first current-version startup over a v0.13 environment: the
	// process-local registry and the newly configured placement DB are empty,
	// while multiple backends already own live and retained tenant data.
	f.addLease("lease-pending-ready", billingtypes.LEASE_STATE_PENDING)
	f.addLease("lease-active", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(2).seedProvision(
		t, "lease-pending-ready", f.providerUUID, backend.ProvisionStatusReady,
	)
	f.backendAt(3).seedProvision(
		t, "lease-active", f.providerUUID, backend.ProvisionStatusReady,
	)
	f.backendAt(1).seedRetention("lease-retained")
	require.Zero(t, f.tracker.Operations().Count())
	require.Empty(t, f.placement.List())

	require.NoError(t, f.sweep())

	for _, srv := range f.servers {
		require.Zero(t, srv.totalProvisionCalls(),
			"startup discovery must not restart or move an existing workload through %s", srv.name)
		require.Zero(t, srv.deprovisionCount("lease-pending-ready"))
		require.Zero(t, srv.deprovisionCount("lease-active"))
		require.Zero(t, srv.deprovisionCount("lease-retained"))
	}
	f.assertPlacementPinned("lease-pending-ready", "backend-2")
	f.assertPlacementPinned("lease-active", "backend-3")
	f.assertPlacementPinned("lease-retained", "backend-1")
	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, fleetLeaseUUID("lease-pending-ready"),
		"a ready v0.13 workload still pending on chain must be acknowledged, not reprovisioned")
}

func TestFleet_ProvisionCarriesExactTypedOperationAcrossHTTP(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-typed", billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweep())

	req, ok := f.backendAt(1).provisionRequest("lease-typed")
	require.True(t, ok, "real HTTP backend should receive the provision request")
	callbackURL, err := url.Parse(req.CallbackURL)
	require.NoError(t, err)
	callbackID, present, err := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, err)
	require.True(t, present)

	record, tracked := f.tracker.Operations().Lookup(fleetLeaseUUID("lease-typed"))
	require.True(t, tracked)
	require.Equal(t, operation.KindProvision, record.Kind)
	require.Equal(t, callbackID, record.ID,
		"the callback capability crossing HTTP must identify the tracked operation")
	require.Equal(t, "backend-1", record.Backend)

	p := f.placement.Lookup(fleetLeaseUUID("lease-typed"))
	require.Equal(t, placement.StateConfirmed, p.State())
	require.Equal(t, "backend-1", p.Backend)
	require.Empty(t, p.Attempt)
}

func TestFleet_CompleteInventoryNeverClearsAmbiguousAttemptFromSilence(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})
	require.NoError(t, f.sweep(), "arm startup placement authority")

	operationID, tracked := f.tracker.TryTrackInFlightWithOperationID(
		fleetLeaseUUID("lease-ambiguous"), "tenant-a", nil, "backend-2",
	)
	require.True(t, tracked)
	baseline := f.placement.CurrentAdmissionBaseline()
	scope, err := f.placement.ScopeAdmission(baseline, backendTopologyNames(f.router))
	require.NoError(t, err)
	_, applied, err := f.placement.BeginNewAttempt(
		scope,
		fleetLeaseUUID("lease-ambiguous"), "backend-2", operationID,
		placement.PayloadFingerprint{}, testBackendRequestSnapshot(t),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, f.tracker.UntrackInFlightIfOperationID(
		fleetLeaseUUID("lease-ambiguous"), operationID),
		"model an ambiguous synchronous response that retained only durable intent")
	require.Equal(t, placement.StateAttempting,
		f.placement.Lookup(fleetLeaseUUID("lease-ambiguous")).State())

	require.NoError(t, f.sweep())

	require.Equal(t, placement.StateAttempting,
		f.placement.Lookup(fleetLeaseUUID("lease-ambiguous")).State(),
		"inventory silence cannot prove that an ambiguously timed-out request never committed later")
	for _, srv := range f.servers {
		require.Zero(t, srv.provisionCount("lease-ambiguous"),
			"settling an inventory-disproved attempt must not manufacture a backend call")
	}
}

func TestFleet_IncompleteInventoryKeepsUnresolvedAttempt(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})
	require.NoError(t, f.sweep(), "arm startup placement authority")

	operationID, tracked := f.tracker.TryTrackInFlightWithOperationID(
		fleetLeaseUUID("lease-unknown"), "tenant-a", nil, "backend-2",
	)
	require.True(t, tracked)
	baseline := f.placement.CurrentAdmissionBaseline()
	scope, err := f.placement.ScopeAdmission(baseline, backendTopologyNames(f.router))
	require.NoError(t, err)
	_, applied, err := f.placement.BeginNewAttempt(
		scope, fleetLeaseUUID("lease-unknown"), "backend-2", operationID,
		placement.PayloadFingerprint{}, testBackendRequestSnapshot(t),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, f.tracker.UntrackInFlightIfOperationID(
		fleetLeaseUUID("lease-unknown"), operationID))
	f.backendAt(2).setFault(faultRetentionsOnly)

	require.NoError(t, f.sweep())

	p := f.placement.Lookup(fleetLeaseUUID("lease-unknown"))
	require.Equal(t, placement.StateAttempting, p.State())
	require.Equal(t, "backend-2", p.Attempt,
		"a missing half of the attempted backend's inventory cannot prove absence")
}

func TestFleet_IncompleteRetentionInventoryCannotAuthorizeProvision(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})
	f.addLease("lease-no-authority", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(3).setFault(faultRetentionsOnly)

	require.NoError(t, f.sweep())

	for _, srv := range f.servers {
		require.Zero(t, srv.provisionCount("lease-no-authority"),
			"a partial retention inventory must not authorize a backend side effect on %s", srv.name)
	}
	require.False(t, f.tracker.Operations().Contains(fleetLeaseUUID("lease-no-authority")),
		"a refused pre-side-effect operation must be released")
	require.Equal(t, placement.StateAbsent,
		f.placement.Lookup(fleetLeaseUUID("lease-no-authority")).State())
}

func TestFleet_HealthyFleet_AcknowledgesReadyLease(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	f.addLease("lease-ready", billingtypes.LEASE_STATE_PENDING)
	f.backendAt(2).seedProvision(t, "lease-ready", f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweep())

	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, fleetLeaseUUID("lease-ready"))
}

func TestFleet_HealthyFleet_DeprovisionsOrphan(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A provision whose lease has closed on chain: gone from the PENDING/ACTIVE
	// lists, still resolvable per-lease as CLOSED.
	f.addLease("lease-orphan", billingtypes.LEASE_STATE_ACTIVE)
	f.closeLease("lease-orphan")
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
	require.Contains(t, acked, fleetLeaseUUID("lease-slow"))
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

// The production data-loss shape, stated exactly. Before any complete fleet
// projection establishes a durable absence baseline, a recordless lease whose
// backend went quiet must not be treated as genuinely new. Otherwise the
// ACTIVE && !isProvisioned row could hand it to a healthy peer and lay a new
// empty volume over live tenant data.
func TestFleet_UnplacedLeaseOnFaultedBackend_IsNotProvisionedOnAPeer(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

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

// A complete inventory durably establishes that recordless means never placed
// under this backend topology. A later outage therefore narrows new admission
// to the backends that answered both endpoints instead of pausing the fleet.
func TestFleet_PriorCompleteSweepAllowsNewPendingLeaseOnHealthyBackend(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// Empty is intentional: a successful no-op projection establishes the
	// durable topology-bound baseline used by later degraded sweeps.
	require.NoError(t, f.sweep())
	require.True(t, f.reconciler.placementSweepSeen.Load())

	f.backendAt(3).setFault(faultConnReset)
	f.addLease("lease-after-trust", billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweep())
	require.Zero(t, f.backendAt(3).provisionCount("lease-after-trust"),
		"routing must never escape the set of backends that answered both inventories")
	f.assertProvisionedExactlyOnce("lease-after-trust")
	require.Equal(t, placement.StateConfirmed,
		f.placement.Lookup(fleetLeaseUUID("lease-after-trust")).State())

	// Recovery must not duplicate the already admitted operation.
	f.backendAt(3).setFault(faultNone)
	require.NoError(t, f.sweep())
	f.assertProvisionedExactlyOnce("lease-after-trust")
}

func TestFleet_DurableBaselineSurvivesRestartAndBackendOutage(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})
	require.NoError(t, f.sweep(), "establish durable topology baseline")
	require.True(t, f.placement.CurrentAdmissionBaseline().Valid())

	f.restartReconciler()
	require.True(t, f.placement.CurrentAdmissionBaseline().Valid(),
		"the admission baseline must survive reopening the placement database")

	f.backendAt(3).setFault(faultConnReset)
	f.addLease("lease-after-restart", billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweep())

	require.Zero(t, f.backendAt(3).provisionCount("lease-after-restart"))
	f.assertProvisionedExactlyOnce("lease-after-restart")

	f.backendAt(3).setFault(faultNone)
	require.NoError(t, f.sweep())
	f.assertProvisionedExactlyOnce("lease-after-restart")
}

func TestFleet_DegradedAdmissionIsPendingOnlyAndRequiresBothInventories(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{backendSKUs: map[int][]string{
		2: {"sku-preferred-but-ineligible"},
	}})
	require.NoError(t, f.sweep(), "establish durable topology baseline")

	// This ACTIVE lease may already have been placed after the baseline was
	// recorded (for example by a v0.13 process). With its actual owner silent,
	// recordlessness is not enough to authorize a move.
	f.addLease("lease-active-unknown", billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(3).seedProvision(
		t, "lease-active-unknown", f.providerUUID, backend.ProvisionStatusReady,
	)
	f.backendAt(3).setFault(faultConnReset)

	// backend-2 answers /provisions but not /retentions. Only backend-1 is an
	// eligible destination for a genuinely new PENDING lease.
	f.backendAt(2).setFault(faultRetentionsOnly)
	f.addLease(
		"lease-new-degraded", billingtypes.LEASE_STATE_PENDING,
		"sku-preferred-but-ineligible",
	)

	require.NoError(t, f.sweep())

	require.Equal(t, 1, f.backendAt(1).provisionCount("lease-new-degraded"))
	require.Zero(t, f.backendAt(2).provisionCount("lease-new-degraded"))
	require.Zero(t, f.backendAt(3).provisionCount("lease-new-degraded"))
	f.assertProvisionedExactlyOnce("lease-new-degraded")
	for _, srv := range f.servers {
		require.Zero(t, srv.provisionCount("lease-active-unknown"),
			"an ACTIVE recordless lease must not move during an outage via %s", srv.name)
	}
	require.Equal(t, placement.StateAbsent,
		f.placement.Lookup(fleetLeaseUUID("lease-active-unknown")).State())
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
	require.True(t, f.payloads.Store(fleetLeaseUUID("lease-payload"), []byte("manifest-bytes")))

	require.NoError(t, f.sweep())
	f.backendAt(2).setFault(faultHTTP500)

	before := f.captureState([]string{"lease-payload"})
	_ = f.sweepN(2)
	f.assertNothingDestroyed(before, []string{"lease-payload"})

	has, err := f.payloads.Has(fleetLeaseUUID("lease-payload"))
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
	f.closeLease("lease-x")
	f.backendAt(2).mock.Clear()

	require.NoError(t, f.sweep())
	require.Empty(t, f.placement.Lookup(fleetLeaseUUID("lease-x")).Backend,
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
	f.closeLease("lease-x")
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

	f.closeLease("lease-x")
	f.backendAt(2).setFault(faultConnReset)

	_ = f.sweepN(2)
	f.assertPlacementPinned("lease-x", "backend-2")
}

// --------------------------------------------------------------------------
// The destructive tail is scoped per lease and per backend
// --------------------------------------------------------------------------
//
// ENG-356 gated all three destructive passes on a complete fleet view, which
// preserved the abort's behaviour exactly. ENG-654 replaced that with the two
// guards the hazards actually call for — per-candidate chain confirmation for
// the stale-snapshot hazard, per-record backend attribution for the partial-
// fleet one — so one silent machine no longer pauses cleanup for the rest.
//
// The tests in this block were inverted at that point. The controls elsewhere in
// this file were not, and are what keeps the inversion honest:
// TestFleet_OrphanOnFaultedBackend_IsNotDeprovisioned and
// TestFleet_PayloadForLiveLease_SurvivesDegradedSweep still assert that a silent
// backend's orphan and a live lease's payload are untouched.

// Orphan classification is safe against partial BACKEND data: candidates are
// provisions minus chain leases, and a backend that did not answer contributes
// no provisions, so partial data can only under-collect. The gate never
// protected the candidate on the answering backend — it only delayed it.
func TestFleet_DegradedSweep_ReapsOrphanOnAnsweringBackend(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A textbook orphan on a HEALTHY backend: provisioned, closed on chain.
	f.addLease("lease-orphan", billingtypes.LEASE_STATE_ACTIVE)
	f.closeLease("lease-orphan")
	f.backendAt(1).seedProvision(t, "lease-orphan", f.providerUUID, backend.ProvisionStatusReady)
	// An unrelated backend is unreachable.
	f.backendAt(3).setFault(faultConnReset)

	require.NoError(t, f.sweep())

	require.Equal(t, 1, f.backendAt(1).deprovisionCount("lease-orphan"),
		"an unrelated backend's outage must not hold an answering backend's orphan")
}

// The stale-chain hazard the old gate was standing in for, now guarded directly:
// a candidate the chain reports live is never deprovisioned, and — unlike the
// fleet-wide gate — that holds on a COMPLETE sweep too, which is where the
// hazard always existed.
func TestFleet_CompleteSweep_DoesNotReapOrphanTheChainReportsLive(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// Provisioned, and still ACTIVE on chain — but invisible to this sweep's
	// list queries, exactly like a lease created between the two of them.
	f.backendAt(1).seedProvision(t, "lease-racing", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-racing", billingtypes.LEASE_STATE_UNSPECIFIED)

	require.NoError(t, f.sweepN(2))

	require.Zero(t, f.backendAt(1).deprovisionCount("lease-racing"),
		"a lease the chain reports live must never be reaped, however the sweep found it")
}

// Absence is not evidence. x/billing never deletes a lease, so a chain with no
// record of one is a phantom provision, a wrong or reset chain, or a lagging RPC
// node — none of which authorise destroying tenant state. This is also the
// blast-radius test: every list query comes back EMPTY, which before ENG-654
// made every provision on every backend an orphan candidate.
func TestFleet_ChainWithNoRecordOfAnyLease_DestroysNothing(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	for i := 1; i <= 3; i++ {
		f.backendAt(i).seedProvision(t, fmt.Sprintf("lease-%d", i), f.providerUUID, backend.ProvisionStatusReady)
	}
	require.True(t, f.payloads.Store(fleetLeaseUUID("lease-1"), []byte("manifest-bytes")))

	leases := []string{"lease-1", "lease-2", "lease-3"}

	// One sweep first, so the placement index is populated and the invariant
	// helper compares like with like — the additive sync CREATES records, and
	// this test is about what gets destroyed.
	require.NoError(t, f.sweep())
	before := f.captureState(leases)
	require.NoError(t, f.sweepN(2))
	f.assertNothingDestroyed(before, leases)

	for i := 1; i <= 3; i++ {
		require.Zero(t, f.backendAt(i).deprovisionCount(fmt.Sprintf("lease-%d", i)),
			"an empty chain is not an authorisation to empty the fleet")
	}
	has, err := f.payloads.Has(fleetLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.True(t, has)
}

// cleanupOrphanedPayloads reads the payload store and the chain. It has no
// backend input at all, so fleet completeness was never relevant to it.
func TestFleet_DegradedSweep_StillCleansOrphanedPayload(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	// A payload whose lease has closed — the shape the cleaner deletes.
	f.addLease("lease-gone", billingtypes.LEASE_STATE_ACTIVE)
	f.closeLease("lease-gone")
	require.True(t, f.payloads.Store(fleetLeaseUUID("lease-gone"), []byte("manifest-bytes")))
	f.backendAt(2).setFault(faultConnReset)

	require.NoError(t, f.sweep())

	has, err := f.payloads.Has(fleetLeaseUUID("lease-gone"))
	require.NoError(t, err)
	require.False(t, has, "a backend outage must not hold up a pass that never reads a backend")
}

// The payload pass's other half: a chain fred cannot reach must not be read as
// "the lease is gone". Deleting a live lease's payload makes the NEXT sweep see
// errPayloadNotAvailable, classify it permanent, and close a healthy ACTIVE
// lease on chain.
func TestFleet_UnreachableChain_KeepsPayload(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	require.True(t, f.payloads.Store(fleetLeaseUUID("lease-unknown"), []byte("manifest-bytes")))
	f.setGetLeaseErr(errors.New("chain unreachable"))

	require.NoError(t, f.sweepN(2))

	has, err := f.payloads.Has(fleetLeaseUUID("lease-unknown"))
	require.NoError(t, err)
	require.True(t, has, "a failed chain read is not evidence the lease finished")
}

// Placement pruning is the one pass that genuinely needs backend evidence — but
// per record, from that record's own backend. A silent backend must cost only
// its own records.
func TestFleet_DegradedSweep_PrunesOnlyAnsweringBackendsPlacements(t *testing.T) {
	t.Parallel()
	f := prunableFleet(t)

	f.backendAt(2).seedProvision(t, "lease-on-2", f.providerUUID, backend.ProvisionStatusReady)
	f.backendAt(3).seedProvision(t, "lease-on-3", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-on-2", billingtypes.LEASE_STATE_ACTIVE)
	f.addLease("lease-on-3", billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-on-2", "backend-2")
	f.assertPlacementPinned("lease-on-3", "backend-3")

	// Both leases close and both backends drop their resources; then backend-3
	// goes silent. Only backend-2 can now account for its own record.
	f.closeLease("lease-on-2")
	f.closeLease("lease-on-3")
	f.backendAt(2).mock.Clear()
	f.backendAt(3).mock.Clear()
	f.backendAt(3).setFault(faultConnReset)

	require.NoError(t, f.sweep())

	require.Empty(t, f.placement.Lookup(fleetLeaseUUID("lease-on-2")).Backend,
		"backend-2 answered, so absence from its report is evidence: prune")
	f.assertPlacementPinned("lease-on-3", "backend-3")
}

// The retentions axis of the same rule. A backend failing /retentions holds off
// pruning ITS records — TestFleet_RetentionsFailureAlone_DoesNotPrunePlacement
// pins that — but must not hold off another backend's.
func TestFleet_RetentionsFailureOnOnePeer_StillPrunesElsewhere(t *testing.T) {
	t.Parallel()
	f := prunableFleet(t)

	f.backendAt(2).seedProvision(t, "lease-on-2", f.providerUUID, backend.ProvisionStatusReady)
	f.addLease("lease-on-2", billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned("lease-on-2", "backend-2")

	f.closeLease("lease-on-2")
	f.backendAt(2).mock.Clear()
	// A DIFFERENT backend is failing /retentions only.
	f.backendAt(3).setFault(faultRetentionsOnly)

	require.NoError(t, f.sweep())

	require.Empty(t, f.placement.Lookup(fleetLeaseUUID("lease-on-2")).Backend,
		"a peer's retention outage says nothing about backend-2's records")
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
	require.Equal(t, answeredSet{
		"backend-1": true,
		"backend-2": false,
		"backend-3": true,
	}, snap.answered, "answered must record every configured backend, false for the silent one")
	require.Equal(t, []string{"backend-2"}, snap.unansweredBackends())

	// The silent backend's lease must be absent — not present-with-stale-data.
	require.NotContains(t, snap.provisions, fleetLeaseUUID("lease-b"),
		"a backend that did not answer must contribute nothing")

	// Answering backends' leases are present and attributed to them.
	require.Equal(t, "backend-1", snap.provisions[fleetLeaseUUID("lease-a")].BackendName)
	require.Equal(t, "backend-3", snap.provisions[fleetLeaseUUID("lease-c")].BackendName)
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

	require.Empty(t, f.placement.Lookup(fleetLeaseUUID("lease-r")).Backend,
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

	require.Equal(t, "backend-1", f.placement.Lookup(fleetLeaseUUID("lease-r")).Backend,
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
	require.Contains(t, acked, fleetLeaseUUID("lease-on-healthy"),
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
	require.Contains(t, acked, fleetLeaseUUID("lease-healthy"),
		"the healthy backend's lease is reconciled")

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
