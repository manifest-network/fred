package provisioner

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// retireBackend runs the offline retirement exactly as
// placement-repair -retire-lost-backend -apply does, then restarts providerd
// with the rewritten config that no longer names the backend.
func (f *fleet) retireBackend(name string) {
	f.t.Helper()
	require.NoError(f.t, f.placement.Close())
	repair, err := placement.OpenAttemptRepair(f.placementPath, f.providerUUID)
	require.NoError(f.t, err)
	pin, bound := repair.ExpectedBackendStorageIdentity(name)
	require.True(f.t, bound)
	plan, err := repair.PlanBackendRetirement(name, pin)
	require.NoError(f.t, err)
	target, err := placement.BindExactBackupTarget(filepath.Join(f.t.TempDir(), "pre-retirement.db"))
	require.NoError(f.t, err)
	require.NoError(f.t, repair.CreateExactBackup(target))
	_, err = repair.RetireBackend(plan, placement.LostBackendAttestationText)
	require.NoError(f.t, err)
	require.NoError(f.t, repair.Sync())
	require.NoError(f.t, repair.Close())
	require.NoError(f.t, target.Close())

	survivors := slices.DeleteFunc(slices.Clone(f.routerEntries), func(entry backend.BackendEntry) bool {
		return entry.Backend.Name() == name
	})
	router, err := backend.NewRouter(backend.RouterConfig{Backends: survivors})
	require.NoError(f.t, err)
	f.router, f.routerEntries = router, survivors
	f.restartReconciler()
}

func TestRetiredBackendLeasesAreClosedRejectedAndPrunedNeverReprovisioned(t *testing.T) {
	f := newFleet(t, fleetOptions{
		interval:     time.Second,
		placementAge: time.Hour,
		backendSKUs:  map[int][]string{3: {"sku-three"}},
	})
	require.NoError(t, f.sweep(), "establish baseline")
	const (
		active    = "lease-active-on-lost-backend"
		pending   = "lease-pending-on-lost-backend"
		survivor  = "lease-on-survivor"
		retiredBy = "backend-3"
	)
	provisionAndActivateOn(t, f, active, 3, "sku-three")
	f.addLease(pending, billingtypes.LEASE_STATE_PENDING, "sku-three")
	require.NoError(t, f.sweep())
	require.Equal(t, 1, f.backendAt(3).provisionCount(pending))
	f.addLease(survivor, billingtypes.LEASE_STATE_ACTIVE)
	f.backendAt(1).seedProvision(t, survivor, f.providerUUID, backend.ProvisionStatusReady)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned(survivor, "backend-1")
	provisionsBefore := map[string]int{}
	for _, srv := range f.servers {
		provisionsBefore[srv.name] = srv.totalProvisionCalls()
	}

	f.retireBackend(retiredBy)
	require.NoError(t, f.sweepN(3))

	_, rejected, closed := f.chainCalls()
	require.Contains(t, closed, fleetLeaseUUID(active), "the lost ACTIVE lease is closed")
	require.Contains(t, rejected, fleetLeaseUUID(pending), "the lost PENDING lease is rejected")
	require.NotContains(t, closed, fleetLeaseUUID(survivor))
	require.NotContains(t, rejected, fleetLeaseUUID(survivor))
	for _, srv := range f.servers {
		require.Equalf(t, provisionsBefore[srv.name], srv.totalProvisionCalls(),
			"no lost lease may be re-provisioned on %s", srv.name)
	}
	f.assertPlacementPinned(survivor, "backend-1")

	// Once the chain shows them terminal, the lost rows are pruned.
	f.closeLease(active)
	f.closeLease(pending)
	require.NoError(t, f.sweepN(3))
	for _, alias := range []string{active, pending} {
		_, lost := f.placement.Lookup(fleetLeaseUUID(alias)).LostBackend()
		require.False(t, lost, "%s must be pruned after an exact terminal read", alias)
		require.Equal(t, placement.StateAbsent, f.placement.Lookup(fleetLeaseUUID(alias)).State())
	}
}

// provisionAndActivateOn provisions a PENDING lease on the given backend,
// acknowledges it once the backend reports Ready, and activates it on chain.
func provisionAndActivateOn(t *testing.T, f *fleet, alias string, backendIndex int, sku string) {
	t.Helper()
	f.addLease(alias, billingtypes.LEASE_STATE_PENDING, sku)
	require.NoError(t, f.sweep())
	require.Equal(t, 1, f.backendAt(backendIndex).provisionCount(alias))
	f.backendAt(backendIndex).mock.SetProvisionStatus(fleetLeaseUUID(alias), backend.ProvisionStatusReady)
	// The harness never delivers the success callback, so a restart clears the
	// in-flight operation and lets reconciliation acknowledge the Ready lease.
	f.restartReconciler()
	require.NoError(t, f.sweep())
	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, fleetLeaseUUID(alias))
	f.addLease(alias, billingtypes.LEASE_STATE_ACTIVE, sku)
	require.NoError(t, f.sweep())
	f.assertPlacementPinned(alias, f.backendAt(backendIndex).name)
}

// TestSurvivorReportingALostLeaseNeitherAdoptsNorWedgesIt pins the absorbing
// rule: a survivor that reports a lost lease (a stale copy, or a container an
// operator restored by hand) cannot turn it back into a live placement, and
// the sweep keeps serving every other lease.
func TestSurvivorReportingALostLeaseNeitherAdoptsNorWedgesIt(t *testing.T) {
	f := newFleet(t, fleetOptions{
		interval:     time.Second,
		placementAge: time.Hour,
		backendSKUs:  map[int][]string{3: {"sku-three"}},
	})
	require.NoError(t, f.sweep(), "establish baseline")
	const lostLease, newcomer = "lease-lost-then-reported", "lease-newcomer"
	provisionAndActivateOn(t, f, lostLease, 3, "sku-three")
	f.retireBackend("backend-3")
	f.backendAt(1).seedProvision(t, fleetLeaseUUID(lostLease), f.providerUUID, backend.ProvisionStatusReady)

	require.NoError(t, f.sweepN(3))
	lostBackend, lost := f.placement.Lookup(fleetLeaseUUID(lostLease)).LostBackend()
	require.True(t, lost, "a survivor's report must not resurrect a lost placement")
	require.Equal(t, "backend-3", lostBackend)
	_, _, closed := f.chainCalls()
	require.Contains(t, closed, fleetLeaseUUID(lostLease))

	f.addLease(newcomer, billingtypes.LEASE_STATE_PENDING)
	require.NoError(t, f.sweepN(2))
	require.Equal(t, 1, f.backendAt(1).provisionCount(newcomer), "admission still serves new leases")

	// Once the lease is terminal the lost row is pruned, and the survivor's
	// copy is an ordinary orphan.
	f.closeLease(lostLease)
	require.NoError(t, f.sweepN(4))
	_, lost = f.placement.Lookup(fleetLeaseUUID(lostLease)).LostBackend()
	require.False(t, lost)
	require.Positive(t, f.backendAt(1).deprovisionCount(lostLease))
}

// TestUnprovenRetirementClosesARecordlessActiveLease covers a retirement that
// could not prove every live lease had a placement row: an ACTIVE lease with
// no row may have lived on the lost storage, so it is closed as lost instead
// of being provisioned empty on a survivor.
func TestUnprovenRetirementClosesARecordlessActiveLease(t *testing.T) {
	f := newFleet(t, fleetOptions{
		interval:     time.Second,
		placementAge: time.Hour,
		backendCount: 3,
	})
	require.NoError(t, f.sweep(), "establish baseline")
	// The first retirement clears the admission baseline; retiring a second
	// backend before any sweep re-establishes it is therefore unproven.
	f.retireBackend("backend-3")
	f.retireBackend("backend-2")
	require.True(t, f.placement.RecordlessLeasesUnproven())

	const recordless = "lease-active-without-row"
	f.addLease(recordless, billingtypes.LEASE_STATE_ACTIVE)
	require.NoError(t, f.sweepN(3))
	_, _, closed := f.chainCalls()
	require.Contains(t, closed, fleetLeaseUUID(recordless))
	require.Zero(t, f.backendAt(1).provisionCount(recordless), "never provisioned empty on a survivor")
}
