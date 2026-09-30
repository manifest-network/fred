package provisioner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// attestRestoredPlacementBackup runs the offline attestation exactly as
// placement-repair -attest-restored-backup -apply does.
func attestRestoredPlacementBackup(t *testing.T, dbPath, providerUUID string) {
	t.Helper()
	repair, err := placement.OpenAttemptRepair(dbPath, providerUUID)
	require.NoError(t, err)
	defer func() { require.NoError(t, repair.Close()) }()
	plan, err := repair.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	require.True(t, plan.Facts().Required)
	target, err := placement.BindExactBackupTarget(filepath.Join(t.TempDir(), "pre-attestation.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Close()) }()
	require.NoError(t, repair.CreateExactBackup(target))
	_, err = repair.AttestRestoredBackup(plan)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
}

// A PENDING lease dispatched after a backup was taken has no row in the
// restored copy. If its owner is unreachable on the first sweep, the copy's
// admission baseline reads that absence as "never placed" and the lease is
// provisioned again on a peer. Attestation removes the baseline, so admission
// waits for the owner and adopts its provision exactly once.
func TestRestoredBackupAttestationStopsADuplicateProvisionOfADispatchedLease(t *testing.T) {
	const (
		dispatched = "lease-dispatched-after-backup"
		unchanged  = "lease-unchanged"
		newLease   = "lease-new-during-outage"
	)
	for _, attested := range []bool{false, true} {
		t.Run(map[bool]string{false: "unattested control", true: "attested"}[attested], func(t *testing.T) {
			f := newFleet(t, fleetOptions{
				interval:     time.Second,
				placementAge: time.Hour,
				backendSKUs:  map[int][]string{3: {"sku-three"}},
			})
			require.NoError(t, f.sweep(), "establish baseline")
			f.addLease(unchanged, billingtypes.LEASE_STATE_ACTIVE)
			f.backendAt(2).seedProvision(t, unchanged, f.providerUUID, backend.ProvisionStatusReady)
			require.NoError(t, f.sweep())

			// A stopped-process byte copy, as the documented backup procedure takes.
			require.NoError(t, f.placement.Close())
			backup, err := os.ReadFile(f.placementPath)
			require.NoError(t, err)
			f.restartReconciler()

			f.addLease(dispatched, billingtypes.LEASE_STATE_PENDING, "sku-three")
			require.NoError(t, f.sweep())
			require.Equal(t, 1, f.backendAt(3).provisionCount(dispatched))

			// The primary is lost before acknowledgement; the older copy is
			// restored while the lease's owner is unreachable.
			require.NoError(t, f.placement.Close())
			require.NoError(t, os.Remove(f.placementPath))
			require.NoError(t, os.WriteFile(f.placementPath, backup, 0o600))
			if attested {
				attestRestoredPlacementBackup(t, f.placementPath, f.providerUUID)
			}
			f.backendAt(3).setFault(faultConnReset)
			f.restartReconciler()
			require.Equal(t, attested, !f.placement.CurrentAdmissionBaseline().Valid())
			f.addLease(newLease, billingtypes.LEASE_STATE_PENDING)
			_ = f.sweepN(2)

			peers := f.backendAt(1).provisionCount(dispatched) + f.backendAt(2).provisionCount(dispatched)
			if !attested {
				require.Positive(t, peers,
					"control: the restored baseline admits the dispatched lease on a peer")
				return
			}
			require.Zero(t, peers, "an attested copy must not admit a lease its owner may hold")
			for _, srv := range f.servers {
				require.Zerof(t, srv.provisionCount(newLease),
					"new admission waits for one complete inventory (%s)", srv.name)
			}

			f.backendAt(3).setFault(faultNone)
			f.backendAt(3).mock.SetProvisionStatus(fleetLeaseUUID(dispatched), backend.ProvisionStatusReady)
			require.NoError(t, f.sweepN(3))
			f.assertProvisionedExactlyOnce(dispatched)
			f.assertPlacementPinned(dispatched, "backend-3")
			f.assertPlacementPinned(unchanged, "backend-2")
			require.True(t, f.placement.CurrentAdmissionBaseline().Valid(),
				"the first complete sweep re-establishes the baseline")
			acked, _, _ := f.chainCalls()
			require.Contains(t, acked, fleetLeaseUUID(dispatched))
			f.assertProvisionedExactlyOnce(newLease)
		})
	}
}
