package placement

import (
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestSoleCandidateQuarantinePrunesOnlyAfterExactTerminalAbsence(t *testing.T) {
	for _, scenario := range []string{
		"terminal absent", "chain live", "chain unknown", "provision still present",
		"retention still present", "retention unanswered", "storage changed", "claim held", "unknown historical owners",
	} {
		t.Run(scenario, func(t *testing.T) {
			reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
				Uuid: reconciliationSweepLease, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LEASE_STATE_CLOSED,
			}}
			store, _, coordinator := newReconciliationSweepFixture(t, &executionTestBackend{name: "backend-a"}, reader)
			_, _, err := coordinator.BindRecovery()
			require.NoError(t, err)
			baseline, _ := completeEmptyReconciliationSweep(t, coordinator)
			baseline.End()
			seed, err := coordinator.BeginSweep()
			require.NoError(t, err)
			storageID := testBackendStorageID("backend-a")
			require.NoError(t, seed.RecordProvision("backend-a", storageID, []backend.ProvisionInfo{{LeaseUUID: reconciliationSweepLease, BackendName: "backend-a"}}))
			require.NoError(t, seed.RecordRetention("backend-a", storageID, []string{reconciliationSweepLease}))
			require.NoError(t, seed.SealInventory())
			_, err = seed.Project(ReconciliationProjection{UntrustedPositives: map[string][]string{reconciliationSweepLease: {"backend-a"}}})
			require.NoError(t, err)
			seed.End()
			record := store.Lookup(reconciliationSweepLease)
			require.True(t, record.untrustedPositive)
			require.Equal(t, []string{"backend-a"}, record.ConflictBackends)
			if scenario == "unknown historical owners" {
				record.ConflictOwnersUnknown = true
				store.cache[reconciliationSweepLease] = record
			}
			switch scenario {
			case "chain live":
				reader.lease.State = billingtypes.LEASE_STATE_ACTIVE
			case "chain unknown":
				reader.lease = nil
			case "claim held":
				claim := coordinator.coordinator.operations.TryClaimLeaseNow(reconciliationSweepLease)
				require.True(t, claim.Acquired())
				defer coordinator.coordinator.operations.ReleaseLease(claim.Claim())
			case "storage changed":
				storageID = testBackendStorageID("replacement")
			}
			sweep, err := coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			var provisions []backend.ProvisionInfo
			var retentions []string
			projection := ReconciliationProjection{}
			if scenario == "provision still present" {
				provisions = []backend.ProvisionInfo{{LeaseUUID: reconciliationSweepLease, BackendName: "backend-a"}}
				projection.Placements = map[string]string{reconciliationSweepLease: "backend-a"}
			}
			if scenario == "retention still present" {
				retentions = []string{reconciliationSweepLease}
				projection.Placements = map[string]string{reconciliationSweepLease: "backend-a"}
			}
			require.NoError(t, sweep.RecordProvision("backend-a", storageID, provisions))
			if scenario != "retention unanswered" {
				require.NoError(t, sweep.RecordRetention("backend-a", storageID, retentions))
			}
			require.NoError(t, sweep.SealInventory())
			projected, err := sweep.Project(projection)
			if scenario == "storage changed" {
				require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
				require.NotEqual(t, StateAbsent, store.Lookup(reconciliationSweepLease).State())
				return
			}
			require.NoError(t, err)
			result := projected.PruneTerminalAbsence(t.Context(), reconciliationSweepLease)
			require.NoError(t, result.Err())
			require.Equal(t, scenario == "terminal absent", result.Deleted())
			if result.Deleted() {
				require.Equal(t, StateAbsent, store.Lookup(reconciliationSweepLease).State())
				sweep.End()
				path := store.db.Path()
				require.NoError(t, store.Close())
				reopened, err := OpenStore(path, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
				require.NoError(t, err)
				defer func() { require.NoError(t, reopened.Close()) }()
				require.Equal(t, StateAbsent, reopened.Lookup(reconciliationSweepLease).State())
			} else {
				require.NotEqual(t, StateAbsent, store.Lookup(reconciliationSweepLease).State())
			}
		})
	}
}
