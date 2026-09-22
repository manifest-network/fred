package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestPairedOverlapPreservesOnlyRepresentedOwner(t *testing.T) {
	for _, scenario := range []string{
		"same owner", "retired owner", "retired during sweep", "captured claim ended", "missing peer", "unknown owner",
		"unresolved attempt", "different generation", "different tenant", "partial principal",
		"unknown generation", "missing principal", "explicit rejection", "another reporter", "historical conflict",
		"unusable lifecycle", "lifecycle backend mismatch", "principal unbound on both sides", "legacy row on typed owner",
	} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newFencedAvailabilityFixture(t)
			leaseUUID := fencedAvailabilityOwner
			if scenario == "unknown owner" {
				leaseUUID = fencedAvailabilitySibling
			}
			if scenario == "unresolved attempt" {
				requireTypedAttempt(t, fixture.store, leaseUUID, "backend-a", requireOperationID(t, "2291"))
			}
			if scenario == "historical conflict" {
				requireConflictPlacement(t, fixture.store, leaseUUID, "backend-a", "backend-b")
			}
			if scenario == "retired owner" {
				retired, retireErr := fixture.store.retireLifecycle(leaseUUID, fixture.lifecycleID)
				require.NoError(t, retireErr)
				require.True(t, retired.RetiredNow())
			}
			switch scenario {
			case "unusable lifecycle", "lifecycle backend mismatch", "principal unbound on both sides":
				capability := fixture.store.lifecycleCache[leaseUUID]
				switch scenario {
				case "unusable lifecycle":
					capability.unusable = true
				case "lifecycle backend mismatch":
					capability.backend = "backend-b"
				case "principal unbound on both sides":
					capability.principal = runtimePrincipal{}
				}
				fixture.store.lifecycleCache[leaseUUID] = capability
			}
			before := fixture.store.Lookup(leaseUUID)
			lifecycleBefore := fixture.store.CurrentLifecycle(leaseUUID)
			var release func()
			if scenario == "captured claim ended" {
				claim := fixture.coordinator.coordinator.operations.TryClaimLeaseNow(leaseUUID)
				require.True(t, claim.Acquired())
				release = func() {
					require.True(t, fixture.coordinator.coordinator.operations.ReleaseLease(claim.Claim()))
				}
			}
			sweep, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			t.Cleanup(sweep.End)
			if release != nil {
				release()
			}
			if scenario == "retired during sweep" {
				retired, retireErr := fixture.store.retireLifecycle(leaseUUID, fixture.lifecycleID)
				require.NoError(t, retireErr)
				require.True(t, retired.RetiredNow())
				before = fixture.store.Lookup(leaseUUID)
				lifecycleBefore = fixture.store.CurrentLifecycle(leaseUUID)
			}
			row := backend.ProvisionInfo{
				LeaseUUID: leaseUUID, BackendName: "backend-a", ProviderUUID: freshTestProviderUUID,
				Tenant: "tenant-test", Status: backend.ProvisionStatusFailed, FailCount: 100,
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped, ID: fixture.lifecycleID.String(),
				},
			}
			switch scenario {
			case "different generation":
				row.LifecycleGeneration.ID = "123e4567-e89b-42d3-a456-426614174000"
			case "different tenant":
				row.Tenant = "other-tenant"
			case "partial principal":
				row.Tenant = ""
			case "unknown generation":
				row.LifecycleGeneration = nil
			case "missing principal", "principal unbound on both sides":
				row.Tenant, row.ProviderUUID = "", ""
			case "legacy row on typed owner":
				row.LifecycleGeneration = &backend.LifecycleGenerationObservation{Kind: backend.LifecycleGenerationLegacy}
			}
			projection := ReconciliationProjection{UntrustedPositives: map[string][]string{leaseUUID: {"backend-a"}}}
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				if scenario == "missing peer" && name == "backend-c" {
					continue
				}
				var rows []backend.ProvisionInfo
				var retained []string
				if name == "backend-a" {
					rows, retained = []backend.ProvisionInfo{row}, []string{leaseUUID}
				}
				if scenario == "another reporter" && name == "backend-b" {
					rows = []backend.ProvisionInfo{{LeaseUUID: leaseUUID, BackendName: name}}
					projection = ReconciliationProjection{Conflicts: map[string][]string{leaseUUID: {"backend-a", "backend-b"}}}
				}
				require.NoError(t, sweep.RecordProvision(name, testBackendStorageID(name), rows))
				require.NoError(t, sweep.RecordRetention(name, testBackendStorageID(name), retained))
			}
			if scenario == "explicit rejection" {
				require.NoError(t, sweep.RecordUntrusted("backend-a", []string{leaseUUID}))
			}
			require.NoError(t, sweep.SealInventory())
			_, err = sweep.Project(projection)
			require.NoError(t, err)
			sweep.End()
			switch scenario {
			case "same owner", "retired owner", "retired during sweep", "captured claim ended", "missing peer":
				assert.Equal(t, before, fixture.store.Lookup(leaseUUID))
				assert.Equal(t, lifecycleBefore, fixture.store.CurrentLifecycle(leaseUUID))
				assert.False(t, fixture.store.inventoryRecoveryRequired,
					"redundant overlap cannot strand a pending inventory marker")
				assert.False(t, fixture.coordinator.AbsenceUntrusted(leaseUUID))
				_, err = fixture.store.placementForDeprovision(leaseUUID)
				assert.NoError(t, err)
				path := fixture.store.db.Path()
				require.NoError(t, fixture.store.Close())
				reopened, openErr := OpenStore(path, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
				require.NoError(t, openErr)
				t.Cleanup(func() { require.NoError(t, reopened.Close()) })
				assert.Equal(t, StateConfirmed, reopened.Lookup(leaseUUID).State())
				assert.Equal(t, "backend-a", reopened.Lookup(leaseUUID).Backend)
				assert.False(t, reopened.inventoryRecoveryRequired)
			default:
				assert.Equal(t, StateUnusable, fixture.store.Lookup(leaseUUID).State())
				assert.Equal(t, before.Attempt, fixture.store.Lookup(leaseUUID).Attempt)
				_, err = beginTestRestore(t, fixture.store, fixture.store.CurrentAdmissionBaseline(),
					leaseUUID, "restore-target", requireOperationID(t, "2292"))
				assert.ErrorIs(t, err, ErrRestoreSourceUnavailable)
			}
		})
	}
}
