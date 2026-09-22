package placement

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestPairedOverlapLegacyOwnerPreservesDurablePrincipal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, path, map[string][]byte{fencedAvailabilityOwner: []byte("backend-a")})
	store, err := newStore(path, true, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b", "backend-c"}))
	require.Equal(t, LifecycleVerdictLegacy, store.CurrentLifecycle(fencedAvailabilityOwner).Verdict())
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(
		&executionTestBackend{name: "backend-a"}, &executionTestBackend{name: "backend-b"},
		&executionTestBackend{name: "backend-c"},
	))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
	require.NoError(t, err)
	row := backend.ProvisionInfo{
		LeaseUUID: fencedAvailabilityOwner, BackendName: "backend-a",
		Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
		LifecycleGeneration: &backend.LifecycleGenerationObservation{Kind: backend.LifecycleGenerationLegacy},
	}
	// The principal comes from a complete, sealed inventory after legitimate
	// legacy adoption. The later overlapping reads must not bootstrap it.
	projectPairedOverlapLineage(t, coordinator, row, false)
	before := store.Lookup(fencedAvailabilityOwner)
	capabilityBefore := store.lifecycleCache[fencedAvailabilityOwner]
	require.True(t, capabilityBefore.principal.valid())
	require.False(t, capabilityBefore.id.Valid())
	projectPairedOverlapLineage(t, coordinator, row, true)
	assert.Equal(t, before, store.Lookup(fencedAvailabilityOwner))
	assert.Equal(t, capabilityBefore, store.lifecycleCache[fencedAvailabilityOwner])
	assert.False(t, store.inventoryRecoveryRequired)
	require.NoError(t, store.Close())
	reopened, err := OpenStore(path, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	assert.Equal(t, StateConfirmed, reopened.Lookup(fencedAvailabilityOwner).State())
	assert.Equal(t, LifecycleVerdictLegacy, reopened.CurrentLifecycle(fencedAvailabilityOwner).Verdict())
	assert.Equal(t, capabilityBefore, reopened.lifecycleCache[fencedAvailabilityOwner])
}

func TestPairedOverlapCannotUseHistoricalAttemptOrLearnPrincipal(t *testing.T) {
	for _, scenario := range []string{"historical attempt marker", "unbound stored principal"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newFencedAvailabilityFixture(t)
			leaseUUID := fencedAvailabilityOwner
			row := backend.ProvisionInfo{
				LeaseUUID: leaseUUID, BackendName: "backend-a",
				Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped, ID: fixture.lifecycleID.String(),
				},
			}
			fixture.store.mu.Lock()
			before := fixture.store.cache[leaseUUID]
			capability := fixture.store.lifecycleCache[leaseUUID]
			switch scenario {
			case "historical attempt marker":
				// Deliberately retain an obsolete marker in the journal pair. A
				// diagnostic representation predicate can recognize such a marker;
				// preserving current ownership must instead require the current ID.
				capability.attemptBackend = "backend-a"
				capability.attemptID = requireLifecycleID(t, "2293")
				row.LifecycleGeneration.ID = capability.attemptID.String()
			case "unbound stored principal":
				// Older optional-principal rows are decodable, but an overlapping
				// observation cannot supply the missing tenant/provider authority.
				capability.principal = runtimePrincipal{}
			}
			err := fixture.store.putPlacementWithLifecycleLocked(leaseUUID, before, capability, "stage overlap lineage fixture")
			fixture.store.mu.Unlock()
			require.NoError(t, err)
			require.Empty(t, before.Attempt)
			projectPairedOverlapLineage(t, fixture.coordinator, row, true)
			assert.Equal(t, StateUnusable, fixture.store.Lookup(leaseUUID).State())
			assert.Equal(t, capability, fixture.store.lifecycleCache[leaseUUID],
				"rejection must not rotate the generation or fill a missing principal")
			_, err = beginTestRestore(t, fixture.store, fixture.store.CurrentAdmissionBaseline(),
				leaseUUID, "restore-target", requireOperationID(t, "2294"))
			assert.ErrorIs(t, err, ErrRestoreSourceUnavailable)
		})
	}
}

func projectPairedOverlapLineage(
	t *testing.T,
	coordinator *ReconciliationCoordinator,
	row backend.ProvisionInfo,
	overlap bool,
) {
	t.Helper()
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
		var provisions []backend.ProvisionInfo
		var retained []string
		if name == row.BackendName {
			provisions = []backend.ProvisionInfo{row}
			if overlap {
				retained = []string{row.LeaseUUID}
			}
		}
		require.NoError(t, sweep.RecordProvision(name, testBackendStorageID(name), provisions))
		require.NoError(t, sweep.RecordRetention(name, testBackendStorageID(name), retained))
	}
	require.NoError(t, sweep.SealInventory())
	assert.Equal(t, !overlap, sweep.InventoryComplete())
	projection := ReconciliationProjection{Placements: map[string]string{row.LeaseUUID: row.BackendName}}
	if overlap {
		projection = ReconciliationProjection{UntrustedPositives: map[string][]string{row.LeaseUUID: {row.BackendName}}}
	}
	_, err = sweep.Project(projection)
	require.NoError(t, err)
}
