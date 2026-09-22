package restore

import (
	"net/http"
	"path/filepath"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func TestServiceCloseBetweenInventoryReadsPreservesRestoreSource(t *testing.T) {
	const (
		sourceUUID = "00000000-0000-4000-8000-000000002290"
		targetUUID = "00000000-0000-4000-8000-000000002291"
	)
	fixture := newOverlapRestoreFixture(t, sourceUUID, targetUUID)
	row := backend.ProvisionInfo{
		LeaseUUID: sourceUUID, BackendName: testBackend,
		ProviderUUID: testProvider, Tenant: testTenant,
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped, ID: testOperationID(t, 2290).String(),
		},
	}
	fixture.backend.setInventory([]backend.ProvisionInfo{row}, nil)
	seed, err := fixture.reconciliation.BeginSweep()
	require.NoError(t, err)
	for _, name := range []string{testBackend, "backend-b"} {
		provisions, collectErr := seed.CollectProvisionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		retentions, collectErr := seed.CollectRetentionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		_, collectErr = seed.RecordBackendInventory(provisions, retentions)
		require.NoError(t, collectErr)
	}
	require.NoError(t, seed.SealInventory())
	_, err = seed.Project(placement.ReconciliationProjection{Placements: map[string]string{sourceUUID: testBackend}})
	require.NoError(t, err)
	seed.End()
	require.True(t, fixture.store.CurrentAdmissionBaseline().Valid())
	require.Equal(t, placement.LifecycleVerdictAuthorized, fixture.store.CurrentLifecycle(sourceUUID).Verdict(),
		"the valid UUID and full inventory must establish the exact typed source generation")
	before := fixture.store.Lookup(sourceUUID)
	sweep, err := fixture.reconciliation.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	for _, name := range []string{testBackend, "backend-b"} {
		client := fixture.backends[name].(*fakeBackend)
		var rows []backend.ProvisionInfo
		if name == testBackend {
			rows = []backend.ProvisionInfo{row}
		}
		client.setInventory(rows, nil)
		provisions, collectErr := sweep.CollectProvisionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		// The source closes after /provisions was read. The later retention
		// response changes lifecycle state, but agrees about the durable owner.
		var retained []backend.RetainedLease
		if name == testBackend {
			retained = []backend.RetainedLease{{LeaseUUID: sourceUUID}}
		}
		client.setInventory(nil, retained)
		retentions, collectErr := sweep.CollectRetentionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		_, collectErr = sweep.RecordBackendInventory(provisions, retentions)
		require.NoError(t, collectErr)
	}
	require.NoError(t, sweep.SealInventory())
	require.False(t, sweep.InventoryComplete(), "overlap still cannot establish a baseline")
	_, err = sweep.Project(placement.ReconciliationProjection{
		UntrustedPositives: map[string][]string{sourceUUID: {testBackend}},
	})
	require.NoError(t, err)
	sweep.End()
	assert.Equal(t, before, fixture.store.Lookup(sourceUUID), "redundant owner evidence must not revoke restore affinity")
	// Affinity alone does not prove a completed close. The backend's atomic
	// restore admission is definitive, including for legacy retained sources or
	// old source callbacks that were already acknowledged without retirement.
	fixture.useCausalRestoreResponse(t, http.StatusConflict, `{"error":"retained source close is pending"}`)
	refused := fixture.service.Execute(t.Context(), Command{
		TargetLeaseUUID: targetUUID, Tenant: testTenant, SourceLeaseUUID: sourceUUID,
	})
	require.Equal(t, OutcomeBackendInvalidState, refused.Outcome)
	require.Equal(t, placement.StateAbsent, fixture.store.Lookup(targetUUID).State())
	require.Equal(t, before, fixture.store.Lookup(sourceUUID))
	fixture.backends[testBackend] = fixture.backend
	result := fixture.service.Execute(t.Context(), Command{
		TargetLeaseUUID: targetUUID, Tenant: testTenant, SourceLeaseUUID: sourceUUID,
	})
	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, 1, fixture.backend.callCount())
	assert.Equal(t, sourceUUID, fixture.backend.lastRequest().FromLeaseUUID)
	assert.Equal(t, testBackend, fixture.store.Lookup(targetUUID).Backend)
}

// This fixture starts without projected placements: the test's first complete
// inventory establishes both generation and principal for canonical lease IDs.
// The general service fixture intentionally uses informal IDs and unknown
// lifecycle observations, which cannot construct runtime-principal authority.
func newOverlapRestoreFixture(t *testing.T, sourceUUID, targetUUID string) *fixture {
	t.Helper()
	store, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "placements.db"), testProvider,
		placement.WithCallbackRouteFactory(testCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(store,
		[]string{testBackend, "backend-b"}, restoreTestBackendStorageIDs(testBackend, "backend-b")))
	owner := &fakeBackend{name: testBackend}
	result := &fixture{
		targets: &targetReader{leases: map[string]*billingtypes.Lease{
			sourceUUID: sourceLease(sourceUUID), targetUUID: pendingLease(targetUUID),
		}},
		backend: owner, backends: backendLookup{testBackend: owner, "backend-b": &fakeBackend{name: "backend-b"}},
		store: store, events: &eventSink{},
	}
	result.coordinator, err = store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	result.runtime = result.coordinator.RuntimeController()
	result.execution, err = result.coordinator.BindBackendRuntime(result.backends,
		restoreReconciliationChain{targetReader: result.targets})
	require.NoError(t, err)
	result.reconciliation, err = result.execution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	result.restore, err = result.execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	result.service, err = NewService(Config{Coordinator: result.restore, Events: result.events})
	require.NoError(t, err)
	return result
}
