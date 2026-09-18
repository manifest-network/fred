package placement

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func testCallbackRoutes(t testing.TB) *CallbackRouteFactory {
	t.Helper()
	factory, err := NewCallbackRouteFactory("https://provider.test/proxy?trace=a%2Fb")
	require.NoError(t, err)
	return factory
}

func TestLegacyUpgradeInventoryBootstrapsDurableRuntimePrincipal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, path, map[string][]byte{
		maintenanceLease: []byte("backend-a"),
	})

	store, err := newStore(
		path, true, WithCallbackRouteFactory(testCallbackRoutes(t)),
	)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(
		store, []string{"backend-a", "backend-b"},
	))
	assert.Equal(t, LifecycleVerdictLegacy,
		store.CurrentLifecycle(maintenanceLease).Verdict(),
		"the first prepared open must preserve the v0.13 tokenless generation")

	fence := store.BeginInventorySession()
	legacy := LifecycleObservation{Kind: LifecycleObservationLegacy}
	provision := backend.ProvisionInfo{
		LeaseUUID:    maintenanceLease,
		Tenant:       "tenant-test",
		ProviderUUID: freshTestProviderUUID,
		BackendName:  "backend-a",
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationLegacy,
		},
	}
	principal, err := store.mintRuntimePrincipalObservation(
		fence, testBackendStorageID("backend-a"), provision,
	)
	require.NoError(t, err)
	projection := inventoryProjectionEvidenceForTest(t, store, InventoryProjection{
		complete: true,
		backendStorageIdentities: testBackendStorageIDs(
			"backend-a", "backend-b",
		),
		emptyBackends: []string{"backend-b"},
		Placements: map[string]string{
			maintenanceLease: "backend-a",
		},
		lifecycles: map[string]LifecycleObservation{
			maintenanceLease: legacy,
		},
		runtimePrincipals: map[string]RuntimePrincipalObservation{
			maintenanceLease: principal,
		},
	})
	_, err = inventoryProjectorForTest(t, store).Project(fence, projection)
	store.EndInventorySession(fence)
	require.NoError(t, err)

	id, err := maintenanceid.Parse(maintenanceIDA)
	require.NoError(t, err)
	prepared, err := store.prepareMaintenanceCommand(
		id, maintenanceLease, MaintenanceCommandRestart, nil,
	)
	require.NoError(t, err)
	command := prepared.Command()
	assert.Equal(t, "tenant-test", command.Tenant())
	assert.Equal(t, freshTestProviderUUID, command.ProviderUUID())
	assert.Equal(t, "backend-a", command.BackendName())
	assert.Equal(t, testBackendStorageID("backend-a"), command.BackendStorageID())
	require.True(t, command.Valid())
	assert.True(t, command.lifecycleLegacy)
	assert.Equal(t,
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb",
		command.CallbackURL(),
	)
	admission, err := store.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	require.True(t, admission.Pending())
	require.NoError(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeAccepted))
	require.NoError(t, store.Close())

	// The principal is now durable. A later restart does not require another
	// fleet-wide inventory, so backend-b may be transiently unavailable without
	// disabling maintenance on backend-a.
	reopened, err := OpenStore(
		path, freshTestProviderUUID,
		WithCallbackRouteFactory(testCallbackRoutes(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	partialFence := reopened.BeginInventorySession()
	partialProjection := inventoryProjectionEvidenceForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{maintenanceLease: "backend-a"},
		lifecycles: map[string]LifecycleObservation{maintenanceLease: legacy},
	})
	_, err = inventoryProjectorForTest(t, reopened).Project(partialFence, partialProjection)
	reopened.EndInventorySession(partialFence)
	require.NoError(t, err,
		"a later partial inventory must preserve the established principal")
	retryID, err := maintenanceid.Parse(maintenanceIDB)
	require.NoError(t, err)
	prepared, err = reopened.prepareMaintenanceCommand(
		retryID, maintenanceLease, MaintenanceCommandRestart, nil,
	)
	require.NoError(t, err)
	assert.True(t, prepared.Valid())
	assert.Equal(t, "backend-a", prepared.Command().BackendName())
	admission, err = reopened.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	assert.True(t, admission.Pending(),
		"an unrelated backend outage cannot disable the available owner's maintenance lane")
}

func TestRuntimePrincipalAuthorityCannotBeSplicedIntoProjection(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	fence := store.BeginInventorySession()
	legacy := LifecycleObservation{Kind: LifecycleObservationLegacy}
	provision := backend.ProvisionInfo{
		LeaseUUID:    maintenanceLease,
		Tenant:       "tenant-test",
		ProviderUUID: freshTestProviderUUID,
		BackendName:  "backend-a",
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationLegacy,
		},
	}
	principal, err := store.mintRuntimePrincipalObservation(
		fence, testBackendStorageID("backend-a"), provision,
	)
	require.NoError(t, err)

	_, err = projectInventoryAtFenceForTest(t, store, fence, InventoryProjection{
		Placements: map[string]string{maintenanceLease: "backend-a"},
		lifecycles: map[string]LifecycleObservation{maintenanceLease: legacy},
		runtimePrincipals: map[string]RuntimePrincipalObservation{
			maintenanceLease: principal,
		},
	})
	require.NoError(t, err)
	id, err := maintenanceid.Parse(maintenanceIDA)
	require.NoError(t, err)
	_, err = store.prepareMaintenanceCommand(
		id, maintenanceLease, MaintenanceCommandRestart, nil,
	)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict,
		"a caller-supplied principal must be ignored when the sealed inventory is partial")
	store.EndInventorySession(fence)
	fence = store.BeginInventorySession()

	_, err = projectInventoryAtFenceForTest(t, store, fence, InventoryProjection{
		complete:                 true,
		backendStorageIdentities: testBackendStorageIDs("backend-a", "backend-b"),
		emptyBackends:            []string{},
		Placements:               map[string]string{maintenanceLease: "backend-a"},
		lifecycles: map[string]LifecycleObservation{
			maintenanceLease: {Kind: LifecycleObservationUnusable},
		},
		runtimePrincipals: map[string]RuntimePrincipalObservation{
			maintenanceLease: principal,
		},
	})
	require.NoError(t, err)
	_, err = store.prepareMaintenanceCommand(
		id, maintenanceLease, MaintenanceCommandRestart, nil,
	)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict,
		"a caller-supplied principal must not override unusable sealed lifecycle evidence")
	store.EndInventorySession(fence)
}
