package api

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func typedMaintenanceLifecycleStore(
	t *testing.T,
	leaseUUID, backendName string,
) (*placement.Store, operation.OperationID) {
	t.Helper()
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{backendName}))
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	store.EndInventorySession(fence)
	require.NoError(t, err)

	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174000")
	require.NoError(t, err)
	scope, err := store.ScopeAdmission(
		store.CurrentAdmissionBaseline(), []string{backendName},
	)
	require.NoError(t, err)
	attempt, begun, err := store.BeginNewAttempt(
		scope, leaseUUID, backendName, operationID,
	)
	require.NoError(t, err)
	require.True(t, begun)
	confirmed, err := store.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	return store, operationID
}

func TestHandlers_MaintenanceCallbackURLUsesCurrentDurableCapability(t *testing.T) {
	const (
		leaseUUID   = "lease-1"
		backendName = "backend-a"
		baseURL     = "https://fred.example/base"
	)
	store, operationID := typedMaintenanceLifecycleStore(t, leaseUUID, backendName)
	handlers := &Handlers{
		callbackBaseURL:    baseURL,
		lifecycleCallbacks: store,
	}

	callbackURL, err := handlers.maintenanceCallbackURL(leaseUUID, backendName)
	require.NoError(t, err)
	assert.Equal(t,
		baseURL+"/callbacks/provision?lifecycle_id="+operationID.String(),
		callbackURL,
	)

	_, err = handlers.maintenanceCallbackURL(leaseUUID, "backend-b")
	assert.Error(t, err, "the placement capability must agree with backend routing")

	retired, err := store.RetireLifecycle(
		leaseUUID, store.CurrentLifecycle(leaseUUID).ID(),
	)
	require.NoError(t, err)
	require.True(t, retired.Retired())
	_, err = handlers.maintenanceCallbackURL(leaseUUID, backendName)
	assert.Error(t, err, "a retired capability must never be reissued")
}

func TestHandlers_MaintenanceCallbackURLKeepsMigratedLeaseLegacy(t *testing.T) {
	const (
		leaseUUID   = "legacy-lease"
		backendName = "backend-a"
		baseURL     = "https://fred.example/base"
	)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{backendName}))
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{
		Complete:   true,
		Placements: map[string]string{leaseUUID: backendName},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)

	handlers := &Handlers{
		callbackBaseURL:    baseURL,
		lifecycleCallbacks: store,
	}
	callbackURL, err := handlers.maintenanceCallbackURL(leaseUUID, backendName)
	require.NoError(t, err)
	assert.Equal(t, baseURL+"/callbacks/provision", callbackURL)
}

func TestHandlers_MaintenanceCallbackURLRejectsTeardownOnlyCapability(t *testing.T) {
	t.Run("typed", func(t *testing.T) {
		store, _ := typedMaintenanceLifecycleStore(t, "lease-1", "backend-a")
		current := store.Lookup("lease-1")
		deleted, err := store.DeleteRecord(current.RecordRevision())
		require.NoError(t, err)
		require.True(t, deleted)
		require.Equal(t, placement.LifecycleVerdictTeardownOnly,
			store.CurrentLifecycle("lease-1").Verdict())

		handlers := &Handlers{
			callbackBaseURL:    "https://fred.example/base",
			lifecycleCallbacks: store,
		}
		_, err = handlers.maintenanceCallbackURL("lease-1", "backend-a")
		assert.Error(t, err, "teardown-only identity must not be reissued for maintenance")
	})

	t.Run("legacy", func(t *testing.T) {
		store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		require.NoError(t, store.ConfigureBackendTopology([]string{"backend-a"}))
		fence := store.BeginInventorySession()
		_, err = store.ProjectInventory(fence, placement.InventoryProjection{
			Complete:   true,
			Placements: map[string]string{"legacy": "backend-a"},
		})
		store.EndInventorySession(fence)
		require.NoError(t, err)
		current := store.Lookup("legacy")
		deleted, err := store.DeleteRecord(current.RecordRevision())
		require.NoError(t, err)
		require.True(t, deleted)
		require.Equal(t, placement.LifecycleVerdictTeardownOnly,
			store.CurrentLifecycle("legacy").Verdict())

		handlers := &Handlers{
			callbackBaseURL:    "https://fred.example/base",
			lifecycleCallbacks: store,
		}
		_, err = handlers.maintenanceCallbackURL("legacy", "backend-a")
		assert.Error(t, err, "legacy teardown authority must not be reissued")
	})
}
