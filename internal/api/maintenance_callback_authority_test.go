package api

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func legacyMaintenanceLifecycleStore(
	t *testing.T,
	leaseUUID, backendName string,
) *placement.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("placements"))
		if err != nil {
			return err
		}
		value, err := json.Marshal(struct {
			Backend string    `json:"backend"`
			SetAt   time.Time `json:"set_at"`
		}{
			Backend: backendName,
			SetAt:   time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		})
		if err != nil {
			return err
		}
		return bucket.Put([]byte(leaseUUID), value)
	}))
	require.NoError(t, db.Close())

	preparer, err := placement.OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	chainProof, err := placementstore.LegacyUpgradeChainProof(
		placementstore.ProviderUUID, leaseUUID,
	)
	require.NoError(t, err)
	backupPath := dbPath + ".v013.backup"
	backupTarget, err := placement.BindExactBackupTarget(backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backupTarget.Close()) })
	inventories := map[string]placement.BackendInventory{
		backendName: {
			StorageIdentity:        testAPIBackendStorageID(backendName),
			Provisions:             []string{leaseUUID},
			ProvisionProviderUUIDs: map[string]string{leaseUUID: ""},
			ProvisionItems: map[string][]backend.LeaseItem{
				leaseUUID: {{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			},
			Retentions: []string{},
		},
	}
	capability, err := preparer.AuthorizePreparation(
		t.Context(), placementstore.ProviderUUID, []string{backendName}, inventories,
		chainProof, backupTarget, placement.LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		t.Context(),
		placementstore.ProviderUUID,
		[]string{backendName},
		inventories,
		chainProof,
		capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())
	store, err := placement.OpenStore(dbPath, placementstore.ProviderUUID)
	require.NoError(t, err)
	require.Equal(t, placement.LifecycleVerdictLegacy,
		store.CurrentLifecycle(leaseUUID).Verdict(),
		"the prepared v0.13 authority must survive its first online open",
	)
	require.NoError(t, store.Close())
	store, err = placement.OpenStore(dbPath, placementstore.ProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func typedMaintenanceLifecycleStore(
	t *testing.T,
	leaseUUID, backendName string,
) (*placement.Store, operation.OperationID) {
	t.Helper()
	store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	configureAPIPlacementTopology(t, store, []string{backendName})
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{
		Complete:                 true,
		BackendStorageIdentities: testAPIBackendStorageIDs(backendName),
		EmptyBackends:            []string{backendName},
	})
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
		placement.PayloadFingerprint{}, testAPIBackendRequestSnapshot(t),
		testAPICallbackPair(t, operationID),
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

func TestHandlers_MaintenanceCallbackURLRejectsMissingAuthority(t *testing.T) {
	handlers := &Handlers{callbackBaseURL: "https://fred.example/base"}
	_, err := handlers.maintenanceCallbackURL("lease-1", "backend-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "authority is unavailable")
}

func TestHandlers_MaintenanceCallbackURLKeepsMigratedLeaseLegacy(t *testing.T) {
	const (
		leaseUUID   = "018f47a2-8b1c-7def-8123-456789abcdef"
		backendName = "backend-a"
		baseURL     = "https://fred.example/base"
	)
	store := legacyMaintenanceLifecycleStore(t, leaseUUID, backendName)

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
		const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdef"
		store := legacyMaintenanceLifecycleStore(t, leaseUUID, "backend-a")
		current := store.Lookup(leaseUUID)
		deleted, err := store.DeleteRecord(current.RecordRevision())
		require.NoError(t, err)
		require.True(t, deleted)
		require.Equal(t, placement.LifecycleVerdictTeardownOnly,
			store.CurrentLifecycle(leaseUUID).Verdict())

		handlers := &Handlers{
			callbackBaseURL:    "https://fred.example/base",
			lifecycleCallbacks: store,
		}
		_, err = handlers.maintenanceCallbackURL(leaseUUID, "backend-a")
		assert.Error(t, err, "legacy teardown authority must not be reissued")
	})
}
