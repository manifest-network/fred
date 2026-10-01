package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

const (
	lostLeaseBackend     = "backend-a"
	lostLeaseSurvivor    = "backend-b"
	lostLeaseBodyPattern = `{"error":"the backend storage holding this lease was irrecoverably lost","code":410,"reason":"backend_storage_lost"}`
)

// lostLeaseStore prepares a two-backend authority whose one lease lived on
// backend-a, retires backend-a as lost through the offline repair API, and
// reopens the store the way providerd would after the operator's retirement.
func lostLeaseStore(t *testing.T, leaseUUID string) *placement.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("placements"))
		if err != nil {
			return err
		}
		value, err := json.Marshal(struct {
			Backend string    `json:"backend"`
			SetAt   time.Time `json:"set_at"`
		}{Backend: lostLeaseBackend, SetAt: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)})
		if err != nil {
			return err
		}
		return bucket.Put([]byte(leaseUUID), value)
	}))
	require.NoError(t, db.Close())

	preparer, err := placement.OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	chainProof, err := placementstore.LegacyUpgradeChainProof(placementstore.ProviderUUID, leaseUUID)
	require.NoError(t, err)
	backupTarget, err := placement.BindExactBackupTarget(dbPath + ".v013.backup")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backupTarget.Close()) })
	backends := []string{lostLeaseBackend, lostLeaseSurvivor}
	inventories := map[string]placement.BackendInventory{
		lostLeaseBackend: {
			StorageIdentity:        testAPIBackendStorageID(lostLeaseBackend),
			Provisions:             []string{leaseUUID},
			ProvisionProviderUUIDs: map[string]string{leaseUUID: ""},
			ProvisionItems: map[string][]backend.LeaseItem{
				leaseUUID: {{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			},
			Retentions: []string{},
		},
		lostLeaseSurvivor: {
			StorageIdentity:        testAPIBackendStorageID(lostLeaseSurvivor),
			Provisions:             []string{},
			ProvisionProviderUUIDs: map[string]string{},
			ProvisionItems:         map[string][]backend.LeaseItem{},
			Retentions:             []string{},
		},
	}
	capability, err := preparer.AuthorizePreparation(
		t.Context(), placementstore.ProviderUUID, backends, inventories,
		chainProof, backupTarget, placement.LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		t.Context(), placementstore.ProviderUUID, backends, inventories, chainProof, capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())

	repair, err := placement.OpenAttemptRepair(dbPath, placementstore.ProviderUUID)
	require.NoError(t, err)
	plan, err := repair.PlanBackendRetirement(lostLeaseBackend, testAPIBackendStorageID(lostLeaseBackend))
	require.NoError(t, err)
	require.Equal(t, []string{leaseUUID}, plan.Facts().LostLeases)
	retirementBackup, err := placement.BindExactBackupTarget(filepath.Join(t.TempDir(), "pre-retirement.db"))
	require.NoError(t, err)
	require.NoError(t, repair.CreateExactBackup(retirementBackup))
	_, err = repair.RetireBackend(plan, placement.LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	require.NoError(t, retirementBackup.Close())

	routes, err := placement.NewCallbackRouteFactory("https://fred.example.test")
	require.NoError(t, err)
	store, err := placement.OpenStore(dbPath, placementstore.ProviderUUID, placement.WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	lostBackend, lost := store.Lookup(leaseUUID).LostBackend()
	require.True(t, lost)
	require.Equal(t, lostLeaseBackend, lostBackend)
	return store
}

// TestLostLeaseAnswersFromTheDurableRecordWithoutAskingABackend pins the
// tenant-facing contract: every read of a lost lease is answered from its
// placement record, and no surviving backend is asked for a lease it never held.
func TestLostLeaseAnswersFromTheDurableRecordWithoutAskingABackend(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	store := lostLeaseStore(t, leaseUUID)
	var survivorHits atomic.Int64
	survivor := countingNotProvisionedServer(t, &survivorHits)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: httpBackend(t, lostLeaseSurvivor, survivor.URL), IsDefault: true},
	}})
	require.NoError(t, err)
	chainClient := &mockChainClient{getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
		require.Equal(t, leaseUUID, uuid)
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: placementstore.ProviderUUID,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}, nil
	}}
	h := &Handlers{
		client: chainClient, backendRouter: router, placementLookup: store,
		providerUUID: placementstore.ProviderUUID, bech32Prefix: "manifest",
	}
	serve := func(route string, handler http.HandlerFunc) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/leases/"+leaseUUID+"/"+route, nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}

	status := serve("status", h.GetLeaseStatus)
	require.Equal(t, http.StatusOK, status.Code, "body: %s", status.Body.String())
	var response LeaseStatusResponse
	require.NoError(t, json.NewDecoder(status.Body).Decode(&response))
	assert.Equal(t, string(backend.ProvisionStatusFailed), response.ProvisionStatus)
	assert.Equal(t, string(backend.ReasonBackendStorageLost), response.Reason)
	assert.Equal(t, backend.MsgBackendStorageLost, response.Message)

	for route, handler := range map[string]http.HandlerFunc{
		"provision": h.GetLeaseProvision,
		"releases":  h.GetLeaseReleases,
	} {
		rec := serve(route, handler)
		assert.Equal(t, http.StatusGone, rec.Code, route)
		assert.JSONEq(t, lostLeaseBodyPattern, rec.Body.String(), route)
	}
	assert.Zero(t, survivorHits.Load(), "no surviving backend is asked about a lost lease")
}
