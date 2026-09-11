package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

func legacyMaintenanceLifecycleStore(
	t *testing.T,
	leaseUUID, backendName string,
) *placement.Store {
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
	backupTarget, err := placement.BindExactBackupTarget(dbPath + ".v013.backup")
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
		t.Context(), placementstore.ProviderUUID, []string{backendName}, inventories,
		chainProof, capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())
	routes, err := placement.NewCallbackRouteFactory("https://fred.example.test")
	require.NoError(t, err)
	store, err := placement.OpenStore(
		dbPath, placementstore.ProviderUUID, placement.WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	require.Equal(t, placement.LifecycleVerdictLegacy,
		store.CurrentLifecycle(leaseUUID).Verdict(),
		"the prepared v0.13 authority must survive its first online open",
	)
	require.NoError(t, store.Close())
	store, err = placement.OpenStore(
		dbPath, placementstore.ProviderUUID, placement.WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// TestV013Upgrade_LegacyCallbackCrossesSignedHTTPAndManager composes the real
// first-upgrade boundary. legacyMaintenanceLifecycleStore writes the v0.13
// revision-zero JSON directly to disk, runs explicit offline preparation, and
// reopens the resulting authority through two online process generations. The
// migrated capability must authorize the old tokenless callback URL through
// HMAC HTTP ingress and Manager's synchronous callback application, reject a
// stale typed identity, and retire terminal authority exactly once.
func TestV013Upgrade_LegacyCallbackCrossesSignedHTTPAndManager(t *testing.T) {
	const backendName = "backend-a"
	leaseUUID := testutil.ValidUUID1
	providerUUID := placementstore.ProviderUUID
	proofVerifier, proofConsumer := hmacauth.NewCallbackProofBoundary()

	placements := legacyMaintenanceLifecycleStore(t, leaseUUID, backendName)
	require.Equal(t, placement.LifecycleVerdictLegacy,
		placements.CurrentLifecycle(leaseUUID).Verdict(),
		"the first open must adopt the revision-zero v0.13 placement",
	)

	backendNode := backend.NewMockBackend(backend.MockBackendConfig{Name: backendName})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendNode, IsDefault: true}},
	})
	require.NoError(t, err)

	chainClient := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}
	events := NewEventBroker()
	t.Cleanup(events.Close)
	leaseEvents, err := events.Subscribe(leaseUUID)
	require.NoError(t, err)

	manager, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:          providerUUID,
		PlacementStore:        placements,
		LeaseEventSink:        events,
		CallbackProofConsumer: proofConsumer,
	}, router, chainClient)
	require.NoError(t, err)

	managerCtx, cancelManager := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(managerCtx) }()
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for callback manager to start")
	}
	t.Cleanup(func() {
		cancelManager()
		require.NoError(t, manager.Close())
		select {
		case <-managerDone:
		case <-time.After(5 * time.Second):
			t.Error("timed out waiting for callback manager to stop")
		}
	})

	auth := newTestCallbackAuthenticatorWithVerifier(t, testCallbackSecret, proofVerifier)
	callbackAPI := &Server{
		callbackPublisher:     manager,
		callbackAuthenticator: auth,
	}
	httpServer := httptest.NewServer(http.HandlerFunc(callbackAPI.handleProvisionCallback))
	t.Cleanup(httpServer.Close)

	send := func(requestURI string, payload backend.CallbackPayload) {
		t.Helper()
		payload.LeaseUUID = leaseUUID
		payload.BackendStorageID = testAPIBackendStorageID(backendName).String()
		body, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		request, requestErr := http.NewRequestWithContext(
			context.Background(), http.MethodPost,
			httpServer.URL+requestURI, bytes.NewReader(body),
		)
		require.NoError(t, requestErr)
		request.Header.Set(CallbackSignatureHeader,
			auth.ComputeSignature(request.Method, request.URL.RequestURI(), body),
		)
		response, requestErr := http.DefaultClient.Do(request)
		require.NoError(t, requestErr)
		assert.Equal(t, http.StatusOK, response.StatusCode)
		require.NoError(t, response.Body.Close())
	}
	requireEvent := func(status backend.ProvisionStatus) backend.LeaseStatusEvent {
		t.Helper()
		select {
		case event := <-leaseEvents:
			assert.Equal(t, leaseUUID, event.LeaseUUID)
			assert.Equal(t, status, event.Status)
			return event
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for migrated legacy %s event", status)
			return backend.LeaseStatusEvent{}
		}
	}
	requireNoEvent := func(reason string) {
		t.Helper()
		select {
		case event := <-leaseEvents:
			t.Fatalf("%s: unexpected callback event: %+v", reason, event)
		default:
		}
	}

	// A typed callback cannot upgrade or replace the explicitly tokenless v0.13
	// authority, even when its signature and storage identity are otherwise valid.
	send(testCallbackURI+"?lifecycle_id=123e4567-e89b-42d3-a456-426614174099",
		backend.CallbackPayload{Status: backend.CallbackStatusSuccess})
	requireNoEvent("stale typed callback against migrated tokenless authority")
	require.Equal(t, placement.LifecycleVerdictLegacy,
		placements.CurrentLifecycle(leaseUUID).Verdict())

	// Deliberately sign and send the bare v0.13 URI: no operation_id or
	// lifecycle_id query parameter exists to manufacture typed authority.
	send(testCallbackURI, backend.CallbackPayload{Status: backend.CallbackStatusSuccess})
	assert.Empty(t, requireEvent(backend.ProvisionStatusReady).Error)

	send(testCallbackURI, backend.CallbackPayload{
		Status: backend.CallbackStatusFailed,
		Error:  "container exited after upgrade",
	})
	assert.Equal(t, "container exited after upgrade",
		requireEvent(backend.ProvisionStatusFailed).Error)

	send(testCallbackURI, backend.CallbackPayload{
		Status:   backend.CallbackStatusDeprovisioned,
		Retained: true,
	})
	assert.Contains(t, requireEvent(backend.ProvisionStatusRetained).Error, "lease data was retained")
	require.True(t, placements.CurrentLifecycle(leaseUUID).Retired(),
		"the authenticated backend's terminal callback must consume legacy teardown authority")

	// A persisted v0.13 sender may redeliver after losing the 2xx response. The
	// retired tokenless capability acknowledges that duplicate without publishing
	// a second terminal event or recreating authority.
	send(testCallbackURI, backend.CallbackPayload{
		Status:   backend.CallbackStatusDeprovisioned,
		Retained: true,
	})
	requireNoEvent("duplicate tokenless callback after terminal retirement")
}
