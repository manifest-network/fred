package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

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
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "https://fred.example.test",
		PlacementStore:  placements,
		LeaseEventSink:  events,
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

	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
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
