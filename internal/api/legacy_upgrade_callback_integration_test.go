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
	"github.com/manifest-network/fred/internal/testutil"
)

// TestV013Upgrade_LegacyCallbackCrossesSignedHTTPAndManager composes the real
// first-upgrade boundary. legacyMaintenanceLifecycleStore writes the v0.13
// revision-zero JSON directly to disk before placement.NewStore first opens it;
// the migrated capability must then authorize the old tokenless callback URL
// through HMAC HTTP ingress and Manager's synchronous callback application.
func TestV013Upgrade_LegacyCallbackCrossesSignedHTTPAndManager(t *testing.T) {
	const backendName = "backend-a"
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

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

	body, err := json.Marshal(backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    backend.CallbackStatusSuccess,
	})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost,
		httpServer.URL+testCallbackURI, bytes.NewReader(body),
	)
	require.NoError(t, err)
	// Deliberately sign and send the bare v0.13 URI: no operation_id or
	// lifecycle_id query parameter exists to manufacture typed authority.
	request.Header.Set(CallbackSignatureHeader,
		auth.ComputeSignature(request.Method, request.URL.RequestURI(), body),
	)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
	assert.Equal(t, http.StatusOK, response.StatusCode)

	select {
	case event := <-leaseEvents:
		assert.Equal(t, leaseUUID, event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for migrated legacy callback event")
	}
}
