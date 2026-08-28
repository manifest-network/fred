package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

type maintenanceOrderingPersister struct{}

func (maintenanceOrderingPersister) OverwritePayload(string, []byte) error { return nil }

func maintenanceOrderingChain(
	leaseUUID, providerUUID, tenant string,
) *mockChainClient {
	return &mockChainClient{
		getActiveLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       tenant,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}
}

func maintenanceOrderingRouter(
	t testing.TB,
	handler http.Handler,
) *backend.Router {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: server.URL,
		Timeout: time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	return router
}

func requireMaintenanceStatuses(
	t testing.TB,
	events <-chan backend.LeaseStatusEvent,
	want ...backend.ProvisionStatus,
) {
	t.Helper()
	for _, status := range want {
		select {
		case event := <-events:
			assert.Equal(t, status, event.Status)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", status)
		}
	}
}

func TestMaintenanceHandlers_OrderFastRemoteCallbackAfterAcceptedStart(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	keyPair := testutil.NewTestKeyPair("maintenance-ordering")
	chain := maintenanceOrderingChain(leaseUUID, providerUUID, keyPair.Address)

	tests := []struct {
		name        string
		path        string
		startStatus backend.ProvisionStatus
		body        string
		invoke      func(*Handlers, http.ResponseWriter, *http.Request)
	}{
		{
			name:        "restart",
			path:        "/restart",
			startStatus: backend.ProvisionStatusRestarting,
			invoke:      (*Handlers).RestartLease,
		},
		{
			name:        "update",
			path:        "/update",
			startStatus: backend.ProvisionStatusUpdating,
			body:        `{"payload":"dGVzdA=="}`,
			invoke:      (*Handlers).UpdateLease,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lifecycleCallbacks, _ := typedMaintenanceLifecycleStore(
				t, leaseUUID, "test-backend",
			)
			broker := NewEventBroker()
			events, err := broker.Subscribe(leaseUUID)
			require.NoError(t, err)

			router := maintenanceOrderingRouter(t, http.HandlerFunc(func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				require.Equal(t, test.path, r.URL.Path)
				// This models a remote backend that synchronously waits for the
				// callback response before it writes its own 202 response.
				broker.Publish(testEvent(leaseUUID, backend.ProvisionStatusReady))
				w.WriteHeader(http.StatusAccepted)
			}))
			handlers := NewHandlers(HandlersConfig{
				Client:             chain,
				BackendRouter:      router,
				EventBroker:        broker,
				PayloadPersister:   maintenanceOrderingPersister{},
				LifecycleCallbacks: lifecycleCallbacks,
				ProviderUUID:       providerUUID,
				Bech32Prefix:       "manifest",
			})

			token := testutil.CreateTestToken(keyPair, leaseUUID, time.Now())
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/leases/"+leaseUUID+test.path,
				strings.NewReader(test.body),
			)
			request.Header.Set("Authorization", "Bearer "+token)
			request.SetPathValue("lease_uuid", leaseUUID)
			response := httptest.NewRecorder()

			test.invoke(handlers, response, request)

			assert.Equal(t, http.StatusAccepted, response.Code)
			requireMaintenanceStatuses(
				t, events, test.startStatus, backend.ProvisionStatusReady,
			)
			assert.Empty(t, broker.transitions)
		})
	}
}

func TestMaintenanceHandlers_SynchronousRefusalPublishesNoStart(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	keyPair := testutil.NewTestKeyPair("maintenance-refusal")
	broker := NewEventBroker()
	events, err := broker.Subscribe(leaseUUID)
	require.NoError(t, err)
	router := maintenanceOrderingRouter(t, http.HandlerFunc(func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		w.WriteHeader(http.StatusConflict)
	}))
	lifecycleCallbacks, _ := typedMaintenanceLifecycleStore(
		t, leaseUUID, "test-backend",
	)
	handlers := NewHandlers(HandlersConfig{
		Client:             maintenanceOrderingChain(leaseUUID, providerUUID, keyPair.Address),
		BackendRouter:      router,
		EventBroker:        broker,
		LifecycleCallbacks: lifecycleCallbacks,
		ProviderUUID:       providerUUID,
		Bech32Prefix:       "manifest",
	})
	token := testutil.CreateTestToken(keyPair, leaseUUID, time.Now())
	request := httptest.NewRequest(
		http.MethodPost, "/v1/leases/"+leaseUUID+"/restart", nil,
	)
	request.Header.Set("Authorization", "Bearer "+token)
	request.SetPathValue("lease_uuid", leaseUUID)
	response := httptest.NewRecorder()

	handlers.RestartLease(response, request)

	assert.Equal(t, http.StatusConflict, response.Code)
	select {
	case event := <-events:
		t.Fatalf("synchronous refusal published spurious %s", event.Status)
	default:
	}
	assert.Empty(t, broker.transitions)
}
