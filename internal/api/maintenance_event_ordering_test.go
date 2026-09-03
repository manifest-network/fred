package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

type maintenanceOrderingPersister struct{}

func (maintenanceOrderingPersister) OverwritePayload(string, []byte) error { return nil }

type blockingMaintenancePersister struct {
	entered chan struct{}
	release chan struct{}
}

type rotatingMaintenanceClaims struct {
	registry  *operation.Registry
	store     *placement.Store
	leaseUUID string
	backend   string
	nextID    operation.OperationID
}

func (claims *rotatingMaintenanceClaims) TryClaimLeaseNow(
	leaseUUID string,
) operation.LeaseClaimResult {
	// Model another operation completing after route/auth resolution but before
	// this handler acquires its lifecycle exclusion claim.
	if leaseUUID == claims.leaseUUID && claims.nextID.Valid() {
		current := claims.store.Lookup(claims.leaseUUID)
		attempt, begun, err := claims.store.BeginOwnedAttempt(
			claims.store.CurrentAdmissionBaseline(),
			current.RecordRevision(),
			claims.backend,
			claims.nextID,
			placement.PayloadFingerprint{},
			testAPIBackendRequestSnapshotFromValues(),
			testAPICallbackPairFromID(claims.nextID),
		)
		if err != nil || !begun {
			panic("failed to begin lifecycle generation rotation in test claim boundary")
		}
		rotated, err := claims.store.ConfirmAttempt(attempt)
		if err != nil || !rotated {
			panic("failed to rotate lifecycle generation in test claim boundary")
		}
		claims.nextID = operation.OperationID{}
	}
	return claims.registry.TryClaimLeaseNow(leaseUUID)
}

func (claims *rotatingMaintenanceClaims) ReleaseLease(claim operation.LeaseClaim) bool {
	return claims.registry.ReleaseLease(claim)
}

func (persister *blockingMaintenancePersister) OverwritePayload(string, []byte) error {
	close(persister.entered)
	<-persister.release
	return nil
}

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
		getLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
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
	client := newBackendHTTPClientForTest(t, backend.HTTPClientConfig{
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
				CallbackBaseURL:    testCallbackBaseURL,
				EventBroker:        broker,
				PayloadPersister:   maintenanceOrderingPersister{},
				LifecycleCallbacks: lifecycleCallbacks,
				MaintenanceClaims:  operation.NewRegistry(),
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
		CallbackBaseURL:    testCallbackBaseURL,
		EventBroker:        broker,
		LifecycleCallbacks: lifecycleCallbacks,
		MaintenanceClaims:  operation.NewRegistry(),
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

func TestMaintenanceHandlers_ConfirmActiveLeaseUnderClaimBeforeDispatch(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	keyPair := testutil.NewTestKeyPair("maintenance-terminal-boundary")
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*Handlers, http.ResponseWriter, *http.Request)
	}{
		{name: "restart", path: "/restart", invoke: (*Handlers).RestartLease},
		{
			name: "update", path: "/update", body: `{"payload":"dGVzdA=="}`,
			invoke: (*Handlers).UpdateLease,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var backendCalls atomic.Int32
			router := maintenanceOrderingRouter(t, http.HandlerFunc(func(
				w http.ResponseWriter,
				_ *http.Request,
			) {
				backendCalls.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			chain := &mockChainClient{
				getActiveLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{
						Uuid: leaseUUID, Tenant: keyPair.Address, ProviderUuid: providerUUID,
						State: billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				},
				getLeaseFunc: func(ctx context.Context, _ string) (*billingtypes.Lease, error) {
					deadline, ok := ctx.Deadline()
					require.True(t, ok, "exact maintenance confirmation must be bounded")
					require.LessOrEqual(t, time.Until(deadline), maintenanceChainConfirmationTimeout)
					return &billingtypes.Lease{
						Uuid: leaseUUID, Tenant: keyPair.Address, ProviderUuid: providerUUID,
						State: billingtypes.LEASE_STATE_CLOSED,
					}, nil
				},
			}
			claims := operation.NewRegistry()
			lifecycleCallbacks, _ := typedMaintenanceLifecycleStore(
				t, leaseUUID, "test-backend",
			)
			handlers := NewHandlers(HandlersConfig{
				Client:             chain,
				BackendRouter:      router,
				CallbackBaseURL:    testCallbackBaseURL,
				PayloadPersister:   maintenanceOrderingPersister{},
				LifecycleCallbacks: lifecycleCallbacks,
				MaintenanceClaims:  claims,
				ProviderUUID:       providerUUID,
				Bech32Prefix:       "manifest",
			})
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/leases/"+leaseUUID+test.path,
				strings.NewReader(test.body),
			)
			request.Header.Set("Authorization", "Bearer "+
				testutil.CreateTestToken(keyPair, leaseUUID, time.Now()))
			request.SetPathValue("lease_uuid", leaseUUID)
			response := httptest.NewRecorder()

			test.invoke(handlers, response, request)

			assert.Equal(t, http.StatusConflict, response.Code)
			assert.Zero(t, backendCalls.Load(),
				"terminal exact chain state must prevent a maintenance side effect")
			claim := claims.TryClaimLeaseNow(leaseUUID)
			require.True(t, claim.Acquired(), "terminal confirmation leaked the lease claim")
			require.True(t, claims.ReleaseLease(claim.Claim()))
		})
	}
}

func TestUpdateLeaseHoldsLifecycleClaimThroughPayloadSettlement(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	keyPair := testutil.NewTestKeyPair("maintenance-settlement")
	claims := operation.NewRegistry()
	persister := &blockingMaintenancePersister{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	var backendCalls atomic.Int32
	router := maintenanceOrderingRouter(t, http.HandlerFunc(func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		backendCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	lifecycleCallbacks, _ := typedMaintenanceLifecycleStore(
		t, leaseUUID, "test-backend",
	)
	handlers := NewHandlers(HandlersConfig{
		Client:             maintenanceOrderingChain(leaseUUID, providerUUID, keyPair.Address),
		BackendRouter:      router,
		CallbackBaseURL:    testCallbackBaseURL,
		PayloadPersister:   persister,
		LifecycleCallbacks: lifecycleCallbacks,
		MaintenanceClaims:  claims,
		ProviderUUID:       providerUUID,
		Bech32Prefix:       "manifest",
	})
	invoke := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/leases/"+leaseUUID+"/update",
			strings.NewReader(`{"payload":"dGVzdA=="}`),
		)
		request.Header.Set("Authorization", "Bearer "+
			testutil.CreateTestToken(keyPair, leaseUUID, time.Now()))
		request.SetPathValue("lease_uuid", leaseUUID)
		response := httptest.NewRecorder()
		handlers.UpdateLease(response, request)
		return response
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- invoke() }()
	select {
	case <-persister.entered:
	case <-time.After(time.Second):
		t.Fatal("first update never reached payload settlement")
	}

	assert.Equal(t, operation.LeaseClaimBusy, claims.TryClaimLeaseNow(leaseUUID).Outcome(),
		"reconciliation must not acquire the lease during accepted settlement")
	second := invoke()
	assert.Equal(t, http.StatusConflict, second.Code)
	assert.Equal(t, int32(1), backendCalls.Load(),
		"a concurrent update must not overtake the unsettled payload write")

	close(persister.release)
	select {
	case first := <-firstDone:
		assert.Equal(t, http.StatusAccepted, first.Code)
	case <-time.After(time.Second):
		t.Fatal("first update did not finish after payload settlement was released")
	}
	claim := claims.TryClaimLeaseNow(leaseUUID)
	require.True(t, claim.Acquired(), "settled update leaked its lifecycle claim")
	require.True(t, claims.ReleaseLease(claim.Claim()))
}

func TestUpdateLeaseReadsLifecycleGenerationAfterExclusiveClaim(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	keyPair := testutil.NewTestKeyPair("maintenance-generation-race")
	store, oldID := typedMaintenanceLifecycleStore(t, leaseUUID, "test-backend")
	newID, err := operation.ParseID("223e4567-e89b-42d3-a456-426614174000")
	require.NoError(t, err)
	claims := &rotatingMaintenanceClaims{
		registry:  operation.NewRegistry(),
		store:     store,
		leaseUUID: leaseUUID,
		backend:   "test-backend",
		nextID:    newID,
	}

	var receivedLifecycleID string
	router := maintenanceOrderingRouter(t, http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		var update backend.UpdateRequest
		decodeErr := json.NewDecoder(r.Body).Decode(&update)
		require.NoError(t, decodeErr)
		callbackURL, parseErr := url.Parse(update.CallbackURL)
		if parseErr == nil {
			receivedLifecycleID = callbackURL.Query().Get(backend.CallbackLifecycleIDQueryParameter)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	handlers := NewHandlers(HandlersConfig{
		Client:             maintenanceOrderingChain(leaseUUID, providerUUID, keyPair.Address),
		BackendRouter:      router,
		CallbackBaseURL:    testCallbackBaseURL,
		PayloadPersister:   maintenanceOrderingPersister{},
		LifecycleCallbacks: store,
		MaintenanceClaims:  claims,
		ProviderUUID:       providerUUID,
		Bech32Prefix:       "manifest",
	})
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/leases/"+leaseUUID+"/update",
		strings.NewReader(`{"payload":"dGVzdA=="}`),
	)
	request.Header.Set("Authorization", "Bearer "+
		testutil.CreateTestToken(keyPair, leaseUUID, time.Now()))
	request.SetPathValue("lease_uuid", leaseUUID)
	response := httptest.NewRecorder()

	handlers.UpdateLease(response, request)

	require.Equal(t, http.StatusAccepted, response.Code)
	assert.NotEqual(t, oldID.String(), receivedLifecycleID)
	assert.Equal(t, newID.String(), receivedLifecycleID,
		"an accepted update must carry the generation current under its lease claim")
}
