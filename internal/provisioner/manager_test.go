package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

const (
	managerLeaseUUID        = "11111111-1111-4111-8111-111111111111"
	managerUnknownLeaseUUID = "22222222-2222-4222-8222-222222222222"
	managerGPULeaseUUID     = "33333333-3333-4333-8333-333333333333"
)

// hashPayload computes the SHA-256 hash of a payload and returns it as a hex string.
func hashPayload(payload []byte) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

// testItems creates a LeaseItem slice for testing.
func testItems(sku string) []backend.LeaseItem {
	if sku == "" {
		return nil
	}
	return []backend.LeaseItem{{SKU: sku, Quantity: 1}}
}

type managerTestLeaseReader struct {
	lease billingtypes.Lease
}

func (reader managerTestLeaseReader) GetLease(
	_ context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	if reader.lease.Uuid != leaseUUID {
		return nil, nil
	}
	lease := reader.lease
	lease.Items = append([]billingtypes.LeaseItem(nil), reader.lease.Items...)
	return &lease, nil
}

func TestManagerProviderControlPlaneNormalizesConcreteNotFoundConvention(t *testing.T) {
	chainClient := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}
	control, _, err := newManagerProviderControlPlane(chainClient, AckBatcherConfig{
		ProviderUUID: placementstore.ProviderUUID,
	})
	require.NoError(t, err)
	lease, err := control.GetLease(t.Context(), "missing-lease")
	assert.Nil(t, lease)
	require.ErrorIs(t, err, billingtypes.ErrLeaseNotFound)
}

func TestManagerProviderControlPlaneRejectsTypedNilChain(t *testing.T) {
	var chainClient *chaintest.MockClient
	control, batcher, err := newManagerProviderControlPlane(chainClient, AckBatcherConfig{})
	require.Error(t, err)
	assert.Nil(t, control)
	assert.Nil(t, batcher)
}

// requireManagerProvisionCallbackOperation arranges an active operation only
// through the production provision application. Manager tests must not regain
// the raw Registry mutation surface removed from production.
func requireManagerProvisionCallbackOperation(
	t testing.TB,
	manager *Manager,
	backendClient *mockManagerBackend,
	leaseUUID, tenant string,
	items []backend.LeaseItem,
) operation.OperationID {
	t.Helper()
	require.NotNil(t, manager)
	require.NotNil(t, backendClient)

	chainItems := make([]billingtypes.LeaseItem, 0, len(items))
	for _, item := range items {
		require.Positive(t, item.Quantity)
		chainItems = append(chainItems, billingtypes.LeaseItem{
			SkuUuid: item.SKU, Quantity: uint64(item.Quantity),
			ServiceName: item.ServiceName, CustomDomain: item.CustomDomain,
		})
	}
	lease := billingtypes.Lease{
		Uuid: leaseUUID, Tenant: tenant, ProviderUuid: manager.providerUUID,
		State: billingtypes.LEASE_STATE_PENDING, Items: chainItems,
	}
	chainValue, ok := testManagerChains.Load(manager)
	require.True(t, ok)
	mock, ok := chainValue.(*chaintest.MockClient)
	require.True(t, ok, "manager callback fixtures require a mutable mock chain")
	previousRead := mock.GetLeaseFunc
	defer func() { mock.GetLeaseFunc = previousRead }()
	mock.GetLeaseFunc = func(ctx context.Context, observed string) (*billingtypes.Lease, error) {
		if observed == leaseUUID {
			copy := lease
			copy.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
			return &copy, nil
		}
		if previousRead != nil {
			return previousRead(ctx, observed)
		}
		return nil, nil
	}
	coordinator, err := manager.executionCoordinator.ProvisionCoordinator(nil)
	require.NoError(t, err)
	event, err := placement.NewProvisionEventRequest(leaseUUID, tenant)
	require.NoError(t, err)
	result := coordinator.ExecuteCurrentLease(t.Context(), event)
	require.NoError(t, result.Err())
	require.Equal(t, placement.ProvisionEventStarted, result.Disposition())

	backendClient.mu.Lock()
	require.NotEmpty(t, backendClient.provisionCalls)
	request := backendClient.provisionCalls[len(backendClient.provisionCalls)-1]
	backendClient.mu.Unlock()
	require.Equal(t, leaseUUID, request.LeaseUUID)
	parsed, err := url.Parse(request.CallbackURL)
	require.NoError(t, err)
	operationID, present, err := operation.ParseQuery(parsed.Query())
	require.NoError(t, err)
	require.True(t, present)
	require.True(t, operationID.Valid())
	return operationID
}

func lastManagerBackendOperationID(
	t testing.TB,
	backendClient *mockManagerBackend,
	leaseUUID string,
) string {
	t.Helper()
	backendClient.mu.Lock()
	require.NotEmpty(t, backendClient.provisionCalls)
	request := backendClient.provisionCalls[len(backendClient.provisionCalls)-1]
	backendClient.mu.Unlock()
	require.Equal(t, leaseUUID, request.LeaseUUID)
	callbackURL, err := url.Parse(request.CallbackURL)
	require.NoError(t, err)
	operationID, present, err := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, err)
	require.True(t, present)
	require.True(t, operationID.Valid(), "lease %s must have one durable operation attempt", leaseUUID)
	return operationID.String()
}

// mockManagerBackend implements backend.Backend for manager tests.
type mockManagerBackend struct {
	mu               sync.Mutex
	name             string
	provisionCalls   []backend.ProvisionRequest
	deprovisionCalls []string
	provisionErr     error
	deprovisionErr   error
}

func (m *mockManagerBackend) Name() string {
	return m.name
}

func (m *mockManagerBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.provisionCalls = append(m.provisionCalls, req)
	return m.provisionErr
}

func (m *mockManagerBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	return nil, nil
}

func (m *mockManagerBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deprovisionCalls = append(m.deprovisionCalls, leaseUUID)
	return m.deprovisionErr
}

func (m *mockManagerBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockManagerBackend) ListProvisionsWithIdentity(
	ctx context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	rows, err := m.ListProvisions(ctx)
	return rows, testBackendStorageID(m.name), err
}

func (m *mockManagerBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockManagerBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockManagerBackend) RefreshState(ctx context.Context) error {
	return nil
}

func (m *mockManagerBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockManagerBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockManagerBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}

func (m *mockManagerBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}

func (m *mockManagerBackend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	return backend.ErrNotRetained
}

func (m *mockManagerBackend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	return nil
}

func (m *mockManagerBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockManagerBackend) GetLoadStats(_ context.Context) (*backend.LoadStats, error) {
	return nil, nil
}

func (m *mockManagerBackend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	return nil, nil
}

func (m *mockManagerBackend) ListRetentionsWithIdentity(
	ctx context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	rows, err := m.ListRetentions(ctx)
	return rows, testBackendStorageID(m.name), err
}

func TestNewManager_Validation(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}
	validPlacement := newTestPlacementAuthority(t)
	configureTestPlacementTopology(t, validPlacement, []string{"test"})
	var typedNilPlacement *placement.Store
	typedNilAuthority := typedNilPlacement
	var typedNilChain *chaintest.MockClient
	var typedNilChainClient ManagerChainClient = typedNilChain

	tests := []struct {
		name        string
		cfg         ManagerConfig
		router      *backend.Router
		chainClient ManagerChainClient
		wantErr     string
	}{
		{
			name:        "missing router",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid"},
			router:      nil,
			chainClient: mockChain,
			wantErr:     "backend router is required",
		},
		{
			name:        "missing chain client",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid"},
			router:      router,
			chainClient: nil,
			wantErr:     "chain client is required",
		},
		{
			name:        "typed nil chain client",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid"},
			router:      router,
			chainClient: typedNilChainClient,
			wantErr:     "chain client is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ManagerConfig{},
			router:      router,
			chainClient: mockChain,
			wantErr:     "provider UUID is required",
		},
		{
			name: "missing placement authority",
			cfg: ManagerConfig{
				ProviderUUID: "test-uuid",
			},
			router:      router,
			chainClient: mockChain,
			wantErr:     ErrPlacementStoreUnavailable.Error(),
		},
		{
			name: "typed nil placement authority",
			cfg: ManagerConfig{
				ProviderUUID:   "test-uuid",
				PlacementStore: typedNilAuthority,
			},
			router:      router,
			chainClient: mockChain,
			wantErr:     ErrPlacementStoreUnavailable.Error(),
		},
		{
			name: "missing callback proof consumer",
			cfg: ManagerConfig{
				ProviderUUID:   placementstore.ProviderUUID,
				PlacementStore: validPlacement,
			},
			router:      router,
			chainClient: mockChain,
			wantErr:     "callback proof consumer is required",
		},
		{
			name: "valid config",
			cfg: ManagerConfig{
				ProviderUUID:          placementstore.ProviderUUID,
				PlacementStore:        validPlacement,
				CallbackProofConsumer: callbackTestProofConsumer,
			},
			router:      router,
			chainClient: mockChain,
			wantErr:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewManager(tt.cfg, tt.router, tt.chainClient)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				require.NotNil(t, manager)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestNewManager_RejectsPlacementAuthorityForDifferentProviderBeforeTopologyUse(t *testing.T) {
	backendClient := &mockManagerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend: backendClient, IsDefault: true,
	}}})
	require.NoError(t, err)
	store := newTestPlacementAuthority(t)
	configureTestPlacementTopology(t, store, []string{backendClient.Name()})
	manager, err := NewManager(ManagerConfig{
		ProviderUUID:   "e58ed763-928c-4e03-bfac-67a92a99de90",
		PlacementStore: store,
	}, router, &chaintest.MockClient{})

	require.ErrorIs(t, err, placement.ErrProviderAuthorityMismatch)
	assert.Nil(t, manager)
}

func TestNewManager_RejectsUncommittedRouterTopologyWithoutMutatingAuthority(t *testing.T) {
	backendA := &mockManagerBackend{name: "backend-a"}
	backendB := &mockManagerBackend{name: "backend-b"}
	oldRouter, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendA, IsDefault: true}},
	})
	require.NoError(t, err)
	currentRouter, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendB},
			{Backend: backendA, IsDefault: true},
		},
	})
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), "placement.db")
	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	seedTestTypedConfirmedPlacementsWithExecution(t, store, oldRouter, nil)
	require.True(t, store.InventoryBootstrapped())
	require.NoError(t, store.Close())
	store, err = placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:   placementstore.ProviderUUID,
		PlacementStore: store,
	}, currentRouter, &chaintest.MockClient{})
	require.ErrorIs(t, err, placement.ErrInvalidBackendTopology)
	assert.Nil(t, manager)
	assert.True(t, store.InventoryBootstrapped(),
		"runtime construction must not mutate the previously committed topology")
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a"}))

	configureTestPlacementTopology(t, store, []string{"backend-a", "backend-b"})
	manager, err = NewManager(ManagerConfig{
		ProviderUUID:          placementstore.ProviderUUID,
		PlacementStore:        store,
		CallbackProofConsumer: callbackTestProofConsumer,
	}, currentRouter, &chaintest.MockClient{})
	require.NoError(t, err)
	require.NotNil(t, manager)
}

func TestNewManager_BackendConfigRevertSurvivesPlacementStoreReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backends := map[string]*mockManagerBackend{
		"backend-a": {name: "backend-a"},
		"backend-b": {name: "backend-b"},
	}

	startWithTopology := func(names ...string) {
		t.Helper()
		entries := make([]backend.BackendEntry, 0, len(names))
		for index, name := range names {
			entries = append(entries, backend.BackendEntry{
				Backend:   backends[name],
				IsDefault: index == 0,
			})
		}
		router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
		require.NoError(t, err)
		store, err := placementstore.NewStore(dbPath)
		require.NoError(t, err)
		configureTestPlacementTopology(t, store, names)
		manager, err := NewManager(ManagerConfig{
			ProviderUUID:          placementstore.ProviderUUID,
			PlacementStore:        store,
			CallbackProofConsumer: callbackTestProofConsumer,
		}, router, &chaintest.MockClient{})
		require.NoError(t, err)
		require.NotNil(t, manager)
		armTestPlacementAdmission(t, store, router)
		require.NoError(t, store.Close())
	}

	startWithTopology("backend-a", "backend-b")
	startWithTopology("backend-a")
	startWithTopology("backend-a", "backend-b")
}

func TestNewManager_RejectsTopologyThatWouldRemoveDurableOwner(t *testing.T) {
	store := newTestPlacementAuthority(t)
	seedTestTypedConfirmedPlacements(t, store, []string{"removed-backend"}, map[string]string{
		handlerTestLeaseOne: "removed-backend",
	})
	remaining := &mockManagerBackend{name: "remaining-backend"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: remaining, IsDefault: true}},
	})
	require.NoError(t, err)

	require.ErrorIs(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(store,
		[]string{"remaining-backend"},
		map[string]backendidentity.ID{
			"remaining-backend": testBackendStorageID("remaining-backend"),
		},
	), placement.ErrBackendTopologyInUse)

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:   placementstore.ProviderUUID,
		PlacementStore: store,
	}, router, &chaintest.MockClient{})
	require.ErrorIs(t, err, placement.ErrInvalidBackendTopology)
	assert.Nil(t, manager)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(handlerTestLeaseOne).State())
	assert.Equal(t, "removed-backend", store.Lookup(handlerTestLeaseOne).Backend)
}

func TestManager_HandleLeaseCreated(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{
					{SkuUuid: "sku-1", Quantity: 1},
				},
			}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Create a lease event message
	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: managerLeaseUUID,
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = handlersOf(manager).HandleLeaseCreated(msg)
	assert.NoError(t, err)

	// Verify provisioning was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, managerLeaseUUID, mockBackend.provisionCalls[0].LeaseUUID)
	assert.Equal(t, "tenant-1", mockBackend.provisionCalls[0].Tenant)
	assert.Equal(t, placementstore.ProviderUUID, mockBackend.provisionCalls[0].ProviderUUID)
	callbackURL, err := url.Parse(mockBackend.provisionCalls[0].CallbackURL)
	require.NoError(t, err)
	callbackID, present, err := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, err)
	require.True(t, present)
	require.True(t, callbackID.Valid())

	// Verify in-flight tracking
	assert.True(t, manager.IsInFlight(managerLeaseUUID), "lease should be in-flight after handleLeaseCreated")
}

func TestManager_HandleLeaseCreated_ProvisionError(t *testing.T) {
	provisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", provisionErr: provisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle should return error for retry
	err = handlersOf(manager).HandleLeaseCreated(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, provisionErr)

	// A generic backend error does not prove that the request was refused before
	// mutation. The process-local row may retire, but the durable attempt must
	// remain as the restart/reconciliation recovery authority.
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne))
	assert.Equal(t, placement.StateAttempting,
		managerTestPlacement(manager).Lookup(handlerTestLeaseOne).State())
}

func TestManager_HandleLeaseCreated_MalformedMessage(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = handlersOf(manager).HandleLeaseCreated(msg)
	assert.NoError(t, err, "should return nil for malformed message")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for malformed message")
}

func TestManager_HandleLeaseClosed(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseOne,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleLeaseClosed(msg)
	assert.NoError(t, err)

	// Verify deprovision was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, handlerTestLeaseOne, mockBackend.deprovisionCalls[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should not be in-flight after handleLeaseClosed")
}

func TestManager_HandleLeaseClosed_DeprovisionError(t *testing.T) {
	deprovisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", deprovisionErr: deprovisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseOne,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleLeaseClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDeprovisionFailed)
}

func TestManager_HandleBackendCallback_Success(t *testing.T) {
	var acknowledgedLeases []string
	var mu sync.Mutex

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			return []billingtypes.Lease{
				{Uuid: managerLeaseUUID, State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)
	// The batcher's lanes belong to Manager.Start (ENG-723); this test drives
	// the handler directly, so it starts them itself. t.Context() stops them.
	startAckBatcherForTest(t, manager)

	// Track the lease first
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, managerLeaseUUID, "tenant-1", testItems("sku-1"),
	)

	callback := backend.CallbackPayload{
		LeaseUUID:        managerLeaseUUID,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusSuccess,
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify acknowledge was called
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, acknowledgedLeases, 1)
	assert.Equal(t, managerLeaseUUID, acknowledgedLeases[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight(managerLeaseUUID), "lease should not be in-flight after successful callback")
}

func TestManager_HandleBackendCallback_Failed(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: managerLeaseUUID, Tenant: "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Track the lease first
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, managerLeaseUUID, "tenant-1", testItems("sku-1"),
	)

	callback := backend.CallbackPayload{
		LeaseUUID:        managerLeaseUUID,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusFailed,
		Error:            "out of resources",
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify removed from in-flight (failed is terminal)
	assert.False(t, manager.IsInFlight(managerLeaseUUID), "lease should not be in-flight after failed callback")
}

func TestManager_HandleBackendCallback_UnknownLease(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)
	// The batcher's lanes belong to Manager.Start (ENG-723); this test drives
	// the handler directly, so it starts them itself. t.Context() stops them.
	startAckBatcherForTest(t, manager)

	// Don't track the lease - simulating unknown callback
	callback := backend.CallbackPayload{
		LeaseUUID: managerUnknownLeaseUUID,
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (ignore unknown callbacks)
	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err, "should return nil for unknown lease")
}

func TestManager_HandleBackendCallback_AcknowledgeError(t *testing.T) {
	acknowledgeErr := errors.New("chain unavailable")

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			return []billingtypes.Lease{
				{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, acknowledgeErr
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)
	// The batcher's lanes belong to Manager.Start (ENG-723); this test drives
	// the handler directly, so it starts them itself. t.Context() stops them.
	startAckBatcherForTest(t, manager)

	// Track the lease
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)

	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusSuccess,
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleBackendCallback(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAcknowledgeFailed)

	// Verify still in-flight (for retry)
	assert.True(t, manager.IsInFlight(handlerTestLeaseOne), "lease should still be in-flight after acknowledge error (for retry)")
}

func TestManager_HandleBackendCallback_AcknowledgeTerminalError(t *testing.T) {
	// Test that a mutation error followed by an exact ACTIVE observation is
	// treated as success. The state, not the error shape, prevents retry loops.
	// Code 22 is ErrLeaseNotPending in the billing module.
	// This simulates a race condition where GetPendingLeases returns PENDING but the lease
	// gets acknowledged by another process before our AcknowledgeLeases call.
	terminalErr := &chain.ChainTxError{Code: 22, Codespace: "billing", RawLog: "lease not in pending state"}

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			// (simulates race condition where state changes between check and ack)
			return []billingtypes.Lease{
				{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, terminalErr
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: handlerTestLeaseOne, Tenant: "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)
	// The batcher's lanes belong to Manager.Start (ENG-723); this test drives
	// the handler directly, so it starts them itself. t.Context() stops them.
	startAckBatcherForTest(t, manager)

	// Track the lease
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)

	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusSuccess,
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// The exact ACTIVE reread proves the acknowledgement already won.
	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err, "should return nil for terminal acknowledge error")

	// Verify removed from in-flight (not stuck for retry)
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should NOT be in-flight after terminal acknowledge error")
}

func TestManager_HandleBackendCallback_UnknownStatus(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Track the lease
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)

	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           "unknown-status",
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// A structurally invalid callback is retryable/fail-closed at the typed
	// application boundary and cannot consume the current operation.
	err = handlersOf(manager).HandleBackendCallback(msg)
	require.ErrorContains(t, err, "invalid status")

	assert.True(t, manager.IsInFlight(handlerTestLeaseOne),
		"invalid status must not settle the exact in-flight operation")
}

func TestManager_PublishLeaseEvent(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	tests := []struct {
		name      string
		eventType chain.LeaseEventType
		wantErr   bool
	}{
		{"lease created", chain.LeaseCreated, false},
		{"lease closed", chain.LeaseClosed, false},
		{"lease expired", chain.LeaseExpired, false},
		{"lease acknowledged", chain.LeaseAcknowledged, false}, // Should return nil, not error
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := chain.LeaseEvent{
				Type:      tt.eventType,
				LeaseUUID: handlerTestLeaseOne,
				Tenant:    "tenant-1",
			}

			err := manager.PublishLeaseEvent(event)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestManager_PublishCallbackRequiresRunningRuntime(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	callback := backend.CallbackPayload{
		LeaseUUID: "b0000000-0000-4000-8000-000000000005",
		Status:    backend.CallbackStatusSuccess,
	}

	err = manager.PublishCallback(context.Background(), callbackCommand(t, callback))
	require.ErrorIs(t, err, errCallbackRuntimeUnavailable)
}

func TestManager_HandleLeaseExpired(t *testing.T) {
	// Verify handleLeaseExpired delegates to handleLeaseClosed
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Send LeaseExpired event (not LeaseClosed)
	event := chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: handlerTestLeaseOne,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleLeaseExpired(msg)
	assert.NoError(t, err)

	// Verify deprovision was called (same behavior as handleLeaseClosed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, handlerTestLeaseOne, mockBackend.deprovisionCalls[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should not be in-flight after handleLeaseExpired")
}

func TestManager_StartAndClose(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Start in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Start(ctx)
	}()

	// Wait for manager to be ready
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}

	// Close should stop the router
	assert.NoError(t, manager.Close())

	// Cancel context to ensure clean shutdown
	cancel()

	// Wait for Start to return
	select {
	case err := <-errCh:
		// Start may return nil or context.Canceled depending on timing
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start() did not return after Close()")
	}
}

func TestManager_CloseCancelsAndJoinsTimeoutChecker(t *testing.T) {
	rejectEntered := make(chan struct{}, 1)
	rejectExited := make(chan struct{})
	mockBackend := &mockManagerBackend{name: "test"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	require.NoError(t, err)
	mockChain := &chaintest.MockClient{RejectLeasesFunc: func(
		ctx context.Context, _ []string, _ string,
	) (uint64, []string, error) {
		select {
		case rejectEntered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		close(rejectExited)
		return 0, nil, ctx.Err()
	}}
	placementStore := newTestPlacementAuthority(t)
	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackTimeout: time.Nanosecond, TimeoutCheckInterval: time.Millisecond,
		PlacementStore: placementStore,
	}, router, mockChain)
	require.NoError(t, err)

	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	startErr := make(chan error, 1)
	go func() { startErr <- manager.Start(startCtx) }()
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}
	items := testItems("sku-1")
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseExpired, "tenant-1", items,
	)
	select {
	case <-rejectEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout checker did not reach the chain rejecter")
	}

	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	select {
	case <-rejectExited:
	case <-time.After(time.Second):
		t.Fatal("Manager.Close did not cancel the timeout chain call")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Manager.Close did not join the timeout checker")
	}
	select {
	case err := <-startErr:
		if err != nil {
			assert.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Manager.Start did not return after Close")
	}
	assert.True(t, manager.IsInFlight(handlerTestLeaseExpired),
		"canceled timeout settlement must preserve the operation")
	record := placementStore.Lookup(handlerTestLeaseExpired)
	assert.Equal(t, placement.StateConfirmed, record.State(),
		"shutdown must preserve the accepted backend owner")
	candidates := manager.timeoutChecker.coordinator.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	assert.Equal(t, operationID, candidates[0].Metadata().ID(),
		"shutdown must preserve the exact operation generation for retry")
}

func TestManager_HandleLeaseCreated_SKUBasedRouting(t *testing.T) {
	// Test that leases are routed to the correct backend based on SKU
	gpuBackend := &mockManagerBackend{name: "gpu-backend"}
	k8sBackend := &mockManagerBackend{name: "k8s-backend"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100-4x"}}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			// Return lease with GPU SKU
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100-4x", Quantity: 1}},
			}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Create a lease event message
	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "gpu-lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = handlersOf(manager).HandleLeaseCreated(msg)
	assert.NoError(t, err)

	// Verify GPU backend received the provision call
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	require.Len(t, gpuCalls, 1)
	assert.Equal(t, "gpu-lease-1", gpuCalls[0].LeaseUUID)
	assert.Equal(t, "gpu-a100-4x", gpuCalls[0].RoutingSKU())

	// Verify K8s backend did NOT receive any calls
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	assert.Empty(t, k8sCalls, "K8s backend should not have received any provision calls")

	// The durable write-ahead record, not a Manager Registry DTO, exposes the
	// selected backend for observation.
	record := managerTestPlacement(manager).Lookup("gpu-lease-1")
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, "gpu-backend", record.Backend)
	callbackURL, parseErr := url.Parse(gpuCalls[0].CallbackURL)
	require.NoError(t, parseErr)
	operationID, present, parseErr := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, parseErr)
	require.True(t, present)
	assert.True(t, operationID.Valid())
}

func TestManager_HandleBackendCallback_FailedRejectsLease(t *testing.T) {
	// Test that failed callbacks trigger lease rejection
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: handlerTestLeaseOne, Tenant: "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Track the lease first
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)

	// Send failed callback with error message
	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusFailed,
		Error:            "out of GPU resources",
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, handlerTestLeaseOne, rejectedLeases[0])
	assert.Equal(t, "out of GPU resources", rejectedReason)

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should not be in-flight after failed callback")
}

func TestManager_HandleBackendCallback_FailedDefaultReason(t *testing.T) {
	// Test that failed callbacks without error message use default reason
	var rejectedReason string
	var mu sync.Mutex

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         handlerTestLeaseOne,
				Tenant:       "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// Track the lease first
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)

	// Send failed callback WITHOUT error message
	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusFailed,
		Error:            "", // Empty error
		OperationID:      operationID.String(),
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify default reason was used
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "provisioning failed", rejectedReason)
}

func TestExtractRoutingSKU(t *testing.T) {
	tests := []struct {
		name  string
		lease *billingtypes.Lease
		want  string
	}{
		{
			name:  "nil lease",
			lease: nil,
			want:  "",
		},
		{
			name:  "empty items",
			lease: &billingtypes.Lease{Items: []billingtypes.LeaseItem{}},
			want:  "",
		},
		{
			name: "single item",
			lease: &billingtypes.Lease{
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-123", Quantity: 1}},
			},
			want: "sku-123",
		},
		{
			name: "multiple items - returns first",
			lease: &billingtypes.Lease{
				Items: []billingtypes.LeaseItem{
					{SkuUuid: "first-sku", Quantity: 1},
					{SkuUuid: "second-sku", Quantity: 2},
				},
			},
			want: "first-sku",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractRoutingSKU(tt.lease)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_WaitForDrain_AlreadyEmpty(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	// No in-flight leases, should return immediately with 0
	remaining := manager.WaitForDrain(context.Background(), 100*time.Millisecond)
	assert.Equal(t, 0, remaining)
}

// mockPayloadStore implements a simple in-memory payload store for testing.
type mockPayloadStore struct {
	mu       sync.Mutex
	payloads map[string][]byte
}

func newMockPayloadStore() *mockPayloadStore {
	return &mockPayloadStore{
		payloads: make(map[string][]byte),
	}
}

func (m *mockPayloadStore) Store(leaseUUID string, payload []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.payloads[leaseUUID]; exists {
		return false // Already exists
	}
	m.payloads[leaseUUID] = payload
	return true
}

func (m *mockPayloadStore) Get(leaseUUID string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.payloads[leaseUUID]
}

func (m *mockPayloadStore) Has(leaseUUID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.payloads[leaseUUID]
	return exists
}

func (m *mockPayloadStore) Delete(leaseUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.payloads, leaseUUID)
}

func TestManager_HandlePayloadReceived(t *testing.T) {
	testPayload := []byte("deployment manifest data")
	testPayloadHash := hashPayload(testPayload)
	chainPayloadHash, err := hex.DecodeString(testPayloadHash)
	require.NoError(t, err)
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{
					{SkuUuid: "sku-1", Quantity: 1},
				},
				MetaHash: chainPayloadHash,
			}, nil
		},
	}

	// Create a real PayloadStore using temp directory
	tempDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload first
	payloadStore.Store(handlerTestLeaseOne, testPayload)

	// Create a payload event message
	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err)

	// Verify provisioning was called with payload
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, handlerTestLeaseOne, mockBackend.provisionCalls[0].LeaseUUID)
	assert.Equal(t, string(testPayload), string(mockBackend.provisionCalls[0].Payload))
	assert.Equal(t, testPayloadHash, mockBackend.provisionCalls[0].PayloadHash)

	// Verify in-flight tracking
	assert.True(t, manager.IsInFlight(handlerTestLeaseOne), "lease should be in-flight after handlePayloadReceived")

	// Verify payload is still in store - it should only be deleted after callback
	// This ensures the payload is available for retry if provisioning fails
	hasP, errP := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP)
	assert.True(t, hasP, "payload should remain in store until callback is received")
}

func TestManager_HandlePayloadReceived_NoPayloadStore(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	// Create manager WITHOUT payload store
	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: nil, // No payload store
	}, router, mockChain)
	require.NoError(t, err)

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (no retry) when payload store is not configured
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for missing payload store")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls when payload store is nil")
}

func TestManager_HandlePayloadReceived_MalformedMessage(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for malformed message")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for malformed message")
}

func TestManager_HandlePayloadReceived_UnknownLeasePreservesPayload(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload
	payloadStore.Store(handlerTestLeaseOne, []byte("payload data"))

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// A nil point-read is not terminal proof. Preserve the only recovery payload
	// and ask Watermill to retry rather than turning RPC lag into data loss.
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.Error(t, err, "unknown chain absence must be retried")

	// Verify payload was preserved.
	hasP2, errP2 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP2)
	assert.True(t, hasP2, "unknown chain absence is not terminal evidence")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for lease not found")
}

func TestManager_HandlePayloadReceived_ActiveLeasePreservesPayload(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_ACTIVE, // Not PENDING
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload
	payloadStore.Store(handlerTestLeaseOne, []byte("payload data"))

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// A delayed duplicate event for an ACTIVE lease is acknowledged without
	// deleting the manifest still needed for reprovision and recovery.
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err, "active duplicate payload event should be acknowledged")

	// Verify payload was preserved.
	hasP3, errP3 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP3)
	assert.True(t, hasP3, "ACTIVE recovery requires the durable manifest")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for non-pending lease")
}

func TestManager_HandlePayloadReceived_ChainError(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	chainErr := errors.New("chain unavailable")
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, chainErr
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = handlersOf(manager).HandlePayloadReceived(msg)
	require.Error(t, err, "should return error for chain error")
}

func TestManager_HandlePayloadReceived_ProvisionError(t *testing.T) {
	provisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", provisionErr: provisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	testPayload := []byte("payload data")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store(handlerTestLeaseOne, testPayload)

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = handlersOf(manager).HandlePayloadReceived(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, provisionErr)

	// Verify lease was untracked after error
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should not be in-flight after provision error")

	// Verify payload was NOT deleted (kept for retry)
	hasP4, errP4 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP4)
	assert.True(t, hasP4, "payload should be kept for retry after provision error")
}

func TestManager_HandlePayloadReceived_AlreadyInFlight(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Arrange concurrent processing through the real provision application,
	// then clear the observation log so this assertion counts only a second call.
	requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, handlerTestLeaseOne, "tenant-1", testItems("sku-1"),
	)
	mockBackend.mu.Lock()
	mockBackend.provisionCalls = nil
	mockBackend.mu.Unlock()

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (skip already in-flight lease)
	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for already in-flight lease")

	// Verify no provisioning was attempted (already being processed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for already in-flight lease")
}

func TestManager_HandlePayloadReceived_MissingPayloadInStore(t *testing.T) {
	payloadHash := sha256.Sum256([]byte("missing payload"))
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				MetaHash:     payloadHash[:],
				Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// DON'T store a payload - simulate race where payload was cleaned up

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: hex.EncodeToString(payloadHash[:]),
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// The durable message is retried until its payload write becomes visible.
	err = handlersOf(manager).HandlePayloadReceived(msg)
	require.EqualError(t, err, "payload is not available")

	// A payload-bearing chain request can never be downgraded to payloadless.
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestManager_HandlePayloadReceived_SKUBasedRouting(t *testing.T) {
	gpuBackend := &mockManagerBackend{name: "gpu-backend"}
	k8sBackend := &mockManagerBackend{name: "k8s-backend"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100"}}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Items:        []billingtypes.LeaseItem{{SkuUuid: "gpu-a100", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	testPayload := []byte("gpu deployment")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store(handlerTestLeaseOne, testPayload)

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = handlersOf(manager).HandlePayloadReceived(msg)
	assert.NoError(t, err)

	// Verify GPU backend received the call
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	require.Len(t, gpuCalls, 1)
	assert.Equal(t, "gpu-a100", gpuCalls[0].RoutingSKU())

	// Verify K8s backend did NOT receive any calls
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	assert.Empty(t, k8sCalls, "K8s backend should not have received any provision calls")

	// Verify the durable operation attempt has the correct backend.
	record := managerTestPlacement(manager).Lookup(handlerTestLeaseOne)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, "gpu-backend", record.Backend)
}

// TestManager_CheckCallbackTimeouts tests the timeout detection and rejection logic.
// TestManager_RunTimeoutChecker tests the background timeout checker goroutine.
func TestManager_RunTimeoutChecker(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var mu sync.Mutex
	var rejectedLeases []string

	mockChain := &chaintest.MockClient{
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID:         "provider-1",
		CallbackTimeout:      50 * time.Millisecond, // Short for testing
		TimeoutCheckInterval: 25 * time.Millisecond, // Check frequently
	}, router, mockChain)
	require.NoError(t, err)

	// Start a real provision operation, then let the background checker age and
	// settle it through the production timeout application.
	requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, "auto-timeout-lease", "tenant-1", testItems("test-sku"),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.timeoutChecker.Start(ctx)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(rejectedLeases) != 0
	}, time.Second, 10*time.Millisecond)

	// Verify the lease was rejected
	mu.Lock()
	found := false
	for _, lease := range rejectedLeases {
		if lease == "auto-timeout-lease" {
			found = true
			break
		}
	}
	mu.Unlock()

	assert.True(t, found, "timeout checker should have rejected the timed-out lease")

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight("auto-timeout-lease"), "lease should be removed from in-flight after timeout")
}

// TestPayloadPersistsUntilCallback is a regression test for the bug where payload
// was deleted immediately after Provision() returned HTTP 202, rather than waiting
// for the backend callback. This caused payloads to be lost if the backend failed
// and the callback didn't arrive (e.g., due to network issues or TLS mismatch).
//
// The fix ensures payloads persist until the callback confirms success or failure,
// allowing reconciliation to retry provisioning if the provider restarts.
func TestPayloadPersistsUntilCallback(t *testing.T) {
	// Setup
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	testPayload := []byte(`{"image": "nginx:alpine"}`)
	testPayloadHash := hashPayload(testPayload)

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: t.TempDir() + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)
	// The batcher's lanes belong to Manager.Start (ENG-723); this test drives
	// the handler directly, so it starts them itself. t.Context() stops them.
	startAckBatcherForTest(t, manager)

	// Store payload (simulating upload)
	require.True(t, payloadStore.Store(handlerTestLeaseOne, testPayload), "failed to store payload")

	// Step 1: Send payload received event to trigger provisioning
	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	// One set, reused: HandlerSet's awaitingPayload state spans these calls.
	h := handlersOf(manager)
	err = h.HandlePayloadReceived(msg)
	require.NoError(t, err)

	// Step 2: REGRESSION CHECK - Payload must still exist after Provision() returns
	// Previously, the payload was deleted here, causing data loss if callback failed
	hasP5, errP5 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP5)
	require.True(t, hasP5, "REGRESSION: payload was deleted after Provision() - should persist until callback")

	// Verify provisioning was called
	mockBackend.mu.Lock()
	require.Len(t, mockBackend.provisionCalls, 1)
	mockBackend.mu.Unlock()

	// Verify lease is in-flight
	require.True(t, manager.IsInFlight(handlerTestLeaseOne), "lease should be in-flight after provisioning started")

	// Step 3: Simulate successful callback - payload should be deleted now
	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusSuccess,
		OperationID:      lastManagerBackendOperationID(t, mockBackend, handlerTestLeaseOne),
	}
	callbackPayload, _ := json.Marshal(callback)
	callbackMsg := message.NewMessage(watermill.NewUUID(), callbackPayload)

	err = h.HandleBackendCallback(callbackMsg)
	require.NoError(t, err)

	// Payload should persist after successful callback — it's retained for
	// potential re-provisioning if the container crashes after acknowledgment.
	// Cleanup happens when the lease is closed or rejected.
	hasP6, errP6 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP6)
	assert.True(t, hasP6, "payload should persist after successful callback for re-provisioning")

	// Lease should no longer be in-flight
	assert.False(t, manager.IsInFlight(handlerTestLeaseOne), "lease should not be in-flight after successful callback")
}

// TestPayloadDeletedAfterFailedCallback verifies that payloads are cleaned up
// after a failed callback, not left orphaned in the store.
func TestPayloadDeletedAfterFailedCallback(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	testPayload := []byte(`{"image": "nginx:alpine"}`)
	testPayloadHash := hashPayload(testPayload)

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: t.TempDir() + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	require.True(t, payloadStore.Store(handlerTestLeaseOne, testPayload), "failed to store payload")

	// Trigger provisioning
	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	// One set, reused: HandlerSet's awaitingPayload state spans these calls.
	h := handlersOf(manager)
	err = h.HandlePayloadReceived(msg)
	require.NoError(t, err)

	// Payload should still exist
	hasP7, errP7 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP7)
	require.True(t, hasP7, "payload should exist after provisioning started")

	// Simulate failed callback
	callback := backend.CallbackPayload{
		LeaseUUID:        handlerTestLeaseOne,
		Backend:          mockBackend.Name(),
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		Status:           backend.CallbackStatusFailed,
		Error:            "unknown SKU",
		OperationID:      lastManagerBackendOperationID(t, mockBackend, handlerTestLeaseOne),
	}
	callbackPayload, _ := json.Marshal(callback)
	callbackMsg := message.NewMessage(watermill.NewUUID(), callbackPayload)

	err = h.HandleBackendCallback(callbackMsg)
	require.NoError(t, err)

	// Payload should be deleted after failed callback (no point keeping it)
	hasP8, errP8 := payloadStore.Has(handlerTestLeaseOne)
	require.NoError(t, errP8)
	assert.False(t, hasP8, "payload should be deleted after failed callback")
}

// TestPayloadSurvivesRestartForReconciliation verifies that if a provider restarts
// before receiving a callback, the payload is still available for reconciliation
// to retry provisioning.
func TestPayloadSurvivesRestartForReconciliation(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	testPayload := []byte(`{"image": "nginx:alpine"}`)
	testPayloadHash := hashPayload(testPayload)

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	// Use a persistent temp dir for the payload store
	dbPath := t.TempDir() + "/payloads.db"

	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload and trigger provisioning
	require.True(t, payloadStore.Store(handlerTestLeaseOne, testPayload), "failed to store payload")

	event := payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	err = handlersOf(manager).HandlePayloadReceived(msg)
	require.NoError(t, err)

	// Verify provisioning was called
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()
	require.Equal(t, 1, provisionCount)

	// Simulate restart: close payload store (manager doesn't need to be started for this test)
	payloadStore.Close()

	// "Restart" - create new payload store from same DB file
	payloadStore2, err := payload.NewStore(payload.StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)
	defer payloadStore2.Close()

	// KEY ASSERTION: Payload should still exist after "restart"
	// This is what allows reconciliation to retry provisioning
	hasP9, errP9 := payloadStore2.Has(handlerTestLeaseOne)
	require.NoError(t, errP9)
	require.True(t, hasP9, "REGRESSION: payload was lost after restart - reconciliation cannot retry")

	// Verify we can retrieve the payload with correct content
	retrievedPayload, err := payloadStore2.Get(handlerTestLeaseOne)
	require.NoError(t, err)
	assert.Equal(t, string(testPayload), string(retrievedPayload))
}

// TestCallbacksRequireRunningManager is a regression test for the startup race
// where the HTTP server can receive a callback before the ack batcher starts.
// The backend must receive a retryable error before Start and a terminal result
// only after the synchronous application path is ready.
func TestCallbacksRequireRunningManager(t *testing.T) {
	const (
		earlyLeaseUUID = "b0000000-0000-4000-8000-000000000001"
		afterLeaseUUID = "b0000000-0000-4000-8000-000000000002"
	)
	// This test verifies that:
	// 1. Publishing before Start is rejected rather than partially applied.
	// 2. Publishing after startup returns only after the callback is processed.

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var ackCalled atomic.Bool
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return the test lease as pending so the batcher will call AcknowledgeLeases
			return []billingtypes.Lease{
				{Uuid: afterLeaseUUID, Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			ackCalled.Store(true)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The callback runtime is deliberately unavailable before Start. In
	// production the API maps this to 503 so the backend's durable outbox keeps
	// the delivery pending.
	earlyOperationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, earlyLeaseUUID, "tenant-1", testItems("sku-1"),
	)
	require.ErrorIs(t, manager.PublishCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:        earlyLeaseUUID,
		Status:           backend.CallbackStatusSuccess,
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		OperationID:      earlyOperationID.String(),
	})), errCallbackRuntimeUnavailable)
	require.True(t, manager.IsInFlight(earlyLeaseUUID))

	// Start manager in background.
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Start(ctx)
	}()

	// Running is a convenient observable startup barrier. Callback admission is
	// opened slightly earlier, immediately after the ack batcher starts.
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start within timeout")
	}

	// Track another lease for the "after Running()" test
	afterOperationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, afterLeaseUUID, "tenant-1", testItems("sku-1"),
	)

	// After startup, PublishCallback applies synchronously.
	callback := backend.CallbackPayload{
		LeaseUUID:        afterLeaseUUID,
		Status:           backend.CallbackStatusSuccess,
		BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
		OperationID:      afterOperationID.String(),
	}
	require.NoError(t, manager.PublishCallback(context.Background(), callbackCommand(t, callback)))

	// Verify lease was removed from in-flight (full handler flow completed)
	assert.False(t, manager.IsInFlight(afterLeaseUUID), "lease should not be in-flight after successful callback")

	// If the lease was untracked, the acknowledge must have been called
	assert.True(t, ackCalled.Load(), "callback returned before chain acknowledgment")

	// Clean up
	cancel()
	manager.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Error("manager.Start() did not return after cancel")
	}
}

// TestManager_CloseCancelsAndDrainsActiveCallback verifies that callbacks
// bypassing Watermill are still owned by Manager's shutdown boundary. Close
// rejects new admissions, cancels active application, drains it, and only then
// stops AckBatcher; it must not wait for an unresponsive chain indefinitely.
func TestManager_CloseCancelsAndDrainsActiveCallback(t *testing.T) {
	const (
		leaseUUID     = "b0000000-0000-4000-8000-000000000003"
		lateLeaseUUID = "b0000000-0000-4000-8000-000000000004"
	)
	ackReached := make(chan struct{}, 1) // signals AcknowledgeLeases is executing
	releaseAck := make(chan struct{})

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			// Signal that the handler has reached the chain acknowledge call
			select {
			case ackReached <- struct{}{}:
			default:
			}
			// Hold the batch operation until Manager.Close cancels its lifecycle.
			select {
			case <-releaseAck:
				return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID:     "provider-1",
		AckBatchInterval: 50 * time.Millisecond, // Flush quickly
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- manager.Start(ctx) }()

	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}

	// Track lease and publish a success callback asynchronously so Close can
	// overlap the synchronous application call.
	operationID := requireManagerProvisionCallbackOperation(
		t, manager, mockBackend, leaseUUID, "tenant-1", testItems("sku-1"),
	)
	callbackErr := make(chan error, 1)
	go func() {
		callbackErr <- manager.PublishCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
			LeaseUUID:        leaseUUID,
			Status:           backend.CallbackStatusSuccess,
			BackendStorageID: testBackendStorageID(mockBackend.Name()).String(),
			OperationID:      operationID.String(),
		}))
	}()

	// Wait for the handler to reach AcknowledgeLeases inside the batcher.
	select {
	case <-ackReached:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach AcknowledgeLeases")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- manager.Close() }()

	require.Eventually(t, func() bool {
		manager.callbackAdmissionMu.Lock()
		defer manager.callbackAdmissionMu.Unlock()
		return manager.callbackClosed
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, manager.PublishCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: lateLeaseUUID,
		Status:    backend.CallbackStatusFailed,
	})), errCallbackRuntimeUnavailable)
	select {
	case err := <-callbackErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel the active callback")
	}
	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close remained blocked after canceling callback application")
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
}

func TestManager_PoisonQueue_BreaksInfiniteLoop(t *testing.T) {
	// This test verifies that when a handler permanently fails (exhausting
	// all retries), the message is sent to the poison queue instead of being
	// re-delivered in an infinite loop.
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var callCount atomic.Int32
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			callCount.Add(1)
			// Always fail to trigger retries
			return nil, errors.New("permanent chain failure")
		},
	}

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- manager.Start(ctx) }()

	// Wait for router to be running
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start in time")
	}

	// Publish a lease event that will permanently fail
	err = manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "poison-lease",
		Tenant:    "tenant-1",
	})
	require.NoError(t, err)

	// Wait for the message to be processed (retries + poison queue)
	time.Sleep(3 * time.Second)

	// The handler should have been called (initial + 3 retries = 4 times),
	// NOT infinitely. With poison queue, the message is acknowledged after
	// retries are exhausted, breaking the loop.
	count := callCount.Load()
	assert.GreaterOrEqual(t, count, int32(4), "handler should be called at least 4 times (1 + 3 retries)")
	assert.LessOrEqual(t, count, int32(8), "handler should NOT be called many more times (poison queue should stop the loop)")

	cancel()
	manager.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Error("manager.Start() did not return after cancel")
	}
}

// --- forwardToEventSink tests ---

// mockLeaseEventSink captures events published to the event sink.
type mockLeaseEventSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
}

func (s *mockLeaseEventSink) Publish(event backend.LeaseStatusEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

type blockingLeaseEventSink struct {
	entered chan struct{}
	release chan struct{}
	event   backend.LeaseStatusEvent
}

func (s *blockingLeaseEventSink) Publish(event backend.LeaseStatusEvent) {
	close(s.entered)
	<-s.release
	s.event = event
}

func TestManager_PublishProvisionStartingAppliesSynchronously(t *testing.T) {
	sink := &blockingLeaseEventSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := &Manager{leaseEventSink: sink}
	done := make(chan struct{})
	go func() {
		manager.PublishProvisionStarting(handlerTestLeaseOne)
		close(done)
	}()

	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("provisioning event did not reach the subscriber sink")
	}
	select {
	case <-done:
		t.Fatal("PublishProvisionStarting returned before the sink applied the event")
	default:
	}
	close(sink.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PublishProvisionStarting did not return after event application")
	}

	assert.Equal(t, handlerTestLeaseOne, sink.event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusProvisioning, sink.event.Status)
}

func TestForwardToEventSink_ForwardsToSink(t *testing.T) {
	sink := &mockLeaseEventSink{}
	m := &Manager{leaseEventSink: sink}

	event := backend.LeaseStatusEvent{
		LeaseUUID: handlerTestLeaseOne,
		Status:    backend.ProvisionStatusReady,
		Error:     "",
		Timestamp: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), data)
	err = m.forwardToEventSink(msg)
	assert.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.events, 1)
	assert.Equal(t, handlerTestLeaseOne, sink.events[0].LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, sink.events[0].Status)
	assert.Empty(t, sink.events[0].Error)
	assert.Equal(t, time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC), sink.events[0].Timestamp)
}

func TestForwardToEventSink_IncludesError(t *testing.T) {
	sink := &mockLeaseEventSink{}
	m := &Manager{leaseEventSink: sink}

	event := backend.LeaseStatusEvent{
		LeaseUUID: handlerTestLeaseTwo,
		Status:    backend.ProvisionStatusFailed,
		Error:     "OOM killed",
		Timestamp: time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(event)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), data)
	err = m.forwardToEventSink(msg)
	assert.NoError(t, err)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.events, 1)
	assert.Equal(t, handlerTestLeaseTwo, sink.events[0].LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, sink.events[0].Status)
	assert.Equal(t, "OOM killed", sink.events[0].Error)
}

func TestForwardToEventSink_MalformedMessage(t *testing.T) {
	sink := &mockLeaseEventSink{}
	m := &Manager{leaseEventSink: sink}

	msg := message.NewMessage(watermill.NewUUID(), []byte("not json"))
	err := m.forwardToEventSink(msg)
	assert.NoError(t, err, "should return nil for malformed messages (don't retry)")

	sink.mu.Lock()
	defer sink.mu.Unlock()
	assert.Empty(t, sink.events, "no events should reach the sink")
}

// --- ENG-619: Manager.OverwritePayload ---

func TestManager_OverwritePayload_ReplacesStoredPayload(t *testing.T) {
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: &mockManagerBackend{name: "test"}, IsDefault: true}},
	})
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: t.TempDir() + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		PayloadStore: payloadStore,
	}, router, &chaintest.MockClient{})
	require.NoError(t, err)

	original := []byte("original manifest")
	updated := []byte("updated manifest")
	require.True(t, manager.StorePayload(handlerTestLeaseOne, original))

	require.NoError(t, manager.OverwritePayload(handlerTestLeaseOne, updated))

	got, err := payloadStore.Get(handlerTestLeaseOne)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	// The recorded hash moves with the payload, which is what lets the
	// reprovision path accept an updated manifest that no longer matches the
	// lease's create-time MetaHash.
	_, gotHash, err := payloadStore.GetWithHash(handlerTestLeaseOne)
	require.NoError(t, err)
	want := sha256.Sum256(updated)
	assert.Equal(t, want[:], gotHash)
}

func TestManager_OverwritePayload_NoStoreReturnsSentinel(t *testing.T) {
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: &mockManagerBackend{name: "test"}, IsDefault: true}},
	})
	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID: "provider-1",
		// PayloadStore deliberately omitted
	}, router, &chaintest.MockClient{})
	require.NoError(t, err)

	// Must be an error, not a silent no-op: the caller has already applied the
	// update to a backend, and reporting success would recreate ENG-619.
	err = manager.OverwritePayload(handlerTestLeaseOne, []byte("updated manifest"))
	require.ErrorIs(t, err, ErrPayloadStoreUnavailable)
}
