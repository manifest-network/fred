package provisioner

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// noopAck is a no-op acknowledger for tests that don't exercise the ack path.
// The zero-value mockAcknowledger returns (true, "tx-hash", nil) by default.
var noopAck = &mockAcknowledger{}

// mockReconcilerBackend implements backend.Backend for testing.
type mockReconcilerBackend struct {
	mu               sync.Mutex
	name             string
	provisions       []backend.ProvisionInfo
	provisionCalls   []backend.ProvisionRequest
	deprovisionCalls []string
	provisionErr     error
	deprovisionErr   error
	listErr          error
	refreshErr       error
}

func (m *mockReconcilerBackend) Name() string {
	return m.name
}

func (m *mockReconcilerBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.provisionCalls = append(m.provisionCalls, req)
	if m.provisionErr != nil {
		return m.provisionErr
	}
	return nil
}

func (m *mockReconcilerBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	info := backend.LeaseInfo{Host: "localhost"}
	return &info, nil
}

func (m *mockReconcilerBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deprovisionCalls = append(m.deprovisionCalls, leaseUUID)
	if m.deprovisionErr != nil {
		return m.deprovisionErr
	}
	return nil
}

func (m *mockReconcilerBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.provisions, nil
}

func (m *mockReconcilerBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	result := make([]backend.ProvisionInfo, 0, len(uuids))
	wanted := make(map[string]struct{}, len(uuids))
	for _, u := range uuids {
		wanted[u] = struct{}{}
	}
	for _, p := range m.provisions {
		if _, ok := wanted[p.LeaseUUID]; ok {
			result = append(result, p)
		}
	}
	return result, nil
}

func (m *mockReconcilerBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockReconcilerBackend) RefreshState(ctx context.Context) error {
	return m.refreshErr
}

func (m *mockReconcilerBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockReconcilerBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockReconcilerBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockReconcilerBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockReconcilerBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func TestNewReconciler_Validation(t *testing.T) {
	mockChain := &chain.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tests := []struct {
		name        string
		cfg         ReconcilerConfig
		chainClient ReconcilerChainClient
		ack         Acknowledger
		router      BackendRouter
		wantErr     string
	}{
		{
			name:        "missing chain client",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: nil,
			ack:         noopAck,
			router:      router,
			wantErr:     "chain client is required",
		},
		{
			name:        "missing acknowledger",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         nil,
			router:      router,
			wantErr:     "acknowledger is required",
		},
		{
			name:        "missing router",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      nil,
			wantErr:     "backend router is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ReconcilerConfig{CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "provider UUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "callback base URL is required",
		},
		{
			name:        "valid config",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReconciler(tt.cfg, tt.chainClient, tt.ack, tt.router, nil, nil)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestReconciler_ReconcileAll_PendingNotProvisioned(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned on backend
	// Expected: Start provisioning
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Empty - not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)
}

func TestReconciler_ReconcileAll_PendingProvisionedReady(t *testing.T) {
	// Setup: Pending lease on chain, provisioned and ready on backend
	// Expected: Acknowledge the lease via the acknowledger
	var acknowledgedLeases []string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUID)
			return true, "tx-hash", nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was acknowledged via the acknowledger
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, acknowledgedLeases, 1)
	assert.Equal(t, "lease-1", acknowledgedLeases[0])
}

func TestReconciler_ReconcileAll_ActiveNotProvisioned(t *testing.T) {
	// Setup: Active lease on chain, not provisioned on backend (anomaly)
	// Expected: Log anomaly and attempt to provision
	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Empty - not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was attempted (anomaly recovery)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
}

func TestReconciler_ReconcileAll_ActiveProvisioned(t *testing.T) {
	// Setup: Active lease on chain, provisioned on backend
	// Expected: Nothing (healthy state)
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount += len(leaseUUIDs)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify nothing was done
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)
	assert.Equal(t, 0, ackCount)
}

func TestReconciler_ReconcileAll_OrphanProvision(t *testing.T) {
	// Setup: No lease on chain, but provisioned on backend (orphan)
	// Expected: Deprovision the orphan
	mockChain := &chain.MockClient{
		// No leases - default funcs return empty slices
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-lease", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify orphan was deprovisioned
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "orphan-lease", mockBackend.deprovisionCalls[0])
}

func TestReconciler_ReconcileAll_ChainErrors(t *testing.T) {
	// Test that chain errors are handled gracefully
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tests := []struct {
		name    string
		setup   func() *chain.MockClient
		wantErr string
	}{
		{
			name: "get pending error",
			setup: func() *chain.MockClient {
				return &chain.MockClient{
					GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
						return nil, errors.New("chain unavailable")
					},
				}
			},
			wantErr: "failed to get pending leases",
		},
		{
			name: "get active error",
			setup: func() *chain.MockClient {
				return &chain.MockClient{
					GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
						return nil, errors.New("chain unavailable")
					},
				}
			},
			wantErr: "failed to get active leases",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChain := tt.setup()

			reconciler, err := NewReconciler(ReconcilerConfig{
				ProviderUUID:    "provider-1",
				CallbackBaseURL: "http://localhost:8080",
			}, mockChain, noopAck, router, nil, nil)
			require.NoError(t, err)

			ctx := t.Context()
			err = reconciler.ReconcileAll(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestReconciler_ReconcileAll_ContextCancellation(t *testing.T) {
	// Test that ReconcileAll respects context cancellation
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Cancel context before calling ReconcileAll
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = reconciler.ReconcileAll(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReconciler_Start_ContextCancellation(t *testing.T) {
	// Test that Start respects context cancellation
	mockChain := &chain.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		Interval:        100 * time.Millisecond, // Short interval for test
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Start in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.Start(ctx)
	}()

	// Give it a moment to start, then cancel
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Should exit with context.Canceled
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Error("Start() did not exit after context cancellation")
	}
}

func TestReconciler_DefaultInterval(t *testing.T) {
	mockChain := &chain.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create with no interval specified
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		// Interval not set
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Verify default interval is 5 minutes
	assert.Equal(t, 5*time.Minute, reconciler.interval)
}

func TestReconciler_RunOnce(t *testing.T) {
	// Verify RunOnce calls ReconcileAll
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.RunOnce(ctx))

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
}

func TestReconciler_ReconcileAll_SkipsInFlightLeases(t *testing.T) {
	// Test that reconciler skips leases that are already being provisioned
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create a manager and mark the lease as in-flight
	manager, _ := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, &chain.MockClient{})
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, manager, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was NOT started (lease is in-flight)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_MultipleBackends(t *testing.T) {
	// Test reconciliation with multiple backends
	mockChain := &chain.MockClient{}

	backend1 := &mockReconcilerBackend{
		name: "backend1",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
		},
	}
	backend2 := &mockReconcilerBackend{
		name: "backend2",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, IsDefault: true},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"b2-sku"}}},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Both orphans should be deprovisioned
	backend1.mu.Lock()
	b1Calls := len(backend1.deprovisionCalls)
	backend1.mu.Unlock()

	backend2.mu.Lock()
	b2Calls := len(backend2.deprovisionCalls)
	backend2.mu.Unlock()

	// Total deprovisions should be 2
	assert.Equal(t, 2, b1Calls+b2Calls)
}

func TestReconciler_ReconcileAll_PendingProvisioning(t *testing.T) {
	// Setup: Pending lease on chain, provisioning in progress on backend
	// Expected: Wait (do nothing) - let the callback complete the flow
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount += len(leaseUUIDs)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no actions were taken
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)
	assert.Equal(t, 0, ackCount)
}

func TestReconciler_ReconcileAll_PendingFailed(t *testing.T) {
	// Setup: Pending lease on chain, provisioning failed on backend
	// Expected: Reject the lease so tenant's credit is released
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no provisioning or deprovisioning
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, rejectedLeases, 1)
	if len(rejectedLeases) > 0 {
		assert.Equal(t, "lease-1", rejectedLeases[0])
	}
	assert.Equal(t, "provisioning failed", rejectedReason)
}

func TestReconciler_ReconcileAll_AcknowledgeFailure(t *testing.T) {
	// Test that acknowledge failure is logged but doesn't stop reconciliation
	acknowledgeErr := errors.New("chain unavailable")

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	failingAck := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", acknowledgeErr
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, failingAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll should succeed even if individual acknowledges fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err)
}

func TestReconciler_ReconcileAll_DeprovisionFailure(t *testing.T) {
	// Test that deprovision failure during orphan cleanup is logged but continues
	deprovisionErr := errors.New("backend unavailable")

	mockChain := &chain.MockClient{
		// No leases - both provisions are orphans
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
		},
		deprovisionErr: deprovisionErr,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll should succeed even if deprovisions fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err)

	// Verify both deprovisions were attempted
	mockBackend.mu.Lock()
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 2, deprovisionCount)
}

func TestReconciler_ReconcileAll_SkipsOtherProviderOrphans(t *testing.T) {
	// Test that reconciler does NOT deprovision orphans belonging to other providers.
	// This is critical when multiple providers share the same backend.
	mockChain := &chain.MockClient{
		// No leases for our provider
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			// Orphan belonging to a DIFFERENT provider - should NOT be deprovisioned
			{LeaseUUID: "other-provider-lease", ProviderUUID: "other-provider-uuid", Status: backend.ProvisionStatusReady},
			// Orphan belonging to OUR provider - should be deprovisioned
			{LeaseUUID: "our-orphan-lease", ProviderUUID: "provider-1", Status: backend.ProvisionStatusReady},
			// Orphan with empty provider UUID (legacy) - should be deprovisioned (conservative)
			{LeaseUUID: "legacy-orphan-lease", ProviderUUID: "", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify only our orphans were deprovisioned (not the other provider's)
	mockBackend.mu.Lock()
	deprovisionCalls := mockBackend.deprovisionCalls
	mockBackend.mu.Unlock()

	// Should have deprovisioned 2 leases: our-orphan-lease and legacy-orphan-lease
	// Should NOT have deprovisioned other-provider-lease
	assert.Len(t, deprovisionCalls, 2)

	// Verify the other provider's lease was NOT deprovisioned
	assert.NotContains(t, deprovisionCalls, "other-provider-lease")
}

// TestReconciler_ConcurrentProvisioningRace is a regression test for the TOCTOU race
// condition between the reconciler and event-driven manager. It simulates multiple
// goroutines (representing manager and reconciler) racing to provision the same lease.
//
// The test verifies that despite concurrent attempts, exactly ONE provision call is
// made to the backend - preventing duplicate resource creation.
//
// Run with: go test -race -run TestReconciler_ConcurrentProvisioningRace -count=10
func TestReconciler_ConcurrentProvisioningRace(t *testing.T) {
	const leaseUUID = "race-test-lease"

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: leaseUUID, Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}

	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create manager (shared between reconciler and simulated event handler)
	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, &chain.MockClient{})
	require.NoError(t, err)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, manager, nil)
	require.NoError(t, err)

	// Simulate concurrent provisioning attempts.
	// Capture ctx before spawning goroutines to avoid calling t.Context()
	// from a background goroutine, which can panic if the test exits early.
	const numGoroutines = 50
	ctx := t.Context()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to synchronize start
	start := make(chan struct{})

	// Half the goroutines simulate manager's TryTrackInFlight + Provision
	// Half simulate reconciler's startProvisioning (which also uses TryTrackInFlight)
	for i := range numGoroutines {
		go func(workerID int) {
			defer wg.Done()
			<-start

			// Simulate the atomic check-and-provision pattern used by both
			// manager.handleLeaseCreated and reconciler.startProvisioning
			if manager.TryTrackInFlight(leaseUUID, "tenant-1", testItems(""), "test") {
				// Only provision if we successfully tracked
				_ = mockBackend.Provision(ctx, backend.ProvisionRequest{
					LeaseUUID:    leaseUUID,
					Tenant:       "tenant-1",
					ProviderUUID: "provider-1",
					CallbackURL:  "http://localhost:8080/callbacks/provision",
				})
			}
		}(i)
	}

	// Start all goroutines simultaneously
	close(start)
	wg.Wait()

	// Verify exactly ONE provision call was made
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 1, provisionCount, "race condition detected!")

	// The lease should be tracked
	assert.True(t, manager.IsInFlight(leaseUUID))

	// Now test that reconciler.ReconcileAll also respects the in-flight tracking
	// Reset the mock to track new calls
	mockBackend.mu.Lock()
	mockBackend.provisionCalls = nil
	mockBackend.mu.Unlock()

	// Run reconciliation - should NOT provision again (already in-flight)
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))

	mockBackend.mu.Lock()
	additionalProvisions := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 0, additionalProvisions, "lease is in-flight")
}

func TestReconciler_ConcurrentReconciliation_NonBlocking(t *testing.T) {
	// Test that concurrent reconciliation attempts don't block - they skip instead.
	// This verifies the atomic flag approach works correctly.
	reconcileStarted := make(chan struct{})
	reconcileCanContinue := make(chan struct{})

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Signal that reconciliation has started
			close(reconcileStarted)
			// Wait for permission to continue (simulates long-running operation)
			<-reconcileCanContinue
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Start first reconciliation in background.
	// Capture ctx before the goroutine to avoid calling t.Context() from a
	// background goroutine, which can panic if the test exits early.
	ctx := t.Context()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- reconciler.ReconcileAll(ctx)
	}()

	// Wait for first reconciliation to start
	<-reconcileStarted

	// Try second reconciliation - should return immediately (non-blocking)
	secondStart := time.Now()
	err = reconciler.ReconcileAll(t.Context())
	secondDuration := time.Since(secondStart)

	// Second call should return quickly (not block waiting for first)
	assert.Less(t, secondDuration, 100*time.Millisecond)

	// No error expected - it just skips
	assert.NoError(t, err)

	// Let first reconciliation complete
	close(reconcileCanContinue)
	assert.NoError(t, <-firstDone)

	// After first completes, a new reconciliation should be allowed
	mockChain.GetPendingLeasesFunc = func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
		return nil, nil
	}
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))
}

func TestReconciler_ReconcileAll_ContextCancelledDuringLoop(t *testing.T) {
	// Test that context cancellation during the reconciliation loop is handled gracefully.
	// This tests the context checks we added within the loop iterations.
	var callCount int
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-3", Tenant: "tenant-3", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}

	// Use a backend that cancels context after first provision
	mockBackend := &mockCancellingBackend{
		name: "test",
		onProvision: func() {
			mu.Lock()
			callCount++
			currentCount := callCount
			mu.Unlock()
			if currentCount == 1 {
				cancel()
			}
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Use MaxWorkers=1 to ensure sequential processing for this cancellation test
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      1,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	assert.ErrorIs(t, err, context.Canceled)

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	// First provision triggers cancel, second should see cancelled context
	assert.Equal(t, 1, finalCount)
}

func TestReconciler_ReconcileAll_ContextCancelledDuringOrphanLoop(t *testing.T) {
	// Test that context cancellation during orphan cleanup loop is handled gracefully.
	ctx, cancel := context.WithCancel(context.Background())
	var deprovisionCount int
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		// No leases - all provisions are orphans
	}

	// Use a backend that cancels context after first deprovision
	mockBackend := &mockCancellingBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-3", Status: backend.ProvisionStatusReady},
		},
		onDeprovision: func() {
			mu.Lock()
			deprovisionCount++
			currentCount := deprovisionCount
			mu.Unlock()
			if currentCount == 1 {
				cancel()
			}
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Use MaxWorkers=1 to ensure sequential processing for this cancellation test
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      1,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	assert.ErrorIs(t, err, context.Canceled)

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := deprovisionCount
	mu.Unlock()

	// First deprovision triggers cancel, second should see cancelled context
	assert.Equal(t, 1, finalCount)
}

// mockCancellingBackend is a test backend with callback hooks for testing cancellation behavior.
type mockCancellingBackend struct {
	name          string
	provisions    []backend.ProvisionInfo
	onProvision   func()
	onDeprovision func()
}

func (m *mockCancellingBackend) Name() string {
	return m.name
}

func (m *mockCancellingBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.onProvision != nil {
		m.onProvision()
	}
	return nil
}

func (m *mockCancellingBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	return nil, nil
}

func (m *mockCancellingBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if m.onDeprovision != nil {
		m.onDeprovision()
	}
	return nil
}

func (m *mockCancellingBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	return m.provisions, nil
}

func (m *mockCancellingBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockCancellingBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockCancellingBackend) RefreshState(ctx context.Context) error {
	return nil
}

func (m *mockCancellingBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockCancellingBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockCancellingBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockCancellingBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockCancellingBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func TestReconciler_ReconcileAll_SKUBasedRouting(t *testing.T) {
	// Test that leases are routed to the correct backend based on SKU
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				// GPU lease should go to gpu-backend
				{
					Uuid:   "gpu-lease",
					Tenant: "tenant-1",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100-4x", Quantity: 1}},
				},
				// K8s lease should go to k8s-backend (default)
				{
					Uuid:   "k8s-lease",
					Tenant: "tenant-2",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "k8s-small", Quantity: 1}},
				},
				// Unknown SKU should go to default backend
				{
					Uuid:   "unknown-lease",
					Tenant: "tenant-3",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "unknown-sku", Quantity: 1}},
				},
			}, nil
		},
	}

	gpuBackend := &mockReconcilerBackend{name: "gpu-backend", provisions: []backend.ProvisionInfo{}}
	k8sBackend := &mockReconcilerBackend{name: "k8s-backend", provisions: []backend.ProvisionInfo{}}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100-4x"}}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify GPU lease went to GPU backend
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	assert.Len(t, gpuCalls, 1)
	if len(gpuCalls) > 0 {
		assert.Equal(t, "gpu-lease", gpuCalls[0].LeaseUUID)
		assert.Equal(t, "gpu-a100-4x", gpuCalls[0].RoutingSKU())
	}

	// Verify K8s and unknown leases went to K8s backend (default)
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	assert.Len(t, k8sCalls, 2)

	// Verify the SKUs are passed correctly
	skus := make(map[string]bool)
	for _, call := range k8sCalls {
		skus[call.RoutingSKU()] = true
	}
	assert.True(t, skus["k8s-small"])
	assert.True(t, skus["unknown-sku"])
}

func TestReconciler_MaxWorkers_Default(t *testing.T) {
	// Test that default MaxWorkers is applied
	mockChain := &chain.MockClient{}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		// MaxWorkers not set - should use default
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, DefaultReconcileWorkers, reconciler.maxWorkers)
}

func TestReconciler_MaxWorkers_Custom(t *testing.T) {
	// Test that custom MaxWorkers is respected
	mockChain := &chain.MockClient{}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      5,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 5, reconciler.maxWorkers)
}

func TestReconciler_ParallelProcessing(t *testing.T) {
	// Test that leases are processed in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		totalProcessed int
	)

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Create enough leases to test parallelism
			leases := make([]billingtypes.Lease, 20)
			for i := range leases {
				leases[i] = billingtypes.Lease{
					Uuid:   "lease-" + string(rune('a'+i)),
					Tenant: "tenant",
					State:  billingtypes.LEASE_STATE_PENDING,
				}
			}
			return leases, nil
		},
	}

	// Backend that tracks concurrent workers
	mockBackend := &mockConcurrencyBackend{
		name: "test",
		onProvision: func() {
			mu.Lock()
			currentWorkers++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			totalProcessed++
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      5, // Limit to 5 concurrent workers
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all leases were processed
	assert.Equal(t, 20, totalProcessed)

	// Verify parallel processing occurred (more than 1 concurrent worker)
	assert.GreaterOrEqual(t, concurrentMax, 2)

	// Verify MaxWorkers limit was respected
	assert.LessOrEqual(t, concurrentMax, 5)
}

func TestReconciler_ParallelOrphanProcessing(t *testing.T) {
	// Test that orphans are processed in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		totalProcessed int
	)

	mockChain := &chain.MockClient{
		// No leases - all provisions are orphans
	}

	// Create many orphan provisions
	provisions := make([]backend.ProvisionInfo, 15)
	for i := range provisions {
		provisions[i] = backend.ProvisionInfo{
			LeaseUUID: "orphan-" + string(rune('a'+i)),
			Status:    backend.ProvisionStatusReady,
		}
	}

	mockBackend := &mockConcurrencyBackend{
		name:       "test",
		provisions: provisions,
		onDeprovision: func() {
			mu.Lock()
			currentWorkers++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			totalProcessed++
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      4, // Limit to 4 concurrent workers
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all orphans were processed
	assert.Equal(t, 15, totalProcessed)

	// Verify parallel processing occurred
	assert.GreaterOrEqual(t, concurrentMax, 2)

	// Verify MaxWorkers limit was respected
	assert.LessOrEqual(t, concurrentMax, 4)
}

func TestReconciler_ParallelBackendFetching(t *testing.T) {
	// Test that backend provisions are fetched in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		fetchCount     int
	)

	mockChain := &chain.MockClient{}

	// Create multiple backends that track concurrent fetches
	backend1 := &mockConcurrencyBackend{
		name: "backend-1",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate network latency
			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	backend2 := &mockConcurrencyBackend{
		name: "backend-2",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	backend3 := &mockConcurrencyBackend{
		name: "backend-3",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100"}}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"vm-basic"}}},
			{Backend: backend3, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all backends were fetched
	assert.Equal(t, 3, fetchCount)

	// Verify parallel fetching occurred (all 3 should run concurrently)
	assert.GreaterOrEqual(t, concurrentMax, 2)
}

// mockConcurrencyBackend is a test backend for verifying parallel execution.
type mockConcurrencyBackend struct {
	name             string
	provisions       []backend.ProvisionInfo
	onProvision      func()
	onDeprovision    func()
	onListProvisions func()
}

func (m *mockConcurrencyBackend) Name() string {
	return m.name
}

func (m *mockConcurrencyBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.onProvision != nil {
		m.onProvision()
	}
	return nil
}

func (m *mockConcurrencyBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	return nil, nil
}

func (m *mockConcurrencyBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if m.onDeprovision != nil {
		m.onDeprovision()
	}
	return nil
}

func (m *mockConcurrencyBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	if m.onListProvisions != nil {
		m.onListProvisions()
	}
	return m.provisions, nil
}

func (m *mockConcurrencyBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockConcurrencyBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockConcurrencyBackend) RefreshState(ctx context.Context) error {
	return nil
}

func (m *mockConcurrencyBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockConcurrencyBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockConcurrencyBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockConcurrencyBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockConcurrencyBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

// mockInFlightTracker implements ReconcilerTracker for testing orphaned payload cleanup.
type mockInFlightTracker struct {
	payloadStore   *payload.Store
	inFlight       map[string]InFlightProvision
	mu             sync.Mutex
	hasPayloadErr  error
	hasPayloadFunc func(leaseUUID string) (bool, error) // optional override
}

func newMockInFlightTracker(payloadStore *payload.Store) *mockInFlightTracker {
	return &mockInFlightTracker{
		payloadStore: payloadStore,
		inFlight:     make(map[string]InFlightProvision),
	}
}

func (m *mockInFlightTracker) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		return false
	}
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
	}
	return true
}

func (m *mockInFlightTracker) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
	}
}

func (m *mockInFlightTracker) UntrackInFlight(leaseUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inFlight, leaseUUID)
}

func (m *mockInFlightTracker) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if exists {
		delete(m.inFlight, leaseUUID)
	}
	return p, exists
}

func (m *mockInFlightTracker) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	return p, exists
}

func (m *mockInFlightTracker) IsInFlight(leaseUUID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.inFlight[leaseUUID]
	return exists
}

func (m *mockInFlightTracker) InFlightCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inFlight)
}

func (m *mockInFlightTracker) GetInFlightLeases() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	leases := make([]string, 0, len(m.inFlight))
	for k := range m.inFlight {
		leases = append(leases, k)
	}
	return leases
}

func (m *mockInFlightTracker) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	return m.InFlightCount()
}

func (m *mockInFlightTracker) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	return nil
}

func (m *mockInFlightTracker) HasPayload(leaseUUID string) (bool, error) {
	if m.hasPayloadErr != nil {
		return false, m.hasPayloadErr
	}
	if m.hasPayloadFunc != nil {
		return m.hasPayloadFunc(leaseUUID)
	}
	if m.payloadStore == nil {
		return false, nil
	}
	return m.payloadStore.Has(leaseUUID)
}

func (m *mockInFlightTracker) PayloadStore() *payload.Store {
	return m.payloadStore
}

func TestReconciler_CleansUpOrphanedPayloads(t *testing.T) {
	// Create a temp dir for the payload store
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	// Store payloads for various leases
	// pending-awaiting: pending lease with MetaHash but hasn't uploaded payload yet - simulates
	// a lease that's still waiting for payload (payload won't be in store, so nothing to clean)
	payloadStore.Store("closed-lease", []byte("closed payload"))      // Will be cleaned (lease is closed/not found)
	payloadStore.Store("nonexistent-lease", []byte("orphan payload")) // Will be cleaned (lease doesn't exist)
	payloadStore.Store("active-lease", []byte("active payload"))      // Retained for re-provisioning

	// Verify all payloads are stored
	require.Equal(t, 3, payloadStore.Count())

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				// Pending lease without payload (no MetaHash) - will be provisioned without payload
				{Uuid: "pending-no-payload", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "active-lease", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_ACTIVE},
				// Note: "closed-lease" and "nonexistent-lease" are not returned (not pending or active)
			}, nil
		},
	}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	require.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify orphaned payloads were cleaned up
	// active-lease: payload should be RETAINED (active leases keep payload for re-provisioning)
	hasActive, err := payloadStore.Has("active-lease")
	require.NoError(t, err)
	assert.True(t, hasActive, "expected active-lease payload to be retained for re-provisioning")

	// closed-lease: payload should be cleaned (lease doesn't exist in chain query results)
	hasClosed, err := payloadStore.Has("closed-lease")
	require.NoError(t, err)
	assert.False(t, hasClosed, "expected closed-lease payload to be cleaned up (lease not found)")

	// nonexistent-lease: payload should be cleaned (lease doesn't exist)
	hasNonexistent, err := payloadStore.Has("nonexistent-lease")
	require.NoError(t, err)
	assert.False(t, hasNonexistent, "expected nonexistent-lease payload to be cleaned up (lease not found)")

	// Verify count - only active-lease payload should remain
	assert.Equal(t, 1, payloadStore.Count())
}

// TestReconciler_ConcurrentReconcileAll tests that concurrent ReconcileAll calls
// are properly serialized by the atomic flag.
func TestReconciler_ReconcileAll_ActiveFailedExhausted(t *testing.T) {
	// Setup: Active lease on chain, failed provision with FailCount >= maxReprovisionAttempts
	// Expected: Close the lease and deprovision the backend resources
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   3, // >= DefaultMaxReprovisionAttempts (3)
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (not rejected — it's ACTIVE, not PENDING)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Contains(t, closedReason, "failed 3 times")

	// Verify backend resources were released immediately
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.deprovisionCalls[0])

	// Verify NO re-provisioning was attempted
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_ActiveFailedBelowMax(t *testing.T) {
	// Setup: Active lease on chain, failed provision with FailCount < maxReprovisionAttempts
	// Expected: Attempt re-provisioning (not close)
	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // < DefaultMaxReprovisionAttempts (3)
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify re-provisioning was attempted (not closed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)

	// No deprovisions (that happens after close, not re-provision)
	assert.Empty(t, mockBackend.deprovisionCalls)
}

func TestReconciler_ConcurrentReconcileAll(t *testing.T) {
	// Create mock chain client that returns one pending lease.
	// Add a delay to ensure reconciliation takes some time, which allows us to
	// verify that concurrent calls are properly serialized by the atomic flag.
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Small delay to ensure concurrent ReconcileAll calls overlap.
			// Without this, the first call would complete before others even start,
			// making them sequential rather than concurrent.
			time.Sleep(10 * time.Millisecond)
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}

	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080/callbacks",
		MaxWorkers:      2,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Start multiple concurrent ReconcileAll calls.
	// Capture ctx before spawning goroutines to avoid calling t.Context()
	// from a background goroutine, which can panic if the test exits early.
	const numGoroutines = 10
	ctx := t.Context()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := make(chan struct{})
	completed := make(chan struct{}, numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			<-start

			_ = reconciler.ReconcileAll(ctx)
			completed <- struct{}{}
		}()
	}

	// Start all goroutines simultaneously
	close(start)
	wg.Wait()
	close(completed)

	// Count how many completed
	var completedCount int
	for range completed {
		completedCount++
	}

	// All should complete (either by running or skipping)
	assert.Equal(t, numGoroutines, completedCount)

	// At most ONE provision call should have been made
	// (the atomic flag prevents concurrent reconciliation)
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.LessOrEqual(t, provisionCount, 1, "concurrent reconciliation not prevented!")
}

func TestReconciler_ReconcileAll_PendingValidationError_Rejects(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned, backend returns ErrValidation
	// Expected: Reject the lease immediately (not retry forever)
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: bad-sku", backend.ErrUnknownSKU),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was rejected (not left to retry forever)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, rejectReasonInvalidSKU, rejectedReason)
}

func TestReconciler_ReconcileAll_PendingCircuitOpen_Rejects(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned, backend returns ErrCircuitOpen
	// Expected: Reject the lease immediately (not left pending forever)
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: backend.ErrCircuitOpen,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was rejected (not left pending forever)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, "backend unavailable", rejectedReason)
}

func TestReconciler_ReconcileAll_PendingWithPayloadValidationError_Rejects(t *testing.T) {
	// Setup: Pending lease with MetaHash, payload available, backend returns ErrValidation
	// Expected: Reject the lease immediately
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	payloadData := []byte("test payload")
	payloadHash := sha256.Sum256(payloadData)
	payloadStore.Store("lease-1", payloadData)

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:], // SHA-256 of payloadData
				},
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
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: bad yaml", backend.ErrInvalidManifest),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, rejectReasonInvalidManifest, rejectedReason)
}

func TestReconciler_ReconcileAll_ActiveNotProvisionedValidationError_Closes(t *testing.T) {
	// Setup: Active lease on chain, not provisioned (anomaly), backend returns ErrValidation
	// Expected: Close the lease (not reject — it's ACTIVE)
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: registry %q", backend.ErrImageNotAllowed, "evil.io"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (not rejected — it's ACTIVE)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Equal(t, rejectReasonImageNotAllowed, closedReason)
}

func TestReconciler_ReconcileAll_ActiveFailedValidationError_Closes(t *testing.T) {
	// Setup: Active lease, failed provision with FailCount < max, re-provision returns ErrValidation
	// Expected: Close the lease immediately (not keep retrying)
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // Below max — would normally re-provision
				BackendName: "test",
			},
		},
		provisionErr: fmt.Errorf("%w: removed-sku", backend.ErrUnknownSKU),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (validation error is permanent)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Equal(t, rejectReasonInvalidSKU, closedReason)
}

// --- Placement store integration tests ---

func TestReconciler_ReconcileAll_SyncsPlacementsFromBackends(t *testing.T) {
	// Setup: Two backends each with provisions. Placement store should be
	// synced with SetBatch from actual backend state.
	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}

	b1 := &mockReconcilerBackend{
		name: "backend-1",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	b2 := &mockReconcilerBackend{
		name: "backend-2",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusReady},
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: b1, IsDefault: true},
			{Backend: b2, Match: backend.MatchCriteria{SKUs: []string{"b2-sku"}}},
		},
	})

	ps := &mockPlacementStore{}

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify placements were synced from backend state
	assert.Equal(t, "backend-1", ps.Get("lease-1"))
	assert.Equal(t, "backend-2", ps.Get("lease-2"))
	assert.Equal(t, 2, ps.Count())
}

func TestReconciler_ReconcileAll_StartProvisioning_RecordsPlacement(t *testing.T) {
	// Setup: Pending lease, not provisioned. After provisioning, placement should be recorded.
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify placement was recorded after provisioning
	assert.Equal(t, "test", ps.Get("lease-1"))
}

func TestReconciler_ReconcileAll_RejectLease_CleansUpPlacement(t *testing.T) {
	// Setup: Pending lease with failed provision. After rejection, placement should be deleted.
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	assert.Empty(t, ps.Get("lease-1"), "placement should be cleaned up after rejection")
}

func TestReconciler_ReconcileAll_OrphanDeprovision_CleansUpPlacement(t *testing.T) {
	// Setup: No lease on chain, provisioned on backend (orphan). Placement should be cleaned up.
	mockChain := &chain.MockClient{}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("orphan-1", "test")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	assert.Empty(t, ps.Get("orphan-1"), "placement should be cleaned up after orphan deprovision")
}

func TestReconciler_ReconcileAll_CloseLease_CleansUpPlacement(t *testing.T) {
	// Setup: Active lease, failed provision exhausted retries. After close, placement cleaned up.
	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   3,
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	assert.Empty(t, ps.Get("lease-1"), "placement should be cleaned up after lease close")
}

// --- Gap tests: fetchAllProvisions, payload lifecycle, RefreshState ---

func TestReconciler_ReconcileAll_ListProvisionError_AbortsReconciliation(t *testing.T) {
	// When ListProvisions fails on any backend, ReconcileAll should abort entirely.
	// Partial data would cause the reconciler to misidentify running containers
	// as orphans and deprovision them.
	mockChain := &chain.MockClient{
		// Return an active lease so that a false orphan would be detected
		// if we proceeded with partial data.
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:    "test",
		listErr: errors.New("docker daemon unavailable"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	err = reconciler.ReconcileAll(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete backend data")

	// Verify no provisioning or deprovisioning occurred
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
	assert.Empty(t, mockBackend.deprovisionCalls)
}

func TestReconciler_ReconcileAll_ActiveFailedPayloadNotAvailable_Closes(t *testing.T) {
	// ACTIVE lease with failed provision (FailCount < max). Re-provisioning
	// requires a payload (MetaHash is set), but the payload store is empty.
	// errPayloadNotAvailable is a permanent failure — the lease should be closed.
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("some manifest"))

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // Below max — would normally re-provision
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Empty payload store — no payload available for re-provisioning
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify the lease was closed (permanent failure, not transient)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Contains(t, closedReason, "payload not available")

	// Verify no provisioning was attempted (error happened before Provision call)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_PendingWithMetaHash_NoPayload_Waits(t *testing.T) {
	// PENDING lease with MetaHash but the payload hasn't been uploaded yet.
	// The reconciler should do nothing — no provisioning, no rejection.
	// It just waits for the tenant to upload the payload.
	var rejectedLeases []string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("future manifest"))

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, errors.New("should not be called")
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Empty payload store — payload not yet uploaded
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no actions were taken — waiting for payload upload
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "should not provision without payload")
	assert.Empty(t, mockBackend.deprovisionCalls, "should not deprovision")

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, rejectedLeases, "should not reject — lease is just waiting for payload")
}

func TestReconciler_ReconcileAll_PayloadHashMismatch_DeletesCorruptPayload(t *testing.T) {
	// Payload is in the store but its hash doesn't match the lease's MetaHash.
	// This simulates disk corruption. The reconciler should:
	// 1. Delete the corrupt payload from the store
	// 2. NOT call Provision (hash check happens before)
	// 3. Treat it as an error (transient — payload needs re-upload)
	payloadData := []byte("original manifest payload")
	wrongHash := sha256.Sum256([]byte("different payload entirely"))

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: wrongHash[:], // Doesn't match payloadData
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	// Store payload that will fail hash verification
	payloadStore.Store("lease-1", payloadData)
	has, err := payloadStore.Has("lease-1")
	require.NoError(t, err)
	require.True(t, has)

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify corrupt payload was deleted from store
	has, err = payloadStore.Has("lease-1")
	require.NoError(t, err)
	assert.False(t, has, "corrupt payload should be deleted from store")

	// Verify no provisioning was attempted (hash check happens before Provision)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_PayloadStoreGetError_TransientError(t *testing.T) {
	// When PayloadStore.Get() returns a database error (not nil payload),
	// the reconciler should treat it as a transient error — NOT close the lease.
	// This prevents a disk hiccup from permanently terminating active leases.
	//
	// Uses ACTIVE + not provisioned (anomaly) to bypass HasPayload and go
	// directly to startProvisioningWithPayload → doStartProvisioning → Get().
	var closedLeases []string
	var mu sync.Mutex

	payloadData := []byte("test manifest")
	payloadHash := sha256.Sum256(payloadData)

	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned (anomaly path)
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create payload store, store data, then close it to force Get() errors
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	payloadStore.Store("lease-1", payloadData)
	require.NoError(t, payloadStore.Close()) // Force "database not open" on Get()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was NOT closed (transient error, should retry next cycle)
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, closedLeases, "DB error is transient — lease should NOT be closed")

	// Verify no provisioning was attempted (error aborted before Provision call)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_RefreshStateError_ContinuesWithStaleData(t *testing.T) {
	// When RefreshState returns an error, reconciliation should continue
	// using stale data rather than aborting. Stale state may be slightly
	// out of date but is far better than no reconciliation at all.
	mockChain := &chain.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
		refreshErr: errors.New("docker daemon busy"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err, "should succeed despite RefreshState error")

	// Verify no unnecessary actions (ACTIVE + Ready → healthy with stale data)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
	assert.Empty(t, mockBackend.deprovisionCalls)
}

func TestReconciler_ReconcileAll_HasPayloadError_CountsAsError(t *testing.T) {
	// When tracker.HasPayload() returns a database error, the reconciler
	// should not attempt provisioning and should count it as a lease error.
	// It should NOT reject the lease (the error is transient, not permanent).
	var rejectedLeases []string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("some manifest"))

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil) // no payload store needed
	tracker.hasPayloadErr = errors.New("disk I/O error")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "should not provision when HasPayload errors")

	// Verify lease was NOT rejected (transient error, not permanent)
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, rejectedLeases, "should not reject — HasPayload error is transient")
}

func TestReconciler_ReconcileAll_SetsLastSuccessTimestamp(t *testing.T) {
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)

	err = reconciler.ReconcileAll(t.Context())
	require.NoError(t, err)

	after := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)
	assert.Greater(t, after, before, "ReconcilerLastSuccessTimestamp should be updated after reconciliation")
	assert.Greater(t, after, float64(0), "ReconcilerLastSuccessTimestamp should be a positive unix timestamp")
}

func TestReconciler_InsufficientResources_IncrementsMetric(t *testing.T) {
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-cap", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}}},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "cap-backend",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("no room: %w", backend.ErrInsufficientResources),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil)
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.BackendInsufficientResourcesTotal.WithLabelValues("cap-backend"))

	_ = reconciler.ReconcileAll(t.Context())

	after := promtestutil.ToFloat64(metrics.BackendInsufficientResourcesTotal.WithLabelValues("cap-backend"))
	assert.Equal(t, 1.0, after-before, "BackendInsufficientResourcesTotal should increment by 1 for reconciler path")
}

func TestReconciler_ReconcileAll_PartialFailureDoesNotUpdateTimestamp(t *testing.T) {
	// When a lease errors during provisioning the outcome is "partial",
	// and ReconcilerLastSuccessTimestamp must NOT be updated.
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-fail", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}}},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: errors.New("backend exploded"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil)
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)

	_ = reconciler.ReconcileAll(t.Context())

	after := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)
	assert.Equal(t, before, after, "ReconcilerLastSuccessTimestamp should NOT be updated on partial failure")
}
