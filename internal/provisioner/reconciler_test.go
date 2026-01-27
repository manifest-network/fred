package provisioner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

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
	info := backend.LeaseInfo{"host": "localhost", "port": 8080}
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

func (m *mockReconcilerBackend) Health(ctx context.Context) error {
	return nil
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
		router      *backend.Router
		wantErr     string
	}{
		{
			name:        "missing chain client",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: nil,
			router:      router,
			wantErr:     "chain client is required",
		},
		{
			name:        "missing router",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			router:      nil,
			wantErr:     "backend router is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ReconcilerConfig{CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			router:      router,
			wantErr:     "provider UUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid"},
			chainClient: mockChain,
			router:      router,
			wantErr:     "callback base URL is required",
		},
		{
			name:        "valid config",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			router:      router,
			wantErr:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReconciler(tt.cfg, tt.chainClient, tt.router, nil)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("NewReconciler() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("NewReconciler() error = nil, want error containing %q", tt.wantErr)
				} else if err.Error() != tt.wantErr {
					t.Errorf("NewReconciler() error = %q, want %q", err.Error(), tt.wantErr)
				}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Errorf("expected 1 provision call, got %d", len(mockBackend.provisionCalls))
	}
	if mockBackend.provisionCalls[0].LeaseUUID != "lease-1" {
		t.Errorf("expected lease-1, got %s", mockBackend.provisionCalls[0].LeaseUUID)
	}
}

func TestReconciler_ReconcileAll_PendingProvisionedReady(t *testing.T) {
	// Setup: Pending lease on chain, provisioned and ready on backend
	// Expected: Acknowledge the lease
	var acknowledgedLeases []string
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
			acknowledgedLeases = append(acknowledgedLeases, leaseUUIDs...)
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify lease was acknowledged
	mu.Lock()
	defer mu.Unlock()
	if len(acknowledgedLeases) != 1 {
		t.Errorf("expected 1 acknowledged lease, got %d", len(acknowledgedLeases))
	}
	if acknowledgedLeases[0] != "lease-1" {
		t.Errorf("expected lease-1, got %s", acknowledgedLeases[0])
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify provisioning was attempted (anomaly recovery)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Errorf("expected 1 provision call (anomaly recovery), got %d", len(mockBackend.provisionCalls))
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify nothing was done
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	if provisionCount != 0 {
		t.Errorf("expected 0 provision calls, got %d", provisionCount)
	}
	if deprovisionCount != 0 {
		t.Errorf("expected 0 deprovision calls, got %d", deprovisionCount)
	}
	if ackCount != 0 {
		t.Errorf("expected 0 acknowledge calls, got %d", ackCount)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify orphan was deprovisioned
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.deprovisionCalls) != 1 {
		t.Errorf("expected 1 deprovision call, got %d", len(mockBackend.deprovisionCalls))
	}
	if mockBackend.deprovisionCalls[0] != "orphan-lease" {
		t.Errorf("expected orphan-lease, got %s", mockBackend.deprovisionCalls[0])
	}
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
			}, mockChain, router, nil)
			if err != nil {
				t.Fatalf("NewReconciler() error = %v", err)
			}

			ctx := context.Background()
			err = reconciler.ReconcileAll(ctx)
			if err == nil {
				t.Errorf("ReconcileAll() error = nil, want error containing %q", tt.wantErr)
			} else if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ReconcileAll() error = %q, want error containing %q", err.Error(), tt.wantErr)
			}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	// Cancel context before calling ReconcileAll
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = reconciler.ReconcileAll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReconcileAll() error = %v, want context.Canceled", err)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

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
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Start() error = %v, want context.Canceled", err)
		}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	// Verify default interval is 5 minutes
	if reconciler.interval != 5*time.Minute {
		t.Errorf("default interval = %v, want %v", reconciler.interval, 5*time.Minute)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.RunOnce(ctx); err != nil {
		t.Errorf("RunOnce() error = %v", err)
	}

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Errorf("expected 1 provision call after RunOnce, got %d", len(mockBackend.provisionCalls))
	}
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
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, router, manager)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify provisioning was NOT started (lease is in-flight)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls (lease in-flight), got %d", len(mockBackend.provisionCalls))
	}
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
			{Backend: backend2, Match: backend.MatchCriteria{SKUPrefix: "b2-"}},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Both orphans should be deprovisioned
	backend1.mu.Lock()
	b1Calls := len(backend1.deprovisionCalls)
	backend1.mu.Unlock()

	backend2.mu.Lock()
	b2Calls := len(backend2.deprovisionCalls)
	backend2.mu.Unlock()

	// Total deprovisions should be 2
	if b1Calls+b2Calls != 2 {
		t.Errorf("expected 2 total deprovision calls, got %d", b1Calls+b2Calls)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify no actions were taken
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	if provisionCount != 0 {
		t.Errorf("expected 0 provision calls (already provisioning), got %d", provisionCount)
	}
	if deprovisionCount != 0 {
		t.Errorf("expected 0 deprovision calls, got %d", deprovisionCount)
	}
	if ackCount != 0 {
		t.Errorf("expected 0 acknowledge calls (not ready yet), got %d", ackCount)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify no provisioning or deprovisioning
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	if provisionCount != 0 {
		t.Errorf("expected 0 provision calls, got %d", provisionCount)
	}
	if deprovisionCount != 0 {
		t.Errorf("expected 0 deprovision calls, got %d", deprovisionCount)
	}

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	if len(rejectedLeases) != 1 {
		t.Errorf("expected 1 rejected lease, got %d", len(rejectedLeases))
	}
	if len(rejectedLeases) > 0 && rejectedLeases[0] != "lease-1" {
		t.Errorf("expected lease-1 to be rejected, got %s", rejectedLeases[0])
	}
	if rejectedReason != "provisioning failed" {
		t.Errorf("expected rejection reason 'provisioning failed', got %q", rejectedReason)
	}
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
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, acknowledgeErr
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	// ReconcileAll should succeed even if individual acknowledges fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	if err != nil {
		t.Errorf("ReconcileAll() error = %v, want nil (acknowledge errors should be logged, not returned)", err)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	// ReconcileAll should succeed even if deprovisions fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	if err != nil {
		t.Errorf("ReconcileAll() error = %v, want nil (deprovision errors should be logged, not returned)", err)
	}

	// Verify both deprovisions were attempted
	mockBackend.mu.Lock()
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	if deprovisionCount != 2 {
		t.Errorf("expected 2 deprovision attempts (even with errors), got %d", deprovisionCount)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify only our orphans were deprovisioned (not the other provider's)
	mockBackend.mu.Lock()
	deprovisionCalls := mockBackend.deprovisionCalls
	mockBackend.mu.Unlock()

	// Should have deprovisioned 2 leases: our-orphan-lease and legacy-orphan-lease
	// Should NOT have deprovisioned other-provider-lease
	if len(deprovisionCalls) != 2 {
		t.Errorf("expected 2 deprovision calls, got %d: %v", len(deprovisionCalls), deprovisionCalls)
	}

	// Verify the other provider's lease was NOT deprovisioned
	for _, call := range deprovisionCalls {
		if call == "other-provider-lease" {
			t.Error("should NOT have deprovisioned lease belonging to other provider")
		}
	}
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
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, router, manager)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	// Simulate concurrent provisioning attempts
	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to synchronize start
	start := make(chan struct{})

	// Half the goroutines simulate manager's TryTrackInFlight + Provision
	// Half simulate reconciler's startProvisioning (which also uses TryTrackInFlight)
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			<-start

			// Simulate the atomic check-and-provision pattern used by both
			// manager.handleLeaseCreated and reconciler.startProvisioning
			if manager.TryTrackInFlight(leaseUUID, "tenant-1", "", "test") {
				// Only provision if we successfully tracked
				_ = mockBackend.Provision(context.Background(), backend.ProvisionRequest{
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

	if provisionCount != 1 {
		t.Errorf("expected exactly 1 provision call, got %d (race condition detected!)", provisionCount)
	}

	// The lease should be tracked
	if !manager.IsInFlight(leaseUUID) {
		t.Error("IsInFlight() = false after concurrent provisioning, want true")
	}

	// Now test that reconciler.ReconcileAll also respects the in-flight tracking
	// Reset the mock to track new calls
	mockBackend.mu.Lock()
	mockBackend.provisionCalls = nil
	mockBackend.mu.Unlock()

	// Run reconciliation - should NOT provision again (already in-flight)
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	mockBackend.mu.Lock()
	additionalProvisions := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	if additionalProvisions != 0 {
		t.Errorf("ReconcileAll() made %d additional provision calls, want 0 (lease is in-flight)", additionalProvisions)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	// Start first reconciliation in background
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- reconciler.ReconcileAll(context.Background())
	}()

	// Wait for first reconciliation to start
	<-reconcileStarted

	// Try second reconciliation - should return immediately (non-blocking)
	secondStart := time.Now()
	err = reconciler.ReconcileAll(context.Background())
	secondDuration := time.Since(secondStart)

	// Second call should return quickly (not block waiting for first)
	if secondDuration > 100*time.Millisecond {
		t.Errorf("concurrent ReconcileAll() took %v, expected to return immediately", secondDuration)
	}

	// No error expected - it just skips
	if err != nil {
		t.Errorf("concurrent ReconcileAll() error = %v, want nil", err)
	}

	// Let first reconciliation complete
	close(reconcileCanContinue)
	if err := <-firstDone; err != nil {
		t.Errorf("first ReconcileAll() error = %v", err)
	}

	// After first completes, a new reconciliation should be allowed
	mockChain.GetPendingLeasesFunc = func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
		return nil, nil
	}
	if err := reconciler.ReconcileAll(context.Background()); err != nil {
		t.Errorf("subsequent ReconcileAll() error = %v", err)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReconcileAll() error = %v, want context.Canceled", err)
	}

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	// First provision triggers cancel, second should see cancelled context
	if finalCount != 1 {
		t.Errorf("expected exactly 1 provision call before cancellation, got %d", finalCount)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReconcileAll() error = %v, want context.Canceled", err)
	}

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := deprovisionCount
	mu.Unlock()

	// First deprovision triggers cancel, second should see cancelled context
	if finalCount != 1 {
		t.Errorf("expected exactly 1 deprovision call before cancellation, got %d", finalCount)
	}
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

func (m *mockCancellingBackend) Health(ctx context.Context) error {
	return nil
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
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	// Verify GPU lease went to GPU backend
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	if len(gpuCalls) != 1 {
		t.Errorf("expected 1 provision call to GPU backend, got %d", len(gpuCalls))
	}
	if len(gpuCalls) > 0 && gpuCalls[0].LeaseUUID != "gpu-lease" {
		t.Errorf("expected gpu-lease, got %s", gpuCalls[0].LeaseUUID)
	}
	if len(gpuCalls) > 0 && gpuCalls[0].SKU != "gpu-a100-4x" {
		t.Errorf("expected SKU gpu-a100-4x, got %s", gpuCalls[0].SKU)
	}

	// Verify K8s and unknown leases went to K8s backend (default)
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	if len(k8sCalls) != 2 {
		t.Errorf("expected 2 provision calls to K8s backend (k8s + unknown), got %d", len(k8sCalls))
	}

	// Verify the SKUs are passed correctly
	skus := make(map[string]bool)
	for _, call := range k8sCalls {
		skus[call.SKU] = true
	}
	if !skus["k8s-small"] {
		t.Error("expected k8s-small SKU in K8s backend calls")
	}
	if !skus["unknown-sku"] {
		t.Error("expected unknown-sku in K8s backend calls (routed to default)")
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	if reconciler.maxWorkers != DefaultReconcileWorkers {
		t.Errorf("maxWorkers = %d, want %d (default)", reconciler.maxWorkers, DefaultReconcileWorkers)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	if reconciler.maxWorkers != 5 {
		t.Errorf("maxWorkers = %d, want 5", reconciler.maxWorkers)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify all leases were processed
	if totalProcessed != 20 {
		t.Errorf("totalProcessed = %d, want 20", totalProcessed)
	}

	// Verify parallel processing occurred (more than 1 concurrent worker)
	if concurrentMax < 2 {
		t.Errorf("concurrentMax = %d, expected parallel processing (> 1)", concurrentMax)
	}

	// Verify MaxWorkers limit was respected
	if concurrentMax > 5 {
		t.Errorf("concurrentMax = %d, exceeded MaxWorkers limit of 5", concurrentMax)
	}
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
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify all orphans were processed
	if totalProcessed != 15 {
		t.Errorf("totalProcessed = %d, want 15", totalProcessed)
	}

	// Verify parallel processing occurred
	if concurrentMax < 2 {
		t.Errorf("concurrentMax = %d, expected parallel processing (> 1)", concurrentMax)
	}

	// Verify MaxWorkers limit was respected
	if concurrentMax > 4 {
		t.Errorf("concurrentMax = %d, exceeded MaxWorkers limit of 4", concurrentMax)
	}
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
			{Backend: backend1, Match: backend.MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUPrefix: "vm-"}},
			{Backend: backend3, Match: backend.MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})

	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, router, nil)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Errorf("ReconcileAll() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify all backends were fetched
	if fetchCount != 3 {
		t.Errorf("fetchCount = %d, want 3", fetchCount)
	}

	// Verify parallel fetching occurred (all 3 should run concurrently)
	if concurrentMax < 2 {
		t.Errorf("concurrentMax = %d, expected parallel backend fetching (> 1)", concurrentMax)
	}
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

func (m *mockConcurrencyBackend) Health(ctx context.Context) error {
	return nil
}

// mockInFlightTracker implements InFlightTracker for testing orphaned payload cleanup.
type mockInFlightTracker struct {
	payloadStore *PayloadStore
	inFlight     map[string]bool
	mu           sync.Mutex
}

func newMockInFlightTracker(payloadStore *PayloadStore) *mockInFlightTracker {
	return &mockInFlightTracker{
		payloadStore: payloadStore,
		inFlight:     make(map[string]bool),
	}
}

func (m *mockInFlightTracker) TryTrackInFlight(leaseUUID, tenant, sku, backendName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inFlight[leaseUUID] {
		return false
	}
	m.inFlight[leaseUUID] = true
	return true
}

func (m *mockInFlightTracker) UntrackInFlight(leaseUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inFlight, leaseUUID)
}

func (m *mockInFlightTracker) HasPayload(leaseUUID string) bool {
	if m.payloadStore == nil {
		return false
	}
	return m.payloadStore.Has(leaseUUID)
}

func (m *mockInFlightTracker) PayloadStore() *PayloadStore {
	return m.payloadStore
}

func TestReconciler_CleansUpOrphanedPayloads(t *testing.T) {
	// Create a temp dir for the payload store
	tmpDir := t.TempDir()
	payloadStore, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer payloadStore.Close()

	// Store payloads for various leases
	// pending-awaiting: pending lease with MetaHash but hasn't uploaded payload yet - simulates
	// a lease that's still waiting for payload (payload won't be in store, so nothing to clean)
	payloadStore.Store("closed-lease", []byte("closed payload"))        // Will be cleaned (lease is closed)
	payloadStore.Store("nonexistent-lease", []byte("orphan payload"))   // Will be cleaned (lease doesn't exist)
	payloadStore.Store("active-lease", []byte("active payload"))        // Will be cleaned (lease is active, not pending)

	// Verify all payloads are stored
	if count := payloadStore.Count(); count != 3 {
		t.Fatalf("expected 3 payloads stored, got %d", count)
	}

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
	}, mockChain, router, mockTracker)
	if err != nil {
		t.Fatalf("NewReconciler() error = %v", err)
	}

	ctx := context.Background()
	if err := reconciler.ReconcileAll(ctx); err != nil {
		t.Fatalf("ReconcileAll() error = %v", err)
	}

	// Verify orphaned payloads were cleaned up
	// active-lease: payload should be cleaned (lease is active, not pending)
	if payloadStore.Has("active-lease") {
		t.Error("expected active-lease payload to be cleaned up (lease is no longer pending)")
	}

	// closed-lease: payload should be cleaned (lease doesn't exist in chain query results)
	if payloadStore.Has("closed-lease") {
		t.Error("expected closed-lease payload to be cleaned up (lease not found)")
	}

	// nonexistent-lease: payload should be cleaned (lease doesn't exist)
	if payloadStore.Has("nonexistent-lease") {
		t.Error("expected nonexistent-lease payload to be cleaned up (lease not found)")
	}

	// Verify count - all orphaned payloads should be cleaned
	if count := payloadStore.Count(); count != 0 {
		t.Errorf("expected 0 payloads remaining (all orphans cleaned), got %d", count)
	}
}
