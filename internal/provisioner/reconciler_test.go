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
)

// mockReconcilerChainClient implements ReconcilerChainClient for testing.
type mockReconcilerChainClient struct {
	mu                  sync.Mutex
	pendingLeases       []billingtypes.Lease
	activeLeases        []billingtypes.Lease
	acknowledgedLeases  []string
	getPendingErr       error
	getActiveErr        error
	acknowledgeErr      error
	acknowledgeCount    uint64
}

func (m *mockReconcilerChainClient) GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getPendingErr != nil {
		return nil, m.getPendingErr
	}
	return m.pendingLeases, nil
}

func (m *mockReconcilerChainClient) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getActiveErr != nil {
		return nil, m.getActiveErr
	}
	return m.activeLeases, nil
}

func (m *mockReconcilerChainClient) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acknowledgeErr != nil {
		return 0, nil, m.acknowledgeErr
	}
	m.acknowledgedLeases = append(m.acknowledgedLeases, leaseUUIDs...)
	m.acknowledgeCount += uint64(len(leaseUUIDs))
	return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
}

// mockReconcilerBackend implements backend.Backend for testing.
type mockReconcilerBackend struct {
	mu              sync.Mutex
	name            string
	provisions      []backend.ProvisionInfo
	provisionCalls  []backend.ProvisionRequest
	deprovisionCalls []string
	provisionErr    error
	deprovisionErr  error
	listErr         error
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

func TestNewReconciler_Validation(t *testing.T) {
	mockChain := &mockReconcilerChainClient{}
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
			wantErr:     "chainClient is required",
		},
		{
			name:        "missing router",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			router:      nil,
			wantErr:     "backendRouter is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ReconcilerConfig{CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			router:      router,
			wantErr:     "ProviderUUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid"},
			chainClient: mockChain,
			router:      router,
			wantErr:     "CallbackBaseURL is required",
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
	mockChain := &mockReconcilerChainClient{
		pendingLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockChain := &mockReconcilerChainClient{
		pendingLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockChain.mu.Lock()
	defer mockChain.mu.Unlock()
	if len(mockChain.acknowledgedLeases) != 1 {
		t.Errorf("expected 1 acknowledged lease, got %d", len(mockChain.acknowledgedLeases))
	}
	if mockChain.acknowledgedLeases[0] != "lease-1" {
		t.Errorf("expected lease-1, got %s", mockChain.acknowledgedLeases[0])
	}
}

func TestReconciler_ReconcileAll_ActiveNotProvisioned(t *testing.T) {
	// Setup: Active lease on chain, not provisioned on backend (anomaly)
	// Expected: Log anomaly and attempt to provision
	mockChain := &mockReconcilerChainClient{
		activeLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
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
	mockChain := &mockReconcilerChainClient{
		activeLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
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

	mockChain.mu.Lock()
	acknowledgeCount := len(mockChain.acknowledgedLeases)
	mockChain.mu.Unlock()

	if provisionCount != 0 {
		t.Errorf("expected 0 provision calls, got %d", provisionCount)
	}
	if deprovisionCount != 0 {
		t.Errorf("expected 0 deprovision calls, got %d", deprovisionCount)
	}
	if acknowledgeCount != 0 {
		t.Errorf("expected 0 acknowledge calls, got %d", acknowledgeCount)
	}
}

func TestReconciler_ReconcileAll_OrphanProvision(t *testing.T) {
	// Setup: No lease on chain, but provisioned on backend (orphan)
	// Expected: Deprovision the orphan
	mockChain := &mockReconcilerChainClient{
		// No leases
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
		name          string
		getPendingErr error
		getActiveErr  error
		wantErr       string
	}{
		{
			name:          "get pending error",
			getPendingErr: errors.New("chain unavailable"),
			wantErr:       "failed to get pending leases",
		},
		{
			name:         "get active error",
			getActiveErr: errors.New("chain unavailable"),
			wantErr:      "failed to get active leases",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChain := &mockReconcilerChainClient{
				getPendingErr: tt.getPendingErr,
				getActiveErr:  tt.getActiveErr,
			}

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
	mockChain := &mockReconcilerChainClient{
		pendingLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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
	mockChain := &mockReconcilerChainClient{}
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
	mockChain := &mockReconcilerChainClient{}
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
	mockChain := &mockReconcilerChainClient{
		pendingLeases: []billingtypes.Lease{
			{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
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

func TestReconciler_MultipleBackends(t *testing.T) {
	// Test reconciliation with multiple backends
	mockChain := &mockReconcilerChainClient{}

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
