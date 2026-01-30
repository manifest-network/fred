package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

// hashPayload computes the SHA-256 hash of a payload and returns it as a hex string.
func hashPayload(payload []byte) string {
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
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

func (m *mockManagerBackend) Health(ctx context.Context) error {
	return nil
}

func TestNewManager_Validation(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	tests := []struct {
		name        string
		cfg         ManagerConfig
		router      *backend.Router
		chainClient ChainClient
		wantErr     string
	}{
		{
			name:        "missing router",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			router:      nil,
			chainClient: mockChain,
			wantErr:     "backend router is required",
		},
		{
			name:        "missing chain client",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			router:      router,
			chainClient: nil,
			wantErr:     "chain client is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ManagerConfig{CallbackBaseURL: "http://localhost"},
			router:      router,
			chainClient: mockChain,
			wantErr:     "provider UUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid"},
			router:      router,
			chainClient: mockChain,
			wantErr:     "callback base URL is required",
		},
		{
			name:        "valid config",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			router:      router,
			chainClient: mockChain,
			wantErr:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewManager(tt.cfg, tt.router, tt.chainClient)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("NewManager() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("NewManager() error = nil, want error containing %q", tt.wantErr)
				} else if err.Error() != tt.wantErr {
					t.Errorf("NewManager() error = %q, want %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestManager_InFlightTracking(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Initially not in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = true before tracking, want false")
	}

	// Track
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "test-backend")

	// Should be in-flight
	if !manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = false after tracking, want true")
	}

	// GetInFlight should return the data
	provision, exists := manager.GetInFlight("lease-1")
	if !exists {
		t.Error("GetInFlight() exists = false, want true")
	}
	if provision.LeaseUUID != "lease-1" {
		t.Errorf("GetInFlight() LeaseUUID = %q, want %q", provision.LeaseUUID, "lease-1")
	}
	if provision.Tenant != "tenant-1" {
		t.Errorf("GetInFlight() Tenant = %q, want %q", provision.Tenant, "tenant-1")
	}
	if provision.SKU != "sku-1" {
		t.Errorf("GetInFlight() SKU = %q, want %q", provision.SKU, "sku-1")
	}
	if provision.Backend != "test-backend" {
		t.Errorf("GetInFlight() Backend = %q, want %q", provision.Backend, "test-backend")
	}

	// Should still be in-flight (GetInFlight doesn't remove)
	if !manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = false after GetInFlight, want true")
	}

	// Untrack
	manager.UntrackInFlight("lease-1")

	// Should no longer be in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = true after untracking, want false")
	}

	// GetInFlight should return false
	_, exists = manager.GetInFlight("lease-1")
	if exists {
		t.Error("GetInFlight() exists = true after untracking, want false")
	}
}

func TestManager_TryTrackInFlight(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// First attempt should succeed
	if !manager.TryTrackInFlight("lease-1", "tenant-1", "sku-1", "test-backend") {
		t.Error("TryTrackInFlight() first call = false, want true")
	}

	// Verify it was tracked
	if !manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = false after TryTrackInFlight, want true")
	}

	// Second attempt for same lease should fail (already tracked)
	if manager.TryTrackInFlight("lease-1", "tenant-2", "sku-2", "other-backend") {
		t.Error("TryTrackInFlight() second call = true, want false")
	}

	// Original tracking data should be preserved
	provision, exists := manager.GetInFlight("lease-1")
	if !exists {
		t.Fatal("GetInFlight() exists = false, want true")
	}
	if provision.Tenant != "tenant-1" {
		t.Errorf("GetInFlight() Tenant = %q, want %q (original data should be preserved)", provision.Tenant, "tenant-1")
	}

	// Different lease should succeed
	if !manager.TryTrackInFlight("lease-2", "tenant-2", "sku-2", "test-backend") {
		t.Error("TryTrackInFlight() for different lease = false, want true")
	}
}

// TestManager_TryTrackInFlight_RaceCondition is a regression test for the TOCTOU
// race condition between the reconciler and event-driven manager. It verifies that
// when multiple goroutines concurrently try to track the same lease, exactly one
// succeeds and the rest fail atomically.
//
// Run with: go test -race -run TestManager_TryTrackInFlight_RaceCondition
func TestManager_TryTrackInFlight_RaceCondition(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	const numGoroutines = 100
	const leaseUUID = "race-test-lease"

	// Track how many goroutines successfully tracked the lease
	var successCount int
	var mu sync.Mutex

	// Use a WaitGroup to synchronize goroutine completion
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Use a channel to synchronize goroutine start (maximize contention)
	start := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()

			// Wait for signal to start (all goroutines start at once)
			<-start

			// Try to track the lease
			if manager.TryTrackInFlight(leaseUUID, "tenant", "sku", "backend") {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	// Start all goroutines simultaneously
	close(start)

	// Wait for all goroutines to complete
	wg.Wait()

	// Exactly one goroutine should have succeeded
	if successCount != 1 {
		t.Errorf("TryTrackInFlight() success count = %d, want exactly 1", successCount)
	}

	// The lease should be tracked
	if !manager.IsInFlight(leaseUUID) {
		t.Error("IsInFlight() = false after race test, want true")
	}
}

func TestManager_PopInFlight(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track a lease
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "test-backend")

	// Pop should return the data and remove it
	provision, exists := manager.PopInFlight("lease-1")
	if !exists {
		t.Error("PopInFlight() exists = false, want true")
	}
	if provision.LeaseUUID != "lease-1" {
		t.Errorf("PopInFlight() LeaseUUID = %q, want %q", provision.LeaseUUID, "lease-1")
	}

	// Should no longer be in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("IsInFlight() = true after PopInFlight, want false")
	}

	// Pop again should return false
	_, exists = manager.PopInFlight("lease-1")
	if exists {
		t.Error("PopInFlight() exists = true on second call, want false")
	}
}

func TestBuildCallbackURL(t *testing.T) {
	expected := "http://localhost:8080/callbacks/provision"
	if got := BuildCallbackURL("http://localhost:8080"); got != expected {
		t.Errorf("BuildCallbackURL() = %q, want %q", got, expected)
	}
}

func TestManager_HandleLeaseCreated(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{
					{SkuUuid: "sku-1"},
				},
			}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Create a lease event message
	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = manager.handleLeaseCreated(msg)
	if err != nil {
		t.Errorf("handleLeaseCreated() error = %v", err)
	}

	// Verify provisioning was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Fatalf("expected 1 provision call, got %d", len(mockBackend.provisionCalls))
	}
	if mockBackend.provisionCalls[0].LeaseUUID != "lease-1" {
		t.Errorf("provision LeaseUUID = %q, want %q", mockBackend.provisionCalls[0].LeaseUUID, "lease-1")
	}
	if mockBackend.provisionCalls[0].Tenant != "tenant-1" {
		t.Errorf("provision Tenant = %q, want %q", mockBackend.provisionCalls[0].Tenant, "tenant-1")
	}
	if mockBackend.provisionCalls[0].ProviderUUID != "provider-1" {
		t.Errorf("provision ProviderUUID = %q, want %q", mockBackend.provisionCalls[0].ProviderUUID, "provider-1")
	}
	if mockBackend.provisionCalls[0].CallbackURL != "http://localhost:8080/callbacks/provision" {
		t.Errorf("provision CallbackURL = %q, want %q", mockBackend.provisionCalls[0].CallbackURL, "http://localhost:8080/callbacks/provision")
	}

	// Verify in-flight tracking
	if !manager.IsInFlight("lease-1") {
		t.Error("lease should be in-flight after handleLeaseCreated")
	}
}

func TestManager_HandleLeaseCreated_ProvisionError(t *testing.T) {
	provisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", provisionErr: provisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle should return error for retry
	err = manager.handleLeaseCreated(msg)
	if err == nil {
		t.Error("handleLeaseCreated() error = nil, want error")
	}
	if !errors.Is(err, ErrProvisioningFailed) {
		t.Errorf("handleLeaseCreated() error = %v, want ErrProvisioningFailed", err)
	}

	// Verify lease was untracked after error
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after provision error")
	}
}

func TestManager_HandleLeaseCreated_MalformedMessage(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = manager.handleLeaseCreated(msg)
	if err != nil {
		t.Errorf("handleLeaseCreated() error = %v, want nil for malformed message", err)
	}

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for malformed message, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandleLeaseClosed(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track lease first (simulating it was being provisioned)
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseClosed(msg)
	if err != nil {
		t.Errorf("handleLeaseClosed() error = %v", err)
	}

	// Verify deprovision was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.deprovisionCalls) != 1 {
		t.Fatalf("expected 1 deprovision call, got %d", len(mockBackend.deprovisionCalls))
	}
	if mockBackend.deprovisionCalls[0] != "lease-1" {
		t.Errorf("deprovision leaseUUID = %q, want %q", mockBackend.deprovisionCalls[0], "lease-1")
	}

	// Verify removed from in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after handleLeaseClosed")
	}
}

func TestManager_HandleLeaseClosed_DeprovisionError(t *testing.T) {
	deprovisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", deprovisionErr: deprovisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseClosed(msg)
	if err == nil {
		t.Error("handleLeaseClosed() error = nil, want error")
	}
	if !errors.Is(err, ErrDeprovisionFailed) {
		t.Errorf("handleLeaseClosed() error = %v, want ErrDeprovisionFailed", err)
	}
}

func TestManager_HandleBackendCallback_Success(t *testing.T) {
	var acknowledgedLeases []string
	var mu sync.Mutex

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			return []billingtypes.Lease{
				{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v", err)
	}

	// Verify acknowledge was called
	mu.Lock()
	defer mu.Unlock()
	if len(acknowledgedLeases) != 1 {
		t.Fatalf("expected 1 acknowledged lease, got %d", len(acknowledgedLeases))
	}
	if acknowledgedLeases[0] != "lease-1" {
		t.Errorf("acknowledged lease = %q, want %q", acknowledgedLeases[0], "lease-1")
	}

	// Verify removed from in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after successful callback")
	}
}

func TestManager_HandleBackendCallback_Failed(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "out of resources",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v", err)
	}

	// Verify removed from in-flight (failed is terminal)
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after failed callback")
	}
}

func TestManager_HandleBackendCallback_UnknownLease(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Don't track the lease - simulating unknown callback
	callback := backend.CallbackPayload{
		LeaseUUID: "unknown-lease",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (ignore unknown callbacks)
	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v, want nil for unknown lease", err)
	}
}

func TestManager_HandleBackendCallback_AcknowledgeError(t *testing.T) {
	acknowledgeErr := errors.New("chain unavailable")

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			return []billingtypes.Lease{
				{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, acknowledgeErr
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	if err == nil {
		t.Error("handleBackendCallback() error = nil, want error")
	}
	if !errors.Is(err, ErrAcknowledgeFailed) {
		t.Errorf("handleBackendCallback() error = %v, want ErrAcknowledgeFailed", err)
	}

	// Verify still in-flight (for retry)
	if !manager.IsInFlight("lease-1") {
		t.Error("lease should still be in-flight after acknowledge error (for retry)")
	}
}

func TestManager_HandleBackendCallback_AcknowledgeTerminalError(t *testing.T) {
	// Test that ErrLeaseNotPending errors are treated as terminal (success)
	// This prevents infinite retry loops when the lease is already acknowledged.
	// Code 22 is ErrLeaseNotPending in the billing module.
	// This simulates a race condition where GetPendingLeases returns PENDING but the lease
	// gets acknowledged by another process before our AcknowledgeLeases call.
	terminalErr := &chain.ChainTxError{Code: 22, Codespace: "billing", RawLog: "lease not in pending state"}

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return lease-1 as pending so ack batcher attempts acknowledgment
			// (simulates race condition where state changes between check and ack)
			return []billingtypes.Lease{
				{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, terminalErr
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (terminal error treated as success)
	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v, want nil for terminal acknowledge error", err)
	}

	// Verify removed from in-flight (not stuck for retry)
	if manager.IsInFlight("lease-1") {
		t.Error("lease should NOT be in-flight after terminal acknowledge error")
	}
}

func TestManager_IsTerminalAcknowledgeError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		terminal bool
	}{
		{
			name:     "nil error",
			err:      nil,
			terminal: false,
		},
		{
			name:     "transient error",
			err:      errors.New("connection refused"),
			terminal: false,
		},
		{
			name:     "lease not pending - chain error",
			err:      &chain.ChainTxError{Code: 22, Codespace: "billing", RawLog: "lease not in pending state"},
			terminal: true,
		},
		{
			name:     "lease not found - chain error",
			err:      &chain.ChainTxError{Code: 2, Codespace: "billing", RawLog: "lease not found"},
			terminal: true,
		},
		{
			name:     "wrong codespace - not terminal",
			err:      &chain.ChainTxError{Code: 22, Codespace: "other-module", RawLog: "some error"},
			terminal: false,
		},
		{
			name:     "wrong code - not terminal",
			err:      &chain.ChainTxError{Code: 999, Codespace: "billing", RawLog: "some error"},
			terminal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTerminalAcknowledgeError(tt.err)
			if got != tt.terminal {
				t.Errorf("isTerminalAcknowledgeError() = %v, want %v", got, tt.terminal)
			}
		})
	}
}

func TestManager_HandleBackendCallback_UnknownStatus(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    "unknown-status",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (unknown status is logged, not retried)
	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v, want nil for unknown status", err)
	}

	// Lease should be removed from in-flight (unknown status is treated as terminal
	// to prevent leases from being stuck indefinitely)
	if manager.IsInFlight("lease-1") {
		t.Error("lease should NOT be in-flight after unknown status callback (treated as terminal)")
	}
}

func TestManager_PublishLeaseEvent(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

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
				LeaseUUID: "lease-1",
				Tenant:    "tenant-1",
			}

			err := manager.PublishLeaseEvent(event)
			if tt.wantErr && err == nil {
				t.Error("PublishLeaseEvent() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("PublishLeaseEvent() error = %v, want nil", err)
			}
		})
	}
}

func TestManager_PublishCallback(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}

	err = manager.PublishCallback(callback)
	if err != nil {
		t.Errorf("PublishCallback() error = %v", err)
	}
}

func TestManager_HandleLeaseExpired(t *testing.T) {
	// Verify handleLeaseExpired delegates to handleLeaseClosed
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track lease first (simulating it was being provisioned)
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	// Send LeaseExpired event (not LeaseClosed)
	event := chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseExpired(msg)
	if err != nil {
		t.Errorf("handleLeaseExpired() error = %v", err)
	}

	// Verify deprovision was called (same behavior as handleLeaseClosed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.deprovisionCalls) != 1 {
		t.Fatalf("expected 1 deprovision call, got %d", len(mockBackend.deprovisionCalls))
	}
	if mockBackend.deprovisionCalls[0] != "lease-1" {
		t.Errorf("deprovision leaseUUID = %q, want %q", mockBackend.deprovisionCalls[0], "lease-1")
	}

	// Verify removed from in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after handleLeaseExpired")
	}
}

func TestManager_CallbackPath(t *testing.T) {
	// Verify the CallbackPath constant is correct
	expected := "/callbacks/provision"
	if CallbackPath != expected {
		t.Errorf("CallbackPath = %q, want %q", CallbackPath, expected)
	}
}

func TestManager_StartAndClose(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

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
	if err := manager.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Cancel context to ensure clean shutdown
	cancel()

	// Wait for Start to return
	select {
	case err := <-errCh:
		// Start may return nil or context.Canceled depending on timing
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Start() returned unexpected error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start() did not return after Close()")
	}
}

func TestManager_HandleLeaseClosed_BackendNameNotFound(t *testing.T) {
	// Test the warning log when backend lookup by name fails
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track with a backend name that doesn't exist in the router
	manager.TrackInFlight("lease-1", "tenant-1", "", "nonexistent-backend")

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should succeed by falling back to default backend
	err = manager.handleLeaseClosed(msg)
	if err != nil {
		t.Errorf("handleLeaseClosed() error = %v, want nil (should fallback to default)", err)
	}

	// Verify deprovision was called on the default backend
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.deprovisionCalls) != 1 {
		t.Fatalf("expected 1 deprovision call, got %d", len(mockBackend.deprovisionCalls))
	}
	if mockBackend.deprovisionCalls[0] != "lease-1" {
		t.Errorf("deprovision leaseUUID = %q, want %q", mockBackend.deprovisionCalls[0], "lease-1")
	}
}

func TestManager_HandleLeaseCreated_SKUBasedRouting(t *testing.T) {
	// Test that leases are routed to the correct backend based on SKU
	gpuBackend := &mockManagerBackend{name: "gpu-backend"}
	k8sBackend := &mockManagerBackend{name: "k8s-backend"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})

	mockChain := &chain.MockClient{
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Create a lease event message
	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "gpu-lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = manager.handleLeaseCreated(msg)
	if err != nil {
		t.Errorf("handleLeaseCreated() error = %v", err)
	}

	// Verify GPU backend received the provision call
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	if len(gpuCalls) != 1 {
		t.Fatalf("expected 1 provision call to GPU backend, got %d", len(gpuCalls))
	}
	if gpuCalls[0].LeaseUUID != "gpu-lease-1" {
		t.Errorf("provision LeaseUUID = %q, want %q", gpuCalls[0].LeaseUUID, "gpu-lease-1")
	}
	if gpuCalls[0].SKU != "gpu-a100-4x" {
		t.Errorf("provision SKU = %q, want %q", gpuCalls[0].SKU, "gpu-a100-4x")
	}

	// Verify K8s backend did NOT receive any calls
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	if len(k8sCalls) != 0 {
		t.Errorf("expected 0 provision calls to K8s backend, got %d", len(k8sCalls))
	}

	// Verify in-flight tracking includes the correct backend
	provision, exists := manager.GetInFlight("gpu-lease-1")
	if !exists {
		t.Fatal("lease should be in-flight")
	}
	if provision.Backend != "gpu-backend" {
		t.Errorf("in-flight Backend = %q, want %q", provision.Backend, "gpu-backend")
	}
	if provision.SKU != "gpu-a100-4x" {
		t.Errorf("in-flight SKU = %q, want %q", provision.SKU, "gpu-a100-4x")
	}
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
	mockChain := &chain.MockClient{
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "test")

	// Send failed callback with error message
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "out of GPU resources",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v", err)
	}

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	if len(rejectedLeases) != 1 {
		t.Fatalf("expected 1 rejected lease, got %d", len(rejectedLeases))
	}
	if rejectedLeases[0] != "lease-1" {
		t.Errorf("rejected lease = %q, want %q", rejectedLeases[0], "lease-1")
	}
	if rejectedReason != "out of GPU resources" {
		t.Errorf("rejection reason = %q, want %q", rejectedReason, "out of GPU resources")
	}

	// Verify removed from in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after failed callback")
	}
}

func TestManager_HandleBackendCallback_FailedDefaultReason(t *testing.T) {
	// Test that failed callbacks without error message use default reason
	var rejectedReason string
	var mu sync.Mutex

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "test")

	// Send failed callback WITHOUT error message
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "", // Empty error
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	if err != nil {
		t.Errorf("handleBackendCallback() error = %v", err)
	}

	// Verify default reason was used
	mu.Lock()
	defer mu.Unlock()
	if rejectedReason != "provisioning failed" {
		t.Errorf("rejection reason = %q, want %q", rejectedReason, "provisioning failed")
	}
}

func TestExtractPrimarySKU(t *testing.T) {
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
			got := ExtractPrimarySKU(tt.lease)
			if got != tt.want {
				t.Errorf("ExtractPrimarySKU() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestManager_GetInFlightLeases(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Initially empty
	leases := manager.GetInFlightLeases()
	if len(leases) != 0 {
		t.Errorf("GetInFlightLeases() returned %d leases, want 0", len(leases))
	}

	// Track some leases
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", "sku-2", "backend-2")
	manager.TrackInFlight("lease-3", "tenant-3", "sku-3", "backend-3")

	// Should return all 3
	leases = manager.GetInFlightLeases()
	if len(leases) != 3 {
		t.Errorf("GetInFlightLeases() returned %d leases, want 3", len(leases))
	}

	// Verify all expected leases are present
	leaseMap := make(map[string]bool)
	for _, uuid := range leases {
		leaseMap[uuid] = true
	}

	for _, expected := range []string{"lease-1", "lease-2", "lease-3"} {
		if !leaseMap[expected] {
			t.Errorf("GetInFlightLeases() missing %q", expected)
		}
	}

	// Untrack one
	manager.UntrackInFlight("lease-2")

	leases = manager.GetInFlightLeases()
	if len(leases) != 2 {
		t.Errorf("GetInFlightLeases() after untrack returned %d leases, want 2", len(leases))
	}
}

func TestManager_WaitForDrain_AlreadyEmpty(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// No in-flight leases, should return immediately with 0
	remaining := manager.WaitForDrain(context.Background(), 100*time.Millisecond)
	if remaining != 0 {
		t.Errorf("WaitForDrain() remaining = %d, want 0", remaining)
	}
}

func TestManager_WaitForDrain_DrainCompletes(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track some leases
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", "sku-2", "backend-2")

	// Start a goroutine to drain the leases after a delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		manager.UntrackInFlight("lease-1")
		manager.UntrackInFlight("lease-2")
	}()

	// Wait for drain with sufficient timeout
	remaining := manager.WaitForDrain(context.Background(), 1*time.Second)
	if remaining != 0 {
		t.Errorf("WaitForDrain() remaining = %d, want 0", remaining)
	}
}

func TestManager_WaitForDrain_TimeoutExpires(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track leases that won't be drained
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", "sku-2", "backend-2")

	// Wait with short timeout - should return remaining count
	remaining := manager.WaitForDrain(context.Background(), 100*time.Millisecond)
	if remaining != 2 {
		t.Errorf("WaitForDrain() remaining = %d, want 2", remaining)
	}

	// Clean up
	manager.UntrackInFlight("lease-1")
	manager.UntrackInFlight("lease-2")
}

func TestManager_WaitForDrain_ContextCancelled(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Track leases that won't be drained
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "backend-1")

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Wait with long timeout but context will be cancelled first
	remaining := manager.WaitForDrain(ctx, 5*time.Second)
	if remaining != 1 {
		t.Errorf("WaitForDrain() remaining = %d, want 1", remaining)
	}

	// Clean up
	manager.UntrackInFlight("lease-1")
}

func TestManager_InFlightCount(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Initially 0
	if manager.InFlightCount() != 0 {
		t.Errorf("InFlightCount() = %d, want 0", manager.InFlightCount())
	}

	// Track some
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "backend-1")
	if manager.InFlightCount() != 1 {
		t.Errorf("InFlightCount() = %d, want 1", manager.InFlightCount())
	}

	manager.TrackInFlight("lease-2", "tenant-2", "sku-2", "backend-2")
	if manager.InFlightCount() != 2 {
		t.Errorf("InFlightCount() = %d, want 2", manager.InFlightCount())
	}

	// Untrack one
	manager.UntrackInFlight("lease-1")
	if manager.InFlightCount() != 1 {
		t.Errorf("InFlightCount() after untrack = %d, want 1", manager.InFlightCount())
	}

	// Untrack the other
	manager.UntrackInFlight("lease-2")
	if manager.InFlightCount() != 0 {
		t.Errorf("InFlightCount() after all untracked = %d, want 0", manager.InFlightCount())
	}
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
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "tenant-1",
				ProviderUuid: "provider-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{
					{SkuUuid: "sku-1"},
				},
				MetaHash: []byte("somehash"),
			}, nil
		},
	}

	// Create a real PayloadStore using temp directory
	tempDir := t.TempDir()
	payloadStore, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Store a payload first
	testPayload := []byte("deployment manifest data")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("lease-1", testPayload)

	// Create a payload event message
	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v", err)
	}

	// Verify provisioning was called with payload
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Fatalf("expected 1 provision call, got %d", len(mockBackend.provisionCalls))
	}
	if mockBackend.provisionCalls[0].LeaseUUID != "lease-1" {
		t.Errorf("provision LeaseUUID = %q, want %q", mockBackend.provisionCalls[0].LeaseUUID, "lease-1")
	}
	if string(mockBackend.provisionCalls[0].Payload) != string(testPayload) {
		t.Errorf("provision Payload = %q, want %q", mockBackend.provisionCalls[0].Payload, testPayload)
	}
	if mockBackend.provisionCalls[0].PayloadHash != testPayloadHash {
		t.Errorf("provision PayloadHash = %q, want %q", mockBackend.provisionCalls[0].PayloadHash, testPayloadHash)
	}

	// Verify in-flight tracking
	if !manager.IsInFlight("lease-1") {
		t.Error("lease should be in-flight after handlePayloadReceived")
	}

	// Verify payload was deleted from store after successful provisioning
	if payloadStore.Has("lease-1") {
		t.Error("payload should be deleted from store after successful provisioning")
	}
}

func TestManager_HandlePayloadReceived_NoPayloadStore(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	// Create manager WITHOUT payload store
	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    nil, // No payload store
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (no retry) when payload store is not configured
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil for missing payload store", err)
	}

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls when payload store is nil, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandlePayloadReceived_MalformedMessage(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil for malformed message", err)
	}

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for malformed message, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandlePayloadReceived_LeaseNotFound(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Store a payload
	payloadStore.Store("lease-1", []byte("payload data"))

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (lease not found, clean up payload)
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil for lease not found", err)
	}

	// Verify payload was cleaned up
	if payloadStore.Has("lease-1") {
		t.Error("payload should be deleted when lease not found")
	}

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for lease not found, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandlePayloadReceived_LeaseNotPending(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_ACTIVE, // Not PENDING
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Store a payload
	payloadStore.Store("lease-1", []byte("payload data"))

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (lease not pending, skip provisioning)
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil for non-pending lease", err)
	}

	// Verify payload was cleaned up
	if payloadStore.Has("lease-1") {
		t.Error("payload should be deleted when lease is not pending")
	}

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for non-pending lease, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandlePayloadReceived_ChainError(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	chainErr := errors.New("chain unavailable")
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, chainErr
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = manager.handlePayloadReceived(msg)
	if err == nil {
		t.Error("handlePayloadReceived() error = nil, want error for chain error")
	}
}

func TestManager_HandlePayloadReceived_ProvisionError(t *testing.T) {
	provisionErr := errors.New("backend unavailable")
	mockBackend := &mockManagerBackend{name: "test", provisionErr: provisionErr}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Store payload
	testPayload := []byte("payload data")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("lease-1", testPayload)

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = manager.handlePayloadReceived(msg)
	if err == nil {
		t.Error("handlePayloadReceived() error = nil, want error")
	}
	if !errors.Is(err, ErrProvisioningFailed) {
		t.Errorf("handlePayloadReceived() error = %v, want ErrProvisioningFailed", err)
	}

	// Verify lease was untracked after error
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after provision error")
	}

	// Verify payload was NOT deleted (kept for retry)
	if !payloadStore.Has("lease-1") {
		t.Error("payload should be kept for retry after provision error")
	}
}

func TestManager_HandlePayloadReceived_AlreadyInFlight(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Pre-track the lease (simulating concurrent processing)
	manager.TrackInFlight("lease-1", "tenant-1", "sku-1", "test")

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (skip already in-flight lease)
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil for already in-flight", err)
	}

	// Verify no provisioning was attempted (already being processed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for already in-flight lease, got %d", len(mockBackend.provisionCalls))
	}
}

func TestManager_HandlePayloadReceived_MissingPayloadInStore(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// DON'T store a payload - simulate race where payload was cleaned up

	event := PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should succeed (proceeds without payload with warning)
	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v, want nil when payload missing from store", err)
	}

	// Verify provisioning was called (with nil payload and no hash)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 1 {
		t.Fatalf("expected 1 provision call, got %d", len(mockBackend.provisionCalls))
	}
	if mockBackend.provisionCalls[0].Payload != nil {
		t.Errorf("provision Payload = %v, want nil", mockBackend.provisionCalls[0].Payload)
	}
	// PayloadHash should be empty when payload is missing - backends should never
	// receive a hash without the corresponding payload data
	if mockBackend.provisionCalls[0].PayloadHash != "" {
		t.Errorf("provision PayloadHash = %q, want empty when payload is missing", mockBackend.provisionCalls[0].PayloadHash)
	}
}

func TestManager_HandlePayloadReceived_SKUBasedRouting(t *testing.T) {
	gpuBackend := &mockManagerBackend{name: "gpu-backend"}
	k8sBackend := &mockManagerBackend{name: "k8s-backend"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := NewPayloadStore(PayloadStoreConfig{
		DBPath: tempDir + "/payloads.db",
		TTL:    time.Hour,
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Store payload
	testPayload := []byte("gpu deployment")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("gpu-lease-1", testPayload)

	event := PayloadEvent{
		LeaseUUID:   "gpu-lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handlePayloadReceived(msg)
	if err != nil {
		t.Errorf("handlePayloadReceived() error = %v", err)
	}

	// Verify GPU backend received the call
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	if len(gpuCalls) != 1 {
		t.Fatalf("expected 1 provision call to GPU backend, got %d", len(gpuCalls))
	}
	if gpuCalls[0].SKU != "gpu-a100" {
		t.Errorf("provision SKU = %q, want %q", gpuCalls[0].SKU, "gpu-a100")
	}

	// Verify K8s backend did NOT receive any calls
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	if len(k8sCalls) != 0 {
		t.Errorf("expected 0 provision calls to K8s backend, got %d", len(k8sCalls))
	}

	// Verify in-flight tracking has correct backend
	provision, exists := manager.GetInFlight("gpu-lease-1")
	if !exists {
		t.Fatal("lease should be in-flight")
	}
	if provision.Backend != "gpu-backend" {
		t.Errorf("in-flight Backend = %q, want %q", provision.Backend, "gpu-backend")
	}
}

// TestManager_CheckCallbackTimeouts tests the timeout detection and rejection logic.
func TestManager_CheckCallbackTimeouts(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var mu sync.Mutex
	var rejectedLeases []string
	var rejectReason string

	mockChain := &chain.MockClient{
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:         "provider-1",
		CallbackBaseURL:      "http://localhost:8080",
		CallbackTimeout:      100 * time.Millisecond, // Short timeout for testing
		TimeoutCheckInterval: 50 * time.Millisecond,
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	t.Run("detects_timed_out_provisions", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track a lease with an artificial old start time
		manager.inFlightMu.Lock()
		manager.inFlight["timeout-lease"] = inFlightProvision{
			LeaseUUID: "timeout-lease",
			Tenant:    "tenant-1",
			SKU:       "test-sku",
			Backend:   "test",
			StartTime: time.Now().Add(-1 * time.Hour), // Very old
		}
		manager.inFlightMu.Unlock()

		// Run timeout check
		manager.checkCallbackTimeouts(context.Background())

		// Verify lease was rejected
		mu.Lock()
		defer mu.Unlock()
		if len(rejectedLeases) != 1 {
			t.Fatalf("expected 1 rejected lease, got %d", len(rejectedLeases))
		}
		if rejectedLeases[0] != "timeout-lease" {
			t.Errorf("rejected lease = %q, want %q", rejectedLeases[0], "timeout-lease")
		}
		if rejectReason != "callback timeout" {
			t.Errorf("reject reason = %q, want %q", rejectReason, "callback timeout")
		}

		// Verify removed from in-flight
		if manager.IsInFlight("timeout-lease") {
			t.Error("timed-out lease should be removed from in-flight")
		}
	})

	t.Run("ignores_recent_provisions", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track a fresh lease
		manager.TrackInFlight("fresh-lease", "tenant-1", "test-sku", "test")

		// Run timeout check
		manager.checkCallbackTimeouts(context.Background())

		// Verify lease was NOT rejected
		mu.Lock()
		rejected := len(rejectedLeases)
		mu.Unlock()

		if rejected != 0 {
			t.Errorf("expected 0 rejected leases, got %d", rejected)
		}

		// Verify still in-flight
		if !manager.IsInFlight("fresh-lease") {
			t.Error("fresh lease should still be in-flight")
		}

		// Cleanup
		manager.UntrackInFlight("fresh-lease")
	})

	t.Run("handles_context_cancellation", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track multiple old leases
		manager.inFlightMu.Lock()
		for i := range 5 {
			leaseID := "cancel-lease-" + string(rune('a'+i))
			manager.inFlight[leaseID] = inFlightProvision{
				LeaseUUID: leaseID,
				Tenant:    "tenant-1",
				SKU:       "test-sku",
				Backend:   "test",
				StartTime: time.Now().Add(-1 * time.Hour),
			}
		}
		manager.inFlightMu.Unlock()

		// Cancel context immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Run timeout check with cancelled context
		manager.checkCallbackTimeouts(ctx)

		// May have processed 0 or 1 lease before noticing cancellation
		mu.Lock()
		rejected := len(rejectedLeases)
		mu.Unlock()

		// Should have stopped early due to cancellation
		if rejected >= 5 {
			t.Errorf("expected less than 5 rejected leases with cancelled context, got %d", rejected)
		}

		// Cleanup remaining
		manager.inFlightMu.Lock()
		for leaseID := range manager.inFlight {
			delete(manager.inFlight, leaseID)
		}
		manager.inFlightMu.Unlock()
	})

	t.Run("handles_chain_reject_error", func(t *testing.T) {
		// Create manager with failing chain client
		failingChain := &chain.MockClient{
			RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
				return 0, nil, errors.New("chain unavailable")
			},
		}

		failManager, err := NewManager(ManagerConfig{
			ProviderUUID:         "provider-1",
			CallbackBaseURL:      "http://localhost:8080",
			CallbackTimeout:      100 * time.Millisecond,
			TimeoutCheckInterval: 50 * time.Millisecond,
		}, router, failingChain)
		if err != nil {
			t.Fatalf("NewManager() error = %v", err)
		}

		// Track an old lease
		failManager.inFlightMu.Lock()
		failManager.inFlight["fail-lease"] = inFlightProvision{
			LeaseUUID: "fail-lease",
			Tenant:    "tenant-1",
			SKU:       "test-sku",
			Backend:   "test",
			StartTime: time.Now().Add(-1 * time.Hour),
		}
		failManager.inFlightMu.Unlock()

		// Run timeout check - should not panic
		failManager.checkCallbackTimeouts(context.Background())

		// Lease should be removed from in-flight even on chain error
		// (metrics recorded, reconciler will pick up later)
		if failManager.IsInFlight("fail-lease") {
			t.Error("lease should be removed from in-flight even on chain reject error")
		}
	})
}

// TestManager_RunTimeoutChecker tests the background timeout checker goroutine.
func TestManager_RunTimeoutChecker(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var mu sync.Mutex
	var rejectedLeases []string

	mockChain := &chain.MockClient{
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:         "provider-1",
		CallbackBaseURL:      "http://localhost:8080",
		CallbackTimeout:      50 * time.Millisecond,  // Short for testing
		TimeoutCheckInterval: 25 * time.Millisecond, // Check frequently
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Start the timeout checker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.runTimeoutChecker(ctx)

	// Track a lease that will timeout
	manager.inFlightMu.Lock()
	manager.inFlight["auto-timeout-lease"] = inFlightProvision{
		LeaseUUID: "auto-timeout-lease",
		Tenant:    "tenant-1",
		SKU:       "test-sku",
		Backend:   "test",
		StartTime: time.Now().Add(-1 * time.Second), // Already old
	}
	manager.inFlightMu.Unlock()

	// Wait for timeout checker to run
	time.Sleep(100 * time.Millisecond)

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

	if !found {
		t.Error("timeout checker should have rejected the timed-out lease")
	}

	// Verify removed from in-flight
	if manager.IsInFlight("auto-timeout-lease") {
		t.Error("lease should be removed from in-flight after timeout")
	}
}
