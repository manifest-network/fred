package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

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
			wantErr:     "router is required",
		},
		{
			name:        "missing chain client",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			router:      router,
			chainClient: nil,
			wantErr:     "chainClient is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ManagerConfig{CallbackBaseURL: "http://localhost"},
			router:      router,
			chainClient: mockChain,
			wantErr:     "ProviderUUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ManagerConfig{ProviderUUID: "test-uuid"},
			router:      router,
			chainClient: mockChain,
			wantErr:     "CallbackBaseURL is required",
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

func TestManager_CallbackURL(t *testing.T) {
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

	expected := "http://localhost:8080/callbacks/provision"
	if got := manager.callbackURL(); got != expected {
		t.Errorf("callbackURL() = %q, want %q", got, expected)
	}
}

func TestManager_HandleLeaseCreated(t *testing.T) {
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
	mockChain := &chain.MockClient{}

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
	terminalErr := &chain.ChainTxError{Code: 22, Codespace: "billing", RawLog: "lease not in pending state"}

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
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

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

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
