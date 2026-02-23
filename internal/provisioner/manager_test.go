package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/provisioner/payload"
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

func (m *mockManagerBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
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
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
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
	require.NoError(t, err)

	// Initially not in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "IsInFlight() should be false before tracking")

	// Track
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test-backend")

	// Should be in-flight
	assert.True(t, manager.IsInFlight("lease-1"), "IsInFlight() should be true after tracking")

	// GetInFlight should return the data
	provision, exists := manager.GetInFlight("lease-1")
	assert.True(t, exists, "GetInFlight() exists should be true")
	assert.Equal(t, "lease-1", provision.LeaseUUID)
	assert.Equal(t, "tenant-1", provision.Tenant)
	assert.Equal(t, "sku-1", provision.RoutingSKU())
	assert.Equal(t, "test-backend", provision.Backend)

	// Should still be in-flight (GetInFlight doesn't remove)
	assert.True(t, manager.IsInFlight("lease-1"), "IsInFlight() should be true after GetInFlight")

	// Untrack
	manager.UntrackInFlight("lease-1")

	// Should no longer be in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "IsInFlight() should be false after untracking")

	// GetInFlight should return false
	_, exists = manager.GetInFlight("lease-1")
	assert.False(t, exists, "GetInFlight() exists should be false after untracking")
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
	require.NoError(t, err)

	// First attempt should succeed
	assert.True(t, manager.TryTrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test-backend"), "TryTrackInFlight() first call should succeed")

	// Verify it was tracked
	assert.True(t, manager.IsInFlight("lease-1"), "IsInFlight() should be true after TryTrackInFlight")

	// Second attempt for same lease should fail (already tracked)
	assert.False(t, manager.TryTrackInFlight("lease-1", "tenant-2", testItems("sku-2"), "other-backend"), "TryTrackInFlight() second call should fail")

	// Original tracking data should be preserved
	provision, exists := manager.GetInFlight("lease-1")
	require.True(t, exists, "GetInFlight() exists should be true")
	assert.Equal(t, "tenant-1", provision.Tenant, "original data should be preserved")

	// Different lease should succeed
	assert.True(t, manager.TryTrackInFlight("lease-2", "tenant-2", testItems("sku-2"), "test-backend"), "TryTrackInFlight() for different lease should succeed")
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
	require.NoError(t, err)

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

	for i := range numGoroutines {
		go func(workerID int) {
			defer wg.Done()

			// Wait for signal to start (all goroutines start at once)
			<-start

			// Try to track the lease
			if manager.TryTrackInFlight(leaseUUID, "tenant", testItems("sku"), "backend") {
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
	assert.Equal(t, 1, successCount, "TryTrackInFlight() should succeed exactly once")

	// The lease should be tracked
	assert.True(t, manager.IsInFlight(leaseUUID), "IsInFlight() should be true after race test")
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
	require.NoError(t, err)

	// Track a lease
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test-backend")

	// Pop should return the data and remove it
	provision, exists := manager.PopInFlight("lease-1")
	assert.True(t, exists, "PopInFlight() exists should be true")
	assert.Equal(t, "lease-1", provision.LeaseUUID)

	// Should no longer be in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "IsInFlight() should be false after PopInFlight")

	// Pop again should return false
	_, exists = manager.PopInFlight("lease-1")
	assert.False(t, exists, "PopInFlight() exists should be false on second call")
}

func TestBuildCallbackURL(t *testing.T) {
	expected := "http://localhost:8080/callbacks/provision"
	assert.Equal(t, expected, BuildCallbackURL("http://localhost:8080"))
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
	require.NoError(t, err)

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
	assert.NoError(t, err)

	// Verify provisioning was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)
	assert.Equal(t, "tenant-1", mockBackend.provisionCalls[0].Tenant)
	assert.Equal(t, "provider-1", mockBackend.provisionCalls[0].ProviderUUID)
	assert.Equal(t, "http://localhost:8080/callbacks/provision", mockBackend.provisionCalls[0].CallbackURL)

	// Verify in-flight tracking
	assert.True(t, manager.IsInFlight("lease-1"), "lease should be in-flight after handleLeaseCreated")
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
	require.NoError(t, err)

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle should return error for retry
	err = manager.handleLeaseCreated(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProvisioningFailed)

	// Verify lease was untracked after error
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after provision error")
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
	require.NoError(t, err)

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = manager.handleLeaseCreated(msg)
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
	mockChain := &chain.MockClient{}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err)

	// Track lease first (simulating it was being provisioned)
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseClosed(msg)
	assert.NoError(t, err)

	// Verify deprovision was called
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.deprovisionCalls[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after handleLeaseClosed")
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
	require.NoError(t, err)

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseClosed(msg)
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
	require.NoError(t, err)

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify acknowledge was called
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, acknowledgedLeases, 1)
	assert.Equal(t, "lease-1", acknowledgedLeases[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after successful callback")
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
	require.NoError(t, err)

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "out of resources",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify removed from in-flight (failed is terminal)
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after failed callback")
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
	require.NoError(t, err)

	// Don't track the lease - simulating unknown callback
	callback := backend.CallbackPayload{
		LeaseUUID: "unknown-lease",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (ignore unknown callbacks)
	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err, "should return nil for unknown lease")
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
	require.NoError(t, err)

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAcknowledgeFailed)

	// Verify still in-flight (for retry)
	assert.True(t, manager.IsInFlight("lease-1"), "lease should still be in-flight after acknowledge error (for retry)")
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
	require.NoError(t, err)

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (terminal error treated as success)
	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err, "should return nil for terminal acknowledge error")

	// Verify removed from in-flight (not stuck for retry)
	assert.False(t, manager.IsInFlight("lease-1"), "lease should NOT be in-flight after terminal acknowledge error")
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
			assert.Equal(t, tt.terminal, got)
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
	require.NoError(t, err)

	// Track the lease
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    "unknown-status",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (unknown status is logged, not retried)
	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err, "should return nil for unknown status")

	// Lease should be removed from in-flight (unknown status is treated as terminal
	// to prevent leases from being stuck indefinitely)
	assert.False(t, manager.IsInFlight("lease-1"), "lease should NOT be in-flight after unknown status callback (treated as terminal)")
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
				LeaseUUID: "lease-1",
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
	require.NoError(t, err)

	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}

	err = manager.PublishCallback(callback)
	assert.NoError(t, err)
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
	require.NoError(t, err)

	// Track lease first (simulating it was being provisioned)
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	// Send LeaseExpired event (not LeaseClosed)
	event := chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleLeaseExpired(msg)
	assert.NoError(t, err)

	// Verify deprovision was called (same behavior as handleLeaseClosed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.deprovisionCalls[0])

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after handleLeaseExpired")
}

func TestManager_CallbackPath(t *testing.T) {
	// Verify the CallbackPath constant is correct
	assert.Equal(t, "/callbacks/provision", CallbackPath)
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
	require.NoError(t, err)

	// Track with a backend name that doesn't exist in the router
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "nonexistent-backend")

	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should succeed by falling back to default backend
	err = manager.handleLeaseClosed(msg)
	assert.NoError(t, err, "should fallback to default backend")

	// Verify deprovision was called on the default backend
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.deprovisionCalls[0])
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
	err = manager.handleLeaseCreated(msg)
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

	// Verify in-flight tracking includes the correct backend
	provision, exists := manager.GetInFlight("gpu-lease-1")
	require.True(t, exists, "lease should be in-flight")
	assert.Equal(t, "gpu-backend", provision.Backend)
	assert.Equal(t, "gpu-a100-4x", provision.RoutingSKU())
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
	require.NoError(t, err)

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test")

	// Send failed callback with error message
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "out of GPU resources",
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
	assert.NoError(t, err)

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, "out of GPU resources", rejectedReason)

	// Verify removed from in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after failed callback")
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
	require.NoError(t, err)

	// Track the lease first
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test")

	// Send failed callback WITHOUT error message
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "", // Empty error
	}
	payload, _ := json.Marshal(callback)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handleBackendCallback(msg)
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
	require.NoError(t, err)

	// Initially empty
	leases := manager.GetInFlightLeases()
	assert.Empty(t, leases)

	// Track some leases
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", testItems("sku-2"), "backend-2")
	manager.TrackInFlight("lease-3", "tenant-3", testItems("sku-3"), "backend-3")

	// Should return all 3
	leases = manager.GetInFlightLeases()
	assert.Len(t, leases, 3)

	// Verify all expected leases are present
	assert.Contains(t, leases, "lease-1")
	assert.Contains(t, leases, "lease-2")
	assert.Contains(t, leases, "lease-3")

	// Untrack one
	manager.UntrackInFlight("lease-2")

	leases = manager.GetInFlightLeases()
	assert.Len(t, leases, 2)
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
	require.NoError(t, err)

	// No in-flight leases, should return immediately with 0
	remaining := manager.WaitForDrain(context.Background(), 100*time.Millisecond)
	assert.Equal(t, 0, remaining)
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
	require.NoError(t, err)

	// Track some leases
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", testItems("sku-2"), "backend-2")

	// Start a goroutine to drain the leases after a delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		manager.UntrackInFlight("lease-1")
		manager.UntrackInFlight("lease-2")
	}()

	// Wait for drain with sufficient timeout
	remaining := manager.WaitForDrain(context.Background(), 1*time.Second)
	assert.Equal(t, 0, remaining)
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
	require.NoError(t, err)

	// Track leases that won't be drained
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "backend-1")
	manager.TrackInFlight("lease-2", "tenant-2", testItems("sku-2"), "backend-2")

	// Wait with short timeout - should return remaining count
	remaining := manager.WaitForDrain(context.Background(), 100*time.Millisecond)
	assert.Equal(t, 2, remaining)

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
	require.NoError(t, err)

	// Track leases that won't be drained
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "backend-1")

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Wait with long timeout but context will be cancelled first
	remaining := manager.WaitForDrain(ctx, 5*time.Second)
	assert.Equal(t, 1, remaining)

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
	require.NoError(t, err)

	// Initially 0
	assert.Equal(t, 0, manager.InFlightCount())

	// Track some
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "backend-1")
	assert.Equal(t, 1, manager.InFlightCount())

	manager.TrackInFlight("lease-2", "tenant-2", testItems("sku-2"), "backend-2")
	assert.Equal(t, 2, manager.InFlightCount())

	// Untrack one
	manager.UntrackInFlight("lease-1")
	assert.Equal(t, 1, manager.InFlightCount())

	// Untrack the other
	manager.UntrackInFlight("lease-2")
	assert.Equal(t, 0, manager.InFlightCount())
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
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload first
	testPayload := []byte("deployment manifest data")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("lease-1", testPayload)

	// Create a payload event message
	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Handle the message
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err)

	// Verify provisioning was called with payload
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)
	assert.Equal(t, string(testPayload), string(mockBackend.provisionCalls[0].Payload))
	assert.Equal(t, testPayloadHash, mockBackend.provisionCalls[0].PayloadHash)

	// Verify in-flight tracking
	assert.True(t, manager.IsInFlight("lease-1"), "lease should be in-flight after handlePayloadReceived")

	// Verify payload is still in store - it should only be deleted after callback
	// This ensures the payload is available for retry if provisioning fails
	hasP, errP := payloadStore.Has("lease-1")
	require.NoError(t, errP)
	assert.True(t, hasP, "payload should remain in store until callback is received")
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
	require.NoError(t, err)

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (no retry) when payload store is not configured
	err = manager.handlePayloadReceived(msg)
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
	mockChain := &chain.MockClient{}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Send invalid JSON
	msg := message.NewMessage(watermill.NewUUID(), []byte("invalid json"))

	// Handle should return nil (don't retry malformed messages)
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for malformed message")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for malformed message")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload
	payloadStore.Store("lease-1", []byte("payload data"))

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (lease not found, clean up payload)
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for lease not found")

	// Verify payload was cleaned up
	hasP2, errP2 := payloadStore.Has("lease-1")
	require.NoError(t, errP2)
	assert.False(t, hasP2, "payload should be deleted when lease not found")

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for lease not found")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store a payload
	payloadStore.Store("lease-1", []byte("payload data"))

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (lease not pending, skip provisioning)
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for non-pending lease")

	// Verify payload was cleaned up
	hasP3, errP3 := payloadStore.Has("lease-1")
	require.NoError(t, errP3)
	assert.False(t, hasP3, "payload should be deleted when lease is not pending")

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
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, chainErr
		},
	}

	tempDir := t.TempDir()
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = manager.handlePayloadReceived(msg)
	require.Error(t, err, "should return error for chain error")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	testPayload := []byte("payload data")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("lease-1", testPayload)

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return error for retry
	err = manager.handlePayloadReceived(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProvisioningFailed)

	// Verify lease was untracked after error
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after provision error")

	// Verify payload was NOT deleted (kept for retry)
	hasP4, errP4 := payloadStore.Has("lease-1")
	require.NoError(t, errP4)
	assert.True(t, hasP4, "payload should be kept for retry after provision error")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Pre-track the lease (simulating concurrent processing)
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test")

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should return nil (skip already in-flight lease)
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil for already in-flight lease")

	// Verify no provisioning was attempted (already being processed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for already in-flight lease")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// DON'T store a payload - simulate race where payload was cleaned up

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: "abc123",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	// Should succeed (proceeds without payload with warning)
	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil when payload missing from store")

	// Verify provisioning was called (with nil payload and no hash)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Nil(t, mockBackend.provisionCalls[0].Payload)
	// PayloadHash should be empty when payload is missing - backends should never
	// receive a hash without the corresponding payload data
	assert.Empty(t, mockBackend.provisionCalls[0].PayloadHash, "PayloadHash should be empty when payload is missing")
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
	payloadStore, _ := payload.NewStore(payload.StoreConfig{
		DBPath: tempDir + "/payloads.db",
	})
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	testPayload := []byte("gpu deployment")
	testPayloadHash := hashPayload(testPayload)
	payloadStore.Store("gpu-lease-1", testPayload)

	event := payload.Event{
		LeaseUUID:   "gpu-lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	err = manager.handlePayloadReceived(msg)
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

	// Verify in-flight tracking has correct backend
	provision, exists := manager.GetInFlight("gpu-lease-1")
	require.True(t, exists, "lease should be in-flight")
	assert.Equal(t, "gpu-backend", provision.Backend)
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
	require.NoError(t, err)

	t.Run("detects_timed_out_provisions", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track a lease with an artificial old start time
		tracker := manager.Tracker().(*DefaultInFlightTracker)
		tracker.TrackInFlightWithStartTime("timeout-lease", "tenant-1", testItems("test-sku"), "test", time.Now().Add(-1*time.Hour))

		// Run timeout check
		manager.checkCallbackTimeouts(context.Background())

		// Verify lease was rejected
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, rejectedLeases, 1)
		assert.Equal(t, "timeout-lease", rejectedLeases[0])
		assert.Equal(t, "callback timeout", rejectReason)

		// Verify removed from in-flight
		assert.False(t, manager.IsInFlight("timeout-lease"), "timed-out lease should be removed from in-flight")
	})

	t.Run("ignores_recent_provisions", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track a fresh lease
		manager.TrackInFlight("fresh-lease", "tenant-1", testItems("test-sku"), "test")

		// Run timeout check
		manager.checkCallbackTimeouts(context.Background())

		// Verify lease was NOT rejected
		mu.Lock()
		rejected := len(rejectedLeases)
		mu.Unlock()

		assert.Equal(t, 0, rejected, "fresh lease should not be rejected")

		// Verify still in-flight
		assert.True(t, manager.IsInFlight("fresh-lease"), "fresh lease should still be in-flight")

		// Cleanup
		manager.UntrackInFlight("fresh-lease")
	})

	t.Run("handles_context_cancellation", func(t *testing.T) {
		mu.Lock()
		rejectedLeases = nil
		mu.Unlock()

		// Track multiple old leases
		tracker := manager.Tracker().(*DefaultInFlightTracker)
		for i := range 5 {
			leaseID := "cancel-lease-" + string(rune('a'+i))
			tracker.TrackInFlightWithStartTime(leaseID, "tenant-1", testItems("test-sku"), "test", time.Now().Add(-1*time.Hour))
		}

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
		assert.Less(t, rejected, 5, "should have stopped early due to cancellation")

		// Cleanup remaining
		for _, leaseID := range manager.GetInFlightLeases() {
			manager.UntrackInFlight(leaseID)
		}
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
		require.NoError(t, err)

		// Track an old lease
		tracker := failManager.Tracker().(*DefaultInFlightTracker)
		tracker.TrackInFlightWithStartTime("fail-lease", "tenant-1", testItems("test-sku"), "test", time.Now().Add(-1*time.Hour))

		// Run timeout check - should not panic
		failManager.checkCallbackTimeouts(context.Background())

		// Lease should REMAIN in-flight when chain reject fails
		// This prevents the reconciler from seeing a PENDING lease not in-flight
		// and trying to re-provision it. The next timeout check will retry the rejection.
		assert.True(t, failManager.IsInFlight("fail-lease"), "lease should remain in-flight when chain reject fails to prevent re-provisioning")

		// Clean up
		failManager.UntrackInFlight("fail-lease")
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
		CallbackTimeout:      50 * time.Millisecond, // Short for testing
		TimeoutCheckInterval: 25 * time.Millisecond, // Check frequently
	}, router, mockChain)
	require.NoError(t, err)

	// Start the timeout checker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go manager.TimeoutChecker().Start(ctx)

	// Track a lease that will timeout
	tracker := manager.Tracker().(*DefaultInFlightTracker)
	tracker.TrackInFlightWithStartTime("auto-timeout-lease", "tenant-1", testItems("test-sku"), "test", time.Now().Add(-1*time.Second))

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

	mockChain := &chain.MockClient{
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload (simulating upload)
	require.True(t, payloadStore.Store("lease-1", testPayload), "failed to store payload")

	// Step 1: Send payload received event to trigger provisioning
	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	err = manager.handlePayloadReceived(msg)
	require.NoError(t, err)

	// Step 2: REGRESSION CHECK - Payload must still exist after Provision() returns
	// Previously, the payload was deleted here, causing data loss if callback failed
	hasP5, errP5 := payloadStore.Has("lease-1")
	require.NoError(t, errP5)
	require.True(t, hasP5, "REGRESSION: payload was deleted after Provision() - should persist until callback")

	// Verify provisioning was called
	mockBackend.mu.Lock()
	require.Len(t, mockBackend.provisionCalls, 1)
	mockBackend.mu.Unlock()

	// Verify lease is in-flight
	require.True(t, manager.IsInFlight("lease-1"), "lease should be in-flight after provisioning started")

	// Step 3: Simulate successful callback - payload should be deleted now
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}
	callbackPayload, _ := json.Marshal(callback)
	callbackMsg := message.NewMessage(watermill.NewUUID(), callbackPayload)

	err = manager.handleBackendCallback(callbackMsg)
	require.NoError(t, err)

	// Payload should persist after successful callback — it's retained for
	// potential re-provisioning if the container crashes after acknowledgment.
	// Cleanup happens when the lease is closed or rejected.
	hasP6, errP6 := payloadStore.Has("lease-1")
	require.NoError(t, errP6)
	assert.True(t, hasP6, "payload should persist after successful callback for re-provisioning")

	// Lease should no longer be in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after successful callback")
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

	mockChain := &chain.MockClient{
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload
	require.True(t, payloadStore.Store("lease-1", testPayload), "failed to store payload")

	// Trigger provisioning
	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	err = manager.handlePayloadReceived(msg)
	require.NoError(t, err)

	// Payload should still exist
	hasP7, errP7 := payloadStore.Has("lease-1")
	require.NoError(t, errP7)
	require.True(t, hasP7, "payload should exist after provisioning started")

	// Simulate failed callback
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "unknown SKU",
	}
	callbackPayload, _ := json.Marshal(callback)
	callbackMsg := message.NewMessage(watermill.NewUUID(), callbackPayload)

	err = manager.handleBackendCallback(callbackMsg)
	require.NoError(t, err)

	// Payload should be deleted after failed callback (no point keeping it)
	hasP8, errP8 := payloadStore.Has("lease-1")
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

	mockChain := &chain.MockClient{
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err)

	// Store payload and trigger provisioning
	require.True(t, payloadStore.Store("lease-1", testPayload), "failed to store payload")

	event := payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: testPayloadHash,
	}
	eventPayload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), eventPayload)

	err = manager.handlePayloadReceived(msg)
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
	hasP9, errP9 := payloadStore2.Has("lease-1")
	require.NoError(t, errP9)
	require.True(t, hasP9, "REGRESSION: payload was lost after restart - reconciliation cannot retry")

	// Verify we can retrieve the payload with correct content
	retrievedPayload, err := payloadStore2.Get("lease-1")
	require.NoError(t, err)
	assert.Equal(t, string(testPayload), string(retrievedPayload))
}

// TestCallbacksRequireRunningRouter is a regression test for the startup race condition
// where callbacks could arrive before Watermill handlers were subscribed.
// The fix ensures we wait for manager.Running() before accepting callbacks.
func TestCallbacksRequireRunningRouter(t *testing.T) {
	// This test verifies that:
	// 1. Publishing before Running() results in lost messages
	// 2. Publishing after Running() ensures messages are processed

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	var ackCalled atomic.Bool
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Return the test lease as pending so the batcher will call AcknowledgeLeases
			return []billingtypes.Lease{
				{Uuid: "lease-after", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			ackCalled.Store(true)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start manager in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Start(ctx)
	}()

	// Track a lease that will receive a callback
	manager.TrackInFlight("lease-early", "tenant-1", testItems("sku-1"), "test")

	// RACE CONDITION SCENARIO: Try to publish callback BEFORE Running()
	// In production, this is what happened when backends sent callbacks
	// before Watermill handlers were subscribed.
	//
	// Note: We can't reliably test the failure case because it depends on timing.
	// The Watermill gochannel pubsub may or may not have subscribers yet.
	// What we CAN test is that after Running(), callbacks always work.

	// Wait for Running() - this is the fix that ensures handlers are subscribed
	select {
	case <-manager.Running():
		// Good - handlers are now subscribed
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start within timeout")
	}

	// Track another lease for the "after Running()" test
	manager.TrackInFlight("lease-after", "tenant-1", testItems("sku-1"), "test")

	// AFTER Running(): Publish callback - this should always work
	callback := backend.CallbackPayload{
		LeaseUUID: "lease-after",
		Status:    backend.CallbackStatusSuccess,
	}

	// Use the manager's PublishCallback method which publishes to the internal topic
	require.NoError(t, manager.PublishCallback(callback), "failed to publish callback")

	// Wait for the full handler flow to complete: message → handler → batcher →
	// chain ack → handler untrack. We poll on the final observable state
	// (!IsInFlight) rather than an intermediate signal (ackCalled) to avoid
	// a race where the chain mock has been called but the handler hasn't yet
	// called UntrackInFlight.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !manager.IsInFlight("lease-after") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify lease was removed from in-flight (full handler flow completed)
	assert.False(t, manager.IsInFlight("lease-after"), "lease should not be in-flight after successful callback")

	// If the lease was untracked, the acknowledge must have been called
	assert.True(t, ackCalled.Load(), "REGRESSION: callback was not processed after Running() - handlers may not be subscribed")

	// Clean up
	cancel()
	manager.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Error("manager.Start() did not return after cancel")
	}
}

// TestManager_Close_NoPanicWithActiveHandler is a regression test for the
// shutdown ordering bug where Manager.Close() stopped the ack batcher before
// closing the Watermill router. If a handler was actively calling Acknowledge()
// during shutdown, the batcher's batchLoop would close b.requests, and the
// handler (or a Watermill retry of it) would panic with send-on-closed-channel.
//
// The fix ensures the Watermill router is closed first (draining all in-progress
// handlers) before stopping the ack batcher.
func TestManager_Close_NoPanicWithActiveHandler(t *testing.T) {
	ackReached := make(chan struct{}, 1) // signals AcknowledgeLeases is executing

	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			// Signal that the handler has reached the chain acknowledge call
			select {
			case ackReached <- struct{}{}:
			default:
			}
			// Simulate a slow chain response. Use a select so the call
			// respects context cancellation (as real chain calls do).
			select {
			case <-time.After(200 * time.Millisecond):
				return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:     "provider-1",
		CallbackBaseURL:  "http://localhost:8080",
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

	// Track lease and publish a success callback. The handler will call
	// Acknowledge(), which sends to the batcher's request channel.
	manager.TrackInFlight("lease-1", "tenant-1", testItems("sku-1"), "test")
	require.NoError(t, manager.PublishCallback(backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	}))

	// Wait for the handler to reach AcknowledgeLeases inside the batcher.
	select {
	case <-ackReached:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach AcknowledgeLeases")
	}

	// Call Close() while the handler is actively using the ack batcher.
	// Do NOT cancel the context first — this exercises the path where
	// Close() alone must safely drain handlers before stopping the batcher.
	//
	// With the old ordering (batcher stopped before router), the batcher's
	// context cancellation causes AcknowledgeLeases to return an error,
	// Watermill retries the handler, and the retried Acknowledge() call
	// sends to the now-closed b.requests channel, panicking.
	assert.NotPanics(t, func() {
		assert.NoError(t, manager.Close())
	})

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
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			callCount.Add(1)
			// Always fail to trigger retries
			return nil, errors.New("permanent chain failure")
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
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

func TestForwardToEventSink_ForwardsToSink(t *testing.T) {
	sink := &mockLeaseEventSink{}
	m := &Manager{leaseEventSink: sink}

	event := backend.LeaseStatusEvent{
		LeaseUUID: "lease-1",
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
	assert.Equal(t, "lease-1", sink.events[0].LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, sink.events[0].Status)
	assert.Empty(t, sink.events[0].Error)
	assert.Equal(t, time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC), sink.events[0].Timestamp)
}

func TestForwardToEventSink_IncludesError(t *testing.T) {
	sink := &mockLeaseEventSink{}
	m := &Manager{leaseEventSink: sink}

	event := backend.LeaseStatusEvent{
		LeaseUUID: "lease-2",
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
	assert.Equal(t, "lease-2", sink.events[0].LeaseUUID)
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
