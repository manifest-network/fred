package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

// startTestManager creates a Manager with the given mocks, starts it, and
// registers cleanup. Returns the running manager.
func startTestManager(t *testing.T, cfg ManagerConfig, mockBackend *mockManagerBackend, mockChain *chain.MockClient) *Manager {
	t.Helper()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	require.NoError(t, err)

	manager, err := NewManager(cfg, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = manager.Close()
	})

	go func() { _ = manager.Start(ctx) }()

	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for manager to start")
	}

	return manager
}

func TestIntegration_FullProvisionAcknowledge(t *testing.T) {
	const leaseUUID = "int-lease-1"
	const tenant = "int-tenant-1"
	const providerUUID = "int-provider-1"
	const skuUUID = "int-sku-1"

	// Channel-based notification for async provision call.
	provisionCh := make(chan backend.ProvisionRequest, 1)
	mockBackend := &mockManagerBackend{name: "test"}
	origProvision := mockBackend.Provision
	_ = origProvision // mockManagerBackend records calls; we layer on notification
	mockBackend.provisionErr = nil

	ackCh := make(chan []string, 1)
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chain.NewMockLeaseWithSKU(uuid, tenant, providerUUID,
				billingtypes.LEASE_STATE_PENDING, skuUUID), nil
		},
		GetPendingLeasesFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				*chain.NewMockLeaseWithSKU(leaseUUID, tenant, providerUUID,
					billingtypes.LEASE_STATE_PENDING, skuUUID),
			}, nil
		},
		AcknowledgeLeasesFunc: func(_ context.Context, uuids []string) (uint64, []string, error) {
			ackCh <- uuids
			return uint64(len(uuids)), []string{"tx-hash-1"}, nil
		},
	}

	manager := startTestManager(t, ManagerConfig{
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "http://localhost:8080",
		AckBatchInterval: 50 * time.Millisecond,
		AckBatchSize:     1,
	}, mockBackend, mockChain)

	// Intercept provision calls via polling (same package, direct field access).
	// We rely on the mockManagerBackend recording calls.

	// Step 1: Publish LeaseCreated event.
	err := manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	})
	require.NoError(t, err)

	// Step 2: Wait for provision call on the mock backend.
	require.Eventually(t, func() bool {
		mockBackend.mu.Lock()
		defer mockBackend.mu.Unlock()
		if len(mockBackend.provisionCalls) > 0 {
			provisionCh <- mockBackend.provisionCalls[0]
			return true
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "timeout waiting for Provision call")

	req := <-provisionCh
	assert.Equal(t, leaseUUID, req.LeaseUUID)
	assert.Equal(t, tenant, req.Tenant)
	assert.Equal(t, providerUUID, req.ProviderUUID)

	// Verify in-flight tracking.
	assert.True(t, manager.IsInFlight(leaseUUID))

	// Step 3: Publish success callback.
	err = manager.PublishCallback(backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    backend.CallbackStatusSuccess,
	})
	require.NoError(t, err)

	// Step 4: Wait for AcknowledgeLeases to be called.
	select {
	case acked := <-ackCh:
		assert.Contains(t, acked, leaseUUID)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for AcknowledgeLeases")
	}

	// Step 5: Verify tracker is cleared.
	require.Eventually(t, func() bool {
		return !manager.IsInFlight(leaseUUID)
	}, 2*time.Second, 20*time.Millisecond, "lease should no longer be in-flight after ack")
}

func TestIntegration_ProvisionFailure_RejectsLease(t *testing.T) {
	const leaseUUID = "int-fail-lease-1"
	const tenant = "int-fail-tenant-1"
	const providerUUID = "int-fail-provider-1"
	const skuUUID = "int-fail-sku-1"

	mockBackend := &mockManagerBackend{name: "test"}

	rejectCh := make(chan []string, 1)
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chain.NewMockLeaseWithSKU(uuid, tenant, providerUUID,
				billingtypes.LEASE_STATE_PENDING, skuUUID), nil
		},
		RejectLeasesFunc: func(_ context.Context, uuids []string, reason string) (uint64, []string, error) {
			rejectCh <- uuids
			return uint64(len(uuids)), []string{"tx-reject-1"}, nil
		},
	}

	manager := startTestManager(t, ManagerConfig{
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "http://localhost:8080",
		AckBatchInterval: 50 * time.Millisecond,
		AckBatchSize:     1,
	}, mockBackend, mockChain)

	// Publish LeaseCreated event.
	err := manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	})
	require.NoError(t, err)

	// Wait for provision call.
	require.Eventually(t, func() bool {
		mockBackend.mu.Lock()
		defer mockBackend.mu.Unlock()
		return len(mockBackend.provisionCalls) > 0
	}, 5*time.Second, 20*time.Millisecond)

	// Verify in-flight.
	assert.True(t, manager.IsInFlight(leaseUUID))

	// Publish failure callback.
	err = manager.PublishCallback(backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    backend.CallbackStatusFailed,
		Error:     "container crashed",
	})
	require.NoError(t, err)

	// Wait for RejectLeases call.
	select {
	case rejected := <-rejectCh:
		assert.Contains(t, rejected, leaseUUID)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for RejectLeases")
	}

	// Verify tracker cleared.
	require.Eventually(t, func() bool {
		return !manager.IsInFlight(leaseUUID)
	}, 2*time.Second, 20*time.Millisecond)
}

func TestIntegration_LeaseClosed_Deprovisions(t *testing.T) {
	const leaseUUID = "int-close-lease-1"
	const tenant = "int-close-tenant-1"
	const providerUUID = "int-close-provider-1"
	const skuUUID = "int-close-sku-1"

	mockBackend := &mockManagerBackend{name: "test"}

	ackCh := make(chan []string, 1)
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chain.NewMockLeaseWithSKU(uuid, tenant, providerUUID,
				billingtypes.LEASE_STATE_ACTIVE, skuUUID), nil
		},
		GetPendingLeasesFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				*chain.NewMockLeaseWithSKU(leaseUUID, tenant, providerUUID,
					billingtypes.LEASE_STATE_PENDING, skuUUID),
			}, nil
		},
		AcknowledgeLeasesFunc: func(_ context.Context, uuids []string) (uint64, []string, error) {
			ackCh <- uuids
			return uint64(len(uuids)), []string{"tx-ack"}, nil
		},
	}

	manager := startTestManager(t, ManagerConfig{
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "http://localhost:8080",
		AckBatchInterval: 50 * time.Millisecond,
		AckBatchSize:     1,
	}, mockBackend, mockChain)

	// First, simulate a successful provision+ack cycle so the lease is "active".
	// We use GetLease returning PENDING for the initial create flow.
	mockChain.GetLeaseFunc = func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
		return chain.NewMockLeaseWithSKU(uuid, tenant, providerUUID,
			billingtypes.LEASE_STATE_PENDING, skuUUID), nil
	}

	err := manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mockBackend.mu.Lock()
		defer mockBackend.mu.Unlock()
		return len(mockBackend.provisionCalls) > 0
	}, 5*time.Second, 20*time.Millisecond)

	err = manager.PublishCallback(backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    backend.CallbackStatusSuccess,
	})
	require.NoError(t, err)

	select {
	case <-ackCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	require.Eventually(t, func() bool {
		return !manager.IsInFlight(leaseUUID)
	}, 2*time.Second, 20*time.Millisecond)

	// Now switch GetLease to return ACTIVE for the close handler's lookup.
	mockChain.GetLeaseFunc = func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
		return chain.NewMockLeaseWithSKU(uuid, tenant, providerUUID,
			billingtypes.LEASE_STATE_ACTIVE, skuUUID), nil
	}

	// Step 2: Publish LeaseClosed.
	err = manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:         chain.LeaseClosed,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	})
	require.NoError(t, err)

	// Wait for Deprovision call.
	require.Eventually(t, func() bool {
		mockBackend.mu.Lock()
		defer mockBackend.mu.Unlock()
		return len(mockBackend.deprovisionCalls) > 0
	}, 5*time.Second, 20*time.Millisecond)

	mockBackend.mu.Lock()
	assert.Contains(t, mockBackend.deprovisionCalls, leaseUUID)
	mockBackend.mu.Unlock()
}

func TestIntegration_PayloadFlow(t *testing.T) {
	const leaseUUID = "int-payload-lease-1"
	const tenant = "int-payload-tenant-1"
	const providerUUID = "int-payload-provider-1"
	const skuUUID = "int-payload-sku-1"

	payloadData := []byte(`{"image":"busybox:latest"}`)
	payloadHash := sha256.Sum256(payloadData)
	payloadHashHex := hex.EncodeToString(payloadHash[:])

	mockBackend := &mockManagerBackend{name: "test"}

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         uuid,
				Tenant:       tenant,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				MetaHash:     payloadHash[:],
				Items: []billingtypes.LeaseItem{
					{SkuUuid: skuUUID, Quantity: 1},
				},
			}, nil
		},
		GetPendingLeasesFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(_ context.Context, uuids []string) (uint64, []string, error) {
			return uint64(len(uuids)), []string{"tx-ack"}, nil
		},
	}

	// Create payload store in temp dir.
	dbPath := filepath.Join(t.TempDir(), "payload-test.db")
	payloadStore, err := NewPayloadStore(PayloadStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	manager := startTestManager(t, ManagerConfig{
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:     payloadStore,
		AckBatchInterval: 50 * time.Millisecond,
		AckBatchSize:     1,
	}, mockBackend, mockChain)

	// Step 1: Publish LeaseCreated. Since MetaHash is set, no provision yet.
	err = manager.PublishLeaseEvent(chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
	})
	require.NoError(t, err)

	// Give the handler time to process but verify NO provision was called.
	time.Sleep(200 * time.Millisecond)
	mockBackend.mu.Lock()
	assert.Empty(t, mockBackend.provisionCalls, "provision should NOT be called before payload upload")
	mockBackend.mu.Unlock()

	// Step 2: Store payload and publish PayloadReceived event.
	stored := manager.StorePayload(leaseUUID, payloadData)
	assert.True(t, stored)

	err = manager.PublishPayload(PayloadEvent{
		LeaseUUID:   leaseUUID,
		Tenant:      tenant,
		MetaHashHex: payloadHashHex,
	})
	require.NoError(t, err)

	// Step 3: Wait for provision call WITH payload.
	require.Eventually(t, func() bool {
		mockBackend.mu.Lock()
		defer mockBackend.mu.Unlock()
		return len(mockBackend.provisionCalls) > 0
	}, 5*time.Second, 20*time.Millisecond, "timeout waiting for provision after payload")

	mockBackend.mu.Lock()
	req := mockBackend.provisionCalls[0]
	mockBackend.mu.Unlock()

	assert.Equal(t, leaseUUID, req.LeaseUUID)
	assert.Equal(t, payloadData, req.Payload, "provision request should include the payload")
	assert.Equal(t, payloadHashHex, req.PayloadHash, "provision request should include the payload hash")
}

