package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// newLeaseEventMsg creates a Watermill message from a LeaseEvent.
func newLeaseEventMsg(t *testing.T, event chain.LeaseEvent) *message.Message {
	t.Helper()
	data, err := json.Marshal(event)
	require.NoError(t, err, "failed to marshal event")
	return message.NewMessage(watermill.NewUUID(), data)
}

// newPayloadEventMsg creates a Watermill message from a payload.Event.
func newPayloadEventMsg(t *testing.T, event payload.Event) *message.Message {
	t.Helper()
	data, err := json.Marshal(event)
	require.NoError(t, err, "failed to marshal event")
	return message.NewMessage(watermill.NewUUID(), data)
}

func TestUnmarshalMessagePayload_Valid(t *testing.T) {
	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	}
	payload, _ := json.Marshal(event)
	msg := message.NewMessage(watermill.NewUUID(), payload)

	result, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	require.True(t, ok, "unmarshalMessagePayload() ok = false, want true")
	assert.Equal(t, "lease-1", result.LeaseUUID, "LeaseUUID")
	assert.Equal(t, "tenant-1", result.Tenant, "Tenant")
}

func TestUnmarshalMessagePayload_Invalid(t *testing.T) {
	msg := message.NewMessage(watermill.NewUUID(), []byte("not json"))

	_, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	assert.False(t, ok, "unmarshalMessagePayload() ok = true for invalid JSON, want false")
}

func TestHandleLeaseCreated_WithMetaHash(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-1",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02, 0x03}, // Has meta hash
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	err = manager.handleLeaseCreated(msg)
	assert.NoError(t, err, "handleLeaseCreated()")

	// Should NOT have called provision (awaiting payload)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "expected 0 provision calls for lease with MetaHash")
}

func TestHandleLeaseCreated_LeaseNotFound(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	// Should return nil (no retry)
	err = manager.handleLeaseCreated(msg)
	assert.NoError(t, err, "handleLeaseCreated() for lease not found")
}

func TestHandleLeaseCreated_ChainError(t *testing.T) {
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	// Should return error (retry)
	err = manager.handleLeaseCreated(msg)
	assert.Error(t, err, "handleLeaseCreated() should return error for chain error")
}

func TestHandleLeaseClosed_RouteBySKU(t *testing.T) {
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
				Uuid:   leaseUUID,
				Tenant: "tenant-1",
				State:  billingtypes.LEASE_STATE_ACTIVE,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100"}},
			}, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	// NOT in-flight, lease found on chain -> deprovision via SKU routing
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	assert.NoError(t, err, "handleLeaseClosed()")

	// GPU backend should have received the deprovision call
	gpuBackend.mu.Lock()
	gpuCalls := len(gpuBackend.deprovisionCalls)
	gpuBackend.mu.Unlock()

	assert.Equal(t, 1, gpuCalls, "deprovision calls to GPU backend")

	// K8s backend should NOT have received any calls
	k8sBackend.mu.Lock()
	k8sCalls := len(k8sBackend.deprovisionCalls)
	k8sBackend.mu.Unlock()

	assert.Equal(t, 0, k8sCalls, "deprovision calls to K8s backend")
}

func TestHandleLeaseClosed_FallbackAllBackends(t *testing.T) {
	backend1 := &mockManagerBackend{name: "backend-1"}
	backend2 := &mockManagerBackend{name: "backend-2"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUs: []string{"a-1"}}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"b-1"}}, IsDefault: true},
		},
	})

	// Not in-flight, lease NOT on chain -> deprovision on all backends
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	assert.NoError(t, err, "handleLeaseClosed()")

	// Both backends should have received deprovision calls
	backend1.mu.Lock()
	b1Calls := len(backend1.deprovisionCalls)
	backend1.mu.Unlock()

	backend2.mu.Lock()
	b2Calls := len(backend2.deprovisionCalls)
	backend2.mu.Unlock()

	assert.Equal(t, 1, b1Calls, "deprovision calls to backend-1")
	assert.Equal(t, 1, b2Calls, "deprovision calls to backend-2")
}

func TestHandleLeaseClosed_AllBackendsFail(t *testing.T) {
	deprovErr := errors.New("deprovision failed")
	backend1 := &mockManagerBackend{name: "backend-1", deprovisionErr: deprovErr}
	backend2 := &mockManagerBackend{name: "backend-2", deprovisionErr: deprovErr}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUs: []string{"a-1"}}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"b-1"}}, IsDefault: true},
		},
	})

	// Not in-flight, lease NOT on chain -> fallback to all backends, all fail
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	require.Error(t, err, "handleLeaseClosed() should return error when all backends fail")
	assert.ErrorIs(t, err, ErrDeprovisionFailed, "handleLeaseClosed() error")
}

func TestHandleLeaseClosed_PayloadCleanup(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chaintest.MockClient{}

	tempDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err, "NewPayloadStore()")
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	// Store a payload for a lease, then track it as in-flight
	payloadStore.Store("lease-1", []byte("deployment data"))
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	assert.NoError(t, err, "handleLeaseClosed()")

	// Payload should be cleaned up
	hasP, errP := payloadStore.Has("lease-1")
	require.NoError(t, errP)
	assert.False(t, hasP, "payload should be deleted when lease closes")
}

func TestHandlePayloadReceived_HashMismatch(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	rejected := false
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-1",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02, 0x03},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejected = true
			assert.Equal(t, []string{"lease-1"}, leaseUUIDs)
			assert.Equal(t, "payload corrupted", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err, "NewPayloadStore()")
	defer payloadStore.Close()

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		PayloadStore:    payloadStore,
	}, router, mockChain)
	require.NoError(t, err, "NewManager()")

	// Store a payload
	testPayload := []byte("deployment manifest")
	payloadStore.Store("lease-1", testPayload)

	// Use a wrong hash (64 hex zeros)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: strings.Repeat("0", 64),
	})

	err = manager.handlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil after rejecting the lease")
	assert.True(t, rejected, "lease should be rejected on-chain")

	// Payload should be deleted after successful rejection
	hasP2, errP2 := payloadStore.Has("lease-1")
	require.NoError(t, errP2)
	assert.False(t, hasP2, "payload should be deleted after rejection")

	// Lease should not be in-flight
	assert.False(t, manager.IsInFlight("lease-1"), "lease should not be in-flight after hash mismatch")
}
