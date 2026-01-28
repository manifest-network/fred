package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

// newLeaseEventMsg creates a Watermill message from a LeaseEvent.
func newLeaseEventMsg(t *testing.T, event chain.LeaseEvent) *message.Message {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	return message.NewMessage(watermill.NewUUID(), payload)
}

// newPayloadEventMsg creates a Watermill message from a PayloadEvent.
func newPayloadEventMsg(t *testing.T, event PayloadEvent) *message.Message {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	return message.NewMessage(watermill.NewUUID(), payload)
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
	if !ok {
		t.Fatal("unmarshalMessagePayload() ok = false, want true")
	}
	if result.LeaseUUID != "lease-1" {
		t.Errorf("LeaseUUID = %q, want %q", result.LeaseUUID, "lease-1")
	}
	if result.Tenant != "tenant-1" {
		t.Errorf("Tenant = %q, want %q", result.Tenant, "tenant-1")
	}
}

func TestUnmarshalMessagePayload_Invalid(t *testing.T) {
	msg := message.NewMessage(watermill.NewUUID(), []byte("not json"))

	_, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	if ok {
		t.Error("unmarshalMessagePayload() ok = true for invalid JSON, want false")
	}
}

func TestHandleLeaseCreated_WithMetaHash(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
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
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	err = manager.handleLeaseCreated(msg)
	if err != nil {
		t.Errorf("handleLeaseCreated() error = %v, want nil", err)
	}

	// Should NOT have called provision (awaiting payload)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	if len(mockBackend.provisionCalls) != 0 {
		t.Errorf("expected 0 provision calls for lease with MetaHash, got %d", len(mockBackend.provisionCalls))
	}
}

func TestHandleLeaseCreated_LeaseNotFound(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	// Should return nil (no retry)
	err = manager.handleLeaseCreated(msg)
	if err != nil {
		t.Errorf("handleLeaseCreated() error = %v, want nil for lease not found", err)
	}
}

func TestHandleLeaseCreated_ChainError(t *testing.T) {
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

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	// Should return error (retry)
	err = manager.handleLeaseCreated(msg)
	if err == nil {
		t.Error("handleLeaseCreated() error = nil, want error for chain error")
	}
}

func TestHandleLeaseClosed_RouteBySKU(t *testing.T) {
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
				State:  billingtypes.LEASE_STATE_ACTIVE,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100"}},
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

	// NOT in-flight, lease found on chain -> deprovision via SKU routing
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	if err != nil {
		t.Errorf("handleLeaseClosed() error = %v", err)
	}

	// GPU backend should have received the deprovision call
	gpuBackend.mu.Lock()
	gpuCalls := len(gpuBackend.deprovisionCalls)
	gpuBackend.mu.Unlock()

	if gpuCalls != 1 {
		t.Errorf("expected 1 deprovision call to GPU backend, got %d", gpuCalls)
	}

	// K8s backend should NOT have received any calls
	k8sBackend.mu.Lock()
	k8sCalls := len(k8sBackend.deprovisionCalls)
	k8sBackend.mu.Unlock()

	if k8sCalls != 0 {
		t.Errorf("expected 0 deprovision calls to K8s backend, got %d", k8sCalls)
	}
}

func TestHandleLeaseClosed_FallbackAllBackends(t *testing.T) {
	backend1 := &mockManagerBackend{name: "backend-1"}
	backend2 := &mockManagerBackend{name: "backend-2"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUPrefix: "a-"}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUPrefix: "b-"}, IsDefault: true},
		},
	})

	// Not in-flight, lease NOT on chain -> deprovision on all backends
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	if err != nil {
		t.Errorf("handleLeaseClosed() error = %v", err)
	}

	// Both backends should have received deprovision calls
	backend1.mu.Lock()
	b1Calls := len(backend1.deprovisionCalls)
	backend1.mu.Unlock()

	backend2.mu.Lock()
	b2Calls := len(backend2.deprovisionCalls)
	backend2.mu.Unlock()

	if b1Calls != 1 {
		t.Errorf("expected 1 deprovision call to backend-1, got %d", b1Calls)
	}
	if b2Calls != 1 {
		t.Errorf("expected 1 deprovision call to backend-2, got %d", b2Calls)
	}
}

func TestHandleLeaseClosed_AllBackendsFail(t *testing.T) {
	deprovErr := errors.New("deprovision failed")
	backend1 := &mockManagerBackend{name: "backend-1", deprovisionErr: deprovErr}
	backend2 := &mockManagerBackend{name: "backend-2", deprovisionErr: deprovErr}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUPrefix: "a-"}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUPrefix: "b-"}, IsDefault: true},
		},
	})

	// Not in-flight, lease NOT on chain -> fallback to all backends, all fail
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}

	manager, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	if err == nil {
		t.Error("handleLeaseClosed() error = nil, want error when all backends fail")
	}
	if !errors.Is(err, ErrDeprovisionFailed) {
		t.Errorf("handleLeaseClosed() error = %v, want ErrDeprovisionFailed", err)
	}
}

func TestHandleLeaseClosed_PayloadCleanup(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{}

	tempDir := t.TempDir()
	payloadStore, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
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

	// Store a payload for a lease, then track it as in-flight
	payloadStore.Store("lease-1", []byte("deployment data"))
	manager.TrackInFlight("lease-1", "tenant-1", "", "test")

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = manager.handleLeaseClosed(msg)
	if err != nil {
		t.Errorf("handleLeaseClosed() error = %v", err)
	}

	// Payload should be cleaned up
	if payloadStore.Has("lease-1") {
		t.Error("payload should be deleted when lease closes")
	}
}

func TestHandlePayloadReceived_HashMismatch(t *testing.T) {
	mockBackend := &mockManagerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-1",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02, 0x03},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	payloadStore, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
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

	// Store a payload
	testPayload := []byte("deployment manifest")
	payloadStore.Store("lease-1", testPayload)

	// Use a wrong hash (64 hex zeros)
	msg := newPayloadEventMsg(t, PayloadEvent{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-1",
		MetaHashHex: strings.Repeat("0", 64),
	})

	err = manager.handlePayloadReceived(msg)
	if err == nil {
		t.Error("handlePayloadReceived() error = nil, want error for hash mismatch")
	}

	// Payload should be deleted on hash mismatch
	if payloadStore.Has("lease-1") {
		t.Error("payload should be deleted on hash mismatch")
	}

	// Lease should not be in-flight
	if manager.IsInFlight("lease-1") {
		t.Error("lease should not be in-flight after hash mismatch")
	}
}
