package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// mockAcknowledger implements Acknowledger for testing.
type mockAcknowledger struct {
	acknowledgeFn func(ctx context.Context, leaseUUID string) (bool, string, error)
}

func (m *mockAcknowledger) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	if m.acknowledgeFn != nil {
		return m.acknowledgeFn(ctx, leaseUUID)
	}
	return true, "tx-hash", nil
}

// mockPlacementStore implements PlacementStore for testing.
type mockPlacementStore struct {
	mu         sync.Mutex
	placements map[string]string
}

func (m *mockPlacementStore) Get(leaseUUID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		return ""
	}
	return m.placements[leaseUUID]
}

func (m *mockPlacementStore) Set(leaseUUID, backendName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	m.placements[leaseUUID] = backendName
	return nil
}

func (m *mockPlacementStore) Delete(leaseUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.placements, leaseUUID)
}

func (m *mockPlacementStore) SetBatch(placements map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	for k, v := range placements {
		m.placements[k] = v
	}
	return nil
}

func (m *mockPlacementStore) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.placements)
}

func (m *mockPlacementStore) List() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	uuids := make([]string, 0, len(m.placements))
	for k := range m.placements {
		uuids = append(uuids, k)
	}
	return uuids
}

func (m *mockPlacementStore) Healthy() error { return nil }
func (m *mockPlacementStore) Close() error   { return nil }

// newTestHandlerSet creates a HandlerSet with mocked dependencies for testing.
func newTestHandlerSet(
	chainClient *chain.MockClient,
	mb *mockManagerBackend,
	ack *mockAcknowledger,
	payloadStore *payload.Store,
) (*HandlerSet, *DefaultInFlightTracker) {
	tracker := NewInFlightTracker()
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend {
			if mb != nil {
				return mb
			}
			return nil
		},
		getBackendByNameFn: func(name string) backend.Backend {
			if mb != nil && mb.name == name {
				return mb
			}
			return nil
		},
		backendsFn: func() []backend.Backend {
			if mb != nil {
				return []backend.Backend{mb}
			}
			return nil
		},
	}

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)
	hs := NewHandlerSet(HandlerDeps{
		ChainClient:  chainClient,
		Orchestrator: orch,
		Tracker:      tracker,
		Acknowledger: ack,
		PayloadStore: payloadStore,
	})
	return hs, tracker
}

// --- HandleLeaseCreated tests ---

func TestHandlerSet_HandleLeaseCreated_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-a",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)

	// Backend should have been called
	mb.mu.Lock()
	assert.Len(t, mb.provisionCalls, 1)
	mb.mu.Unlock()

	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleLeaseCreated_WithMetaHash_SkipsProvisioning(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls, "should not provision when MetaHash is set")
	mb.mu.Unlock()

	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleLeaseCreated_LeaseNotFound(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}

	hs, _ := newTestHandlerSet(mockChain, nil, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err, "should return nil for not-found lease")
}

func TestHandlerSet_HandleLeaseCreated_ChainError(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, errors.New("chain unavailable")
		},
	}

	hs, _ := newTestHandlerSet(mockChain, nil, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-1",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.Error(t, err, "should return error for retry")
}

func TestHandlerSet_HandleLeaseCreated_ValidationError_PublishesFailedEvent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{
		name:         "test-backend",
		provisionErr: fmt.Errorf("%w: %w: bad-sku", backend.ErrValidation, backend.ErrUnknownSKU),
	}
	rejectCalled := false
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-a",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{"lease-val"}, leaseUUIDs)
			assert.Equal(t, "invalid SKU", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	hs, _ := newTestHandlerSet(mockChain, mb, nil, nil)
	hs.deps.Publisher = pub

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "lease-val",
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled, "lease should be rejected on chain")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish exactly one failed event")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-val", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "invalid SKU", event.Error)
}

// --- HandleLeaseClosed tests ---

func TestHandlerSet_HandleLeaseClosed_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseClosed(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

func TestHandlerSet_HandleLeaseClosed_CleansUpPayload(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, mb, nil, ps)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: "lease-1",
	})

	err = hs.HandleLeaseClosed(msg)
	assert.NoError(t, err)
	hasPayload, err := ps.Has("lease-1")
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be cleaned up")
}

func TestHandlerSet_HandleLeaseExpired_DelegatesToClosed(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, _ := newTestHandlerSet(mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: "lease-1",
	})

	err := hs.HandleLeaseExpired(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

// --- HandleBackendCallback tests ---

func TestHandlerSet_HandleBackendCallback_Success(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx-abc", nil
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Should be untracked after successful ack
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Success_TerminalAckError(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", billingtypes.ErrLeaseNotPending
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "terminal ack error should be treated as success")
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Success_TerminalAckError_PublishesReadyEvent(t *testing.T) {
	pub := newMockPublisher()
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", billingtypes.ErrLeaseNotPending
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	hs.deps.Publisher = pub
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish ready event even on terminal ack error")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-1", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	assert.Empty(t, event.Error)
}

func TestHandlerSet_HandleBackendCallback_Success_TransientAckError(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", errors.New("chain timeout")
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")
	assert.ErrorIs(t, err, ErrAcknowledgeFailed)

	// Should still be in-flight for retry
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Failed_PendingLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	rejectCalled := false
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{"lease-1"}, leaseUUIDs)
			assert.Equal(t, "container crash", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "container crash",
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled)
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Failed_ActiveLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "re-provision failed",
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "active lease failure should not error")

	// Should be untracked so reconciler can pick it up
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Failed_RejectFails(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 0, nil, errors.New("chain error")
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "failed",
	})

	err := hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")

	// Should still be in-flight to prevent reconciler race
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandleBackendCallback_Failed_EmptyReason(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	var receivedReason string
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			receivedReason = reason
			return 1, nil, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "", // Empty
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.Equal(t, "provisioning failed", receivedReason, "should use default reason")
}

func TestHandlerSet_HandleBackendCallback_UnknownLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, _ := newTestHandlerSet(mockChain, mb, ack, nil)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "unknown-lease",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "should ignore callback for unknown lease")
}

// TestHandlerSet_HandleBackendCallback_NonInFlight_PublishesEvent verifies that
// callbacks for non-in-flight leases (restart/update completions) publish a
// status event so WebSocket clients see the ready/failed transition.
func TestHandlerSet_HandleBackendCallback_NonInFlight_PublishesEvent(t *testing.T) {
	pub := newMockPublisher()

	hs := NewHandlerSet(HandlerDeps{
		Tracker:   NewInFlightTracker(),
		Publisher: pub,
	})

	t.Run("success_publishes_ready", func(t *testing.T) {
		pub.mu.Lock()
		pub.published = make(map[string][]*message.Message)
		pub.mu.Unlock()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID: "lease-restart",
			Status:    backend.CallbackStatusSuccess,
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		pub.mu.Lock()
		msgs := pub.published[TopicLeaseEvent]
		pub.mu.Unlock()
		require.Len(t, msgs, 1)

		var event backend.LeaseStatusEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "lease-restart", event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	})

	t.Run("failed_publishes_failed", func(t *testing.T) {
		pub.mu.Lock()
		pub.published = make(map[string][]*message.Message)
		pub.mu.Unlock()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID: "lease-update",
			Status:    backend.CallbackStatusFailed,
			Error:     "container crashed",
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		pub.mu.Lock()
		msgs := pub.published[TopicLeaseEvent]
		pub.mu.Unlock()
		require.Len(t, msgs, 1)

		var event backend.LeaseStatusEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "lease-update", event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
		assert.Equal(t, "container crashed", event.Error)
	})
}

func TestHandlerSet_HandleBackendCallback_UnknownStatus(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    "weird-status",
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Should be untracked to prevent being stuck
	assert.False(t, tracker.IsInFlight("lease-1"))
}

// --- HandlePayloadReceived tests ---

func TestHandlerSet_HandlePayloadReceived_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	payloadData := []byte(`{"image":"nginx:latest"}`)
	ps.Store("lease-1", payloadData)

	hs, tracker := newTestHandlerSet(mockChain, mb, nil, ps)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	require.Len(t, mb.provisionCalls, 1)
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Equal(t, payloadData, req.Payload)
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestHandlerSet_HandlePayloadReceived_Success_PublishesProvisioningEvent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	payloadData := []byte(`{"image":"nginx:latest"}`)
	ps.Store("lease-1", payloadData)

	hs, _ := newTestHandlerSet(mockChain, mb, nil, ps)
	hs.deps.Publisher = pub

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish provisioning event")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-1", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusProvisioning, event.Status)
	assert.Empty(t, event.Error)
}

func TestHandlerSet_HandlePayloadReceived_NilPayloadStore(t *testing.T) {
	mockChain := &chain.MockClient{}
	hs, _ := newTestHandlerSet(mockChain, nil, nil, nil)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: "lease-1",
	})

	err := hs.HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil when payload store is nil")
}

func TestHandlerSet_HandlePayloadReceived_LeaseNotFound(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: "lease-1",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)
	hasPayload, err := ps.Has("lease-1")
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be cleaned up")
}

func TestHandlerSet_HandlePayloadReceived_LeaseNotPending(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE, // Not pending
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: "lease-1",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)
	hasPayload, err := ps.Has("lease-1")
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be cleaned up")
}

func TestHandlerSet_HandlePayloadReceived_ChainError(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, errors.New("chain error")
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: "lease-1",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.Error(t, err, "should return error for retry")

	// Payload should be preserved for retry
	hasPayloadRetry, errRetry := ps.Has("lease-1")
	require.NoError(t, errRetry)
	assert.True(t, hasPayloadRetry)
}

func TestHandlerSet_HandlePayloadReceived_HashMismatch(t *testing.T) {
	rejected := false
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
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
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-1",
		MetaHashHex: "0000000000000000000000000000000000000000000000000000000000000000",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil after rejecting the lease")
	assert.True(t, rejected, "lease should be rejected on-chain")
	hasPayloadHash, errHash := ps.Has("lease-1")
	require.NoError(t, errHash)
	assert.False(t, hasPayloadHash, "payload should be deleted after successful rejection")
}

func TestHandlerSet_HandlePayloadReceived_ValidationError_PublishesFailedEvent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{
		name:         "test-backend",
		provisionErr: fmt.Errorf("%w: %w: evil.io/malware", backend.ErrValidation, backend.ErrImageNotAllowed),
	}
	rejectCalled := false
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{"lease-val"}, leaseUUIDs)
			assert.Equal(t, "image not allowed", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	payloadData := []byte(`{"image":"evil.io/malware"}`)
	ps.Store("lease-val", payloadData)

	hs, _ := newTestHandlerSet(mockChain, mb, nil, ps)
	hs.deps.Publisher = pub

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-val",
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled, "lease should be rejected on chain")

	// Payload should be cleaned up
	hasPayload, err := ps.Has("lease-val")
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be deleted after validation error")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish exactly one failed event")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-val", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "image not allowed", event.Error)
}

// --- truncateRejectReason tests ---

func TestTruncateRejectReason(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"short string unchanged", "short error", "short error"},
		{"empty string unchanged", "", ""},
		{"exactly 256 bytes unchanged", strings.Repeat("a", 256), strings.Repeat("a", 256)},
		{"257 bytes truncated", strings.Repeat("a", 257), strings.Repeat("a", 253) + "..."},
		{"500 bytes truncated", strings.Repeat("b", 500), strings.Repeat("b", 253) + "..."},
		// "é" is 2 bytes (0xC3 0xA9). 128 runes = 256 bytes fits exactly.
		{"multibyte exactly at limit", strings.Repeat("\u00e9", 128), strings.Repeat("\u00e9", 128)},
		// 129 "é" = 258 bytes > 256. Truncated: must back up to rune boundary.
		// 253 bytes / 2 = 126 full runes (252 bytes) + "..." (3 bytes) = 255 bytes.
		{"multibyte over limit", strings.Repeat("\u00e9", 129), strings.Repeat("\u00e9", 126) + "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateRejectReason(tt.input)
			assert.Equal(t, tt.expect, result)
			assert.LessOrEqual(t, len(result), maxRejectReasonLen, "must fit on-chain byte limit")
			assert.True(t, utf8.ValidString(result), "result should be valid UTF-8")
		})
	}
}

func TestValidationErrorToRejectReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		// Direct backend errors (docker backend path)
		{"unknown SKU direct", fmt.Errorf("%w: gpu-xl (profile: gpu-xl)", backend.ErrUnknownSKU), rejectReasonInvalidSKU},
		{"invalid manifest direct", fmt.Errorf("%w: %w", backend.ErrInvalidManifest, errors.New("unexpected end of JSON input")), rejectReasonInvalidManifest},
		{"image not allowed direct", fmt.Errorf("%w: registry %q; allowed registries: %v", backend.ErrImageNotAllowed, "evil.io", []string{"docker.io"}), rejectReasonImageNotAllowed},
		// Nested wrapping (e.g., docker backend wraps config error which wraps sentinel)
		{"unknown SKU nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: bad-sku", backend.ErrUnknownSKU)), rejectReasonInvalidSKU},
		{"invalid manifest nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: bad yaml", backend.ErrInvalidManifest)), rejectReasonInvalidManifest},
		{"image not allowed nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: evil.io/malware", backend.ErrImageNotAllowed)), rejectReasonImageNotAllowed},
		// Catch-all
		{"unknown error", fmt.Errorf("%w: something unexpected", backend.ErrValidation), rejectReasonValidationError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validationErrorToRejectReason(tt.err))
		})
	}
}

func TestHandlerSet_HandleBackendCallback_LongReasonTruncated(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	var receivedReason string
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			receivedReason = reason
			return 1, nil, nil
		},
	}

	hs, tracker := newTestHandlerSet(mockChain, mb, ack, nil)
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	longReason := strings.Repeat("x", 500)
	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     longReason,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.LessOrEqual(t, len(receivedReason), maxRejectReasonLen,
		"rejection reason should be truncated to fit on-chain limit")
	assert.True(t, strings.HasSuffix(receivedReason, "..."),
		"truncated reason should end with ellipsis")
}

// --- isTerminalAcknowledgeError tests ---

func TestIsTerminalAcknowledgeError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"nil", nil, false},
		{"generic error", errors.New("timeout"), false},
		{"ErrLeaseNotPending", billingtypes.ErrLeaseNotPending, true},
		{"ErrLeaseNotFound", billingtypes.ErrLeaseNotFound, true},
		{"wrapped ErrLeaseNotPending", fmt.Errorf("wrapped: %w", billingtypes.ErrLeaseNotPending), true},
		{"wrapped ErrLeaseNotFound", fmt.Errorf("wrapped: %w", billingtypes.ErrLeaseNotFound), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.terminal, isTerminalAcknowledgeError(tt.err))
		})
	}
}

// placementTestFixture holds the shared setup for placement-related callback tests.
type placementTestFixture struct {
	hs      *HandlerSet
	tracker *DefaultInFlightTracker
	ps      *mockPlacementStore
	mb      *mockManagerBackend
}

// newPlacementTestFixture creates a HandlerSet wired with a mockPlacementStore.
func newPlacementTestFixture(chainClient *chain.MockClient, ack *mockAcknowledger) placementTestFixture {
	mb := &mockManagerBackend{name: "test-backend"}
	ps := &mockPlacementStore{}
	tracker := NewInFlightTracker()
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
		getBackendByNameFn: func(name string) backend.Backend {
			if name == mb.name {
				return mb
			}
			return nil
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{mb} },
	}
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)
	hs := NewHandlerSet(HandlerDeps{
		ChainClient:  chainClient,
		Orchestrator: orch,
		Tracker:      tracker,
		Acknowledger: ack,
	})
	return placementTestFixture{hs: hs, tracker: tracker, ps: ps, mb: mb}
}

func TestHandlerSet_HandleBackendCallback_Failed_PendingLease_CleansUpPlacement(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 1, []string{"tx-rej"}, nil
		},
	}

	f := newPlacementTestFixture(mockChain, &mockAcknowledger{})
	f.ps.Set("lease-1", "test-backend")
	f.tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "container crash",
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.False(t, f.tracker.IsInFlight("lease-1"))
	assert.Empty(t, f.ps.Get("lease-1"), "placement should be deleted after rejection")
}

func TestHandlerSet_HandleBackendCallback_Failed_RejectFails_PreservesPlacement(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 0, nil, errors.New("chain error")
		},
	}

	f := newPlacementTestFixture(mockChain, &mockAcknowledger{})
	f.ps.Set("lease-1", "test-backend")
	f.tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "failed",
	})

	err := f.hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")

	// Placement must be preserved so the retry can still find the backend
	assert.True(t, f.tracker.IsInFlight("lease-1"), "should stay in-flight for retry")
	assert.Equal(t, "test-backend", f.ps.Get("lease-1"), "placement should be preserved when reject fails")
}

func TestHandlerSet_HandleBackendCallback_Failed_ActiveLease_PreservesPlacement(t *testing.T) {
	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
		},
	}

	f := newPlacementTestFixture(mockChain, &mockAcknowledger{})
	f.ps.Set("lease-1", "test-backend")
	f.tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusFailed,
		Error:     "re-provision failed",
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Placement must be preserved — reconciler needs it to find the backend
	assert.False(t, f.tracker.IsInFlight("lease-1"), "should be untracked for reconciler")
	assert.Equal(t, "test-backend", f.ps.Get("lease-1"), "placement should be preserved for active lease")
}

func TestHandlerSet_HandleBackendCallback_Success_PreservesPlacement(t *testing.T) {
	mockChain := &chain.MockClient{}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx-abc", nil
		},
	}

	f := newPlacementTestFixture(mockChain, ack)
	f.ps.Set("lease-1", "test-backend")
	f.tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-1",
		Status:    backend.CallbackStatusSuccess,
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Placement must be preserved — the lease is now ACTIVE and the container
	// could crash later, requiring reads/re-provision from the same backend.
	assert.False(t, f.tracker.IsInFlight("lease-1"))
	assert.Equal(t, "test-backend", f.ps.Get("lease-1"), "placement should be preserved after success")
}

// --- publishLeaseEvent tests ---

// mockPublisher implements message.Publisher for testing publishLeaseEvent.
type mockPublisher struct {
	mu         sync.Mutex
	published  map[string][]*message.Message // topic → messages
	publishErr error
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{published: make(map[string][]*message.Message)}
}

func (p *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	if p.publishErr != nil {
		return p.publishErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published[topic] = append(p.published[topic], messages...)
	return nil
}

func (p *mockPublisher) Close() error { return nil }

func TestPublishLeaseEvent_PublishesToTopic(t *testing.T) {
	pub := newMockPublisher()
	hs := NewHandlerSet(HandlerDeps{
		Publisher: pub,
	})

	hs.publishLeaseEvent("lease-1", backend.ProvisionStatusReady, "")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	require.Len(t, msgs, 1, "should publish exactly one message")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-1", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	assert.Empty(t, event.Error)
	assert.False(t, event.Timestamp.IsZero(), "timestamp should be set")
}

func TestPublishLeaseEvent_IncludesError(t *testing.T) {
	pub := newMockPublisher()
	hs := NewHandlerSet(HandlerDeps{
		Publisher: pub,
	})

	hs.publishLeaseEvent("lease-2", backend.ProvisionStatusFailed, "container crashed")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	require.Len(t, msgs, 1)

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, "lease-2", event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "container crashed", event.Error)
}

func TestPublishLeaseEvent_NilPublisher(t *testing.T) {
	hs := NewHandlerSet(HandlerDeps{
		Publisher: nil,
	})

	// Should not panic
	hs.publishLeaseEvent("lease-1", backend.ProvisionStatusReady, "")
}

func TestPublishLeaseEvent_PublishError(t *testing.T) {
	pub := newMockPublisher()
	pub.publishErr = errors.New("pubsub down")
	hs := NewHandlerSet(HandlerDeps{
		Publisher: pub,
	})

	// Should not panic — publish errors are logged, not propagated
	hs.publishLeaseEvent("lease-1", backend.ProvisionStatusReady, "")
}

// --- Metric tests ---

func TestHandlerSet_HandleBackendCallback_NonInFlight_IncrementsNonInFlightCallbacks(t *testing.T) {
	before := promtestutil.ToFloat64(metrics.NonInFlightCallbacksTotal)

	hs := NewHandlerSet(HandlerDeps{
		Tracker: NewInFlightTracker(),
	})

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: "lease-not-in-flight",
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	after := promtestutil.ToFloat64(metrics.NonInFlightCallbacksTotal)
	assert.Equal(t, 1.0, after-before, "NonInFlightCallbacksTotal should increment by 1")
}

// --- Helper ---

// newCallbackMsg creates a Watermill message from a CallbackPayload.
func newCallbackMsg(t *testing.T, payload backend.CallbackPayload) *message.Message {
	t.Helper()
	return newLeaseEventMsg_raw(t, payload)
}

// newLeaseEventMsg_raw creates a Watermill message from any JSON-serializable value.
func newLeaseEventMsg_raw(t *testing.T, v any) *message.Message {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return message.NewMessage(watermill.NewUUID(), data)
}
