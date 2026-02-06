package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker)
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

	ps.Store("lease-1", []byte("data"))

	hs, _ := newTestHandlerSet(mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   "lease-1",
		MetaHashHex: "0000000000000000000000000000000000000000000000000000000000000000",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.Error(t, err, "should return error for hash mismatch")
	hasPayloadHash, errHash := ps.Has("lease-1")
	require.NoError(t, errHash)
	assert.False(t, hasPayloadHash, "payload should be deleted on hash mismatch")
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
		{"exactly 256 chars unchanged", strings.Repeat("a", 256), strings.Repeat("a", 256)},
		{"257 chars truncated", strings.Repeat("a", 257), strings.Repeat("a", 253) + "..."},
		{"500 chars truncated", strings.Repeat("b", 500), strings.Repeat("b", 253) + "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateRejectReason(tt.input)
			assert.Equal(t, tt.expect, result)
			assert.LessOrEqual(t, len(result), maxRejectReasonLen)
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
		{"unknown SKU direct", fmt.Errorf("%w: unknown SKU: gpu-xl (profile: gpu-xl)", backend.ErrValidation), rejectReasonInvalidSKU},
		{"invalid manifest direct", fmt.Errorf("%w: invalid manifest: %w", backend.ErrValidation, errors.New("invalid manifest JSON: unexpected end of JSON input")), rejectReasonInvalidManifest},
		{"image not allowed direct", fmt.Errorf("%w: image from registry %q is not allowed; allowed registries: %v", backend.ErrValidation, "evil.io", []string{"docker.io"}), rejectReasonImageNotAllowed},
		// HTTP client errors (wraps JSON body from remote backend)
		{"unknown SKU HTTP", fmt.Errorf("%w: {\"error\":\"unknown SKU: invalid-sku\"}", backend.ErrValidation), rejectReasonInvalidSKU},
		{"invalid manifest HTTP", fmt.Errorf("%w: {\"error\":\"invalid manifest: bad yaml\"}", backend.ErrValidation), rejectReasonInvalidManifest},
		{"image not allowed HTTP", fmt.Errorf("%w: {\"error\":\"image not allowed: evil.io/malware\"}", backend.ErrValidation), rejectReasonImageNotAllowed},
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
