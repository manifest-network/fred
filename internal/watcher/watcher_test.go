package watcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/testutil"
)

// mockChainClient implements ChainClient for testing.
type mockChainClient struct {
	getActiveLeasesByProviderFunc func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
}

func (m *mockChainClient) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	if m.getActiveLeasesByProviderFunc != nil {
		return m.getActiveLeasesByProviderFunc(ctx, providerUUID)
	}
	return nil, nil
}

func TestNew(t *testing.T) {
	providerUUID := testutil.ValidUUID1

	w := New(nil, nil, providerUUID)

	require.NotNil(t, w, "New() returned nil")
	assert.Equal(t, providerUUID, w.providerUUID)
	assert.NotNil(t, w.tenantLeaseCounts, "tenantLeaseCounts map is nil")
}

func TestAddActiveTenant(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	tenant := "manifest1abc"
	w.addActiveTenant(tenant)

	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 1, count, "addActiveTenant() count mismatch")

	// Add same tenant again - count should increment
	w.addActiveTenant(tenant)

	w.mu.Lock()
	count = w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 2, count, "addActiveTenant() second call count mismatch")
}

func TestAddActiveTenant_Empty(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	// Empty tenant should be ignored
	w.addActiveTenant("")

	w.mu.Lock()
	count := len(w.tenantLeaseCounts)
	w.mu.Unlock()

	assert.Equal(t, 0, count, "addActiveTenant(\"\") should not add empty tenant")
}

func TestAddActiveTenant_ThreadSafe(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	var wg sync.WaitGroup
	tenants := []string{
		"manifest1tenant1",
		"manifest1tenant2",
		"manifest1tenant3",
	}

	for _, tenant := range tenants {
		wg.Go(func() {
			w.addActiveTenant(tenant)
		})
	}

	wg.Wait()

	w.mu.Lock()
	count := len(w.tenantLeaseCounts)
	w.mu.Unlock()

	assert.Equal(t, len(tenants), count)
}

func TestHandleEvent_LeaseAcknowledged(t *testing.T) {
	providerUUID := testutil.ValidUUID1
	w := &Watcher{
		providerUUID:      providerUUID,
		tenantLeaseCounts: make(map[string]int),
	}

	tenant := "manifest1tenant"
	event := chain.LeaseEvent{
		Type:         chain.LeaseAcknowledged,
		LeaseUUID:    testutil.ValidUUID2,
		Tenant:       tenant,
		ProviderUUID: providerUUID, // Our provider
	}

	w.handleEvent(event)

	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 1, count)
}

func TestHandleEvent_LeaseAutoClosed_CrossProvider(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	otherProviderUUID := testutil.ValidUUID2
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:      ourProviderUUID,
		tenantLeaseCounts: map[string]int{tenant: 1},
		withdrawTrigger: func() {
			triggered = true
		},
	}

	// Event from another provider should trigger withdrawal
	event := chain.LeaseEvent{
		Type:         chain.LeaseAutoClosed,
		ProviderUUID: otherProviderUUID,
		Tenant:       tenant,
	}

	w.handleEvent(event)

	assert.True(t, triggered, "handleEvent(LeaseAutoClosed) from other provider should trigger withdrawal")
}

func TestHandleEvent_LeaseAutoClosed_OwnProvider(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:      ourProviderUUID,
		tenantLeaseCounts: map[string]int{tenant: 1},
		withdrawTrigger: func() {
			triggered = true
		},
	}

	// Event from our own provider should NOT trigger withdrawal but should decrement count
	event := chain.LeaseEvent{
		Type:         chain.LeaseAutoClosed,
		ProviderUUID: ourProviderUUID,
		Tenant:       tenant,
	}

	w.handleEvent(event)

	assert.False(t, triggered, "handleEvent(LeaseAutoClosed) from own provider should NOT trigger withdrawal")

	// Verify tenant was removed (count was 1, now should be 0/deleted)
	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 0, count, "handleEvent(LeaseAutoClosed) from own provider should remove tenant")
}

func TestHandleEvent_LeaseAutoClosed_NoActiveLease(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	otherProviderUUID := testutil.ValidUUID2
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:      ourProviderUUID,
		tenantLeaseCounts: make(map[string]int), // No active tenants
		withdrawTrigger: func() {
			triggered = true
		},
	}

	// Event for tenant we don't have should NOT trigger withdrawal
	event := chain.LeaseEvent{
		Type:         chain.LeaseAutoClosed,
		ProviderUUID: otherProviderUUID,
		Tenant:       tenant,
	}

	w.handleEvent(event)

	assert.False(t, triggered, "handleEvent(LeaseAutoClosed) for unknown tenant should NOT trigger withdrawal")
}

func TestSetWithdrawTrigger(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	assert.Nil(t, w.withdrawTrigger, "withdrawTrigger should be nil initially")

	called := false
	trigger := func() {
		called = true
	}

	w.SetWithdrawTrigger(trigger)

	assert.NotNil(t, w.withdrawTrigger, "SetWithdrawTrigger() should set withdrawTrigger")

	w.withdrawTrigger()
	assert.True(t, called, "withdrawTrigger was not called")
}

func TestHandleEvent_UnhandledEventTypes(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	// These event types should be silently ignored (no panic)
	eventTypes := []chain.LeaseEventType{
		chain.LeaseCreated,
		chain.LeaseRejected,
	}

	for _, eventType := range eventTypes {
		event := chain.LeaseEvent{
			Type:      eventType,
			LeaseUUID: testutil.ValidUUID1,
		}
		// Should not panic
		w.handleEvent(event)
	}
}

func TestRemoveActiveTenant(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	tenant := "manifest1abc"

	// Add tenant with 2 leases
	w.addActiveTenant(tenant)
	w.addActiveTenant(tenant)

	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 2, count, "initial count mismatch")

	// Remove one lease
	w.removeActiveTenant(tenant)

	w.mu.Lock()
	count = w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 1, count, "after first remove count mismatch")

	// Remove last lease - tenant should be deleted
	w.removeActiveTenant(tenant)

	w.mu.Lock()
	_, exists := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.False(t, exists, "tenant should be removed when count reaches 0")
}

func TestRemoveActiveTenant_Empty(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
	}

	// Removing empty tenant should not panic
	w.removeActiveTenant("")

	// Removing non-existent tenant should not panic
	w.removeActiveTenant("nonexistent")
}

func TestHandleEvent_LeaseClosed(t *testing.T) {
	providerUUID := testutil.ValidUUID1
	tenant := "manifest1tenant"

	w := &Watcher{
		providerUUID:      providerUUID,
		tenantLeaseCounts: map[string]int{tenant: 2},
	}

	// Close one lease from our provider
	event := chain.LeaseEvent{
		Type:         chain.LeaseClosed,
		LeaseUUID:    testutil.ValidUUID2,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
	}

	w.handleEvent(event)

	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 1, count)
}

func TestHandleEvent_LeaseExpired(t *testing.T) {
	providerUUID := testutil.ValidUUID1
	tenant := "manifest1tenant"

	w := &Watcher{
		providerUUID:      providerUUID,
		tenantLeaseCounts: map[string]int{tenant: 1},
	}

	// Expire lease from our provider
	event := chain.LeaseEvent{
		Type:         chain.LeaseExpired,
		LeaseUUID:    testutil.ValidUUID2,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
	}

	w.handleEvent(event)

	w.mu.Lock()
	_, exists := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.False(t, exists, "handleEvent(LeaseExpired) should remove tenant when last lease expires")
}

func TestHandleEvent_IgnoresOtherProviderLeaseClose(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	otherProviderUUID := testutil.ValidUUID2
	tenant := "manifest1tenant"

	w := &Watcher{
		providerUUID:      ourProviderUUID,
		tenantLeaseCounts: map[string]int{tenant: 1},
	}

	// Close event from other provider should be ignored
	event := chain.LeaseEvent{
		Type:         chain.LeaseClosed,
		LeaseUUID:    testutil.ValidUUID3,
		Tenant:       tenant,
		ProviderUUID: otherProviderUUID,
	}

	w.handleEvent(event)

	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	assert.Equal(t, 1, count, "handleEvent(LeaseClosed) from other provider should not decrement")
}

// TestLoadActiveTenants tests the initial tenant loading from chain.
func TestLoadActiveTenants(t *testing.T) {
	providerUUID := testutil.ValidUUID1

	t.Run("loads_tenants_from_chain", func(t *testing.T) {
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				assert.Equal(t, providerUUID, uuid, "GetActiveLeasesByProvider called with wrong UUID")
				return []billingtypes.Lease{
					{Tenant: "tenant1"},
					{Tenant: "tenant1"}, // Same tenant, 2 leases
					{Tenant: "tenant2"},
				}, nil
			},
		}

		w := New(client, nil, providerUUID)

		err := w.loadActiveTenants(context.Background())
		require.NoError(t, err)

		w.mu.Lock()
		defer w.mu.Unlock()

		assert.Equal(t, 2, w.tenantLeaseCounts["tenant1"])
		assert.Equal(t, 1, w.tenantLeaseCounts["tenant2"])
	})

	t.Run("handles_empty_leases", func(t *testing.T) {
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}

		w := New(client, nil, providerUUID)

		err := w.loadActiveTenants(context.Background())
		require.NoError(t, err)

		w.mu.Lock()
		count := len(w.tenantLeaseCounts)
		w.mu.Unlock()

		assert.Equal(t, 0, count)
	})

	t.Run("returns_error_on_chain_failure", func(t *testing.T) {
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		w := New(client, nil, providerUUID)

		err := w.loadActiveTenants(context.Background())
		assert.Error(t, err, "loadActiveTenants() should return error on chain failure")
	})
}

// TestWatcher_Start tests the event processing loop.
func TestWatcher_Start(t *testing.T) {
	providerUUID := testutil.ValidUUID1

	// Helper to create event subscriber for tests
	newTestEventSubscriber := func(t *testing.T) *chain.EventSubscriber {
		t.Helper()
		eventSub, err := chain.NewEventSubscriber(chain.EventSubscriberConfig{
			URL:          "ws://localhost:26657/websocket",
			ProviderUUID: providerUUID,
		})
		require.NoError(t, err)
		return eventSub
	}

	t.Run("returns_on_context_cancellation", func(t *testing.T) {
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}

		eventSub := newTestEventSubscriber(t)
		w := New(client, eventSub, providerUUID)

		ctx, cancel := context.WithCancel(context.Background())

		// Start watcher in goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- w.Start(ctx)
		}()

		// Give it a moment to start and subscribe
		time.Sleep(10 * time.Millisecond)

		// Cancel context
		cancel()

		// Wait for Start to return
		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Error("Start() did not return after context cancellation")
		}
	})

	t.Run("continues_on_load_failure", func(t *testing.T) {
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		eventSub := newTestEventSubscriber(t)
		w := New(client, eventSub, providerUUID)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- w.Start(ctx)
		}()

		// Give it time to fail loading but continue
		time.Sleep(10 * time.Millisecond)

		cancel()

		select {
		case err := <-errCh:
			// Should return context.Canceled, not the load error
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Error("Start() did not return")
		}
	})

	t.Run("returns_nil_on_channel_close", func(t *testing.T) {
		// Test that Start returns nil when the event channel is closed
		// (as opposed to context.Canceled when context is cancelled)
		client := &mockChainClient{
			getActiveLeasesByProviderFunc: func(ctx context.Context, uuid string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}

		eventSub := newTestEventSubscriber(t)
		w := New(client, eventSub, providerUUID)

		// Use a non-cancellable context - we want to test channel close, not context cancellation
		ctx := context.Background()

		errCh := make(chan error, 1)
		go func() {
			errCh <- w.Start(ctx)
		}()

		// Give it time to start and subscribe
		time.Sleep(10 * time.Millisecond)

		// Close the EventSubscriber - this closes all subscriber channels
		eventSub.Close()

		select {
		case err := <-errCh:
			// Should return nil (not context.Canceled) when channel is closed
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("Start() did not return after channel close")
		}
	})
}
