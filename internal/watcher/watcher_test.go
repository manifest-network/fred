package watcher

import (
	"sync"
	"testing"

	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestNew(t *testing.T) {
	providerUUID := testutil.ValidUUID1

	w := New(nil, nil, providerUUID)

	if w == nil {
		t.Fatal("New() returned nil")
	}
	if w.providerUUID != providerUUID {
		t.Errorf("providerUUID = %q, want %q", w.providerUUID, providerUUID)
	}
	if w.activeTenants == nil {
		t.Error("activeTenants map is nil")
	}
}

func TestAddActiveTenant(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
	}

	tenant := "manifest1abc"
	w.addActiveTenant(tenant)

	w.mu.Lock()
	_, exists := w.activeTenants[tenant]
	w.mu.Unlock()

	if !exists {
		t.Error("addActiveTenant() did not add tenant to active set")
	}
}

func TestAddActiveTenant_Empty(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
	}

	// Empty tenant should be ignored
	w.addActiveTenant("")

	w.mu.Lock()
	count := len(w.activeTenants)
	w.mu.Unlock()

	if count != 0 {
		t.Errorf("addActiveTenant(\"\") should not add empty tenant, got count %d", count)
	}
}

func TestAddActiveTenant_ThreadSafe(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
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
	count := len(w.activeTenants)
	w.mu.Unlock()

	if count != len(tenants) {
		t.Errorf("activeTenants count = %d, want %d", count, len(tenants))
	}
}

func TestHandleEvent_LeaseAcknowledged(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
	}

	tenant := "manifest1tenant"
	event := chain.LeaseEvent{
		Type:      chain.LeaseAcknowledged,
		LeaseUUID: testutil.ValidUUID1,
		Tenant:    tenant,
	}

	w.handleEvent(event)

	w.mu.Lock()
	_, exists := w.activeTenants[tenant]
	w.mu.Unlock()

	if !exists {
		t.Error("handleEvent(LeaseAcknowledged) did not add tenant to active set")
	}
}

func TestHandleEvent_LeaseAutoClosed_CrossProvider(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	otherProviderUUID := testutil.ValidUUID2
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:  ourProviderUUID,
		activeTenants: map[string]struct{}{tenant: {}},
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

	if !triggered {
		t.Error("handleEvent(LeaseAutoClosed) from other provider should trigger withdrawal")
	}
}

func TestHandleEvent_LeaseAutoClosed_OwnProvider(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:  ourProviderUUID,
		activeTenants: map[string]struct{}{tenant: {}},
		withdrawTrigger: func() {
			triggered = true
		},
	}

	// Event from our own provider should NOT trigger withdrawal
	event := chain.LeaseEvent{
		Type:         chain.LeaseAutoClosed,
		ProviderUUID: ourProviderUUID,
		Tenant:       tenant,
	}

	w.handleEvent(event)

	if triggered {
		t.Error("handleEvent(LeaseAutoClosed) from own provider should NOT trigger withdrawal")
	}
}

func TestHandleEvent_LeaseAutoClosed_NoActiveLease(t *testing.T) {
	ourProviderUUID := testutil.ValidUUID1
	otherProviderUUID := testutil.ValidUUID2
	tenant := "manifest1tenant"

	triggered := false
	w := &Watcher{
		providerUUID:  ourProviderUUID,
		activeTenants: make(map[string]struct{}), // No active tenants
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

	if triggered {
		t.Error("handleEvent(LeaseAutoClosed) for unknown tenant should NOT trigger withdrawal")
	}
}

func TestSetWithdrawTrigger(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
	}

	if w.withdrawTrigger != nil {
		t.Error("withdrawTrigger should be nil initially")
	}

	called := false
	trigger := func() {
		called = true
	}

	w.SetWithdrawTrigger(trigger)

	if w.withdrawTrigger == nil {
		t.Error("SetWithdrawTrigger() should set withdrawTrigger")
	}

	w.withdrawTrigger()
	if !called {
		t.Error("withdrawTrigger was not called")
	}
}

func TestHandleEvent_UnhandledEventTypes(t *testing.T) {
	w := &Watcher{
		activeTenants: make(map[string]struct{}),
	}

	// These event types should be silently ignored (no panic)
	eventTypes := []chain.LeaseEventType{
		chain.LeaseCreated,
		chain.LeaseRejected,
		chain.LeaseClosed,
		chain.LeaseExpired,
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
