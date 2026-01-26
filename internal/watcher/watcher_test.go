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
	if w.tenantLeaseCounts == nil {
		t.Error("tenantLeaseCounts map is nil")
	}
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

	if count != 1 {
		t.Errorf("addActiveTenant() count = %d, want 1", count)
	}

	// Add same tenant again - count should increment
	w.addActiveTenant(tenant)

	w.mu.Lock()
	count = w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	if count != 2 {
		t.Errorf("addActiveTenant() second call count = %d, want 2", count)
	}
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

	if count != 0 {
		t.Errorf("addActiveTenant(\"\") should not add empty tenant, got count %d", count)
	}
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

	if count != len(tenants) {
		t.Errorf("tenantLeaseCounts count = %d, want %d", count, len(tenants))
	}
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

	if count != 1 {
		t.Errorf("handleEvent(LeaseAcknowledged) count = %d, want 1", count)
	}
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

	if !triggered {
		t.Error("handleEvent(LeaseAutoClosed) from other provider should trigger withdrawal")
	}
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

	if triggered {
		t.Error("handleEvent(LeaseAutoClosed) from own provider should NOT trigger withdrawal")
	}

	// Verify tenant was removed (count was 1, now should be 0/deleted)
	w.mu.Lock()
	count := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	if count != 0 {
		t.Errorf("handleEvent(LeaseAutoClosed) from own provider should remove tenant, got count %d", count)
	}
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

	if triggered {
		t.Error("handleEvent(LeaseAutoClosed) for unknown tenant should NOT trigger withdrawal")
	}
}

func TestSetWithdrawTrigger(t *testing.T) {
	w := &Watcher{
		tenantLeaseCounts: make(map[string]int),
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

	if count != 2 {
		t.Errorf("initial count = %d, want 2", count)
	}

	// Remove one lease
	w.removeActiveTenant(tenant)

	w.mu.Lock()
	count = w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	if count != 1 {
		t.Errorf("after first remove count = %d, want 1", count)
	}

	// Remove last lease - tenant should be deleted
	w.removeActiveTenant(tenant)

	w.mu.Lock()
	_, exists := w.tenantLeaseCounts[tenant]
	w.mu.Unlock()

	if exists {
		t.Error("tenant should be removed when count reaches 0")
	}
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

	if count != 1 {
		t.Errorf("handleEvent(LeaseClosed) count = %d, want 1", count)
	}
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

	if exists {
		t.Error("handleEvent(LeaseExpired) should remove tenant when last lease expires")
	}
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

	if count != 1 {
		t.Errorf("handleEvent(LeaseClosed) from other provider should not decrement, got count %d", count)
	}
}
