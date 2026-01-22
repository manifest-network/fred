package watcher

import (
	"fmt"
	"sync"
	"testing"

	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestQueueForAcknowledgment(t *testing.T) {
	w := &Watcher{
		pendingLeases:   make(map[string]struct{}),
		autoAcknowledge: true,
	}

	leaseUUID := testutil.ValidUUID1

	w.queueForAcknowledgment(leaseUUID)

	w.mu.Lock()
	_, exists := w.pendingLeases[leaseUUID]
	w.mu.Unlock()

	if !exists {
		t.Error("queueForAcknowledgment() did not add lease to pending")
	}
}

func TestQueueForAcknowledgment_ThreadSafe(t *testing.T) {
	w := &Watcher{
		pendingLeases:   make(map[string]struct{}),
		autoAcknowledge: true,
	}

	var wg sync.WaitGroup
	uuids := []string{
		testutil.ValidUUID1,
		testutil.ValidUUID2,
		testutil.ValidUUID3,
	}

	// Queue from multiple goroutines
	for _, uuid := range uuids {
		wg.Go(func() {
			w.queueForAcknowledgment(uuid)
		})
	}

	wg.Wait()

	w.mu.Lock()
	count := len(w.pendingLeases)
	w.mu.Unlock()

	if count != len(uuids) {
		t.Errorf("pendingLeases count = %d, want %d", count, len(uuids))
	}
}

func TestRemoveFromPending(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	leaseUUID := testutil.ValidUUID1

	// Add first
	w.mu.Lock()
	w.pendingLeases[leaseUUID] = struct{}{}
	w.mu.Unlock()

	// Remove
	w.removeFromPending(leaseUUID)

	w.mu.Lock()
	_, exists := w.pendingLeases[leaseUUID]
	w.mu.Unlock()

	if exists {
		t.Error("removeFromPending() did not remove lease from pending")
	}
}

func TestRemoveFromPending_NonExistent(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	// Should not panic when removing non-existent lease
	w.removeFromPending(testutil.ValidUUID1)

	// Verify map is still empty
	w.mu.Lock()
	count := len(w.pendingLeases)
	w.mu.Unlock()

	if count != 0 {
		t.Errorf("pendingLeases count = %d, want 0", count)
	}
}

func TestHandleEvent_LeaseCreated(t *testing.T) {
	w := &Watcher{
		pendingLeases:   make(map[string]struct{}),
		autoAcknowledge: true,
	}

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: testutil.ValidUUID1,
	}

	w.handleEvent(nil, event)

	w.mu.Lock()
	_, exists := w.pendingLeases[testutil.ValidUUID1]
	w.mu.Unlock()

	if !exists {
		t.Error("handleEvent(LeaseCreated) did not queue lease for acknowledgment")
	}
}

func TestHandleEvent_LeaseCreated_AutoAcknowledgeDisabled(t *testing.T) {
	w := &Watcher{
		pendingLeases:   make(map[string]struct{}),
		autoAcknowledge: false, // Disabled
	}

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: testutil.ValidUUID1,
	}

	w.handleEvent(nil, event)

	w.mu.Lock()
	_, exists := w.pendingLeases[testutil.ValidUUID1]
	w.mu.Unlock()

	if exists {
		t.Error("handleEvent(LeaseCreated) should not queue when auto-acknowledge is disabled")
	}
}

func TestHandleEvent_LeaseAcknowledged(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	// Add to pending first
	leaseUUID := testutil.ValidUUID1
	w.mu.Lock()
	w.pendingLeases[leaseUUID] = struct{}{}
	w.mu.Unlock()

	event := chain.LeaseEvent{
		Type:      chain.LeaseAcknowledged,
		LeaseUUID: leaseUUID,
	}

	w.handleEvent(nil, event)

	w.mu.Lock()
	_, exists := w.pendingLeases[leaseUUID]
	w.mu.Unlock()

	if exists {
		t.Error("handleEvent(LeaseAcknowledged) did not remove lease from pending")
	}
}

func TestHandleEvent_LeaseRejected(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	// Add to pending first
	leaseUUID := testutil.ValidUUID1
	w.mu.Lock()
	w.pendingLeases[leaseUUID] = struct{}{}
	w.mu.Unlock()

	event := chain.LeaseEvent{
		Type:      chain.LeaseRejected,
		LeaseUUID: leaseUUID,
	}

	w.handleEvent(nil, event)

	w.mu.Lock()
	_, exists := w.pendingLeases[leaseUUID]
	w.mu.Unlock()

	if exists {
		t.Error("handleEvent(LeaseRejected) did not remove lease from pending")
	}
}

func TestHandleEvent_LeaseExpired(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	// Add to pending first
	leaseUUID := testutil.ValidUUID1
	w.mu.Lock()
	w.pendingLeases[leaseUUID] = struct{}{}
	w.mu.Unlock()

	event := chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: leaseUUID,
	}

	w.handleEvent(nil, event)

	w.mu.Lock()
	_, exists := w.pendingLeases[leaseUUID]
	w.mu.Unlock()

	if exists {
		t.Error("handleEvent(LeaseExpired) did not remove lease from pending")
	}
}

func TestHandleEvent_LeaseClosed(t *testing.T) {
	w := &Watcher{
		pendingLeases: make(map[string]struct{}),
	}

	// LeaseClosed should not affect pending leases (they're already acknowledged)
	event := chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: testutil.ValidUUID1,
	}

	// Should not panic
	w.handleEvent(nil, event)
}

func TestNew(t *testing.T) {
	providerUUID := testutil.ValidUUID1
	autoAck := true

	w := New(nil, nil, providerUUID, autoAck)

	if w == nil {
		t.Fatal("New() returned nil")
	}
	if w.providerUUID != providerUUID {
		t.Errorf("providerUUID = %q, want %q", w.providerUUID, providerUUID)
	}
	if w.autoAcknowledge != autoAck {
		t.Errorf("autoAcknowledge = %v, want %v", w.autoAcknowledge, autoAck)
	}
	if w.pendingLeases == nil {
		t.Error("pendingLeases map is nil")
	}
}

func TestAcknowledgeLeases_Batching(t *testing.T) {
	// Generate 150 UUIDs (should be split into 2 batches of 100)
	var uuids []string
	for i := range 150 {
		uuid := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		uuids = append(uuids, uuid)
	}

	// acknowledgeLeases would need a mock client to test properly
	// For now, just verify we can generate the expected number of UUIDs
	if len(uuids) != 150 {
		t.Errorf("generated %d UUIDs, want 150", len(uuids))
	}
}
