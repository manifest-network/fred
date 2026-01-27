package provisioner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// mockAckChainClient implements ChainClient for ack batcher tests
type mockAckChainClient struct {
	acknowledgeFunc      func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	getLeaseFunc         func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	getPendingLeasesFunc func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	// pendingLeases is used when getPendingLeasesFunc is not set
	pendingLeases []string
}

func (m *mockAckChainClient) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	if m.acknowledgeFunc != nil {
		return m.acknowledgeFunc(ctx, leaseUUIDs)
	}
	return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
}

func (m *mockAckChainClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getLeaseFunc != nil {
		return m.getLeaseFunc(ctx, leaseUUID)
	}
	// Default: return a PENDING lease so acknowledgment proceeds
	return &billingtypes.Lease{
		Uuid:  leaseUUID,
		State: billingtypes.LEASE_STATE_PENDING,
	}, nil
}

func (m *mockAckChainClient) GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	if m.getPendingLeasesFunc != nil {
		return m.getPendingLeasesFunc(ctx, providerUUID)
	}
	// Default: return leases from pendingLeases list, or treat all as pending if not set
	var leases []billingtypes.Lease
	for _, uuid := range m.pendingLeases {
		leases = append(leases, billingtypes.Lease{
			Uuid:  uuid,
			State: billingtypes.LEASE_STATE_PENDING,
		})
	}
	return leases, nil
}

func (m *mockAckChainClient) RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	return 0, nil, nil
}

const testProviderUUID = "01234567-89ab-cdef-0123-456789abcdef"

func TestAckBatcher_BatchesMultipleRequests(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string

	client := &mockAckChainClient{
		// All requested leases are pending
		pendingLeases: []string{"lease-a", "lease-b", "lease-c", "lease-d", "lease-e"},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			batches = append(batches, leaseUUIDs)
			mu.Unlock()
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 100 * time.Millisecond,
		BatchSize:     10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	defer batcher.Stop()

	// Send multiple requests concurrently
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			leaseUUID := "lease-" + string(rune('a'+i))
			acked, _, err := batcher.Acknowledge(ctx, leaseUUID)
			if err != nil {
				t.Errorf("Acknowledge(%s) error = %v", leaseUUID, err)
			}
			if !acked {
				t.Errorf("Acknowledge(%s) acked = false, want true", leaseUUID)
			}
		}(i)
	}

	wg.Wait()

	// Check that requests were batched
	mu.Lock()
	defer mu.Unlock()

	totalAcked := 0
	for _, batch := range batches {
		totalAcked += len(batch)
	}

	if totalAcked != 5 {
		t.Errorf("total acknowledged = %d, want 5", totalAcked)
	}

	// Should have been batched into 1-2 batches, not 5 individual calls
	if len(batches) > 2 {
		t.Errorf("batches = %d, want <= 2 (requests should be batched)", len(batches))
	}
}

func TestAckBatcher_FallsBackToIndividualOnBatchFailure(t *testing.T) {
	var callCount atomic.Int32
	var mu sync.Mutex
	var individualCalls []string

	client := &mockAckChainClient{
		pendingLeases: []string{"lease-a", "lease-b", "lease-c"},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			count := callCount.Add(1)

			// First call (batch) fails
			if count == 1 && len(leaseUUIDs) > 1 {
				return 0, nil, errors.New("batch failed")
			}

			// Individual calls succeed
			mu.Lock()
			individualCalls = append(individualCalls, leaseUUIDs...)
			mu.Unlock()
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 50 * time.Millisecond,
		BatchSize:     10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	defer batcher.Stop()

	// Send multiple requests
	var wg sync.WaitGroup
	results := make([]bool, 3)
	errs := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			leaseUUID := "lease-" + string(rune('a'+i))
			results[i], _, errs[i] = batcher.Acknowledge(ctx, leaseUUID)
		}(i)
	}

	wg.Wait()

	// All should succeed (via individual fallback)
	for i, err := range errs {
		if err != nil {
			t.Errorf("Acknowledge[%d] error = %v, want nil", i, err)
		}
		if !results[i] {
			t.Errorf("Acknowledge[%d] acked = false, want true", i)
		}
	}

	// Should have individual calls after batch failure
	mu.Lock()
	defer mu.Unlock()

	if len(individualCalls) != 3 {
		t.Errorf("individual calls = %d, want 3", len(individualCalls))
	}
}

func TestAckBatcher_FlushesOnBatchSizeReached(t *testing.T) {
	var mu sync.Mutex
	var batches [][]string

	client := &mockAckChainClient{
		pendingLeases: []string{"lease-a", "lease-b", "lease-c"},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			batches = append(batches, leaseUUIDs)
			mu.Unlock()
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 10 * time.Second, // Long interval so only batch size triggers flush
		BatchSize:     3,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	defer batcher.Stop()

	// Send exactly batch size requests
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			leaseUUID := "lease-" + string(rune('a'+i))
			_, _, _ = batcher.Acknowledge(ctx, leaseUUID)
		}(i)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Should have flushed when batch size was reached
	if len(batches) != 1 {
		t.Errorf("batches = %d, want 1", len(batches))
	}
	if len(batches) > 0 && len(batches[0]) != 3 {
		t.Errorf("batch size = %d, want 3", len(batches[0]))
	}
}

func TestAckBatcher_ContextCancellation(t *testing.T) {
	client := &mockAckChainClient{
		pendingLeases: []string{"lease-1"},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			// Simulate slow acknowledgment
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
			}
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 10 * time.Second,
		BatchSize:     100,
	})

	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)

	// Start an acknowledge request
	done := make(chan struct{})
	var err error
	go func() {
		_, _, err = batcher.Acknowledge(ctx, "lease-1")
		close(done)
	}()

	// Give request time to be queued
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()
	batcher.Stop()

	// Request should complete with error
	select {
	case <-done:
		if err == nil {
			t.Error("expected error after context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Error("Acknowledge did not return after context cancellation")
	}
}

func TestAckBatcher_SkipsAlreadyAcknowledgedLeases(t *testing.T) {
	var mu sync.Mutex
	var ackCalls [][]string

	client := &mockAckChainClient{
		// Only lease-b is pending; lease-a and lease-c are already acknowledged
		getPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			ackCalls = append(ackCalls, leaseUUIDs)
			mu.Unlock()
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 50 * time.Millisecond,
		BatchSize:     10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	defer batcher.Stop()

	// Send requests for all three leases
	var wg sync.WaitGroup
	results := make([]bool, 3)
	errs := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			leaseUUID := "lease-" + string(rune('a'+i))
			results[i], _, errs[i] = batcher.Acknowledge(ctx, leaseUUID)
		}(i)
	}

	wg.Wait()

	// All should report success (including already-acknowledged ones)
	for i, err := range errs {
		if err != nil {
			t.Errorf("Acknowledge[%d] error = %v, want nil", i, err)
		}
		if !results[i] {
			t.Errorf("Acknowledge[%d] acked = false, want true", i)
		}
	}

	// Only lease-b should have been sent to AcknowledgeLeases
	mu.Lock()
	defer mu.Unlock()

	totalAcked := 0
	for _, batch := range ackCalls {
		totalAcked += len(batch)
	}

	if totalAcked != 1 {
		t.Errorf("total acknowledged = %d, want 1 (only lease-b should need ack)", totalAcked)
	}

	// Verify lease-b was in the ack call
	found := false
	for _, batch := range ackCalls {
		for _, uuid := range batch {
			if uuid == "lease-b" {
				found = true
			}
			if uuid == "lease-a" || uuid == "lease-c" {
				t.Errorf("already-acknowledged lease %s was sent to AcknowledgeLeases", uuid)
			}
		}
	}
	if !found && totalAcked > 0 {
		t.Error("lease-b was not in any ack batch")
	}
}

func TestAckBatcher_SkipsNotFoundLeases(t *testing.T) {
	var ackCalled atomic.Bool

	client := &mockAckChainClient{
		// Return empty list - no pending leases exist
		getPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
		acknowledgeFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			ackCalled.Store(true)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 50 * time.Millisecond,
		BatchSize:     10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	defer batcher.Stop()

	// Try to acknowledge a non-existent lease
	acked, _, err := batcher.Acknowledge(ctx, "non-existent-lease")

	// Should report success (lease doesn't exist, nothing to do)
	if err != nil {
		t.Errorf("Acknowledge() error = %v, want nil", err)
	}
	if !acked {
		t.Error("Acknowledge() acked = false, want true")
	}

	// AcknowledgeLeases should NOT have been called
	if ackCalled.Load() {
		t.Error("AcknowledgeLeases was called for non-existent lease")
	}
}
