package chain

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// mockLeaseGetter is a minimal mock for testing the cached client.
type mockLeaseGetter struct {
	leases    map[string]*billingtypes.Lease
	callCount atomic.Int32
	mu        sync.RWMutex
	err       error
}

func newMockLeaseGetter() *mockLeaseGetter {
	return &mockLeaseGetter{
		leases: make(map[string]*billingtypes.Lease),
	}
}

func (m *mockLeaseGetter) setLease(uuid string, lease *billingtypes.Lease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leases[uuid] = lease
}

func (m *mockLeaseGetter) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *mockLeaseGetter) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	m.callCount.Add(1)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.err != nil {
		return nil, m.err
	}

	lease, ok := m.leases[leaseUUID]
	if !ok {
		return nil, errors.New("lease not found")
	}

	return lease, nil
}

func (m *mockLeaseGetter) getCallCount() int {
	return int(m.callCount.Load())
}

// testCachedClient is a cached client that uses a mock instead of a real Client.
type testCachedClient struct {
	*CachedClient
	mock *mockLeaseGetter
}

func newTestCachedClient(size int, ttl time.Duration) *testCachedClient {
	mock := newMockLeaseGetter()

	// Create a CachedClient with nil Client (we'll override GetLease)
	cached := NewCachedClient(nil, size, ttl)

	return &testCachedClient{
		CachedClient: cached,
		mock:         mock,
	}
}

// GetLease overrides to use mock instead of real client.
func (c *testCachedClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	// Check cache first
	if lease, ok := c.cache.Get(leaseUUID); ok {
		return lease, nil
	}

	// Fetch from mock
	lease, err := c.mock.GetLease(ctx, leaseUUID)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.cache.Add(leaseUUID, lease)

	return lease, nil
}

func TestCachedClient_CacheHit(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	lease := &billingtypes.Lease{
		Uuid:  "test-uuid",
		State: billingtypes.LEASE_STATE_ACTIVE,
	}
	client.mock.setLease("test-uuid", lease)

	// First call - cache miss
	got, err := client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if got.Uuid != lease.Uuid {
		t.Errorf("GetLease() uuid = %v, want %v", got.Uuid, lease.Uuid)
	}
	if client.mock.getCallCount() != 1 {
		t.Errorf("call count = %d, want 1", client.mock.getCallCount())
	}

	// Second call - cache hit
	got, err = client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if got.Uuid != lease.Uuid {
		t.Errorf("GetLease() uuid = %v, want %v", got.Uuid, lease.Uuid)
	}
	if client.mock.getCallCount() != 1 {
		t.Errorf("call count = %d, want 1 (should hit cache)", client.mock.getCallCount())
	}
}

func TestCachedClient_CacheExpiration(t *testing.T) {
	// Use very short TTL for testing
	client := newTestCachedClient(100, 50*time.Millisecond)

	lease := &billingtypes.Lease{
		Uuid:  "test-uuid",
		State: billingtypes.LEASE_STATE_ACTIVE,
	}
	client.mock.setLease("test-uuid", lease)

	// First call - cache miss
	_, err := client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if client.mock.getCallCount() != 1 {
		t.Errorf("call count = %d, want 1", client.mock.getCallCount())
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Third call - cache miss (expired)
	_, err = client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if client.mock.getCallCount() != 2 {
		t.Errorf("call count = %d, want 2 (should have expired)", client.mock.getCallCount())
	}
}

func TestCachedClient_InvalidateLease(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	lease := &billingtypes.Lease{
		Uuid:  "test-uuid",
		State: billingtypes.LEASE_STATE_ACTIVE,
	}
	client.mock.setLease("test-uuid", lease)

	// First call - cache miss
	_, err := client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if client.mock.getCallCount() != 1 {
		t.Errorf("call count = %d, want 1", client.mock.getCallCount())
	}

	// Invalidate
	client.InvalidateLease("test-uuid")

	// Next call - cache miss (invalidated)
	_, err = client.GetLease(context.Background(), "test-uuid")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if client.mock.getCallCount() != 2 {
		t.Errorf("call count = %d, want 2 (should be invalidated)", client.mock.getCallCount())
	}
}

func TestCachedClient_InvalidateLeases(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	// Add multiple leases
	for i := 0; i < 3; i++ {
		uuid := "uuid-" + string(rune('a'+i))
		client.mock.setLease(uuid, &billingtypes.Lease{
			Uuid:  uuid,
			State: billingtypes.LEASE_STATE_ACTIVE,
		})
		_, _ = client.GetLease(context.Background(), uuid)
	}

	if client.Len() != 3 {
		t.Errorf("cache length = %d, want 3", client.Len())
	}

	// Invalidate some
	client.InvalidateLeases([]string{"uuid-a", "uuid-c"})

	if client.Len() != 1 {
		t.Errorf("cache length = %d, want 1", client.Len())
	}
}

func TestCachedClient_InvalidateAll(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	// Add multiple leases
	for i := 0; i < 5; i++ {
		uuid := "uuid-" + string(rune('a'+i))
		client.mock.setLease(uuid, &billingtypes.Lease{
			Uuid:  uuid,
			State: billingtypes.LEASE_STATE_ACTIVE,
		})
		_, _ = client.GetLease(context.Background(), uuid)
	}

	if client.Len() != 5 {
		t.Errorf("cache length = %d, want 5", client.Len())
	}

	// Invalidate all
	client.InvalidateAll()

	if client.Len() != 0 {
		t.Errorf("cache length = %d, want 0", client.Len())
	}
}

func TestCachedClient_Error(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	expectedErr := errors.New("test error")
	client.mock.setError(expectedErr)

	_, err := client.GetLease(context.Background(), "test-uuid")
	if err == nil {
		t.Fatal("GetLease() expected error, got nil")
	}
	if err.Error() != expectedErr.Error() {
		t.Errorf("GetLease() error = %v, want %v", err, expectedErr)
	}

	// Verify error was not cached (cache should be empty)
	if client.Len() != 0 {
		t.Errorf("cache length = %d, want 0 (errors should not be cached)", client.Len())
	}
}

func TestCachedClient_LRUEviction(t *testing.T) {
	// Small cache size to trigger eviction
	client := newTestCachedClient(3, time.Minute)

	// Add 5 leases to a cache of size 3
	for i := 0; i < 5; i++ {
		uuid := "uuid-" + string(rune('a'+i))
		client.mock.setLease(uuid, &billingtypes.Lease{
			Uuid:  uuid,
			State: billingtypes.LEASE_STATE_ACTIVE,
		})
		_, _ = client.GetLease(context.Background(), uuid)
	}

	// Cache should only have 3 entries (LRU eviction)
	if client.Len() != 3 {
		t.Errorf("cache length = %d, want 3 (LRU eviction)", client.Len())
	}

	// The oldest entries (a, b) should be evicted
	// The newest entries (c, d, e) should be in cache
	initialCallCount := client.mock.getCallCount()

	// Access uuid-e (should be cached)
	_, _ = client.GetLease(context.Background(), "uuid-e")
	if client.mock.getCallCount() != initialCallCount {
		t.Error("uuid-e should have been cached")
	}

	// Access uuid-a (should NOT be cached, was evicted)
	_, _ = client.GetLease(context.Background(), "uuid-a")
	if client.mock.getCallCount() != initialCallCount+1 {
		t.Error("uuid-a should have been evicted and refetched")
	}
}

func TestCachedClient_Defaults(t *testing.T) {
	// Test default values
	cached := NewCachedClient(nil, 0, 0)

	// The cache should be created with defaults
	if cached.cache == nil {
		t.Error("cache should not be nil")
	}
}

func TestCachedClient_Concurrent(t *testing.T) {
	client := newTestCachedClient(100, time.Minute)

	lease := &billingtypes.Lease{
		Uuid:  "test-uuid",
		State: billingtypes.LEASE_STATE_ACTIVE,
	}
	client.mock.setLease("test-uuid", lease)

	// Run concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.GetLease(context.Background(), "test-uuid")
			if err != nil {
				t.Errorf("GetLease() error = %v", err)
			}
		}()
	}
	wg.Wait()

	// Should have at most a few calls due to caching
	// (some initial concurrent requests may hit the backend before cache is populated)
	callCount := client.mock.getCallCount()
	if callCount > 10 {
		t.Errorf("call count = %d, expected much less due to caching", callCount)
	}
}
