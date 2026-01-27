package chain

import (
	"context"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/metrics"
)

const (
	// DefaultLeaseCacheTTL is the default time-to-live for cached leases.
	// Keep short since lease state can change (pending → active → closed).
	DefaultLeaseCacheTTL = 5 * time.Second

	// DefaultLeaseCacheSize is the maximum number of leases to cache.
	DefaultLeaseCacheSize = 1000
)

// CachedClient wraps a Client with a short-TTL cache for lease queries.
// This reduces redundant chain queries during high-throughput scenarios
// where the same lease may be queried multiple times in quick succession.
type CachedClient struct {
	*Client
	cache *expirable.LRU[string, *billingtypes.Lease]
}

// NewCachedClient creates a new cached client wrapper.
// Size controls the maximum number of cached entries (0 = default 1000).
// TTL controls how long entries remain valid (0 = default 5s).
func NewCachedClient(client *Client, size int, ttl time.Duration) *CachedClient {
	if size <= 0 {
		size = DefaultLeaseCacheSize
	}
	if ttl == 0 {
		ttl = DefaultLeaseCacheTTL
	}

	cache := expirable.NewLRU[string, *billingtypes.Lease](size, nil, ttl)

	return &CachedClient{
		Client: client,
		cache:  cache,
	}
}

// GetLease returns a lease by UUID, using cache when available.
func (c *CachedClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	// Check cache first
	if lease, ok := c.cache.Get(leaseUUID); ok {
		metrics.LeaseCacheHits.Inc()
		return lease, nil
	}

	metrics.LeaseCacheMisses.Inc()

	// Fetch from chain
	lease, err := c.Client.GetLease(ctx, leaseUUID)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.cache.Add(leaseUUID, lease)

	return lease, nil
}

// GetActiveLease returns an active lease, using cache when available.
// Returns nil if the lease exists but is not in ACTIVE state.
func (c *CachedClient) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	lease, err := c.GetLease(ctx, leaseUUID)
	if err != nil {
		return nil, err
	}

	if lease.State != billingtypes.LEASE_STATE_ACTIVE {
		return nil, nil
	}

	return lease, nil
}

// InvalidateLease removes a lease from the cache.
// Call this after state-changing operations (acknowledge, reject, close).
func (c *CachedClient) InvalidateLease(leaseUUID string) {
	c.cache.Remove(leaseUUID)
}

// InvalidateLeases removes multiple leases from the cache.
func (c *CachedClient) InvalidateLeases(leaseUUIDs []string) {
	for _, uuid := range leaseUUIDs {
		c.cache.Remove(uuid)
	}
}

// InvalidateAll clears the entire cache.
func (c *CachedClient) InvalidateAll() {
	c.cache.Purge()
}

// Len returns the number of items currently in the cache.
func (c *CachedClient) Len() int {
	return c.cache.Len()
}

// AcknowledgeLeases acknowledges leases and invalidates them from cache.
// This ensures subsequent GetLease calls fetch fresh state from chain.
func (c *CachedClient) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	acknowledged, txHashes, err := c.Client.AcknowledgeLeases(ctx, leaseUUIDs)
	if err == nil {
		// Invalidate cache for all leases that were part of the request,
		// regardless of how many were actually acknowledged.
		// This ensures we don't serve stale state.
		c.InvalidateLeases(leaseUUIDs)
	}
	return acknowledged, txHashes, err
}

// RejectLeases rejects leases and invalidates them from cache.
func (c *CachedClient) RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	rejected, txHashes, err := c.Client.RejectLeases(ctx, leaseUUIDs, reason)
	if err == nil {
		c.InvalidateLeases(leaseUUIDs)
	}
	return rejected, txHashes, err
}

// CloseLeases closes leases and invalidates them from cache.
func (c *CachedClient) CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	closed, txHashes, err := c.Client.CloseLeases(ctx, leaseUUIDs, reason)
	if err == nil {
		c.InvalidateLeases(leaseUUIDs)
	}
	return closed, txHashes, err
}
