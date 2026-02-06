package shared

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourcePool(t *testing.T) {
	profiles := map[string]SKUProfile{
		"small": {CPUCores: 1.0, MemoryMB: 512, DiskMB: 1024},
		"large": {CPUCores: 4.0, MemoryMB: 4096, DiskMB: 8192},
	}

	// Helper to create a resolver from profiles map
	makeResolver := func(profiles map[string]SKUProfile) SKUResolver {
		return func(sku string) (SKUProfile, error) {
			if p, ok := profiles[sku]; ok {
				return p, nil
			}
			return SKUProfile{}, fmt.Errorf("unknown SKU: %s", sku)
		}
	}

	t.Run("allocate and release", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		// Allocate
		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		stats := pool.Stats()
		assert.Equal(t, 1.0, stats.AllocatedCPU)
		assert.Equal(t, int64(512), stats.AllocatedMemoryMB)

		// Release
		pool.Release("lease-1")

		stats = pool.Stats()
		assert.Equal(t, 0.0, stats.AllocatedCPU)
	})

	t.Run("insufficient resources", func(t *testing.T) {
		// Small pool
		pool := NewResourcePool(2.0, 1024, 2048, makeResolver(profiles), nil)

		// First allocation succeeds
		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		// Second allocation should fail (not enough for large)
		err = pool.TryAllocate("lease-2", "large", "tenant-a")
		assert.Error(t, err)
	})

	t.Run("unknown SKU", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		err := pool.TryAllocate("lease-1", "nonexistent", "tenant-a")
		assert.Error(t, err)
	})

	t.Run("duplicate allocation", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		err = pool.TryAllocate("lease-1", "small", "tenant-a")
		assert.Error(t, err)
	})

	t.Run("release nonexistent", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		// Should not panic
		pool.Release("nonexistent")
	})

	t.Run("reset", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		// Allocate something
		pool.TryAllocate("lease-1", "small", "tenant-a")

		// Reset with different allocations
		allocations := []ResourceAllocation{
			{LeaseUUID: "lease-2", Tenant: "tenant-b", SKU: "large", CPUCores: 4.0, MemoryMB: 4096, DiskMB: 8192},
		}
		pool.Reset(allocations)

		stats := pool.Stats()
		assert.Equal(t, 1, stats.AllocationCount)
		assert.Equal(t, 4.0, stats.AllocatedCPU)

		// Original allocation should be gone
		alloc := pool.GetAllocation("lease-1")
		assert.Nil(t, alloc)
	})

	t.Run("tenant quota enforcement", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 1024,
			MaxDiskMB:   2048,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		// First allocation within quota succeeds
		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		// Second allocation within quota succeeds
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		require.NoError(t, err)

		// Third allocation exceeds tenant CPU quota (3 * 1.0 > 2.0)
		err = pool.TryAllocate("lease-3", "small", "tenant-a")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant tenant-a")

		// Different tenant can still allocate
		err = pool.TryAllocate("lease-4", "small", "tenant-b")
		require.NoError(t, err)

		// Release from tenant-a frees quota
		pool.Release("lease-1")
		err = pool.TryAllocate("lease-5", "small", "tenant-a")
		require.NoError(t, err)
	})

	t.Run("tenant stats", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 4.0,
			MaxMemoryMB: 4096,
			MaxDiskMB:   8192,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		pool.TryAllocate("lease-1", "small", "tenant-a")

		stats := pool.TenantStats("tenant-a")
		assert.Equal(t, 1.0, stats.AllocatedCPU)
		assert.Equal(t, int64(512), stats.AllocatedMemoryMB)
		assert.Equal(t, 4.0, stats.TotalCPU)

		// Empty tenant returns zeroes
		stats = pool.TenantStats("tenant-b")
		assert.Equal(t, 0.0, stats.AllocatedCPU)
	})

	t.Run("tenant memory quota exceeded", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 8.0,
			MaxMemoryMB: 600,
			MaxDiskMB:   102400,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		// small = 512MB, second would be 1024MB > quota of 600MB
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "memory quota exceeded")
	})

	t.Run("tenant disk quota exceeded", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 8.0,
			MaxMemoryMB: 16384,
			MaxDiskMB:   1500,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)

		// small = 1024MB disk, second would be 2048MB > quota of 1500MB
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "disk quota exceeded")
	})

	t.Run("no quota allows full pool usage by one tenant", func(t *testing.T) {
		pool := NewResourcePool(2.0, 1024, 2048, makeResolver(profiles), nil)

		err := pool.TryAllocate("lease-1", "small", "tenant-a")
		require.NoError(t, err)
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		require.NoError(t, err)
	})

	t.Run("empty tenant bypasses quota check", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 0.1,
			MaxMemoryMB: 1,
			MaxDiskMB:   1,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		// Empty tenant string should bypass tenant quota
		err := pool.TryAllocate("lease-1", "small", "")
		require.NoError(t, err)
	})

	t.Run("reset rebuilds tenant aggregates", func(t *testing.T) {
		quota := &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 1024,
			MaxDiskMB:   2048,
		}
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), quota)

		pool.TryAllocate("lease-1", "small", "tenant-a")

		// Reset with allocation for different tenant
		pool.Reset([]ResourceAllocation{
			{LeaseUUID: "lease-2", Tenant: "tenant-b", SKU: "small", CPUCores: 1.0, MemoryMB: 512, DiskMB: 1024},
		})

		// tenant-a should have no usage after reset
		stats := pool.TenantStats("tenant-a")
		assert.Equal(t, 0.0, stats.AllocatedCPU)

		// tenant-b should have usage
		stats = pool.TenantStats("tenant-b")
		assert.Equal(t, 1.0, stats.AllocatedCPU)
	})

	t.Run("release cleans up tenant entry at zero", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		pool.TryAllocate("lease-1", "small", "tenant-a")
		pool.Release("lease-1")

		stats := pool.TenantStats("tenant-a")
		assert.Equal(t, 0.0, stats.AllocatedCPU)
		assert.Equal(t, int64(0), stats.AllocatedMemoryMB)
		assert.Equal(t, int64(0), stats.AllocatedDiskMB)
	})

	t.Run("allocation stores tenant in record", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		pool.TryAllocate("lease-1", "small", "tenant-a")

		alloc := pool.GetAllocation("lease-1")
		require.NotNil(t, alloc)
		assert.Equal(t, "tenant-a", alloc.Tenant)
	})

	t.Run("list allocations", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		pool.TryAllocate("lease-1", "small", "tenant-a")
		pool.TryAllocate("lease-2", "large", "tenant-b")

		allocs := pool.ListAllocations()
		assert.Len(t, allocs, 2)

		// Verify contents (order not guaranteed from map iteration)
		found := map[string]bool{}
		for _, a := range allocs {
			found[a.LeaseUUID] = true
		}
		assert.True(t, found["lease-1"])
		assert.True(t, found["lease-2"])
	})

	t.Run("available resources", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		stats := pool.Stats()
		assert.Equal(t, 8.0, stats.AvailableCPU())
		assert.Equal(t, int64(16384), stats.AvailableMemoryMB())
		assert.Equal(t, int64(102400), stats.AvailableDiskMB())

		pool.TryAllocate("lease-1", "small", "tenant-a")

		stats = pool.Stats()
		assert.Equal(t, 7.0, stats.AvailableCPU())
		assert.Equal(t, int64(16384-512), stats.AvailableMemoryMB())
		assert.Equal(t, int64(102400-1024), stats.AvailableDiskMB())
	})

	t.Run("concurrent allocate and release", func(t *testing.T) {
		pool := NewResourcePool(100.0, 102400, 1024000, makeResolver(profiles), nil)

		const goroutines = 20
		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := range goroutines {
			go func(id int) {
				defer wg.Done()
				leaseID := fmt.Sprintf("lease-%d", id)
				if err := pool.TryAllocate(leaseID, "small", "tenant-a"); err == nil {
					pool.Release(leaseID)
				}
			}(i)
		}
		wg.Wait()

		stats := pool.Stats()
		assert.Equal(t, 0.0, stats.AllocatedCPU)
		assert.Equal(t, int64(0), stats.AllocatedMemoryMB)
		assert.Equal(t, int64(0), stats.AllocatedDiskMB)
		assert.Equal(t, 0, stats.AllocationCount)
	})

	t.Run("insufficient memory with ample CPU and disk", func(t *testing.T) {
		// Ample CPU (100) and disk (100000), but tight memory (600 MB)
		pool := NewResourcePool(100.0, 600, 100000, makeResolver(profiles), nil)

		err := pool.TryAllocate("lease-1", "small", "tenant-a") // 512 MB
		require.NoError(t, err)

		// Second small would need 512 MB more, only 88 MB left
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		assert.ErrorContains(t, err, "insufficient memory")
	})

	t.Run("insufficient disk with ample CPU and memory", func(t *testing.T) {
		// Ample CPU (100) and memory (100000), but tight disk (1500 MB)
		pool := NewResourcePool(100.0, 100000, 1500, makeResolver(profiles), nil)

		err := pool.TryAllocate("lease-1", "small", "tenant-a") // 1024 MB disk
		require.NoError(t, err)

		// Second small would need 1024 MB more, only 476 MB left
		err = pool.TryAllocate("lease-2", "small", "tenant-a")
		assert.ErrorContains(t, err, "insufficient disk")
	})

	t.Run("TryAllocate after Reset respects restored usage", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles), nil)

		// Reset with an allocation consuming most of the CPU
		pool.Reset([]ResourceAllocation{
			{LeaseUUID: "existing", Tenant: "tenant-a", SKU: "large", CPUCores: 7.5, MemoryMB: 4096, DiskMB: 8192},
		})

		// New allocation should fail — only 0.5 CPU left, small needs 1.0
		err := pool.TryAllocate("new-lease", "small", "tenant-a")
		assert.ErrorContains(t, err, "insufficient CPU")

		// Release the restored allocation, then allocate should succeed
		pool.Release("existing")
		err = pool.TryAllocate("new-lease", "small", "tenant-a")
		require.NoError(t, err)
	})

	t.Run("nil resolver panics", func(t *testing.T) {
		assert.Panics(t, func() {
			NewResourcePool(8.0, 16384, 102400, nil, nil)
		})
	})
}
