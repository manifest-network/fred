package shared

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResourcePool_RejectsInvalidConstructionBoundaries(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
	}
	tests := []struct {
		name        string
		totalCPU    float64
		totalMemory int64
		totalDisk   int64
		quota       *TenantQuotaConfig
		wantPanic   string
	}{
		{name: "NaN total CPU", totalCPU: math.NaN(), totalMemory: 1, totalDisk: 1, wantPanic: "shared.NewResourcePool: total CPU must be finite"},
		{name: "infinite total CPU", totalCPU: math.Inf(1), totalMemory: 1, totalDisk: 1, wantPanic: "shared.NewResourcePool: total CPU must be finite"},
		{name: "zero total CPU", totalCPU: 0, totalMemory: 1, totalDisk: 1, wantPanic: "shared.NewResourcePool: total CPU must be positive"},
		{name: "negative total CPU", totalCPU: -1, totalMemory: 1, totalDisk: 1, wantPanic: "shared.NewResourcePool: total CPU must be positive"},
		{name: "zero total memory", totalCPU: 1, totalMemory: 0, totalDisk: 1, wantPanic: "shared.NewResourcePool: total memory must be positive"},
		{name: "negative total disk", totalCPU: 1, totalMemory: 1, totalDisk: -1, wantPanic: "shared.NewResourcePool: total disk must be positive"},
		{
			name:        "NaN tenant CPU quota",
			totalCPU:    1,
			totalMemory: 1,
			totalDisk:   1,
			quota:       &TenantQuotaConfig{MaxCPUCores: math.NaN(), MaxMemoryMB: 1, MaxDiskMB: 1},
			wantPanic:   "shared.NewResourcePool: invalid tenant quota: max_cpu_cores must be finite",
		},
		{
			name:        "invalid tenant memory quota",
			totalCPU:    1,
			totalMemory: 1,
			totalDisk:   1,
			quota:       &TenantQuotaConfig{MaxCPUCores: 1, MaxMemoryMB: 0, MaxDiskMB: 1},
			wantPanic:   "shared.NewResourcePool: invalid tenant quota: max_memory_mb must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tt.wantPanic, func() {
				NewResourcePool(tt.totalCPU, tt.totalMemory, tt.totalDisk, resolver, tt.quota)
			})
		})
	}
}

func TestNewResourcePool_CopiesTenantQuota(t *testing.T) {
	quota := &TenantQuotaConfig{MaxCPUCores: 1, MaxMemoryMB: 2, MaxDiskMB: 2}
	pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
	}, quota)

	// Mutation of caller-owned configuration after construction must not disable
	// the live pool's quota gate.
	quota.MaxCPUCores = math.NaN()
	require.NoError(t, pool.TryAllocate("lease-1", "sku", "tenant-a"))
	require.ErrorContains(t, pool.TryAllocate("lease-2", "sku", "tenant-a"), "CPU quota exceeded")
}

func TestResourcePool_ReleasePublishesExactEmptyStateDespiteFloatRoundoff(t *testing.T) {
	profiles := map[string]SKUProfile{
		"first":  {CPUCores: 0.762, MemoryMB: 1, DiskMB: 1},
		"second": {CPUCores: 0.973, MemoryMB: 1, DiskMB: 1},
	}
	quota := &TenantQuotaConfig{MaxCPUCores: 2, MaxMemoryMB: 2, MaxDiskMB: 2}
	pool := NewResourcePool(2, 2, 2, func(sku string) (SKUProfile, error) {
		return profiles[sku], nil
	}, quota)

	require.NoError(t, pool.TryAllocate("first", "first", "tenant-a"))
	require.NoError(t, pool.TryAllocate("second", "second", "tenant-a"))
	// Releasing in the opposite order from admission makes ordinary float64
	// subtraction end slightly below zero for these two valid profiles.
	pool.Release("second")
	pool.Release("first")

	stats := pool.Stats()
	assert.Zero(t, stats.AllocatedCPU)
	assert.Zero(t, stats.AllocatedMemoryMB)
	assert.Zero(t, stats.AllocatedDiskMB)
	assert.Zero(t, stats.AllocationCount)
	assert.Zero(t, pool.TenantStats("tenant-a").AllocatedCPU)
	require.NoError(t, pool.TryAllocate("replacement", "first", "tenant-a"),
		"floating-point release round-off must not fail-close an empty pool forever")
}

func TestResourcePool_RejectsNonFiniteResolverProfileWithoutPoisoningAccounting(t *testing.T) {
	paths := []struct {
		name     string
		allocate func(*ResourcePool) error
	}{
		{
			name: "fresh allocation",
			allocate: func(pool *ResourcePool) error {
				return pool.TryAllocate("lease-1", "bad", "tenant-a")
			},
		},
		{
			name: "restore adoption",
			allocate: func(pool *ResourcePool) error {
				return pool.TryAllocateAdoptAll(
					[]AdoptInstance{{ID: "lease-1-app-0", SKU: "bad"}},
					"tenant-a",
					0,
				)
			},
		},
	}
	values := []struct {
		name string
		cpu  float64
	}{
		{name: "NaN", cpu: math.NaN()},
		{name: "positive infinity", cpu: math.Inf(1)},
		{name: "negative infinity", cpu: math.Inf(-1)},
	}

	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			for _, value := range values {
				t.Run(value.name, func(t *testing.T) {
					pool := NewResourcePool(8, 8192, 8192, func(string) (SKUProfile, error) {
						return SKUProfile{CPUCores: value.cpu, MemoryMB: 512, DiskMB: 1024}, nil
					}, nil)

					err := path.allocate(pool)
					require.ErrorContains(t, err, "cpu_cores must be finite")
					stats := pool.Stats()
					assert.Zero(t, stats.AllocationCount)
					assert.Zero(t, stats.AllocatedCPU)
					assert.False(t, math.IsNaN(stats.AllocatedCPU),
						"a rejected profile must not poison subsequent comparisons")
				})
			}
		})
	}
}

func TestResourcePool_AdmissionArithmeticDoesNotOverflow(t *testing.T) {
	t.Run("CPU headroom", func(t *testing.T) {
		pool := NewResourcePool(math.MaxFloat64, math.MaxInt64, math.MaxInt64, func(sku string) (SKUProfile, error) {
			if sku == "fills" {
				return SKUProfile{CPUCores: math.MaxFloat64, MemoryMB: 1, DiskMB: 1}, nil
			}
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
		}, nil)
		require.NoError(t, pool.TryAllocate("lease-1", "fills", "tenant-a"))
		require.ErrorContains(t, pool.TryAllocate("lease-2", "extra", "tenant-a"), "insufficient CPU")
		assert.Equal(t, 1, pool.Stats().AllocationCount)
	})

	t.Run("memory headroom", func(t *testing.T) {
		pool := NewResourcePool(10, math.MaxInt64, math.MaxInt64, func(sku string) (SKUProfile, error) {
			if sku == "fills" {
				return SKUProfile{CPUCores: 1, MemoryMB: math.MaxInt64 - 5, DiskMB: 1}, nil
			}
			return SKUProfile{CPUCores: 1, MemoryMB: 10, DiskMB: 1}, nil
		}, nil)
		require.NoError(t, pool.TryAllocate("lease-1", "fills", "tenant-a"))
		require.ErrorContains(t, pool.TryAllocate("lease-2", "extra", "tenant-a"), "insufficient memory")
		assert.Equal(t, 1, pool.Stats().AllocationCount)
	})

	t.Run("disk headroom with retained projection", func(t *testing.T) {
		pool := NewResourcePool(10, 10, math.MaxInt64, func(string) (SKUProfile, error) {
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 10}, nil
		}, nil)
		require.NoError(t, pool.SetRetainedDisk(math.MaxInt64-5))
		err := pool.TryAllocate("lease-1", "sku", "tenant-a")
		require.ErrorContains(t, err, "insufficient disk")
		assert.ErrorContains(t, err, "have 5 MB available")
		assert.Zero(t, pool.Stats().AllocationCount)
	})

	t.Run("restore aggregate", func(t *testing.T) {
		pool := NewResourcePool(10, math.MaxInt64, math.MaxInt64, func(string) (SKUProfile, error) {
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64}, nil
		}, nil)
		err := pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "a", SKU: "sku"}, {ID: "b", SKU: "sku"}}, "tenant-a", 0)
		require.ErrorContains(t, err, "restore disk requirement exceeds supported range")
		assert.Zero(t, pool.Stats().AllocationCount)
	})

	t.Run("restore live accounting", func(t *testing.T) {
		pool := NewResourcePool(10, 10, math.MaxInt64, func(sku string) (SKUProfile, error) {
			if sku == "live" {
				return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64 - 5}, nil
			}
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 10}, nil
		}, nil)
		require.NoError(t, pool.TryAllocate("live", "live", "tenant-a"))
		require.NoError(t, pool.SetRetainedDisk(10))

		err := pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "restore", SKU: "adopt"}}, "tenant-a", 10)
		require.ErrorContains(t, err, "disk accounting exceeds supported range")
		assert.Equal(t, 1, pool.Stats().AllocationCount)
		assert.Nil(t, pool.GetAllocation("restore"))
	})

	t.Run("negative retained input", func(t *testing.T) {
		pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
		}, nil)
		err := pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "restore", SKU: "sku"}}, "tenant-a", -1)
		require.ErrorContains(t, err, "old retained disk must not be negative")
		assert.Zero(t, pool.Stats().AllocationCount)
	})
}

func TestResourcePool_AdoptResolvesEachInstanceExactlyOnce(t *testing.T) {
	var calls int
	pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
		calls++
		if calls == 1 {
			return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
		}
		return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 10}, nil
	}, nil)

	require.NoError(t, pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "lease-1-app-0", SKU: "sku"}}, "tenant-a", 0))
	assert.Equal(t, 1, calls)
	assert.Equal(t, int64(1), pool.Stats().AllocatedDiskMB,
		"the profile admitted by the aggregate gate must be the profile committed")
}

func TestResourcePool_ReplaceResolvedAllPublishesOneAtomicGeneration(t *testing.T) {
	resolverCalls := 0
	pool := NewResourcePool(20, 20, 20, func(string) (SKUProfile, error) {
		resolverCalls++
		return SKUProfile{}, assert.AnError
	}, &TenantQuotaConfig{MaxCPUCores: 20, MaxMemoryMB: 20, MaxDiskMB: 20})
	old := SKUResourceSnapshot{SKU: "old", CPUCores: 2, MemoryMB: 2, DiskMB: 2}
	unrelated := SKUResourceSnapshot{SKU: "other", CPUCores: 1, MemoryMB: 1, DiskMB: 1}
	replacement := SKUResourceSnapshot{SKU: "new", CPUCores: 3, MemoryMB: 3, DiskMB: 3}
	require.NoError(t, pool.TryAllocateResolved("lease-app-0", "tenant-a", old))
	require.NoError(t, pool.TryAllocateResolved("lease-app-1", "tenant-a", old))
	require.NoError(t, pool.TryAllocateResolved("other-app-0", "tenant-b", unrelated))

	require.NoError(t, pool.ReplaceResolvedAll(
		[]string{"lease-app-0", "lease-app-1"},
		[]ResolvedAdoptInstance{
			{ID: "lease-next-0", Resources: replacement},
			{ID: "lease-next-1", Resources: replacement},
		},
		"tenant-a",
	))

	assert.Zero(t, resolverCalls, "replacement must consume only its durable snapshots")
	assert.Nil(t, pool.GetAllocation("lease-app-0"))
	assert.Nil(t, pool.GetAllocation("lease-app-1"))
	assert.Equal(t, &ResourceAllocation{
		LeaseUUID: "lease-next-0", Tenant: "tenant-a", SKU: "new",
		CPUCores: 3, MemoryMB: 3, DiskMB: 3,
	}, pool.GetAllocation("lease-next-0"))
	assert.NotNil(t, pool.GetAllocation("other-app-0"), "unrelated authority must remain")
	assert.Equal(t, ResourceStats{
		TotalCPU: 20, TotalMemoryMB: 20, TotalDiskMB: 20,
		AllocatedCPU: 7, AllocatedMemoryMB: 7, AllocatedDiskMB: 7,
		AllocationCount: 3,
	}, pool.Stats())
	assert.Equal(t, float64(6), pool.TenantStats("tenant-a").AllocatedCPU)
	assert.Equal(t, int64(6), pool.TenantStats("tenant-a").AllocatedMemoryMB)
	assert.Equal(t, int64(6), pool.TenantStats("tenant-a").AllocatedDiskMB)
}

func TestResourcePool_ReplaceResolvedAllRollsBackExactAccounting(t *testing.T) {
	tests := []struct {
		name         string
		replacements []ResolvedAdoptInstance
		wantError    string
	}{
		{
			name: "later capacity failure",
			replacements: []ResolvedAdoptInstance{
				{ID: "candidate-0", Resources: SKUResourceSnapshot{SKU: "small", CPUCores: 3, MemoryMB: 3, DiskMB: 3}},
				{ID: "candidate-1", Resources: SKUResourceSnapshot{SKU: "large", CPUCores: 4, MemoryMB: 4, DiskMB: 4}},
			},
			wantError: "insufficient CPU",
		},
		{
			name: "duplicate candidate id",
			replacements: []ResolvedAdoptInstance{
				{ID: "candidate", Resources: SKUResourceSnapshot{SKU: "small", CPUCores: 1, MemoryMB: 1, DiskMB: 1}},
				{ID: "candidate", Resources: SKUResourceSnapshot{SKU: "small", CPUCores: 1, MemoryMB: 1, DiskMB: 1}},
			},
			wantError: "already has allocated resources",
		},
		{
			name: "candidate collides with unrelated allocation",
			replacements: []ResolvedAdoptInstance{
				{ID: "other-app-0", Resources: SKUResourceSnapshot{SKU: "small", CPUCores: 1, MemoryMB: 1, DiskMB: 1}},
			},
			wantError: "already has allocated resources",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolverCalls := 0
			pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
				resolverCalls++
				return SKUProfile{}, assert.AnError
			}, &TenantQuotaConfig{MaxCPUCores: 10, MaxMemoryMB: 10, MaxDiskMB: 10})
			old := SKUResourceSnapshot{SKU: "old", CPUCores: 2, MemoryMB: 2, DiskMB: 2}
			other := SKUResourceSnapshot{SKU: "other", CPUCores: 4, MemoryMB: 4, DiskMB: 4}
			require.NoError(t, pool.TryAllocateResolved("lease-app-0", "tenant-a", old))
			require.NoError(t, pool.TryAllocateResolved("lease-app-1", "tenant-a", old))
			require.NoError(t, pool.TryAllocateResolved("other-app-0", "tenant-b", other))
			require.NoError(t, pool.SetRetainedDisk(1))
			statsBefore := pool.Stats()
			tenantABefore := pool.TenantStats("tenant-a")
			tenantBBefore := pool.TenantStats("tenant-b")
			allocationsBefore := map[string]*ResourceAllocation{
				"lease-app-0": pool.GetAllocation("lease-app-0"),
				"lease-app-1": pool.GetAllocation("lease-app-1"),
				"other-app-0": pool.GetAllocation("other-app-0"),
			}

			err := pool.ReplaceResolvedAll(
				[]string{"lease-app-0", "lease-app-1"}, tt.replacements, "tenant-a",
			)
			require.ErrorContains(t, err, tt.wantError)
			assert.Zero(t, resolverCalls, "replacement must consume only its durable snapshots")
			assert.Equal(t, statsBefore, pool.Stats())
			assert.Equal(t, tenantABefore, pool.TenantStats("tenant-a"))
			assert.Equal(t, tenantBBefore, pool.TenantStats("tenant-b"))
			for id, allocation := range allocationsBefore {
				assert.Equal(t, allocation, pool.GetAllocation(id), "allocation %s", id)
			}
			for _, replacement := range tt.replacements {
				if _, existed := allocationsBefore[replacement.ID]; !existed {
					assert.Nil(t, pool.GetAllocation(replacement.ID), "candidate %s leaked", replacement.ID)
				}
			}
		})
	}
}

func TestResourcePool_ResetRejectsInvalidAggregateAtomically(t *testing.T) {
	pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 1}, nil
	}, nil)
	require.NoError(t, pool.TryAllocate("existing", "sku", "tenant-a"))
	wantStats := pool.Stats()

	tests := []struct {
		name        string
		allocations []ResourceAllocation
		wantErr     string
	}{
		{
			name:        "non-finite CPU",
			allocations: []ResourceAllocation{{LeaseUUID: "bad", CPUCores: math.NaN(), MemoryMB: 1, DiskMB: 1}},
			wantErr:     `rebuild resource allocations: allocation "bad" CPU must be finite`,
		},
		{
			name: "memory overflow",
			allocations: []ResourceAllocation{
				{LeaseUUID: "a", CPUCores: 1, MemoryMB: math.MaxInt64, DiskMB: 1},
				{LeaseUUID: "b", CPUCores: 1, MemoryMB: 1, DiskMB: 1},
			},
			wantErr: "rebuild resource allocations: aggregate memory exceeds supported range",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.EqualError(t, pool.Reset(tt.allocations), tt.wantErr)
			assert.Equal(t, wantStats, pool.Stats(), "a rejected reset must not publish partial accounting")
			assert.NotNil(t, pool.GetAllocation("existing"))
		})
	}
}

func TestResourcePool_ResetAcceptsStatelessZeroDiskAllocation(t *testing.T) {
	pool := NewResourcePool(10, 10, 10, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 1, DiskMB: 0}, nil
	}, nil)
	require.NoError(t, pool.Reset([]ResourceAllocation{
		{LeaseUUID: "stateless-app-0", Tenant: "tenant-a", SKU: "stateless", CPUCores: 1, MemoryMB: 1, DiskMB: 0},
	}))

	stats := pool.Stats()
	assert.Equal(t, 1, stats.AllocationCount)
	assert.Equal(t, float64(1), stats.AllocatedCPU)
	assert.Equal(t, int64(1), stats.AllocatedMemoryMB)
	assert.Zero(t, stats.AllocatedDiskMB)
	assert.Zero(t, pool.TenantStats("tenant-a").AllocatedDiskMB)

	// Recovery must leave the pool usable for another zero-disk instance rather
	// than merely accepting the rebuilt snapshot.
	require.NoError(t, pool.TryAllocate("stateless-app-1", "stateless", "tenant-a"))
	assert.Equal(t, 2, pool.Stats().AllocationCount)
}

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
		require.NoError(t, pool.Reset(allocations))

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
		require.NoError(t, pool.Reset([]ResourceAllocation{
			{LeaseUUID: "lease-2", Tenant: "tenant-b", SKU: "small", CPUCores: 1.0, MemoryMB: 512, DiskMB: 1024},
		}))

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
		require.NoError(t, pool.Reset([]ResourceAllocation{
			{LeaseUUID: "existing", Tenant: "tenant-a", SKU: "large", CPUCores: 7.5, MemoryMB: 4096, DiskMB: 8192},
		}))

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

func TestRetainedDiskCountsAgainstPool(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}
	// Total disk 2048 MB.
	p := NewResourcePool(8, 8192, 2048, resolver, nil)

	// Reserve 1024 MB as retained (a soft-deleted volume still on disk).
	require.NoError(t, p.SetRetainedDisk(1024))

	// A 1024 MB live allocation exactly fills the remaining headroom.
	require.NoError(t, p.TryAllocate("lease-1", "sku", "tenant"))

	// A second 1024 MB allocation must be denied:
	// 1024 (live) + 1024 (retained) + 1024 (new) > 2048.
	err := p.TryAllocate("lease-2", "sku", "tenant")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient disk")
}

func TestStatsIncludesRetainedDisk(t *testing.T) {
	resolver := func(string) (SKUProfile, error) { return SKUProfile{}, nil }
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(1500))

	s := p.Stats()
	assert.Equal(t, int64(1500), s.RetainedDiskMB)
	assert.Equal(t, int64(4096-1500), s.AvailableDiskMB())
}

func TestSetRetainedDiskRejectsNegativeAndPreservesProjection(t *testing.T) {
	resolver := func(string) (SKUProfile, error) { return SKUProfile{}, nil }
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(500))
	require.ErrorContains(t, p.SetRetainedDisk(-500), "must be non-negative")
	assert.Equal(t, int64(500), p.Stats().RetainedDiskMB,
		"invalid recovery data must not erase the last authoritative projection")
}

func TestResetPreservesRetainedDisk(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(1024))
	// Reset rebuilds live allocations; it must NOT clobber the retained projection
	// (the backend re-pushes it via refreshRetentionAccounting after recover, but
	// Reset itself owns only live state).
	require.NoError(t, p.Reset([]ResourceAllocation{{LeaseUUID: "l1", SKU: "sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}))
	assert.Equal(t, int64(1024), p.Stats().RetainedDiskMB)
}

func resetPreservingResolver() SKUResolver {
	return func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}
}

func TestResetPreserving_NilKeepEqualsReset(t *testing.T) {
	list := []ResourceAllocation{{LeaseUUID: "l1", Tenant: "t1", SKU: "sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}

	a := NewResourcePool(8, 8192, 8192, resetPreservingResolver(), nil)
	require.NoError(t, a.TryAllocate("stale-0", "sku", "t9")) // prior state that must be cleared
	require.NoError(t, a.Reset(list))

	b := NewResourcePool(8, 8192, 8192, resetPreservingResolver(), nil)
	require.NoError(t, b.TryAllocate("stale-0", "sku", "t9"))
	require.NoError(t, b.ResetPreserving(list, nil))

	assert.Equal(t, a.Stats(), b.Stats(), "nil keep must behave identically to Reset")
	assert.Nil(t, b.GetAllocation("stale-0"), "nil keep preserves nothing")
}

func TestResetPreserving_RetainsMarkedEntryAbsentFromNewList(t *testing.T) {
	p := NewResourcePool(8, 8192, 8192, resetPreservingResolver(), nil)
	require.NoError(t, p.TryAllocate("inflight-app-0", "sku", "t1")) // the in-flight reservation to keep

	// Rebuild from a snapshot that OMITS the in-flight lease (no container yet),
	// but mark it to keep — mirroring recoverState dropping an in-flight lease
	// from the container-derived list.
	require.NoError(t, p.ResetPreserving(
		[]ResourceAllocation{{LeaseUUID: "ready-app-0", Tenant: "t2", SKU: "sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		func(key string) bool { return key == "inflight-app-0" },
	))

	s := p.Stats()
	assert.Equal(t, 2, s.AllocationCount, "both the rebuilt Ready lease and the preserved in-flight lease")
	assert.Equal(t, int64(2048), s.AllocatedDiskMB)
	assert.Equal(t, 2.0, s.AllocatedCPU)
	assert.Equal(t, int64(1024), s.AllocatedMemoryMB)
	assert.NotNil(t, p.GetAllocation("inflight-app-0"), "preserved entry survives")
	assert.Equal(t, int64(1024), p.TenantStats("t1").AllocatedDiskMB, "preserved lease's per-tenant usage is rebuilt")
}

func TestResetPreserving_PreservedWinsOverSameKeyInList_NoDoubleCount(t *testing.T) {
	p := NewResourcePool(8, 8192, 8192, resetPreservingResolver(), nil)
	require.NoError(t, p.TryAllocate("dup-app-0", "sku", "t1"))

	// The same key appears BOTH in the rebuild list and is marked to keep. It must
	// be counted exactly once (the preserved entry wins).
	require.NoError(t, p.ResetPreserving(
		[]ResourceAllocation{{LeaseUUID: "dup-app-0", Tenant: "t1", SKU: "sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		func(key string) bool { return key == "dup-app-0" },
	))

	s := p.Stats()
	assert.Equal(t, 1, s.AllocationCount, "same key must not double-count")
	assert.Equal(t, int64(1024), s.AllocatedDiskMB)
	assert.Equal(t, 1.0, s.AllocatedCPU)
	assert.Equal(t, int64(1024), p.TenantStats("t1").AllocatedDiskMB, "per-tenant usage counted once")
}

func TestResetPreserving_DropsUnmarkedEntryAbsentFromNewList(t *testing.T) {
	p := NewResourcePool(8, 8192, 8192, resetPreservingResolver(), nil)
	require.NoError(t, p.TryAllocate("stale-app-0", "sku", "t1"))

	// keep matches nothing and the stale entry is absent from the new list → dropped
	// (a Failed/Ready lease whose containers are gone must not be over-preserved).
	require.NoError(t, p.ResetPreserving(nil, func(key string) bool { return false }))

	s := p.Stats()
	assert.Equal(t, 0, s.AllocationCount, "unmarked entry absent from the rebuild list is dropped")
	assert.Equal(t, int64(0), s.AllocatedDiskMB)
	assert.Nil(t, p.GetAllocation("stale-app-0"))
}

func TestAvailableDiskMB_ClampsToZero(t *testing.T) {
	s := ResourceStats{TotalDiskMB: 1000, RetainedDiskMB: 2000}
	assert.Equal(t, int64(0), s.AvailableDiskMB(), "available disk must clamp to 0, never negative (e.g. after a total_disk_mb shrink)")
}

func TestTryAllocate_AvailableNeverNegativeInError(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 100}, nil
	}
	p := NewResourcePool(8, 8192, 1000, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(2000)) // retained alone exceeds total (e.g. after a total_disk_mb shrink)
	err := p.TryAllocate("l1", "sku", "t1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "have 0 MB available", "available must clamp to 0, never negative")
}

func TestTryAllocateResolved_UsesPinnedProfileWithoutConsultingResolver(t *testing.T) {
	resolverCalls := 0
	p := NewResourcePool(8, 8192, 8192, func(string) (SKUProfile, error) {
		resolverCalls++
		return SKUProfile{}, fmt.Errorf("mutable resolver must not be consulted")
	}, nil)
	pinned := SKUResourceSnapshot{
		SKU: "removed-sku", CPUCores: 1.25, MemoryMB: 768, ScratchDiskMB: 1536,
	}

	require.NoError(t, p.TryAllocateResolved("lease-1", "tenant-a", pinned))
	assert.Zero(t, resolverCalls)
	assert.Equal(t, &ResourceAllocation{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		SKU:       "removed-sku",
		CPUCores:  1.25,
		MemoryMB:  768,
		DiskMB:    1536,
	}, p.GetAllocation("lease-1"))
}

func TestTryAllocateAdoptAllResolved_UsesPinnedProfilesAndRollsBackAtomically(t *testing.T) {
	resolverCalls := 0
	newPool := func(cpu float64) *ResourcePool {
		return NewResourcePool(cpu, 8192, 4096, func(string) (SKUProfile, error) {
			resolverCalls++
			return SKUProfile{}, fmt.Errorf("mutable resolver must not be consulted")
		}, nil)
	}
	instances := []ResolvedAdoptInstance{
		{ID: "restore-0", Resources: SKUResourceSnapshot{SKU: "removed-sku", CPUCores: 1, MemoryMB: 256, ScratchDiskMB: 1024}},
		{ID: "restore-1", Resources: SKUResourceSnapshot{SKU: "removed-sku", CPUCores: 1, MemoryMB: 256, ScratchDiskMB: 1024}},
	}

	p := newPool(8)
	require.NoError(t, p.TryAllocateAdoptAllResolved(instances, "tenant-a", 2048))
	assert.Zero(t, resolverCalls)
	assert.Equal(t, 2, p.Stats().AllocationCount)
	assert.Equal(t, int64(2048), p.Stats().AllocatedDiskMB)

	p = newPool(1.5)
	require.ErrorContains(t,
		p.TryAllocateAdoptAllResolved(instances, "tenant-a", 2048),
		"insufficient CPU",
	)
	assert.Zero(t, resolverCalls)
	assert.Equal(t, 0, p.Stats().AllocationCount,
		"a later pinned-profile failure must roll back the entire batch")
}

func TestTryAllocateAdoptAll_SkipsPerInstanceDiskGate(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: 200}, nil
	}
	p := NewResourcePool(8, 8192, 1000, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(900)) // 900 retained (other leases) + a fresh 200 would be 1100 > 1000

	// A normal provision is correctly denied (would over-commit disk).
	require.Error(t, p.TryAllocate("fresh", "sku", "t1"))
	// A same-tier adopt skips the disk gate and succeeds — passing the retained
	// footprint (200) equal to the new tier makes the promote delta 0.
	require.NoError(t, p.TryAllocateAdoptAll([]AdoptInstance{{ID: "restore-0", SKU: "sku"}}, "t1", 200))
	// ... and still records the disk in live accounting.
	assert.Equal(t, int64(200), p.Stats().AllocatedDiskMB)
}

func TestTryAllocateAdoptAll_StillGatesCPU(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 10, MemoryMB: 256, DiskMB: 200}, nil // 10 cores > 8 total
	}
	p := NewResourcePool(8, 8192, 1000, resolver, nil)
	require.Error(t, p.TryAllocateAdoptAll([]AdoptInstance{{ID: "restore-0", SKU: "sku"}}, "t1", 0),
		"adopt must still gate CPU/memory (new containers)")
	assert.Equal(t, float64(0), p.Stats().AllocatedCPU, "a rejected adopt reserves nothing")
}

// TestTryAllocateAdoptAll_GatesAggregateDelta is the ENG-545 regression at the pool
// level: a restore's disk capacity is gated once against the AGGREGATE promote
// delta (the pool's own new total minus the caller's oldRetainedDiskMB), never per
// adopted volume, so a fitting multi-volume promote is admitted rather than
// double-counted and rejected.
func TestTryAllocateAdoptAll_GatesAggregateDelta(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: 2048}, nil // new tier
	}
	// total 4096; the lease under restore has a retained footprint of 2048 (2×1024
	// old) in the projection. Two instances → the pool computes new = 2×2048 = 4096.
	newPool := func() *ResourcePool {
		p := NewResourcePool(8, 8192, 4096, resolver, nil)
		require.NoError(t, p.SetRetainedDisk(2048))
		return p
	}
	two := []AdoptInstance{{ID: "r-0", SKU: "sku"}, {ID: "r-1", SKU: "sku"}}

	// Fitting multi-volume promote: new(4096) - oldRetained(2048) = delta 2048;
	// 0 + retained(2048) + 2048 == 4096 == total → admitted. A per-volume gate would
	// have double-counted the retained bytes and rejected the second instance.
	p := newPool()
	require.NoError(t, p.TryAllocateAdoptAll(two, "t1", 2048),
		"a fitting multi-volume promote must be admitted")
	assert.Equal(t, int64(4096), p.Stats().AllocatedDiskMB, "both instances reserved")

	// A smaller old footprint enlarges the delta past capacity → rejected, nothing
	// reserved: new(4096) - oldRetained(2047) = delta 2049 > 2048 available.
	p = newPool()
	err := p.TryAllocateAdoptAll(two, "t1", 2047)
	require.Error(t, err, "a promote delta exceeding capacity must be gated (ENG-545)")
	assert.Contains(t, err.Error(), "insufficient disk")
	assert.Equal(t, int64(0), p.Stats().AllocatedDiskMB, "a rejected batch reserves nothing")

	// Same-tier (oldRetained == new) and demote (oldRetained > new) give delta <= 0
	// and always pass, even at zero apparent headroom.
	p = newPool()
	require.NoError(t, p.TryAllocateAdoptAll(two, "t1", 4096), "same-tier restore must pass")
	p = newPool()
	require.NoError(t, p.TryAllocateAdoptAll(two, "t1", 4608), "demote restore must pass")
}

// TestTryAllocateAdoptAll_AtomicNoOverCommit is the PR #184 review regression: the
// aggregate disk gate and the per-instance reservations run under a single lock, so
// an allocation that consumed headroom before the batch is seen by the batch's gate
// — the restore is rejected rather than over-committing the pool. A separate
// headroom-check-then-adopt design over-committed in exactly this scenario.
func TestTryAllocateAdoptAll_AtomicNoOverCommit(t *testing.T) {
	resolver := func(sku string) (SKUProfile, error) {
		switch sku {
		case "big":
			return SKUProfile{CPUCores: 0.1, MemoryMB: 16, DiskMB: 2048}, nil
		case "filler":
			return SKUProfile{CPUCores: 0.1, MemoryMB: 16, DiskMB: 1024}, nil
		}
		return SKUProfile{}, fmt.Errorf("unknown sku %q", sku)
	}
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(2048)) // the lease under restore, old footprint

	// A fresh provision consumes 1024 of the headroom the restore would need.
	require.NoError(t, p.TryAllocate("intruder", "filler", "t2"))

	// The restore batch (delta 2048) now sees allocatedDisk(1024) + retained(2048) +
	// 2048 = 5120 > 4096 and is rejected with nothing reserved — no over-commit.
	err := p.TryAllocateAdoptAll([]AdoptInstance{{ID: "r-0", SKU: "big"}, {ID: "r-1", SKU: "big"}}, "t1", 2048)
	require.Error(t, err, "batch must re-check capacity atomically against the concurrent allocation")

	s := p.Stats()
	assert.LessOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, s.TotalDiskMB, "pool must never be over-committed")
	assert.Equal(t, int64(1024), s.AllocatedDiskMB, "only the intruder is reserved; the rejected restore reserved nothing")
}

// TestTryAllocateAdoptAll_AllOrNothing verifies the batch is atomic on failure: if
// a later instance cannot be reserved (here the 3rd exhausts CPU), the instances
// already reserved in the call are rolled back, leaving the pool untouched.
func TestTryAllocateAdoptAll_AllOrNothing(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 3, MemoryMB: 16, DiskMB: 10}, nil // 3 CPU each; 3rd needs 9 > 8
	}
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	three := []AdoptInstance{{ID: "r-0", SKU: "s"}, {ID: "r-1", SKU: "s"}, {ID: "r-2", SKU: "s"}}
	require.Error(t, p.TryAllocateAdoptAll(three, "t1", 0), "the 3rd instance must exhaust CPU")
	s := p.Stats()
	assert.Equal(t, float64(0), s.AllocatedCPU, "partial reservations must be rolled back")
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "partial reservations must be rolled back")
	assert.Equal(t, 0, s.AllocationCount, "no allocation may remain after an all-or-nothing failure")
}

// TestTryAllocateAdoptAll_ConcurrentNoOverCommit stresses the atomicity guarantee:
// many goroutines interleave fresh provisions and restore batches against a tight
// pool, and the invariant allocatedDisk+retainedDisk <= totalDisk must never be
// observed violated (each restore here has no retained footprint of its own, so
// delta == reserved and no per-lease double-count is expected). Run with -race and
// -count to exercise the interleavings.
func TestTryAllocateAdoptAll_ConcurrentNoOverCommit(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 0.001, MemoryMB: 1, DiskMB: 256}, nil
	}
	p := NewResourcePool(1000, 1_000_000, 4096, resolver, nil)
	require.NoError(t, p.SetRetainedDisk(1024)) // static footprint of other (non-restoring) leases

	violation := make(chan string, 1)
	flag := func() {
		s := p.Stats()
		if s.AllocatedDiskMB+s.RetainedDiskMB > s.TotalDiskMB {
			select {
			case violation <- fmt.Sprintf("over-commit: alloc=%d retained=%d total=%d",
				s.AllocatedDiskMB, s.RetainedDiskMB, s.TotalDiskMB):
			default:
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_ = p.TryAllocate(fmt.Sprintf("prov-%d", i), "s", "t")
			} else {
				_ = p.TryAllocateAdoptAll([]AdoptInstance{
					{ID: fmt.Sprintf("r-%d-0", i), SKU: "s"},
					{ID: fmt.Sprintf("r-%d-1", i), SKU: "s"},
				}, "t", 0) // oldRetained 0; the pool computes new = 2×256 = 512 itself
			}
			flag()
		}(i)
	}
	wg.Wait()
	flag()

	select {
	case msg := <-violation:
		t.Fatal(msg)
	default:
	}
}

// TestTryAllocateAdoptAll_GatesNewFromPoolResolver is the PR #184 review regression:
// the pool computes the new total from its OWN resolver (the values it reserves),
// so a caller cannot under-gate disk by reporting a smaller new footprint. Even
// with a zero retained hint, a restore whose real (pool-resolved) footprint exceeds
// capacity is rejected with nothing reserved — no over-commit.
func TestTryAllocateAdoptAll_GatesNewFromPoolResolver(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 0.1, MemoryMB: 16, DiskMB: 2048}, nil
	}
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	require.NoError(t, p.TryAllocate("filler", "s", "t0")) // allocatedDisk = 2048; 2048 available

	// The pool resolves 2×2048 = 4096 for the batch itself, so it gates the true
	// footprint against the 2048 available and rejects — the caller cannot make it
	// under-gate by passing a small oldRetained hint.
	err := p.TryAllocateAdoptAll(
		[]AdoptInstance{{ID: "r-0", SKU: "s"}, {ID: "r-1", SKU: "s"}}, "t1", 0)
	require.Error(t, err, "the pool must gate the new total from its own resolver, not a caller value")
	s := p.Stats()
	assert.LessOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, s.TotalDiskMB, "no over-commit")
	assert.Equal(t, int64(2048), s.AllocatedDiskMB, "only the filler is reserved")
}
