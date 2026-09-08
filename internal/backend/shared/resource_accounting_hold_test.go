package shared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceAccountingHoldExcludesAllAllocationPathsWithoutReplacingLedger(t *testing.T) {
	const existing = "550e8400-e29b-41d4-a716-446655440000-app-0"
	profile := SKUProfile{CPUCores: 1, MemoryMB: 128, DiskMB: 256}
	pool := NewResourcePool(8, 8192, 16384,
		func(string) (SKUProfile, error) { return profile, nil }, nil)
	require.NoError(t, pool.TryAllocate(existing, "small", "tenant"))
	before := pool.ListAllocations()
	first := pool.HoldUnaccountedFootprint()
	copyOfFirst := first
	second := pool.HoldUnaccountedFootprint()

	resources := SKUResourceSnapshot{SKU: "small", CPUCores: 1, MemoryMB: 128, DiskMB: 256}
	require.ErrorIs(t, pool.TryAllocate("new", "small", "tenant"), ErrResourceAccountingIncomplete)
	require.ErrorIs(t, pool.TryAllocateResolved("new", "tenant", resources), ErrResourceAccountingIncomplete)
	require.ErrorIs(t, pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "new", SKU: "small"}}, "tenant", 256), ErrResourceAccountingIncomplete)
	require.ErrorIs(t, pool.TryAllocateAdoptAllResolved([]ResolvedAdoptInstance{{ID: "new", Resources: resources}}, "tenant", 256), ErrResourceAccountingIncomplete)
	require.ErrorIs(t, pool.ReplaceResolvedAll([]string{existing}, []ResolvedAdoptInstance{{ID: "new", Resources: resources}}, "tenant"), ErrResourceAccountingIncomplete)
	require.Equal(t, before, pool.ListAllocations())
	require.NoError(t, pool.ResetConservatively(before), "recovery remains available")
	require.NoError(t, pool.SetRetainedDisk(512), "retained accounting remains available")
	require.True(t, pool.Stats().AccountingHeld)
	require.Zero(t, pool.Stats().AvailableCPU())
	require.Zero(t, pool.Stats().AvailableMemoryMB())
	require.Zero(t, pool.Stats().AvailableDiskMB())

	first.Release()
	copyOfFirst.Release()
	require.True(t, pool.Stats().AccountingHeld, "releasing copies cannot consume another observer's hold")
	pool.Release(existing)
	require.Empty(t, pool.ListAllocations(), "deprovision remains available")
	second.Release()
	require.False(t, pool.Stats().AccountingHeld)
	require.NoError(t, pool.TryAllocate("new", "small", "tenant"))
}
