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
	require.Equal(t, before, pool.ListAllocations())
	require.NoError(t, pool.ResetConservatively(before), "recovery remains available")
	require.NoError(t, pool.SetRetainedDisk(512), "retained accounting remains available")
	require.True(t, pool.Stats().AccountingHeld)
	require.Zero(t, pool.Stats().AvailableCPU())
	require.Zero(t, pool.Stats().AvailableMemoryMB())
	require.Zero(t, pool.Stats().AvailableDiskMB())
	load, err := pool.Stats().RoutingLoadStats()
	require.ErrorIs(t, err, ErrResourceAccountingIncomplete)
	require.Nil(t, load, "an incomplete ledger must not issue a routable load snapshot")

	first.Release()
	copyOfFirst.Release()
	require.True(t, pool.Stats().AccountingHeld, "releasing copies cannot consume another observer's hold")
	pool.Release(existing)
	require.Empty(t, pool.ListAllocations(), "deprovision remains available")
	second.Release()
	require.False(t, pool.Stats().AccountingHeld)
	require.NoError(t, pool.TryAllocate("new", "small", "tenant"))
	load, err = pool.Stats().RoutingLoadStats()
	require.NoError(t, err)
	require.Equal(t, float64(8), load.TotalCPUCores)
	require.Equal(t, profile.CPUCores, load.AllocatedCPUCores)
	require.Equal(t, 1, load.ActiveContainers)
}

// A disk hold refuses only what needs disk: every disk gate refuses a
// positive need with the typed error, a diskless allocation and a no-growth
// adoption stay admissible, and SetRetainedDisk cannot clear it.
func TestDiskAccountingHoldRefusesOnlyDiskBearingAllocations(t *testing.T) {
	profiles := map[string]SKUProfile{
		"stateful":  {CPUCores: 1, MemoryMB: 128, DiskMB: 256},
		"stateless": {CPUCores: 1, MemoryMB: 128, DiskMB: 0},
	}
	pool := NewResourcePool(8, 8192, 16384,
		func(sku string) (SKUProfile, error) { return profiles[sku], nil }, nil)
	first := pool.HoldUnsizedDiskFootprint()
	copyOfFirst := first
	second := pool.HoldUnsizedDiskFootprint()

	stateful := SKUResourceSnapshot{SKU: "stateful", CPUCores: 1, MemoryMB: 128, DiskMB: 256}
	require.ErrorIs(t, pool.TryAllocate("disk-0", "stateful", "tenant"), ErrDiskAccountingIncomplete)
	require.ErrorIs(t, pool.TryAllocateResolved("disk-1", "tenant", stateful), ErrDiskAccountingIncomplete)
	require.ErrorIs(t, pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "promote-0", SKU: "stateful"}}, "tenant", 0),
		ErrDiskAccountingIncomplete, "an adoption that grows disk is refused")
	require.NoError(t, pool.TryAllocateAdoptAll([]AdoptInstance{{ID: "same-0", SKU: "stateful"}}, "tenant", 256),
		"an adoption that adds no disk stays admissible")
	require.NoError(t, pool.TryAllocate("diskless-0", "stateless", "tenant"),
		"an allocation that needs no disk stays admissible")
	require.NoError(t, pool.SetRetainedDisk(512))

	stats := pool.Stats()
	require.True(t, stats.DiskAccountingHeld)
	require.False(t, stats.AccountingHeld, "only disk is withheld")
	require.Zero(t, stats.AvailableDiskMB())
	require.Positive(t, stats.AvailableCPU())
	_, err := stats.RoutingLoadStats()
	require.NoError(t, err, "routing load does not depend on disk")

	first.Release()
	copyOfFirst.Release()
	require.True(t, pool.Stats().DiskAccountingHeld, "releasing copies cannot consume another owner's hold")
	require.ErrorIs(t, pool.TryAllocate("disk-0", "stateful", "tenant"), ErrDiskAccountingIncomplete)
	second.Release()
	require.False(t, pool.Stats().DiskAccountingHeld)
	require.NoError(t, pool.TryAllocate("disk-0", "stateful", "tenant"))
	require.Equal(t, int64(16384-256-256-512), pool.Stats().AvailableDiskMB())
	DiskAccountingHold{}.Release() // the zero value grants nothing and is harmless
}
