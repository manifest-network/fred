package shared

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestBuildSKUResourceSnapshotCanonicalizesAndResolvesEachSKUOnce(t *testing.T) {
	items := []backend.LeaseItem{
		{SKU: "z-large", Quantity: 1, ServiceName: "api"},
		{SKU: "a-small", Quantity: 2, ServiceName: "worker"},
		{SKU: "z-large", Quantity: 3, ServiceName: "jobs"},
	}
	profiles := map[string]SKUProfile{
		"a-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
		"z-large": {CPUCores: 2, MemoryMB: 4096, DiskMB: 8192},
	}
	resolved := map[string]int{}
	snapshot, err := BuildSKUResourceSnapshot(items, func(sku string) (SKUProfile, error) {
		resolved[sku]++
		profile, ok := profiles[sku]
		if !ok {
			return SKUProfile{}, errors.New("unknown SKU")
		}
		return profile, nil
	})
	require.NoError(t, err)
	require.Equal(t, []SKUResourceSnapshot{
		{SKU: "a-small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
		{SKU: "z-large", CPUCores: 2, MemoryMB: 4096, DiskMB: 8192},
	}, snapshot)
	assert.Equal(t, map[string]int{"a-small": 1, "z-large": 1}, resolved)
	require.NoError(t, ValidateSKUResourceSnapshot(items, snapshot))

	profile, ok := LookupSKUResourceSnapshot(snapshot, "z-large")
	require.True(t, ok)
	assert.Equal(t, profiles["z-large"], profile)
	_, ok = LookupSKUResourceSnapshot(snapshot, "missing")
	assert.False(t, ok)

	clone := CloneSKUResourceSnapshot(snapshot)
	clone[0].DiskMB = 1
	assert.EqualValues(t, 1024, snapshot[0].DiskMB)
}

func TestBuildSKUResourceSnapshotRejectsUnresolvableAndInvalidProfiles(t *testing.T) {
	items := []backend.LeaseItem{{SKU: "small", Quantity: 1, ServiceName: "app"}}
	_, err := BuildSKUResourceSnapshot(items, nil)
	require.ErrorContains(t, err, "resolver is required")

	_, err = BuildSKUResourceSnapshot(items, func(string) (SKUProfile, error) {
		return SKUProfile{}, errors.New("retired")
	})
	require.ErrorContains(t, err, "retired")

	_, err = BuildSKUResourceSnapshot(items, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: math.Inf(1), MemoryMB: 512}, nil
	})
	require.ErrorContains(t, err, "non-finite")
}

func TestValidateSKUResourceSnapshotRequiresExactCanonicalCoverage(t *testing.T) {
	items := []backend.LeaseItem{
		{SKU: "a", Quantity: 1, ServiceName: "api"},
		{SKU: "b", Quantity: 1, ServiceName: "worker"},
	}
	row := func(sku string) SKUResourceSnapshot {
		return SKUResourceSnapshot{SKU: sku, CPUCores: 1, MemoryMB: 512, DiskMB: 1024}
	}
	for _, test := range []struct {
		name     string
		snapshot []SKUResourceSnapshot
		want     string
	}{
		{name: "missing", snapshot: []SKUResourceSnapshot{row("a")}, want: "want exactly 2"},
		{name: "extra", snapshot: []SKUResourceSnapshot{row("a"), row("b"), row("c")}, want: "want exactly 2"},
		{name: "unreferenced", snapshot: []SKUResourceSnapshot{row("a"), row("c")}, want: "unreferenced SKU"},
		{name: "unsorted", snapshot: []SKUResourceSnapshot{row("b"), row("a")}, want: "strictly sorted"},
		{name: "duplicate", snapshot: []SKUResourceSnapshot{row("a"), row("a")}, want: "strictly sorted"},
		{name: "zero CPU", snapshot: []SKUResourceSnapshot{{SKU: "a", MemoryMB: 512}, row("b")}, want: "cpu_cores must be positive"},
		{name: "NaN CPU", snapshot: []SKUResourceSnapshot{{SKU: "a", CPUCores: math.NaN(), MemoryMB: 512}, row("b")}, want: "non-finite"},
		{name: "negative disk", snapshot: []SKUResourceSnapshot{{SKU: "a", CPUCores: 1, MemoryMB: 512, DiskMB: -1}, row("b")}, want: "disk_mb must be non-negative"},
		{name: "negative scratch", snapshot: []SKUResourceSnapshot{{SKU: "a", CPUCores: 1, MemoryMB: 512, ScratchDiskMB: -1}, row("b")}, want: "negative scratch_disk_mb"},
		{name: "durable and scratch", snapshot: []SKUResourceSnapshot{{SKU: "a", CPUCores: 1, MemoryMB: 512, DiskMB: 1, ScratchDiskMB: 1}, row("b")}, want: "cannot combine durable and scratch disk"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSKUResourceSnapshot(items, test.snapshot)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestSKUResourceSnapshotScratchRoundTripAndEffectiveDisk(t *testing.T) {
	scratch := SKUResourceSnapshot{
		SKU: "ephemeral", CPUCores: 0.5, MemoryMB: 256, ScratchDiskMB: 64,
	}
	encoded, err := json.Marshal(scratch)
	require.NoError(t, err)
	var decoded SKUResourceSnapshot
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, scratch, decoded)

	effective, err := decoded.EffectiveDiskMB()
	require.NoError(t, err)
	assert.EqualValues(t, 64, effective)
	row, ok := LookupSKUResourceSnapshotRow([]SKUResourceSnapshot{decoded}, "ephemeral")
	require.True(t, ok)
	assert.Equal(t, scratch, row)

	durable := SKUResourceSnapshot{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}
	effective, err = durable.EffectiveDiskMB()
	require.NoError(t, err)
	assert.EqualValues(t, 1024, effective)

	_, err = (SKUResourceSnapshot{
		SKU: "invalid", CPUCores: 1, MemoryMB: 1, DiskMB: 1, ScratchDiskMB: 1,
	}).EffectiveDiskMB()
	require.ErrorContains(t, err, "cannot combine durable and scratch disk")
}

func TestValidateSKUResourceSnapshotAllowsEmptyTopology(t *testing.T) {
	require.NoError(t, ValidateSKUResourceSnapshot(nil, nil))
}

func TestSumSKUResourceSnapshotDiskMBIsExactAndOverflowChecked(t *testing.T) {
	items := []backend.LeaseItem{
		{SKU: "a", Quantity: 2, ServiceName: "api"},
		{SKU: "b", Quantity: 3, ServiceName: "worker"},
	}
	snapshot := []SKUResourceSnapshot{
		{SKU: "a", CPUCores: 1, MemoryMB: 512, DiskMB: 10},
		{SKU: "b", CPUCores: 2, MemoryMB: 1024, DiskMB: 20},
	}
	total, err := SumSKUResourceSnapshotDiskMB(items, snapshot)
	require.NoError(t, err)
	assert.EqualValues(t, 80, total)

	// Scratch is live ephemeral capacity, never durable/retainable footprint.
	scratchOnly, err := SumSKUResourceSnapshotDiskMB(
		[]backend.LeaseItem{{SKU: "ephemeral", Quantity: 2, ServiceName: "cache"}},
		[]SKUResourceSnapshot{{
			SKU: "ephemeral", CPUCores: 1, MemoryMB: 256, ScratchDiskMB: 64,
		}},
	)
	require.NoError(t, err)
	assert.Zero(t, scratchOnly)

	overflowItems := []backend.LeaseItem{{SKU: "huge", Quantity: 2, ServiceName: "app"}}
	overflowSnapshot := []SKUResourceSnapshot{{
		SKU: "huge", CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64,
	}}
	_, err = SumSKUResourceSnapshotDiskMB(overflowItems, overflowSnapshot)
	require.ErrorContains(t, err, "overflows int64")

	_, err = SumSKUResourceSnapshotDiskMB(
		[]backend.LeaseItem{{SKU: "a", Quantity: 0, ServiceName: "app"}},
		[]SKUResourceSnapshot{{SKU: "a", CPUCores: 1, MemoryMB: 1}},
	)
	require.ErrorContains(t, err, "out of range")
}
