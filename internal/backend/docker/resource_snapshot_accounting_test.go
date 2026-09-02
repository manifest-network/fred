package docker

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestRetentionAccounting_ImmutableSnapshotSurvivesSKUResizeAndRemoval(t *testing.T) {
	b, store := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	entry := retentionEntryFixture("lease-exact", "tenant-a", time.Now())
	snapshot, err := shared.BuildSKUResourceSnapshot(entry.Items, b.cfg.GetSKUProfile)
	require.NoError(t, err)
	entry.ResourceProfiles = snapshot
	require.NoError(t, store.Put(entry))

	// Repricing the current config cannot resize an already-retained footprint.
	withMicroSKU(b, 1)
	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)

	// Removing the profile entirely is also harmless for a snapshot-backed row.
	delete(b.cfg.SKUProfiles, "docker-micro")
	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
}

func TestReapingAccounting_ImmutableSnapshotSurvivesSKURemoval(t *testing.T) {
	b, store := newBackendWithRetention(t)
	withMicroSKU(b, 1536)

	entry := retentionEntryFixture("lease-reaping-exact", "tenant-a", time.Now())
	entry.Status = shared.RetentionStatusReaping
	snapshot, err := shared.BuildSKUResourceSnapshot(entry.Items, b.cfg.GetSKUProfile)
	require.NoError(t, err)
	entry.ResourceProfiles = snapshot
	require.NoError(t, store.Put(entry))
	delete(b.cfg.SKUProfiles, "docker-micro")

	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.Equal(t, int64(3072), b.pool.Stats().RetainedDiskMB)
}

func TestRetentionAccounting_AggregateOverflowKeepsLastProjection(t *testing.T) {
	b, store := newBackendWithRetention(t)
	require.NoError(t, b.pool.SetRetainedDisk(99))

	for _, leaseUUID := range []string{"lease-overflow-a", "lease-overflow-b"} {
		items := []backend.LeaseItem{{SKU: "huge", ServiceName: "app", Quantity: 1}}
		require.NoError(t, store.Put(shared.RetentionEntry{
			OriginalLeaseUUID: leaseUUID,
			Tenant:            "tenant-a",
			ProviderUUID:      "provider-a",
			Items:             items,
			ResourceProfiles: []shared.SKUResourceSnapshot{{
				SKU: "huge", CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64/2 + 1,
			}},
			Status:    shared.RetentionStatusActive,
			CreatedAt: time.Now(),
		}))
	}

	err := b.refreshRetentionAccountingChecked()
	require.ErrorContains(t, err, "projection overflow")
	require.Equal(t, int64(99), b.pool.Stats().RetainedDiskMB,
		"overflow must preserve the last attested projection, never wrap to a negative value")
}

func TestRetentionCap_UsesImmutableCloseSnapshotAfterSKUResize(t *testing.T) {
	items := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}}
	resourceProfiles := []shared.SKUResourceSnapshot{{
		SKU: "docker-micro", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024,
	}}

	t.Run("runtime growth cannot spuriously destroy old fitting footprint", func(t *testing.T) {
		b, _ := newBackendWithRetention(t)
		withMicroSKU(b, 4096)
		b.cfg.MaxRetainedDiskMB = 1536

		_, breached := b.breachRetentionCapsWithResourceProfiles(
			"tenant-a", "", items, resourceProfiles, resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.False(t, breached)
		_, mutableBreached := b.breachRetentionCaps(
			"tenant-a", "", items, resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.True(t, mutableBreached,
			"the regression control proves mutable config would choose the destructive branch")
	})

	t.Run("runtime shrink cannot under-enforce old footprint", func(t *testing.T) {
		b, _ := newBackendWithRetention(t)
		withMicroSKU(b, 1)
		b.cfg.MaxRetainedDiskMB = 512

		scope, breached := b.breachRetentionCapsWithResourceProfiles(
			"tenant-a", "", items, resourceProfiles, resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.True(t, breached)
		require.Equal(t, refuseScopeGlobal, scope)
		_, mutableBreached := b.breachRetentionCaps(
			"tenant-a", "", items, resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.False(t, mutableBreached,
			"the regression control proves mutable config would under-enforce the cap")
	})
}

func TestRetentionCap_InvalidOrUnresolvedIncomingFootprintFailsOpen(t *testing.T) {
	t.Run("snapshot arithmetic overflow", func(t *testing.T) {
		b, _ := newBackendWithRetention(t)
		b.cfg.MaxRetainedDiskMB = 1
		items := []backend.LeaseItem{{SKU: "huge", ServiceName: "app", Quantity: 2}}
		profiles := []shared.SKUResourceSnapshot{{
			SKU: "huge", CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64,
		}}

		_, breached := b.breachRetentionCapsWithResourceProfiles(
			"tenant-a", "", items, profiles, resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.False(t, breached, "invalid arithmetic is never destruction authority")
	})

	t.Run("legacy unknown SKU", func(t *testing.T) {
		b, _ := newBackendWithRetention(t)
		b.cfg.MaxRetainedDiskMB = 1
		_, breached := b.breachRetentionCaps(
			"tenant-a", "",
			[]backend.LeaseItem{{SKU: "removed", ServiceName: "app", Quantity: 1}},
			resolveTenantRetentionBudget(b.cfg, "tenant-a"),
		)
		require.False(t, breached, "unresolved legacy sizing is never destruction authority")
	})
}

func TestRecoveredSnapshotAllocations_DoNotConsultCurrentSKUConfig(t *testing.T) {
	items := []backend.LeaseItem{{SKU: "retired", ServiceName: "api", Quantity: 2}}
	snapshot := []shared.SKUResourceSnapshot{{
		SKU: "retired", CPUCores: 1.5, MemoryMB: 768, DiskMB: 4096,
	}}

	allocations, err := recoveredSnapshotAllocations("lease-a", "tenant-a", items, snapshot)
	require.NoError(t, err)
	require.Equal(t, []shared.ResourceAllocation{
		{
			LeaseUUID: "lease-a-api-0", Tenant: "tenant-a", SKU: "retired",
			CPUCores: 1.5, MemoryMB: 768, DiskMB: 4096,
		},
		{
			LeaseUUID: "lease-a-api-1", Tenant: "tenant-a", SKU: "retired",
			CPUCores: 1.5, MemoryMB: 768, DiskMB: 4096,
		},
	}, allocations)
}

func TestRecoveredSnapshotAllocations_AccountPinnedScratch(t *testing.T) {
	items := []backend.LeaseItem{{SKU: "retired-diskless", ServiceName: "api", Quantity: 2}}
	snapshot := []shared.SKUResourceSnapshot{{
		SKU: "retired-diskless", CPUCores: 0.25, MemoryMB: 192, ScratchDiskMB: 73,
	}}

	allocations, err := recoveredSnapshotAllocations("lease-scratch", "tenant-a", items, snapshot)
	require.NoError(t, err)
	require.Equal(t, int64(73), allocations[0].DiskMB)
	require.Equal(t, int64(73), allocations[1].DiskMB)

	_, err = recoveredSnapshotAllocations("lease-scratch", "tenant-a", items, []shared.SKUResourceSnapshot{{
		SKU: "retired-diskless", CPUCores: 0.25, MemoryMB: 192,
	}})
	require.ErrorContains(t, err, "no pinned scratch disk")
}

func TestRetentionCap_MixedLeaseSelectsOnlyDurableSnapshotRows(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	items := []backend.LeaseItem{{SKU: "stateful", ServiceName: "db", Quantity: 1}}
	fullSnapshot := []shared.SKUResourceSnapshot{
		{SKU: "diskless", CPUCores: 0.25, MemoryMB: 128, ScratchDiskMB: 64},
		{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
	}

	mb, unresolved, err := b.closeFootprintDiskMB(items, fullSnapshot)
	require.NoError(t, err)
	require.Empty(t, unresolved)
	require.Equal(t, int64(1024), mb)
}

func TestRetentionCap_RetainedScratchCountsPhysicallyButNotTowardPolicyCaps(t *testing.T) {
	b, store := newBackendWithRetention(t)
	const (
		leaseUUID = "lease-retained-scratch"
		tenant    = "tenant-a"
		partition = "partition-a"
	)
	scratchItems := []backend.LeaseItem{{
		SKU: "scratch", ServiceName: "app", Quantity: 2,
	}}
	scratchProfiles := []shared.SKUResourceSnapshot{{
		SKU: "scratch", CPUCores: 0.25, MemoryMB: 128, ScratchDiskMB: 73,
	}}
	entry := shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            tenant,
		Partition:         partition,
		ProviderUUID:      "provider-a",
		Items:             scratchItems,
		ResourceProfiles:  scratchProfiles,
		// Conservative classification retained only one of the two exact scratch
		// volumes. Physical accounting follows names, not desired quantity.
		RetainedVolumeNames: []string{
			retainedName(canonicalVolumeName(leaseUUID, "app", 1)),
		},
		Status:    shared.RetentionStatusActive,
		CreatedAt: time.Now(),
	}

	physicalMB, unresolved, err := b.retentionEntryDiskMB(entry)
	require.NoError(t, err)
	require.Empty(t, unresolved)
	require.Equal(t, int64(73), physicalMB)
	capMB, unresolved, err := b.retentionEntryCapDiskMB(entry)
	require.NoError(t, err)
	require.Empty(t, unresolved)
	require.Zero(t, capMB, "scratch is not durable retention entitlement")

	require.NoError(t, store.Put(entry))
	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.Equal(t, int64(73), b.pool.Stats().RetainedDiskMB,
		"the exact retained scratch path must remain physically reserved")
	b.cfg.MaxRetainedDiskMB = 1
	scope, breached := b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, scratchItems, scratchProfiles, retentionBudget{},
	)
	require.False(t, breached, "incoming scratch is not durable retention entitlement")
	require.Empty(t, scope)

	incomingItems := []backend.LeaseItem{{
		SKU: "durable", ServiceName: "db", Quantity: 1,
	}}
	incomingProfiles := []shared.SKUResourceSnapshot{{
		SKU: "durable", CPUCores: 0.5, MemoryMB: 256, DiskMB: 40,
	}}

	// Every scope consumes the same durable-only policy footprint. A 50 MB cap
	// admits the incoming 40 MB even though physical retained scratch makes the
	// host projection 113 MB; lowering the cap to 39 proves durable bytes still
	// enforce normally.
	b.cfg.MaxRetainedDiskMB = 50
	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles, retentionBudget{},
	)
	require.False(t, breached)
	require.Empty(t, scope)
	b.cfg.MaxRetainedDiskMB = 39
	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles, retentionBudget{},
	)
	require.True(t, breached)
	require.Equal(t, refuseScopeGlobal, scope)

	b.cfg.MaxRetainedDiskMB = 0
	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles, retentionBudget{DiskCapMB: 50},
	)
	require.False(t, breached)
	require.Empty(t, scope)
	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles, retentionBudget{DiskCapMB: 39},
	)
	require.True(t, breached)
	require.Equal(t, refuseScopeTenant, scope)

	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles,
		retentionBudget{MaxPartitions: 1, PerPartDiskMB: 50},
	)
	require.False(t, breached)
	require.Empty(t, scope)
	scope, breached = b.breachRetentionCapsWithResourceProfiles(
		tenant, partition, incomingItems, incomingProfiles,
		retentionBudget{MaxPartitions: 1, PerPartDiskMB: 39},
	)
	require.True(t, breached)
	require.Equal(t, refuseScopePartition, scope)
}

func TestCheckDemoteFit_UsesRetainedSnapshotAfterOldSKURemoval(t *testing.T) {
	called := false
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{
		UsageFn: func(context.Context, string) (int64, error) {
			called = true
			return 0, nil
		},
	}
	delete(b.cfg.SKUProfiles, "retired")
	record := &shared.RetentionEntry{
		OriginalLeaseUUID:   "source",
		Items:               []backend.LeaseItem{{SKU: "retired", ServiceName: "app", Quantity: 1}},
		ResourceProfiles:    []shared.SKUResourceSnapshot{{SKU: "retired", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}},
		RetainedVolumeNames: []string{gRetVol("source", "app", 0)},
	}
	newItems := []backend.LeaseItem{{SKU: "docker-medium", ServiceName: "app", Quantity: 1}}

	require.NoError(t, b.checkDemoteFit(
		context.Background(), record, newItems, gProfiles(b, "docker-medium"), b.logger,
	))
	require.False(t, called,
		"the persisted old cap proves this is a promotion even when the old SKU no longer exists in config")
}
