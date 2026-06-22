package docker

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// withMicroSKU sets a known stateful SKU on the test backend's config so the
// projection (which reads b.cfg.GetSKUProfile) resolves a deterministic DiskMB.
func withMicroSKU(b *Backend, diskMB int64) {
	b.cfg.SKUProfiles = map[string]shared.SKUProfile{
		"docker-micro": {CPUCores: 1, MemoryMB: 256, DiskMB: diskMB},
	}
}

func TestComputeRetainedDiskMB(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	// Two active retained leases: one with qty 2, one with qty 1 → 3 * 1024 MB.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now())))     // qty 2
	one := retentionEntryFixture("lease-b", "t1", time.Now())
	one.Items = []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	require.NoError(t, rs.Put(one))
	// A restoring record must NOT count toward the active projection.
	restoring := retentionEntryFixture("lease-c", "t1", time.Now())
	restoring.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(restoring))

	mb, count, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, int64(3*1024), mb)
	assert.Equal(t, 2, count)
}

func TestRefreshRetentionAccounting_PushesToPoolAndMetrics(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // qty 2 → 2048 MB

	b.refreshRetentionAccounting()
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retainedVolumes))

	// Remove the record and refresh → projection drops to 0.
	require.NoError(t, rs.Delete("lease-a"))
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedVolumes))
}

func TestLeaseDiskMB_UnknownSKUSkipped(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}, // 2048
		{SKU: "ghost-sku", Quantity: 5, ServiceName: "db"},     // unknown → skipped
	}
	assert.Equal(t, int64(2048), b.leaseDiskMB(items))
}

func TestRecoverRebuildsRetainedProjection(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	// 1024 (not 512 like the ordering test) is fine: this test exercises only the
	// computeRetainedDiskMB path (reads b.cfg.GetSKUProfile), never b.pool.TryAllocate,
	// so the pool's constructor-time resolver is not involved.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // 2048 MB

	// Wire ListManagedContainers to return no containers so recoverState can
	// run without a full Docker daemon; the test exercises the retained
	// projection rebuild only.
	b.docker = &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}

	// recoverState with no live containers must still rebuild the retained
	// projection from the store (pool.Reset zeroes live; refresh re-pushes
	// retained).
	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
}

// TestRestoreOrdering_NeverUnderCounts verifies that live+retained never dips
// below the lease's footprint during the claim window (active→restoring flip).
// Note: the pool resolver was bound at construction time to the original cfg
// (DiskMB=512 for docker-micro from defaultTestSKUProfiles), while
// b.cfg.GetSKUProfile (used by computeRetainedDiskMB) sees the withMicroSKU
// override. To keep the invariant consistent, this test uses DiskMB=512 so
// both the pool allocator and the retention projection agree on the per-unit
// cost. The fixture has qty=2 → footprint = 2 * 512 = 1024 MB.
func TestBreachRetentionCap(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	incoming := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}} // 2048 MB

	// Cap unset (0) → never breaches.
	b.cfg.MaxRetainedDiskMB = 0
	b.pool.SetRetainedDisk(1_000_000)
	assert.False(t, b.breachRetentionCap(incoming))

	// Cap 3000 MB, 2000 already retained: 2000 + 2048 > 3000 → breach.
	b.cfg.MaxRetainedDiskMB = 3000
	b.pool.SetRetainedDisk(2000)
	assert.True(t, b.breachRetentionCap(incoming))

	// Cap 5000 MB, 2000 already retained: 2000 + 2048 <= 5000 → no breach.
	b.cfg.MaxRetainedDiskMB = 5000
	b.pool.SetRetainedDisk(2000)
	assert.False(t, b.breachRetentionCap(incoming))
}

func TestRestoreOrdering_NeverUnderCounts(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	// Use 512 MB to match the pool resolver's view of docker-micro
	// (defaultTestSKUProfiles: DiskMB=512). computeRetainedDiskMB also
	// reads b.cfg.GetSKUProfile; withMicroSKU(b, 512) aligns both views.
	withMicroSKU(b, 512)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // active, 2 x 512 = 1024 MB
	b.refreshRetentionAccounting()
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)

	// Claim ordering: the restored lease is allocated LIVE before the record
	// flips active→restoring (which a refresh drops from the active projection).
	// Model that order; live+retained must never dip below the footprint
	// (under-count == over-admit, the dangerous direction).
	require.NoError(t, b.pool.TryAllocate("lease-a-web-0", "docker-micro", "t1")) // +512 live
	require.NoError(t, b.pool.TryAllocate("lease-a-web-1", "docker-micro", "t1")) // +512 live
	s := b.pool.Stats()
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024),
		"live+retained must never under-count during the claim window")

	// Record flips to restoring; refresh drops it from the active projection.
	entry := retentionEntryFixture("lease-a", "t1", time.Now())
	entry.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(entry))
	b.refreshRetentionAccounting()
	s = b.pool.Stats()
	assert.Equal(t, int64(0), s.RetainedDiskMB, "restoring record leaves the active projection")
	assert.Equal(t, int64(1024), s.AllocatedDiskMB, "bytes are now counted as live")
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024))
}

func TestRefuseToRetain_DestroysAndCounts(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // smaller than a single 2 x 1024 = 2048 MB lease
	b.pool.SetRetainedDisk(0)

	// A capturing fake volume manager records Destroy calls (mirrors the
	// fakeVolumeBackend pattern in testsupport_test.go).
	fv := &fakeVolumeBackend{}
	b.volumes = fv

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	require.True(t, b.breachRetentionCap(items), "a 2048 MB lease must breach a 1000 MB cap")

	before := testutil.ToFloat64(retentionRefusedTotal)
	canonical := []string{"fred-lease-z-web-0", "fred-lease-z-web-1"}
	errs := b.destroyOnRefuseToRetain(context.Background(), canonical, "lease-z", "t1", b.logger)

	assert.Empty(t, errs)
	assert.Equal(t, before+1, testutil.ToFloat64(retentionRefusedTotal), "production code increments the counter")
}
