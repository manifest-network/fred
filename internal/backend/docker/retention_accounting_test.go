package docker

import (
	"testing"
	"time"

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

	// Remove the record and refresh → projection drops to 0.
	require.NoError(t, rs.Delete("lease-a"))
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
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
