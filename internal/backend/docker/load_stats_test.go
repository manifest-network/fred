package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackend_GetLoadStats verifies the in-process docker backend exposes the
// same CPU-load signal as its HTTP GET /stats endpoint (ENG-318): GetLoadStats
// wraps the resource pool's Stats().
func TestBackend_GetLoadStats(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)

	// Fresh backend: total CPU capacity configured, nothing allocated.
	stats, err := b.GetLoadStats(context.Background())
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Greater(t, stats.TotalCPUCores, float64(0), "pool exposes total CPU capacity")
	assert.Equal(t, float64(0), stats.AllocatedCPUCores)
	assert.Equal(t, 0, stats.ActiveContainers)

	// After allocating, the snapshot reflects the pool.
	require.NoError(t, b.pool.TryAllocate("lease-x-app-0", "docker-small", "tenant-a"))

	stats, err = b.GetLoadStats(context.Background())
	require.NoError(t, err)
	assert.Greater(t, stats.AllocatedCPUCores, float64(0), "allocation reflected in load stats")
	assert.Equal(t, 1, stats.ActiveContainers)

	// It mirrors Stats()/the /stats wire shape exactly (same source).
	ps := b.Stats()
	assert.Equal(t, ps.TotalCPU, stats.TotalCPUCores)
	assert.Equal(t, ps.AllocatedCPU, stats.AllocatedCPUCores)
	assert.Equal(t, ps.AllocationCount, stats.ActiveContainers)
}
