package docker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateRetentionMetrics(t *testing.T) {
	updateRetentionMetrics(2048, 3) // 2048 MB, 3 retained leases
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(3), testutil.ToFloat64(retainedLeases))
}

func TestSetStaticPoolMetrics(t *testing.T) {
	cfg := Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40}
	setStaticPoolMetrics(cfg)
	assert.Equal(t, float64(100)*bytesPerMiB, testutil.ToFloat64(diskPoolBytes))
	assert.Equal(t, float64(40)*bytesPerMiB, testutil.ToFloat64(retainedDiskCapBytes))
}

func TestSetStaticPoolMetrics_ResetsCapWhenUnset(t *testing.T) {
	setStaticPoolMetrics(Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40})
	require.Equal(t, float64(40)*bytesPerMiB, testutil.ToFloat64(retainedDiskCapBytes))
	// A later construction with no cap must reset the gauge to 0 (no stale value).
	setStaticPoolMetrics(Config{TotalDiskMB: 100, MaxRetainedDiskMB: 0})
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedDiskCapBytes))
}
