package docker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestUpdateRetentionMetrics(t *testing.T) {
	updateRetentionMetrics(2048, 3) // 2048 MB, 3 retained leases
	assert.Equal(t, float64(2048)*(1<<20), testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(3), testutil.ToFloat64(retainedVolumes))
}

func TestSetStaticPoolMetrics(t *testing.T) {
	cfg := Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40}
	setStaticPoolMetrics(cfg)
	assert.Equal(t, float64(100)*(1<<20), testutil.ToFloat64(diskPoolBytes))
	assert.Equal(t, float64(40)*(1<<20), testutil.ToFloat64(retainedDiskCapBytes))
}
