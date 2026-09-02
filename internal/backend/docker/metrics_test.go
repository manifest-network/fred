package docker

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestUpdateRetentionMetrics(t *testing.T) {
	updateRetentionMetrics(2048, 3, 512, 1, 0) // 2048 MB admission total, 3 active, 512 MB reaping (1 record), 0 partitions
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(3), testutil.ToFloat64(retainedLeases))
	assert.Equal(t, float64(512)*bytesPerMiB, testutil.ToFloat64(retentionReapingBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retentionReapingLeases))
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

func TestSampleCloseIntentMetrics_ReportsAggregateCountAndOldestAge(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, true, "")
	defer closeCloseRecoveryBackend(t, b, stores)

	b.sampleCloseIntentMetrics(claim.CreatedAt().Add(2 * time.Minute))
	assert.Equal(t, float64(1), testutil.ToFloat64(pendingCloseIntents))
	assert.InDelta(t, 120, testutil.ToFloat64(oldestCloseIntentAgeSeconds), 0.1)

	_, err := stores.callbacks.ResolveCloseIntent(
		claim,
		backend.CallbackStatusDeprovisioned,
		"",
		false,
	)
	require.NoError(t, err)
	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(0), testutil.ToFloat64(pendingCloseIntents))
	assert.Equal(t, float64(0), testutil.ToFloat64(oldestCloseIntentAgeSeconds))
}
