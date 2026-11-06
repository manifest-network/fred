package k3s

import (
	"os"
	"os/exec"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCounterSeriesExistAtStartup pins ENG-1116 for k3s-backend: every
// series of every labeled counter exists at 0 from package initialization.
// The assertions run in a fresh test process because other tests create
// series.
func TestCounterSeriesExistAtStartup(t *testing.T) {
	const child = "FRED_TEST_K3S_SERIES_FRESH_PROCESS"
	if os.Getenv(child) == "" {
		command := exec.CommandContext(t.Context(), os.Args[0],
			"-test.run=^TestCounterSeriesExistAtStartup$", "-test.count=1")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "fresh process:\n%s", output)
		return
	}
	for name, vec := range map[string]*prometheus.CounterVec{
		"provisions":          provisionsTotal,
		"callback deliveries": callbackDeliveryTotal,
	} {
		assert.Equal(t, 2, testutil.CollectAndCount(vec), name)
	}
	assert.Zero(t, testutil.ToFloat64(provisionOutcomes.rejected))
	assert.Zero(t, testutil.ToFloat64(callbackDeliveries.failure))
}
