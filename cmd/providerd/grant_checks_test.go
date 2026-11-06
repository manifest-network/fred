package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

type fakeSubSignerPool bool

func (pool fakeSubSignerPool) HasSubSigners() bool { return bool(pool) }

// TestGrantCheckSeriesExistOnlyWithTheirWorker pins the heartbeat contract:
// no series while no maintenance worker runs (no sub-signers, or a pool the
// startup check demoted), and both series once one does, with the startup
// check counted as its first sweep. The cases run in order because the series
// are process-global and binding cannot be undone.
func TestGrantCheckSeriesExistOnlyWithTheirWorker(t *testing.T) {
	grantChecks := func(outcome string) (float64, bool) {
		return testutil.CounterValue(t, "fred_signer_grant_check_total", map[string]string{"outcome": outcome})
	}

	_, ok := newGrantCheckSweeps(fakeSubSignerPool(false), errors.New("grants missing; demoted"))
	assert.False(t, ok, "a demoted pool runs no maintenance worker")
	for _, outcome := range []string{"success", "error"} {
		_, exists := grantChecks(outcome)
		require.False(t, exists, "no worker, no %s series", outcome)
	}

	sweeps, ok := newGrantCheckSweeps(fakeSubSignerPool(true), errors.New("transient query failure"))
	require.True(t, ok)
	failures, exists := grantChecks("error")
	require.True(t, exists)
	assert.Equal(t, 1.0, failures, "the startup check is the worker's first sweep")
	successes, exists := grantChecks("success")
	require.True(t, exists, "both outcomes exist before their first event")
	assert.Equal(t, 0.0, successes)

	sweeps.record(nil)
	successes, _ = grantChecks("success")
	assert.Equal(t, 1.0, successes)
}
