package main

import "github.com/manifest-network/fred/internal/metrics"

// subSignerPool is the part of the signer pool that decides whether the
// sub-signer maintenance worker runs.
type subSignerPool interface {
	HasSubSigners() bool
}

// grantCheckSweeps counts the grant sweeps of the sub-signer maintenance
// worker. It exists only when that worker runs, so its series exist exactly
// when something sweeps: a provider without sub-signers, or one demoted to a
// single signer at startup, exports none.
type grantCheckSweeps struct {
	counters metrics.SignerGrantCheckCounters
}

// newGrantCheckSweeps binds the grant-check series for a pool that still has
// sub-signers after the startup check, and counts that check as the worker's
// first sweep. It reports false when no worker will run.
func newGrantCheckSweeps(pool subSignerPool, startup error) (grantCheckSweeps, bool) {
	if !pool.HasSubSigners() {
		return grantCheckSweeps{}, false
	}
	sweeps := grantCheckSweeps{counters: metrics.BindSignerGrantCheck()}
	sweeps.record(startup)
	return sweeps, true
}

// record counts one sweep by its result.
func (sweeps grantCheckSweeps) record(err error) {
	if err != nil {
		sweeps.counters.Error.Inc()
		return
	}
	sweeps.counters.Success.Inc()
}
