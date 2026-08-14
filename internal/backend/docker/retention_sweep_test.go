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

// The ENG-680 pins: a degraded retention store must reach every stage of the sweep, and
// must be visible in a metric rather than only in one log line per tick.

// TestRunRetentionSweep_StoreError_ReachesEveryStage is the headline.
//
// runRetentionSweep used to bare-return on the first store error. List, ListExpired,
// ListReaping and ListRestoring are all one filter() over one bucket, so they fail on
// exactly the same inputs — which meant the sweep always died at the FIRST of them and
// reconcileOrphanedRetentions was never reached. Its store_error arm was therefore dead
// code under precisely the condition it was written to report, while
// retention_reap_skips_total's own doc pointed at it as "the ticketing signal" for the
// orphan pruner.
//
// The assertion that matters is that orphan_skips{store_error} MOVES. The rest pins that
// the sweep reports the failure once, as a whole.
func TestRunRetentionSweep_StoreError_ReachesEveryStage(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	b.cfg.RetentionMaxAge = time.Hour
	b.cfg.RetainOnClose = true
	b.cfg.VolumeDataPath = t.TempDir() // a configured, readable root: the orphan pass gets past G2
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return nil, nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("a degraded store must destroy nothing; got %q", id)
			return nil
		},
	}
	require.NoError(t, rs.Close()) // every enumeration now fails

	orphanBefore := testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipStoreError))
	errBefore := testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeError))
	acctBefore := testutil.ToFloat64(retentionAccountingRefreshFailedTotal)

	err := b.runRetentionSweep(context.Background())

	require.Error(t, err)
	assert.Equal(t, orphanBefore+1,
		testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipStoreError)),
		"ENG-680: the orphan pass must be REACHED under a broken store — this counter was "+
			"unreachable under exactly the condition it exists to report")
	assert.Equal(t, errBefore+1, testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeError)),
		"one increment per pass, whatever the outcome")
	assert.Equal(t, acctBefore+1, testutil.ToFloat64(retentionAccountingRefreshFailedTotal),
		"the accounting refresh still ran and still failed; a frozen projection is its own signal")

	// Every stage is named, so the single log line StartCleanupLoop emits says how much of
	// the sweep was lost rather than just reporting the first casualty.
	for _, stage := range []string{"reap expired", "retry reaping", "list restoring", "reconcile orphans"} {
		assert.ErrorContains(t, err, stage)
	}
}

// TestRunRetentionSweep_HealthyPass_CountsSuccess is the other half of the heartbeat
// property the liveness query depends on: the sum across outcomes must advance on EVERY
// pass, so that `sum without (outcome) (increase(...)) == 0` means "the sweep stopped
// running" and nothing else.
func TestRunRetentionSweep_HealthyPass_CountsSuccess(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	b.cfg.RetentionMaxAge = time.Hour
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return nil, nil }}

	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "u1", Tenant: "t1",
		Status: shared.RetentionStatusActive, CreatedAt: time.Now(),
	}))

	okBefore := testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeSuccess))
	errBefore := testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeError))

	require.NoError(t, b.runRetentionSweep(context.Background()))

	assert.Equal(t, okBefore+1, testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeSuccess)))
	assert.Equal(t, errBefore, testutil.ToFloat64(retentionSweepTotal.WithLabelValues(sweepOutcomeError)),
		"a clean pass must not also count an error")
}

// TestRunRetentionSweep_NilStore_SucceedsWithoutPanic pins the one guard the restructure
// could plausibly drop. reapExpiredRetentions, retryReapingRecords,
// reconcileOrphanedRetentions and the accounting refresh all nil-check themselves;
// ListRestoring does NOT — it takes the store's mutex on the receiver, so a nil store is a
// panic there, not an error. The early `return nil` that used to cover it is gone, because
// the accounting refresh and the outcome record are now unconditional.
func TestRunRetentionSweep_NilStore_SucceedsWithoutPanic(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.retentionStore = nil

	assert.NotPanics(t, func() {
		require.NoError(t, b.runRetentionSweep(context.Background()))
	})
}

// TestRefreshRetentionAccounting_StoreError_KeepsLastValueAndCounts pins ENG-680's
// consequence #3. Keeping the last value on a store read error is correct — a zeroed
// projection would over-admit immediately — but on its own it is silent: all five gauges
// and the pool's retained input hold plausible numbers for as long as the store is broken,
// which looks exactly like a healthy provider.
func TestRefreshRetentionAccounting_StoreError_KeepsLastValueAndCounts(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "u1", Tenant: "t1",
		Items:  []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		Status: shared.RetentionStatusActive, CreatedAt: time.Now(),
	}))
	b.refreshRetentionAccounting()
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB, "precondition: a real value is published")

	before := testutil.ToFloat64(retentionAccountingRefreshFailedTotal)
	require.NoError(t, rs.Close())

	b.refreshRetentionAccounting()

	assert.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB,
		"keep-last-value: a zeroed projection would over-admit against real bytes")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionAccountingRefreshFailedTotal),
		"...but it must not be silent — this is the only signal that the gauges are stale")
}

// TestRetentionSweepMetrics_PreInitialized guards the alert-facing half of the contract.
// The liveness query reads the SUM across outcomes, and increase() over a series that has
// never been exported is no-data, not zero — so a provider between process start and its
// first sweep tick (an hour at the default cadence) would otherwise be indistinguishable
// from one whose sweep has stalled. That ambiguity is ENG-680 in miniature.
func TestRetentionSweepMetrics_PreInitialized(t *testing.T) {
	assert.Equal(t, len(sweepOutcomes), testutil.CollectAndCount(retentionSweepTotal),
		"every sweep outcome must export a series before the first sweep runs")
	assert.Equal(t, 1, testutil.CollectAndCount(retentionAccountingRefreshFailedTotal))
}
