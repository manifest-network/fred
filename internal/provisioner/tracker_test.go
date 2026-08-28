package provisioner

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestTracker_TrackInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")

	assert.True(t, tracker.IsInFlight("lease-1"))
	assert.Equal(t, 1, tracker.InFlightCount())

	prov, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, "lease-1", prov.LeaseUUID)
	assert.Equal(t, "tenant-a", prov.Tenant)
	assert.Equal(t, "backend-1", prov.Backend)
	assert.Equal(t, items, prov.Items)
	assert.WithinDuration(t, time.Now(), prov.StartTime, 2*time.Second)
}

func TestTracker_OperationsExposesSoleRegistry(t *testing.T) {
	tracker := NewInFlightTracker()
	result := tracker.Operations().TryTrack(operation.TrackSpec{
		LeaseUUID: "lease-typed",
		Tenant:    "tenant-a",
		Backend:   "backend-a",
		Kind:      operation.KindProvision,
	})
	require.True(t, result.Started())
	t.Cleanup(func() { tracker.Operations().Abort(result.Token()) })

	legacy, exists := tracker.GetInFlight("lease-typed")
	require.True(t, exists)
	assert.Equal(t, result.Token().ID(), legacy.OperationID)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.InFlightProvisions))

	tracker.TrackInFlight("lease-legacy", "tenant-b", nil, "backend-b")
	legacyRecord, exists := tracker.Operations().Lookup("lease-legacy")
	require.True(t, exists)
	assert.Equal(t, "tenant-b", legacyRecord.Tenant)
	assert.Equal(t, 2, tracker.Operations().Count())
	tracker.UntrackInFlight("lease-legacy")
}

func TestTracker_TrackInFlight_Overwrites(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")
	tracker.TrackInFlight("lease-1", "tenant-b", items, "backend-2")

	prov, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, "tenant-b", prov.Tenant)
	assert.Equal(t, "backend-2", prov.Backend)
	assert.Equal(t, 1, tracker.InFlightCount())
}

func TestTracker_TryTrackInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	ok := tracker.TryTrackInFlight("lease-1", "tenant-a", items, "backend-1")
	assert.True(t, ok, "first TryTrackInFlight should succeed")
	assert.True(t, tracker.IsInFlight("lease-1"))

	ok = tracker.TryTrackInFlight("lease-1", "tenant-b", items, "backend-2")
	assert.False(t, ok, "second TryTrackInFlight should fail for same lease")

	// Original values should be preserved
	prov, _ := tracker.GetInFlight("lease-1")
	assert.Equal(t, "tenant-a", prov.Tenant)
	assert.Equal(t, "backend-1", prov.Backend)
}

func TestTracker_TryTrackInFlight_DefaultsToProvisionKind(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	require.True(t, tracker.TryTrackInFlight("lease-1", "tenant-a", items, "backend-1"))

	prov, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, KindProvision, prov.Kind, "a fresh provision must be tracked as KindProvision")
}

func TestTracker_TryTrackRestoreInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	ok := tracker.TryTrackRestoreInFlight("lease-1", "tenant-a", items, "backend-1")
	assert.True(t, ok, "first TryTrackRestoreInFlight should succeed")
	assert.True(t, tracker.IsInFlight("lease-1"))

	prov, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, KindRestore, prov.Kind, "a restore must be tracked as KindRestore so its callback metrics carry operation=restore")
	assert.Equal(t, "tenant-a", prov.Tenant)
	assert.Equal(t, "backend-1", prov.Backend)

	// Idempotent like TryTrackInFlight: a second attempt on the same lease fails
	// and leaves the original entry intact (so a duplicate restore POST is a 409,
	// not an overwrite).
	ok = tracker.TryTrackRestoreInFlight("lease-1", "tenant-b", items, "backend-2")
	assert.False(t, ok, "second TryTrackRestoreInFlight should fail for same lease")
	prov, _ = tracker.GetInFlight("lease-1")
	assert.Equal(t, "tenant-a", prov.Tenant)
}

func TestTracker_TryTrackInFlight_Concurrent(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}
	const goroutines = 100

	var successes atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			if tracker.TryTrackInFlight("lease-1", "tenant", items, "backend") {
				successes.Add(1)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), successes.Load(), "exactly one goroutine should succeed")
	assert.Equal(t, 1, tracker.InFlightCount())
}

func TestTracker_UntrackInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")
	assert.Equal(t, 1, tracker.InFlightCount())

	tracker.UntrackInFlight("lease-1")
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.Equal(t, 0, tracker.InFlightCount())

	// Untracking nonexistent should not panic or decrement below 0
	tracker.UntrackInFlight("nonexistent")
	assert.Equal(t, 0, tracker.InFlightCount())
}

func TestTracker_UntrackInFlightIfOperationID_PreservesReplacement(t *testing.T) {
	tracker := NewInFlightTracker()
	items := testItems("sku-1")

	first, ok := tracker.TryTrackInFlightWithOperationID("lease-1", "tenant-a", items, "backend-a")
	require.True(t, ok)
	require.True(t, tracker.UntrackInFlightIfOperationID("lease-1", first))

	second, ok := tracker.TryTrackInFlightWithOperationID("lease-1", "tenant-a", items, "backend-b")
	require.True(t, ok)
	require.NotEqual(t, first, second)

	assert.False(t, tracker.UntrackInFlightIfOperationID("lease-1", first),
		"late cleanup from the first operation must not remove its replacement")
	p, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, second, p.OperationID)
	assert.Equal(t, "backend-b", p.Backend)
}

func TestTracker_ClaimBlocksRemovalAndReplacementUntilRelease(t *testing.T) {
	tracker := NewInFlightTracker()
	items := testItems("sku-1")
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", items, "backend-a",
	)
	require.True(t, tracked)

	claimed, ok := tracker.TryClaimInFlight("lease-1", generation)
	require.True(t, ok)
	assert.Equal(t, generation, claimed.OperationID)
	assert.Equal(t, "backend-a", claimed.Backend)

	tracker.UntrackInFlight("lease-1")
	assert.True(t, tracker.IsInFlight("lease-1"), "legacy untrack must not bypass a claim")
	assert.False(t, tracker.UntrackInFlightIfOperationID("lease-1", generation),
		"generation-scoped untrack must not bypass a claim")
	_, popped := tracker.PopInFlight("lease-1")
	assert.False(t, popped, "pop must not bypass a claim")
	_, tracked = tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-b", items, "backend-b",
	)
	assert.False(t, tracked, "a replacement must not start while settlement is claimed")
	tracker.TrackInFlight("lease-1", "tenant-b", items, "backend-b")
	current, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, generation, current.OperationID, "legacy track must not overwrite a claim")
	assert.Equal(t, "backend-a", current.Backend)

	require.True(t, tracker.ReleaseInFlightClaim("lease-1", generation))
	require.True(t, tracker.UntrackInFlightIfOperationID("lease-1", generation),
		"release must restore ordinary generation-scoped cleanup")
	replacementGeneration, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-b", items, "backend-b",
	)
	require.True(t, tracked, "a replacement may start after release and cleanup")
	assert.NotEqual(t, generation, replacementGeneration)
}

func TestTracker_FinishClaimedInFlightRemovesGeneration(t *testing.T) {
	tracker := NewInFlightTracker()
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), "backend-a",
	)
	require.True(t, tracked)
	_, claimed := tracker.TryClaimInFlight("lease-1", generation)
	require.True(t, claimed)

	require.True(t, tracker.FinishClaimedInFlight("lease-1", generation))
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.False(t, tracker.ReleaseInFlightClaim("lease-1", generation),
		"a finished claim no longer exists to release")
	assert.False(t, tracker.FinishClaimedInFlight("lease-1", generation),
		"finishing the same generation twice is a no-op")

	replacementGeneration, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-b", testItems("sku-1"), "backend-b",
	)
	require.True(t, tracked)
	assert.NotEqual(t, generation, replacementGeneration)
}

func TestTracker_PopInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")

	prov, ok := tracker.PopInFlight("lease-1")
	assert.True(t, ok)
	assert.Equal(t, "lease-1", prov.LeaseUUID)
	assert.Equal(t, "tenant-a", prov.Tenant)

	// Should be removed after pop
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.Equal(t, 0, tracker.InFlightCount())

	// Second pop should return false
	_, ok = tracker.PopInFlight("lease-1")
	assert.False(t, ok)
}

func TestTracker_GetInFlight_DoesNotRemove(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")

	prov, ok := tracker.GetInFlight("lease-1")
	assert.True(t, ok)
	assert.Equal(t, "lease-1", prov.LeaseUUID)

	// Should still be tracked
	assert.True(t, tracker.IsInFlight("lease-1"))
	assert.Equal(t, 1, tracker.InFlightCount())
}

func TestTracker_GetInFlight_NotFound(t *testing.T) {
	tracker := NewInFlightTracker()

	_, ok := tracker.GetInFlight("nonexistent")
	assert.False(t, ok)
}

func TestTracker_IsInFlight(t *testing.T) {
	tracker := NewInFlightTracker()

	assert.False(t, tracker.IsInFlight("lease-1"))

	tracker.TrackInFlight("lease-1", "tenant-a", nil, "")
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestTracker_InFlightCount(t *testing.T) {
	tracker := NewInFlightTracker()
	assert.Equal(t, 0, tracker.InFlightCount())

	tracker.TrackInFlight("lease-1", "t", nil, "")
	assert.Equal(t, 1, tracker.InFlightCount())

	tracker.TrackInFlight("lease-2", "t", nil, "")
	assert.Equal(t, 2, tracker.InFlightCount())

	tracker.UntrackInFlight("lease-1")
	assert.Equal(t, 1, tracker.InFlightCount())
}

func TestTracker_GetInFlightLeases(t *testing.T) {
	tracker := NewInFlightTracker()

	leases := tracker.GetInFlightLeases()
	assert.Empty(t, leases)

	tracker.TrackInFlight("lease-1", "t", nil, "")
	tracker.TrackInFlight("lease-2", "t", nil, "")
	tracker.TrackInFlight("lease-3", "t", nil, "")

	leases = tracker.GetInFlightLeases()
	sort.Strings(leases)
	assert.Equal(t, []string{"lease-1", "lease-2", "lease-3"}, leases)
}

func TestTracker_WaitForDrain_AlreadyEmpty(t *testing.T) {
	tracker := NewInFlightTracker()

	remaining := tracker.WaitForDrain(context.Background(), 5*time.Second)
	assert.Equal(t, 0, remaining)
}

func TestTracker_WaitForDrain_DrainsSuccessfully(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", nil, "")

	go func() {
		time.Sleep(200 * time.Millisecond)
		tracker.UntrackInFlight("lease-1")
	}()

	remaining := tracker.WaitForDrain(context.Background(), 5*time.Second)
	assert.Equal(t, 0, remaining)
}

func TestTracker_WaitForDrain_Timeout(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", nil, "")

	// Use a timeout well above the internal 500ms poll interval to avoid
	// coupling this test to the implementation's tick frequency.
	remaining := tracker.WaitForDrain(context.Background(), 2*time.Second)
	assert.Equal(t, 1, remaining)
}

func TestTracker_WaitForDrain_ContextCanceled(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	remaining := tracker.WaitForDrain(ctx, 30*time.Second)
	assert.Equal(t, 1, remaining)
}

func TestTracker_GetTimedOutProvisions(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	// Track with a start time in the past
	tracker.TrackInFlightWithStartTime("lease-old", "t", items, "b", time.Now().Add(-10*time.Minute))
	tracker.TrackInFlight("lease-new", "t", items, "b")

	timedOut := tracker.GetTimedOutProvisions(5 * time.Minute)
	require.Len(t, timedOut, 1)
	assert.Equal(t, "lease-old", timedOut[0].LeaseUUID)

	// Both should still be tracked (read-only operation)
	assert.Equal(t, 2, tracker.InFlightCount())
}

func TestTracker_GetTimedOutProvisions_None(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", nil, "")

	timedOut := tracker.GetTimedOutProvisions(1 * time.Hour)
	assert.Empty(t, timedOut)
}

func TestTracker_InFlightGauge_MatchesMapSize(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	gaugeValue := func() float64 {
		return promtestutil.ToFloat64(metrics.InFlightProvisions)
	}

	// Track 3 leases — gauge should equal map size
	tracker.TrackInFlight("lease-1", "t", items, "b")
	tracker.TrackInFlight("lease-2", "t", items, "b")
	tracker.TrackInFlight("lease-3", "t", items, "b")
	assert.Equal(t, 3.0, gaugeValue())

	// Untrack 1
	tracker.UntrackInFlight("lease-2")
	assert.Equal(t, 2.0, gaugeValue())

	// Pop 1
	tracker.PopInFlight("lease-1")
	assert.Equal(t, 1.0, gaugeValue())

	// Untrack remaining
	tracker.UntrackInFlight("lease-3")
	assert.Equal(t, 0.0, gaugeValue())
}

func TestTracker_InFlightGauge_OverwriteDoesNotDrift(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}

	// Track the same lease twice (overwrite). With Inc/Dec this would
	// double-increment the gauge; with Set(len) it stays correct.
	tracker.TrackInFlight("lease-1", "tenant-a", items, "backend-1")
	tracker.TrackInFlight("lease-1", "tenant-b", items, "backend-2")
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.InFlightProvisions),
		"gauge must reflect map size (1), not number of TrackInFlight calls (2)")

	// Single untrack should bring gauge to 0
	tracker.UntrackInFlight("lease-1")
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.InFlightProvisions))
}

func TestTracker_InFlightGauge_DoubleUntrackDoesNotGoNegative(t *testing.T) {
	tracker := NewInFlightTracker()

	tracker.TrackInFlight("lease-1", "t", nil, "b")
	tracker.UntrackInFlight("lease-1")
	tracker.UntrackInFlight("lease-1") // no-op

	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.InFlightProvisions))
}

func TestTracker_InFlightGauge_PopNonexistent(t *testing.T) {
	tracker := NewInFlightTracker()
	t.Cleanup(func() { tracker.UntrackInFlight("lease-1") })

	tracker.TrackInFlight("lease-1", "t", nil, "b")
	tracker.PopInFlight("nonexistent")

	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.InFlightProvisions))
}

func TestTracker_InFlightGauge_TryTrack(t *testing.T) {
	tracker := NewInFlightTracker()
	items := []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}
	t.Cleanup(func() { tracker.UntrackInFlight("lease-1") })

	ok := tracker.TryTrackInFlight("lease-1", "t", items, "b")
	assert.True(t, ok)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.InFlightProvisions))

	// Duplicate — should not change gauge
	ok = tracker.TryTrackInFlight("lease-1", "t", items, "b")
	assert.False(t, ok)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.InFlightProvisions))
}

func TestTracker_RoutingSKU(t *testing.T) {
	t.Run("with items", func(t *testing.T) {
		p := InFlightProvision{
			Items: []backend.LeaseItem{
				{SKU: "sku-first", Quantity: 1},
				{SKU: "sku-second", Quantity: 2},
			},
		}
		assert.Equal(t, "sku-first", p.RoutingSKU())
	})

	t.Run("empty items", func(t *testing.T) {
		p := InFlightProvision{}
		assert.Equal(t, "", p.RoutingSKU())
	})
}

func TestDefaultInFlightTracker_InFlightCountsByBackend(t *testing.T) {
	tr := NewInFlightTracker()
	assert.Empty(t, tr.InFlightCountsByBackend())

	tr.TrackInFlight("l1", "ten", nil, "docker-1")
	tr.TrackInFlight("l2", "ten", nil, "docker-1")
	tr.TrackInFlight("l3", "ten", nil, "docker-2")

	counts := tr.InFlightCountsByBackend()
	assert.Equal(t, 2, counts["docker-1"])
	assert.Equal(t, 1, counts["docker-2"])

	tr.UntrackInFlight("l1")
	counts = tr.InFlightCountsByBackend()
	assert.Equal(t, 1, counts["docker-1"])
	assert.Equal(t, 1, counts["docker-2"])
}

func TestDefaultInFlightTracker_InFlightCountsByBackend_Concurrent(t *testing.T) {
	tr := NewInFlightTracker()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr.TrackInFlight(fmt.Sprintf("l%d", i), "ten", nil, "docker-1")
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tr.InFlightCountsByBackend()
		}()
	}
	wg.Wait()
	assert.Equal(t, 100, tr.InFlightCountsByBackend()["docker-1"])
}

func TestTracker_LeaseActionMutationFence(t *testing.T) {
	t.Run("completed action after boundary is stale", func(t *testing.T) {
		tracker := NewInFlightTracker()
		cutoff := tracker.SnapshotMutationRevision()
		require.True(t, tracker.TryClaimLeaseAction("lease-1"))
		require.True(t, tracker.ReleaseLeaseAction("lease-1"))

		claimed, stale := tracker.TryClaimLeaseActionIfNotNewer("lease-1", cutoff)
		assert.False(t, claimed)
		assert.True(t, stale)
	})

	t.Run("action straddling boundary is stale after release", func(t *testing.T) {
		tracker := NewInFlightTracker()
		require.True(t, tracker.TryClaimLeaseAction("lease-1"))
		cutoff := tracker.SnapshotMutationRevision()
		require.True(t, tracker.ReleaseLeaseAction("lease-1"))

		claimed, stale := tracker.TryClaimLeaseActionIfNotNewer("lease-1", cutoff)
		assert.False(t, claimed)
		assert.True(t, stale)
	})

	t.Run("reconciler claim blocks events through in-flight cleanup", func(t *testing.T) {
		tracker := NewInFlightTracker()
		cutoff := tracker.SnapshotMutationRevision()
		claimed, stale := tracker.TryClaimLeaseActionIfNotNewer("lease-1", cutoff)
		require.True(t, claimed)
		require.False(t, stale)

		_, eventTracked := tracker.TryTrackInFlightWithOperationID(
			"lease-1", "tenant-a", nil, "backend-a",
		)
		assert.False(t, eventTracked)
		generation, reconcileTracked, stale := tracker.TryTrackInFlightWithOperationIDIfNotNewer(
			"lease-1", "tenant-a", nil, "backend-a", cutoff,
		)
		assert.True(t, reconcileTracked)
		assert.False(t, stale)

		require.True(t, tracker.UntrackInFlightIfOperationID("lease-1", generation))
		_, eventTracked = tracker.TryTrackInFlightWithOperationID(
			"lease-1", "tenant-a", nil, "backend-a",
		)
		assert.False(t, eventTracked, "preflight cleanup must not expose the lease before the worker finishes")
		require.True(t, tracker.ReleaseLeaseAction("lease-1"))
		_, eventTracked = tracker.TryTrackInFlightWithOperationID(
			"lease-1", "tenant-a", nil, "backend-a",
		)
		assert.True(t, eventTracked)
	})
}

func TestTracker_StaleGenerationCannotSettleReplacementConcurrently(t *testing.T) {
	tracker := NewInFlightTracker()
	first, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), "backend-a",
	)
	require.True(t, tracked)
	_, claimed := tracker.TryClaimInFlight("lease-1", first)
	require.True(t, claimed)
	require.True(t, tracker.FinishClaimedInFlight("lease-1", first))

	second, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-b", testItems("sku-2"), "backend-b",
	)
	require.True(t, tracked)
	require.NotEqual(t, first, second)
	t.Cleanup(func() { tracker.UntrackInFlightIfOperationID("lease-1", second) })

	const workers = 64
	var unexpected atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, ok := tracker.TryClaimInFlight("lease-1", first); ok {
				unexpected.Add(1)
			}
			if _, ok := tracker.TryClaimInFlightForDeprovision("lease-1", first); ok {
				unexpected.Add(1)
			}
			if tracker.ReleaseInFlightClaim("lease-1", first) {
				unexpected.Add(1)
			}
			if tracker.FinishClaimedInFlight("lease-1", first) {
				unexpected.Add(1)
			}
			if tracker.UntrackInFlightIfOperationID("lease-1", first) {
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Zero(t, unexpected.Load())
	current, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, second, current.OperationID)
	assert.Equal(t, "backend-b", current.Backend)
}

func TestTracker_StaleSnapshotHandleCannotClaimAfterInterveningOperation(t *testing.T) {
	tracker := NewInFlightTracker()
	snapshotHandle := tracker.SnapshotMutationRevision()
	require.NotZero(t, snapshotHandle)

	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), "backend-a",
	)
	require.True(t, tracked)
	require.True(t, tracker.UntrackInFlightIfOperationID("lease-1", generation))

	claimed, stale := tracker.TryClaimLeaseActionIfNotNewer("lease-1", snapshotHandle)
	assert.False(t, claimed)
	assert.True(t, stale)
	assert.False(t, tracker.ReleaseLeaseAction("lease-1"))
}

func TestTracker_StaleRawReleaseCannotReleaseTypedReplacementClaims(t *testing.T) {
	t.Run("lease claim", func(t *testing.T) {
		tracker := NewInFlightTracker()
		require.True(t, tracker.TryClaimLeaseAction("lease-1"))
		require.True(t, tracker.ReleaseLeaseAction("lease-1"))

		replacement := tracker.Operations().TryClaimLeaseNow("lease-1")
		require.True(t, replacement.Acquired())
		assert.False(t, tracker.ReleaseLeaseAction("lease-1"),
			"the stale raw release has no opaque capability for the replacement")
		assert.True(t, tracker.Operations().ReleaseLease(replacement.Claim()))
	})

	t.Run("settlement claim", func(t *testing.T) {
		tracker := NewInFlightTracker()
		generation, tracked := tracker.TryTrackInFlightWithOperationID(
			"lease-1", "tenant-a", testItems("sku-1"), "backend-a",
		)
		require.True(t, tracked)
		_, claimed := tracker.TryClaimInFlight("lease-1", generation)
		require.True(t, claimed)
		require.True(t, tracker.ReleaseInFlightClaim("lease-1", generation))

		record, exists := tracker.Operations().Lookup("lease-1")
		require.True(t, exists)
		replacement := tracker.Operations().TryClaimDeprovision("lease-1", record.ID)
		require.True(t, replacement.Claimed())
		assert.False(t, tracker.ReleaseInFlightClaim("lease-1", generation),
			"the stale raw release has no opaque capability for the replacement")
		assert.False(t, tracker.FinishClaimedInFlight("lease-1", generation))
		assert.True(t, tracker.Operations().FinishSettlement(replacement.Claim()))
	})
}
