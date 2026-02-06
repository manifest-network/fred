package provisioner

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
