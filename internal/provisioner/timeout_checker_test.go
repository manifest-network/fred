package provisioner

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// mockRejecter implements LeaseRejecter for testing.
type mockRejecter struct {
	rejectFn func(ctx context.Context, uuids []string, reason string) (uint64, []string, error)
}

func (m *mockRejecter) RejectLeases(ctx context.Context, uuids []string, reason string) (uint64, []string, error) {
	return m.rejectFn(ctx, uuids, reason)
}

func newTimeoutCheckerForTest(tracker *DefaultInFlightTracker, rejecter LeaseRejecter, timeout time.Duration) *TimeoutChecker {
	return NewTimeoutChecker(TimeoutCheckerConfig{
		Operations:    tracker.Operations(),
		Rejecter:      rejecter,
		Timeout:       timeout,
		CheckInterval: time.Hour, // irrelevant; we call CheckOnce directly
	})
}

func TestNewTimeoutChecker_RetainsOnlyTypedCapabilities(t *testing.T) {
	registry := operation.NewRegistry()
	checker := NewTimeoutChecker(TimeoutCheckerConfig{
		Operations: registry,
		Rejecter: &mockRejecter{rejectFn: func(
			context.Context, []string, string,
		) (uint64, []string, error) {
			return 0, nil, nil
		}},
	})
	assert.Same(t, registry, checker.operations)

	var typedNilOperations *operation.Registry
	checker = NewTimeoutChecker(TimeoutCheckerConfig{
		Operations: typedNilOperations,
	})
	assert.Nil(t, checker.operations, "a typed-nil operations port must fail closed")
	assert.NotPanics(t, func() { checker.CheckOnce(context.Background()) })

	var typedNilRejecter *mockRejecter
	checker = NewTimeoutChecker(TimeoutCheckerConfig{
		Operations: registry,
		Rejecter:   typedNilRejecter,
	})
	assert.Nil(t, checker.rejecter)
	assert.NotPanics(t, func() { checker.CheckOnce(context.Background()) })
}

func TestTimeoutChecker_StartStopsWithContext(t *testing.T) {
	checker := NewTimeoutChecker(TimeoutCheckerConfig{
		Operations:    operation.NewRegistry(),
		Rejecter:      &mockRejecter{},
		CheckInterval: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		checker.Start(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout checker did not stop after context cancellation")
	}
}

func TestTimeoutOperationLabel(t *testing.T) {
	assert.Equal(t, metrics.OperationProvision, timeoutOperationLabel(operation.KindProvision))
	assert.Equal(t, metrics.OperationRestore, timeoutOperationLabel(operation.KindRestore))
	assert.Equal(t, metrics.OperationProvision, timeoutOperationLabel(operation.KindInvalid),
		"invalid records cannot come from Registry; the fallback remains bounded")
}

func TestCheckOnce_NoTimeouts(t *testing.T) {
	tracker := NewInFlightTracker()
	// Track a recent provision (not timed out).
	tracker.TrackInFlight("lease-1", "tenant-1", []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			t.Fatal("RejectLeases should not be called when nothing is timed out")
			return 0, nil, nil
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-1"), "lease should still be in-flight")
}

func TestCheckOnce_SingleTimeout_RejectsAndUntracks(t *testing.T) {
	tracker := NewInFlightTracker()
	// Simulate a provision that started 20 minutes ago.
	tracker.TrackInFlightWithStartTime("lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	var rejectedUUIDs []string
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, reason string) (uint64, []string, error) {
			rejectedUUIDs = uuids
			assert.Equal(t, "callback timeout", reason)
			return uint64(len(uuids)), []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	require.Len(t, rejectedUUIDs, 1)
	assert.Equal(t, "lease-old", rejectedUUIDs[0])
	assert.False(t, tracker.IsInFlight("lease-old"), "lease should be untracked after rejection")
}

func TestCheckOnce_ClaimPreventsReplacementDuringReject(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a",
		time.Now().Add(-20*time.Minute))
	old, exists := tracker.GetInFlight("lease-old")
	require.True(t, exists)

	rejecter := &mockRejecter{rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
		assert.False(t, tracker.UntrackInFlightIfOperationID("lease-old", old.OperationID),
			"callback cleanup must not remove a generation while timeout settlement owns it")
		_, popped := tracker.PopInFlight("lease-old")
		assert.False(t, popped, "legacy pop must not bypass a settlement claim")
		_, claimed := tracker.TryClaimInFlight("lease-old", old.OperationID)
		assert.False(t, claimed, "a second settlement actor must not claim the same generation")
		_, tracked := tracker.TryTrackInFlightWithOperationID(
			"lease-old", "tenant-1", []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-b",
		)
		assert.False(t, tracked, "a replacement must not start while the old generation is claimed")
		return 1, []string{"tx"}, nil
	}}

	newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute).CheckOnce(context.Background())
	assert.False(t, tracker.IsInFlight("lease-old"), "successful settlement must finish the claimed generation")

	replacementGeneration, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-old", "tenant-1", []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-b",
	)
	require.True(t, tracked)
	current, exists := tracker.GetInFlight("lease-old")
	require.True(t, exists)
	assert.Equal(t, replacementGeneration, current.OperationID)
	assert.Equal(t, "backend-b", current.Backend)
}

func TestCheckOnce_AlreadyClaimedGenerationIsSkipped(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a",
		time.Now().Add(-20*time.Minute))
	p, exists := tracker.GetInFlight("lease-old")
	require.True(t, exists)
	_, claimed := tracker.TryClaimInFlight(p.LeaseUUID, p.OperationID)
	require.True(t, claimed)

	rejectCalls := 0
	rejecter := &mockRejecter{rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
		rejectCalls++
		return 1, []string{"tx"}, nil
	}}
	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)

	checker.CheckOnce(context.Background())
	assert.Equal(t, 0, rejectCalls, "the actor holding the claim owns settlement")
	assert.True(t, tracker.IsInFlight("lease-old"))

	require.True(t, tracker.ReleaseInFlightClaim(p.LeaseUUID, p.OperationID))
	checker.CheckOnce(context.Background())
	assert.Equal(t, 1, rejectCalls)
	assert.False(t, tracker.IsInFlight("lease-old"))
}

func TestCheckOnce_ConcurrentSweepsRejectGenerationOnce(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a",
		time.Now().Add(-20*time.Minute))

	var rejectCalls atomic.Int32
	rejectStarted := make(chan struct{})
	allowReject := make(chan struct{})
	rejecter := &mockRejecter{rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
		if rejectCalls.Add(1) == 1 {
			close(rejectStarted)
		}
		<-allowReject
		return 1, []string{"tx"}, nil
	}}
	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)

	firstDone := make(chan struct{})
	go func() {
		checker.CheckOnce(context.Background())
		close(firstDone)
	}()
	<-rejectStarted

	secondDone := make(chan struct{})
	go func() {
		checker.CheckOnce(context.Background())
		close(secondDone)
	}()
	select {
	case <-secondDone:
		// The second sweep saw the claim and skipped this generation.
	case <-time.After(time.Second):
		close(allowReject)
		<-firstDone
		<-secondDone
		t.Fatal("second timeout sweep did not skip the claimed generation")
	}

	close(allowReject)
	<-firstDone
	assert.Equal(t, int32(1), rejectCalls.Load())
	assert.False(t, tracker.IsInFlight("lease-old"))
}

func TestCheckOnce_RejectFailure_KeepsInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-stuck", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			rejectCalls++
			if rejectCalls == 1 {
				return 0, nil, fmt.Errorf("chain unavailable")
			}
			return 1, []string{"tx"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-stuck"),
		"lease should remain in-flight when rejection fails")

	checker.CheckOnce(context.Background())
	assert.Equal(t, 2, rejectCalls, "a retryable failure must release the claim for the next sweep")
	assert.False(t, tracker.IsInFlight("lease-stuck"))
}

// TestCheckOnce_ActiveReprovisionNotPending_UntracksAndHandsBack covers ENG-337.
// The reconciler registers ACTIVE-lease re-provisions in the SAME shared in-flight
// tracker the checker scans. When such a re-provision's callback is lost, the
// timed-out lease is no longer PENDING, so the chain rejects RejectLeases with
// ErrLeaseNotPending. The checker must NOT keep retrying reject forever (which
// wedges the lease in-flight permanently and inflates InFlightProvisions); it must
// untrack the lease and hand it back to the reconciler, which owns the ACTIVE-lease
// re-provision / FailCount / close path.
func TestCheckOnce_ActiveReprovisionNotPending_UntracksAndHandsBack(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-active", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			rejectCalls++
			return 0, nil, billingtypes.ErrLeaseNotPending
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.Equal(t, 1, rejectCalls, "should attempt reject once, not retry a non-pending lease")
	assert.False(t, tracker.IsInFlight("lease-active"),
		"non-pending lease must be untracked and handed back to the reconciler, not kept in-flight")
}

// TestCheckOnce_LeaseNotFound_Untracks ensures a timed-out provision for a lease
// that no longer exists on chain is untracked rather than retried forever. Like
// ErrLeaseNotPending, ErrLeaseNotFound is terminal for RejectLeases.
func TestCheckOnce_LeaseNotFound_Untracks(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-gone", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			return 0, nil, billingtypes.ErrLeaseNotFound
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.False(t, tracker.IsInFlight("lease-gone"),
		"lease that no longer exists must be untracked, not retried forever")
}

func TestCheckOnce_ContextCanceled_StopsEarly(t *testing.T) {
	tracker := NewInFlightTracker()
	// Add two timed-out provisions.
	tracker.TrackInFlightWithStartTime("lease-a", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))
	tracker.TrackInFlightWithStartTime("lease-b", "tenant-2",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	ctx, cancel := context.WithCancel(context.Background())

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			rejectCalls++
			// Cancel context after first rejection to simulate shutdown.
			cancel()
			return uint64(len(uuids)), []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(ctx)

	// At most one rejection should have been processed before ctx was canceled.
	assert.Equal(t, 1, rejectCalls, "should stop processing after context cancellation")
}

func TestCheckOnce_MultipleTimeouts_PartialFailure(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-ok", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))
	tracker.TrackInFlightWithStartTime("lease-fail", "tenant-2",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			if uuids[0] == "lease-fail" {
				return 0, nil, fmt.Errorf("chain error")
			}
			return 1, []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	// The successfully rejected lease should be untracked.
	// The failed one should remain.
	assert.False(t, tracker.IsInFlight("lease-ok"), "successfully rejected lease should be untracked")
	assert.True(t, tracker.IsInFlight("lease-fail"), "failed rejection should keep lease in-flight")
}
