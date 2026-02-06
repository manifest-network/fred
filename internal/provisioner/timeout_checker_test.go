package provisioner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
		Tracker:       tracker,
		Rejecter:      rejecter,
		Timeout:       timeout,
		CheckInterval: time.Hour, // irrelevant; we call CheckOnce directly
	})
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

func TestCheckOnce_RejectFailure_KeepsInFlight(t *testing.T) {
	tracker := NewInFlightTracker()
	tracker.TrackInFlightWithStartTime("lease-stuck", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend",
		time.Now().Add(-20*time.Minute))

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			return 0, nil, fmt.Errorf("chain unavailable")
		},
	}

	checker := newTimeoutCheckerForTest(tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-stuck"),
		"lease should remain in-flight when rejection fails")
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
