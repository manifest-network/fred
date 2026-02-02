package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
)

// TimeoutChecker monitors in-flight provisions and rejects timed-out ones.
// It runs as a background goroutine and periodically checks for provisions
// that have exceeded the callback timeout.
type TimeoutChecker struct {
	tracker       InFlightTracker
	rejecter      LeaseRejecter
	timeout       time.Duration
	checkInterval time.Duration
}

// TimeoutCheckerConfig configures the timeout checker.
type TimeoutCheckerConfig struct {
	Tracker       InFlightTracker
	Rejecter      LeaseRejecter
	Timeout       time.Duration // Callback timeout (how long to wait before considering a provision timed out)
	CheckInterval time.Duration // How often to check for timeouts
}

// NewTimeoutChecker creates a new TimeoutChecker.
func NewTimeoutChecker(cfg TimeoutCheckerConfig) *TimeoutChecker {
	return &TimeoutChecker{
		tracker:       cfg.Tracker,
		rejecter:      cfg.Rejecter,
		timeout:       cfg.Timeout,
		checkInterval: cfg.CheckInterval,
	}
}

// Start begins the timeout checker loop. It runs until the context is canceled.
func (c *TimeoutChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.CheckOnce(ctx)
		}
	}
}

// CheckOnce performs a single timeout check. This is exposed for testing.
func (c *TimeoutChecker) CheckOnce(ctx context.Context) {
	timedOut := c.tracker.GetTimedOutProvisions(c.timeout)

	if len(timedOut) == 0 {
		return
	}

	slog.Warn("found timed-out provisions",
		"count", len(timedOut),
		"timeout", c.timeout,
	)

	now := time.Now()

	// Process each timed-out provision
	for _, p := range timedOut {
		// Check context before each operation
		if ctx.Err() != nil {
			return
		}

		// Reject the lease on chain FIRST, before untracking.
		// This prevents a race where the reconciler sees a PENDING lease that's
		// not in-flight and tries to provision it again.
		rejected, txHashes, err := c.rejecter.RejectLeases(ctx, []string{p.LeaseUUID}, "callback timeout")
		if err != nil {
			slog.Error("failed to reject timed-out lease, keeping in-flight to prevent re-provision",
				"lease_uuid", p.LeaseUUID,
				"error", err,
			)
			// Keep in-flight so reconciler doesn't try to re-provision.
			// Next timeout check will retry the rejection.
			continue
		}

		// Only untrack AFTER successful rejection
		c.tracker.UntrackInFlight(p.LeaseUUID)
		metrics.CallbackTimeoutsTotal.Inc()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, p.Backend).Inc()

		// Record duration (from start until timeout)
		duration := now.Sub(p.StartTime).Seconds()
		metrics.ProvisioningDuration.WithLabelValues(p.Backend).Observe(duration)

		slog.Warn("rejected timed-out provision",
			"lease_uuid", p.LeaseUUID,
			"tenant", p.Tenant,
			"backend", p.Backend,
			"age", now.Sub(p.StartTime),
			"rejected", rejected,
			"tx_hashes", txHashes,
		)
	}
}
