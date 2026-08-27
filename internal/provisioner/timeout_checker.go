package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// TimeoutOperations is the lifecycle authority needed by TimeoutChecker. The
// checker deliberately depends on this narrow capability surface instead of
// the legacy tracker API, so a timeout can only release or finish the exact
// operation claim it acquired.
type TimeoutOperations interface {
	TimedOut(time.Duration) []operation.Record
	TryClaimTimeout(string, operation.OperationID) operation.SettlementResult
	ReleaseSettlement(operation.SettlementClaim) bool
	FinishSettlement(operation.SettlementClaim) bool
}

// TimeoutChecker monitors in-flight provisions and rejects timed-out ones.
// It runs as a background goroutine and periodically checks for provisions
// that have exceeded the callback timeout.
type TimeoutChecker struct {
	operations    TimeoutOperations
	rejecter      LeaseRejecter
	timeout       time.Duration
	checkInterval time.Duration
}

// TimeoutCheckerConfig configures the timeout checker.
type TimeoutCheckerConfig struct {
	Operations    TimeoutOperations
	Rejecter      LeaseRejecter
	Timeout       time.Duration // Callback timeout (how long to wait before considering a provision timed out)
	CheckInterval time.Duration // How often to check for timeouts
}

// NewTimeoutChecker creates a new TimeoutChecker.
func NewTimeoutChecker(cfg TimeoutCheckerConfig) *TimeoutChecker {
	operations := cfg.Operations
	if isNilCapability(operations) {
		operations = nil
	}
	rejecter := cfg.Rejecter
	if isNilCapability(rejecter) {
		rejecter = nil
	}
	return &TimeoutChecker{
		operations:    operations,
		rejecter:      rejecter,
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

// CheckOnce performs one sweep of the in-flight tracker: every provision
// older than the callback timeout is rejected on-chain, and untracked
// only once that rejection has either succeeded or become impossible.
// This is the body of Start's ticker loop, run on every tick.
func (c *TimeoutChecker) CheckOnce(ctx context.Context) {
	if c.operations == nil || c.rejecter == nil {
		slog.Error("timeout checker is missing lifecycle authority",
			"operations_configured", c.operations != nil,
			"rejecter_configured", c.rejecter != nil,
		)
		return
	}
	timedOut := c.operations.TimedOut(c.timeout)

	if len(timedOut) == 0 {
		return
	}

	slog.Warn("found timed-out provisions",
		"count", len(timedOut),
		"timeout", c.timeout,
	)

	now := time.Now()

	// Process each timed-out provision.
	for _, candidate := range timedOut {
		// Check context before each operation
		if ctx.Err() != nil {
			return
		}

		// The timeout snapshot can race a callback or another timeout sweep. Claim
		// this exact operation before making the on-chain rejection so only one
		// actor can settle it, and so no replacement can be installed underneath
		// the eventual cleanup.
		result := c.operations.TryClaimTimeout(candidate.LeaseUUID, candidate.ID)
		if !result.Claimed() {
			continue
		}

		c.settleTimedOutProvision(ctx, result.Record(), result.Claim(), now)
	}
}

func (c *TimeoutChecker) settleTimedOutProvision(
	ctx context.Context,
	p operation.Record,
	claim operation.SettlementClaim,
	now time.Time,
) {
	claimFinished := false
	defer func() {
		// A retryable chain failure must leave the provision available to the
		// next timeout sweep. This also releases the claim if a future early exit
		// or panic is added before terminal settlement completes.
		if !claimFinished {
			c.operations.ReleaseSettlement(claim)
		}
	}()

	// Reject the lease on chain FIRST, before untracking.
	// This prevents a race where the reconciler sees a PENDING lease that's
	// not in-flight and tries to provision it again.
	rejected, txHashes, err := c.rejecter.RejectLeases(ctx, []string{p.LeaseUUID}, truncateRejectReason("callback timeout"))
	if err != nil {
		if isTerminalAcknowledgeError(err) {
			// The lease is no longer PENDING (ErrLeaseNotPending) or no longer
			// exists (ErrLeaseNotFound), so RejectLeases can NEVER succeed for
			// it. This can be a reconciler-registered ACTIVE-lease re-provision
			// (reconciler.go) whose callback was lost, or an originally-PENDING
			// entry whose state changed concurrently in the unlocked window
			// between the timeout snapshot and this reject (e.g. a racing
			// success-ack that already flipped it ACTIVE). Either way, retrying
			// forever would wedge the lease in-flight permanently and inflate
			// InFlightProvisions (which also skews capacity/routing signals).
			// Untrack it and hand it back to the reconciler, which owns the
			// re-provision / FailCount / close path. (ENG-337)
			claimFinished = c.operations.FinishSettlement(claim)
			metrics.CallbackTimeoutsTotal.Inc()
			slog.Warn("timed-out provision is not a pending lease; untracked and handed back to reconciler",
				"lease_uuid", p.LeaseUUID,
				"tenant", p.Tenant,
				"backend", p.Backend,
				"age", now.Sub(p.StartedAt),
				"error", err,
			)
			return
		}
		slog.Error("failed to reject timed-out lease, keeping in-flight to prevent re-provision",
			"lease_uuid", p.LeaseUUID,
			"error", err,
		)
		// Keep in-flight so reconciler doesn't try to re-provision.
		// Next timeout check will retry the rejection.
		return
	}

	// Only untrack AFTER successful rejection
	operationLabel := timeoutOperationLabel(p.Kind)
	claimFinished = c.operations.FinishSettlement(claim)
	metrics.CallbackTimeoutsTotal.Inc()
	metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, p.Backend, operationLabel).Inc()

	// Record duration (from start until timeout)
	duration := now.Sub(p.StartedAt).Seconds()
	metrics.ProvisioningDuration.WithLabelValues(p.Backend, operationLabel).Observe(duration)

	slog.Warn("rejected timed-out provision",
		"lease_uuid", p.LeaseUUID,
		"tenant", p.Tenant,
		"backend", p.Backend,
		"operation", operationLabel,
		"age", now.Sub(p.StartedAt),
		"rejected", rejected,
		"tx_hashes", txHashes,
	)
}

func timeoutOperationLabel(kind operation.Kind) string {
	if kind == operation.KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}

var _ TimeoutOperations = (*operation.Registry)(nil)
