package provisioner

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
	backgroundmetrics "github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	defaultTimeoutSettlementBudget  = 10 * time.Second
	defaultTimeoutSettlementWorkers = 4
)

// TimeoutChecker monitors in-flight provisions and rejects timed-out ones.
// It runs as a background goroutine and periodically checks for provisions
// that have exceeded the callback timeout.
type TimeoutChecker struct {
	coordinator   *placement.TimeoutCoordinator
	timeout       time.Duration
	checkInterval time.Duration
	settleBudget  time.Duration
	workers       int

	// A timeout candidate is a one-shot join across the volatile Registry and
	// durable placement generation. Overlapping sweeps add no liveness and make
	// fair cursor advancement ambiguous, so only one pass may own candidates.
	sweepMu sync.Mutex
	cursor  string
}

// TimeoutCheckerConfig configures the timeout checker.
type TimeoutCheckerConfig struct {
	Coordinator   *placement.TimeoutCoordinator
	Timeout       time.Duration // Callback timeout (how long to wait before considering a provision timed out)
	CheckInterval time.Duration // How often to check for timeouts
	// SettlementBudget bounds the whole sweep, not each candidate. A stalled
	// chain endpoint can therefore delay one cadence but never N candidates x N
	// per-call deadlines. Zero selects the production default.
	SettlementBudget time.Duration
	// Workers bounds independent exact timeout settlements. Zero selects the
	// production default.
	Workers int
}

// NewTimeoutChecker creates a new TimeoutChecker.
func NewTimeoutChecker(cfg TimeoutCheckerConfig) (*TimeoutChecker, error) {
	if cfg.Coordinator == nil || !cfg.Coordinator.Valid() {
		return nil, errors.New("timeout checker requires a joined operation coordinator")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("timeout checker callback timeout must be positive")
	}
	if cfg.CheckInterval <= 0 {
		return nil, errors.New("timeout checker interval must be positive")
	}
	budget := cfg.SettlementBudget
	if budget == 0 {
		budget = defaultTimeoutSettlementBudget
	}
	if budget < 0 {
		return nil, errors.New("timeout checker settlement budget must be positive")
	}
	workers := cfg.Workers
	if workers == 0 {
		workers = defaultTimeoutSettlementWorkers
	}
	if workers < 0 {
		return nil, errors.New("timeout checker worker count must be positive")
	}
	return &TimeoutChecker{
		coordinator: cfg.Coordinator,
		timeout:     cfg.Timeout, checkInterval: cfg.CheckInterval,
		settleBudget: budget, workers: workers,
	}, nil
}

// Start begins the timeout checker loop. It runs until the context is canceled.
func (c *TimeoutChecker) Start(ctx context.Context) {
	if c == nil {
		return
	}
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

// CheckOnce performs one sweep of the operation registry: every provision
// older than the callback timeout is rejected on-chain, and untracked
// only once that rejection has either succeeded or become impossible.
// This is the body of Start's ticker loop, run on every tick.
func (c *TimeoutChecker) CheckOnce(ctx context.Context) {
	if c == nil || ctx == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			backgroundmetrics.GoroutinePanicsTotal.WithLabelValues("timeout_checker_sweep").Inc()
			slog.Error("callback timeout sweep panicked; preserving unsettled operations",
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	if !c.sweepMu.TryLock() {
		return
	}
	defer c.sweepMu.Unlock()

	timedOut := c.coordinator.TimedOut(c.timeout)

	if len(timedOut) == 0 {
		return
	}

	slog.Warn("found timed-out provisions",
		"count", len(timedOut),
		"timeout", c.timeout,
	)

	// Registry iteration is map-backed. Stable ordering plus a durable-for-this-
	// process cursor means a full lane of stalled calls cannot monopolize every
	// later cadence: the next pass starts strictly after the last submitted key.
	slices.SortFunc(timedOut, func(left, right placement.TimeoutCandidate) int {
		return strings.Compare(timeoutCandidateKey(left), timeoutCandidateKey(right))
	})
	start := timeoutCandidateStart(timedOut, c.cursor)
	ordered := make([]placement.TimeoutCandidate, 0, len(timedOut))
	for offset := range len(timedOut) {
		ordered = append(ordered, timedOut[(start+offset)%len(timedOut)])
	}

	sweepCtx, cancel := context.WithTimeout(ctx, c.settleBudget)
	defer cancel()
	jobs := make(chan placement.TimeoutCandidate)
	workerCount := min(c.workers, len(ordered))
	var group sync.WaitGroup
	for range workerCount {
		group.Go(func() {
			for candidate := range jobs {
				if sweepCtx.Err() != nil {
					continue
				}
				c.settleTimeoutCandidatePanicSafe(sweepCtx, candidate)
			}
		})
	}

	lastSubmitted := ""
submit:
	for _, candidate := range ordered {
		if sweepCtx.Err() != nil {
			break
		}
		select {
		case <-sweepCtx.Done():
			break submit
		case jobs <- candidate:
			lastSubmitted = timeoutCandidateKey(candidate)
		}
	}
	close(jobs)
	group.Wait()
	if lastSubmitted != "" {
		c.cursor = lastSubmitted
	}
}

func (c *TimeoutChecker) settleTimeoutCandidatePanicSafe(
	ctx context.Context,
	candidate placement.TimeoutCandidate,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			backgroundmetrics.GoroutinePanicsTotal.WithLabelValues("timeout_checker_candidate").Inc()
			metadata := candidate.Metadata()
			slog.Error("callback timeout candidate panicked; preserving exact operation",
				"lease_uuid", metadata.LeaseUUID(),
				"operation_fingerprint", metadata.ID(),
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	c.settleTimeoutCandidate(ctx, candidate)
}

func timeoutCandidateKey(candidate placement.TimeoutCandidate) string {
	metadata := candidate.Metadata()
	return metadata.LeaseUUID() + "\x00" + metadata.ID().String()
}

func timeoutCandidateStart(candidates []placement.TimeoutCandidate, after string) int {
	if len(candidates) == 0 || after == "" {
		return 0
	}
	index, _ := slices.BinarySearchFunc(candidates, after,
		func(candidate placement.TimeoutCandidate, target string) int {
			return strings.Compare(timeoutCandidateKey(candidate), target)
		})
	for index < len(candidates) && timeoutCandidateKey(candidates[index]) <= after {
		index++
	}
	if index == len(candidates) {
		return 0
	}
	return index
}

func (c *TimeoutChecker) settleTimeoutCandidate(
	ctx context.Context,
	candidate placement.TimeoutCandidate,
) {
	if ctx.Err() != nil {
		return
	}
	result := c.coordinator.Settle(ctx, candidate)
	p := result.Metadata()
	now := time.Now()
	switch result.Disposition() {
	case placement.TimeoutInvalid:
		slog.Error("invalid timeout settlement candidate",
			"lease_uuid", p.LeaseUUID(), "operation_fingerprint", p.ID(), "error", result.Err())
		return
	case placement.TimeoutSkipped:
		if result.Err() != nil {
			slog.Error("failed to join timeout operation with durable placement generation",
				"lease_uuid", p.LeaseUUID(), "operation_fingerprint", p.ID(), "error", result.Err())
		}
		return
	case placement.TimeoutRetry:
		slog.Error("failed to settle timed-out lease, preserving exact operation for retry",
			"lease_uuid", p.LeaseUUID(), "operation_fingerprint", p.ID(), "error", result.Err())
		return
	case placement.TimeoutLeaseTerminal:
		metrics.CallbackTimeoutsTotal.Inc()
		slog.Warn("timed-out provision is not a pending lease; untracked and handed back to reconciler",
			"lease_uuid", p.LeaseUUID(), "tenant", p.Tenant(), "backend", p.Backend(),
			"age", now.Sub(p.StartedAt()), "error", result.Err())
		return
	case placement.TimeoutRejected:
	default:
		slog.Error("timeout coordinator returned an unknown disposition",
			"lease_uuid", p.LeaseUUID(), "operation_fingerprint", p.ID())
		return
	}

	operationLabel := timeoutOperationLabel(p.Kind())
	metrics.CallbackTimeoutsTotal.Inc()
	metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, p.Backend(), operationLabel).Inc()

	// Record duration (from start until timeout)
	duration := now.Sub(p.StartedAt()).Seconds()
	metrics.ProvisioningDuration.WithLabelValues(p.Backend(), operationLabel).Observe(duration)

	slog.Warn("rejected timed-out provision",
		"lease_uuid", p.LeaseUUID(),
		"tenant", p.Tenant(),
		"backend", p.Backend(),
		"operation", operationLabel,
		"age", now.Sub(p.StartedAt()),
		"rejected", result.Rejected(),
		"tx_hashes", result.TxHashes(),
	)
}

func timeoutOperationLabel(kind operation.Kind) string {
	if kind == operation.KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}
