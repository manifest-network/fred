package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	deferredCloseCapacity       = 1024
	deferredCloseWorkers        = 4
	deferredCloseInterval       = time.Second
	deferredCloseMaxInterval    = 5 * time.Second
	deferredCloseAttemptTimeout = 30 * time.Second
)

var errDeferredCloseUnavailable = errors.New("deferred close capacity is unavailable")

// Every hint has a distinct immutable identity, even when its proof names the
// same lease. A running attempt can therefore consume only the hint it saw.
type deferredCloseHint struct {
	proof placement.DeferredDeprovision
}

type deferredCloseEntry struct {
	hint       *deferredCloseHint
	admittedAt time.Time
	next       time.Time
	delay      time.Duration
	running    *deferredCloseAttempt
}

// Only dispatch constructs attempts, while holding the scheduler mutex. The
// worker reads the captured hint; coalescing can replace the entry's hint
// without changing the proof or lifetime of work already dispatched.
type deferredCloseAttempt struct {
	entry *deferredCloseEntry
	hint  *deferredCloseHint
}

// deferredCloseScheduler owns bounded, coalesced retry hints. Only an opaque
// coordinator-issued deferral can enter it; each retry reclaims current
// placement and lifecycle authority. The chain and periodic reconciliation
// remain the durable recovery path across queue saturation and process loss.
type deferredCloseScheduler struct {
	mu      sync.Mutex
	entries map[string]*deferredCloseEntry
	started bool
	closed  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	wake    chan struct{}
}

func newDeferredCloseScheduler() *deferredCloseScheduler {
	return &deferredCloseScheduler{entries: make(map[string]*deferredCloseEntry), wake: make(chan struct{}, 1)}
}

func (scheduler *deferredCloseScheduler) enqueue(proof placement.DeferredDeprovision) error {
	if !proof.Valid() {
		return errors.New("invalid deferred close proof")
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed || !scheduler.started {
		metrics.DeferredClosesTotal.WithLabelValues("unavailable", string(proof.Reason())).Inc()
		return errDeferredCloseUnavailable
	}
	defer scheduler.updateOldestAgeLocked(time.Now())
	if entry, exists := scheduler.entries[proof.LeaseUUID()]; exists {
		entry.hint = &deferredCloseHint{proof: proof}
		metrics.DeferredClosesTotal.WithLabelValues("coalesced", string(proof.Reason())).Inc()
		return nil
	}
	if len(scheduler.entries) >= deferredCloseCapacity {
		metrics.DeferredClosesTotal.WithLabelValues("full", string(proof.Reason())).Inc()
		return errDeferredCloseUnavailable
	}
	now := time.Now()
	scheduler.entries[proof.LeaseUUID()] = &deferredCloseEntry{
		hint: &deferredCloseHint{proof: proof}, admittedAt: now,
		delay: deferredCloseInterval, next: now.Add(deferredCloseInterval),
	}
	metrics.DeferredClosesPending.Inc()
	metrics.DeferredClosesTotal.WithLabelValues("queued", string(proof.Reason())).Inc()
	scheduler.wakeDispatch()
	slog.Info("lease close deferred", "lease_uuid", proof.LeaseUUID(), "reason", proof.Reason())
	return nil
}

func (scheduler *deferredCloseScheduler) start(parent context.Context) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.started || scheduler.closed {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	scheduler.cancel, scheduler.started = cancel, true
	jobs := make(chan *deferredCloseAttempt)
	for range deferredCloseWorkers {
		scheduler.wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case attempt := <-jobs:
					scheduler.retry(ctx, attempt)
				}
			}
		})
	}
	scheduler.wg.Go(func() {
		ticker := time.NewTicker(deferredCloseInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				scheduler.dispatch(now, jobs)
			case <-scheduler.wake:
				scheduler.dispatch(time.Now(), jobs)
			}
		}
	})
}

func (scheduler *deferredCloseScheduler) dispatch(now time.Time, jobs chan<- *deferredCloseAttempt) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return
	}
	scheduler.updateOldestAgeLocked(now)
	for range deferredCloseWorkers {
		var next *deferredCloseEntry
		for _, entry := range scheduler.entries {
			if entry.running == nil && !entry.next.After(now) && (next == nil || entry.next.Before(next.next)) {
				next = entry
			}
		}
		if next == nil {
			return
		}
		attempt := &deferredCloseAttempt{entry: next, hint: next.hint}
		select {
		case jobs <- attempt:
			next.running = attempt
		default:
			return
		}
	}
}

func (scheduler *deferredCloseScheduler) retry(ctx context.Context, attempt *deferredCloseAttempt) {
	defer scheduler.wakeDispatch()
	attemptCtx, cancel := context.WithTimeout(ctx, deferredCloseAttemptTimeout)
	result, panicErr := executeDeferredClose(attemptCtx, attempt.hint.proof)
	cancel()
	scheduler.finishAttempt(attempt, result, panicErr)
}

func (scheduler *deferredCloseScheduler) finishAttempt(
	attempt *deferredCloseAttempt,
	result placement.DeprovisionEventResult,
	panicErr error,
) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed || attempt == nil || attempt.hint == nil {
		return
	}
	proof := attempt.hint.proof
	entry := scheduler.entries[proof.LeaseUUID()]
	if entry == nil || entry != attempt.entry || entry.running != attempt {
		return
	}
	defer scheduler.updateOldestAgeLocked(time.Now())
	entry.running = nil
	newerHint := entry.hint != attempt.hint
	if result.Disposition() == placement.DeprovisionEventDeferred {
		if !newerHint {
			entry.hint = &deferredCloseHint{proof: result.Deferred()}
		}
		entry.delay = min(2*entry.delay, deferredCloseMaxInterval)
		entry.next = time.Now().Add(entry.delay)
		metrics.DeferredClosesTotal.WithLabelValues("retry", string(result.Deferred().Reason())).Inc()
		return
	}
	if newerHint {
		// Completion accounts for the dispatched hint only. A later hint must
		// reacquire current authority in a separate attempt, even if this one
		// failed or panicked. It retains the same bounded queue slot and age.
		entry.delay = deferredCloseInterval
		entry.next = time.Now().Add(entry.delay)
	} else {
		delete(scheduler.entries, proof.LeaseUUID())
		metrics.DeferredClosesPending.Dec()
	}
	if result.Disposition() == placement.DeprovisionEventCompleted {
		metrics.DeferredClosesTotal.WithLabelValues("dispatched", string(proof.Reason())).Inc()
		slog.Info("deferred lease close dispatched", "lease_uuid", proof.LeaseUUID())
		return
	}
	metrics.DeferredClosesTotal.WithLabelValues("failed", string(proof.Reason())).Inc()
	slog.Error("deferred lease close failed; reconciliation will retry",
		"lease_uuid", proof.LeaseUUID(), "error", errors.Join(result.Err(), panicErr))
}

// Age includes running work and survives coalescing and backoff. Dispatch's
// one-second ticker refreshes it even when every worker is occupied.
// Caller holds scheduler.mu.
func (scheduler *deferredCloseScheduler) updateOldestAgeLocked(now time.Time) {
	var oldestAge float64
	for _, entry := range scheduler.entries {
		oldestAge = max(oldestAge, now.Sub(entry.admittedAt).Seconds())
	}
	metrics.DeferredClosesOldestAge.Set(oldestAge)
}

func (scheduler *deferredCloseScheduler) wakeDispatch() {
	select {
	case scheduler.wake <- struct{}{}:
	default:
	}
}

// Retrying leaves Watermill's panic boundary. Contain a failed attempt before
// returning its capacity; the worker remains available to other leases.
func executeDeferredClose(ctx context.Context, proof placement.DeferredDeprovision) (
	result placement.DeprovisionEventResult, err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("deferred close panicked (%T)", recovered)
		}
	}()
	return proof.Retry(ctx), nil
}

// stop closes admission and cancels workers without waiting under the manager's
// drain boundary. Close joins workers before any store can be closed.
func (scheduler *deferredCloseScheduler) stop() {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return
	}
	scheduler.closed = true
	if scheduler.cancel != nil {
		scheduler.cancel()
	}
	for _, entry := range scheduler.entries {
		metrics.DeferredClosesTotal.WithLabelValues("stopped", string(entry.hint.proof.Reason())).Inc()
	}
	metrics.DeferredClosesPending.Sub(float64(len(scheduler.entries)))
	clear(scheduler.entries)
	metrics.DeferredClosesOldestAge.Set(0)
}
