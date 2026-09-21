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

type deferredCloseEntry struct {
	proof   placement.DeferredDeprovision
	next    time.Time
	delay   time.Duration
	running bool
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
	if _, exists := scheduler.entries[proof.LeaseUUID()]; exists {
		metrics.DeferredClosesTotal.WithLabelValues("coalesced", string(proof.Reason())).Inc()
		return nil
	}
	if len(scheduler.entries) >= deferredCloseCapacity {
		metrics.DeferredClosesTotal.WithLabelValues("full", string(proof.Reason())).Inc()
		return errDeferredCloseUnavailable
	}
	scheduler.entries[proof.LeaseUUID()] = &deferredCloseEntry{
		proof: proof, delay: deferredCloseInterval, next: time.Now().Add(deferredCloseInterval),
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
	jobs := make(chan *deferredCloseEntry)
	for range deferredCloseWorkers {
		scheduler.wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entry := <-jobs:
					scheduler.retry(ctx, entry)
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

func (scheduler *deferredCloseScheduler) dispatch(now time.Time, jobs chan<- *deferredCloseEntry) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return
	}
	for range deferredCloseWorkers {
		var next *deferredCloseEntry
		for _, entry := range scheduler.entries {
			if !entry.running && !entry.next.After(now) && (next == nil || entry.next.Before(next.next)) {
				next = entry
			}
		}
		if next == nil {
			return
		}
		select {
		case jobs <- next:
			next.running = true
		default:
			return
		}
	}
}

func (scheduler *deferredCloseScheduler) retry(ctx context.Context, entry *deferredCloseEntry) {
	defer scheduler.wakeDispatch()
	attemptCtx, cancel := context.WithTimeout(ctx, deferredCloseAttemptTimeout)
	result, panicErr := executeDeferredClose(attemptCtx, entry.proof)
	cancel()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if scheduler.closed {
		return
	}
	if result.Disposition() == placement.DeprovisionEventDeferred {
		entry.proof = result.Deferred()
		entry.running = false
		entry.delay = min(2*entry.delay, deferredCloseMaxInterval)
		entry.next = time.Now().Add(entry.delay)
		metrics.DeferredClosesTotal.WithLabelValues("retry", string(entry.proof.Reason())).Inc()
		return
	}
	delete(scheduler.entries, entry.proof.LeaseUUID())
	metrics.DeferredClosesPending.Dec()
	if result.Disposition() == placement.DeprovisionEventCompleted {
		metrics.DeferredClosesTotal.WithLabelValues("dispatched", string(entry.proof.Reason())).Inc()
		slog.Info("deferred lease close dispatched", "lease_uuid", entry.proof.LeaseUUID())
		return
	}
	metrics.DeferredClosesTotal.WithLabelValues("failed", string(entry.proof.Reason())).Inc()
	slog.Error("deferred lease close failed; reconciliation will retry",
		"lease_uuid", entry.proof.LeaseUUID(), "error", errors.Join(result.Err(), panicErr))
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
		metrics.DeferredClosesTotal.WithLabelValues("stopped", string(entry.proof.Reason())).Inc()
	}
	metrics.DeferredClosesPending.Sub(float64(len(scheduler.entries)))
	clear(scheduler.entries)
}
