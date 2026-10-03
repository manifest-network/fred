package docker

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	"github.com/manifest-network/fred/internal/metrics/background"
)

// The container event loop delivers container deaths to their lease actors
// within moments, ahead of the periodic reconcile sweep. Since ENG-799 it is
// also the only source of the live provenance that lets a death count against
// a lease's terminal budget: a death the sweep finds carries none and never
// counts. It therefore lives apart from recover.go's sweep, and is built to
// stay subscribed:
//
//   - The reader (container_event_reader.go) reads one subscription at a time
//     and owns its failurecause event session, which never leaves the
//     reader's frame. Its per-event work is bookkeeping only; each death goes
//     to a bounded queue. dockerd skips events for a subscriber that falls
//     behind (a 1,024-event buffer, then 100ms per event), and a skipped
//     "kill" would let the following "die" count, so the reader waits on
//     nothing but its stream: not on death processing, and not on log output.
//     It has no logger; a death it drops from a full queue is only counted.
//   - One dispatcher drains the queue in arrival order, which also keeps each
//     container's deaths in order. It does the blocking work: storage identity
//     re-verification, the lease lookup, the runtime-generation proof and
//     actor routing. A death it cannot deliver is dropped and counted; the
//     sweep later finds it, unattributed.
//   - One recorder writes every death the reader hands it, through its own
//     bounded queue, into the live-death ledger (live_death_ledger.go), for a
//     container that is in no Ready projection yet and so cannot be routed
//     (ENG-1125). The ledger takes a lock, so the reader never writes it.
//   - One reporter logs the deaths the reader dropped, at most once per
//     containerDeathOverflowReportInterval, so a stalled log sink stalls only
//     the reporter.
//   - A stream error, a closed stream or a non-terminal verification failure
//     backs off and reconnects with a fresh session; only shutdown or a
//     latched terminal storage-authority failure stops the loop.
//
// fred_docker_backend_container_event_stream_total counts every connect,
// reconnect and exit. Deaths during a gap, and the first death of any run that
// started before the stream (re)connected, are attributed unknown.

const (
	// containerDeathQueueCapacity bounds the deaths waiting for dispatch. Only
	// deaths queue; start and kill never wait. Generous next to the hundreds of
	// leases a host runs, it absorbs a death storm while one verification is
	// slow. A death that finds it full is dropped toward unknown, never blocks.
	containerDeathQueueCapacity = 4096
	// containerEventVerifyTimeout bounds each storage re-verification the loop
	// makes, so a daemon that stops answering "docker info" cannot wedge it.
	// It bounds the verification only: a death's wait in the queue, and the
	// dispatcher's wait for the backend's storage-verification lock while
	// another verifier holds it with a longer deadline, come on top. That wait
	// shows as fred_docker_backend_container_death_queue_depth.
	containerEventVerifyTimeout = 10 * time.Second
	// containerEventRetryInitial and containerEventRetryMax bound the backoff
	// between reconnect attempts. The backoff resets once a subscription
	// delivers an event.
	containerEventRetryInitial = time.Second
	containerEventRetryMax     = 30 * time.Second
	// containerDeathOverflowReportInterval spaces the reporter's log lines. The
	// first death dropped after a quiet interval is logged at once; later drops
	// are summed into the next line.
	containerDeathOverflowReportInterval = 10 * time.Second
	// containerDeathOverflowMessage is the reporter's summary of the deaths the
	// reader dropped, with their count as "dropped".
	containerDeathOverflowMessage = "container deaths dropped: dispatch queue full; the reconcile sweep will find them, unattributed"
	// containerDeathOverflowReporter labels the reporter's contained panics in
	// fred_background_goroutine_panics_total.
	containerDeathOverflowReporter = "container_death_overflow_reporter"
)

// Outcomes of fred_docker_backend_container_event_stream_total, a closed set.
const (
	containerEventStreamConnected = "connected"
	containerEventStreamReconnect = "reconnect"
	containerEventStreamExited    = "exited"
)

var containerEventStreamOutcomes = []string{
	containerEventStreamConnected, containerEventStreamReconnect, containerEventStreamExited,
}

// dieEventSourceEventLoop labels the event loop's deaths in
// fred_docker_backend_die_event_dropped_total.
const dieEventSourceEventLoop = "event_loop"

// eventLoopDeathsDropped is the event loop's series of that counter, resolved
// once so that counting a drop is one atomic add, with no label lookup.
var eventLoopDeathsDropped = dieEventDroppedTotal.WithLabelValues(dieEventSourceEventLoop)

// containerEventLoop subscribes to the Docker container events that matter to
// leases and dispatches every death to its lease actor; see the file comment.
func (b *Backend) containerEventLoop() {
	b.runContainerEventLoop(containerEventRetryInitial, containerEventRetryMax)
}

// runContainerEventLoop is containerEventLoop with its reconnect backoff as
// parameters, so a test can drive many reconnects quickly. It owns the
// dispatcher and the overflow reporter, and stops both before it returns.
func (b *Backend) runContainerEventLoop(retryInitial, retryMax time.Duration) {
	deaths := make(chan failurecause.Provenance, containerDeathQueueCapacity)
	ledger := make(chan failurecause.Provenance, liveDeathLedgerCapacity)
	overflow := newContainerDeathOverflow()
	reporting, stopReporting := context.WithCancel(b.stopCtx)
	var workers sync.WaitGroup
	workers.Go(func() { b.dispatchLiveContainerDeaths(deaths) })
	workers.Go(func() { b.recordLiveContainerDeaths(ledger) })
	workers.Go(func() {
		b.reportContainerDeathOverflow(reporting, overflow, containerDeathOverflowReportInterval)
	})
	defer func() {
		close(deaths)
		close(ledger)
		stopReporting()
		workers.Wait()
		containerDeathQueueDepth.Set(0)
		containerEventStreamTotal.WithLabelValues(containerEventStreamExited).Inc()
	}()
	reader := containerEventReader{stop: b.stopCtx.Done(), deaths: deaths, ledger: ledger, overflow: overflow}

	retry := retryInitial
	for b.containerEventLoopMayRun() {
		if err := b.verifyContainerEventAuthority(); err != nil {
			if !b.containerEventLoopMayRun() {
				break
			}
			b.logger.Warn("container event stream not connected: backend storage identity unverified; retrying",
				"error", err, "retry_in", retry)
		} else {
			containerEventStreamTotal.WithLabelValues(containerEventStreamConnected).Inc()
			events, errs := b.docker.ContainerEvents(b.stopCtx)
			// While no subscription is held, no death can be recorded, so a
			// startup failure does not wait for one (ENG-1125).
			b.liveDeaths.markLiveDeathStream(true)
			delivered, err := reader.consume(events, errs)
			b.liveDeaths.markLiveDeathStream(false)
			if delivered {
				retry = retryInitial
			}
			// Logged here, after the subscription is gone, never by the reader.
			if err != nil {
				b.logger.Warn("container event stream error, reconnecting", "error", err)
			}
			if !b.containerEventLoopMayRun() {
				break
			}
		}
		containerEventStreamTotal.WithLabelValues(containerEventStreamReconnect).Inc()
		select {
		case <-b.stopCtx.Done():
		case <-time.After(retry):
		}
		retry = min(retry*2, retryMax)
	}
}

// containerEventLoopMayRun is false once the backend is stopping or its
// storage authority has latched a terminal failure; anything else is retried.
func (b *Backend) containerEventLoopMayRun() bool {
	return b.stopCtx.Err() == nil && b.terminalStorageAuthorityError() == nil
}

// verifyContainerEventAuthority re-attests storage identity, bounded so that a
// hung daemon cannot wedge the loop. Before every subscription and every death
// dispatch, as ENG-632 requires before observing backend state.
func (b *Backend) verifyContainerEventAuthority() error {
	ctx, cancel := context.WithTimeout(b.stopCtx, containerEventVerifyTimeout)
	defer cancel()
	return b.requireStorageIdentity(ctx)
}

// reportContainerDeathOverflow logs the deaths the reader dropped from a full
// queue, on this goroutine rather than the reader's: a stalled log sink holds
// only the reporter, and the reader keeps counting. The first drop after a
// quiet interval is logged at once, later ones are summed into at most one
// line per interval, and any still unreported when ctx ends are logged then.
func (b *Backend) reportContainerDeathOverflow(ctx context.Context, overflow *containerDeathOverflow, interval time.Duration) {
	defer func() { b.logContainerDeathOverflow(overflow.takeUnreported()) }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-overflow.wakeups():
		}
		b.logContainerDeathOverflow(overflow.takeUnreported())
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// logContainerDeathOverflow logs one summary of dropped deaths. The log
// handler is foreign code, so a panic in it is contained and counted: the
// reporter keeps running, and the drops stay counted in
// die_event_dropped_total either way.
func (b *Backend) logContainerDeathOverflow(dropped uint64) {
	if dropped == 0 {
		return
	}
	defer func() {
		value := recover()
		if value == nil {
			return
		}
		background.GoroutinePanicsTotal.WithLabelValues(containerDeathOverflowReporter).Inc()
		// The handler most likely panicked itself, so reporting the panic
		// through it is best effort: a second panic ends here.
		defer func() { _ = recover() }()
		b.logger.Error("container death overflow report panicked", "dropped", dropped, "panic", value)
	}()
	b.logger.Warn(containerDeathOverflowMessage, "dropped", dropped)
}

// recordLiveContainerDeaths writes the deaths the reader handed over into the
// live-death ledger until the loop closes the queue. It is the ledger's only
// writer of deaths, on its own goroutine because the ledger takes a lock the
// reader must never wait on (ENG-799, ENG-1125). It does no other work, so it
// keeps up with the reader far ahead of the dispatcher.
func (b *Backend) recordLiveContainerDeaths(ledger <-chan failurecause.Provenance) {
	for death := range ledger {
		b.liveDeaths.recordLiveDeath(death)
	}
}

// dispatchLiveContainerDeaths routes queued deaths in arrival order until the
// loop closes the queue. The queue depth is sampled at each enqueue and each
// dequeue, so a dispatcher that falls behind shows as a rising depth before
// any death is dropped.
func (b *Backend) dispatchLiveContainerDeaths(deaths <-chan failurecause.Provenance) {
	for death := range deaths {
		containerDeathQueueDepth.Set(float64(len(deaths)))
		b.dispatchLiveContainerDeath(death)
	}
}

// dispatchLiveContainerDeath routes one live death. The provenance names the
// container it was minted for, so the observation is built for exactly that
// container. This is the only caller of leasesm.NewLiveContainerDiedObservation
// (internal/testutil).
func (b *Backend) dispatchLiveContainerDeath(death failurecause.Provenance) {
	containerID := death.InstanceID()
	if b.stopCtx.Err() != nil || containerID == "" {
		return
	}
	if err := b.verifyContainerEventAuthority(); err != nil {
		eventLoopDeathsDropped.Inc()
		b.logger.Error("container death dropped: backend storage identity unverified; the reconcile sweep will find it, unattributed",
			"container_id", leasesm.ShortID(containerID), "error", err)
		return
	}
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	if b.releaseStore == nil {
		b.logger.Error("container event ignored without durable release authority",
			"lease_uuid", leaseUUID)
		return
	}
	generation, err := b.releaseStore.ProveRuntimeGeneration(leaseUUID)
	if err != nil {
		b.logger.Warn("container event ignored without current runtime generation",
			"lease_uuid", leaseUUID, "error", err)
		return
	}
	observation, err := leasesm.NewLiveContainerDiedObservation(generation, death)
	if err != nil {
		b.logger.Error("invalid container event ignored", "error", err)
		return
	}
	b.dispatchContainerDeathObservation(observation, containerID, dieEventSourceEventLoop)
}

// redispatchStartupDeaths hands the recorded live deaths of a new Ready
// cohort's containers back to the dispatch path. Such a death raced the
// provision's Ready transition: its die event came while the container was in
// no Ready projection, and the event loop could not route it (ENG-1125). It
// runs at the provision store's Ready entry, under the projection lock, so it
// never blocks: the routing happens on a worker, after the Ready projection
// is visible, and the actor handles the death after its Ready transition.
func (b *Backend) redispatchStartupDeaths(containerIDs []string) {
	deaths := b.liveDeaths.takeLiveDeaths(containerIDs)
	if len(deaths) == 0 || b.stopCtx.Err() != nil {
		return
	}
	b.wg.Go(func() {
		for _, death := range deaths {
			b.dispatchLiveContainerDeath(death)
		}
	})
}

// findLeaseByContainerID returns the lease UUID and true if a provision
// containing the given container ID is found. Returns ("", false) otherwise.
// Called under no lock; acquires read lock internally.
//
// O(N*M) linear scan over all leases and their containers. A reverse index
// would be O(1) but adds sync overhead across provision/deprovision/restart/
// update/recover. Fine at expected scale (hundreds of leases, 1-10 containers).
func (b *Backend) findLeaseByContainerID(containerID string) (string, bool) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	leaseUUID := ""
	for uuid, prov := range b.provisions {
		for _, cid := range prov.ContainerIDs {
			if cid == containerID {
				if leaseUUID != "" && leaseUUID != uuid {
					// A duplicate substrate identity has no unique actor owner.
					return "", false
				}
				leaseUUID = uuid
			}
		}
	}
	return leaseUUID, leaseUUID != ""
}
