package docker

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
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
//     to the subscription's bounded queue. dockerd skips events for a
//     subscriber that falls behind (a 1,024-event buffer, then 100ms per
//     event), and a skipped "kill" would let the following "die" count, so
//     the reader waits on nothing but its stream: not on death processing,
//     and not on log output. It has no logger; a death it drops from a full
//     queue is only counted.
//   - One recorder is the reader's only consumer. The loop hands it each
//     subscription's queue before the subscription opens and closes the queue
//     once the reader has returned. The recorder writes every death into the
//     live-death ledger (live_death_ledger.go) and only then forwards it to
//     the dispatcher, without waiting, so a death the dispatcher cannot route
//     yet is already recorded when a provision's Ready entry looks for it
//     (ENG-1125). It marks the stream in the ledger in the same order: up
//     before the first death of a subscription, down after the last one. The
//     ledger takes a lock, so the reader never writes it.
//   - One dispatcher drains its queue in arrival order, which also keeps each
//     container's deaths in order. It does the blocking work: storage identity
//     re-verification, the lease lookup, the runtime-generation proof and
//     actor routing. A death whose container is in no projection, or only in
//     a provision that is not Ready yet, is left to the ledger's readers; any
//     other death it cannot deliver is dropped and counted, and the sweep
//     later finds it, unattributed.
//   - One reporter logs the deaths the reader or the recorder dropped, at most
//     once per containerDeathOverflowReportInterval, so a stalled log sink
//     stalls only the reporter.
//   - A stream error, a closed stream or a non-terminal verification failure
//     backs off and reconnects with a fresh session; only shutdown or a
//     latched terminal storage-authority failure stops the loop.
//
// fred_docker_backend_container_event_stream_total counts every connect,
// reconnect and exit. Deaths during a gap, and the first death of any run that
// started before the stream (re)connected, are attributed unknown.

const (
	// containerDeathQueueCapacity bounds each of the loop's death queues: a
	// subscription's, which the recorder drains, and the dispatcher's. Only
	// deaths queue; start and kill never wait. Generous next to the hundreds
	// of leases a host runs, it absorbs a death storm while one verification
	// is slow. A death that finds a queue full is dropped toward unknown,
	// never blocks.
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
	// loop dropped, with their count as "dropped".
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
// recorder, the dispatcher and the overflow reporter, and stops them in that
// order before it returns: the recorder once it has recorded and forwarded
// the last death, the dispatcher once it has drained its queue, and the
// reporter last, so that it logs every drop either of the others counted.
func (b *Backend) runContainerEventLoop(retryInitial, retryMax time.Duration) {
	subscriptions := make(chan (<-chan failurecause.Provenance))
	dispatch := make(chan failurecause.Provenance, containerDeathQueueCapacity)
	overflow := newContainerDeathOverflow()
	reporting, stopReporting := context.WithCancel(b.stopCtx)
	var pipeline, reporter sync.WaitGroup
	pipeline.Go(func() { b.recordLiveContainerDeaths(subscriptions, dispatch, overflow) })
	pipeline.Go(func() { b.dispatchLiveContainerDeaths(dispatch) })
	reporter.Go(func() {
		b.reportContainerDeathOverflow(reporting, overflow, containerDeathOverflowReportInterval)
	})
	defer func() {
		// The recorder closes the dispatcher's queue once this closes its own.
		close(subscriptions)
		pipeline.Wait()
		stopReporting()
		reporter.Wait()
		containerDeathQueueDepth.Set(0)
		containerEventStreamTotal.WithLabelValues(containerEventStreamExited).Inc()
	}()

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
			delivered, err := b.consumeContainerEventSubscription(subscriptions, overflow)
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

// consumeContainerEventSubscription opens one subscription and reads it until
// it ends, reporting what the reader reports. The subscription's death queue
// goes to the recorder before the subscription opens, so the loop never waits
// while one is open: the hand-over waits only for the recorder to finish the
// previous subscription's deaths. Closing the queue once the reader has
// returned is how the recorder learns that the reader handed over its last
// death.
func (b *Backend) consumeContainerEventSubscription(
	subscriptions chan<- (<-chan failurecause.Provenance), overflow *containerDeathOverflow,
) (delivered bool, err error) {
	deaths := make(chan failurecause.Provenance, containerDeathQueueCapacity)
	subscriptions <- deaths
	defer close(deaths)
	events, errs := b.docker.ContainerEvents(b.stopCtx)
	reader := containerEventReader{stop: b.stopCtx.Done(), deaths: deaths, overflow: overflow}
	return reader.consume(events, errs)
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

// reportContainerDeathOverflow logs the deaths the loop dropped from a full
// queue, on this goroutine rather than the reader's or the recorder's: a
// stalled log sink holds only the reporter, and the drops keep being counted.
// The first drop after a quiet interval is logged at once, later ones are
// summed into at most one line per interval, and any still unreported when
// ctx ends are logged then.
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

// recordLiveContainerDeaths is the reader's only consumer and the live-death
// ledger's only writer. It takes the subscriptions' queues from the loop in
// order. For each one it marks the stream up, then records every death the
// reader handed over and only after that forwards it to the dispatcher, and
// once the loop has closed the queue it marks the stream down. So a death is
// in the ledger before the dispatcher can look it up (ENG-1125), and the
// stream reads down only after every death the reader observed on it is
// recorded. The forward never waits: a full dispatcher queue drops the
// dispatch, counted like the reader's drops, and the death stays recorded. It
// runs on its own goroutine because the ledger takes a lock the reader must
// never wait on (ENG-799). It closes the dispatcher's queue when the loop
// closes its own.
func (b *Backend) recordLiveContainerDeaths(
	subscriptions <-chan (<-chan failurecause.Provenance),
	dispatch chan<- failurecause.Provenance,
	overflow *containerDeathOverflow,
) {
	defer close(dispatch)
	for deaths := range subscriptions {
		b.liveDeaths.markLiveDeathStream(true)
		for death := range deaths {
			b.liveDeaths.recordLiveDeath(death)
			select {
			case dispatch <- death:
				containerDeathQueueDepth.Set(float64(len(dispatch)))
			default:
				overflow.record()
			}
		}
		b.liveDeaths.markLiveDeathStream(false)
	}
}

// dispatchLiveContainerDeaths routes the recorded deaths in arrival order
// until the recorder closes the queue. The queue depth is sampled at each
// enqueue and each dequeue, so a dispatcher that falls behind shows as a
// rising depth before any death is dropped.
func (b *Backend) dispatchLiveContainerDeaths(dispatch <-chan failurecause.Provenance) {
	for death := range dispatch {
		containerDeathQueueDepth.Set(float64(len(dispatch)))
		b.dispatchLiveContainerDeath(death)
	}
}

// dispatchLiveContainerDeath routes one live death. The provenance names the
// container it was minted for, so the observation is built for exactly that
// container. This is the only caller of leasesm.NewLiveContainerDiedObservation,
// and only the dispatcher and the Ready-entry re-dispatch call it
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
	leaseUUID, status, found := b.findLeaseByContainerID(containerID)
	if !found || status == backend.ProvisionStatusProvisioning {
		// No actor can take this death now: no projection names the
		// container, or only a provision that is not Ready yet, which routing
		// refuses. The recorder wrote the death into the live-death ledger
		// before forwarding it here, so when the container is in the
		// provision's Ready cohort, the Ready entry takes it from there
		// (redispatchStartupDeaths), and a startup failure takes its own
		// container's. A refusal here would count it as a lost death.
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
// no Ready projection, and the event loop could not route it (ENG-1125). The
// recorder writes every death into the ledger before the dispatcher can look
// it up, and this runs at the provision store's Ready entry, under the same
// projection lock that the lookup reads: a lookup that missed the Ready
// projection came before this entry, so the death it looked up was recorded
// before this reads the ledger. It never blocks: the routing happens on a
// worker, after the Ready projection is visible, and the actor handles the
// death after its Ready transition.
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

// findLeaseByContainerID returns the UUID and the projection status of the
// one provision whose containers include containerID, both read under one
// read lock. found is false when no provision, or more than one, includes it.
// Called under no lock.
//
// O(N*M) linear scan over all leases and their containers. A reverse index
// would be O(1) but adds sync overhead across provision/deprovision/restart/
// update/recover. Fine at expected scale (hundreds of leases, 1-10 containers).
func (b *Backend) findLeaseByContainerID(containerID string) (leaseUUID string, status backend.ProvisionStatus, found bool) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	for uuid, prov := range b.provisions {
		for _, cid := range prov.ContainerIDs {
			if cid == containerID {
				if leaseUUID != "" && leaseUUID != uuid {
					// A duplicate substrate identity has no unique actor owner.
					return "", "", false
				}
				leaseUUID, status = uuid, prov.Status
			}
		}
	}
	return leaseUUID, status, leaseUUID != ""
}
