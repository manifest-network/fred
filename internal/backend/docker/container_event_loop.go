package docker

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// The container event loop delivers container deaths to their lease actors
// within moments, ahead of the periodic reconcile sweep. Since ENG-799 it is
// also the only source of the live provenance that lets a death count against
// a lease's terminal budget: a death the sweep finds carries none and never
// counts. It therefore lives apart from recover.go's sweep, and is built to
// stay subscribed:
//
//   - The reader owns one subscription and its failurecause event session,
//     which never leaves the reader's frame. Its per-event work is bookkeeping
//     only; each death goes to a bounded queue. dockerd skips events for a
//     subscriber that falls behind (a 1,024-event buffer, then 100ms per
//     event), and a skipped "kill" would let the following "die" count, so the
//     reader never waits on death processing.
//   - One dispatcher drains the queue in arrival order, which also keeps each
//     container's deaths in order. It does the blocking work: storage identity
//     re-verification, the lease lookup, the runtime-generation proof and
//     actor routing. A death it cannot deliver is dropped and counted; the
//     sweep later finds it, unattributed.
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
	containerEventVerifyTimeout = 10 * time.Second
	// containerEventRetryInitial and containerEventRetryMax bound the backoff
	// between reconnect attempts. The backoff resets once a subscription
	// delivers an event.
	containerEventRetryInitial = time.Second
	containerEventRetryMax     = 30 * time.Second
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

// liveContainerDeath is one die event as the reader observed it, queued for
// dispatch with the provenance its session minted.
type liveContainerDeath struct {
	containerID string
	provenance  failurecause.Provenance
}

// containerEventLoop subscribes to the Docker container events that matter to
// leases and dispatches every death to its lease actor; see the file comment.
func (b *Backend) containerEventLoop() {
	b.runContainerEventLoop(containerEventRetryInitial, containerEventRetryMax)
}

// runContainerEventLoop is containerEventLoop with its reconnect backoff as
// parameters, so a test can drive many reconnects quickly.
func (b *Backend) runContainerEventLoop(retryInitial, retryMax time.Duration) {
	deaths := make(chan liveContainerDeath, containerDeathQueueCapacity)
	var dispatcher sync.WaitGroup
	dispatcher.Go(func() { b.dispatchLiveContainerDeaths(deaths) })
	defer func() {
		close(deaths)
		dispatcher.Wait()
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
			if b.consumeContainerEventStream(deaths) {
				retry = retryInitial
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

// consumeContainerEventStream reads one subscription until it ends or the
// backend stops, and reports whether it delivered any event. It owns that
// subscription's event session: one per connection, because events missed in
// a reconnect gap are unknowable.
func (b *Backend) consumeContainerEventStream(deaths chan<- liveContainerDeath) bool {
	eventCh, errCh := b.docker.ContainerEvents(b.stopCtx)
	session := failurecause.NewEventSession()
	delivered := false
	for {
		select {
		case <-b.stopCtx.Done():
			return delivered
		case event, ok := <-eventCh:
			if !ok {
				return delivered
			}
			delivered = true
			switch event.Action {
			case containerEventStart:
				session.ObserveStart(event.ContainerID)
			case containerEventKill:
				session.ObserveSignal(event.ContainerID)
			case containerEventDie:
				// Consume the run's record first, so a death of an untracked
				// container still frees its entry.
				b.enqueueLiveContainerDeath(deaths, liveContainerDeath{
					containerID: event.ContainerID,
					provenance:  session.ObserveExit(event.ContainerID),
				})
			}
		case err, ok := <-errCh:
			if !ok {
				return delivered
			}
			b.logger.Warn("container event stream error, reconnecting", "error", err)
			return delivered
		}
	}
}

// enqueueLiveContainerDeath hands one death to the dispatcher without ever
// blocking the reader. A full queue drops the dispatch, toward unknown.
func (b *Backend) enqueueLiveContainerDeath(deaths chan<- liveContainerDeath, death liveContainerDeath) {
	select {
	case deaths <- death:
	default:
		dieEventDroppedTotal.WithLabelValues(dieEventSourceEventLoop).Inc()
		b.logger.Warn("container death dropped: dispatch queue full; the reconcile sweep will find it, unattributed",
			"container_id", leasesm.ShortID(death.containerID))
	}
}

// dispatchLiveContainerDeaths routes queued deaths in arrival order until the
// reader closes the queue.
func (b *Backend) dispatchLiveContainerDeaths(deaths <-chan liveContainerDeath) {
	for death := range deaths {
		b.dispatchLiveContainerDeath(death)
	}
}

func (b *Backend) dispatchLiveContainerDeath(death liveContainerDeath) {
	if b.stopCtx.Err() != nil {
		return
	}
	if err := b.verifyContainerEventAuthority(); err != nil {
		dieEventDroppedTotal.WithLabelValues(dieEventSourceEventLoop).Inc()
		b.logger.Error("container death dropped: backend storage identity unverified; the reconcile sweep will find it, unattributed",
			"container_id", leasesm.ShortID(death.containerID), "error", err)
		return
	}
	leaseUUID, found := b.findLeaseByContainerID(death.containerID)
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
	observation, err := leasesm.NewLiveContainerDiedObservation(death.containerID, generation, death.provenance)
	if err != nil {
		b.logger.Error("invalid container event ignored", "error", err)
		return
	}
	b.dispatchContainerDeathObservation(observation, death.containerID, dieEventSourceEventLoop)
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
