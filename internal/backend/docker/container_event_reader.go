package docker

import (
	"sync/atomic"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// The container event loop's reader (see container_event_loop.go) lives in
// this file alone, and the file is a closed world. dockerd skips events for a
// subscriber that falls behind, and a skipped "kill" would make the following
// "die" count against the tenant's terminal budget, so the reader may wait on
// nothing but its own stream: not on recording or dispatching a death, and
// not on log output, which production writes synchronously to stdout
// (ENG-799). It holds no logger and no backend. A death it drops from a full
// queue is counted and handed through containerDeathOverflow to the event
// loop's reporter, which logs it on its own goroutine.
//
// internal/testutil/container_event_reader_guard_test.go keeps it that way:
// this file imports only failurecause and sync/atomic; names nothing else
// from package docker but the event type, its actions and the drop counter
// the reader updates; declares no function or interface type and no function
// literal, so no caller can hand the reader a logger; sends only from a
// select that has a default case; and waits only in consume's select over
// its two streams and its stop signal. Because any file of the package could
// add methods to these types, the guard also rejects a method on them, or on
// the event type, declared anywhere else.

// containerEventReader reads one container event subscription. Its only
// capabilities are the backend's stop signal, the subscription's death queue
// and the overflow record: nothing it can reach logs or waits. The queue has
// one consumer, the event loop's recorder, which writes each death into the
// live-death ledger before it hands it to the dispatcher (ENG-1125).
type containerEventReader struct {
	stop     <-chan struct{}
	deaths   chan<- failurecause.Provenance
	overflow *containerDeathOverflow
}

// consume reads one subscription until it ends or the backend stops. It
// reports whether the subscription delivered any event, and the stream error
// that ended it, if any, for its caller to log once the subscription is gone.
//
// It owns the subscription's event session: one per connection, because
// events missed in a reconnect gap are unknowable. The session is the only
// minter of live provenance and never leaves this frame (its type cannot even
// be named here); internal/testutil confines failurecause.NewEventSession and
// the session's Observe methods to this method.
func (r containerEventReader) consume(events <-chan ContainerEvent, errs <-chan error) (delivered bool, streamErr error) {
	session := failurecause.NewEventSession()
	for {
		select {
		case <-r.stop:
			return delivered, nil
		case event, ok := <-events:
			if !ok {
				return delivered, nil
			}
			delivered = true
			switch event.Action {
			case containerEventStart:
				session.ObserveStart(event.ContainerID)
			case containerEventKill:
				session.ObserveSignal(event.ContainerID)
			case containerEventDie:
				// Consume the run's record first, so a death of an untracked
				// container still frees its entry. The provenance is bound to
				// this container and is all the recorder and the dispatcher
				// need.
				r.enqueue(session.ObserveExit(event.ContainerID))
			}
		case err, ok := <-errs:
			if !ok {
				return delivered, nil
			}
			return delivered, err
		}
	}
}

// enqueue hands one death to the recorder without ever blocking. A full queue
// drops the death, toward unknown: it is neither recorded nor dispatched, so a
// startup failure finds no live provenance for it, and the reconcile sweep
// finds a Ready workload's death later, unattributed. The reader only counts
// the drop.
func (r containerEventReader) enqueue(death failurecause.Provenance) {
	select {
	case r.deaths <- death:
	default:
		r.overflow.record()
	}
}

// containerDeathOverflow carries the deaths the event loop drops from a full
// queue, the reader's and the recorder's, to the reporter that logs them. Its
// recording side, record, is two atomic adds and a send that never waits.
// Wakeups coalesce in a one-slot channel, so a burst of drops wakes the
// reporter once, and it reads the accumulated count.
type containerDeathOverflow struct {
	unreported atomic.Uint64
	wake       chan struct{}
}

func newContainerDeathOverflow() *containerDeathOverflow {
	return &containerDeathOverflow{wake: make(chan struct{}, 1)}
}

// record counts one dropped death and wakes the reporter. It never blocks.
func (o *containerDeathOverflow) record() {
	eventLoopDeathsDropped.Inc()
	o.unreported.Add(1)
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// wakeups delivers the reporter's pending wakeup, if any.
func (o *containerDeathOverflow) wakeups() <-chan struct{} { return o.wake }

// takeUnreported returns the drops recorded since its last call and resets
// the count. A drop recorded after a wakeup was taken is included either in
// this count or in the next, never lost or counted twice.
func (o *containerDeathOverflow) takeUnreported() uint64 { return o.unreported.Swap(0) }
