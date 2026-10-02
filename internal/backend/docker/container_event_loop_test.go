package docker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func eventStreamCount(outcome string) float64 {
	return testutil.ToFloat64(containerEventStreamTotal.WithLabelValues(outcome))
}

func eventLoopDrops() float64 {
	return testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues(dieEventSourceEventLoop))
}

// flakyStorageVerifier fails the next failures verifications with a
// non-terminal error (no identity drift), then passes.
type flakyStorageVerifier struct {
	identity backendidentity.ID
	failures atomic.Int32
	calls    atomic.Int32
}

func (v *flakyStorageVerifier) StorageIdentity() backendidentity.ID { return v.identity }

func (v *flakyStorageVerifier) Verify(context.Context) error {
	v.calls.Add(1)
	if v.failures.Add(-1) >= 0 {
		return errors.New("revalidate Docker daemon identity: connection refused")
	}
	v.failures.Store(-1)
	return nil
}

// The event loop is the only source of counting provenance, so a transient
// storage-verification failure, at connect time or on a death, must not stop
// it for good (ENG-799 review). It backs off, reconnects with a fresh session,
// and a later crash still counts.
func TestContainerEventLoop_SurvivesTransientIdentityErrors(t *testing.T) {
	h := newUnstartedBudgetEventHarness(t)
	verifier := &flakyStorageVerifier{identity: h.b.storageIdentity}
	verifier.failures.Store(2)
	h.b.storageVerifier = verifier
	connected, reconnects, exited := eventStreamCount(containerEventStreamConnected),
		eventStreamCount(containerEventStreamReconnect), eventStreamCount(containerEventStreamExited)
	h.start(func(b *Backend) { b.runContainerEventLoop(time.Millisecond, 4*time.Millisecond) })

	require.Eventually(t, func() bool {
		return eventStreamCount(containerEventStreamConnected)-connected >= 1
	}, 5*time.Second, time.Millisecond, "the loop must connect once verification recovers")
	assert.GreaterOrEqual(t, eventStreamCount(containerEventStreamReconnect)-reconnects, 2.0,
		"each failed verification is a counted retry")

	// A verification failure while dispatching one death drops only that death.
	dropsBefore := eventLoopDrops()
	unknownBefore, tenantBefore := failureCount("unknown"), failureCount("tenant_workload")
	verifier.failures.Store(1)
	h.setContainer("exited", 1, false)
	h.send(containerEventStart, containerEventDie)
	require.Eventually(t, func() bool { return eventLoopDrops()-dropsBefore == 1 },
		5*time.Second, time.Millisecond, "the undispatchable death is counted")
	assert.Equal(t, backend.ProvisionStatusReady, h.status(), "the dropped death never reached the actor")

	// The same loop and session still attribute the next observed run.
	h.send(containerEventStart, containerEventDie)
	h.awaitStatus(backend.ProvisionStatusFailed)
	assert.Equal(t, 1.0, failureCount("tenant_workload")-tenantBefore)
	assert.Equal(t, 0.0, failureCount("unknown")-unknownBefore)
	h.requireWire(1, backend.TerminalVerdictRetry)
	assert.Equal(t, exited, eventStreamCount(containerEventStreamExited), "the loop is still running")

	h.b.stopCancel()
	<-h.loopDone
	assert.Equal(t, 1.0, eventStreamCount(containerEventStreamExited)-exited)
}

// Only shutdown or withdrawn storage authority stops the loop.
func TestContainerEventLoop_StopsOnLatchedStorageAuthority(t *testing.T) {
	var subscriptions atomic.Int32
	b := newBackendForTest(&mockDockerClient{
		ContainerEventsFn: func(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
			subscriptions.Add(1)
			events, errs := make(chan ContainerEvent), make(chan error)
			go func() {
				<-ctx.Done()
				close(events)
			}()
			return events, errs
		},
	}, nil)
	t.Cleanup(b.stopCancel)
	exited := eventStreamCount(containerEventStreamExited)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runContainerEventLoop(time.Millisecond, 4*time.Millisecond)
	}()
	require.Eventually(t, func() bool { return subscriptions.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.Error(t, b.latchTerminalStorageAuthority(backendidentity.ErrIdentityDrift))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop kept running after storage authority was withdrawn")
	}
	assert.Equal(t, 1.0, eventStreamCount(containerEventStreamExited)-exited)
	assert.Equal(t, int32(1), subscriptions.Load(), "no resubscription after the latch")
}

// The reader never waits on a death (ENG-799 review): dockerd skips events
// for a subscriber that falls behind, and a skipped kill would let the next
// die count. With the dispatcher stuck on one death, the reader keeps taking
// every event from an unbuffered stream; past the queue's capacity a death is
// dropped and counted rather than blocking it.
func TestContainerEventLoop_ReaderNeverWaitsOnDeathDispatch(t *testing.T) {
	events := make(chan ContainerEvent)
	b := newBackendForTest(&mockDockerClient{
		ContainerEventsFn: func(context.Context) (<-chan ContainerEvent, <-chan error) {
			return events, make(chan error)
		},
	}, nil)
	t.Cleanup(b.stopCancel)
	verifying := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	// stuck holds the dispatcher in its verification until release, ignoring
	// the verification deadline so a slow run cannot drain the queue early.
	var stuck atomic.Bool
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		identity: func() backendidentity.ID { return b.storageIdentity },
		verify: func(context.Context) error {
			if stuck.Load() {
				select {
				case verifying <- struct{}{}:
				default:
				}
				<-release
			}
			return nil
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runContainerEventLoop(time.Millisecond, 4*time.Millisecond)
	}()
	send := func(event ContainerEvent) {
		t.Helper()
		select {
		case events <- event:
		case <-time.After(5 * time.Second):
			t.Fatalf("the reader stopped taking events at %+v", event)
		}
	}
	send(ContainerEvent{ContainerID: "warmup", Action: containerEventStart})
	stuck.Store(true)
	send(ContainerEvent{ContainerID: "c0", Action: containerEventDie})
	select {
	case <-verifying:
	case <-time.After(5 * time.Second):
		t.Fatal("the dispatcher never picked up the death")
	}

	drops := eventLoopDrops()
	for index := range containerDeathQueueCapacity + 10 {
		id := fmt.Sprintf("c%d", index+1)
		send(ContainerEvent{ContainerID: id, Action: containerEventStart})
		send(ContainerEvent{ContainerID: id, Action: containerEventKill})
		send(ContainerEvent{ContainerID: id, Action: containerEventDie})
	}
	// The reader finishes one event before it takes the next, so taking this
	// sentinel proves the last death above was queued or dropped.
	send(ContainerEvent{ContainerID: "sentinel", Action: containerEventStart})
	assert.Equal(t, 10.0, eventLoopDrops()-drops,
		"the queue holds its capacity; every death past it is dropped, counted, and never blocks")

	stuck.Store(false)
	releaseOnce()
	b.stopCancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not stop")
	}
}

func TestContainerEventStreamOutcomesArePreinitialized(t *testing.T) {
	assert.Equal(t, len(containerEventStreamOutcomes), testutil.CollectAndCount(containerEventStreamTotal),
		"exactly the closed outcome set is exported, before any stream event")
	assert.ElementsMatch(t, []string{"connected", "reconnect", "exited"}, containerEventStreamOutcomes)
}
