package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics/background"
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
	assert.Equal(t, float64(containerDeathQueueCapacity), testutil.ToFloat64(containerDeathQueueDepth),
		"the depth gauge shows the backlog of a stuck dispatcher before and while deaths are dropped")

	stuck.Store(false)
	releaseOnce()
	b.stopCancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the loop did not stop")
	}
	assert.Zero(t, testutil.ToFloat64(containerDeathQueueDepth),
		"an exited loop leaves no stale depth behind (it read the full capacity above)")
}

// stalledLogSink is a log handler that, once stalled, holds every record until
// released: production logs synchronously to stdout, and a consumer of that
// output that falls behind holds every logging caller the same way.
type stalledLogSink struct {
	stalled  atomic.Bool
	blocked  chan struct{} // signaled when a record is held
	released chan struct{}
	release  func()

	mu      sync.Mutex
	records []slog.Record
}

func newStalledLogSink() *stalledLogSink {
	sink := &stalledLogSink{blocked: make(chan struct{}, 1), released: make(chan struct{})}
	sink.release = sync.OnceFunc(func() { close(sink.released) })
	return sink
}

func (s *stalledLogSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *stalledLogSink) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *stalledLogSink) WithGroup(string) slog.Handler            { return s }

func (s *stalledLogSink) Handle(_ context.Context, record slog.Record) error {
	if s.stalled.Load() {
		select {
		case s.blocked <- struct{}{}:
		default:
		}
		<-s.released
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record.Clone())
	return nil
}

// overflowReports returns the dropped count of every overflow summary logged.
func (s *stalledLogSink) overflowReports() []uint64 {
	return s.droppedCounts(containerDeathOverflowMessage)
}

// droppedCounts returns the "dropped" attribute of every record logged with
// message, in order.
func (s *stalledLogSink) droppedCounts(message string) []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var counts []uint64
	for _, record := range s.records {
		if record.Message != message {
			continue
		}
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "dropped" {
				counts = append(counts, attr.Value.Uint64())
			}
			return true
		})
	}
	return counts
}

// panickingLogSink panics on as many records as panics holds, then forwards
// the rest to next.
type panickingLogSink struct {
	next   slog.Handler
	panics atomic.Int32
}

func (h *panickingLogSink) Enabled(context.Context, slog.Level) bool { return true }
func (h *panickingLogSink) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *panickingLogSink) WithGroup(string) slog.Handler            { return h }

func (h *panickingLogSink) Handle(ctx context.Context, record slog.Record) error {
	if h.panics.Add(-1) >= 0 {
		panic("log sink failed")
	}
	return h.next.Handle(ctx, record)
}

// The reporter logs the first drop at once and sums later ones into at most
// one line per interval; what is left when the loop stops is logged then.
// Every drop is reported exactly once.
func TestContainerDeathOverflowReporter_SummarizesAtMostOncePerInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newStalledLogSink()
		b := &Backend{logger: slog.New(sink)}
		overflow := newContainerDeathOverflow()
		ctx, cancel := context.WithCancel(context.Background())
		drops := eventLoopDrops()
		done := make(chan struct{})
		go func() {
			defer close(done)
			b.reportContainerDeathOverflow(ctx, overflow, containerDeathOverflowReportInterval)
		}()
		synctest.Wait()
		assert.Empty(t, sink.overflowReports(), "nothing dropped, nothing logged")

		overflow.record()
		synctest.Wait()
		assert.Equal(t, []uint64{1}, sink.overflowReports(), "the first drop is logged at once")

		for range 3 {
			overflow.record()
		}
		synctest.Wait()
		assert.Equal(t, []uint64{1}, sink.overflowReports(), "later drops wait for the interval")
		time.Sleep(containerDeathOverflowReportInterval)
		synctest.Wait()
		assert.Equal(t, []uint64{1, 3}, sink.overflowReports(), "and are summed into one line")

		time.Sleep(containerDeathOverflowReportInterval)
		synctest.Wait()
		assert.Equal(t, []uint64{1, 3}, sink.overflowReports(), "a quiet interval logs nothing")
		overflow.record()
		synctest.Wait()
		assert.Equal(t, []uint64{1, 3, 1}, sink.overflowReports(), "after a quiet interval a drop is logged at once")

		overflow.record()
		overflow.record()
		cancel()
		<-done
		assert.Equal(t, []uint64{1, 3, 1, 2}, sink.overflowReports(), "drops left when the loop stops are logged then")
		assert.Equal(t, 7.0, eventLoopDrops()-drops, "the reader's counter saw every drop")
	})
}

// The log handler is foreign code. A panic in it, even one that repeats while
// the reporter reports it, is contained and counted, and the reporter goes on
// reporting later drops.
func TestContainerDeathOverflowReporter_ContainsLogHandlerPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := newStalledLogSink()
		handler := &panickingLogSink{next: sink}
		b := &Backend{logger: slog.New(handler)}
		overflow := newContainerDeathOverflow()
		ctx, cancel := context.WithCancel(context.Background())
		panics := background.GoroutinePanicsTotal.WithLabelValues(containerDeathOverflowReporter)
		before := testutil.ToFloat64(panics)
		done := make(chan struct{})
		go func() {
			defer close(done)
			b.reportContainerDeathOverflow(ctx, overflow, containerDeathOverflowReportInterval)
		}()

		// The summary panics, and so does the report of its panic.
		handler.panics.Store(2)
		overflow.record()
		synctest.Wait()
		assert.Equal(t, before+1, testutil.ToFloat64(panics), "the panic is counted")
		assert.Empty(t, sink.droppedCounts("container death overflow report panicked"))

		// Only the summary panics: its panic is reported.
		handler.panics.Store(1)
		overflow.record()
		time.Sleep(containerDeathOverflowReportInterval)
		synctest.Wait()
		assert.Equal(t, before+2, testutil.ToFloat64(panics))
		assert.Equal(t, []uint64{1}, sink.droppedCounts("container death overflow report panicked"))

		// The reporter still runs and reports the next drop.
		overflow.record()
		time.Sleep(containerDeathOverflowReportInterval)
		synctest.Wait()
		assert.Equal(t, []uint64{1}, sink.overflowReports())
		assert.Equal(t, before+2, testutil.ToFloat64(panics))

		cancel()
		<-done
	})
}

// A stalled log sink cannot stall the reader (ENG-799 review). If the reader
// logged the death a full queue drops, output backpressure would hold it;
// dockerd skips events meanwhile, and a skipped "kill" would make the
// following "die" count against the tenant's terminal budget. With the queue
// full and the sink holding the overflow's report, the reader still takes the
// next kill at once.
func TestContainerEventLoop_StalledLogSinkNeverStallsTheReader(t *testing.T) {
	events := make(chan ContainerEvent)
	b := newBackendForTest(&mockDockerClient{
		ContainerEventsFn: func(context.Context) (<-chan ContainerEvent, <-chan error) {
			return events, make(chan error)
		},
	}, nil)
	sink := newStalledLogSink()
	b.logger = slog.New(sink)
	verifying := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseDispatcher := sync.OnceFunc(func() { close(release) })
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
	t.Cleanup(func() {
		sink.release()
		stuck.Store(false)
		releaseDispatcher()
		b.stopCancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the loop did not stop")
		}
	})
	send := func(event ContainerEvent) {
		t.Helper()
		select {
		case events <- event:
		case <-time.After(5 * time.Second):
			t.Fatalf("the reader stopped taking events at %+v", event)
		}
	}

	// Hold the dispatcher on one death, then fill the queue behind it.
	send(ContainerEvent{ContainerID: "warmup", Action: containerEventStart})
	stuck.Store(true)
	send(ContainerEvent{ContainerID: "held", Action: containerEventDie})
	select {
	case <-verifying:
	case <-time.After(5 * time.Second):
		t.Fatal("the dispatcher never picked up the death")
	}
	for index := range containerDeathQueueCapacity {
		send(ContainerEvent{ContainerID: fmt.Sprintf("queued-%d", index), Action: containerEventDie})
	}
	send(ContainerEvent{ContainerID: "victim", Action: containerEventStart})

	// Stall the sink, then overflow the queue: the drop's report is held.
	sink.stalled.Store(true)
	drops := eventLoopDrops()
	send(ContainerEvent{ContainerID: "overflow", Action: containerEventDie})
	select {
	case <-sink.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the dropped death was never reported")
	}

	// The reader takes the next kill while the report is held, and finishes
	// handling it: it takes one event at a time, so the sentinel proves it.
	send(ContainerEvent{ContainerID: "victim", Action: containerEventKill})
	send(ContainerEvent{ContainerID: "sentinel", Action: containerEventStart})
	assert.Equal(t, 1.0, eventLoopDrops()-drops, "the overflow is counted while its report is held")
	assert.Empty(t, sink.overflowReports(), "the report is still held by the stalled sink")

	sink.release()
	require.Eventually(t, func() bool { return len(sink.overflowReports()) == 1 },
		5*time.Second, time.Millisecond, "the held report completes once the sink drains")
	assert.Equal(t, []uint64{1}, sink.overflowReports())
}

func TestContainerEventStreamOutcomesArePreinitialized(t *testing.T) {
	assert.Equal(t, len(containerEventStreamOutcomes), testutil.CollectAndCount(containerEventStreamTotal),
		"exactly the closed outcome set is exported, before any stream event")
	assert.ElementsMatch(t, []string{"connected", "reconnect", "exited"}, containerEventStreamOutcomes)
}
