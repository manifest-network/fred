package provisioner

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// Lifecycle tests for the ack batcher's ownership (ENG-723).
//
// The batcher used to be started from NewManager() on context.Background(),
// so its lane goroutines were owned by a constructor rather than by a
// lifecycle — the ENG-592 shape. These tests pin the replacement: the
// constructor starts nothing, Manager.Start() launches the lanes on the
// manager's own lifecycle context, and canceling that context is by itself a
// complete stop path (no Stop() required).

// newLifecycleTestManager builds a Manager wired to in-package mocks, with a
// short ack batch interval so a real ack round-trip completes quickly.
func newLifecycleTestManager(t *testing.T) *Manager {
	t.Helper()
	return newLifecycleTestManagerWithPayloadStore(t, nil)
}

// newLifecycleTestManagerWithPayloadStore is newLifecycleTestManager with a
// payload store attached, for the tests that assert Close() shuts it down.
func newLifecycleTestManagerWithPayloadStore(t *testing.T, payloadStore *payload.Store) *Manager {
	t.Helper()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: &mockManagerBackend{name: "test"}, IsDefault: true}},
	})
	require.NoError(t, err)

	m, err := NewManager(ManagerConfig{
		ProviderUUID:     "provider-1",
		CallbackBaseURL:  "http://localhost:8080",
		PayloadStore:     payloadStore,
		AckBatchInterval: 20 * time.Millisecond,
		AckBatchSize:     10,
	}, router, &mockAckChainClient{pendingLeases: []string{"lease-1"}})
	require.NoError(t, err)

	return m
}

// waitGroupSettled reports whether wg reached zero within d. It is used instead
// of a bare wg.Wait() so a goroutine set that never exits fails the assertion
// rather than hanging until the package test timeout.
func waitGroupSettled(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// TestManager_ConstructorStartsNoAckBatcherGoroutines pins that constructing a
// Manager launches nothing. b.cancel is written only by AckBatcher.Start, so a
// nil cancel is the exact statement "Start has not been called".
func TestManager_ConstructorStartsNoAckBatcherGoroutines(t *testing.T) {
	m := newLifecycleTestManager(t)

	require.Nil(t, m.ackBatcher.cancel,
		"NewManager must not start the ack batcher: long-lived goroutines belong to a lifecycle, not to a constructor (ENG-723)")
	require.NotNil(t, m.stopCtx, "NewManager must create the manager's lifecycle context")
	require.NoError(t, m.stopCtx.Err(), "lifecycle context must be live after NewManager")

	// The shutdown steps Close() runs must stay safe when Start() was never
	// called — a path any construct-then-fail sequence in main can take. They
	// are exercised directly rather than through Close(), which would block on
	// Watermill's 30s CloseTimeout for a router that never ran.
	assert.NotPanics(t, func() { m.ackBatcher.Stop() })
	assert.True(t, waitGroupSettled(m.ackBatcher.wg, time.Second),
		"Stop() on a never-started batcher must return immediately")
	assert.NotPanics(t, func() { m.stopCancel() })
	assert.Error(t, m.stopCtx.Err(), "stopCancel must cancel the lifecycle context")
}

// TestManager_CloseCancelsLifecycleContext pins the other half of the
// ownership: whatever Start() launched on m.stopCtx is reclaimed by Close().
func TestManager_CloseCancelsLifecycleContext(t *testing.T) {
	m := newLifecycleTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- m.Start(ctx) }()

	select {
	case <-m.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}

	// Positive control: the lanes really are running.
	acked, _, err := m.AckBatcher().Acknowledge(ctx, "lease-1")
	require.NoError(t, err)
	require.True(t, acked)
	require.NoError(t, m.stopCtx.Err(), "lifecycle context must still be live while running")

	require.NoError(t, m.Close())

	assert.Error(t, m.stopCtx.Err(), "Close() must cancel the lifecycle context")
	assert.True(t, waitGroupSettled(m.ackBatcher.wg, 5*time.Second),
		"Close() must reclaim the ack batcher lanes")

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Error("Manager.Start() did not return after Close() + cancel")
	}
}

// stuckSubscriber is a message.Subscriber whose subscription channel is never
// closed. Watermill's handler body is `for msg := range h.messagesCh`
// (watermill v1.5.1 message/router.go), so the handler goroutine never returns
// and Router.handlersWg never drains. That is the one and only condition under
// which Router.Close() returns an error: it reports "router close timeout" when
// waitForHandlers exceeds RouterConfig.CloseTimeout. (Closing an already-closed
// router cannot error — Close() returns nil early on r.closed — so a double
// close is not a usable failure injection here.)
type stuckSubscriber struct {
	ch chan *message.Message
}

func (s *stuckSubscriber) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	return s.ch, nil
}

func (s *stuckSubscriber) Close() error { return nil }

// TestManager_CloseFinishesShutdownWhenRouterCloseFails is the regression test
// for the early return Close() used to take (ENG-723):
//
//	if err := m.wmRouter.Close(); err != nil { return err }
//
// which skipped the ack batcher stop, the lifecycle-context cancellation and
// the payload store close — every reclaim step Close() exists to perform. The
// step it skipped last is the costliest: Manager.Close() is the only closer of
// the payload store in production (cmd/providerd/main.go builds it with no
// defer of its own), and payload.Store.Close() is what cancels the store's
// context and waits for writerLoop's final drain-and-flush, so an early return
// loses whatever that last flush would have committed. Callers are not
// stranded either way — writerLoop's flush ticker releases each result channel
// while the loop lives, and once the store's context is done a caller returns
// immediately — so the cost is durability, not a blocked goroutine.
//
// Reintroducing that early return keeps assertion (a) passing — it returns the
// router error too — and fails (b), (c) and (d). Verified by mutation, not
// assumed.
func TestManager_CloseFinishesShutdownWhenRouterCloseFails(t *testing.T) {
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	// Close() is idempotent; this only covers an early t.Fatal below.
	t.Cleanup(func() { _ = payloadStore.Close() })

	m := newLifecycleTestManagerWithPayloadStore(t, payloadStore)

	// Swap in a router that cannot close cleanly. NewManager hardcodes
	// message.RouterConfig{}, whose CloseTimeout defaults to 30s, and exposes no
	// knob for it — so the substitution is also what keeps this test at ~0.3s
	// rather than ~30s. Assigning an unexported field from an in-package test
	// adds nothing to production code (cf. manager_test.go's
	// &Manager{leaseEventSink: sink}).
	stuck := &stuckSubscriber{ch: make(chan *message.Message)}
	// Reclaim the wedged handler goroutine when the test ends: closing the
	// channel ends its range loop. watchAllHandlersStopped then observes
	// IsClosed() == true and returns instead of closing the router again.
	t.Cleanup(func() { close(stuck.ch) })

	wedged, err := message.NewRouter(
		message.RouterConfig{CloseTimeout: 300 * time.Millisecond},
		watermill.NopLogger{},
	)
	require.NoError(t, err)
	wedged.AddNoPublisherHandler("wedged", "wedged-topic", stuck, func(*message.Message) error { return nil })
	m.wmRouter = wedged

	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()

	errCh := make(chan error, 1)
	go func() { errCh <- m.Start(startCtx) }()

	select {
	case <-m.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}

	// Positive controls. Without them every post-Close assertion below passes
	// vacuously against a batcher that never ran and a store that was never open.
	acked, _, ackErr := m.AckBatcher().Acknowledge(startCtx, "lease-1")
	require.NoError(t, ackErr, "ack lanes must be live before Close()")
	require.True(t, acked, "ack lanes must be live before Close()")
	require.NoError(t, m.stopCtx.Err(), "lifecycle context must be live before Close()")
	require.NoError(t, m.payloadStore.Healthy(), "payload store must be open before Close()")

	closeErr := m.Close()

	// (a) The router error reaches the caller, joined with the payload store's.
	// Watermill builds it with errors.New at the call site, so a substring is
	// the only handle; go.mod pins v1.5.1, where it is Router.Close()'s only
	// error, so a bump that reworded it would fail here loudly.
	assert.ErrorContains(t, closeErr, "router close timeout",
		"Close() must report the router failure")

	// (b) The lifecycle context is canceled anyway.
	assert.Error(t, m.stopCtx.Err(),
		"a failing router close must not skip the lifecycle-context cancellation (ENG-723)")

	// (c) The ack batcher lanes are stopped anyway — and gone, not merely quiescent.
	assert.True(t, waitGroupSettled(m.ackBatcher.wg, 5*time.Second),
		"a failing router close must not skip the ack batcher shutdown (ENG-723)")
	_, _, ackErr = m.AckBatcher().Acknowledge(context.Background(), "lease-1")
	assert.ErrorIs(t, ackErr, errAckLaneUnavailable,
		"the ack lanes must be gone after Close(), not merely idle")

	// (d) The payload store is closed anyway. bbolt reports "database not open"
	// for a View against a closed DB.
	assert.Error(t, m.payloadStore.Healthy(),
		"a failing router close must not skip the payload store close (ENG-723)")

	cancelStart()
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Error("Manager.Start() did not return after Close() + cancel")
	}
}

// TestManager_AckBatcherExitsOnLifecycleContextCancel is the regression test
// for ENG-723. Before the fix the lanes ran on a context.Background() created
// inside NewManager, so nothing the Manager owned could stop them — only an
// explicit Stop() could. Here the lifecycle context is canceled and NOTHING
// else: Stop() is never called, Close() is never called, and the ctx handed to
// Start() stays alive. The lanes must still exit.
func TestManager_AckBatcherExitsOnLifecycleContextCancel(t *testing.T) {
	m := newLifecycleTestManager(t)

	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()

	errCh := make(chan error, 1)
	go func() { errCh <- m.Start(startCtx) }()

	select {
	case <-m.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not start")
	}

	// Positive control: prove the lanes are actually RUNNING before we cancel.
	// Without this, a batcher that was never started would satisfy the exit
	// assertion below vacuously.
	acked, _, err := m.AckBatcher().Acknowledge(startCtx, "lease-1")
	require.NoError(t, err, "ack batcher must be live once Manager.Start has run")
	require.True(t, acked, "ack batcher must be live once Manager.Start has run")

	// Cancel the lifecycle context only.
	m.stopCancel()

	assert.True(t, waitGroupSettled(m.ackBatcher.wg, 5*time.Second),
		"ack batcher lanes did not exit on lifecycle-context cancellation — they are not owned by the manager's lifecycle (ENG-723)")

	// And the batcher is genuinely gone, not merely quiescent.
	_, _, err = m.AckBatcher().Acknowledge(startCtx, "lease-1")
	assert.Error(t, err, "Acknowledge must fail once the lifecycle context is canceled")

	assert.NoError(t, m.Close())
	cancelStart()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Error("Manager.Start() did not return after Close() + cancel")
	}
}

// TestAckBatcher_LanesExitOnContextCancelWithoutStop pins the contract
// Manager.Start relies on: the ctx passed to Start is a full stop path on its
// own. The pre-existing TestAckBatcher_ContextCancellation calls cancel() AND
// Stop(), so it cannot distinguish the two.
func TestAckBatcher_LanesExitOnContextCancelWithoutStop(t *testing.T) {
	batcher := NewAckBatcher(&mockAckChainClient{pendingLeases: []string{"l1"}}, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 20 * time.Millisecond,
		BatchSize:     10,
		LaneCount:     3,
	})

	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)

	// Positive control: all lanes serve a real request first.
	for range 3 {
		acked, _, err := batcher.Acknowledge(ctx, "l1")
		require.NoError(t, err)
		require.True(t, acked)
	}

	cancel() // deliberately NOT batcher.Stop()

	assert.True(t, waitGroupSettled(batcher.wg, 5*time.Second),
		"lanes must exit on context cancellation alone, without Stop()")
}

// TestAckBatcher_StartIsOnceOnly guards the hazard that moving the start out of
// NewManager introduces. A second Start would overwrite b.cancel (orphaning the
// first lane set) and spawn a second batchLoop per lane holding the same done
// channel — both would run `defer close(done)`, panicking the process with
// "close of closed channel".
func TestAckBatcher_StartIsOnceOnly(t *testing.T) {
	batcher := NewAckBatcher(&mockAckChainClient{pendingLeases: []string{"l1"}}, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 20 * time.Millisecond,
		BatchSize:     10,
		LaneCount:     2,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher.Start(ctx)
	batcher.Start(ctx) // must be a no-op

	acked, _, err := batcher.Acknowledge(ctx, "l1")
	require.NoError(t, err)
	require.True(t, acked)

	stopped := make(chan struct{})
	go func() {
		batcher.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() hung after a repeated Start() — a second lane set was spawned")
	}

	assert.True(t, waitGroupSettled(batcher.wg, time.Second),
		"all lanes must be accounted for after Stop()")
}

// TestAckBatcher_ConcurrentStartStop covers the window this change opened.
// Start writes b.cancel inside startOnce.Do and Stop reads it; sync.Once orders
// a Do only against other Do calls, so a Stop racing a first Start used to be a
// genuine data race. It was unreachable while Start ran inside NewManager (the
// write completed before the *Manager was published), and became reachable the
// moment Start moved to the manager's lifecycle. Stop now enters the Once too,
// which is the edge. Run under -race; without the fix this reports on
// b.cancel. Both orderings must also terminate: Start-then-Stop stops the
// lanes, Stop-then-Start leaves the Once consumed so no lane set is created.
func TestAckBatcher_ConcurrentStartStop(t *testing.T) {
	// Repeat with a fresh batcher each time: the window is narrow, and one
	// scheduling of the pair proves little either way.
	for range 50 {
		batcher := NewAckBatcher(&mockAckChainClient{pendingLeases: []string{"l1"}}, AckBatcherConfig{
			ProviderUUID:  testProviderUUID,
			BatchInterval: 20 * time.Millisecond,
			BatchSize:     10,
			LaneCount:     2,
		})

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			batcher.Start(ctx)
		}()
		go func() {
			defer wg.Done()
			batcher.Stop()
		}()

		require.True(t, waitGroupSettled(&wg, 5*time.Second),
			"a Start racing a Stop must not deadlock")
		// Whichever won, no lane may be left running: a Stop that wins consumes
		// the Once so Start creates nothing, and a Start that wins is stopped.
		// The assertion deliberately does NOT cancel ctx first — ctx is the only
		// other thing that could reap a leaked lane set.
		require.True(t, waitGroupSettled(batcher.wg, 5*time.Second),
			"no lane may outlive a Start/Stop race")

		cancel()
	}
}

// startAckBatcherForTest launches m's ack batcher lanes on the test's context.
// Tests that drive Manager's handlers directly, rather than going through
// Manager.Start, need it: since ENG-723 the lanes belong to Start, so without
// it Acknowledge returns errAckLaneUnavailable immediately (the started guard —
// see TestAckBatcher_AcknowledgeBeforeStartFailsFast) and the callback handler
// wraps that into ErrAcknowledgeFailed instead of taking the success path these
// tests assert. t.Context() is canceled when the test finishes, which stops the
// lanes.
func startAckBatcherForTest(t *testing.T, m *Manager) {
	t.Helper()
	m.ackBatcher.Start(t.Context())
}

// TestAckBatcher_AcknowledgeBeforeStartFailsFast pins that a caller which acks
// before Start does not wedge. Without the guard the request lands in the
// buffered lane channel that no goroutine is draining, and Acknowledge blocks
// on a result that never arrives — forever, for a caller whose context has no
// deadline. The error must be the retryable lane-unavailable one, so Watermill
// redelivers rather than poison-dropping the message (ENG-589).
func TestAckBatcher_AcknowledgeBeforeStartFailsFast(t *testing.T) {
	batcher := NewAckBatcher(&mockAckChainClient{pendingLeases: []string{"l1"}}, AckBatcherConfig{
		ProviderUUID:  testProviderUUID,
		BatchInterval: 20 * time.Millisecond,
		BatchSize:     10,
	})

	done := make(chan error, 1)
	go func() {
		// Deliberately a context with no deadline: the guard, not the caller,
		// has to be what ends this call.
		_, _, err := batcher.Acknowledge(context.Background(), "l1")
		done <- err
	}()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, errAckLaneUnavailable,
			"Acknowledge before Start must fail retryably, not terminally")
	case <-time.After(2 * time.Second):
		t.Fatal("Acknowledge hung against a batcher that was never started")
	}
}
