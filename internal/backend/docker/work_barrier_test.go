package docker

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWorkBarrier_ZeroAtStart: a fresh barrier has count=0, so Zero()
// must return an already-closed channel so callers don't block.
func TestWorkBarrier_ZeroAtStart(t *testing.T) {
	b := newWorkBarrier()
	select {
	case <-b.Zero():
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Zero() on a fresh barrier must be already closed")
	}
}

// TestWorkBarrier_BlocksUntilDone: Add() installs an open zeroCh;
// Zero() blocks; Done() closes it.
func TestWorkBarrier_BlocksUntilDone(t *testing.T) {
	b := newWorkBarrier()
	b.Add()

	ch := b.Zero()
	select {
	case <-ch:
		t.Fatal("Zero() must block while count > 0")
	case <-time.After(20 * time.Millisecond):
	}

	b.Done()
	select {
	case <-ch:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Zero() must close when count drops to 0")
	}
}

// TestWorkBarrier_MultipleAddDone: only the final Done (count→0)
// closes the channel.
func TestWorkBarrier_MultipleAddDone(t *testing.T) {
	b := newWorkBarrier()
	b.Add()
	b.Add()
	b.Add()

	ch := b.Zero()
	b.Done()
	b.Done()
	select {
	case <-ch:
		t.Fatal("Zero() must not close until count reaches 0")
	case <-time.After(20 * time.Millisecond):
	}

	b.Done()
	select {
	case <-ch:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Zero() must close on the final Done")
	}
}

// TestWorkBarrier_ResetAfterDone: after a 1→0→1 cycle, the barrier
// must install a FRESH zeroCh on the 0→1 transition. A caller that
// got the prior (closed) zeroCh sees it closed — but a new Zero()
// call returns the fresh un-closed channel.
func TestWorkBarrier_ResetAfterDone(t *testing.T) {
	b := newWorkBarrier()
	b.Add()
	stale := b.Zero()
	b.Done()

	// Stale handle observes the N→0 close.
	select {
	case <-stale:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("stale Zero() handle must be closed after Done")
	}

	// Second cycle: Add re-arms the barrier.
	b.Add()
	fresh := b.Zero()

	// Stale handle is still closed (reading a closed channel is safe).
	select {
	case <-stale:
	default:
		t.Fatal("stale handle must remain closed — Add() must not un-close it")
	}

	// Fresh handle blocks until the second Done.
	select {
	case <-fresh:
		t.Fatal("fresh Zero() must block while the new worker is in flight")
	case <-time.After(20 * time.Millisecond):
	}

	b.Done()
	select {
	case <-fresh:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("fresh Zero() must close on the second Done")
	}
}

// TestWorkBarrier_DonePanicsOnZero: Done with count=0 catches
// mismatched Add/Done pairs (mirrors sync.WaitGroup semantics).
func TestWorkBarrier_DonePanicsOnZero(t *testing.T) {
	b := newWorkBarrier()
	assert.Panics(t, func() { b.Done() },
		"Done() with count=0 must panic")
}

// TestWorkBarrier_ConcurrentAddDone: exercise many concurrent
// Add/Done pairs. Must pass -race. Final Zero() resolves after all
// goroutines Done.
func TestWorkBarrier_ConcurrentAddDone(t *testing.T) {
	b := newWorkBarrier()
	const N = 1000

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		b.Add()
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i%3) * time.Microsecond)
			b.Done()
		}()
	}

	// All goroutines should complete AND the barrier should zero.
	wg.Wait()
	select {
	case <-b.Zero():
	case <-time.After(1 * time.Second):
		t.Fatal("Zero() must close after N concurrent Add/Done pairs complete")
	}
}

// TestWorkBarrier_NoGoroutineLeak is the critical test: verify that
// many wait-with-timeout cycles never increase the runtime goroutine
// count. This is the whole reason workBarrier exists. If this test
// regresses, the primitive no longer solves the problem.
func TestWorkBarrier_NoGoroutineLeak(t *testing.T) {
	// Baseline goroutine count.
	runtime.GC()
	time.Sleep(20 * time.Millisecond) // let any stragglers exit
	before := runtime.NumGoroutine()

	const cycles = 200
	for i := 0; i < cycles; i++ {
		b := newWorkBarrier()
		b.Add()
		// Simulate a "wedged" worker: never Done. Select with a short
		// timeout and bail. A sync.WaitGroup-based pattern would leak
		// a waiter goroutine here every iteration.
		select {
		case <-b.Zero():
			t.Fatal("Zero() must block while count > 0")
		case <-time.After(1 * time.Millisecond):
		}
		// Intentionally leave b.count at 1 — we're simulating the
		// pathological "worker never exits" case. The barrier itself
		// must not have spawned anything.
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow +/-2 goroutines for runtime bookkeeping jitter (GC, etc.).
	// The critical property: we did NOT grow by ~`cycles`.
	delta := after - before
	assert.LessOrEqualf(t, delta, 2,
		"workBarrier must not spawn goroutines: delta=%d after %d wait cycles (before=%d, after=%d)",
		delta, cycles, before, after)
}

// TestWorkBarrier_MultipleWaitersOnSameZero: two concurrent waiters
// selecting on the same zeroCh both unblock on the final Done.
func TestWorkBarrier_MultipleWaitersOnSameZero(t *testing.T) {
	b := newWorkBarrier()
	b.Add()

	var unblocked atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-b.Zero()
			unblocked.Add(1)
		}()
	}

	// All waiters blocked.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(0), unblocked.Load(),
		"waiters must block while count > 0")

	b.Done()
	wg.Wait()
	assert.Equal(t, int32(5), unblocked.Load(),
		"every waiter on the same zeroCh must unblock on Done")
}

// TestWorkBarrier_SelectWithTimeout is the usage-site test: demonstrate
// the exact pattern waitForWorkers uses and verify both arms work
// without leaking.
func TestWorkBarrier_SelectWithTimeout(t *testing.T) {
	t.Run("zero arm fires when workers finish", func(t *testing.T) {
		b := newWorkBarrier()
		b.Add()
		go func() {
			time.Sleep(10 * time.Millisecond)
			b.Done()
		}()
		select {
		case <-b.Zero():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout arm fired when workers should have finished")
		}
	})

	t.Run("timeout arm fires when worker never exits", func(t *testing.T) {
		b := newWorkBarrier()
		b.Add()
		// No Done() — simulate wedged worker.
		select {
		case <-b.Zero():
			t.Fatal("zero arm fired when worker was never Done")
		case <-time.After(10 * time.Millisecond):
		}
		// Verify we can still make progress: a real worker eventually
		// completes and barrier becomes usable again.
		b.Done()
		select {
		case <-b.Zero():
		case <-time.After(50 * time.Millisecond):
			t.Fatal("Zero() must close once the delinquent worker finishes")
		}
	})
}

// TestWorkBarrier_ZeroSnapshotSurvivesAdd: a caller captures Zero()
// while count=0 (closed). A subsequent Add must not affect that
// caller's snapshot — reading a closed channel remains non-blocking.
// A fresh Zero() call sees the new (un-closed) channel.
func TestWorkBarrier_ZeroSnapshotSurvivesAdd(t *testing.T) {
	b := newWorkBarrier()

	snapshot := b.Zero()
	select {
	case <-snapshot:
	default:
		t.Fatal("initial snapshot must be closed (count=0)")
	}

	b.Add()

	// Snapshot is still closed — the zeroCh object is immutable once closed.
	select {
	case <-snapshot:
	default:
		t.Fatal("captured snapshot must remain closed after Add")
	}

	// Fresh Zero() reflects the new state.
	fresh := b.Zero()
	select {
	case <-fresh:
		t.Fatal("fresh Zero() after Add must not be closed")
	default:
	}

	b.Done()

	// Fresh channel closes.
	select {
	case <-fresh:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("fresh Zero() must close on Done")
	}
}

// TestWorkBarrier_UnderRaceDetector: combined stress pass meant to
// exercise mu under the race detector. Non-deterministic interleavings
// of Add/Done/Zero under concurrent callers.
func TestWorkBarrier_UnderRaceDetector(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	b := newWorkBarrier()
	const workers = 50
	const reps = 200

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reps; j++ {
				b.Add()
				go func() { b.Done() }()
				select {
				case <-b.Zero():
				case <-time.After(100 * time.Millisecond):
				}
			}
		}()
	}

	wg.Wait()
	// Drain any in-flight Done goroutines the test launched.
	require.Eventually(t, func() bool {
		select {
		case <-b.Zero():
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond,
		"barrier must eventually settle to zero after all workers finish")
}
