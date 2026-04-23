package docker

import "sync"

// workBarrier is a reference-counted barrier that exposes a channel
// which closes whenever the count reaches zero. It exists so callers
// can implement "wait for all workers to finish, with a timeout" using
// a plain `select` — without spawning a helper goroutine that would
// leak if the count never drops to zero.
//
// This is the reason it exists instead of sync.WaitGroup: the idiomatic
// "wg.Wait with timeout" pattern requires spawning a goroutine that
// calls wg.Wait() and closes a channel when it returns, then racing
// that channel against a timer. If the timer wins, the waiter goroutine
// is stuck on wg.Wait() forever (it only exits once the count reaches
// zero). In the actor model here, `waitForWorkers` is called on every
// SM transition out of a work-owning state AND on actor exit — so the
// pathological case (wedged worker) compounds into multiple leaked
// waiter goroutines per wedged worker. workBarrier closes that gap by
// making the "count reached zero" signal a real channel that callers
// can select on directly.
//
// # Invariants
//
//  1. count >= 0 at all times.
//  2. zeroCh is always non-nil (initialized by newWorkBarrier).
//  3. A Zero() call returns a channel `ch` such that `ch` is closed
//     exactly when count has transitioned to zero via Done — or was
//     zero at the time Zero() was called.
//  4. On every 0→1 transition (via Add), a fresh zeroCh is installed.
//     Any caller that captured the previous zeroCh (which is now closed)
//     still observes it as closed, which is correct: their snapshot of
//     "barrier is at zero" was accurate at the moment of their Zero()
//     call.
//
// # Usage
//
// Mirrors sync.WaitGroup: Add before spawning a goroutine, Done from
// the goroutine (typically in a defer). Wait by selecting on Zero():
//
//	b.Add()
//	go func() {
//	    defer b.Done()
//	    // work...
//	}()
//
//	select {
//	case <-b.Zero():
//	    // all workers finished
//	case <-time.After(timeout):
//	    // gave up waiting — no goroutine spawned by workBarrier
//	}
//
// # Correctness argument
//
// All mutations (count, zeroCh) happen under `mu`. Readers of zeroCh
// via Zero() take a snapshot of the current channel under `mu`. Two
// cases:
//
//   - If count == 0 when Zero() runs, the snapshot is a channel that
//     was closed by the most recent N→0 Done (or by newWorkBarrier).
//     The caller immediately unblocks.
//   - If count > 0 when Zero() runs, the snapshot is a channel that
//     will be closed by the next N→0 Done. The caller blocks until
//     that close — or the caller gives up via the select's other arms.
//
// A later Add that transitions 0→1 installs a NEW zeroCh; the snapshot
// the caller already holds is unaffected. That's what we want: once
// Zero() observes "count is 0", the caller should be able to proceed
// as if the barrier were at zero, regardless of subsequent Adds.
//
// Done panics if called when count is already 0: this catches the
// caller-side bug of mismatched Add/Done pairs, mirroring
// sync.WaitGroup semantics.
type workBarrier struct {
	mu     sync.Mutex
	count  int
	zeroCh chan struct{}
}

func newWorkBarrier() *workBarrier {
	ch := make(chan struct{})
	close(ch)
	return &workBarrier{zeroCh: ch}
}

func (b *workBarrier) Add() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.count == 0 {
		b.zeroCh = make(chan struct{})
	}
	b.count++
}

func (b *workBarrier) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.count == 0 {
		panic("workBarrier: Done called with count 0")
	}
	b.count--
	if b.count == 0 {
		close(b.zeroCh)
	}
}

// Zero returns a channel that is (or will be) closed when the count
// reaches zero. The channel snapshot is valid for exactly one wait
// cycle — callers that wait again after a subsequent 0→1 transition
// must call Zero() again to get the fresh channel.
func (b *workBarrier) Zero() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.zeroCh
}
