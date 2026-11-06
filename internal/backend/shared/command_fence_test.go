package shared

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func commandFenceRefs(f *CommandFence, key string) (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	refs := 0
	if entry := f.entries[key]; entry != nil {
		refs = entry.refs
	}
	return refs, len(f.entries)
}

func waitForCommandFenceRefs(t *testing.T, f *CommandFence, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if refs, _ := commandFenceRefs(f, key); refs == want {
			return
		}
		if time.Now().After(deadline) {
			refs, entries := commandFenceRefs(f, key)
			t.Fatalf("command fence refs for %q did not reach %d (refs=%d entries=%d)", key, want, refs, entries)
		}
		runtime.Gosched()
	}
}

func TestCommandFenceSameKeyExcludesAndCleansUp(t *testing.T) {
	var fence CommandFence
	unlockFirst := fence.Lock("lease-a").Release
	secondAcquired := make(chan struct{})
	releaseSecond := make(chan struct{})
	go func() {
		unlockSecond := fence.Lock("lease-a").Release
		close(secondAcquired)
		<-releaseSecond
		unlockSecond()
	}()
	waitForCommandFenceRefs(t, &fence, "lease-a", 2)
	select {
	case <-secondAcquired:
		t.Fatal("same-key waiter acquired before the current holder released")
	default:
	}

	unlockFirst()
	select {
	case <-secondAcquired:
	case <-time.After(time.Second):
		t.Fatal("same-key waiter did not acquire after release")
	}
	waitForCommandFenceRefs(t, &fence, "lease-a", 1)
	close(releaseSecond)
	waitForCommandFenceRefs(t, &fence, "lease-a", 0)
	if _, entries := commandFenceRefs(&fence, "lease-a"); entries != 0 {
		t.Fatalf("idle command-fence entry leaked: %d entries", entries)
	}
}

func TestCommandFenceUnrelatedKeysDoNotBlock(t *testing.T) {
	var fence CommandFence
	unlockA := fence.Lock("lease-a").Release
	defer unlockA()

	acquiredB := make(chan CommandFenceHold, 1)
	go func() { acquiredB <- fence.Lock("lease-b") }()
	select {
	case holdB := <-acquiredB:
		holdB.Release()
	case <-time.After(time.Second):
		t.Fatal("unrelated lease was blocked by another key")
	}
}

func TestCommandFenceTryLockDefersBusyKeyWithoutLeakingReference(t *testing.T) {
	var fence CommandFence
	unlock := fence.Lock("lease-a").Release
	if _, acquired := fence.TryLock("lease-a"); acquired {
		t.Fatal("TryLock acquired a key owned by a live command")
	}
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 1 || entries != 1 {
		t.Fatalf("failed TryLock leaked a reference: refs=%d entries=%d", refs, entries)
	}
	unlock()

	tryHold, acquired := fence.TryLock("lease-a")
	if !acquired {
		t.Fatal("TryLock did not acquire an idle key")
	}
	tryHold.Release()
	tryHold.Release()
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 0 || entries != 0 {
		t.Fatalf("TryLock unlock leaked state: refs=%d entries=%d", refs, entries)
	}
}

func TestCommandFenceLockContextStopsWaitingWithoutLeakingReference(t *testing.T) {
	var fence CommandFence
	unlock := fence.Lock("lease-a").Release
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := fence.LockContext(ctx, "lease-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LockContext on a held key returned %v, want the context's deadline", err)
	}
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 1 || entries != 1 {
		t.Fatalf("abandoned LockContext leaked a reference: refs=%d entries=%d", refs, entries)
	}
	unlock()

	contextHold, err := fence.LockContext(t.Context(), "lease-a")
	if err != nil {
		t.Fatalf("LockContext did not acquire an idle key: %v", err)
	}
	if _, acquired := fence.TryLock("lease-a"); acquired {
		t.Fatal("LockContext did not exclude other holders")
	}
	contextHold.Release()
	contextHold.Release()
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 0 || entries != 0 {
		t.Fatalf("LockContext unlock leaked state: refs=%d entries=%d", refs, entries)
	}
}

// Copies of a hold share one ownership: a copy released after the fence moved
// to a new holder must not release the new holder's fence.
func TestCommandFenceHoldCopiesShareOneRelease(t *testing.T) {
	var fence CommandFence
	first := fence.Lock("lease-a")
	moved := first
	require.Equal(t, "lease-a", moved.LeaseUUID())
	moved.Release()
	second, acquired := fence.TryLock("lease-a")
	require.True(t, acquired, "the first release gives the fence back")
	first.Release()
	_, acquired = fence.TryLock("lease-a")
	require.False(t, acquired, "a stale copy cannot release the next holder's fence")
	second.Release()
	require.Empty(t, CommandFenceHold{}.LeaseUUID())
	CommandFenceHold{}.Release()
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 0 || entries != 0 {
		t.Fatalf("released holds leaked state: refs=%d entries=%d", refs, entries)
	}
}

// Transfer moves ownership: every copy of the old hold loses it at once, and
// only the new hold owns, releases or transfers the fence. Owns binds a hold
// to the exact fence and lease that minted it.
func TestCommandFenceTransferMovesOwnership(t *testing.T) {
	var fence, other CommandFence
	original := fence.Lock("lease-a")
	alias := original
	require.True(t, original.Owns(&fence, "lease-a"))
	require.False(t, original.Owns(&other, "lease-a"), "a hold belongs to the fence that minted it")
	require.False(t, original.Owns(&fence, "lease-b"), "a hold belongs to its lease")
	require.False(t, original.Owns(nil, "lease-a"))

	moved, err := original.Transfer()
	require.NoError(t, err)
	require.True(t, moved.Owns(&fence, "lease-a"))
	require.False(t, alias.Owns(&fence, "lease-a"), "every copy of the moved hold loses ownership")
	alias.Release()
	_, free := fence.TryLock("lease-a")
	require.False(t, free, "a moved hold's copy cannot release the new owner's fence")
	_, err = alias.Transfer()
	require.Error(t, err, "a moved hold cannot be transferred again")

	moved.Release()
	require.False(t, moved.Owns(&fence, "lease-a"))
	_, err = moved.Transfer()
	require.Error(t, err, "a released hold cannot be transferred")
	_, err = CommandFenceHold{}.Transfer()
	require.Error(t, err)
	retaken, free := fence.TryLock("lease-a")
	require.True(t, free)
	retaken.Release()
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 0 || entries != 0 {
		t.Fatalf("transferred holds leaked state: refs=%d entries=%d", refs, entries)
	}
}

func TestCommandFenceUnlockIsIdempotent(t *testing.T) {
	var fence CommandFence
	unlock := fence.Lock("lease-a").Release
	var callers sync.WaitGroup
	for range 16 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			unlock()
		}()
	}
	callers.Wait()
	if refs, entries := commandFenceRefs(&fence, "lease-a"); refs != 0 || entries != 0 {
		t.Fatalf("idempotent unlock leaked state: refs=%d entries=%d", refs, entries)
	}
	// A duplicate unlock must not have unlocked a future generation for the key.
	unlockAgain := fence.Lock("lease-a").Release
	unlock()
	if refs, _ := commandFenceRefs(&fence, "lease-a"); refs != 1 {
		t.Fatalf("stale unlock affected a later lock generation: refs=%d", refs)
	}
	unlockAgain()
}

func TestCommandFenceConcurrentChurn(t *testing.T) {
	var fence CommandFence
	var inside [8]atomic.Int32
	var violated atomic.Bool
	var workers sync.WaitGroup
	for worker := range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := range 200 {
				keyIndex := (worker + iteration) % len(inside)
				key := string(rune('a' + keyIndex))
				unlock := fence.Lock(key).Release
				if inside[keyIndex].Add(1) != 1 {
					violated.Store(true)
				}
				runtime.Gosched()
				inside[keyIndex].Add(-1)
				unlock()
				if iteration%17 == 0 {
					unlock() // exercise idempotence under churn
				}
			}
		}()
	}
	workers.Wait()
	if violated.Load() {
		t.Fatal("two goroutines entered the same key concurrently")
	}
	fence.mu.Lock()
	entries := len(fence.entries)
	fence.mu.Unlock()
	if entries != 0 {
		t.Fatalf("command-fence churn leaked %d idle entries", entries)
	}
}
