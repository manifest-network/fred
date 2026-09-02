package shared

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	unlockFirst := fence.Lock("lease-a")
	secondAcquired := make(chan struct{})
	releaseSecond := make(chan struct{})
	go func() {
		unlockSecond := fence.Lock("lease-a")
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
	unlockA := fence.Lock("lease-a")
	defer unlockA()

	acquiredB := make(chan func(), 1)
	go func() { acquiredB <- fence.Lock("lease-b") }()
	select {
	case unlockB := <-acquiredB:
		unlockB()
	case <-time.After(time.Second):
		t.Fatal("unrelated lease was blocked by another key")
	}
}

func TestCommandFenceUnlockIsIdempotent(t *testing.T) {
	var fence CommandFence
	unlock := fence.Lock("lease-a")
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
	unlockAgain := fence.Lock("lease-a")
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
				unlock := fence.Lock(key)
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
