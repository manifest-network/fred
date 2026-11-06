package shared

import (
	"context"
	"sync"
)

type commandFenceEntry struct {
	// held has capacity one: a send acquires the fence, a receive releases it.
	// A channel rather than a mutex lets a bounded caller stop waiting.
	held chan struct{}
	refs int
}

// CommandFence serializes admission, reconciliation, and teardown for one
// lease. It is a zero-value-ready, ref-counted keyed mutex registry: unrelated
// leases never block one another, and an entry is removed after its last holder
// or waiter releases it. It deliberately carries no lifecycle state; durable
// intent/outbox records remain the crash-recovery authority.
//
// CommandFence must not be copied after first use, like sync.Mutex.
type CommandFence struct {
	mu      sync.Mutex
	entries map[string]*commandFenceEntry
}

// Lock acquires the mutex for leaseUUID and returns an idempotent unlock
// function. Refcounts include both the current holder and queued waiters, which
// prevents an entry from being removed while another goroutine can still acquire
// its mutex.
func (f *CommandFence) Lock(leaseUUID string) func() {
	entry := f.reference(leaseUUID)
	entry.held <- struct{}{}
	return f.unlockFunc(leaseUUID, entry)
}

// LockContext is Lock for a caller whose wait is bounded. When ctx ends first
// it holds nothing and returns ctx's error.
func (f *CommandFence) LockContext(ctx context.Context, leaseUUID string) (func(), error) {
	entry := f.reference(leaseUUID)
	select {
	case entry.held <- struct{}{}:
		return f.unlockFunc(leaseUUID, entry), nil
	case <-ctx.Done():
		f.release(leaseUUID, entry)
		return nil, ctx.Err()
	}
}

// TryLock acquires leaseUUID only when doing so cannot wait. It is intended for
// level-triggered background convergence: a live caller already owning the
// command is authoritative, so recovery should defer that lease without
// stalling unrelated work. The returned unlock function is idempotent.
func (f *CommandFence) TryLock(leaseUUID string) (func(), bool) {
	entry := f.reference(leaseUUID)
	select {
	case entry.held <- struct{}{}:
		return f.unlockFunc(leaseUUID, entry), true
	default:
		f.release(leaseUUID, entry)
		return nil, false
	}
}

// reference counts one more holder or waiter of leaseUUID's entry.
func (f *CommandFence) reference(leaseUUID string) *commandFenceEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.entries == nil {
		f.entries = make(map[string]*commandFenceEntry)
	}
	entry := f.entries[leaseUUID]
	if entry == nil {
		entry = &commandFenceEntry{held: make(chan struct{}, 1)}
		f.entries[leaseUUID] = entry
	}
	entry.refs++
	return entry
}

// release drops one reference and removes the entry after its last one.
func (f *CommandFence) release(leaseUUID string, entry *commandFenceEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && f.entries[leaseUUID] == entry {
		delete(f.entries, leaseUUID)
	}
}

func (f *CommandFence) unlockFunc(leaseUUID string, entry *commandFenceEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			<-entry.held
			f.release(leaseUUID, entry)
		})
	}
}
