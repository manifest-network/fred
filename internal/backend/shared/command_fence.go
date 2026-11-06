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

// CommandFenceHold is exclusive ownership of one lease's command fence. Only a
// CommandFence mints one. Its copies share that ownership, so it can move
// into a command that releases it later: the first Release gives the fence
// back, and later ones, through any copy, do nothing. The zero value holds
// nothing.
type CommandFenceHold struct{ held *commandFenceHeld }

type commandFenceHeld struct {
	fence     *CommandFence
	leaseUUID string
	entry     *commandFenceEntry
	once      sync.Once
}

// LeaseUUID names the lease whose fence this is, or "" for the zero value.
func (hold CommandFenceHold) LeaseUUID() string {
	if hold.held == nil {
		return ""
	}
	return hold.held.leaseUUID
}

// Release gives the fence back the first time any copy calls it.
func (hold CommandFenceHold) Release() {
	if hold.held == nil {
		return
	}
	hold.held.once.Do(func() {
		<-hold.held.entry.held
		hold.held.fence.release(hold.held.leaseUUID, hold.held.entry)
	})
}

// Lock acquires the fence for leaseUUID. Refcounts include both the current
// holder and queued waiters, which prevents an entry from being removed while
// another goroutine can still acquire it.
func (f *CommandFence) Lock(leaseUUID string) CommandFenceHold {
	entry := f.reference(leaseUUID)
	entry.held <- struct{}{}
	return f.hold(leaseUUID, entry)
}

// LockContext is Lock for a caller whose wait is bounded. When ctx ends first
// it holds nothing and returns ctx's error.
func (f *CommandFence) LockContext(ctx context.Context, leaseUUID string) (CommandFenceHold, error) {
	entry := f.reference(leaseUUID)
	select {
	case entry.held <- struct{}{}:
		return f.hold(leaseUUID, entry), nil
	case <-ctx.Done():
		f.release(leaseUUID, entry)
		return CommandFenceHold{}, ctx.Err()
	}
}

// TryLock acquires leaseUUID only when doing so cannot wait. It is intended for
// level-triggered background convergence: a live caller already owning the
// command is authoritative, so recovery should defer that lease without
// stalling unrelated work.
func (f *CommandFence) TryLock(leaseUUID string) (CommandFenceHold, bool) {
	entry := f.reference(leaseUUID)
	select {
	case entry.held <- struct{}{}:
		return f.hold(leaseUUID, entry), true
	default:
		f.release(leaseUUID, entry)
		return CommandFenceHold{}, false
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

func (f *CommandFence) hold(leaseUUID string, entry *commandFenceEntry) CommandFenceHold {
	return CommandFenceHold{held: &commandFenceHeld{fence: f, leaseUUID: leaseUUID, entry: entry}}
}
