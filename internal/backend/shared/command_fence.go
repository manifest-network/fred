package shared

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
// CommandFence mints one, and only Transfer moves it on. A hold's copies share
// one ownership, so Release through any of them gives the fence back once and
// later calls do nothing. Transfer moves ownership into a new hold and leaves
// every copy of the old one powerless: none can release the fence the new hold
// owns, and none can transfer it again. The zero value owns nothing.
type CommandFenceHold struct{ owner *commandFenceOwner }

// commandFenceOwner is one ownership of a fence. At most one owner of an entry
// is held at a time: Transfer retires the old owner before minting the next.
type commandFenceOwner struct {
	fence     *CommandFence
	leaseUUID string
	entry     *commandFenceEntry
	state     atomic.Uint32
}

const (
	commandFenceOwned uint32 = iota
	commandFenceMoved
	commandFenceReleased
)

// LeaseUUID names the lease whose fence this is, or "" for the zero value.
func (hold CommandFenceHold) LeaseUUID() string {
	if hold.owner == nil {
		return ""
	}
	return hold.owner.leaseUUID
}

// Owns reports whether hold still owns fence's fence for leaseUUID: it was
// minted by fence for that lease, and has been neither released nor moved.
func (hold CommandFenceHold) Owns(fence *CommandFence, leaseUUID string) bool {
	return hold.owner != nil && fence != nil && hold.owner.fence == fence &&
		leaseUUID != "" && hold.owner.leaseUUID == leaseUUID &&
		hold.owner.state.Load() == commandFenceOwned
}

// Transfer moves the ownership into a new hold, which the caller passes on
// without keeping. It fails when hold no longer owns its fence.
func (hold CommandFenceHold) Transfer() (CommandFenceHold, error) {
	if hold.owner == nil || !hold.owner.state.CompareAndSwap(commandFenceOwned, commandFenceMoved) {
		return CommandFenceHold{}, errors.New("command fence hold does not own its fence")
	}
	return hold.owner.fence.hold(hold.owner.leaseUUID, hold.owner.entry), nil
}

// Release gives the fence back if hold still owns it.
func (hold CommandFenceHold) Release() {
	if hold.owner == nil || !hold.owner.state.CompareAndSwap(commandFenceOwned, commandFenceReleased) {
		return
	}
	<-hold.owner.entry.held
	hold.owner.fence.release(hold.owner.leaseUUID, hold.owner.entry)
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
	return CommandFenceHold{owner: &commandFenceOwner{fence: f, leaseUUID: leaseUUID, entry: entry}}
}
