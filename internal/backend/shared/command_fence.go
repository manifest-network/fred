package shared

import "sync"

type commandFenceEntry struct {
	mu   sync.Mutex
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
	f.mu.Lock()
	if f.entries == nil {
		f.entries = make(map[string]*commandFenceEntry)
	}
	entry := f.entries[leaseUUID]
	if entry == nil {
		entry = &commandFenceEntry{}
		f.entries[leaseUUID] = entry
	}
	entry.refs++
	f.mu.Unlock()

	entry.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Unlock()

			f.mu.Lock()
			entry.refs--
			if entry.refs == 0 && f.entries[leaseUUID] == entry {
				delete(f.entries, leaseUUID)
			}
			f.mu.Unlock()
		})
	}
}
