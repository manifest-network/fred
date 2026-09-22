package docker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// volumeAccessCoordinator owns the namespace and the physical directories used
// by managed launches. Namespace changes take the lease write side; a launch retains
// the lease read side and its exact directory reservations through ContainerStart.
// Different names for one directory therefore cannot create different locks.
//
// Lock order: volume-name stripe (namespace mutations only), startup layout gate,
// sorted lease namespaces, sorted physical identities. A launch never takes a name stripe or waits for a worker
// while holding these reservations. Image admission precedes acquisition;
// filesystem preparation remains inside the reservation.
type volumeAccessCoordinator struct {
	namespaceOnce sync.Once
	namespace     *semaphore.Weighted
	scopes        map[string]*volumeNamespaceEntry
	mu            sync.Mutex
	active        map[fsidentity.Identity]*volumeAccessEntry
}

const volumeNamespaceExclusive = int64(1 << 62)

func (c *volumeAccessCoordinator) namespaceGate() *semaphore.Weighted {
	c.namespaceOnce.Do(func() { c.namespace = semaphore.NewWeighted(volumeNamespaceExclusive) })
	return c.namespace
}

// Ordinary namespace mutations contend only with launches in the same lease.
// The global write side is reserved for startup manager recovery, which can
// remount canonical roots before their identities are available to reserve.
func (c *volumeAccessCoordinator) retainNamespace(ctx context.Context, names []managedVolumeName) (func(), error) {
	return c.acquireNamespace(ctx, names, 1)
}

func (c *volumeAccessCoordinator) mutateNamespace(ctx context.Context, names []managedVolumeName, action func(context.Context) error) error {
	release, err := c.acquireNamespace(ctx, names, volumeNamespaceExclusive)
	if err != nil {
		return err
	}
	defer release()
	return action(ctx)
}

func (c *volumeAccessCoordinator) recoverNamespace(ctx context.Context, action func(context.Context) error) error {
	gate := c.namespaceGate()
	if err := gate.Acquire(ctx, volumeNamespaceExclusive); err != nil {
		return err
	}
	defer gate.Release(volumeNamespaceExclusive)
	return action(ctx)
}

type volumeNamespaceEntry struct {
	gate  *semaphore.Weighted
	users int
}

func (c *volumeAccessCoordinator) acquireNamespace(ctx context.Context, names []managedVolumeName, weight int64) (func(), error) {
	leases := make([]string, 0, len(names))
	for _, name := range names {
		if _, err := parseManagedVolumeName(name.value()); err != nil {
			return nil, err
		}
		leases = append(leases, managedVolumeLeaseUUID(name))
	}
	slices.Sort(leases)
	leases = slices.Compact(leases)
	gate := c.namespaceGate()
	if err := gate.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	var acquired []*volumeNamespaceEntry
	release := sync.OnceFunc(func() {
		for index := len(acquired) - 1; index >= 0; index-- {
			entry := acquired[index]
			entry.gate.Release(weight)
			c.dropNamespaceUser(leases[index], entry)
		}
		gate.Release(1)
	})
	for _, lease := range leases {
		c.mu.Lock()
		if c.scopes == nil {
			c.scopes = make(map[string]*volumeNamespaceEntry)
		}
		entry := c.scopes[lease]
		if entry == nil {
			entry = &volumeNamespaceEntry{gate: semaphore.NewWeighted(volumeNamespaceExclusive)}
			c.scopes[lease] = entry
		}
		entry.users++
		c.mu.Unlock()
		if err := entry.gate.Acquire(ctx, weight); err != nil {
			c.dropNamespaceUser(lease, entry)
			release()
			return nil, err
		}
		acquired = append(acquired, entry)
	}
	return release, nil
}

func (c *volumeAccessCoordinator) dropNamespaceUser(lease string, entry *volumeNamespaceEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.users--
	if entry.users == 0 {
		delete(c.scopes, lease)
	}
}

type volumeAccessEntry struct {
	available chan struct{}
	users     int
}

// reservedVolumeSet is callback-scoped exclusion, not proof that a container
// has stopped. Only the launch boundary can turn it into quiescedVolumes after
// complete writer inventory and positive stop observations.
type reservedVolumeSet struct {
	owner    *volumeAccessCoordinator
	entries  map[fsidentity.Identity]*volumeAccessEntry
	ids      []fsidentity.Identity
	lifetime *volumeReservationLifetime
}

type volumeReservationLifetime struct {
	once     sync.Once
	released atomic.Bool
}

func (c *volumeAccessCoordinator) reserve(ctx context.Context, identities []fsidentity.Identity) (*reservedVolumeSet, error) {
	ids := slices.Clone(identities)
	for _, id := range ids {
		if !id.Valid() {
			return nil, errors.New("volume reservation requires attested directory identities")
		}
	}
	slices.SortFunc(ids, func(a, b fsidentity.Identity) int {
		if a.Device < b.Device || (a.Device == b.Device && a.Inode < b.Inode) {
			return -1
		}
		if a == b {
			return 0
		}
		return 1
	})
	ids = slices.Compact(ids)
	set := &reservedVolumeSet{owner: c, entries: make(map[fsidentity.Identity]*volumeAccessEntry, len(ids)), lifetime: &volumeReservationLifetime{}}
	for _, id := range ids {
		c.mu.Lock()
		if c.active == nil {
			c.active = make(map[fsidentity.Identity]*volumeAccessEntry)
		}
		entry := c.active[id]
		if entry == nil {
			entry = &volumeAccessEntry{available: make(chan struct{}, 1)}
			entry.available <- struct{}{}
			c.active[id] = entry
		}
		entry.users++
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			c.dropUser(id, entry)
			set.release()
			return nil, fmt.Errorf("reserve volume: %w", ctx.Err())
		case <-entry.available:
			set.entries[id] = entry
			set.ids = append(set.ids, id)
		}
	}
	if err := ctx.Err(); err != nil {
		set.release()
		return nil, err
	}
	return set, nil
}

func (c *volumeAccessCoordinator) dropUser(id fsidentity.Identity, entry *volumeAccessEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.users--
	if entry.users == 0 {
		delete(c.active, id)
	}
}

func (set *reservedVolumeSet) release() {
	if set == nil || set.lifetime == nil {
		return
	}
	set.lifetime.once.Do(func() {
		set.lifetime.released.Store(true)
		for _, id := range slices.Backward(set.ids) {
			entry := set.entries[id]
			entry.available <- struct{}{}
			set.owner.dropUser(id, entry)
		}
	})
}
