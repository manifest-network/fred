package docker

import (
	"context"
	"errors"
	"slices"
	"sync"
)

const maxImageStages = 4

// imageTenantShares owns the entire staging pool. A sole tenant can use every
// slot; a waiting tenant with fewer active stages gets the next released slot.
// Equal shares and requests within a tenant follow arrival order. Cached image
// preparation never enters this queue. There is no second semaphore whose
// acquisition order could undo this scheduling decision.
type imageTenantShares struct {
	mu      sync.Mutex
	active  map[string]int
	used    int
	waiters []*imageTenantWaiter
}
type imageTenantWaiter struct {
	owner                       *imageTenantShares
	members                     map[string]int
	tenants                     []string
	ready                       chan struct{}
	tenant                      string // Active accounting sponsor; the slot itself belongs to the flight.
	claimed, admitted, released bool
}

// updateMember changes scheduling evidence, never staging authority. A flight
// competes at its least-loaded live member's share, and transfers an active
// charge if that sponsor leaves. The flight retains the slot until cleanup.
func (s *imageTenantShares) updateMember(w *imageTenantWaiter, tenant string, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.members[tenant] == 0 && delta > 0 {
		w.tenants = append(w.tenants, tenant)
	}
	w.members[tenant] += delta
	if w.members[tenant] == 0 {
		delete(w.members, tenant)
		for i, member := range w.tenants {
			if member == tenant {
				w.tenants = slices.Delete(w.tenants, i, i+1)
				break
			}
		}
	}
	if w.admitted && !w.released && w.members[w.tenant] == 0 {
		s.uncharge(w.tenant)
		w.tenant = s.bestTenant(w)
		if w.tenant != "" {
			s.active[w.tenant]++
		}
	}
	s.admit()
}

func (s *imageTenantShares) acquire(ctx context.Context, waiter *imageTenantWaiter) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if waiter == nil || waiter.owner != s || waiter.claimed || len(waiter.members) == 0 {
		s.mu.Unlock()
		return nil, errors.New("image staging requires an unconsumed live flight")
	}
	if s.active == nil {
		s.active = make(map[string]int)
	}
	waiter.claimed = true
	s.waiters = append(s.waiters, waiter)
	s.admit()
	s.mu.Unlock()
	release := sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		waiter.released = true
		s.used--
		s.uncharge(waiter.tenant)
		s.admit()
	})
	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
		return release, nil
	case <-ctx.Done():
		s.mu.Lock()
		if waiter.admitted {
			s.mu.Unlock()
			release()
		} else {
			for index, pending := range s.waiters {
				if pending == waiter {
					s.waiters = slices.Delete(s.waiters, index, index+1)
					break
				}
			}
			s.mu.Unlock()
		}
		return nil, ctx.Err()
	}
}

func (s *imageTenantShares) uncharge(tenant string) {
	if tenant == "" {
		return
	}
	s.active[tenant]--
	if s.active[tenant] == 0 {
		delete(s.active, tenant)
	}
}

func (s *imageTenantShares) bestTenant(w *imageTenantWaiter) string {
	var selected string
	for _, tenant := range w.tenants {
		if selected == "" || s.active[tenant] < s.active[selected] {
			selected = tenant
		}
	}
	return selected
}

func (s *imageTenantShares) admit() {
	for s.used < maxImageStages && len(s.waiters) != 0 {
		selected, tenant := -1, ""
		for index, waiter := range s.waiters {
			candidate := s.bestTenant(waiter)
			if candidate != "" && (selected < 0 || s.active[candidate] < s.active[tenant]) {
				selected, tenant = index, candidate
			}
		}
		if selected < 0 {
			return
		}
		waiter := s.waiters[selected]
		s.waiters = slices.Delete(s.waiters, selected, selected+1)
		s.used++
		s.active[tenant]++
		waiter.tenant, waiter.admitted = tenant, true
		close(waiter.ready)
	}
}

// imageTenantPreparation comes only from a Started journal subject, never a
// caller-selected tenant string. It can join flights but cannot stage work.
// Closing it releases membership; only the flight owns staged files and slots.
type imageTenantPreparation struct{ state *imageTenantPreparationState }
type imageTenantPreparationState struct {
	owner   *imageCapacityManager
	tenant  string
	mu      sync.Mutex
	closed  bool
	flights map[*imageFlightMembership]struct{}
}

func (m *imageCapacityManager) beginTenantPreparation(ctx context.Context, mutations *storageMutations) (imageTenantPreparation, error) {
	var tenant string
	if mutations != nil {
		if subject := mutations.operationSubject; subject.Valid() {
			tenant = subject.Intent().Tenant()
		} else if subject := mutations.maintenanceSubject; subject.Valid() {
			tenant = subject.Intent().Tenant()
		}
	}
	if tenant == "" {
		return imageTenantPreparation{}, errors.New("image preparation requires a Started tenant authority")
	}
	if err := ctx.Err(); err != nil {
		return imageTenantPreparation{}, err
	}
	return imageTenantPreparation{state: &imageTenantPreparationState{owner: m, tenant: tenant}}, nil
}

func (p imageTenantPreparation) close() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.closed = true
	for flight := range p.state.flights {
		flight.close()
	}
	p.state.flights = nil
}
