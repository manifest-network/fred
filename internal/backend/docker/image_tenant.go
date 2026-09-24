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
	tenant   string
	ready    chan struct{}
	admitted bool
}

func (s *imageTenantShares) acquire(ctx context.Context, tenant string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.active == nil {
		s.active = make(map[string]int)
	}
	waiter := &imageTenantWaiter{tenant: tenant, ready: make(chan struct{})}
	s.waiters = append(s.waiters, waiter)
	s.admit()
	s.mu.Unlock()
	release := sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.used--
		s.active[tenant]--
		if s.active[tenant] == 0 {
			delete(s.active, tenant)
		}
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

func (s *imageTenantShares) admit() {
	for s.used < maxImageStages && len(s.waiters) != 0 {
		selected := 0
		for index, waiter := range s.waiters {
			if s.active[waiter.tenant] < s.active[s.waiters[selected].tenant] {
				selected = index
			}
		}
		waiter := s.waiters[selected]
		s.waiters = slices.Delete(s.waiters, selected, selected+1)
		s.used++
		s.active[waiter.tenant]++
		waiter.admitted = true
		close(waiter.ready)
	}
}

// imageTenantPreparation comes only from a Started journal subject, never a
// caller-selected tenant string. Copies share a single-use staging entitlement.
// Constructing it takes no capacity. Only actual staging consumes a slot, and
// retains that slot until cleanup even if a copied parent closes first.
type imageTenantPreparation struct{ state *imageTenantPreparationState }
type imageTenantPreparationState struct {
	owner        *imageCapacityManager
	tenant       string
	release      func()
	mu           sync.Mutex
	closed       bool
	stageClaimed bool
	staging      bool
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

func (p imageTenantPreparation) startStaging(ctx context.Context, m *imageCapacityManager) error {
	if p.state == nil || p.state.owner != m {
		return errors.New("image staging requires its tenant preparation owner")
	}
	p.state.mu.Lock()
	if p.state.closed || p.state.stageClaimed {
		p.state.mu.Unlock()
		return errors.New("image tenant preparation staging is already consumed")
	}
	p.state.stageClaimed, p.state.staging = true, true
	p.state.mu.Unlock()
	release, err := m.tenantShares.acquire(ctx, p.state.tenant)
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if err != nil {
		p.state.staging = false
		return err
	}
	p.state.release = release
	return nil
}

func (p imageTenantPreparation) finishStaging() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.staging = false
	if p.state.release != nil {
		p.state.release()
	}
}

func (p imageTenantPreparation) close() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.closed = true
	if !p.state.staging && p.state.release != nil {
		p.state.release()
	}
}
