package docker

import (
	"context"
	"errors"
	"sync"
)

// imageTenantShares admits one preparation per durable tenant. Waiters hold no
// provider staging slot or GC admission. Entries exist only while owned work is
// live, so abandoned or canceled callers cannot grow a tenant registry forever.
type imageTenantShares struct {
	mu     sync.Mutex
	active map[string]chan struct{}
}

func (s *imageTenantShares) acquire(ctx context.Context, tenant string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.active == nil {
			s.active = make(map[string]chan struct{})
		}
		done, busy := s.active[tenant]
		if !busy {
			done = make(chan struct{})
			s.active[tenant] = done
			s.mu.Unlock()
			return sync.OnceFunc(func() {
				s.mu.Lock()
				delete(s.active, tenant)
				close(done)
				s.mu.Unlock()
			}), nil
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
		}
	}
}

// imageTenantPreparation comes only from a Started journal subject, never a
// caller-selected tenant string. Copies share a single-use staging entitlement.
// A stage retains the tenant share until it closes, even if its parent closes
// first, so misplaced cleanup cannot admit a second tenant stage concurrently.
type imageTenantPreparation struct{ state *imageTenantPreparationState }
type imageTenantPreparationState struct {
	owner        *imageCapacityManager
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
	release, err := m.tenantShares.acquire(ctx, tenant)
	if err != nil {
		return imageTenantPreparation{}, err
	}
	return imageTenantPreparation{state: &imageTenantPreparationState{owner: m, release: release}}, nil
}

func (p imageTenantPreparation) startStaging(m *imageCapacityManager) error {
	if p.state == nil || p.state.owner != m {
		return errors.New("image staging requires its tenant preparation owner")
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.closed || p.state.stageClaimed {
		return errors.New("image tenant preparation staging is already consumed")
	}
	p.state.stageClaimed, p.state.staging = true, true
	return nil
}

func (p imageTenantPreparation) finishStaging() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.staging = false
	if p.state.closed {
		p.state.release()
	}
}

func (p imageTenantPreparation) close() {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.closed = true
	if !p.state.staging {
		p.state.release()
	}
}
