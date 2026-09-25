package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

// imageFlights coalesces preparation of one immutable selection. Membership is
// held through each participant's pin publication, including the interval after
// import but before the first pin exists. It never carries a tenant's authority.
type imageFlights struct {
	mu      sync.Mutex
	active  map[imageFlightKey]*imageFlightState
	workers int
	closed  bool
	drained chan struct{}
}

type imageFlightKey struct {
	source, os, architecture, variant, version, features string
	verification                                         imagebudget.VerificationBudget
}

type imageFlightState struct {
	owner   *imageFlights
	manager *imageCapacityManager
	ctx     context.Context
	cancel  context.CancelFunc
	share   *imageTenantWaiter
	key     imageFlightKey
	done    chan struct{}
	members int
	outcome imageFlightOutcome
	finish  sync.Once
	start   atomic.Bool
}

type imageFlightMembership struct {
	state       *imageFlightState
	preparation *imageTenantPreparationState
	close       func()
}

// Only the manager-owned worker can stage and publish completion. Membership
// holds no worker authority. Every lease obtains its own execution capability.
type imageFlightLeader struct{ state *imageFlightState }

type imageFlightContent struct {
	id, source string
	platform   ocispec.Platform
	budget     imagebudget.Budget
}

type imageFlightOutcome interface{ imageFlightOutcome() }

type imageFlightVerified struct{ content imageFlightContent }
type imageFlightFailure struct{ err error }

// Retry can be issued on owner cancellation only while import is proven undispatched.
type imageFlightRetry struct{}

func (imageFlightVerified) imageFlightOutcome() {}
func (imageFlightFailure) imageFlightOutcome()  {}
func (imageFlightRetry) imageFlightOutcome()    {}

func imageFlightBeforeDispatchFailure(ctx context.Context, err error) imageFlightOutcome {
	if ctx.Err() != nil {
		return imageFlightRetry{}
	}
	return imageFlightFailure{err: err}
}

func (p imageTenantPreparation) joinFlight(m *imageCapacityManager, resolution imagefetch.Resolution, budget imagebudget.VerificationBudget) (*imageFlightMembership, *imageFlightLeader, error) {
	if p.state == nil || p.state.owner != m || resolution.SourceReference() == "" || !budget.Valid() || m.lifetime == nil {
		return nil, nil, errors.New("image flight requires its tenant preparation and immutable resolution")
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.closed {
		return nil, nil, errors.New("image tenant preparation is closed")
	}
	platform := platforms.Normalize(resolution.Platform())
	slices.Sort(platform.OSFeatures)
	features, _ := json.Marshal(platform.OSFeatures)
	key := imageFlightKey{source: resolution.SourceReference(), os: platform.OS, architecture: platform.Architecture,
		variant: platform.Variant, version: platform.OSVersion, features: string(features), verification: budget}
	flights := &m.flights
	flights.mu.Lock()
	defer flights.mu.Unlock()
	if flights.closed || m.lifetime.Err() != nil {
		return nil, nil, errors.New("image flight manager is shut down")
	}
	if flights.active == nil {
		flights.active = make(map[imageFlightKey]*imageFlightState)
	}
	state := flights.active[key]
	var leader *imageFlightLeader
	if state == nil {
		ctx, cancel := context.WithCancel(m.lifetime)
		state = &imageFlightState{owner: flights, manager: m, key: key, done: make(chan struct{}), ctx: ctx, cancel: cancel, share: &imageTenantWaiter{owner: &m.tenantShares, ready: make(chan struct{}), members: make(map[string]int)}}
		flights.workers++
		flights.active[key] = state
		leader = &imageFlightLeader{state: state}
	}
	state.members++
	m.tenantShares.updateMember(state.share, p.state.tenant, 1)
	member := &imageFlightMembership{state: state, preparation: p.state}
	member.close = sync.OnceFunc(func() {
		flights.mu.Lock()
		defer flights.mu.Unlock()
		state.members--
		m.tenantShares.updateMember(state.share, p.state.tenant, -1)
		if state.members == 0 {
			state.cancel()
		}
		select {
		case <-state.done:
			if state.members == 0 && flights.active[key] == state {
				delete(flights.active, key)
			}
		default:
		}
	})
	if p.state.flights == nil {
		p.state.flights = make(map[*imageFlightMembership]struct{})
	}
	p.state.flights[member] = struct{}{}
	return member, leader, nil
}

func (m *imageFlightMembership) retire() {
	m.preparation.mu.Lock()
	delete(m.preparation.flights, m)
	m.preparation.mu.Unlock()
	m.close()
}

func (l *imageFlightLeader) complete(outcome imageFlightOutcome) {
	l.state.finish.Do(func() {
		state := l.state
		state.owner.mu.Lock()
		defer state.owner.mu.Unlock()
		state.cancel()
		state.outcome = outcome
		close(state.done)
		_, retry := outcome.(imageFlightRetry)
		if (retry || state.members == 0) && state.owner.active[state.key] == state {
			delete(state.owner.active, state.key)
		}
		state.owner.workers--
		if state.owner.closed && state.owner.workers == 0 {
			close(state.owner.drained)
		}
	})
}

func (m *imageFlightMembership) wait(ctx context.Context) (imageFlightOutcome, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.state.done:
		return m.state.outcome, ctx.Err()
	}
}

// shutdown closes worker admission under the same lock as joinFlight. An SDK
// that ignores cancellation retains its dependencies until the actual worker
// returns; callers may bound their wait without pretending that ownership ended.
func (m *imageFlights) shutdown(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		m.drained = make(chan struct{})
		for _, state := range m.active {
			state.cancel()
		}
		if m.workers == 0 {
			close(m.drained)
		}
	}
	drained := m.drained
	m.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *imageFlightLeader) startStaging(ctx context.Context, m *imageCapacityManager) (func(), error) {
	if l == nil || l.state == nil || l.state.manager != m {
		return nil, errors.New("image staging requires its flight owner")
	}
	return m.tenantShares.acquire(ctx, l.state.share)
}

// run is a goroutine and foreign-registry/SDK boundary. A panic cannot strand
// members or turn an ambiguous import into a new dispatch.
func (l *imageFlightLeader) run(work func(context.Context) imageFlightOutcome) {
	if !l.state.start.CompareAndSwap(false, true) {
		return
	}
	outcome := imageFlightOutcome(imageFlightFailure{err: errors.New("image preparation ended without completion")})
	defer func() {
		if value := recover(); value != nil {
			leaseWorkerPanicsTotal.WithLabelValues("image_flight").Inc()
			slog.Error("image preparation worker panicked", "backend", l.state.manager.cfg.Name, "panic", value)
			outcome = imageFlightFailure{err: fmt.Errorf("image preparation panicked: %v", value)}
		}
		l.complete(outcome)
	}()
	outcome = work(l.state.ctx)
}
