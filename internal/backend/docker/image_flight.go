package docker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

// imageFlights coalesces preparation of one immutable selection. Membership is
// held through each participant's pin publication, including the interval after
// import but before the first pin exists. It never carries a tenant's authority.
type imageFlights struct {
	mu     sync.Mutex
	active map[imageFlightKey]*imageFlightState
}

type imageFlightKey struct {
	source, os, architecture, variant, version, features string
}

type imageFlightState struct {
	owner   *imageFlights
	key     imageFlightKey
	done    chan struct{}
	members int
	outcome imageFlightOutcome
	finish  sync.Once
}

type imageFlightMembership struct {
	state       *imageFlightState
	preparation *imageTenantPreparationState
	close       func()
}

// Only the elected leader can publish completion. Followers receive immutable
// content evidence and must obtain a fresh execution capability for their lease.
type imageFlightLeader struct{ state *imageFlightState }

type imageFlightContent struct {
	id, source string
	platform   ocispec.Platform
	bytes      int64
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

func (p imageTenantPreparation) joinFlight(m *imageCapacityManager, resolution imagefetch.Resolution) (*imageFlightMembership, *imageFlightLeader, error) {
	if p.state == nil || p.state.owner != m || resolution.SourceReference() == "" {
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
		variant: platform.Variant, version: platform.OSVersion, features: string(features)}
	flights := &m.flights
	flights.mu.Lock()
	defer flights.mu.Unlock()
	if flights.active == nil {
		flights.active = make(map[imageFlightKey]*imageFlightState)
	}
	state := flights.active[key]
	var leader *imageFlightLeader
	if state == nil {
		state = &imageFlightState{owner: flights, key: key, done: make(chan struct{})}
		flights.active[key] = state
		leader = &imageFlightLeader{state: state}
	}
	state.members++
	member := &imageFlightMembership{state: state, preparation: p.state}
	member.close = sync.OnceFunc(func() {
		flights.mu.Lock()
		defer flights.mu.Unlock()
		state.members--
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
		state.outcome = outcome
		close(state.done)
		_, retry := outcome.(imageFlightRetry)
		if (retry || state.members == 0) && state.owner.active[state.key] == state {
			delete(state.owner.active, state.key)
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
