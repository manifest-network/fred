// Package completion owns the bounded completion window of an admitted effect.
package completion

import (
	"context"
	"sync"
	"time"
)

// Lifetime keeps an admitted exchange alive for its caller's normal lifetime,
// then grants a bounded completion grace after the first cancellation. Copies
// share ownership of the context, cancellation callbacks and grace timer.
type Lifetime struct{ state *state }

type state struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	grace  time.Duration
	mu     sync.Mutex
	closed bool
	timer  *time.Timer
	stops  []func() bool
}

// New preserves parent's values and observes its cancellation plus any extra
// owner lifetimes. No timer runs while every observed lifetime remains live.
// A caller deadline starts the grace when that deadline actually expires.
func New(parent context.Context, grace time.Duration, cancellation ...context.Context) Lifetime {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	s := &state{ctx: ctx, cancel: cancel, grace: grace}
	parents := append([]context.Context{parent}, cancellation...)
	for _, parent := range parents {
		s.stops = append(s.stops, context.AfterFunc(parent, func() { s.beginGrace(context.Cause(parent)) }))
	}
	return Lifetime{state: s}
}

func (s *state) beginGrace(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.timer != nil {
		return
	}
	s.timer = time.AfterFunc(s.grace, func() { s.cancel(cause) })
}

var unavailable = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// Context is the exchange context. A zero Lifetime cannot authorize work.
func (l Lifetime) Context() context.Context {
	if l.state == nil {
		return unavailable
	}
	return l.state.ctx
}

// Close releases completion ownership and is safe through any copied handle.
func (l Lifetime) Close() {
	if l.state == nil {
		return
	}
	s := l.state
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	stops := s.stops
	s.stops = nil
	s.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
	s.cancel(context.Canceled)
}
