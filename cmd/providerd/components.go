package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
)

// components supervises providerd's long-lived goroutines (ENG-1053 M1). A
// component runs until shutdown begins, which is when the work context the
// supervisor owns is done. Returning before that for any reason, a nil error
// included, failing with an error other than cancellation, or panicking is a
// failure. The first failure cancels the work context, so it also starts
// shutdown. Err reports every recorded failure, and run returns it:
// providerd cannot exit 0 after a component failed, so a supervisor's
// Restart=on-failure restarts it.
//
// The supervisor derives the work context itself, so failure-triggered
// shutdown cannot be wired to an unrelated context. It also owns the groups
// shutdown joins, so run holds no WaitGroup a component could be started on
// unsupervised. TestProviderdGoroutinesStartThroughComponents rejects every go
// statement and every call to a method named Go in this package outside its
// reviewed functions; the supervisor deliberately has no Go method. That test
// reads syntax, so it cannot see a goroutine started inside a library call
// such as context.AfterFunc or an HTTP server.
type components struct {
	stopping <-chan struct{}
	stop     context.CancelFunc
	// runtime holds every component but the store readers.
	runtime sync.WaitGroup
	// storeReaders read the live stores, so shutdown joins them on their own
	// before the stores close, whatever the shutdown timeout did to runtime.
	storeReaders sync.WaitGroup

	mu       sync.Mutex
	failures []error
}

// newComponents returns a supervisor and the work context it owns. The
// context ends when parent ends, at BeginShutdown, or on the first component
// failure.
func newComponents(parent context.Context) (*components, context.Context) {
	work, stop := context.WithCancel(parent)
	return &components{stopping: work.Done(), stop: stop}, work
}

// BeginShutdown ends the work context. Components stopping after it are not
// failures.
func (c *components) BeginShutdown() {
	c.stop()
}

// Start starts fn as the named component.
func (c *components) Start(name string, fn func() error) {
	c.supervise(&c.runtime, name, fn)
}

// StartStoreReader starts fn as a named component that reads the live stores.
// WaitStoreReaders joins it before they close.
func (c *components) StartStoreReader(name string, fn func() error) {
	c.supervise(&c.storeReaders, name, fn)
}

func (c *components) supervise(join *sync.WaitGroup, name string, fn func() error) {
	join.Go(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("component panicked", "component", name,
					"panic", recovered, "stack", string(debug.Stack()))
				c.fail(fmt.Errorf("%s panic: %v", name, recovered))
			}
		}()
		err := fn()
		switch {
		case err != nil && !errors.Is(err, context.Canceled):
			c.fail(fmt.Errorf("%s error: %w", name, err))
		case !c.shuttingDown():
			c.fail(fmt.Errorf("%s stopped before shutdown", name))
		}
	})
}

func (c *components) shuttingDown() bool {
	select {
	case <-c.stopping:
		return true
	default:
		return false
	}
}

func (c *components) fail(err error) {
	slog.Error("component failed; shutting down", "error", err)
	c.mu.Lock()
	c.failures = append(c.failures, err)
	c.mu.Unlock()
	c.stop()
}

// Err joins every failure recorded so far, or returns nil if there is none.
func (c *components) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return errors.Join(c.failures...)
}

// Wait reports whether every component but the store readers stopped before
// ctx ended.
func (c *components) Wait(ctx context.Context) bool {
	return joinWithin(ctx, &c.runtime)
}

// WaitStoreReaders reports whether every store reader stopped before ctx
// ended.
func (c *components) WaitStoreReaders(ctx context.Context) bool {
	return joinWithin(ctx, &c.storeReaders)
}
