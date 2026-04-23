package util

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"
)

// CleanupFunc is a function that performs cleanup and returns an error if it fails.
type CleanupFunc func() error

// CleanupPanicHandler is called when a cleanup function panics and is
// recovered. Intended for wiring up a metric counter in the caller
// (injection keeps internal/util free of the metrics dependency).
// Receives the component name and the recovered panic value.
type CleanupPanicHandler func(component string, recovered any)

// cleanupPanicHandler is the process-wide handler invoked when
// StartCleanupLoop recovers a panic. nil means no-op (log only).
// Set once during process init via SetCleanupPanicHandler.
var cleanupPanicHandler CleanupPanicHandler

// SetCleanupPanicHandler installs a process-wide handler that fires
// every time StartCleanupLoop recovers a panic in a cleanup function.
// Typically used to bump a Prometheus counter. Safe to leave nil for
// tests; logs still emit regardless.
func SetCleanupPanicHandler(h CleanupPanicHandler) {
	cleanupPanicHandler = h
}

// StartCleanupLoop runs a cleanup function periodically until the context is canceled.
// It logs errors using slog with the provided component name.
// The caller is responsible for goroutine lifecycle management.
// Typically used with wg.Go() (Go 1.25+) which handles Done() automatically.
//
// Panic recovery: each cleanup invocation is wrapped in recover() so one
// bad iteration does NOT crash the entire fred process. On panic, logs
// the panic value + stack with the component label, invokes the
// process-wide CleanupPanicHandler (if set) for metric bumping, and
// continues the loop — the next tick tries again.
//
// Example usage:
//
//	wg.Go(func() {
//	    util.StartCleanupLoop(ctx, interval, cleanupFunc, "token_tracker")
//	})
func StartCleanupLoop(ctx context.Context, interval time.Duration, cleanup CleanupFunc, component string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error(component+" cleanup panic — recovering to keep fred alive",
					"panic", r,
					"stack", string(debug.Stack()),
				)
				if cleanupPanicHandler != nil {
					cleanupPanicHandler(component, r)
				}
			}
		}()
		if err := cleanup(); err != nil {
			slog.Error(component+" cleanup failed", "error", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
