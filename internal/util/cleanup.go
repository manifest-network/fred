package util

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"
)

// CleanupFunc performs cleanup and returns an error if it fails.
type CleanupFunc func() error

// PanicHandler is called (from the loop goroutine's recover) when
// cleanup panics. The caller passes its own trusted handler — typically
// a closure that bumps a component-specific Prometheus counter. Keeping
// the handler a per-call parameter (instead of a package global) means:
// no data race to guard against, no public injection point for untrusted
// code, and each call site makes its own observability wiring explicit.
type PanicHandler func(recovered any)

// StartCleanupLoop runs cleanup periodically until ctx is canceled.
// Every invocation is wrapped in recover() so one bad iteration does
// NOT crash the fred process. On panic, logs the panic + stack with the
// component label and invokes onPanic (if non-nil) for metric bumping.
// The loop continues on the next tick regardless.
//
// onPanic is expected to be in-tree code (e.g., a metrics increment);
// a panic inside onPanic itself would propagate. Pass nil to skip.
//
// Example usage:
//
//	wg.Go(func() {
//	    util.StartCleanupLoop(ctx, interval, cleanupFn, "token_tracker",
//	        func(any) { metrics.CleanupPanicsTotal.WithLabelValues("token_tracker").Inc() })
//	})
func StartCleanupLoop(ctx context.Context, interval time.Duration, cleanup CleanupFunc, component string, onPanic PanicHandler) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error(component+" cleanup panic — recovering to keep fred alive",
					"panic", r,
					"stack", string(debug.Stack()),
				)
				if onPanic != nil {
					onPanic(r)
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
