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
// The "cleanup loop keeps fred alive" invariant is unconditional:
// a panic inside onPanic itself is also recovered (and logged), so a
// buggy observability hook can't take down the loop goroutine.
//
// Example usage:
//
//	wg.Go(func() {
//	    util.StartCleanupLoop(ctx, interval, cleanupFn, "token",
//	        func(any) { metrics.CleanupPanicsTotal.WithLabelValues("token").Inc() })
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
					// Nested recover: a panic inside the observability
					// hook must not escape and crash the process. The
					// outer recover above has already consumed the
					// original panic, so any further panic in onPanic
					// would otherwise propagate unconstrained.
					func() {
						defer func() {
							if r2 := recover(); r2 != nil {
								slog.Error("cleanup panic handler itself panicked",
									"component", component,
									"panic", r2,
									"stack", string(debug.Stack()),
								)
							}
						}()
						onPanic(r)
					}()
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
