package util

import (
	"context"
	"log/slog"
	"time"
)

// CleanupFunc is a function that performs cleanup and returns an error if it fails.
type CleanupFunc func() error

// StartCleanupLoop runs a cleanup function periodically until the context is cancelled.
// It logs errors using slog with the provided component name.
// The caller is responsible for calling wg.Done() if using a WaitGroup.
//
// Example usage:
//
//	go func() {
//	    defer wg.Done()
//	    util.StartCleanupLoop(ctx, interval, cleanupFunc, "token_tracker")
//	}()
func StartCleanupLoop(ctx context.Context, interval time.Duration, cleanup CleanupFunc, component string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := cleanup(); err != nil {
				slog.Error(component+" cleanup failed", "error", err)
			}
		}
	}
}
