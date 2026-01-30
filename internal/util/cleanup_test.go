package util

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartCleanupLoop(t *testing.T) {
	t.Run("runs_cleanup_periodically", func(t *testing.T) {
		var callCount atomic.Int32

		cleanup := func() error {
			callCount.Add(1)
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())

		go StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test")

		// Wait for at least 2 cleanup runs
		time.Sleep(35 * time.Millisecond)
		cancel()

		count := callCount.Load()
		if count < 2 {
			t.Errorf("cleanup called %d times, want at least 2", count)
		}
	})

	t.Run("stops_on_context_cancellation", func(t *testing.T) {
		var callCount atomic.Int32

		cleanup := func() error {
			callCount.Add(1)
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test")
			close(done)
		}()

		// Let it run a bit
		time.Sleep(25 * time.Millisecond)

		// Cancel and wait for goroutine to exit
		cancel()

		select {
		case <-done:
			// Good - loop exited
		case <-time.After(100 * time.Millisecond):
			t.Error("StartCleanupLoop did not exit after context cancellation")
		}
	})

	t.Run("continues_on_cleanup_error", func(t *testing.T) {
		var callCount atomic.Int32

		cleanup := func() error {
			callCount.Add(1)
			return errors.New("cleanup failed")
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test")

		// Wait for multiple cleanup runs despite errors
		time.Sleep(35 * time.Millisecond)

		count := callCount.Load()
		if count < 2 {
			t.Errorf("cleanup should continue despite errors, called %d times", count)
		}
	})

	t.Run("respects_interval", func(t *testing.T) {
		var callCount atomic.Int32

		cleanup := func() error {
			callCount.Add(1)
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())

		go StartCleanupLoop(ctx, 50*time.Millisecond, cleanup, "test")

		// Wait less than one interval
		time.Sleep(30 * time.Millisecond)
		cancel()

		count := callCount.Load()
		if count != 0 {
			t.Errorf("cleanup called %d times before interval elapsed, want 0", count)
		}
	})
}
