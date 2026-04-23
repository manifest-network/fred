package util

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		assert.GreaterOrEqual(t, count, int32(2))
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
			require.Fail(t, "StartCleanupLoop did not exit after context cancellation")
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
		assert.GreaterOrEqual(t, count, int32(2), "cleanup should continue despite errors")
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
		assert.Equal(t, int32(0), count, "cleanup called %d times before interval elapsed, want 0", count)
	})

	t.Run("panic_in_cleanup_does_not_crash_loop", func(t *testing.T) {
		// Pin the invariant that one panicking cleanup iteration does
		// NOT crash fred. The loop must continue running and call
		// cleanup again on the next tick. The process-wide handler
		// must fire with the right component name.
		var callCount atomic.Int32
		var handlerFired atomic.Int32
		var handlerComponent atomic.Value

		SetCleanupPanicHandler(func(component string, _ any) {
			handlerFired.Add(1)
			handlerComponent.Store(component)
		})
		defer SetCleanupPanicHandler(nil)

		cleanup := func() error {
			callCount.Add(1)
			if callCount.Load() == 1 {
				panic("synthetic cleanup panic")
			}
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "unit_test")
			close(done)
		}()

		require.Eventually(t, func() bool {
			return callCount.Load() >= 2
		}, time.Second, 10*time.Millisecond,
			"cleanup must be called again after a panicking iteration (loop must not die)")

		cancel()
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			require.Fail(t, "StartCleanupLoop did not exit cleanly after panic recovery")
		}

		assert.Equal(t, int32(1), handlerFired.Load(),
			"cleanup panic handler must fire exactly once")
		assert.Equal(t, "unit_test", handlerComponent.Load(),
			"handler must receive the component name")
	})
}
