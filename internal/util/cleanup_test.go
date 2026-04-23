package util

import (
	"context"
	"errors"
	"fmt"
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

		go StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test", nil)

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
			StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test", nil)
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

		go StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "test", nil)

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

		go StartCleanupLoop(ctx, 50*time.Millisecond, cleanup, "test", nil)

		// Wait less than one interval
		time.Sleep(30 * time.Millisecond)
		cancel()

		count := callCount.Load()
		assert.Equal(t, int32(0), count, "cleanup called %d times before interval elapsed, want 0", count)
	})

	t.Run("panic_in_cleanup_does_not_crash_loop", func(t *testing.T) {
		// Pin the invariant that one panicking cleanup iteration does
		// NOT crash fred. The loop must continue running and call
		// cleanup again on the next tick. The injected onPanic handler
		// must fire with the recovered panic value.
		var callCount atomic.Int32
		var handlerFired atomic.Int32
		var handlerPanicVal atomic.Value

		onPanic := func(r any) {
			handlerFired.Add(1)
			handlerPanicVal.Store(fmt.Sprint(r))
		}

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
			StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "unit_test", onPanic)
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

		// require.Equal rather than assert.Equal so a failure here stops
		// the test instead of cascading into Load() below on an unset
		// value (which would turn a clean assertion failure into a
		// harder-to-read message).
		require.Equal(t, int32(1), handlerFired.Load(),
			"onPanic must fire exactly once")
		require.Equal(t, "synthetic cleanup panic", handlerPanicVal.Load(),
			"handler must receive the recovered panic value")
	})

	t.Run("panic_in_onPanic_handler_does_not_crash_loop", func(t *testing.T) {
		// Pin the stronger invariant that the cleanup loop survives
		// even if the observability hook ITSELF panics. Without the
		// nested recover around onPanic, a buggy handler would escape
		// the outer recover and crash the process.
		var cleanupCallCount atomic.Int32
		var onPanicCallCount atomic.Int32

		onPanic := func(r any) {
			onPanicCallCount.Add(1)
			panic("synthetic onPanic hook panic")
		}

		cleanup := func() error {
			cleanupCallCount.Add(1)
			if cleanupCallCount.Load() == 1 {
				panic("synthetic cleanup panic")
			}
			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			StartCleanupLoop(ctx, 10*time.Millisecond, cleanup, "unit_test_nested", onPanic)
			close(done)
		}()

		require.Eventually(t, func() bool {
			return cleanupCallCount.Load() >= 2
		}, time.Second, 10*time.Millisecond,
			"loop must keep running even when the onPanic handler itself panics")

		cancel()
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			require.Fail(t, "StartCleanupLoop did not exit cleanly after nested panic recovery")
		}

		require.GreaterOrEqual(t, onPanicCallCount.Load(), int32(1),
			"onPanic handler must have been invoked at least once")
	})
}
