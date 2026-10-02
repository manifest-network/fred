package provisioner

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
)

var testSweepGrace = sweepGrace{configured: time.Second}

type sweepDrainKey struct{}

// watchedParent is a cancelable parent that neither derives from a
// context.WithCancel context nor implements AfterFunc, so context.AfterFunc
// has to watch it with a goroutine. Only the stop function AfterFunc returns,
// or the parent's cancellation, ends that goroutine. Inside a synctest bubble
// a dropped stop therefore leaves it durably blocked, and the bubble fails.
type watchedParent struct {
	context.Context
	done chan struct{}
}

func newWatchedParent() watchedParent {
	return watchedParent{
		Context: context.WithValue(context.Background(), sweepDrainKey{}, "kept"),
		done:    make(chan struct{}),
	}
}

func (parent watchedParent) Done() <-chan struct{} { return parent.done }

func (parent watchedParent) Err() error {
	select {
	case <-parent.done:
		return context.Canceled
	default:
		return nil
	}
}

// The bubble's fake clock makes each grace boundary exact, and synctest fails
// the test if a goroutine started here is still blocked when it ends.
func TestSweepDrainContext(t *testing.T) {
	t.Run("a sweep that finishes inside the grace is never canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(context.WithValue(t.Context(), sweepDrainKey{}, "kept"))
			ctx, release := sweepDrainContext(parent, testSweepGrace)
			cancelParent()
			time.Sleep(testSweepGrace.duration() - time.Nanosecond)
			synctest.Wait()
			require.NoError(t, ctx.Err(), "the sweep keeps running inside its grace")
			require.Equal(t, "kept", ctx.Value(sweepDrainKey{}), "the sweep keeps its parent's values")
			release()
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.ErrorIs(t, context.Cause(ctx), context.Canceled)
			synctest.Wait()
		})
	})

	t.Run("an expired grace cancels the sweep with the parent's cause", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			stopping := errors.New("providerd stopping")
			parent, cancelParent := context.WithCancelCause(t.Context())
			ctx, release := sweepDrainContext(parent, testSweepGrace)
			defer release()
			cancelParent(stopping)
			time.Sleep(testSweepGrace.duration())
			synctest.Wait()
			require.ErrorIs(t, ctx.Err(), context.Canceled,
				"an abandoned sweep sees an ordinary cancellation")
			require.ErrorIs(t, context.Cause(ctx), stopping)
		})
	})

	t.Run("a parent canceled before the call still gets the full grace", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(t.Context())
			cancelParent()
			ctx, release := sweepDrainContext(parent, testSweepGrace)
			defer release()
			time.Sleep(testSweepGrace.duration() - time.Nanosecond)
			synctest.Wait()
			require.NoError(t, ctx.Err())
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			require.ErrorIs(t, ctx.Err(), context.Canceled)
		})
	})

	t.Run("a zero or negative grace is the default, never no grace", func(t *testing.T) {
		for _, grace := range []sweepGrace{{}, {configured: -time.Second}} {
			synctest.Test(t, func(t *testing.T) {
				parent, cancelParent := context.WithCancel(t.Context())
				ctx, release := sweepDrainContext(parent, grace)
				defer release()
				cancelParent()
				time.Sleep(defaultShutdownSweepGrace - time.Nanosecond)
				synctest.Wait()
				require.NoError(t, ctx.Err(), "grace %+v must not disable the drain", grace)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			})
		}
	})

	t.Run("release unhooks the sweep from a parent that is never canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// AfterFunc watches this parent with a goroutine that only release,
			// through stop, ends. Without it that goroutine stays blocked on
			// the never-closed parent, and synctest fails the bubble.
			parent := newWatchedParent()
			ctx, release := sweepDrainContext(parent, testSweepGrace)
			require.NoError(t, ctx.Err())
			require.Equal(t, "kept", ctx.Value(sweepDrainKey{}))
			release()
			release()
			require.ErrorIs(t, ctx.Err(), context.Canceled, "release ends the sweep context")
			synctest.Wait()
		})
	})
}

func TestNewReconcilerShutdownSweepGraceDefaults(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"unset", 0, defaultShutdownSweepGrace},
		{"negative", -time.Second, defaultShutdownSweepGrace},
		{"configured", 3 * time.Second, 3 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: &mockReconcilerBackend{name: "test"}, IsDefault: true}},
			})
			require.NoError(t, err)
			reconciler, err := newTestReconciler(t, ReconcilerConfig{ShutdownSweepGrace: test.configured},
				&chaintest.MockClient{}, noopAck, router, nil, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, reconciler.shutdownSweepGrace.duration())
		})
	}
}
