package completion

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLifetimeGraceStartsOnlyAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		owned := New(parent, 30*time.Second)
		defer owned.Close()
		time.Sleep(2 * time.Minute)
		require.NoError(t, owned.Context().Err(), "uncanceled work has no artificial duration cap")
		cancel()
		synctest.Wait()
		time.Sleep(29 * time.Second)
		require.NoError(t, owned.Context().Err())
		time.Sleep(time.Second)
		synctest.Wait()
		require.ErrorIs(t, owned.Context().Err(), context.Canceled)
	})
}

func TestLifetimePreservesCallerDeadlineThenCompletionGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		owned := New(parent, 30*time.Second)
		defer owned.Close()
		time.Sleep(89 * time.Second)
		require.NoError(t, parent.Err())
		require.NoError(t, owned.Context().Err())
		time.Sleep(time.Second)
		synctest.Wait()
		require.ErrorIs(t, parent.Err(), context.DeadlineExceeded)
		time.Sleep(29 * time.Second)
		require.NoError(t, owned.Context().Err())
		time.Sleep(time.Second)
		synctest.Wait()
		require.ErrorIs(t, context.Cause(owned.Context()), context.DeadlineExceeded)
	})
}

func TestLifetimeFirstOwnerCancellationCannotExtendGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		backend, stop := context.WithCancelCause(t.Context())
		defer stop(nil)
		owned := New(parent, 30*time.Second, backend)
		defer owned.Close()
		stopped := errors.New("backend stopped")
		stop(stopped)
		synctest.Wait()
		time.Sleep(20 * time.Second)
		cancel()
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.ErrorIs(t, context.Cause(owned.Context()), stopped)
	})
}

func TestLifetimeCopiesShareCompletionOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		owned := New(parent, 30*time.Second)
		copied := owned
		go owned.Close()
		go copied.Close()
		synctest.Wait()
		require.ErrorIs(t, owned.Context().Err(), context.Canceled)
		require.Same(t, owned.Context(), copied.Context())
		cancel()
		synctest.Wait()
	})
	var zero Lifetime
	zero.Close()
	require.ErrorIs(t, zero.Context().Err(), context.Canceled)
}
