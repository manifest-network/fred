package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/chain"
)

// TestDemoteOnGrantSetupError guards the reachable half of the ENG-688 fix. The
// sentinel itself is covered in internal/chain, but this is the branch that
// actually decides whether providerd sheds its sub-signers, and an inverted
// condition here would silently reinstate the bug.
func TestDemoteOnGrantSetupError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "success does not demote",
			err:  nil,
			want: false,
		},
		{
			name: "unverified grants do not demote",
			err:  chain.ErrGrantsUnverified,
			want: false,
		},
		{
			name: "wrapped unverified grants do not demote",
			err:  fmt.Errorf("ensure authz grants after 3 attempts: %w: i/o timeout", chain.ErrGrantsUnverified),
			want: false,
		},
		{
			name: "grants known missing and uncreatable demotes",
			err:  errors.New("failed to create grant: insufficient fee"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, demoteOnGrantSetupError(tt.err))
		})
	}
}

func TestInitialProviderWork_SignalCancellationStopsSequence(t *testing.T) {
	signalCtx, signalCancel := context.WithCancel(t.Context())
	defer signalCancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	reconciled := false
	go func() {
		done <- runInitialProviderWork(signalCtx, func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}, func(context.Context) error { reconciled = true; return nil })
	}()
	<-entered
	signalCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("startup did not observe signal cancellation")
	}
	require.False(t, reconciled, "canceled startup must not issue a later reconciliation")
}

func TestInitialProviderWork_CanceledBeforeStartIssuesNoOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	operation := func(context.Context) error { calls++; return nil }
	require.ErrorIs(t, runInitialProviderWork(ctx, operation, operation), context.Canceled)
	require.Zero(t, calls)
}

func TestInitialProviderWork_TransientFailureStillReconciles(t *testing.T) {
	var calls []string
	err := runInitialProviderWork(t.Context(), func(context.Context) error {
		calls = append(calls, "withdraw")
		return errors.New("chain temporarily unavailable")
	}, func(context.Context) error {
		calls = append(calls, "reconcile")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"withdraw", "reconcile"}, calls)
}

// TestShutdownSweepGraceForStaysWithinTheShutdownTimeout pins the providerd
// wiring of the reconciler's sweep drain: the grace is never zero, which the
// reconciler would read as its 15s default, and never exceeds the configured
// shutdown_timeout, even when that timeout is too short to halve.
func TestShutdownSweepGraceForStaysWithinTheShutdownTimeout(t *testing.T) {
	for _, test := range []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{timeout: time.Nanosecond, want: time.Nanosecond},
		{timeout: 2 * time.Nanosecond, want: time.Nanosecond},
		{timeout: time.Second, want: 500 * time.Millisecond},
		{timeout: 30 * time.Second, want: 15 * time.Second},
	} {
		t.Run(test.timeout.String(), func(t *testing.T) {
			got := shutdownSweepGraceFor(test.timeout)
			assert.Equal(t, test.want, got)
			assert.Positive(t, got)
			assert.LessOrEqual(t, got, test.timeout)
		})
	}
}
