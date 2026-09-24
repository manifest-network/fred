package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type budgetedHTTPShutdown struct{}

func (owner budgetedHTTPShutdown) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type budgetedBackendShutdown struct{ remaining chan time.Duration }

func (owner budgetedBackendShutdown) StopContext(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	owner.remaining <- time.Until(deadline)
	<-ctx.Done()
	return ctx.Err()
}

func TestHTTPAndBackendShutdownShareProcessBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		remaining := make(chan time.Duration, 1)
		httpErr, backendErr := drainHTTPAndBackend(t.Context(), budgetedHTTPShutdown{}, budgetedBackendShutdown{remaining: remaining})
		require.ErrorIs(t, httpErr, context.DeadlineExceeded)
		require.ErrorIs(t, backendErr, context.DeadlineExceeded)
		require.Equal(t, 45*time.Second, <-remaining, "HTTP consumes the same process budget as backend drain")
		require.Equal(t, 75*time.Second, time.Since(started))
	})
}
