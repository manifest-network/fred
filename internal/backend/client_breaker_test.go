package backend

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"
)

func TestCallerCancellationIsNeutralForEveryBreakerOperation(t *testing.T) {
	for _, name := range []string{"info", "lookup", "provision", "deprovision", "restart", "update", "restore", "domain"} {
		t.Run(name, func(t *testing.T) {
			client := newUnboundHTTPClientForTest(HTTPClientConfig{
				Name: "cancellation", BaseURL: "http://backend.invalid", CBFailureThresh: 2,
			})
			_, err := client.execute(t.Context(), func() (any, error) { return nil, errors.New("real failure") })
			require.Error(t, err)
			before := client.cb.Counts()
			for range 6 {
				ctx, cancel := context.WithCancel(t.Context())
				client.httpClient.Transport = causalOutcomeRoundTripper(func(req *http.Request) (*http.Response, error) {
					cancel()
					return nil, req.Context().Err()
				})
				var callErr error
				switch name {
				case "info":
					_, callErr = client.GetInfo(ctx, "lease")
				case "lookup":
					_, callErr = client.LookupProvisions(ctx, []string{"lease"})
				case "provision":
					callErr = client.Provision(ctx, ProvisionRequest{})
				case "deprovision":
					callErr = client.Deprovision(ctx, "lease")
				case "restart":
					callErr = client.Restart(ctx, RestartRequest{MaintenanceID: testMaintenanceRequestID(t)})
				case "update":
					callErr = client.Update(ctx, UpdateRequest{MaintenanceID: testMaintenanceRequestID(t)})
				case "restore":
					callErr = client.Restore(ctx, RestoreRequest{})
				case "domain":
					callErr = client.ReconcileCustomDomain(ctx, "lease", nil)
				}
				cancel()
				require.ErrorIs(t, callErr, context.Canceled)
				require.Equal(t, gobreaker.StateClosed, client.cb.State())
				after := client.cb.Counts()
				require.Equal(t, before.TotalFailures, after.TotalFailures)
				require.Equal(t, before.TotalSuccesses, after.TotalSuccesses)
				require.Equal(t, before.ConsecutiveFailures, after.ConsecutiveFailures, "caller abandonment must not reset a genuine failure streak")
				require.Equal(t, before.ConsecutiveSuccesses, after.ConsecutiveSuccesses)
			}
			_, err = client.execute(t.Context(), func() (any, error) { return nil, errors.New("second real failure") })
			require.Error(t, err)
			require.Equal(t, gobreaker.StateOpen, client.cb.State())
		})
	}
}

func TestBackendTimeoutWithLiveCallerStillTripsBreaker(t *testing.T) {
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "timeout", BaseURL: "http://backend.invalid", CBFailureThresh: 1,
	})
	client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	_, err := client.GetInfo(t.Context(), "lease")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, t.Context().Err())
	require.Equal(t, gobreaker.StateOpen, client.cb.State())
}

func TestExcludedHalfOpenProbeReleasesItsSlot(t *testing.T) {
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "half-open", BaseURL: "http://backend.invalid", CBFailureThresh: 1, CBTimeout: time.Millisecond,
	})
	_, err := client.execute(t.Context(), func() (any, error) { return nil, errors.New("real failure") })
	require.Error(t, err)
	require.Eventually(t, func() bool { return client.cb.State() == gobreaker.StateHalfOpen }, time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	_, err = client.execute(ctx, func() (any, error) { cancel(); return nil, ctx.Err() })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, gobreaker.StateHalfOpen, client.cb.State())
	_, err = client.execute(t.Context(), func() (any, error) { return nil, nil })
	require.NoError(t, err)
	require.Equal(t, gobreaker.StateClosed, client.cb.State())
}
