package backend

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"
)

func TestBenignBackendRefusalsDoNotTripBreaker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		invoke func(*HTTPClient, context.Context) error
		want   error
	}{
		{"close deferred", http.StatusConflict, `{"error":"waiting for lifecycle work","code":"close_deferred"}`, func(c *HTTPClient, ctx context.Context) error { return c.Deprovision(ctx, "lease") }, ErrCloseDeferred},
		{"provision state", http.StatusConflict, `{"error":"invalid state","code":"invalid_state"}`, func(c *HTTPClient, ctx context.Context) error { return c.Provision(ctx, ProvisionRequest{}) }, ErrInvalidState},
		{"accounting hold", http.StatusServiceUnavailable, `{"error":"accounting incomplete","code":"insufficient_resources"}`, func(c *HTTPClient, ctx context.Context) error { _, err := c.GetLoadStats(ctx); return err }, nil},
		{"domain capacity", http.StatusServiceUnavailable, `{"error":"no capacity","code":"insufficient_resources"}`, func(c *HTTPClient, ctx context.Context) error { return c.ReconcileCustomDomain(ctx, "lease", nil) }, ErrInsufficientResources},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "refusal", BaseURL: "http://backend.invalid", CBFailureThresh: 1})
			c.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			for range 6 {
				err := tc.invoke(c, t.Context())
				if tc.name == "accounting hold" {
					require.True(t, IsReadCapacity(err))
				} else {
					require.ErrorIs(t, err, tc.want)
				}
				require.False(t, DeprovisionNotDispatched(c, "lease", err))
			}
			require.Equal(t, gobreaker.StateClosed, c.cb.State())
		})
	}
}

func TestUnknownDeprovisionConflictCannotBecomeCloseDeferral(t *testing.T) {
	c := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "unknown", BaseURL: "http://backend.invalid", CBFailureThresh: 1})
	c.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusConflict, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"conflict","code":"foreign"}`))}, nil
	})
	err := c.Deprovision(t.Context(), "lease")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCloseDeferred)
	require.False(t, DeprovisionNotDispatched(c, "lease", err))
	require.Equal(t, gobreaker.StateOpen, c.cb.State())
}
