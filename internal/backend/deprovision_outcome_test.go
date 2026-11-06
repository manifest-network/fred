package backend

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func TestDeprovisionNotDispatchedBindsCircuitRefusalToExactRequest(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
		CBFailureThresh: 1, CBTimeout: time.Hour,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	calls := 0
	client.wire.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
			Body: io.NopCloser(strings.NewReader("circuit breaker is open"))}, nil
	})
	unknown := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.Error(t, unknown)
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, unknown))
	refused := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.ErrorIs(t, refused, ErrCircuitOpen)
	require.Equal(t, 1, calls, "open circuit cannot enter the transport")
	require.Equal(t, DeprovisionRefusedCircuitOpen, DeprovisionRefusalOf(client, testBackendStorageIDA, refused))
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, fmt.Errorf("wrapped: %w", refused)))
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, errors.Join(refused, unknown)))
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDB, refused))
	other := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "close-owner", BaseURL: "https://backend.example"})
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(other, testBackendStorageIDA, refused))
	for _, unproven := range []error{ErrCircuitOpen, gobreaker.ErrOpenState, errors.New(refused.Error()), nil} {
		require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, unproven))
	}
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(nil, testBackendStorageIDA, refused))
}

func TestDeprovisionLifecyclePendingBindsExactTransportLeaseAndEndpoint(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
		CBFailureThresh: 1,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.wire.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: header,
			Body: io.NopCloser(strings.NewReader(`{"error":"pending","code":"lifecycle_pending"}`))}, nil
	})
	pending := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.True(t, DeprovisionLifecyclePending(client, testBackendStorageIDA, pending))
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, pending))
	require.Equal(t, gobreaker.StateClosed, client.cb.State())
	other := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "close-owner", BaseURL: "https://backend.example"})
	require.False(t, DeprovisionLifecyclePending(other, testBackendStorageIDA, pending))
	require.False(t, DeprovisionLifecyclePending(client, testBackendStorageIDB, pending))
	require.False(t, DeprovisionLifecyclePending(nil, testBackendStorageIDA, pending))
	for _, unrelated := range []error{
		nil, errors.New(pending.Error()), &lifecyclePendingResponse{},
		fmt.Errorf("wrapped: %w", pending), errors.Join(pending, errors.New("unknown transport effect")),
	} {
		require.False(t, DeprovisionLifecyclePending(client, testBackendStorageIDA, unrelated))
	}
	id, err := maintenanceid.New()
	require.NoError(t, err)
	restart := InvokeRestart(t.Context(), client, RestartRequest{LeaseUUID: testBackendStorageIDA, MaintenanceID: id})
	require.True(t, isLifecyclePendingResponse(restart.Err()))
	require.False(t, DeprovisionLifecyclePending(client, testBackendStorageIDA, restart.Err()),
		"another endpoint cannot authorize deferral of an unobserved close")
}

// A lifecycle_pending answer is counted as pending, not as an error: a close
// that outlasts the backend's response budget is still running, and the
// deploy's backend-failure alert must not read it as a failed request.
func TestLifecyclePendingAnswerIsCountedAsPending(t *testing.T) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_pending_requests_total", Help: "test",
	}, []string{"backend", "operation", "status"})
	policy, err := NewConnectionPolicy(ConnectionConfig{Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey})
	require.NoError(t, err)
	client, err := NewIdentityBoundHTTPClient(policy, HTTPClientOptions{RequestCounters: vecRequestCounters(requests, nil), CBFailureThresh: 1},
		&testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.wire.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: header,
			Body: io.NopCloser(strings.NewReader(`{"error":"pending","code":"lifecycle_pending"}`))}, nil
	})
	require.True(t, DeprovisionLifecyclePending(client, testBackendStorageIDA, client.Deprovision(t.Context(), testBackendStorageIDA)))
	require.InDelta(t, 1.0, promtestutil.ToFloat64(requests.WithLabelValues("close-owner", "deprovision", "pending")), 0)
	require.InDelta(t, 0.0, promtestutil.ToFloat64(requests.WithLabelValues("close-owner", "deprovision", "error")), 0)
}

// /deprovision always outlasts the backend's answer budget, whatever timeout
// the connection was configured with, so a close still running gets its
// lifecycle_pending answer delivered instead of a client timeout.
func TestDeprovisionRequestTimeoutCoversTheBackendsAnswerBudget(t *testing.T) {
	for _, configured := range []time.Duration{0, time.Second, DeprovisionResponseBudget, time.Hour} {
		policy, err := NewConnectionPolicy(ConnectionConfig{
			Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey, Timeout: configured,
		})
		require.NoError(t, err)
		client, err := NewIdentityBoundHTTPClient(policy, HTTPClientOptions{},
			&testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
		require.NoError(t, err)
		deprovision := client.wire.deprovisionClient()
		want := max(cmp.Or(configured, defaultRequestTimeout), minDeprovisionRequestTimeout)
		require.Equal(t, want, deprovision.Timeout, "configured %s", configured)
		require.Greater(t, deprovision.Timeout, DeprovisionResponseBudget)
		require.Equal(t, cmp.Or(configured, defaultRequestTimeout), client.wire.httpClient.Timeout,
			"other requests keep the configured timeout")
		require.Same(t, client.wire.httpClient.Transport, deprovision.Transport)
	}
}

func TestDeprovisionTransportCannotForgeLocalCircuitRefusal(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.wire.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, gobreaker.ErrOpenState
	})
	err = client.Deprovision(t.Context(), testBackendStorageIDA)
	require.Error(t, err)
	require.Equal(t, DeprovisionRefusalUnproven, DeprovisionRefusalOf(client, testBackendStorageIDA, err),
		"the circuit callback ran, so even a matching transport sentinel is uncertain")
}
