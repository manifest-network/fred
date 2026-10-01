package backend

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

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
	client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
			Body: io.NopCloser(strings.NewReader("circuit breaker is open"))}, nil
	})
	unknown := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.Error(t, unknown)
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, unknown))
	refused := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.ErrorIs(t, refused, ErrCircuitOpen)
	require.Equal(t, 1, calls, "open circuit cannot enter the transport")
	require.True(t, DeprovisionNotDispatched(client, testBackendStorageIDA, refused))
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, fmt.Errorf("wrapped: %w", refused)))
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, errors.Join(refused, unknown)))
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDB, refused))
	other := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "close-owner", BaseURL: "https://backend.example"})
	require.False(t, DeprovisionNotDispatched(other, testBackendStorageIDA, refused))
	for _, unproven := range []error{ErrCircuitOpen, gobreaker.ErrOpenState, errors.New(refused.Error()), nil} {
		require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, unproven))
	}
	require.False(t, DeprovisionNotDispatched(nil, testBackendStorageIDA, refused))
}

func TestDeprovisionLifecyclePendingBindsExactTransportLeaseAndEndpoint(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
		CBFailureThresh: 1,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: header,
			Body: io.NopCloser(strings.NewReader(`{"error":"pending","code":"lifecycle_pending"}`))}, nil
	})
	pending := client.Deprovision(t.Context(), testBackendStorageIDA)
	require.True(t, DeprovisionLifecyclePending(client, testBackendStorageIDA, pending))
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, pending))
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

func TestDeprovisionTransportCannotForgeLocalCircuitRefusal(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "close-owner", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, gobreaker.ErrOpenState
	})
	err = client.Deprovision(t.Context(), testBackendStorageIDA)
	require.Error(t, err)
	require.False(t, DeprovisionNotDispatched(client, testBackendStorageIDA, err),
		"the circuit callback ran, so even a matching transport sentinel is uncertain")
}
