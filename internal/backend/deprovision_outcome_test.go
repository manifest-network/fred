package backend

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
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
