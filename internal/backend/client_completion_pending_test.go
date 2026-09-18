package backend

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestOperationCompletionPendingIsAmbiguousWithoutTrippingBreaker(t *testing.T) {
	for _, operation := range []string{"provision", "restore"} {
		t.Run(operation, func(t *testing.T) {
			client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
				Name: "pending-completion", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
				CBFailureThresh: 1, CBTimeout: time.Hour,
			}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
			require.NoError(t, err)
			calls := 0
			client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				header := make(http.Header)
				header.Set(backendidentity.ResponseHeader, testBackendStorageIDA)
				return &http.Response{StatusCode: http.StatusConflict, Header: header,
					Body: io.NopCloser(strings.NewReader(`{"error":"an earlier operation completion is pending","code":"operation_completion_pending"}`))}, nil
			})
			for range 6 {
				var callErr error
				if operation == "provision" {
					outcome := InvokeProvision(t.Context(), client, ProvisionRequest{LeaseUUID: testBackendStorageIDB})
					require.True(t, outcome.Ambiguous())
					require.False(t, outcome.Refused())
					require.False(t, outcome.NotDispatched())
					callErr = outcome.Err()
				} else {
					outcome := InvokeRestore(t.Context(), client, RestoreRequest{LeaseUUID: testBackendStorageIDB})
					require.True(t, outcome.Ambiguous())
					require.False(t, outcome.Refused())
					require.False(t, outcome.NotDispatched())
					callErr = outcome.Err()
				}
				require.True(t, isOperationCompletionPendingResponse(callErr))
				for _, sentinel := range []error{ErrAlreadyProvisioned, ErrInvalidState, ErrCapacityRefused, ErrValidation} {
					require.False(t, errors.Is(callErr, sentinel), "availability does not grant settlement: %v", sentinel)
				}
			}
			require.Equal(t, 6, calls)
			require.Equal(t, gobreaker.StateClosed, client.cb.State())
			require.EqualValues(t, 6, client.cb.Counts().TotalSuccesses)
		})
	}
}

func TestMalformedCompletionPendingAndGenericFailureStillTripBreaker(t *testing.T) {
	for _, operation := range []string{"provision", "restore"} {
		for _, tc := range []struct {
			name   string
			status int
			body   string
		}{
			{"generic500", http.StatusInternalServerError, `{"error":"an earlier operation completion is pending"}`},
			{"missingError", http.StatusConflict, `{"code":"operation_completion_pending"}`},
			{"duplicateCode", http.StatusConflict, `{"error":"busy","code":"other","code":"operation_completion_pending"}`},
			{"trailingJSON", http.StatusConflict, `{"error":"busy","code":"operation_completion_pending"}{}`},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				client := newUnboundHTTPClientForTest(HTTPClientConfig{
					Name: "failure", BaseURL: "http://backend.example", CBFailureThresh: 1,
				})
				client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				})
				if operation == "provision" {
					outcome := InvokeProvision(t.Context(), client, ProvisionRequest{})
					require.True(t, outcome.Ambiguous())
					require.False(t, isOperationCompletionPendingResponse(outcome.Err()))
				} else {
					outcome := InvokeRestore(t.Context(), client, RestoreRequest{})
					require.True(t, outcome.Ambiguous())
					require.False(t, isOperationCompletionPendingResponse(outcome.Err()))
				}
				require.Equal(t, gobreaker.StateOpen, client.cb.State())
			})
		}
	}
}
