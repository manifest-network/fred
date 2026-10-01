package backend

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/maintenanceid"
)

func TestLifecyclePendingRequiresExactUnavailableEnvelope(t *testing.T) {
	for _, operation := range []string{"restart", "update", "deprovision"} {
		for _, tc := range []struct {
			name   string
			status int
			body   string
		}{
			{"generic500", http.StatusInternalServerError, `{"error":"admitted lifecycle work remains pending","code":"lifecycle_pending"}`},
			{"missingError", http.StatusServiceUnavailable, `{"code":"lifecycle_pending"}`},
			{"duplicateCode", http.StatusServiceUnavailable, `{"error":"pending","code":"other","code":"lifecycle_pending"}`},
			{"trailingJSON", http.StatusServiceUnavailable, `{"error":"pending","code":"lifecycle_pending"}{}`},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				id, idErr := maintenanceid.New()
				require.NoError(t, idErr)
				client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "pending-invalid", BaseURL: "http://backend.invalid", CBFailureThresh: 1})
				client.httpClient.Transport = causalOutcomeRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				})
				var err error
				if operation == "deprovision" {
					err = client.Deprovision(t.Context(), "lease")
					require.False(t, DeprovisionNotDispatched(client, "lease", err))
					require.False(t, DeprovisionLifecyclePending(client, "lease", err))
				} else {
					var outcome MaintenanceCallOutcome
					if operation == "restart" {
						outcome = InvokeRestart(t.Context(), client, RestartRequest{MaintenanceID: id})
					} else {
						outcome = InvokeUpdate(t.Context(), client, UpdateRequest{MaintenanceID: id})
					}
					require.True(t, outcome.Ambiguous())
					err = outcome.Err()
				}
				require.False(t, isLifecyclePendingResponse(err))
				require.Equal(t, gobreaker.StateOpen, client.cb.State())
			})
		}
	}
}
