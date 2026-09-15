package backend

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackendRefusalRequiresCompleteExactEnvelope(t *testing.T) {
	const capacity = `{"error":"capacity","code":"insufficient_resources"}`
	const validation = `{"error":"invalid manifest","validation_code":"invalid_manifest"}`
	for _, response := range []struct {
		name   string
		status int
		body   string
	}{
		{"capacity", http.StatusServiceUnavailable, capacity},
		{"validation", http.StatusBadRequest, validation},
	} {
		for _, test := range []struct {
			name  string
			body  string
			valid bool
		}{
			{"4095 bytes", response.body + strings.Repeat(" ", 4095-len(response.body)), true},
			{"4096 bytes", response.body + strings.Repeat(" ", 4096-len(response.body)), true},
			{"4097 bytes", response.body + strings.Repeat(" ", 4097-len(response.body)), false},
			{"hidden suffix", response.body + strings.Repeat(" ", 4096-len(response.body)) + `{}`, false},
			{"second value", response.body + `{}`, false},
			{"duplicate error", strings.TrimSuffix(response.body, "}") + `,"error":"replacement"}`, false},
			{"case alias", strings.Replace(response.body, `"error"`, `"Error"`, 1), false},
			{"unknown field", strings.TrimSuffix(response.body, "}") + `,"proxy":true}`, false},
		} {
			t.Run(response.name+"/"+test.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(response.status)
					_, _ = fmt.Fprint(w, test.body)
				}))
				t.Cleanup(server.Close)
				client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "envelope", BaseURL: server.URL, Timeout: time.Second})
				outcome := InvokeProvision(t.Context(), client, ProvisionRequest{LeaseUUID: "lease"})
				if test.valid {
					require.True(t, outcome.Refused())
					return
				}
				require.ErrorIs(t, outcome.Err(), ErrMalformedErrorBody)
				require.True(t, outcome.Ambiguous(), "malformed bytes cannot grant durable refusal authority")
				require.False(t, outcome.Refused())
			})
		}
	}
}

func TestBackendErrorEnvelopeOnlyZeroBytesIsBare(t *testing.T) {
	client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "bare", BaseURL: "http://backend.invalid"})
	_, _, err := client.parseErrorCode(nil, "restore")
	require.NoError(t, err)
	for _, body := range []string{" ", "\n\t", strings.Repeat(" ", 4097)} {
		_, _, err := client.parseErrorCode([]byte(body), "restore")
		require.ErrorIs(t, err, ErrMalformedErrorBody)
	}
}
