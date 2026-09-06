package hmacauth

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifiedRequestFormattingRedactsAuthenticatedEnvelope(t *testing.T) {
	const (
		secret = "format-test-secret-that-is-long-enough"
		uri    = "/callbacks/provision?operation_id=2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a"
		route  = "550e8400-e29b-41d4-a716-446655440000"
	)
	body := []byte(`{"lease_uuid":"sensitive-lease","status":"success"}`)
	now := time.Unix(1_700_000_000, 0)
	verifier, _ := NewCallbackProofBoundary()
	proof, err := verifier.VerifyRoutedWithTime(
		secret, http.MethodPost, uri, body,
		SignWithTime(secret, http.MethodPost, uri, body, now),
		route, "/callbacks/provision", time.Minute, time.Minute, now,
	)
	require.NoError(t, err)

	output := formattedAndLoggedValue(proof)
	assert.Contains(t, output, verifiedRequestDiagnostic)
	for _, sensitive := range []string{uri, route, string(body), "sensitive-lease"} {
		assert.NotContains(t, output, sensitive)
	}
}

func formattedAndLoggedValue(value any) string {
	var output strings.Builder
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X"} {
		output.WriteString(fmt.Sprintf(format, value))
		output.WriteByte('\n')
	}
	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("capability", "value", value)
	output.WriteString(logged.String())
	return output.String()
}
