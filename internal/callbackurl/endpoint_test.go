package callbackurl

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEndpointPreservesRequestURIBytes(t *testing.T) {
	const raw = "https://fred.example/api/callbacks/provision?trace=a%20b&opaque=%ff&operation_id=550e8400-e29b-41d4-a716-446655440000"
	endpoint, err := ParseEndpoint(raw)
	require.NoError(t, err)
	assert.Equal(t, raw, endpoint.String())
	assert.Equal(t, "/api/callbacks/provision", endpoint.EscapedPath())
	assert.Equal(t, "trace=a%20b&opaque=%ff&operation_id=550e8400-e29b-41d4-a716-446655440000", endpoint.RawQuery())

	req, err := http.NewRequest(http.MethodPost, endpoint.String(), nil)
	require.NoError(t, err)
	assert.Equal(t, "/api/callbacks/provision?"+endpoint.RawQuery(), req.URL.RequestURI())

	rewritten, err := endpoint.WithRawQuery("trace=a%20b&lifecycle_id=550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	assert.Equal(t, "https://fred.example/api/callbacks/provision?trace=a%20b&lifecycle_id=550e8400-e29b-41d4-a716-446655440000", rewritten)
}

func TestParseEndpointAcceptsCanonicalEscapedUnicodePrefix(t *testing.T) {
	const raw = "https://fred.example/%C3%A5/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	endpoint, err := ParseEndpoint(raw)
	require.NoError(t, err)
	assert.Equal(t, raw, endpoint.String())
	assert.Equal(t, "/%C3%A5/callbacks/provision", endpoint.EscapedPath())
}

func TestParseEndpointRejectsAmbiguousOrUnstableDestinations(t *testing.T) {
	tests := []string{
		"",
		"/callbacks/provision",
		"ftp://fred.example/callbacks/provision",
		"https://:443/callbacks/provision",
		"https://./callbacks/provision",
		"https://fred.example:/callbacks/provision",
		"https://fred.example:0/callbacks/provision",
		"https://fred.example:65536/callbacks/provision",
		"https://user@fred.example/callbacks/provision",
		"https://fred.example/callbacks/provision#",
		"https://fred.example/callbacks/provision#fragment",
		"https://fred.example/callbacks/provision?",
		"https://fred.example//callbacks/provision",
		"https://fred.example/./callbacks/provision",
		"https://fred.example/api/../callbacks/provision",
		"https://fred.example/callbacks/provision/",
		`https://fred.example/api\callbacks/provision`,
		"https://fred.example/api%2Fcallbacks/provision",
		"https://fred.example/api%5ccallbacks/provision",
		"https://fred.example/api%252Fcallbacks/provision",
		"https://fred.example/api/%252e%252e/callbacks/provision",
		"https://fred.example/%63allbacks/provision",
		"https://fred.example/☃/callbacks/provision",
		"https://fred.example/callbacks/provision?trace=a b",
		"https://fred.example/callbacks/provision?trace=%ZZ",
		"https://fred.example/callback",
		"https://fred.example/callbacks/provision-extra",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			endpoint, err := ParseEndpoint(raw)
			assert.Error(t, err)
			assert.Empty(t, endpoint.String())
		})
	}
}

func TestEndpointZeroValueIsInvalid(t *testing.T) {
	var endpoint Endpoint
	assert.Empty(t, endpoint.String())
	assert.Empty(t, endpoint.EscapedPath())
	assert.Empty(t, endpoint.RawQuery())
	assert.Empty(t, endpoint.Hostname())
	_, err := endpoint.WithRawQuery("trace=1")
	assert.ErrorIs(t, err, errInvalidEndpoint)
}

func TestParseEndpointSyntaxErrorDoesNotReturnBearerCapability(t *testing.T) {
	const canary = "550e8400-e29b-41d4-a716-446655440000"
	_, err := ParseEndpoint("https://fred.example/\x7f/callbacks/provision?operation_id=" + canary)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), canary)
}
