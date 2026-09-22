package httpurl

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateAuthority(t *testing.T) {
	t.Parallel()

	for _, rawURL := range []string{
		"https://backend.example",
		"https://backend.example.",
		"https://backend.example:1",
		"https://backend.example:65535",
		"https://[2001:db8::1]:443",
		"https://[2001:db8::1%25eth0]:443",
	} {
		t.Run("accept "+rawURL, func(t *testing.T) {
			t.Parallel()
			parsed, err := url.Parse(rawURL)
			require.NoError(t, err)
			require.NoError(t, ValidateAuthority(parsed))
		})
	}

	tests := []struct {
		name   string
		rawURL string
		want   error
	}{
		{name: "missing host", rawURL: "https:///callback", want: ErrMissingHost},
		{name: "port-only host", rawURL: "https://:443/callback", want: ErrInvalidHostname},
		{name: "dot-only host", rawURL: "https://./callback", want: ErrInvalidHostname},
		{name: "multiple-dot host", rawURL: "https://.../callback", want: ErrInvalidHostname},
		{name: "Unicode dot host", rawURL: "https://。/callback", want: ErrInvalidHostname},
		{name: "Unicode IDN must use punycode", rawURL: "https://münich.example/callback", want: ErrInvalidHostname},
		{name: "empty explicit port", rawURL: "https://backend.example:/callback", want: ErrInvalidPort},
		{name: "zero port", rawURL: "https://backend.example:0/callback", want: ErrInvalidPort},
		{name: "oversized port", rawURL: "https://backend.example:65536/callback", want: ErrInvalidPort},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := url.Parse(test.rawURL)
			require.NoError(t, err)
			require.ErrorIs(t, ValidateAuthority(parsed), test.want)
		})
	}

	require.ErrorIs(t, ValidateAuthority(nil), ErrMissingHost)
}

func TestNormalizeOrigin(t *testing.T) {
	for raw, want := range map[string]string{
		"http://backend.example":       "http://backend.example",
		"https://backend.example/":     "https://backend.example",
		"https://backend.example:8443": "https://backend.example:8443",
		"https://[2001:db8::1]:443/":   "https://[2001:db8::1]:443",
	} {
		t.Run("accept "+raw, func(t *testing.T) {
			got, err := NormalizeOrigin(raw)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}

	for _, raw := range []string{
		"", "backend.example", "ftp://backend.example", "https://:443",
		"https://.", "https://backend.example:", "https://backend.example:0",
		"https://backend.example:65536", "https://user@backend.example",
		"https://backend.example/path", "https://backend.example?query=value",
		"https://backend.example?", "https://backend.example#fragment",
		"https://backend.example#", "https://backend.example/%2f",
		"https://[backend.example]", "https://[fe80::1%25]",
	} {
		t.Run("reject "+raw, func(t *testing.T) {
			got, err := NormalizeOrigin(raw)
			assert.Error(t, err)
			assert.Empty(t, got)
		})
	}
}

func TestNormalizeOriginSyntaxErrorDoesNotReturnOpaqueValue(t *testing.T) {
	const canary = "operator-secret-canary"
	_, err := NormalizeOrigin("https://backend.example/\x7f?trace=" + canary)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), canary)
}
