package callbackurl

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBaseCanonicalizesPathAndPreservesRawQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		raw         string
		want        string
		wantEscaped string
	}{
		{
			name: "root separators",
			raw:  "https://fred.example///",
			want: "https://fred.example",
		},
		{
			name:        "raw query is byte exact",
			raw:         "https://fred.example/api///?trace=a%2fb&&flag&z=last",
			want:        "https://fred.example/api?trace=a%2fb&&flag&z=last",
			wantEscaped: "/api",
		},
		{
			name:        "empty query marker",
			raw:         "https://fred.example/api?",
			want:        "https://fred.example/api",
			wantEscaped: "/api",
		},
		{
			name:        "encoded unreserved path has one spelling",
			raw:         "https://fred.example/api%2Efred",
			want:        "https://fred.example/api.fred",
			wantEscaped: "/api.fred",
		},
		{
			name:        "unicode and lowercase escapes become canonical",
			raw:         "https://fred.example/%c3%a5",
			want:        "https://fred.example/%C3%A5",
			wantEscaped: "/%C3%A5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			base, err := ParseBase(test.raw)
			require.NoError(t, err)
			assert.Equal(t, test.want, base.String())
			assert.Equal(t, test.wantEscaped, base.EscapedPath())

			provisionURL, err := base.ProvisionURL()
			require.NoError(t, err)
			assert.Equal(t, test.wantEscaped+ProvisionPath, provisionURL.EscapedPath())
			assert.Equal(t, mustRawQuery(t, test.want), provisionURL.RawQuery)
		})
	}
}

func TestParseBaseRejectsAmbiguousOrUnusableURLs(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"/relative/callback",
		"ftp://fred.example/callback",
		"https:///callback",
		"https://:443/callback",
		"https://./callback",
		"https://fred.example:/callback",
		"https://fred.example:0/callback",
		"https://fred.example:65536/callback",
		"https://operator@fred.example/callback",
		"https:opaque",
		"https://fred.example/callback#not-sent",
		"https://fred.example/callback#",
		"https://fred.example/callback?trace=%ZZ",
		"https://fred.example/callback?trace=x;y",
		"https://fred.example/callback?trace=a b",
		"https://fred.example/callback?trace=a|b",
		"https://fred.example/callback?trace=a[b]",
		`https://fred.example/callback?trace=a\b`,
		"https://fred.example/callback?trace=" + string([]byte{0xff}),
		"https://fred.example/callback?operation%5Fid=shadowed",
		"https://fred.example/callback?lifecycle_id=shadowed",
		"https://fred.example/root/../callback",
		"https://fred.example/root//callback",
		"https://fred.example/root%2Fcallback",
		"https://fred.example/root%5ccallback",
		"https://fred.example/root%252Fcallback",
		"https://fred.example/root/%252e%252e/callback",
		"https://fred.example/root%00callback",
		`https://fred.example/root\callback`,
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			base, err := ParseBase(raw)
			require.Error(t, err)
			assert.Empty(t, base.String())
		})
	}
}

func TestParseBaseSyntaxErrorDoesNotReturnOpaqueQueryValue(t *testing.T) {
	const canary = "operator-secret-canary"
	_, err := ParseBase("https://fred.example/\x7f?trace=" + canary)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), canary)
}

func TestBaseProvisionURLPreservesAcceptedRawQueryOnHTTPRequest(t *testing.T) {
	t.Parallel()

	for _, rawQuery := range []string{
		"trace=a%20b",
		"trace=a+b",
		"trace=%ff",
		"trace=%00",
		"trace=a%7Cb&brackets=%5Bvalue%5D&slash=%5C",
		"trace=a%2fb&&flag&z=last",
	} {
		t.Run(rawQuery, func(t *testing.T) {
			t.Parallel()
			base, err := ParseBase("https://fred.example/api?" + rawQuery)
			require.NoError(t, err)
			callback, err := base.ProvisionURL()
			require.NoError(t, err)
			request, err := http.NewRequest(http.MethodPost, callback.String(), nil)
			require.NoError(t, err)
			assert.Equal(t, rawQuery, callback.RawQuery)
			assert.Equal(t, "/api"+ProvisionPath+"?"+rawQuery, request.URL.RequestURI())
		})
	}
}

func TestBaseZeroValueCannotBuild(t *testing.T) {
	t.Parallel()

	var base Base
	assert.Empty(t, base.String())
	assert.Empty(t, base.EscapedPath())
	result, err := base.ProvisionURL()
	assert.Nil(t, result)
	assert.ErrorIs(t, err, errInvalidBase)
}

func mustRawQuery(t *testing.T, rawURL string) string {
	t.Helper()
	base, err := ParseBase(rawURL)
	require.NoError(t, err)
	result, err := base.ProvisionURL()
	require.NoError(t, err)
	return result.RawQuery
}
