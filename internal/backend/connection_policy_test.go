package backend

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionPolicyRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*ConnectionConfig)
	}{
		{"negative timeout", func(c *ConnectionConfig) { c.Timeout = -time.Second }},
		{"missing secret", func(c *ConnectionConfig) { c.Secret = "" }},
		{"weak secret", func(c *ConnectionConfig) { c.Secret = "short" }},
		{"non-origin URL", func(c *ConnectionConfig) { c.BaseURL += "/path" }},
		{"URL credentials", func(c *ConnectionConfig) { c.BaseURL = "https://user:pass@backend.invalid" }},
		{"unreadable CA", func(c *ConnectionConfig) { c.TLSCAFile = "does-not-exist.pem" }},
		{"missing client key", func(c *ConnectionConfig) { c.TLSClientCertFile = "client.pem" }},
		{"missing client certificate", func(c *ConnectionConfig) { c.TLSClientKeyFile = "client.key" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := ConnectionConfig{Name: "backend-a", BaseURL: "https://backend.invalid", Secret: testIdentityClientKey}
			test.change(&cfg)
			policy, err := NewConnectionPolicy(cfg)
			require.Error(t, err)
			assert.Equal(t, ConnectionPolicy{}, policy)
		})
	}
}

func TestClientFactoriesRejectZeroConnectionPolicy(t *testing.T) {
	t.Parallel()
	bootstrap, err := NewBootstrapInventoryClient(ConnectionPolicy{}, HTTPClientOptions{})
	require.ErrorContains(t, err, "connection policy is required")
	assert.Nil(t, bootstrap)
	runtime, err := NewIdentityBoundHTTPClient(ConnectionPolicy{}, HTTPClientOptions{}, &testStorageIdentityResolver{})
	require.ErrorContains(t, err, "connection policy is required")
	assert.Nil(t, runtime)
}

func TestConnectionPolicyOwnsAndSeparatesClientTLSConfiguration(t *testing.T) {
	t.Parallel()
	cfg := ConnectionConfig{Name: "backend-a", BaseURL: "https://backend.invalid/", Secret: testIdentityClientKey, Timeout: 5 * time.Second}
	policy, err := NewConnectionPolicy(cfg)
	require.NoError(t, err)
	cfg.BaseURL = "http://another-backend.invalid"
	cfg.Timeout = time.Hour
	bootstrap, err := NewBootstrapInventoryClient(policy, HTTPClientOptions{})
	require.NoError(t, err)
	runtime, err := NewIdentityBoundHTTPClient(policy, HTTPClientOptions{}, &testStorageIdentityResolver{})
	require.NoError(t, err)
	assert.Equal(t, "https://backend.invalid", runtime.baseURL)
	assert.Equal(t, 5*time.Second, runtime.httpClient.Timeout)
	readTransport := bootstrap.(bootstrapInventoryClient).client.httpClient.Transport.(*http.Transport)
	runtimeTransport := runtime.httpClient.Transport.(*http.Transport)
	require.NotNil(t, readTransport.TLSClientConfig)
	require.NotNil(t, runtimeTransport.TLSClientConfig)
	assert.Equal(t, uint16(tls.VersionTLS13), readTransport.TLSClientConfig.MinVersion)
	assert.Equal(t, uint16(tls.VersionTLS13), runtimeTransport.TLSClientConfig.MinVersion)
	assert.Nil(t, readTransport.TLSClientConfig.RootCAs, "no override uses standard verified system roots")
	assert.False(t, readTransport.TLSClientConfig.InsecureSkipVerify)
	assert.NotSame(t, readTransport.TLSClientConfig, runtimeTransport.TLSClientConfig)
	// These internals are inaccessible to callers. Even a future internal
	// per-transport adjustment must not change the reusable connection policy.
	readTransport.TLSClientConfig.MinVersion = tls.VersionTLS12
	assert.Equal(t, uint16(tls.VersionTLS13), runtimeTransport.TLSClientConfig.MinVersion)
	assert.Equal(t, uint16(tls.VersionTLS13), policy.state.tlsConfig.MinVersion)
}

func TestConnectionPolicyRedactsCredentials(t *testing.T) {
	t.Parallel()
	policy, err := NewConnectionPolicy(ConnectionConfig{
		Name: "backend-a", BaseURL: "https://backend.invalid", Secret: testIdentityClientKey,
	})
	require.NoError(t, err)
	for _, format := range []string{"%v", "%+v", "%#v"} {
		assert.NotContains(t, fmt.Sprintf(format, policy), testIdentityClientKey)
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Info("connection", "policy", policy)
	assert.NotContains(t, output.String(), testIdentityClientKey)
}
