package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCORSTestServer builds a Server with the given allowed origins.
func newCORSTestServer(t *testing.T, addr string, origins []string) *Server {
	t.Helper()

	s, err := NewServer(
		ServerConfig{
			Addr:           addr,
			ProviderUUID:   "01234567-89ab-cdef-0123-456789abcdef",
			Bech32Prefix:   "manifest",
			RateLimitRPS:   100,
			RateLimitBurst: 200,
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
			IdleTimeout:    30 * time.Second,
			RequestTimeout: 5 * time.Second,
			CORSOrigins:    origins,
		},
		ServerDeps{
			ChainClient:       &mockChainClient{},
			CallbackPublisher: &mockCallbackPublisher{},
			StatusChecker:     &mockStatusChecker{},
		},
	)
	require.NoError(t, err)
	return s
}

func TestCORS_PreflightAllowed(t *testing.T) {
	const allowedOrigin = "https://admin.example.com"
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{allowedOrigin})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodOptions, fmt.Sprintf("http://%s/workloads", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", allowedOrigin)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Preflight responses are 2xx (rs/cors uses 204) when the origin matches.
	assert.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"preflight should succeed with 2xx, got %d", resp.StatusCode)
	assert.Equal(t, allowedOrigin, resp.Header.Get("Access-Control-Allow-Origin"),
		"allowed origin should be echoed back")
}

func TestCORS_PreflightAllowsAuthorizationHeader(t *testing.T) {
	// Browser clients calling /v1/leases/* routes need to send an Authorization
	// header (Bearer tokens). The CORS preflight must echo Authorization in
	// Access-Control-Allow-Headers or the browser blocks the real request.
	const allowedOrigin = "https://admin.example.com"
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{allowedOrigin})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodOptions,
		fmt.Sprintf("http://%s/v1/leases/01234567-89ab-cdef-0123-456789abcdef/connection", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", allowedOrigin)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	// Real browsers send Access-Control-Request-Headers values in lowercase
	// per the Fetch spec; rs/cors enforces this strictly. See cors.go:192.
	req.Header.Set("Access-Control-Request-Headers", "authorization")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"preflight should succeed with 2xx, got %d", resp.StatusCode)
	allowedHeaders := resp.Header.Get("Access-Control-Allow-Headers")
	assert.Contains(t, strings.ToLower(allowedHeaders), "authorization",
		"Authorization must appear in Access-Control-Allow-Headers")
}

func TestCORS_WildcardPreflightAllowed(t *testing.T) {
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{"*"})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodOptions,
		fmt.Sprintf("http://%s/v1/leases/01234567-89ab-cdef-0123-456789abcdef/connection", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://anywhere.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	req.Header.Set("Access-Control-Request-Headers", "authorization")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"wildcard preflight should succeed with 2xx, got %d", resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"),
		"wildcard origin should be returned for preflight")
	assert.Contains(t, strings.ToLower(resp.Header.Get("Access-Control-Allow-Headers")), "authorization",
		"Authorization must appear in Access-Control-Allow-Headers")
}

func TestCORS_PreflightBlockedOnDisallowedOrigin(t *testing.T) {
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{"https://admin.example.com"})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodOptions, fmt.Sprintf("http://%s/workloads", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://attacker.example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// rs/cors does not echo the disallowed origin in the response header.
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"),
		"disallowed origin must not appear in Access-Control-Allow-Origin")
}

func TestCORS_WildcardAllowsAnyOrigin(t *testing.T) {
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{"*"})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/health", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://anywhere.example.com")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"),
		"wildcard origin should allow any request")
}

func TestCORS_DisabledWhenOriginsEmpty(t *testing.T) {
	addr := freePort(t)
	s := newCORSTestServer(t, addr, nil)
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	// Passing nil (or an empty list) opts out of CORS entirely.
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/health", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://anywhere.example.com")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"),
		"CORS headers must be absent when cors_origins is empty")
}

func TestCORS_RealRequestStillCarriesSecurityHeaders(t *testing.T) {
	const allowedOrigin = "https://admin.example.com"
	addr := freePort(t)
	s := newCORSTestServer(t, addr, []string{allowedOrigin})
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/health", addr), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", allowedOrigin)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// CORS headers
	assert.Equal(t, allowedOrigin, resp.Header.Get("Access-Control-Allow-Origin"))
	// securityHeadersMiddleware still runs after CORS for real requests
	assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
}
