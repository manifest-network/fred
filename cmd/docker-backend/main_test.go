package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/hmacauth"
)

const testSecret = "test-secret-that-is-at-least-32-chars!"

func TestVerifySignature(t *testing.T) {
	body := []byte(`{"lease_uuid":"abc-123"}`)

	t.Run("valid signature", func(t *testing.T) {
		sig := hmacauth.Sign(testSecret, body)
		assert.NoError(t, hmacauth.Verify(testSecret, body, sig, 5*time.Minute))
	})

	t.Run("expired timestamp", func(t *testing.T) {
		sig := hmacauth.SignWithTime(testSecret, body, time.Now().Add(-10*time.Minute))
		assert.Error(t, hmacauth.Verify(testSecret, body, sig, 5*time.Minute))
	})

	t.Run("wrong secret", func(t *testing.T) {
		sig := hmacauth.Sign("wrong-secret-wrong-secret-wrong!", body)
		assert.Error(t, hmacauth.Verify(testSecret, body, sig, 5*time.Minute))
	})

	t.Run("tampered body", func(t *testing.T) {
		sig := hmacauth.Sign(testSecret, body)
		assert.Error(t, hmacauth.Verify(testSecret, []byte(`{"lease_uuid":"TAMPERED"}`), sig, 5*time.Minute))
	})

	t.Run("malformed signature missing sha256", func(t *testing.T) {
		sig := "t=1234567890"
		assert.Error(t, hmacauth.Verify(testSecret, body, sig, 5*time.Minute))
	})

	t.Run("malformed signature missing timestamp", func(t *testing.T) {
		assert.Error(t, hmacauth.Verify(testSecret, body, "sha256=abc123", 5*time.Minute))
	})
}

// newTestHandler creates a Handler backed by a nil docker.Backend.
//
// This is intentional: tests for middleware (HMAC auth) and parameter validation
// (tail limits) exercise code paths that reject requests before calling backend
// methods. For tests where validation passes, use assert.Panics to verify the
// request reached the backend (which will panic on nil dereference).
//
// This pattern avoids the complexity of mocking a full docker.Backend for tests
// that don't need it. If a handler change causes unexpected panics, it means
// the change touched the backend in a path that previously didn't.
func newTestHandler() http.Handler {
	s := NewServer(nil, testSecret, slog.Default())
	return s.Handler()
}

func TestGetLogs_TailExceedsMax(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", fmt.Sprintf("/logs/lease-1?tail=%d", maxTailLines+1), nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "must not exceed")
}

func TestGetLogs_TailAtMax(t *testing.T) {
	// Tail exactly at the limit should pass validation. The handler will then
	// call s.backend.GetLogs which panics with a nil backend, so we recover.
	handler := newTestHandler()

	req := httptest.NewRequest("GET", fmt.Sprintf("/logs/lease-1?tail=%d", maxTailLines), nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	assert.Panics(t, func() {
		handler.ServeHTTP(w, req)
	}, "expected panic from nil backend after passing validation")
}

func TestGetLogs_TailNegative(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/logs/lease-1?tail=-5", nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "positive integer")
}

func TestGetLogs_TailDefault(t *testing.T) {
	// No tail parameter should pass validation (defaults to 100) and reach the backend.
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/logs/lease-1", nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	assert.Panics(t, func() {
		handler.ServeHTTP(w, req)
	}, "expected panic from nil backend after passing validation")
}

// --- HMAC auth on GET endpoints ---

func TestGetInfo_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	t.Run("missing signature returns 401", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/info/lease-1", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Contains(t, w.Body.String(), "missing signature")
	})

	t.Run("wrong secret returns 401", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/info/lease-1", nil)
		req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign("wrong-secret-wrong-secret-wrong!", nil))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Contains(t, w.Body.String(), "invalid signature")
	})

	t.Run("valid signature passes auth", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/info/lease-1", nil)
		req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))
		w := httptest.NewRecorder()
		// Passes auth, panics on nil backend — that's expected.
		assert.Panics(t, func() { handler.ServeHTTP(w, req) })
	})
}

func TestGetLogs_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/logs/lease-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing signature")
}

func TestListProvisions_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/provisions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing signature")
}

func TestHealth_NoAuthRequired(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	// Passes without auth, panics on nil backend.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
}

func TestStats_NoAuthRequired(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	// Passes without auth, panics on nil backend.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
}

func TestValidateCallbackURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		// Valid URLs
		{name: "valid https", url: "https://example.com/callback", wantErr: ""},
		{name: "valid http", url: "http://example.com/callback", wantErr: ""},
		{name: "valid with port", url: "https://example.com:8443/callback", wantErr: ""},
		{name: "valid public IP", url: "https://203.0.113.50/callback", wantErr: ""},

		// Localhost is allowed (common in development, and callback URL comes from trusted Fred)
		{name: "localhost", url: "http://localhost/callback", wantErr: ""},
		{name: "localhost https", url: "https://localhost:8080/callback", wantErr: ""},
		{name: "127.0.0.1", url: "http://127.0.0.1/callback", wantErr: ""},
		{name: "::1", url: "http://[::1]/callback", wantErr: ""},

		// Private networks are allowed (backends often run on private networks)
		{name: "private 10.x", url: "http://10.0.0.1/callback", wantErr: ""},
		{name: "private 172.16.x", url: "http://172.16.0.1/callback", wantErr: ""},
		{name: "private 192.168.x", url: "http://192.168.1.1/callback", wantErr: ""},

		// Invalid schemes
		{name: "file scheme", url: "file:///etc/passwd", wantErr: "scheme must be http or https"},
		{name: "ftp scheme", url: "ftp://example.com/file", wantErr: "scheme must be http or https"},
		{name: "no scheme", url: "example.com/callback", wantErr: "scheme must be http or https"},

		// Cloud metadata endpoints are blocked (SSRF risk)
		{name: "AWS metadata", url: "http://169.254.169.254/latest/meta-data/", wantErr: "link-local addresses are not allowed"},
		{name: "link-local", url: "http://169.254.1.1/callback", wantErr: "link-local addresses are not allowed"},

		// Malformed
		{name: "empty host", url: "http:///callback", wantErr: "host is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCallbackURL(tt.url)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
