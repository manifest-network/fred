package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
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

func TestGetProvision_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/provisions/lease-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing signature")
}

func TestGetProvision_PassesAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/provisions/lease-1", nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	// Passes auth, panics on nil backend — expected.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
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

// --- Restart handler tests ---

func TestRestart_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("POST", "/restart", strings.NewReader(`{"lease_uuid":"lease-1","callback_url":"http://localhost/cb"}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing signature")
}

func TestRestart_MissingLeaseUUID(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"callback_url":"http://localhost/cb"}`)
	req := httptest.NewRequest("POST", "/restart", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "lease_uuid is required")
}

func TestRestart_MissingCallbackURL(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1"}`)
	req := httptest.NewRequest("POST", "/restart", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "callback_url is required")
}

func TestRestart_InvalidCallbackURL(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","callback_url":"ftp://invalid/cb"}`)
	req := httptest.NewRequest("POST", "/restart", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid callback_url")
}

func TestRestart_PassesValidation(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","callback_url":"http://localhost/cb"}`)
	req := httptest.NewRequest("POST", "/restart", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	// Passes validation, panics on nil backend.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
}

// --- Update handler tests ---

func TestUpdate_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("POST", "/update", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestUpdate_MissingLeaseUUID(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"callback_url":"http://localhost/cb","payload":"eyJpbWFnZSI6Im5naW54In0="}`)
	req := httptest.NewRequest("POST", "/update", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "lease_uuid is required")
}

func TestUpdate_MissingCallbackURL(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","payload":"eyJpbWFnZSI6Im5naW54In0="}`)
	req := httptest.NewRequest("POST", "/update", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "callback_url is required")
}

func TestUpdate_MissingPayload(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","callback_url":"http://localhost/cb"}`)
	req := httptest.NewRequest("POST", "/update", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "payload is required")
}

func TestUpdate_InvalidCallbackURL(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","callback_url":"ftp://invalid","payload":"eyJpbWFnZSI6Im5naW54In0="}`)
	req := httptest.NewRequest("POST", "/update", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid callback_url")
}

func TestUpdate_PassesValidation(t *testing.T) {
	handler := newTestHandler()

	body := []byte(`{"lease_uuid":"lease-1","callback_url":"http://localhost/cb","payload":"eyJpbWFnZSI6Im5naW54In0="}`)
	req := httptest.NewRequest("POST", "/update", bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, body))

	w := httptest.NewRecorder()
	// Passes validation, panics on nil backend.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
}

// --- GetReleases handler tests ---

func TestGetReleases_RequiresAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/releases/lease-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "missing signature")
}

func TestGetReleases_PassesAuth(t *testing.T) {
	handler := newTestHandler()

	req := httptest.NewRequest("GET", "/releases/lease-1", nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))

	w := httptest.NewRecorder()
	// Passes auth, panics on nil backend.
	assert.Panics(t, func() { handler.ServeHTTP(w, req) })
}

// --- mockBackend and helpers for handler logic tests ---

type mockBackend struct {
	ProvisionFunc      func(ctx context.Context, req backend.ProvisionRequest) error
	DeprovisionFunc    func(ctx context.Context, leaseUUID string) error
	GetInfoFunc        func(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error)
	GetLogsFunc        func(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)
	GetProvisionFunc   func(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	ListProvisionsFunc func(ctx context.Context) ([]backend.ProvisionInfo, error)
	RestartFunc        func(ctx context.Context, req backend.RestartRequest) error
	UpdateFunc         func(ctx context.Context, req backend.UpdateRequest) error
	GetReleasesFunc    func(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error)
	HealthFunc         func(ctx context.Context) error
	StatsFunc          func() shared.ResourceStats
}

func (m *mockBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.ProvisionFunc == nil {
		panic("mockBackend.Provision called but not configured")
	}
	return m.ProvisionFunc(ctx, req)
}

func (m *mockBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if m.DeprovisionFunc == nil {
		panic("mockBackend.Deprovision called but not configured")
	}
	return m.DeprovisionFunc(ctx, leaseUUID)
}

func (m *mockBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	if m.GetInfoFunc == nil {
		panic("mockBackend.GetInfo called but not configured")
	}
	return m.GetInfoFunc(ctx, leaseUUID)
}

func (m *mockBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	if m.GetLogsFunc == nil {
		panic("mockBackend.GetLogs called but not configured")
	}
	return m.GetLogsFunc(ctx, leaseUUID, tail)
}

func (m *mockBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	if m.GetProvisionFunc == nil {
		panic("mockBackend.GetProvision called but not configured")
	}
	return m.GetProvisionFunc(ctx, leaseUUID)
}

func (m *mockBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	if m.ListProvisionsFunc == nil {
		panic("mockBackend.ListProvisions called but not configured")
	}
	return m.ListProvisionsFunc(ctx)
}

func (m *mockBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	if m.RestartFunc == nil {
		panic("mockBackend.Restart called but not configured")
	}
	return m.RestartFunc(ctx, req)
}

func (m *mockBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	if m.UpdateFunc == nil {
		panic("mockBackend.Update called but not configured")
	}
	return m.UpdateFunc(ctx, req)
}

func (m *mockBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	if m.GetReleasesFunc == nil {
		panic("mockBackend.GetReleases called but not configured")
	}
	return m.GetReleasesFunc(ctx, leaseUUID)
}

func (m *mockBackend) Health(ctx context.Context) error {
	if m.HealthFunc == nil {
		panic("mockBackend.Health called but not configured")
	}
	return m.HealthFunc(ctx)
}

func (m *mockBackend) Stats() shared.ResourceStats {
	if m.StatsFunc == nil {
		panic("mockBackend.Stats called but not configured")
	}
	return m.StatsFunc()
}

// newMockHandler creates a Handler backed by the given mockBackend.
func newMockHandler(mb *mockBackend) http.Handler {
	s := NewServer(mb, testSecret, slog.Default())
	return s.Handler()
}

// signedPostRequest creates a POST request with a valid HMAC signature.
func signedPostRequest(path, body string) *http.Request {
	b := []byte(body)
	req := httptest.NewRequest("POST", path, bytes.NewReader(b))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, b))
	return req
}

// signedGetRequest creates a GET request with a valid HMAC signature.
func signedGetRequest(path string) *http.Request {
	req := httptest.NewRequest("GET", path, nil)
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, nil))
	return req
}

// --- Handler logic tests (using mockBackend) ---

func TestHandleProvision(t *testing.T) {
	validBody := `{"lease_uuid":"lease-1","callback_url":"http://localhost/cb","items":[{"sku":"docker-micro","quantity":1}]}`

	t.Run("success returns 202", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(_ context.Context, req backend.ProvisionRequest) error {
				assert.Equal(t, "lease-1", req.LeaseUUID)
				return nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusAccepted, w.Code)
		var resp backend.ProvisionResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "lease-1", resp.ProvisionID)
	})

	t.Run("ErrAlreadyProvisioned returns 409", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("wrap: %w", backend.ErrAlreadyProvisioned)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusConflict, w.Code)
		assert.Contains(t, w.Body.String(), "already provisioned")
	})

	t.Run("ErrUnknownSKU returns 400 with validation_code", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("bad sku: %w", backend.ErrUnknownSKU)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		assert.Equal(t, backend.ValidationCodeUnknownSKU, errResp.ValidationCode)
	})

	t.Run("ErrInvalidManifest returns 400 with validation_code", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("bad manifest: %w", backend.ErrInvalidManifest)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		assert.Equal(t, backend.ValidationCodeInvalidManifest, errResp.ValidationCode)
	})

	t.Run("ErrImageNotAllowed returns 400 with validation_code", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("blocked: %w", backend.ErrImageNotAllowed)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		assert.Equal(t, backend.ValidationCodeImageNotAllowed, errResp.ValidationCode)
	})

	t.Run("ErrInsufficientResources returns 503", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("no capacity: %w", backend.ErrInsufficientResources)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			ProvisionFunc: func(context.Context, backend.ProvisionRequest) error {
				return fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/provision", validBody))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleDeprovision(t *testing.T) {
	validBody := `{"lease_uuid":"lease-1"}`

	t.Run("success returns 200", func(t *testing.T) {
		mb := &mockBackend{
			DeprovisionFunc: func(_ context.Context, leaseUUID string) error {
				assert.Equal(t, "lease-1", leaseUUID)
				return nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/deprovision", validBody))

		assert.Equal(t, http.StatusOK, w.Code)
		var resp StatusResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "ok", resp.Status)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			DeprovisionFunc: func(context.Context, string) error {
				return fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/deprovision", validBody))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleGetInfo(t *testing.T) {
	t.Run("success returns 200 with LeaseInfo", func(t *testing.T) {
		info := &backend.LeaseInfo{"host": "example.com", "port": float64(8080)}
		mb := &mockBackend{
			GetInfoFunc: func(_ context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
				assert.Equal(t, "lease-1", leaseUUID)
				return info, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/info/lease-1"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got backend.LeaseInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, "example.com", got["host"])
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			GetInfoFunc: func(context.Context, string) (*backend.LeaseInfo, error) {
				return nil, backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/info/lease-1"))

		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Contains(t, w.Body.String(), "not provisioned")
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			GetInfoFunc: func(context.Context, string) (*backend.LeaseInfo, error) {
				return nil, fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/info/lease-1"))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleGetLogs(t *testing.T) {
	t.Run("success with default tail returns 200", func(t *testing.T) {
		mb := &mockBackend{
			GetLogsFunc: func(_ context.Context, leaseUUID string, tail int) (map[string]string, error) {
				assert.Equal(t, "lease-1", leaseUUID)
				assert.Equal(t, 100, tail) // default
				return map[string]string{"container-1": "log output"}, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/logs/lease-1"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, "log output", got["container-1"])
	})

	t.Run("success with custom tail", func(t *testing.T) {
		mb := &mockBackend{
			GetLogsFunc: func(_ context.Context, _ string, tail int) (map[string]string, error) {
				assert.Equal(t, 50, tail)
				return map[string]string{"c": "ok"}, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/logs/lease-1?tail=50"))

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			GetLogsFunc: func(context.Context, string, int) (map[string]string, error) {
				return nil, backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/logs/lease-1"))

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			GetLogsFunc: func(context.Context, string, int) (map[string]string, error) {
				return nil, fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/logs/lease-1"))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleGetProvision(t *testing.T) {
	t.Run("success returns 200 with ProvisionInfo", func(t *testing.T) {
		info := &backend.ProvisionInfo{
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
		}
		mb := &mockBackend{
			GetProvisionFunc: func(_ context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
				assert.Equal(t, "lease-1", leaseUUID)
				return info, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions/lease-1"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got backend.ProvisionInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, backend.ProvisionStatusReady, got.Status)
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			GetProvisionFunc: func(context.Context, string) (*backend.ProvisionInfo, error) {
				return nil, backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions/lease-1"))

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			GetProvisionFunc: func(context.Context, string) (*backend.ProvisionInfo, error) {
				return nil, fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions/lease-1"))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleListProvisions(t *testing.T) {
	t.Run("success with items returns 200", func(t *testing.T) {
		items := []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusProvisioning},
		}
		mb := &mockBackend{
			ListProvisionsFunc: func(context.Context) ([]backend.ProvisionInfo, error) {
				return items, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got backend.ListProvisionsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Len(t, got.Provisions, 2)
	})

	t.Run("success empty returns 200", func(t *testing.T) {
		mb := &mockBackend{
			ListProvisionsFunc: func(context.Context) ([]backend.ProvisionInfo, error) {
				return nil, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got backend.ListProvisionsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Empty(t, got.Provisions)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			ListProvisionsFunc: func(context.Context) ([]backend.ProvisionInfo, error) {
				return nil, fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/provisions"))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleRestart(t *testing.T) {
	validBody := `{"lease_uuid":"lease-1","callback_url":"http://localhost/cb"}`

	t.Run("success returns 202", func(t *testing.T) {
		mb := &mockBackend{
			RestartFunc: func(_ context.Context, req backend.RestartRequest) error {
				assert.Equal(t, "lease-1", req.LeaseUUID)
				return nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/restart", validBody))

		assert.Equal(t, http.StatusAccepted, w.Code)
		var resp StatusResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "restarting", resp.Status)
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			RestartFunc: func(context.Context, backend.RestartRequest) error {
				return backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/restart", validBody))

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("ErrInvalidState returns 409", func(t *testing.T) {
		mb := &mockBackend{
			RestartFunc: func(context.Context, backend.RestartRequest) error {
				return backend.ErrInvalidState
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/restart", validBody))

		assert.Equal(t, http.StatusConflict, w.Code)
		assert.Contains(t, w.Body.String(), "invalid state for restart")
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			RestartFunc: func(context.Context, backend.RestartRequest) error {
				return fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/restart", validBody))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleUpdate(t *testing.T) {
	validBody := `{"lease_uuid":"lease-1","callback_url":"http://localhost/cb","payload":"eyJpbWFnZSI6Im5naW54In0="}`

	t.Run("success returns 202", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(_ context.Context, req backend.UpdateRequest) error {
				assert.Equal(t, "lease-1", req.LeaseUUID)
				return nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusAccepted, w.Code)
		var resp StatusResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "updating", resp.Status)
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(context.Context, backend.UpdateRequest) error {
				return backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("ErrInvalidState returns 409", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(context.Context, backend.UpdateRequest) error {
				return backend.ErrInvalidState
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusConflict, w.Code)
		assert.Contains(t, w.Body.String(), "invalid state for update")
	})

	t.Run("ErrUnknownSKU returns 400 with validation_code", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(context.Context, backend.UpdateRequest) error {
				return fmt.Errorf("bad: %w", backend.ErrUnknownSKU)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		assert.Equal(t, backend.ValidationCodeUnknownSKU, errResp.ValidationCode)
	})

	t.Run("ErrInvalidManifest returns 400 with validation_code", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(context.Context, backend.UpdateRequest) error {
				return fmt.Errorf("bad: %w", backend.ErrInvalidManifest)
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
		assert.Equal(t, backend.ValidationCodeInvalidManifest, errResp.ValidationCode)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			UpdateFunc: func(context.Context, backend.UpdateRequest) error {
				return fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedPostRequest("/update", validBody))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleGetReleases(t *testing.T) {
	t.Run("success with releases returns 200", func(t *testing.T) {
		releases := []backend.ReleaseInfo{
			{Version: 1, Image: "nginx:1.0", Status: "ready"},
			{Version: 2, Image: "nginx:2.0", Status: "ready"},
		}
		mb := &mockBackend{
			GetReleasesFunc: func(_ context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
				assert.Equal(t, "lease-1", leaseUUID)
				return releases, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/releases/lease-1"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got []backend.ReleaseInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Len(t, got, 2)
		assert.Equal(t, "nginx:2.0", got[1].Image)
	})

	t.Run("success empty returns 200", func(t *testing.T) {
		mb := &mockBackend{
			GetReleasesFunc: func(context.Context, string) ([]backend.ReleaseInfo, error) {
				return []backend.ReleaseInfo{}, nil
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/releases/lease-1"))

		assert.Equal(t, http.StatusOK, w.Code)
		var got []backend.ReleaseInfo
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Empty(t, got)
	})

	t.Run("ErrNotProvisioned returns 404", func(t *testing.T) {
		mb := &mockBackend{
			GetReleasesFunc: func(context.Context, string) ([]backend.ReleaseInfo, error) {
				return nil, backend.ErrNotProvisioned
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/releases/lease-1"))

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("generic error returns 500", func(t *testing.T) {
		mb := &mockBackend{
			GetReleasesFunc: func(context.Context, string) ([]backend.ReleaseInfo, error) {
				return nil, fmt.Errorf("boom")
			},
		}
		w := httptest.NewRecorder()
		newMockHandler(mb).ServeHTTP(w, signedGetRequest("/releases/lease-1"))

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandleHealth(t *testing.T) {
	t.Run("healthy returns 200", func(t *testing.T) {
		mb := &mockBackend{
			HealthFunc: func(context.Context) error { return nil },
		}
		handler := newMockHandler(mb)

		// No auth required for /health
		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp StatusResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, "healthy", resp.Status)
	})

	t.Run("unhealthy returns 503", func(t *testing.T) {
		mb := &mockBackend{
			HealthFunc: func(context.Context) error { return fmt.Errorf("docker down") },
		}
		handler := newMockHandler(mb)

		req := httptest.NewRequest("GET", "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "docker down")
	})
}

func TestHandleStats(t *testing.T) {
	t.Run("returns computed stats", func(t *testing.T) {
		mb := &mockBackend{
			StatsFunc: func() shared.ResourceStats {
				return shared.ResourceStats{
					TotalCPU:          8.0,
					TotalMemoryMB:     16384,
					TotalDiskMB:       102400,
					AllocatedCPU:      3.0,
					AllocatedMemoryMB: 4096,
					AllocatedDiskMB:   20480,
					AllocationCount:   2,
				}
			},
		}
		handler := newMockHandler(mb)

		// No auth required for /stats
		req := httptest.NewRequest("GET", "/stats", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var got StatsResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

		assert.InDelta(t, 8.0, got.TotalCPUCores, 0.001)
		assert.Equal(t, int64(16384), got.TotalMemoryMB)
		assert.Equal(t, int64(102400), got.TotalDiskMB)
		assert.InDelta(t, 3.0, got.AllocatedCPUCores, 0.001)
		assert.Equal(t, int64(4096), got.AllocatedMemoryMB)
		assert.Equal(t, int64(20480), got.AllocatedDiskMB)

		// Verify Available* fields are computed (Total - Allocated)
		assert.InDelta(t, 5.0, got.AvailableCPUCores, 0.001)
		assert.Equal(t, int64(12288), got.AvailableMemoryMB)
		assert.Equal(t, int64(81920), got.AvailableDiskMB)

		assert.Equal(t, 2, got.ActiveContainers)
	})
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
