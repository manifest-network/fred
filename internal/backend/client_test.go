package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestHTTPClient_Provision(t *testing.T) {
	// Create test server
	var receivedReq ProvisionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/provision" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			t.Errorf("failed to decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(ProvisionResponse{ProvisionID: "test-123"})
	}))
	defer server.Close()

	// Create client
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	// Test provision
	err := client.Provision(context.Background(), ProvisionRequest{
		LeaseUUID:    "lease-uuid-1",
		Tenant:       "tenant-1",
		ProviderUUID: "provider-1",
		Items:        []LeaseItem{{SKU: "gpu-a100", Quantity: 1}},
		CallbackURL:  "http://fred/callback",
	})

	require.NoError(t, err)

	assert.Equal(t, "lease-uuid-1", receivedReq.LeaseUUID)
	assert.Equal(t, "gpu-a100", receivedReq.RoutingSKU())
}

// TestHTTPClient_Provision_ValidationError verifies that 400 Bad Request responses
// with a validation_code return the correct sub-category sentinel.
func TestHTTPClient_Provision_ValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":           "validation error: unknown SKU: invalid-sku",
			"validation_code": "unknown_sku",
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-validation",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Provision(context.Background(), ProvisionRequest{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-1",
		ProviderUUID: "provider-1",
		Items:        []LeaseItem{{SKU: "invalid-sku", Quantity: 1}},
		CallbackURL:  "http://fred/callback",
	})

	assert.ErrorIs(t, err, ErrValidation)
	assert.ErrorIs(t, err, ErrUnknownSKU)
	assert.Contains(t, err.Error(), "unknown SKU")
}

// TestHTTPClient_Provision_ValidationErrorCodes verifies that validation_code
// in 400 responses maps to the correct sentinel error, with backwards-compatible
// fallback for missing or unknown codes.
func TestHTTPClient_Provision_ValidationErrorCodes(t *testing.T) {
	tests := []struct {
		name         string
		responseBody string
		wantSentinel error
		wantMessage  string
	}{
		{
			"unknown SKU code",
			`{"error":"validation error: unknown SKU: bad-sku","validation_code":"unknown_sku"}`,
			ErrUnknownSKU,
			"unknown SKU: bad-sku",
		},
		{
			"invalid manifest code",
			`{"error":"validation error: invalid manifest: bad json","validation_code":"invalid_manifest"}`,
			ErrInvalidManifest,
			"invalid manifest: bad json",
		},
		{
			"image not allowed code",
			`{"error":"validation error: image not allowed: evil.io","validation_code":"image_not_allowed"}`,
			ErrImageNotAllowed,
			"image not allowed: evil.io",
		},
		{
			"missing code (backwards compat)",
			`{"error":"some validation error"}`,
			ErrValidation,
			"some validation error",
		},
		{
			"unknown code",
			`{"error":"something new","validation_code":"future_code"}`,
			ErrValidation,
			"something new",
		},
		{
			"non-JSON body (backwards compat)",
			`plain text error`,
			ErrValidation,
			"plain text error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(tt.responseBody))
			}))
			defer server.Close()

			client := NewHTTPClient(HTTPClientConfig{
				Name:    "test-codes",
				BaseURL: server.URL,
				Timeout: 5 * time.Second,
			})

			err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
			assert.ErrorIs(t, err, tt.wantSentinel)
			assert.ErrorIs(t, err, ErrValidation, "all validation errors should match ErrValidation")
			assert.Contains(t, err.Error(), tt.wantMessage)
		})
	}
}

// TestHTTPClient_Provision_ValidationError_DoesNotTripCircuitBreaker verifies that
// 400 errors don't trip the circuit breaker (they're permanent client errors, not transient backend failures).
func TestHTTPClient_Provision_ValidationError_DoesNotTripCircuitBreaker(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		http.Error(w, `{"error":"validation error"}`, http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-validation-cb",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
	})

	// Make 10 requests that all return 400 (ErrValidation)
	for i := range 10 {
		err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
		assert.ErrorIs(t, err, ErrValidation, "Request %d should return ErrValidation", i+1)
		assert.NotErrorIs(t, err, ErrCircuitOpen, "Circuit should not have opened at request %d", i+1)
	}

	// All 10 requests should have reached the server
	assert.Equal(t, 10, requestCount)
}

func TestHTTPClient_GetInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if r.URL.Path == "/info/found-uuid" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(LeaseInfo{
				Host:     "test.example.com",
				Protocol: "https",
			})
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	// Test found
	info, err := client.GetInfo(context.Background(), "found-uuid")
	require.NoError(t, err)
	assert.Equal(t, "test.example.com", info.Host)

	// Test not found
	_, err = client.GetInfo(context.Background(), "not-found-uuid")
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

func TestHTTPClient_Deprovision(t *testing.T) {
	var receivedUUID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/deprovision" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		var req map[string]string
		json.NewDecoder(r.Body).Decode(&req)
		receivedUUID = req["lease_uuid"]

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	err := client.Deprovision(context.Background(), "lease-to-delete")
	require.NoError(t, err)
	assert.Equal(t, "lease-to-delete", receivedUUID)
}

func TestHTTPClient_ListProvisions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/provisions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"provisions": []ProvisionInfo{
				{LeaseUUID: "uuid-1", Status: ProvisionStatusReady},
				{LeaseUUID: "uuid-2", Status: ProvisionStatusProvisioning},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	provisions, err := client.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, provisions, 2)
}

func TestHTTPClient_Name(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "my-backend",
		BaseURL: "http://example.com",
	})

	assert.Equal(t, "my-backend", client.Name())
}

func TestHTTPClient_CircuitBreaker(t *testing.T) {
	failCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failCount++
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	// Configure circuit breaker to trip after 3 failures with a short timeout
	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-cb",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
		CBTimeout:       100 * time.Millisecond, // Short timeout for testing
	})

	// First 3 calls should go through to server (and fail)
	for range 3 {
		err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, ErrCircuitOpen)
	}

	// Verify server received all 3 requests
	assert.Equal(t, 3, failCount)

	// 4th call should fail immediately with circuit open error
	err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	assert.ErrorIs(t, err, ErrCircuitOpen)

	// Server should not receive the 4th request (circuit is open)
	assert.Equal(t, 3, failCount)

	// Wait for circuit breaker timeout to transition to half-open
	time.Sleep(150 * time.Millisecond)

	// Next call should go through (half-open state) but still fail
	err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	assert.NotErrorIs(t, err, ErrCircuitOpen)
	assert.Equal(t, 4, failCount)
}

func TestHTTPClient_CircuitBreaker_Recovery(t *testing.T) {
	callCount := 0
	shouldFail := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if shouldFail {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	// Configure circuit breaker to trip after 2 failures
	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-cb-recovery",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 2,
		CBTimeout:       100 * time.Millisecond,
	})

	// Fail twice to trip the breaker
	for range 2 {
		_ = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	}

	// Verify circuit is open
	err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	assert.ErrorIs(t, err, ErrCircuitOpen)

	// Wait for half-open
	time.Sleep(150 * time.Millisecond)

	// Now make the server healthy
	shouldFail = false

	// This call should succeed and close the circuit
	err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	assert.NoError(t, err)

	// Verify circuit is closed by making more calls
	for range 3 {
		err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
		assert.NoError(t, err)
	}
}

func TestHTTPClient_Health_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("Expected /health path, got %s", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-health",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Health(context.Background())
	assert.NoError(t, err)
}

func TestHTTPClient_Health_Unhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-health-unhealthy",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Health(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "503")
}

func TestHTTPClient_Health_ConnectionError(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-health-conn-err",
		BaseURL: "http://localhost:59999", // Unlikely to be in use
		Timeout: 100 * time.Millisecond,
	})

	err := client.Health(context.Background())
	assert.Error(t, err)
}

func TestHTTPClient_Health_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-health-ctx",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := client.Health(ctx)
	assert.Error(t, err)
}

// TestHTTPClient_CircuitBreaker_NotProvisioned_DoesNotTrip verifies that 404 responses
// from GetInfo (ErrNotProvisioned) do not count toward the circuit breaker failure threshold.
// This is important because "lease not provisioned" is a valid response, not a backend failure.
func TestHTTPClient_CircuitBreaker_NotProvisioned_DoesNotTrip(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Always return 404 - lease not provisioned
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Configure circuit breaker to trip after 3 failures
	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-cb-404",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
		CBTimeout:       100 * time.Millisecond,
	})

	// Make 10 requests that all return 404 (ErrNotProvisioned)
	for i := range 10 {
		_, err := client.GetInfo(context.Background(), "nonexistent-lease")
		assert.ErrorIs(t, err, ErrNotProvisioned, "GetInfo() call %d", i+1)
	}

	// All 10 requests should have reached the server (circuit never opened)
	assert.Equal(t, 10, requestCount, "Server should have received 10 requests (circuit should not have opened)")

	// Verify circuit is still closed by making another request
	_, err := client.GetInfo(context.Background(), "another-lease")
	assert.NotErrorIs(t, err, ErrCircuitOpen)
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

// TestHTTPClient_CircuitBreaker_404sResetFailureCount verifies that 404 responses
// reset the consecutive failure count (since they're valid responses).
// This means intermixed 404s and 500s won't trip the breaker unless there are
// enough consecutive 500s.
func TestHTTPClient_CircuitBreaker_404sResetFailureCount(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Alternate: 404, 500, 404, 500... - never 3 consecutive 500s
		if requestCount%2 == 0 {
			http.Error(w, "server error", http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-cb-mixed",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
		CBTimeout:       100 * time.Millisecond,
	})

	// Make 10 requests - circuit should NOT open because 404s reset consecutive failure count
	for i := range 10 {
		_, err := client.GetInfo(context.Background(), "test-lease")
		assert.NotErrorIs(t, err, ErrCircuitOpen, "Circuit should not have opened at request %d - 404s should reset failure count", i+1)
	}

	// All 10 requests should have gone through
	assert.Equal(t, 10, requestCount)
}

// TestHTTPClient_CircuitBreaker_ConsecutiveFailuresTrip verifies that consecutive
// real errors (500s) still trip the circuit breaker.
func TestHTTPClient_CircuitBreaker_ConsecutiveFailuresTrip(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Always return 500
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-cb-consecutive",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
		CBTimeout:       100 * time.Millisecond,
	})

	// First 3 calls should go through and fail
	for i := range 3 {
		_, err := client.GetInfo(context.Background(), "test-lease")
		assert.NotErrorIs(t, err, ErrCircuitOpen, "Circuit should not be open yet at call %d", i+1)
	}

	// 4th call should fail with circuit open
	_, err := client.GetInfo(context.Background(), "test-lease")
	assert.ErrorIs(t, err, ErrCircuitOpen)

	// Server should have received exactly 3 requests
	assert.Equal(t, 3, requestCount)
}

func TestHTTPClient_Provision_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-provision"

	var capturedSig string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	err := client.Provision(context.Background(), ProvisionRequest{
		LeaseUUID:    "lease-hmac-1",
		Tenant:       "tenant-1",
		ProviderUUID: "provider-1",
		Items:        []LeaseItem{{SKU: "gpu-a100", Quantity: 1}},
		CallbackURL:  "http://fred/callback",
	})
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	err = hmacauth.Verify(secret, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_Deprovision_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-deprovision"

	var capturedSig string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-deprov",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	err := client.Deprovision(context.Background(), "lease-to-delete")
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	err = hmacauth.Verify(secret, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_Provision_NoHMAC_NoHeader(t *testing.T) {
	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	// No Secret configured
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-no-hmac",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Provision(context.Background(), ProvisionRequest{
		LeaseUUID:    "lease-no-hmac",
		Tenant:       "tenant-1",
		ProviderUUID: "provider-1",
		Items:        []LeaseItem{{SKU: "gpu-a100", Quantity: 1}},
		CallbackURL:  "http://fred/callback",
	})
	require.NoError(t, err)

	assert.Empty(t, capturedSig, "X-Fred-Signature header should be absent when no secret configured")
}

func TestHTTPClient_GetInfo_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-getinfo!"

	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LeaseInfo{Host: "example.com"})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-getinfo",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	info, err := client.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)
	require.NotNil(t, info)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present on GET /info")
	// GET requests sign an empty body.
	err = hmacauth.Verify(secret, nil, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature for GET request (nil body) should verify")
}

func TestHTTPClient_ListProvisions_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-listprov"

	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"provisions": []ProvisionInfo{},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-listprov",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	_, err := client.ListProvisions(context.Background())
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present on GET /provisions")
	err = hmacauth.Verify(secret, nil, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature for GET request (nil body) should verify")
}

func TestHTTPClient_GetInfo_NoHMAC_NoHeader(t *testing.T) {
	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LeaseInfo{Host: "example.com"})
	}))
	defer server.Close()

	// No Secret configured
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-no-hmac-getinfo",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	_, err := client.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.Empty(t, capturedSig, "X-Fred-Signature header should be absent when no secret configured")
}

func TestHTTPClient_Restart(t *testing.T) {
	var receivedReq RestartRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/restart" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-restart",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Restart(context.Background(), RestartRequest{
		LeaseUUID:   "lease-restart-1",
		CallbackURL: "http://fred/callback",
	})
	require.NoError(t, err)
	assert.Equal(t, "lease-restart-1", receivedReq.LeaseUUID)
	assert.Equal(t, "http://fred/callback", receivedReq.CallbackURL)
}

func TestHTTPClient_Restart_NotProvisioned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-restart-404",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Restart(context.Background(), RestartRequest{LeaseUUID: "nonexistent"})
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

func TestHTTPClient_Restart_InvalidState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-restart-409",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Restart(context.Background(), RestartRequest{LeaseUUID: "busy-lease"})
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestHTTPClient_Restart_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-restart-500",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Restart(context.Background(), RestartRequest{LeaseUUID: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPClient_Restart_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-restart"

	var capturedSig string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-restart",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	err := client.Restart(context.Background(), RestartRequest{
		LeaseUUID:   "lease-hmac-restart",
		CallbackURL: "http://fred/callback",
	})
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	err = hmacauth.Verify(secret, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_Update(t *testing.T) {
	var receivedReq UpdateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/update" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-update",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Update(context.Background(), UpdateRequest{
		LeaseUUID:   "lease-update-1",
		CallbackURL: "http://fred/callback",
		Payload:     []byte("base64-manifest"),
		PayloadHash: "sha256-hash",
	})
	require.NoError(t, err)
	assert.Equal(t, "lease-update-1", receivedReq.LeaseUUID)
	assert.Equal(t, "sha256-hash", receivedReq.PayloadHash)
}

func TestHTTPClient_Update_NotProvisioned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-update-404",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Update(context.Background(), UpdateRequest{LeaseUUID: "nonexistent"})
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

func TestHTTPClient_Update_InvalidState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-update-409",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Update(context.Background(), UpdateRequest{LeaseUUID: "busy-lease"})
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestHTTPClient_Update_ValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":           "validation error: invalid manifest: bad json",
			"validation_code": "invalid_manifest",
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-update-400",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Update(context.Background(), UpdateRequest{LeaseUUID: "test"})
	assert.ErrorIs(t, err, ErrValidation)
	assert.ErrorIs(t, err, ErrInvalidManifest)
}

func TestHTTPClient_Update_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-update-500",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.Update(context.Background(), UpdateRequest{LeaseUUID: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPClient_Update_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-update"

	var capturedSig string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-update",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	err := client.Update(context.Background(), UpdateRequest{
		LeaseUUID:   "lease-hmac-update",
		CallbackURL: "http://fred/callback",
		Payload:     []byte("manifest-data"),
	})
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	err = hmacauth.Verify(secret, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_GetReleases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/releases/lease-rel-1" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]ReleaseInfo{
			{Version: 1, Image: "nginx:1.0", Status: "superseded"},
			{Version: 2, Image: "nginx:2.0", Status: "active"},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-releases",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	releases, err := client.GetReleases(context.Background(), "lease-rel-1")
	require.NoError(t, err)
	require.Len(t, releases, 2)
	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "nginx:1.0", releases[0].Image)
	assert.Equal(t, "superseded", releases[0].Status)
	assert.Equal(t, 2, releases[1].Version)
	assert.Equal(t, "nginx:2.0", releases[1].Image)
	assert.Equal(t, "active", releases[1].Status)
}

func TestHTTPClient_GetReleases_NotProvisioned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-releases-404",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	_, err := client.GetReleases(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

func TestHTTPClient_GetReleases_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-releases-500",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	_, err := client.GetReleases(context.Background(), "test")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPClient_GetReleases_EmptyList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]ReleaseInfo{})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-releases-empty",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	releases, err := client.GetReleases(context.Background(), "lease-no-releases")
	require.NoError(t, err)
	assert.Empty(t, releases)
}

func TestHTTPClient_GetReleases_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-releases"

	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]ReleaseInfo{})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-releases",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	_, err := client.GetReleases(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present on GET /releases")
	err = hmacauth.Verify(secret, nil, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature for GET request (nil body) should verify")
}

func TestDecodeJSONLimited(t *testing.T) {
	t.Run("within limit", func(t *testing.T) {
		body := io.NopCloser(bytes.NewReader([]byte(`{"host":"example.com"}`)))
		var info LeaseInfo
		err := decodeJSONLimited(body, 1024, &info)
		require.NoError(t, err)
		assert.Equal(t, "example.com", info.Host)
	})

	t.Run("exactly at limit", func(t *testing.T) {
		payload := `{"k":"` + strings.Repeat("x", 90) + `"}`
		body := io.NopCloser(bytes.NewReader([]byte(payload)))
		var info LeaseInfo
		err := decodeJSONLimited(body, int64(len(payload)), &info)
		require.NoError(t, err)
	})

	t.Run("exceeds limit by one byte", func(t *testing.T) {
		payload := `{"k":"` + strings.Repeat("x", 90) + `"}`
		body := io.NopCloser(bytes.NewReader([]byte(payload)))
		var info LeaseInfo
		err := decodeJSONLimited(body, int64(len(payload))-1, &info)
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("invalid JSON within limit", func(t *testing.T) {
		body := io.NopCloser(bytes.NewReader([]byte(`not json`)))
		var info LeaseInfo
		err := decodeJSONLimited(body, 1024, &info)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, ErrResponseTooLarge)
	})
}

func TestHTTPClient_ResponseSizeLimits(t *testing.T) {
	// oversizedHandler returns an HTTP handler that writes exactly size bytes
	// of valid JSON for the given endpoint type.
	oversizedJSON := func(size int) []byte {
		// Build a JSON object with a padding field large enough to exceed the limit.
		// {"pad":"xxxx..."} — overhead is ~10 bytes for framing.
		padLen := size - 10
		if padLen < 0 {
			padLen = 0
		}
		return []byte(`{"pad":"` + strings.Repeat("x", padLen) + `"}`)
	}

	t.Run("GetInfo rejects oversized response", func(t *testing.T) {
		const limit int64 = 512
		body := oversizedJSON(int(limit) + 100)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:         "test-size-info",
			BaseURL:      server.URL,
			Timeout:      5 * time.Second,
			MaxInfoBytes: limit,
		})

		_, err := client.GetInfo(context.Background(), "lease-1")
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("GetProvision rejects oversized response", func(t *testing.T) {
		const limit int64 = 512
		body, _ := json.Marshal(ProvisionInfo{LeaseUUID: strings.Repeat("x", int(limit))})

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:              "test-size-provision",
			BaseURL:           server.URL,
			Timeout:           5 * time.Second,
			MaxProvisionBytes: limit,
		})

		_, err := client.GetProvision(context.Background(), "lease-1")
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("ListProvisions rejects oversized response", func(t *testing.T) {
		const limit int64 = 256

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(oversizedJSON(int(limit) + 100))
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:               "test-size-provisions",
			BaseURL:            server.URL,
			Timeout:            5 * time.Second,
			MaxProvisionsBytes: limit,
		})

		_, err := client.ListProvisions(context.Background())
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("GetLogs rejects oversized response", func(t *testing.T) {
		const limit int64 = 256

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(oversizedJSON(int(limit) + 100))
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:         "test-size-logs",
			BaseURL:      server.URL,
			Timeout:      5 * time.Second,
			MaxLogsBytes: limit,
		})

		_, err := client.GetLogs(context.Background(), "lease-1", 100)
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("GetReleases rejects oversized response", func(t *testing.T) {
		const limit int64 = 256

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(oversizedJSON(int(limit) + 100))
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:             "test-size-releases",
			BaseURL:          server.URL,
			Timeout:          5 * time.Second,
			MaxReleasesBytes: limit,
		})

		_, err := client.GetReleases(context.Background(), "lease-1")
		assert.ErrorIs(t, err, ErrResponseTooLarge)
	})

	t.Run("within-limit responses still succeed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(LeaseInfo{Host: "ok.example.com"})
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:         "test-size-ok",
			BaseURL:      server.URL,
			Timeout:      5 * time.Second,
			MaxInfoBytes: 4096,
		})

		info, err := client.GetInfo(context.Background(), "lease-1")
		require.NoError(t, err)
		assert.Equal(t, "ok.example.com", info.Host)
	})
}

func TestPositiveOr(t *testing.T) {
	assert.Equal(t, int64(100), positiveOr(100, 999), "positive value should be used")
	assert.Equal(t, int64(999), positiveOr(0, 999), "zero should fall back to default")
	assert.Equal(t, int64(999), positiveOr(-1, 999), "negative should fall back to default")
	assert.Equal(t, int64(999), positiveOr(-999, 999), "large negative should fall back to default")
	assert.Equal(t, int64(1), positiveOr(1, 999), "boundary: 1 is positive")
}

func TestNewHTTPClient_NegativeLimitsNormalized(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:               "test-negative-limits",
		BaseURL:            "http://example.com",
		MaxInfoBytes:       -1,
		MaxProvisionBytes:  -100,
		MaxProvisionsBytes: 0,
		MaxLogsBytes:       -999,
		MaxReleasesBytes:   -1,
	})

	assert.Equal(t, DefaultMaxInfoBytes, client.maxInfoBytes)
	assert.Equal(t, DefaultMaxProvisionBytes, client.maxProvisionBytes)
	assert.Equal(t, DefaultMaxProvisionsBytes, client.maxProvisionsBytes)
	assert.Equal(t, DefaultMaxLogsBytes, client.maxLogsBytes)
	assert.Equal(t, DefaultMaxReleasesBytes, client.maxReleasesBytes)
}

func TestIsStack(t *testing.T) {
	t.Run("empty items", func(t *testing.T) {
		result, err := IsStack(nil)
		require.NoError(t, err)
		assert.False(t, result)

		result, err = IsStack([]LeaseItem{})
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("all with ServiceName", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		}
		result, err := IsStack(items)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("none with ServiceName", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1},
			{SKU: "docker-small", Quantity: 2},
		}
		result, err := IsStack(items)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("single item with ServiceName", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		}
		result, err := IsStack(items)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("single item without ServiceName", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1},
		}
		result, err := IsStack(items)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("mixed: first has, second doesn't", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1},
		}
		_, err := IsStack(items)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mixed")
	})

	t.Run("mixed: first doesn't, second has", func(t *testing.T) {
		items := []LeaseItem{
			{SKU: "docker-small", Quantity: 1},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		}
		_, err := IsStack(items)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mixed")
	})
}

func TestRecordMetrics_NilCollectors(t *testing.T) {
	t.Run("both nil — no panic", func(t *testing.T) {
		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: "http://localhost"})
		assert.NotPanics(t, func() {
			client.recordMetrics("provision", time.Now(), nil)
			client.recordMetrics("deprovision", time.Now(), assert.AnError)
		})
	})

	t.Run("only RequestDuration set — no panic", func(t *testing.T) {
		hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "test_duration_partial",
			Help: "test",
		}, []string{"backend", "operation", "status"})

		client := NewHTTPClient(HTTPClientConfig{
			Name:            "test",
			BaseURL:         "http://localhost",
			RequestDuration: hist,
		})
		assert.NotPanics(t, func() {
			client.recordMetrics("provision", time.Now(), nil)
		})
		// Histogram was observed (1 sample); use testutil.CollectAndCount to verify.
		assert.Equal(t, 1, promtestutil.CollectAndCount(hist))
	})

	t.Run("only RequestsTotal set", func(t *testing.T) {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_requests_partial",
			Help: "test",
		}, []string{"backend", "operation", "status"})

		client := NewHTTPClient(HTTPClientConfig{
			Name:          "test",
			BaseURL:       "http://localhost",
			RequestsTotal: counter,
		})
		assert.NotPanics(t, func() {
			client.recordMetrics("provision", time.Now(), nil)
		})
		assert.Equal(t, 1.0, promtestutil.ToFloat64(counter.WithLabelValues("test", "provision", "success")))
	})

	t.Run("both set — records to both", func(t *testing.T) {
		hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "test_duration_both",
			Help: "test",
		}, []string{"backend", "operation", "status"})
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_requests_both",
			Help: "test",
		}, []string{"backend", "operation", "status"})

		client := NewHTTPClient(HTTPClientConfig{
			Name:            "test",
			BaseURL:         "http://localhost",
			RequestDuration: hist,
			RequestsTotal:   counter,
		})
		client.recordMetrics("get_info", time.Now(), nil)
		client.recordMetrics("get_info", time.Now(), assert.AnError)

		assert.Equal(t, 1.0, promtestutil.ToFloat64(counter.WithLabelValues("test", "get_info", "success")))
		assert.Equal(t, 1.0, promtestutil.ToFloat64(counter.WithLabelValues("test", "get_info", "error")))
	})
}
