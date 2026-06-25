package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_Deprovision_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-deprovision"

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute)
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

	var capturedSig, capturedMethod, capturedURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, nil, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature for GET request (nil body) should verify")
}

func TestHTTPClient_ListProvisions_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-signing-listprov"

	var capturedSig, capturedMethod, capturedURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, nil, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature for GET request (nil body) should verify")
}

func TestHTTPClient_LookupProvisions(t *testing.T) {
	t.Run("OK with subset", func(t *testing.T) {
		var capturedQuery []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedQuery = r.URL.Query()["lease_uuid"]
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ListProvisionsResponse{
				Provisions: []ProvisionInfo{
					{LeaseUUID: "uuid-1", Status: ProvisionStatusReady},
				},
			})
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
		got, err := client.LookupProvisions(context.Background(), []string{"uuid-1", "uuid-2"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "uuid-1", got[0].LeaseUUID)
		assert.Equal(t, []string{"uuid-1", "uuid-2"}, capturedQuery)
	})

	t.Run("rejects empty input", func(t *testing.T) {
		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: "http://example.com"})
		_, err := client.LookupProvisions(context.Background(), nil)
		assert.Error(t, err)
		_, err = client.LookupProvisions(context.Background(), []string{})
		assert.Error(t, err)
	})

	t.Run("rejects over cap", func(t *testing.T) {
		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: "http://example.com"})
		tooMany := make([]string, MaxLookupUUIDs+1)
		for i := range tooMany {
			tooMany[i] = "uuid"
		}
		_, err := client.LookupProvisions(context.Background(), tooMany)
		assert.Error(t, err)
	})

	t.Run("404 returns nil result with no error (defensive)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
		got, err := client.LookupProvisions(context.Background(), []string{"uuid-1"})
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("backend error propagates", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
		_, err := client.LookupProvisions(context.Background(), []string{"uuid-1"})
		assert.Error(t, err)
	})

	t.Run("HMAC signing on GET (nil body)", func(t *testing.T) {
		const secret = "test-secret-for-lookup-provisions"

		var capturedSig, capturedMethod, capturedURI string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedSig = r.Header.Get(hmacauth.SignatureHeader)
			capturedMethod = r.Method
			capturedURI = r.URL.RequestURI()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: []ProvisionInfo{}})
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name: "test-hmac-lookup", BaseURL: server.URL, Timeout: 5 * time.Second, Secret: secret,
		})

		_, err := client.LookupProvisions(context.Background(), []string{"uuid-1"})
		require.NoError(t, err)
		assert.NotEmpty(t, capturedSig)
		assert.NoError(t, hmacauth.Verify(secret, capturedMethod, capturedURI, nil, capturedSig, 5*time.Minute))
	})

	t.Run("stable URL ordering across permutations", func(t *testing.T) {
		// Verify the in-place sort produces identical URLs regardless of caller order.
		var capturedURLs []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedURLs = append(capturedURLs, r.URL.RawQuery)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: []ProvisionInfo{}})
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
		_, err := client.LookupProvisions(context.Background(), []string{"uuid-3", "uuid-1", "uuid-2"})
		require.NoError(t, err)
		_, err = client.LookupProvisions(context.Background(), []string{"uuid-1", "uuid-3", "uuid-2"})
		require.NoError(t, err)

		require.Len(t, capturedURLs, 2)
		assert.Equal(t, capturedURLs[0], capturedURLs[1],
			"identical UUID sets should produce identical URLs regardless of caller order")
	})

	t.Run("does not mutate caller slice", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: []ProvisionInfo{}})
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
		input := []string{"uuid-3", "uuid-1", "uuid-2"}
		original := append([]string(nil), input...)
		_, err := client.LookupProvisions(context.Background(), input)
		require.NoError(t, err)
		assert.Equal(t, original, input, "LookupProvisions must not sort the caller's slice")
	})
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

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_ReconcileCustomDomain(t *testing.T) {
	var receivedReq ReconcileCustomDomainRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/reconcile_custom_domain" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-reconcile",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	items := []LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	err := client.ReconcileCustomDomain(context.Background(), "lease-1", items)
	require.NoError(t, err)
	assert.Equal(t, "lease-1", receivedReq.LeaseUUID)
	require.Len(t, receivedReq.Items, 2)
	assert.Equal(t, "web", receivedReq.Items[0].ServiceName)
	assert.Equal(t, "foo.example.com", receivedReq.Items[0].CustomDomain)
	assert.Equal(t, "db", receivedReq.Items[1].ServiceName)
	assert.Equal(t, "", receivedReq.Items[1].CustomDomain)
}

func TestHTTPClient_ReconcileCustomDomain_AcceptsAccepted(t *testing.T) {
	// Server may return 202 Accepted as well as 204 No Content; client
	// must accept both.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-reconcile-202",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	require.NoError(t, client.ReconcileCustomDomain(context.Background(), "lease-1", nil))
}

func TestHTTPClient_ReconcileCustomDomain_NotProvisioned(t *testing.T) {
	// 404 from /reconcile_custom_domain must map back to ErrNotProvisioned
	// so the circuit breaker's IsSuccessful exemption applies; otherwise
	// routine reconcile-tick races on a deprovisioned lease would count
	// toward the CB trip threshold.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-reconcile-404",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.ReconcileCustomDomain(context.Background(), "lease-1", nil)
	assert.ErrorIs(t, err, ErrNotProvisioned)
}

func TestHTTPClient_ReconcileCustomDomain_InvalidState(t *testing.T) {
	// Same rationale as the 404 mapping: 409 must surface as ErrInvalidState
	// so it stays exempt from the circuit breaker.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-reconcile-409",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.ReconcileCustomDomain(context.Background(), "lease-1", nil)
	assert.ErrorIs(t, err, ErrInvalidState)
}

func TestHTTPClient_ReconcileCustomDomain_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-reconcile-500",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	err := client.ReconcileCustomDomain(context.Background(), "lease-1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPClient_ReconcileCustomDomain_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-reconcile"

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-reconcile",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	require.NoError(t, client.ReconcileCustomDomain(context.Background(), "lease-1", []LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
	}))

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	require.NoError(t, hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute))
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

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute)
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

	var capturedSig, capturedMethod, capturedURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
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
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, nil, capturedSig, 5*time.Minute)
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

	t.Run("LookupProvisions rejects oversized response", func(t *testing.T) {
		const limit int64 = 256

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(oversizedJSON(int(limit) + 100))
		}))
		defer server.Close()

		client := NewHTTPClient(HTTPClientConfig{
			Name:                     "test-size-lookup",
			BaseURL:                  server.URL,
			Timeout:                  5 * time.Second,
			MaxLookupProvisionsBytes: limit,
		})

		_, err := client.LookupProvisions(context.Background(), []string{"uuid-1"})
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

// --- Boundary-normalization contract (Task 1, plan §Task 1.2) ---
//
// These tests lock the contract for NormalizeProvisionRequest, the entry-point
// helper that converts legacy single-item-without-service_name requests into
// the stack-shaped form expected by every downstream code path.
//
// Will compile/pass only after Task 3 adds NormalizeProvisionRequest; until
// then they are the RED that proves the contract.

func TestNormalizeProvisionRequest_AutoTagsSingleUnnamedItem(t *testing.T) {
	req := ProvisionRequest{
		LeaseUUID: "u", Tenant: "t",
		Items: []LeaseItem{{SKU: "docker-micro", Quantity: 1}},
	}
	if err := NormalizeProvisionRequest(&req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := req.Items[0].ServiceName; got != "app" {
		t.Fatalf("expected service_name=app, got %q", got)
	}
}

func TestNormalizeProvisionRequest_PreservesNamedItems(t *testing.T) {
	req := ProvisionRequest{
		Items: []LeaseItem{
			{SKU: "docker-micro", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		},
	}
	if err := NormalizeProvisionRequest(&req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Items[0].ServiceName != "web" || req.Items[1].ServiceName != "db" {
		t.Fatalf("service_name modified unexpectedly: %+v", req.Items)
	}
}

func TestNormalizeProvisionRequest_RejectsMixed(t *testing.T) {
	req := ProvisionRequest{
		Items: []LeaseItem{
			{SKU: "docker-micro", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1}, // missing
		},
	}
	err := NormalizeProvisionRequest(&req)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidManifest,
		"mixed service_name presence must surface as ErrInvalidManifest (caller filtering depends on it)")
}

// TestNormalizeProvisionRequest_RejectsEmptyItems covers the
// zero-items edge case: every provision must carry at least one
// LeaseItem after normalization, so an empty Items slice must be
// rejected at the boundary rather than allowed to silently produce
// a no-op provision downstream.
func TestNormalizeProvisionRequest_RejectsEmptyItems(t *testing.T) {
	req := ProvisionRequest{
		LeaseUUID: "u", Tenant: "t",
		Items: nil,
	}
	err := NormalizeProvisionRequest(&req)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrValidation,
		"empty items must surface as ErrValidation (a structural request error, not a manifest shape error)")
}

// TestNormalizeProvisionRequest_RejectsNilRequest covers the
// programmer-error case where a nil *ProvisionRequest is passed in.
// Should return ErrValidation rather than panicking with nil deref.
func TestNormalizeProvisionRequest_RejectsNilRequest(t *testing.T) {
	err := NormalizeProvisionRequest(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrValidation,
		"nil request must surface as ErrValidation rather than panicking")
}

// TestNormalizeProvisionRequest_RejectsMultipleUnnamed covers the
// case where the request claims to be "legacy form" (no service
// names) but carries multiple items. The legacy single-service
// contract structurally allows only ONE item per lease; multiple
// unnamed items have no defined semantics — reject rather than
// auto-tag them all with the same service name (which would create
// a name collision).
func TestNormalizeProvisionRequest_RejectsMultipleUnnamed(t *testing.T) {
	req := ProvisionRequest{
		LeaseUUID: "u", Tenant: "t",
		Items: []LeaseItem{
			{SKU: "docker-micro", Quantity: 1},
			{SKU: "docker-micro", Quantity: 1},
		},
	}
	err := NormalizeProvisionRequest(&req)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidManifest,
		"multiple unnamed items must surface as ErrInvalidManifest (structurally ambiguous wire shape)")
}

// TestNormalizeProvisionRequest_AcceptsSingleNamedItem locks in the
// boundary case adjacent to AutoTags: a single explicitly-named item
// passes through unchanged (no auto-tagging, no rejection).
func TestNormalizeProvisionRequest_AcceptsSingleNamedItem(t *testing.T) {
	req := ProvisionRequest{
		LeaseUUID: "u", Tenant: "t",
		Items: []LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "custom"}},
	}
	require.NoError(t, NormalizeProvisionRequest(&req))
	require.Equal(t, "custom", req.Items[0].ServiceName,
		"single explicitly-named item must pass through unchanged")
}

func TestNewHTTPClient_AppliesTLSClientConfig(t *testing.T) {
	sentinel := &tls.Config{MinVersion: tls.VersionTLS13}
	c := NewHTTPClient(HTTPClientConfig{
		Name:            "tls-backend",
		BaseURL:         "https://backend.example:9001",
		TLSClientConfig: sentinel,
	})
	tr, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Same(t, sentinel, tr.TLSClientConfig)
}

func TestNewHTTPClient_NoTLSConfig_LeavesTransportDefault(t *testing.T) {
	c := NewHTTPClient(HTTPClientConfig{Name: "plain", BaseURL: "http://backend:9001"})
	tr, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, tr.TLSClientConfig)
}

func TestHTTPClient_GetLoadStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/stats", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_cpu_cores":8,"allocated_cpu_cores":2,"active_containers":3,"total_memory_mb":1024}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})

	stats, err := client.GetLoadStats(context.Background())
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, 8.0, stats.TotalCPUCores)
	assert.Equal(t, 2.0, stats.AllocatedCPUCores)
	assert.Equal(t, 3, stats.ActiveContainers)

	ratio, ok := stats.CPUAllocatedRatio()
	assert.True(t, ok)
	assert.InDelta(t, 0.25, ratio, 1e-9)
}

func TestLoadStats_CPUAllocatedRatio(t *testing.T) {
	var nilStats *LoadStats
	_, ok := nilStats.CPUAllocatedRatio()
	assert.False(t, ok, "nil stats has no usable ratio")

	_, ok = (&LoadStats{TotalCPUCores: 0, AllocatedCPUCores: 5}).CPUAllocatedRatio()
	assert.False(t, ok, "zero total capacity has no usable ratio")

	ratio, ok := (&LoadStats{TotalCPUCores: 4, AllocatedCPUCores: 1}).CPUAllocatedRatio()
	assert.True(t, ok)
	assert.InDelta(t, 0.25, ratio, 1e-9)
}

// --- HTTPClient.Restore tests ---

func TestHTTPClientRestore_StatusMapping(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string // optional response body (for code-discriminated cases)
		wantErr    error
		wantNil    bool
	}{
		{
			name:       "202 Accepted returns nil",
			statusCode: http.StatusAccepted,
			wantNil:    true,
		},
		{
			name:       "422 Unprocessable Entity returns ErrNotRetained",
			statusCode: http.StatusUnprocessableEntity,
			wantErr:    ErrNotRetained,
		},
		{
			name:       "409 Conflict (no code) returns ErrInvalidState",
			statusCode: http.StatusConflict,
			wantErr:    ErrInvalidState,
		},
		{
			name:       "409 Conflict with already_provisioned code returns ErrAlreadyProvisioned",
			statusCode: http.StatusConflict,
			body:       `{"error":"lease already provisioned","code":"already_provisioned"}`,
			wantErr:    ErrAlreadyProvisioned,
		},
		{
			name:       "503 Service Unavailable returns ErrInsufficientResources",
			statusCode: http.StatusServiceUnavailable,
			wantErr:    ErrInsufficientResources,
		},
		{
			name:       "400 Bad Request returns ErrValidation",
			statusCode: http.StatusBadRequest,
			wantErr:    ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/restore" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				w.WriteHeader(tt.statusCode)
				if tt.body != "" {
					_, _ = w.Write([]byte(tt.body))
				}
			}))
			defer server.Close()

			client := NewHTTPClient(HTTPClientConfig{
				Name:    "test-restore-status",
				BaseURL: server.URL,
				Timeout: 5 * time.Second,
			})

			err := client.Restore(context.Background(), RestoreRequest{
				LeaseUUID:     "new-lease-1",
				FromLeaseUUID: "old-lease-1",
				Tenant:        "tenant-1",
				CallbackURL:   "http://fred/callback",
			})

			if tt.wantNil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestHTTPClientRestore_ValidationCodeRoundTrips verifies that a 400 response
// carrying a validation_code reconstructs the SPECIFIC sub-category sentinel
// (not just the generic ErrValidation), matching Provision/Update. Restore's
// prelude returns ErrUnknownSKU/ErrInvalidManifest/ErrImageNotAllowed and the
// docker-backend handler encodes validation_code, so dropping it client-side
// would lose the sub-category across the wire.
func TestHTTPClientRestore_ValidationCodeRoundTrips(t *testing.T) {
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
			`{"error":"validation error: invalid manifest: bad shape","validation_code":"invalid_manifest"}`,
			ErrInvalidManifest,
			"invalid manifest: bad shape",
		},
		{
			"image not allowed code",
			`{"error":"validation error: image not allowed: evil.io","validation_code":"image_not_allowed"}`,
			ErrImageNotAllowed,
			"image not allowed: evil.io",
		},
		{
			"missing code falls back to generic ErrValidation",
			`{"error":"some validation error"}`,
			ErrValidation,
			"some validation error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/restore" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(tt.responseBody))
			}))
			defer server.Close()

			client := NewHTTPClient(HTTPClientConfig{
				Name:    "test-restore-validation-code",
				BaseURL: server.URL,
				Timeout: 5 * time.Second,
			})

			err := client.Restore(context.Background(), RestoreRequest{
				LeaseUUID:     "new-lease-1",
				FromLeaseUUID: "old-lease-1",
				Tenant:        "tenant-1",
				CallbackURL:   "http://fred/callback",
			})

			assert.ErrorIs(t, err, tt.wantSentinel)
			assert.ErrorIs(t, err, ErrValidation, "all validation errors should match ErrValidation")
			assert.Contains(t, err.Error(), tt.wantMessage)
		})
	}
}

func TestHTTPClientRestore_500TripsBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-restore-500-cb",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: 3,
		CBTimeout:       100 * time.Millisecond,
	})

	req := RestoreRequest{
		LeaseUUID:     "new-lease-1",
		FromLeaseUUID: "old-lease-1",
		Tenant:        "t",
		CallbackURL:   "http://fred/callback",
	}

	// First 3 calls should reach the server
	for i := range 3 {
		err := client.Restore(context.Background(), req)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, ErrCircuitOpen, "circuit should not be open at call %d", i+1)
	}

	// 4th call should get ErrCircuitOpen (breaker tripped)
	err := client.Restore(context.Background(), req)
	assert.ErrorIs(t, err, ErrCircuitOpen)
}

// TestHTTPClientRestore_422DoesNotTripBreaker is the primary circuit-breaker
// allowlist test: 422 (ErrNotRetained) is a benign client condition and must
// NOT count toward the failure threshold, no matter how many times it occurs.
func TestHTTPClientRestore_422DoesNotTripBreaker(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer server.Close()

	// Set a low threshold to make any tripping obvious.
	const threshold = 3
	client := NewHTTPClient(HTTPClientConfig{
		Name:            "test-restore-422-cb",
		BaseURL:         server.URL,
		Timeout:         5 * time.Second,
		CBFailureThresh: threshold,
	})

	req := RestoreRequest{
		LeaseUUID:     "new-lease-1",
		FromLeaseUUID: "old-lease-1",
		Tenant:        "t",
		CallbackURL:   "http://fred/callback",
	}

	// Make more requests than the threshold — circuit must stay closed.
	const n = threshold * 4
	for i := range n {
		err := client.Restore(context.Background(), req)
		assert.ErrorIs(t, err, ErrNotRetained, "call %d should return ErrNotRetained", i+1)
		assert.NotErrorIs(t, err, ErrCircuitOpen, "circuit must not open on ErrNotRetained (call %d)", i+1)
	}

	// All requests should have reached the server (none short-circuited).
	assert.Equal(t, n, requestCount, "all %d requests should have reached the server", n)
}

func TestHTTPClientRestore_WithHMAC(t *testing.T) {
	const secret = "test-secret-for-hmac-restore-abcde"

	var capturedSig, capturedMethod, capturedURI string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-hmac-restore",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
		Secret:  secret,
	})

	err := client.Restore(context.Background(), RestoreRequest{
		LeaseUUID:     "new-lease-hmac",
		FromLeaseUUID: "old-lease-hmac",
		Tenant:        "tenant-1",
		CallbackURL:   "http://fred/callback",
	})
	require.NoError(t, err)

	assert.NotEmpty(t, capturedSig, "X-Fred-Signature header should be present")
	err = hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, 5*time.Minute)
	assert.NoError(t, err, "HMAC signature should verify successfully")
}

func TestHTTPClient_ListRetentions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/retentions", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"retentions":[{"lease_uuid":"lease-a"},{"lease_uuid":"lease-b"}]}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})

	got, err := client.ListRetentions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "lease-a", got[0].LeaseUUID)
	assert.Equal(t, "lease-b", got[1].LeaseUUID)
}

func TestHTTPClient_ListRetentions_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"retentions":[]}`))
	}))
	defer server.Close()
	client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})
	got, err := client.ListRetentions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestNewHTTPClient_ProvisionsPageLimitDefault(t *testing.T) {
	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com"})
	assert.Equal(t, DefaultProvisionsPageLimit, c.provisionsPageLimit, "zero config falls back to default")

	c2 := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com", ProvisionsPageLimit: 250})
	assert.Equal(t, 250, c2.provisionsPageLimit, "explicit config is honored")

	c3 := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com", ProvisionsPageLimit: -5})
	assert.Equal(t, DefaultProvisionsPageLimit, c3.provisionsPageLimit, "negative config falls back to default (never sent as limit=-5)")
}

// pagingProvisionServer serves /provisions using the real PaginateProvisions
// helper so the client loop is exercised against production paging semantics.
func pagingProvisionServer(t *testing.T, all []ProvisionInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/provisions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		limit, cont, err := ParseProvisionsPageParams(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		page, next := PaginateProvisions(all, cont, limit)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: page, Continue: next})
	}))
}

// provUUID renders i as a lexically-sortable valid UUID. The client loop sends
// the last LeaseUUID back as the `continue` query param, which the producer's
// ParseProvisionsPageParams validates as a UUID — so the IDs in these
// end-to-end tests must be real UUIDs (zero-padded so lexical == numeric order).
func provUUID(i int) string {
	return fmt.Sprintf("%08d-0000-0000-0000-000000000000", i)
}

func manyProvisions(n int) []ProvisionInfo {
	out := make([]ProvisionInfo, n)
	for i := range out {
		out[i] = ProvisionInfo{LeaseUUID: provUUID(i), Status: ProvisionStatusReady}
	}
	return out
}

func TestHTTPClient_ListProvisions_ReassemblesAllPages(t *testing.T) {
	all := manyProvisions(2500) // > default page size of 1000 -> 3 pages
	server := pagingProvisionServer(t, all)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	got, err := c.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2500)

	seen := make(map[string]int)
	for _, p := range got {
		seen[p.LeaseUUID]++
	}
	assert.Len(t, seen, 2500, "no duplicates across pages")
	assert.Equal(t, provUUID(0), got[0].LeaseUUID, "sorted order preserved")
}

func TestHTTPClient_ListProvisions_SmallPageLimit(t *testing.T) {
	all := manyProvisions(25)
	server := pagingProvisionServer(t, all)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, ProvisionsPageLimit: 10})
	got, err := c.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, got, 25)
}

func TestHTTPClient_ListProvisions_NonAdvancingContinueErrors(t *testing.T) {
	// Malicious/buggy server: always returns the same continue token.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{
			Provisions: []ProvisionInfo{{LeaseUUID: "stuck"}},
			Continue:   "stuck", // never advances
		})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	_, err := c.ListProvisions(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-advancing")
}

func TestHTTPClient_ListProvisions_PerPageTooLargeErrors(t *testing.T) {
	big := strings.Repeat("x", 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps := make([]ProvisionInfo, 0, 2000)
		for i := 0; i < 2000; i++ {
			ps = append(ps, ProvisionInfo{LeaseUUID: padID(i), LastError: big})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: ps, Continue: padID(1999)})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, MaxProvisionsBytes: 64 << 10}) // 64 KiB per page
	_, err := c.ListProvisions(context.Background())
	require.Error(t, err, "an oversized page errors (fail-closed), not silent truncation")
}

func TestHTTPClient_ListProvisions_BackCompatSinglePageNoContinue(t *testing.T) {
	// Old server: ignores limit/continue, returns full list, no continue field.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"provisions": []ProvisionInfo{{LeaseUUID: "a"}, {LeaseUUID: "b"}},
		})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	got, err := c.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, got, 2, "no continue field => stop after one page")
}

func TestHTTPClient_ListProvisions_ContinuesOnShortNonFinalPage(t *testing.T) {
	// A server may return fewer than `limit` items but still set continue; the
	// client must NOT stop on a short page — only an empty continue ends the walk.
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: []ProvisionInfo{{LeaseUUID: "a"}}, Continue: "a"})
			return
		}
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: []ProvisionInfo{{LeaseUUID: "b"}}, Continue: ""})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	got, err := c.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"a", "b"}, []string{got[0].LeaseUUID, got[1].LeaseUUID})
}

// Producer that errors on the 2nd page exercises complete-or-error end to end.
func TestHTTPClient_ListProvisions_PageErrorAborts(t *testing.T) {
	all := manyProvisions(25)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls >= 2 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		limit, cont, _ := ParseProvisionsPageParams(r.URL.Query())
		page, next := PaginateProvisions(all, cont, limit)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: page, Continue: next})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, ProvisionsPageLimit: 10})
	_, err := c.ListProvisions(context.Background())
	require.Error(t, err, "a failed page aborts the whole fetch (complete-or-error)")
}

// Directional safety: a provision deleted between pages is simply absent from the
// assembled set — never duplicated, never an error. (Orphan detection is
// orphans = backend - chain, so an omitted entry can only SHRINK the
// deprovision-candidate set; it is re-checked next tick.)
func TestHTTPClient_ListProvisions_ToleratesMidFetchDeletion(t *testing.T) {
	live := manyProvisions(25) // provUUID(0)..provUUID(24)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		set := live
		if calls >= 2 {
			// After page 1 (provUUID(0)..provUUID(9)), delete a not-yet-returned item.
			set = make([]ProvisionInfo, 0, len(live))
			for _, p := range live {
				if p.LeaseUUID == provUUID(20) {
					continue
				}
				set = append(set, p)
			}
		}
		limit, cont, _ := ParseProvisionsPageParams(r.URL.Query())
		page, next := PaginateProvisions(set, cont, limit)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListProvisionsResponse{Provisions: page, Continue: next})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, ProvisionsPageLimit: 10})
	got, err := c.ListProvisions(context.Background())
	require.NoError(t, err)

	seen := map[string]int{}
	for _, p := range got {
		seen[p.LeaseUUID]++
		assert.Equal(t, 1, seen[p.LeaseUUID], "no duplicates across pages")
	}
	assert.NotContains(t, seen, provUUID(20), "deleted-mid-fetch item is simply absent")
	assert.Len(t, got, 24)
}
