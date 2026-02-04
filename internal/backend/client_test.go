package backend

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
// return ErrValidation to indicate a permanent error that shouldn't be retried.
func TestHTTPClient_Provision_ValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 400 with error details
		http.Error(w, `{"error":"unknown SKU: invalid-sku"}`, http.StatusBadRequest)
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

	// Should return ErrValidation for 400 responses
	assert.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "unknown SKU")
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
				"host":     "test.example.com",
				"port":     8080,
				"protocol": "https",
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
	assert.Equal(t, "test.example.com", (*info)["host"])

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
	for i := 0; i < 3; i++ {
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
	for i := 0; i < 2; i++ {
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
	for i := 0; i < 3; i++ {
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
		json.NewEncoder(w).Encode(LeaseInfo{"host": "example.com"})
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
		json.NewEncoder(w).Encode(LeaseInfo{"host": "example.com"})
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
