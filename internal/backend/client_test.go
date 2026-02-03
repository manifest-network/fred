package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

	if err != nil {
		t.Fatalf("Provision() error = %v", err)
	}

	if receivedReq.LeaseUUID != "lease-uuid-1" {
		t.Errorf("LeaseUUID = %q, want %q", receivedReq.LeaseUUID, "lease-uuid-1")
	}
	if receivedReq.RoutingSKU() != "gpu-a100" {
		t.Errorf("PrimarySKU() = %q, want %q", receivedReq.RoutingSKU(), "gpu-a100")
	}
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
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if (*info)["host"] != "test.example.com" {
		t.Errorf("Host = %q, want %q", (*info)["host"], "test.example.com")
	}

	// Test not found
	_, err = client.GetInfo(context.Background(), "not-found-uuid")
	if err != ErrNotProvisioned {
		t.Errorf("GetInfo() error = %v, want ErrNotProvisioned", err)
	}
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
	if err != nil {
		t.Fatalf("Deprovision() error = %v", err)
	}
	if receivedUUID != "lease-to-delete" {
		t.Errorf("received UUID = %q, want %q", receivedUUID, "lease-to-delete")
	}
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
	if err != nil {
		t.Fatalf("ListProvisions() error = %v", err)
	}
	if len(provisions) != 2 {
		t.Errorf("got %d provisions, want 2", len(provisions))
	}
}

func TestHTTPClient_Name(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "my-backend",
		BaseURL: "http://example.com",
	})

	if client.Name() != "my-backend" {
		t.Errorf("Name() = %q, want %q", client.Name(), "my-backend")
	}
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
		if err == nil {
			t.Fatalf("Provision() call %d should have failed", i+1)
		}
		if err == ErrCircuitOpen {
			t.Fatalf("Circuit should not be open yet at call %d", i+1)
		}
	}

	// Verify server received all 3 requests
	if failCount != 3 {
		t.Errorf("Server should have received 3 requests, got %d", failCount)
	}

	// 4th call should fail immediately with circuit open error
	err := client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	if err != ErrCircuitOpen {
		t.Errorf("Provision() should return ErrCircuitOpen, got %v", err)
	}

	// Server should not receive the 4th request (circuit is open)
	if failCount != 3 {
		t.Errorf("Server should still have 3 requests after circuit open, got %d", failCount)
	}

	// Wait for circuit breaker timeout to transition to half-open
	time.Sleep(150 * time.Millisecond)

	// Next call should go through (half-open state) but still fail
	err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	if err == ErrCircuitOpen {
		t.Error("Circuit should be half-open, not open")
	}
	if failCount != 4 {
		t.Errorf("Server should have received 4 requests after half-open, got %d", failCount)
	}
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
	if err != ErrCircuitOpen {
		t.Errorf("Circuit should be open, got %v", err)
	}

	// Wait for half-open
	time.Sleep(150 * time.Millisecond)

	// Now make the server healthy
	shouldFail = false

	// This call should succeed and close the circuit
	err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
	if err != nil {
		t.Errorf("Provision() should succeed after recovery, got %v", err)
	}

	// Verify circuit is closed by making more calls
	for i := 0; i < 3; i++ {
		err = client.Provision(context.Background(), ProvisionRequest{LeaseUUID: "test"})
		if err != nil {
			t.Errorf("Provision() call %d should succeed, got %v", i+1, err)
		}
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
	if err != nil {
		t.Errorf("Health() error = %v, want nil", err)
	}
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
	if err == nil {
		t.Error("Health() should return error for unhealthy backend")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("Health() error should mention status code, got: %v", err)
	}
}

func TestHTTPClient_Health_ConnectionError(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test-health-conn-err",
		BaseURL: "http://localhost:59999", // Unlikely to be in use
		Timeout: 100 * time.Millisecond,
	})

	err := client.Health(context.Background())
	if err == nil {
		t.Error("Health() should return error when backend is unreachable")
	}
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
	if err == nil {
		t.Error("Health() should return error when context is cancelled")
	}
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
		if err != ErrNotProvisioned {
			t.Fatalf("GetInfo() call %d error = %v, want ErrNotProvisioned", i+1, err)
		}
	}

	// All 10 requests should have reached the server (circuit never opened)
	if requestCount != 10 {
		t.Errorf("Server received %d requests, want 10 (circuit should not have opened)", requestCount)
	}

	// Verify circuit is still closed by making another request
	_, err := client.GetInfo(context.Background(), "another-lease")
	if err == ErrCircuitOpen {
		t.Error("Circuit breaker should not have opened from 404 responses")
	}
	if err != ErrNotProvisioned {
		t.Errorf("GetInfo() error = %v, want ErrNotProvisioned", err)
	}
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
		if err == ErrCircuitOpen {
			t.Fatalf("Circuit should not have opened at request %d - 404s should reset failure count", i+1)
		}
	}

	// All 10 requests should have gone through
	if requestCount != 10 {
		t.Errorf("Server received %d requests, want 10", requestCount)
	}
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
		if err == ErrCircuitOpen {
			t.Fatalf("Circuit should not be open yet at call %d", i+1)
		}
	}

	// 4th call should fail with circuit open
	_, err := client.GetInfo(context.Background(), "test-lease")
	if err != ErrCircuitOpen {
		t.Errorf("Circuit should be open after 3 consecutive failures, got %v", err)
	}

	// Server should have received exactly 3 requests
	if requestCount != 3 {
		t.Errorf("Server received %d requests, want 3", requestCount)
	}
}
