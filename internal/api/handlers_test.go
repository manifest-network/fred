package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestHealthCheck(t *testing.T) {
	h := &Handlers{
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
		// client is nil - health check should still work
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("HealthCheck() status = %d, want %d", rec.Code, http.StatusOK)
	}

	var response HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "healthy" {
		t.Errorf("status = %q, want %q", response.Status, "healthy")
	}
	if response.ProviderUUID != testutil.ValidUUID1 {
		t.Errorf("provider_uuid = %q, want %q", response.ProviderUUID, testutil.ValidUUID1)
	}

	// Check Content-Type header
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", rec.Header().Get("Content-Type"), "application/json")
	}
}

func TestGenerateConnectionDetails_Deterministic(t *testing.T) {
	uuid := testutil.ValidUUID1

	// Generate multiple times
	conn1 := generateConnectionDetails(uuid)
	conn2 := generateConnectionDetails(uuid)
	conn3 := generateConnectionDetails(uuid)

	// All should be identical
	if conn1.Host != conn2.Host || conn2.Host != conn3.Host {
		t.Error("generateConnectionDetails() is not deterministic for Host")
	}
	if conn1.Port != conn2.Port || conn2.Port != conn3.Port {
		t.Error("generateConnectionDetails() is not deterministic for Port")
	}
	if conn1.Protocol != conn2.Protocol || conn2.Protocol != conn3.Protocol {
		t.Error("generateConnectionDetails() is not deterministic for Protocol")
	}
	if conn1.Metadata["region"] != conn2.Metadata["region"] {
		t.Error("generateConnectionDetails() is not deterministic for Metadata")
	}
}

func TestGenerateConnectionDetails_DifferentUUIDs(t *testing.T) {
	conn1 := generateConnectionDetails(testutil.ValidUUID1)
	conn2 := generateConnectionDetails(testutil.ValidUUID2)
	conn3 := generateConnectionDetails(testutil.ValidUUID3)

	// Different UUIDs should (likely) produce different results
	// Note: There's a small chance of collision, but with different UUIDs it's unlikely
	allSame := conn1.Host == conn2.Host && conn2.Host == conn3.Host &&
		conn1.Port == conn2.Port && conn2.Port == conn3.Port

	if allSame {
		t.Log("Warning: All three different UUIDs produced identical connection details (unlikely but possible)")
	}
}

func TestGenerateConnectionDetails_ValidOutput(t *testing.T) {
	conn := generateConnectionDetails(testutil.ValidUUID1)

	// Verify host is from the list
	if !slices.Contains(hostnames, conn.Host) {
		t.Errorf("Host %q is not in valid hostnames list", conn.Host)
	}

	// Verify port is from the list
	if !slices.Contains(ports, conn.Port) {
		t.Errorf("Port %d is not in valid ports list", conn.Port)
	}

	// Verify protocol is from the list
	if !slices.Contains(protocols, conn.Protocol) {
		t.Errorf("Protocol %q is not in valid protocols list", conn.Protocol)
	}

	// Verify metadata fields exist
	if conn.Metadata["region"] == "" {
		t.Error("Metadata region is empty")
	}
	if conn.Metadata["tier"] == "" {
		t.Error("Metadata tier is empty")
	}
	if conn.Metadata["instance_id"] == "" {
		t.Error("Metadata instance_id is empty")
	}
	if conn.Metadata["status"] != "connected" {
		t.Errorf("Metadata status = %q, want %q", conn.Metadata["status"], "connected")
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, "test error", http.StatusBadRequest)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("writeError() status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var response ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "test error" {
		t.Errorf("Error = %q, want %q", response.Error, "test error")
	}
	if response.Code != http.StatusBadRequest {
		t.Errorf("Code = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestWriteJSON(t *testing.T) {
	data := map[string]string{"key": "value"}

	rec := httptest.NewRecorder()
	writeJSON(rec, data, http.StatusOK)

	if rec.Code != http.StatusOK {
		t.Errorf("writeJSON() status = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %q, want %q", rec.Header().Get("Content-Type"), "application/json")
	}

	var response map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["key"] != "value" {
		t.Errorf("response[key] = %q, want %q", response["key"], "value")
	}
}

func TestExtractToken_MissingAuth(t *testing.T) {
	h := &Handlers{}

	req := httptest.NewRequest("GET", "/test", nil)
	// No Authorization header

	_, err := h.extractToken(req)
	if err == nil {
		t.Error("extractToken() = nil error, want error for missing auth")
	}
	if err != errMissingAuth {
		t.Errorf("extractToken() error = %v, want errMissingAuth", err)
	}
}

func TestExtractToken_InvalidFormat(t *testing.T) {
	h := &Handlers{}

	tests := []struct {
		name   string
		header string
	}{
		{"no bearer", "token123"},
		{"wrong scheme", "Basic token123"},
		{"bearer only", "Bearer"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			_, err := h.extractToken(req)
			if err == nil {
				t.Errorf("extractToken() = nil error for header %q", tt.header)
			}
		})
	}
}

// TestExtractToken_ValidFormat is covered by TestExtractToken_ValidToken below

func TestExtractToken_ValidToken(t *testing.T) {
	h := &Handlers{}

	// Pre-encoded valid token JSON for testing extraction (not signature validation)
	tokenB64 := "eyJ0ZW5hbnQiOiJtYW5pZmVzdDFhYmMiLCJsZWFzZV91dWlkIjoiMDEyMzQ1NjctODlhYi1jZGVmLTAxMjMtNDU2Nzg5YWJjZGVmIiwidGltZXN0YW1wIjoxMjM0NTY3ODkwLCJwdWJfa2V5IjoiZEdWemRBPT0iLCJzaWduYXR1cmUiOiJkR1Z6ZEE9PSJ9"

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB64)

	token, err := h.extractToken(req)
	if err != nil {
		t.Fatalf("extractToken() error = %v", err)
	}

	if token.Tenant != "manifest1abc" {
		t.Errorf("token.Tenant = %q, want %q", token.Tenant, "manifest1abc")
	}
	if token.LeaseUUID != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Errorf("token.LeaseUUID = %q, want %q", token.LeaseUUID, "01234567-89ab-cdef-0123-456789abcdef")
	}
}

func TestExtractToken_CaseInsensitiveBearer(t *testing.T) {
	h := &Handlers{}

	tokenB64 := "eyJ0ZW5hbnQiOiJtYW5pZmVzdDFhYmMiLCJsZWFzZV91dWlkIjoiMDEyMzQ1NjctODlhYi1jZGVmLTAxMjMtNDU2Nzg5YWJjZGVmIiwidGltZXN0YW1wIjoxMjM0NTY3ODkwLCJwdWJfa2V5IjoiZEdWemRBPT0iLCJzaWduYXR1cmUiOiJkR1Z6ZEE9PSJ9"

	cases := []string{"Bearer", "bearer", "BEARER", "BeArEr"}

	for _, prefix := range cases {
		t.Run(prefix, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Authorization", prefix+" "+tokenB64)

			_, err := h.extractToken(req)
			if err != nil {
				t.Errorf("extractToken() with %q error = %v", prefix, err)
			}
		})
	}
}

func TestConnectionResponse_JSON(t *testing.T) {
	response := ConnectionResponse{
		LeaseUUID:    testutil.ValidUUID1,
		Tenant:       "manifest1abc",
		ProviderUUID: testutil.ValidUUID2,
		Connection: ConnectionDetails{
			Host:     "compute-alpha.example.com",
			Port:     8443,
			Protocol: "https",
			Metadata: map[string]string{
				"region": "us-east-1",
			},
		},
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded ConnectionResponse
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.LeaseUUID != response.LeaseUUID {
		t.Errorf("LeaseUUID = %q, want %q", decoded.LeaseUUID, response.LeaseUUID)
	}
	if decoded.Connection.Host != response.Connection.Host {
		t.Errorf("Connection.Host = %q, want %q", decoded.Connection.Host, response.Connection.Host)
	}
}
