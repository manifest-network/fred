package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
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

// mockChainClient implements ChainClient for testing.
type mockChainClient struct {
	getLeaseFunc       func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	getActiveLeaseFunc func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	pingFunc           func(ctx context.Context) error
}

func (m *mockChainClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getLeaseFunc != nil {
		return m.getLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

func (m *mockChainClient) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getActiveLeaseFunc != nil {
		return m.getActiveLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

func (m *mockChainClient) Ping(ctx context.Context) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

// TestGetLeaseConnection_BackendIntegration tests the backend integration path
// in GetLeaseConnection using httptest.Server and a real backend.Router.
func TestGetLeaseConnection_BackendIntegration(t *testing.T) {
	// Create a test key pair for signing tokens
	kp := testutil.NewTestKeyPair("test-tenant")

	// Test lease details
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Create a valid auth token
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	// Mock chain client that returns an active lease
	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil, // No backend router configured
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "service not configured" {
			t.Errorf("error = %q, want %q", errResp.Error, "service not configured")
		}
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		// Create backend server that returns 404 (not provisioned)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("not found"))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		// Create real backend client and router
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "lease not yet provisioned" {
			t.Errorf("error = %q, want %q", errResp.Error, "lease not yet provisioned")
		}
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		// Create backend server that returns 500
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "internal server error" {
			t.Errorf("error = %q, want %q", errResp.Error, "internal server error")
		}
	})

	t.Run("happy_path_extracts_connection_details", func(t *testing.T) {
		// Create backend server that returns valid lease info
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"host":     "compute-alpha.example.com",
					"port":     8443,
					"protocol": "https",
					"metadata": map[string]any{
						"region":  "us-east-1",
						"backend": "test-backend",
					},
					"credentials": map[string]any{"token": "secret"}, // non-string map, ignored
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response ConnectionResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Verify response fields
		if response.LeaseUUID != leaseUUID {
			t.Errorf("LeaseUUID = %q, want %q", response.LeaseUUID, leaseUUID)
		}
		if response.Tenant != kp.Address {
			t.Errorf("Tenant = %q, want %q", response.Tenant, kp.Address)
		}
		if response.ProviderUUID != providerUUID {
			t.Errorf("ProviderUUID = %q, want %q", response.ProviderUUID, providerUUID)
		}

		// Verify connection details extraction
		if response.Connection.Host != "compute-alpha.example.com" {
			t.Errorf("Connection.Host = %q, want %q", response.Connection.Host, "compute-alpha.example.com")
		}
		if response.Connection.Port != 8443 {
			t.Errorf("Connection.Port = %d, want %d", response.Connection.Port, 8443)
		}
		if response.Connection.Protocol != "https" {
			t.Errorf("Connection.Protocol = %q, want %q", response.Connection.Protocol, "https")
		}
		if response.Connection.Metadata["region"] != "us-east-1" {
			t.Errorf("Connection.Metadata[region] = %q, want %q", response.Connection.Metadata["region"], "us-east-1")
		}
		if response.Connection.Metadata["backend"] != "test-backend" {
			t.Errorf("Connection.Metadata[backend] = %q, want %q", response.Connection.Metadata["backend"], "test-backend")
		}
	})

	t.Run("happy_path_with_int_port", func(t *testing.T) {
		// Test that integer ports (not float64 from JSON) are handled correctly
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				// Return raw JSON to ensure port comes as float64 from json.Unmarshal
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"host":"test.example.com","port":9000,"protocol":"grpc"}`))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response ConnectionResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response.Connection.Port != 9000 {
			t.Errorf("Connection.Port = %d, want %d", response.Connection.Port, 9000)
		}
	})
}

// TestExtractConnectionDetails tests the extractConnectionDetails helper function.
func TestExtractConnectionDetails(t *testing.T) {
	tests := []struct {
		name     string
		input    backend.LeaseInfo
		expected ConnectionDetails
	}{
		{
			name: "full info with string metadata",
			input: backend.LeaseInfo{
				"host":     "test.example.com",
				"port":     float64(8080), // JSON numbers are float64
				"protocol": "https",
				"metadata": map[string]string{"key": "value"},
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Port:     8080,
				Protocol: "https",
				Metadata: map[string]string{"key": "value"},
			},
		},
		{
			name: "full info with any metadata",
			input: backend.LeaseInfo{
				"host":     "test.example.com",
				"port":     float64(8080),
				"protocol": "https",
				"metadata": map[string]any{"key": "value", "number": 123},
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Port:     8080,
				Protocol: "https",
				Metadata: map[string]string{"key": "value"}, // non-string values filtered
			},
		},
		{
			name: "int port instead of float64",
			input: backend.LeaseInfo{
				"host": "test.example.com",
				"port": 9000, // int instead of float64
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Port:     9000,
				Metadata: map[string]string{},
			},
		},
		{
			name:  "empty info",
			input: backend.LeaseInfo{},
			expected: ConnectionDetails{
				Metadata: map[string]string{},
			},
		},
		{
			name: "missing optional fields",
			input: backend.LeaseInfo{
				"host": "test.example.com",
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Metadata: map[string]string{},
			},
		},
		{
			name: "unknown string fields go to metadata",
			input: backend.LeaseInfo{
				"host":        "test.example.com",
				"port":        float64(8080),
				"region":      "us-east-1",
				"backend":     "kubernetes",
				"credentials": map[string]string{"token": "secret"}, // non-string, ignored
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Port:     8080,
				Metadata: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
			},
		},
		{
			name: "unknown fields merged with explicit metadata",
			input: backend.LeaseInfo{
				"host":     "test.example.com",
				"metadata": map[string]string{"key": "value"},
				"region":   "us-west-2",
			},
			expected: ConnectionDetails{
				Host:     "test.example.com",
				Metadata: map[string]string{"key": "value", "region": "us-west-2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractConnectionDetails(tt.input)

			if result.Host != tt.expected.Host {
				t.Errorf("Host = %q, want %q", result.Host, tt.expected.Host)
			}
			if result.Port != tt.expected.Port {
				t.Errorf("Port = %d, want %d", result.Port, tt.expected.Port)
			}
			if result.Protocol != tt.expected.Protocol {
				t.Errorf("Protocol = %q, want %q", result.Protocol, tt.expected.Protocol)
			}
			if len(result.Metadata) != len(tt.expected.Metadata) {
				t.Errorf("Metadata length = %d, want %d", len(result.Metadata), len(tt.expected.Metadata))
			}
			for k, v := range tt.expected.Metadata {
				if result.Metadata[k] != v {
					t.Errorf("Metadata[%q] = %q, want %q", k, result.Metadata[k], v)
				}
			}
		})
	}
}

// TestGetLeaseConnection_TokenReplayProtection tests the token replay protection.
func TestGetLeaseConnection_TokenReplayProtection(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Mock chain client that returns an active lease
	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("replayed_token_rejected", func(t *testing.T) {
		dbPath := t.TempDir() + "/tokens.db"
		tokenTracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tokenTracker.Close()

		// Create a backend server
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "test.example.com",
				"port":     8443,
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  tokenTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// Create a valid token
		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		// First request should succeed
		req1 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req1.Header.Set("Authorization", "Bearer "+validToken)
		req1 = mux.SetURLVars(req1, map[string]string{"lease_uuid": leaseUUID})

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		if rec1.Code != http.StatusOK {
			t.Errorf("first request status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())
		}

		// Second request with same token should be rejected
		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+validToken)
		req2 = mux.SetURLVars(req2, map[string]string{"lease_uuid": leaseUUID})

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		if rec2.Code != http.StatusUnauthorized {
			t.Errorf("second request (replay) status = %d, want %d", rec2.Code, http.StatusUnauthorized)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec2.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "unauthorized" {
			t.Errorf("error = %q, want %q", errResp.Error, "unauthorized")
		}
	})

	t.Run("different_tokens_both_succeed", func(t *testing.T) {
		dbPath := t.TempDir() + "/tokens.db"
		tokenTracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tokenTracker.Close()

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "test.example.com",
				"port":     8443,
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  tokenTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// Create two different tokens (different timestamps = different signatures)
		// Timestamps are Unix seconds, so we need different seconds for different signatures
		now := time.Now()
		token1 := testutil.CreateTestToken(kp, leaseUUID, now)
		token2 := testutil.CreateTestToken(kp, leaseUUID, now.Add(1*time.Second))

		// Both requests should succeed since they're different tokens
		req1 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req1.Header.Set("Authorization", "Bearer "+token1)
		req1 = mux.SetURLVars(req1, map[string]string{"lease_uuid": leaseUUID})

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		if rec1.Code != http.StatusOK {
			t.Errorf("first token status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())
		}

		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		req2 = mux.SetURLVars(req2, map[string]string{"lease_uuid": leaseUUID})

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		if rec2.Code != http.StatusOK {
			t.Errorf("second token status = %d, want %d; body: %s", rec2.Code, http.StatusOK, rec2.Body.String())
		}
	})

	t.Run("no_tracker_allows_replay", func(t *testing.T) {
		// When no token tracker is configured, replays should be allowed
		// (graceful degradation)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "test.example.com",
				"port":     8443,
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  nil, // No tracker
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		// Both requests should succeed without tracker
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
			req.Header.Set("Authorization", "Bearer "+validToken)
			req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

			rec := httptest.NewRecorder()
			h.GetLeaseConnection(rec, req)

			if rec.Code != http.StatusOK {
				t.Errorf("request %d status = %d, want %d; body: %s", i+1, rec.Code, http.StatusOK, rec.Body.String())
			}
		}
	})
}

// TestGetLeaseConnection_ChainErrors tests chain-related error paths.
func TestGetLeaseConnection_ChainErrors(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	t.Run("chain_error_returns_500", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
	})

	t.Run("lease_not_found_returns_404", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, nil // Lease not found
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different", // Different tenant
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("provider_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: testutil.ValidUUID3, // Different provider
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})
}

// mockStatusChecker implements StatusChecker for testing.
type mockStatusChecker struct {
	hasPayload map[string]bool
	isInFlight map[string]bool
}

func (m *mockStatusChecker) HasPayload(leaseUUID string) bool {
	if m.hasPayload == nil {
		return false
	}
	return m.hasPayload[leaseUUID]
}

func (m *mockStatusChecker) IsInFlight(leaseUUID string) bool {
	if m.isInFlight == nil {
		return false
	}
	return m.isInFlight[leaseUUID]
}

// TestGetLeaseStatus tests the GetLeaseStatus endpoint.
func TestGetLeaseStatus(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	t.Run("pending_without_meta_hash", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     nil, // No payload required
					}, nil
				}
				return nil, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response LeaseStatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response.LeaseUUID != leaseUUID {
			t.Errorf("LeaseUUID = %q, want %q", response.LeaseUUID, leaseUUID)
		}
		if response.State != "LEASE_STATE_PENDING" {
			t.Errorf("State = %q, want %q", response.State, "LEASE_STATE_PENDING")
		}
		if response.RequiresPayload {
			t.Errorf("RequiresPayload = %v, want false", response.RequiresPayload)
		}
	})

	t.Run("pending_with_meta_hash_no_payload", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     []byte{1, 2, 3, 4}, // Has meta hash - requires payload
					}, nil
				}
				return nil, nil
			},
		}

		statusChecker := &mockStatusChecker{
			hasPayload: map[string]bool{leaseUUID: false},
			isInFlight: map[string]bool{leaseUUID: false},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: statusChecker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response LeaseStatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !response.RequiresPayload {
			t.Errorf("RequiresPayload = %v, want true", response.RequiresPayload)
		}
		if response.PayloadReceived {
			t.Errorf("PayloadReceived = %v, want false", response.PayloadReceived)
		}
		if response.ProvisioningStarted {
			t.Errorf("ProvisioningStarted = %v, want false", response.ProvisioningStarted)
		}
	})

	t.Run("pending_with_payload_received", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     []byte{1, 2, 3, 4},
					}, nil
				}
				return nil, nil
			},
		}

		statusChecker := &mockStatusChecker{
			hasPayload: map[string]bool{leaseUUID: true},
			isInFlight: map[string]bool{leaseUUID: true},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: statusChecker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response LeaseStatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !response.PayloadReceived {
			t.Errorf("PayloadReceived = %v, want true", response.PayloadReceived)
		}
		if !response.ProvisioningStarted {
			t.Errorf("ProvisioningStarted = %v, want true", response.ProvisioningStarted)
		}
	})

	t.Run("active_lease", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		var response LeaseStatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response.State != "LEASE_STATE_ACTIVE" {
			t.Errorf("State = %q, want %q", response.State, "LEASE_STATE_ACTIVE")
		}
	})

	t.Run("invalid_uuid_returns_400", func(t *testing.T) {
		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/not-a-uuid/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": "not-a-uuid"})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		// No Authorization header
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("lease_uuid_mismatch_returns_401", func(t *testing.T) {
		// Token is for different lease UUID
		differentLeaseToken := testutil.CreateTestToken(kp, testutil.ValidUUID3, time.Now())

		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+differentLeaseToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("lease_not_found_returns_404", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, nil // Lease not found
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("provider_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: testutil.ValidUUID3, // Different provider
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("chain_error_returns_500", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
	})
}

// mockTokenTracker implements a mock TokenTracker for testing.
type mockTokenTracker struct {
	tryUseFunc func(signature string) error
}

func (m *mockTokenTracker) TryUse(signature string) error {
	if m.tryUseFunc != nil {
		return m.tryUseFunc(signature)
	}
	return nil
}

func (m *mockTokenTracker) Close() error {
	return nil
}

// TestTokenTracker_FailClosed tests that database errors result in 503 Service Unavailable.
func TestTokenTracker_FailClosed(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"host":     "test.example.com",
			"port":     8443,
			"protocol": "https",
		})
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
		},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	t.Run("database_error_returns_503", func(t *testing.T) {
		// Create a mock token tracker that returns a database error
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return fmt.Errorf("bbolt: database not open")
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "service temporarily unavailable" {
			t.Errorf("error = %q, want %q", errResp.Error, "service temporarily unavailable")
		}
	})

	t.Run("replay_detected_returns_401", func(t *testing.T) {
		// Ensure replay detection still returns 401, not 503
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return ErrTokenAlreadyUsed
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}

		var errResp ErrorResponse
		if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if errResp.Error != "unauthorized" {
			t.Errorf("error = %q, want %q", errResp.Error, "unauthorized")
		}
	})

	t.Run("success_returns_200", func(t *testing.T) {
		// Ensure successful token use still works
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return nil // Success
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}
	})

	t.Run("various_database_errors_return_503", func(t *testing.T) {
		dbErrors := []error{
			fmt.Errorf("bbolt: database not open"),
			fmt.Errorf("disk full"),
			fmt.Errorf("i/o timeout"),
			fmt.Errorf("database is locked"),
		}

		for _, dbErr := range dbErrors {
			t.Run(dbErr.Error(), func(t *testing.T) {
				mockTracker := &mockTokenTracker{
					tryUseFunc: func(signature string) error {
						return dbErr
					},
				}

				h := &Handlers{
					client:        chainClient,
					backendRouter: router,
					tokenTracker:  mockTracker,
					providerUUID:  providerUUID,
					bech32Prefix:  "manifest",
				}

				validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

				req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
				req.Header.Set("Authorization", "Bearer "+validToken)
				req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})

				rec := httptest.NewRecorder()
				h.GetLeaseConnection(rec, req)

				if rec.Code != http.StatusServiceUnavailable {
					t.Errorf("status = %d, want %d for error %q", rec.Code, http.StatusServiceUnavailable, dbErr)
				}
			})
		}
	})
}

// TestCallbackResponse_JSON tests the CallbackResponse type serialization.
func TestCallbackResponse_JSON(t *testing.T) {
	response := CallbackResponse{
		Status:  "already_processed",
		Message: "callback for this lease was already handled",
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded CallbackResponse
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Status != response.Status {
		t.Errorf("Status = %q, want %q", decoded.Status, response.Status)
	}
	if decoded.Message != response.Message {
		t.Errorf("Message = %q, want %q", decoded.Message, response.Message)
	}
}

// TestCallbackResponse_OmitEmptyMessage tests that empty message is omitted.
func TestCallbackResponse_OmitEmptyMessage(t *testing.T) {
	response := CallbackResponse{
		Status:  "ok",
		Message: "", // Should be omitted
	}

	jsonBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	jsonStr := string(jsonBytes)
	if strings.Contains(jsonStr, "message") {
		t.Errorf("JSON should not contain 'message' when empty, got %s", jsonStr)
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "lease connection endpoint",
			path: "/v1/leases/550e8400-e29b-41d4-a716-446655440000/connection",
			want: "/v1/leases/{uuid}/connection",
		},
		{
			name: "lease status endpoint",
			path: "/v1/leases/550e8400-e29b-41d4-a716-446655440000/status",
			want: "/v1/leases/{uuid}/status",
		},
		{
			name: "lease data endpoint",
			path: "/v1/leases/550e8400-e29b-41d4-a716-446655440000/data",
			want: "/v1/leases/{uuid}/data",
		},
		{
			name: "health endpoint (no UUID)",
			path: "/health",
			want: "/health",
		},
		{
			name: "metrics endpoint (no UUID)",
			path: "/metrics",
			want: "/metrics",
		},
		{
			name: "callbacks endpoint with UUID",
			path: "/callbacks/provision",
			want: "/callbacks/provision",
		},
		{
			name: "multiple UUIDs in path",
			path: "/v1/providers/550e8400-e29b-41d4-a716-446655440000/leases/123e4567-e89b-12d3-a456-426614174000",
			want: "/v1/providers/{uuid}/leases/{uuid}",
		},
		{
			name: "uppercase UUID",
			path: "/v1/leases/550E8400-E29B-41D4-A716-446655440000/connection",
			want: "/v1/leases/{uuid}/connection",
		},
		{
			name: "root path",
			path: "/",
			want: "/",
		},
		{
			name: "empty path",
			path: "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizePath(tt.path)
			if got != tt.want {
				t.Errorf("normalizePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
