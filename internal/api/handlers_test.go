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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	assert.Equal(t, testutil.ValidUUID1, response.ProviderUUID)

	// Check Content-Type header
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

// TestHealthCheck_ChainUnavailable tests health check when chain ping fails.
func TestHealthCheck_ChainUnavailable(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return fmt.Errorf("connection refused")
		},
	}

	h := &Handlers{
		client:       chainClient,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	// Should return 503 Service Unavailable
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "unhealthy", response.Status)

	// Check that chain check shows unhealthy with error message
	chainCheck, ok := response.Checks["chain"]
	require.True(t, ok, "missing chain check in response")
	assert.Equal(t, "unhealthy", chainCheck.Status)
	assert.Equal(t, "connection refused", chainCheck.Message)
}

// TestHealthCheck_ChainHealthy tests health check when chain is available.
func TestHealthCheck_ChainHealthy(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return nil // Healthy
		},
	}

	h := &Handlers{
		client:       chainClient,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)

	chainCheck, ok := response.Checks["chain"]
	require.True(t, ok, "missing chain check in response")
	assert.Equal(t, "healthy", chainCheck.Status)
}

// TestHealthCheck_BackendUnhealthy tests health check when a backend is unavailable.
func TestHealthCheck_BackendUnhealthy(t *testing.T) {
	// Create a backend server that returns unhealthy
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("backend down"))
			return
		}
		w.WriteHeader(http.StatusOK)
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
	require.NoError(t, err)

	h := &Handlers{
		client:        nil, // No chain client
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	// Should return 503 due to unhealthy backend
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "unhealthy", response.Status)

	// Check backend health status
	backendCheck, ok := response.Checks["backend:test-backend"]
	require.True(t, ok, "missing backend check in response, got: %v", response.Checks)
	assert.Equal(t, "unhealthy", backendCheck.Status)
}

// TestHealthCheck_AllHealthy tests health check when both chain and backend are healthy.
func TestHealthCheck_AllHealthy(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return nil
		},
	}

	// Create a healthy backend
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "healthy-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
		},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)

	// Both checks should be healthy
	assert.Equal(t, "healthy", response.Checks["chain"].Status)
	assert.Equal(t, "healthy", response.Checks["backend:healthy-backend"].Status)
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, "test error", http.StatusBadRequest)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var response ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "test error", response.Error)
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

func TestWriteJSON(t *testing.T) {
	data := map[string]string{"key": "value"}

	rec := httptest.NewRecorder()
	writeJSON(rec, data, http.StatusOK)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var response map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "value", response["key"])
}

func TestExtractToken_MissingAuth(t *testing.T) {
	h := &Handlers{}

	req := httptest.NewRequest("GET", "/test", nil)
	// No Authorization header

	_, err := h.extractToken(req)
	assert.Error(t, err)
	assert.Equal(t, errMissingAuth, err)
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
			assert.Error(t, err, "extractToken() = nil error for header %q", tt.header)
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
	require.NoError(t, err)

	assert.Equal(t, "manifest1abc", token.Tenant)
	assert.Equal(t, "01234567-89ab-cdef-0123-456789abcdef", token.LeaseUUID)
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
			assert.NoError(t, err, "extractToken() with %q error = %v", prefix, err)
		})
	}
}

func TestConnectionResponse_JSON(t *testing.T) {
	response := ConnectionResponse{
		LeaseUUID:    testutil.ValidUUID1,
		Tenant:       "manifest1abc",
		ProviderUUID: testutil.ValidUUID2,
		Connection: ConnectionDetails{
			Host: "compute-alpha.example.com",
			Ports: map[string]PortMapping{
				"443/tcp": {HostIP: "0.0.0.0", HostPort: 8443},
			},
			Protocol: "https",
			Metadata: map[string]string{
				"region": "us-east-1",
			},
		},
	}

	jsonBytes, err := json.Marshal(response)
	require.NoError(t, err)

	var decoded ConnectionResponse
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))

	assert.Equal(t, response.LeaseUUID, decoded.LeaseUUID)
	assert.Equal(t, response.Connection.Host, decoded.Connection.Host)
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

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_extracts_connection_details", func(t *testing.T) {
		// Create backend server that returns valid lease info
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"host":     "compute-alpha.example.com",
					"protocol": "https",
					"ports": map[string]any{
						"443/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "8443"},
					},
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		// Verify response fields
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)

		// Verify connection details extraction
		assert.Equal(t, "compute-alpha.example.com", response.Connection.Host)
		assert.Equal(t, 8443, response.Connection.Ports["443/tcp"].HostPort)
		assert.Equal(t, "https", response.Connection.Protocol)
		assert.Equal(t, "us-east-1", response.Connection.Metadata["region"])
		assert.Equal(t, "test-backend", response.Connection.Metadata["backend"])
	})

	t.Run("happy_path_with_multiple_ports", func(t *testing.T) {
		// Test that multiple port mappings are handled correctly
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"host":"test.example.com","ports":{"80/tcp":{"host_ip":"0.0.0.0","host_port":"8080"},"443/tcp":{"host_ip":"0.0.0.0","host_port":"8443"}},"protocol":"grpc"}`))
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Len(t, response.Connection.Ports, 2)
		assert.Equal(t, 8080, response.Connection.Ports["80/tcp"].HostPort)
		assert.Equal(t, 8443, response.Connection.Ports["443/tcp"].HostPort)
	})
}

// TestExtractConnectionDetails tests the extractConnectionDetails helper function.
func TestExtractConnectionDetails(t *testing.T) {
	tests := []struct {
		name              string
		input             backend.LeaseInfo
		expectedHost      string
		expectedProto     string
		expectedMeta      map[string]string
		expectedPorts     map[string]PortMapping
		expectedInstances []InstanceInfo
	}{
		{
			name: "full info with ports and metadata",
			input: backend.LeaseInfo{
				"host":     "test.example.com",
				"protocol": "https",
				"ports": map[string]map[string]string{
					"80/tcp": {"host_ip": "0.0.0.0", "host_port": "8080"},
				},
				"metadata": map[string]string{"key": "value"},
			},
			expectedHost:  "test.example.com",
			expectedProto: "https",
			expectedMeta:  map[string]string{"key": "value"},
			expectedPorts: map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 8080}},
		},
		{
			name: "ports with any type values",
			input: backend.LeaseInfo{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "8443"},
				},
			},
			expectedHost:  "test.example.com",
			expectedMeta:  map[string]string{},
			expectedPorts: map[string]PortMapping{"443/tcp": {HostIP: "0.0.0.0", HostPort: 8443}},
		},
		{
			name:         "empty info",
			input:        backend.LeaseInfo{},
			expectedMeta: map[string]string{},
		},
		{
			name: "missing optional fields",
			input: backend.LeaseInfo{
				"host": "test.example.com",
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{},
		},
		{
			name: "unknown string fields go to metadata",
			input: backend.LeaseInfo{
				"host":        "test.example.com",
				"region":      "us-east-1",
				"backend":     "kubernetes",
				"credentials": map[string]string{"token": "secret"}, // non-string, ignored
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
		},
		{
			name: "unknown fields merged with explicit metadata",
			input: backend.LeaseInfo{
				"host":     "test.example.com",
				"metadata": map[string]string{"key": "value"},
				"region":   "us-west-2",
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{"key": "value", "region": "us-west-2"},
		},
		{
			name: "single instance with ports",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": float64(0),
						"container_id":   "abc123def456",
						"image":          "nginx:latest",
						"status":         "running",
						"ports": map[string]any{
							"80/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "32768"},
						},
					},
				},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123def456",
					Image:         "nginx:latest",
					Status:        "running",
					Ports:         map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 32768}},
				},
			},
		},
		{
			name: "multiple instances",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": float64(0),
						"container_id":   "container1",
						"image":          "nginx:latest",
						"status":         "running",
						"ports": map[string]any{
							"80/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "32768"},
						},
					},
					map[string]any{
						"instance_index": float64(1),
						"container_id":   "container2",
						"image":          "redis:alpine",
						"status":         "running",
						"ports": map[string]any{
							"6379/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "32769"},
						},
					},
				},
				"metadata": map[string]any{"backend": "docker"},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{"backend": "docker"},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "container1",
					Image:         "nginx:latest",
					Status:        "running",
					Ports:         map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 32768}},
				},
				{
					InstanceIndex: 1,
					ContainerID:   "container2",
					Image:         "redis:alpine",
					Status:        "running",
					Ports:         map[string]PortMapping{"6379/tcp": {HostIP: "0.0.0.0", HostPort: 32769}},
				},
			},
		},
		{
			name: "instance with numeric host_port",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": float64(0),
						"container_id":   "abc123",
						"ports": map[string]any{
							"8080/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": float64(32770)},
						},
					},
				},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123",
					Ports:         map[string]PortMapping{"8080/tcp": {HostIP: "0.0.0.0", HostPort: 32770}},
				},
			},
		},
		{
			name: "instance without ports",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": float64(0),
						"container_id":   "abc123",
						"image":          "busybox:latest",
						"status":         "running",
					},
				},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123",
					Image:         "busybox:latest",
					Status:        "running",
				},
			},
		},
		{
			name: "instances field is not included in metadata",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": float64(0),
						"container_id":   "abc123",
					},
				},
				"region": "us-east-1",
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{"region": "us-east-1"},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractConnectionDetails(tt.input)

			assert.Equal(t, tt.expectedHost, result.Host)
			assert.Equal(t, tt.expectedProto, result.Protocol)
			assert.Len(t, result.Metadata, len(tt.expectedMeta))
			for k, v := range tt.expectedMeta {
				assert.Equal(t, v, result.Metadata[k])
			}
			assert.Len(t, result.Ports, len(tt.expectedPorts))
			for k, v := range tt.expectedPorts {
				assert.Equal(t, v, result.Ports[k])
			}

			// Verify instances
			assert.Len(t, result.Instances, len(tt.expectedInstances))
			for i, expected := range tt.expectedInstances {
				actual := result.Instances[i]
				assert.Equal(t, expected.InstanceIndex, actual.InstanceIndex)
				assert.Equal(t, expected.ContainerID, actual.ContainerID)
				assert.Equal(t, expected.Image, actual.Image)
				assert.Equal(t, expected.Status, actual.Status)
				assert.Len(t, actual.Ports, len(expected.Ports))
				for k, v := range expected.Ports {
					assert.Equal(t, v, actual.Ports[k])
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
		require.NoError(t, err)
		defer tokenTracker.Close()

		// Create a backend server
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusOK, rec1.Code, "first request status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		// Second request with same token should be rejected
		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+validToken)
		req2 = mux.SetURLVars(req2, map[string]string{"lease_uuid": leaseUUID})

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		assert.Equal(t, http.StatusUnauthorized, rec2.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec2.Body).Decode(&errResp))
		assert.Equal(t, "unauthorized", errResp.Error)
	})

	t.Run("different_tokens_both_succeed", func(t *testing.T) {
		dbPath := t.TempDir() + "/tokens.db"
		tokenTracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tokenTracker.Close()

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
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
		require.NoError(t, err)

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

		assert.Equal(t, http.StatusOK, rec1.Code, "first token status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		req2 = mux.SetURLVars(req2, map[string]string{"lease_uuid": leaseUUID})

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code, "second token status = %d, want %d; body: %s", rec2.Code, http.StatusOK, rec2.Body.String())
	})

	t.Run("no_tracker_allows_replay", func(t *testing.T) {
		// When no token tracker is configured, replays should be allowed
		// (graceful degradation)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
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
		require.NoError(t, err)

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

			assert.Equal(t, http.StatusOK, rec.Code, "request %d status = %d, want %d; body: %s", i+1, rec.Code, http.StatusOK, rec.Body.String())
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

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
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

		assert.Equal(t, http.StatusNotFound, rec.Code)
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

		assert.Equal(t, http.StatusForbidden, rec.Code)
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

		assert.Equal(t, http.StatusForbidden, rec.Code)
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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, "LEASE_STATE_PENDING", response.State)
		assert.False(t, response.RequiresPayload)
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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.True(t, response.RequiresPayload)
		assert.False(t, response.PayloadReceived)
		assert.False(t, response.ProvisioningStarted)
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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.True(t, response.PayloadReceived)
		assert.True(t, response.ProvisioningStarted)
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

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
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

		assert.Equal(t, http.StatusBadRequest, rec.Code)
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

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
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

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
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

		assert.Equal(t, http.StatusNotFound, rec.Code)
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

		assert.Equal(t, http.StatusForbidden, rec.Code)
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

		assert.Equal(t, http.StatusForbidden, rec.Code)
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

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
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
			"host": "test.example.com",
			"ports": map[string]any{
				"443/tcp": map[string]any{
					"host_ip":   "0.0.0.0",
					"host_port": "8443",
				},
			},
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
	require.NoError(t, err)

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

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service temporarily unavailable", errResp.Error)
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

		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "unauthorized", errResp.Error)
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

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
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

				assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "status = %d, want %d for error %q", rec.Code, http.StatusServiceUnavailable, dbErr)
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
	require.NoError(t, err)

	var decoded CallbackResponse
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))

	assert.Equal(t, response.Status, decoded.Status)
	assert.Equal(t, response.Message, decoded.Message)
}

// TestCallbackResponse_OmitEmptyMessage tests that empty message is omitted.
func TestCallbackResponse_OmitEmptyMessage(t *testing.T) {
	response := CallbackResponse{
		Status:  "ok",
		Message: "", // Should be omitted
	}

	jsonBytes, err := json.Marshal(response)
	require.NoError(t, err)

	jsonStr := string(jsonBytes)
	assert.False(t, strings.Contains(jsonStr, "message"), "JSON should not contain 'message' when empty, got %s", jsonStr)
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
			assert.Equal(t, tt.want, got)
		})
	}
}
