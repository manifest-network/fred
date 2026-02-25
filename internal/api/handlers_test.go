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

	// Check that chain check shows unhealthy with sanitized message
	// (raw error details are logged server-side, not exposed to clients)
	chainCheck, ok := response.Checks["chain"]
	require.True(t, ok, "missing chain check in response")
	assert.Equal(t, "unhealthy", chainCheck.Status)
	assert.Equal(t, "chain connectivity failed", chainCheck.Message)
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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
			name: "instance with typed port map from docker backend",
			input: backend.LeaseInfo{
				"host": "docker-host.example.com",
				"instances": []any{
					map[string]any{
						"instance_index": 0,
						"container_id":   "abc123",
						"ports": map[string]map[string]string{
							"80/tcp": {"host_ip": "0.0.0.0", "host_port": "8080"},
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
					Ports:         map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 8080}},
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
				assert.Equal(t, expected.FQDN, actual.FQDN)
				assert.Len(t, actual.Ports, len(expected.Ports))
				for k, v := range expected.Ports {
					assert.Equal(t, v, actual.Ports[k])
				}
			}
		})
	}
}

func TestExtractConnectionDetails_Services(t *testing.T) {
	t.Run("stack with two services", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "abc123",
							"image":          "nginx:latest",
							"status":         "running",
							"ports": map[string]any{
								"80/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "8080"},
							},
						},
					},
				},
				"db": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "def456",
							"image":          "postgres:16",
							"status":         "running",
						},
					},
				},
			},
		}

		result := extractConnectionDetails(input)
		assert.Equal(t, "docker-host.example.com", result.Host)
		assert.Nil(t, result.Instances, "stack response should not have flat instances")
		require.Len(t, result.Services, 2)

		// web service
		webSvc := result.Services["web"]
		require.Len(t, webSvc.Instances, 1)
		assert.Equal(t, 0, webSvc.Instances[0].InstanceIndex)
		assert.Equal(t, "abc123", webSvc.Instances[0].ContainerID)
		assert.Equal(t, "nginx:latest", webSvc.Instances[0].Image)
		assert.Equal(t, "running", webSvc.Instances[0].Status)
		require.Len(t, webSvc.Instances[0].Ports, 1)
		assert.Equal(t, PortMapping{HostIP: "0.0.0.0", HostPort: 8080}, webSvc.Instances[0].Ports["80/tcp"])

		// db service
		dbSvc := result.Services["db"]
		require.Len(t, dbSvc.Instances, 1)
		assert.Equal(t, "postgres:16", dbSvc.Instances[0].Image)
	})

	t.Run("service instance ports with typed map from docker backend", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": 2,
							"container_id":   "abc123",
							"ports": map[string]map[string]string{
								"80/tcp": {"host_ip": "0.0.0.0", "host_port": "8080"},
							},
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		webSvc := result.Services["web"]
		require.Len(t, webSvc.Instances, 1)
		assert.Equal(t, 2, webSvc.Instances[0].InstanceIndex)
		require.Len(t, webSvc.Instances[0].Ports, 1)
		assert.Equal(t, PortMapping{HostIP: "0.0.0.0", HostPort: 8080}, webSvc.Instances[0].Ports["80/tcp"])
	})

	t.Run("services field not in metadata", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "abc",
						},
					},
				},
			},
			"region": "us-east-1",
		}

		result := extractConnectionDetails(input)
		assert.Len(t, result.Services, 1)
		assert.Equal(t, "us-east-1", result.Metadata["region"])
		// "services" should NOT appear in metadata.
		_, inMeta := result.Metadata["services"]
		assert.False(t, inMeta)
	})
}

func TestExtractConnectionDetails_FQDN(t *testing.T) {
	t.Run("direct fqdn in lease info", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"fqdn": "myapp.example.com",
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "myapp.example.com", result.FQDN)
		_, inMeta := result.Metadata["fqdn"]
		assert.False(t, inMeta, "fqdn should not appear in metadata")
	})

	t.Run("fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"instances": []any{
				map[string]any{
					"instance_index": float64(0),
					"container_id":   "abc123",
					"fqdn":           "inst.example.com",
				},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "inst.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "inst.example.com", result.Instances[0].FQDN)
	})

	t.Run("direct fqdn takes precedence over instance fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"fqdn": "top-level.example.com",
			"instances": []any{
				map[string]any{
					"instance_index": float64(0),
					"container_id":   "abc123",
					"fqdn":           "instance-level.example.com",
				},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "top-level.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "instance-level.example.com", result.Instances[0].FQDN)
	})

	t.Run("multi-instance each with unique fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"instances": []any{
				map[string]any{
					"instance_index": float64(0),
					"container_id":   "container0",
					"fqdn":           "0-abc1234.example.com",
				},
				map[string]any{
					"instance_index": float64(1),
					"container_id":   "container1",
					"fqdn":           "1-def5678.example.com",
				},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "0-abc1234.example.com", result.FQDN, "top-level propagated from first instance")
		require.Len(t, result.Instances, 2)
		assert.Equal(t, "0-abc1234.example.com", result.Instances[0].FQDN)
		assert.Equal(t, "1-def5678.example.com", result.Instances[1].FQDN)
	})

	t.Run("no fqdn anywhere", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"instances": []any{
				map[string]any{
					"instance_index": float64(0),
					"container_id":   "abc123",
				},
			},
		}
		result := extractConnectionDetails(input)
		assert.Empty(t, result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Empty(t, result.Instances[0].FQDN)
	})

	t.Run("service-level fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "abc123",
							"fqdn":           "web.example.com",
						},
					},
				},
				"db": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "def456",
							"fqdn":           "db.example.com",
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 2)
		assert.Equal(t, "web.example.com", result.Services["web"].FQDN)
		assert.Equal(t, "db.example.com", result.Services["db"].FQDN)
		assert.Equal(t, "web.example.com", result.Services["web"].Instances[0].FQDN)
		assert.Equal(t, "db.example.com", result.Services["db"].Instances[0].FQDN)
	})

	t.Run("service with multi-instance unique fqdns", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "web0",
							"fqdn":           "web-0-abc.example.com",
						},
						map[string]any{
							"instance_index": float64(1),
							"container_id":   "web1",
							"fqdn":           "web-1-def.example.com",
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		webSvc := result.Services["web"]
		assert.Equal(t, "web-0-abc.example.com", webSvc.FQDN, "service-level propagated from first instance")
		require.Len(t, webSvc.Instances, 2)
		assert.Equal(t, "web-0-abc.example.com", webSvc.Instances[0].FQDN)
		assert.Equal(t, "web-1-def.example.com", webSvc.Instances[1].FQDN)
	})

	t.Run("explicit service fqdn takes precedence over instance fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"fqdn": "explicit-web.example.com",
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "abc123",
							"fqdn":           "instance-web.example.com",
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		assert.Equal(t, "explicit-web.example.com", result.Services["web"].FQDN)
		assert.Equal(t, "instance-web.example.com", result.Services["web"].Instances[0].FQDN)
	})

	t.Run("service without fqdn in instances", func(t *testing.T) {
		input := backend.LeaseInfo{
			"host": "docker-host.example.com",
			"services": map[string]any{
				"web": map[string]any{
					"instances": []any{
						map[string]any{
							"instance_index": float64(0),
							"container_id":   "abc123",
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		assert.Empty(t, result.Services["web"].FQDN)
		assert.Empty(t, result.Services["web"].Instances[0].FQDN)
	})
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
		req1.SetPathValue("lease_uuid", leaseUUID)

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code, "first request status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		// Second request with same token should be rejected
		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+validToken)
		req2.SetPathValue("lease_uuid", leaseUUID)

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
		req1.SetPathValue("lease_uuid", leaseUUID)

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code, "first token status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		req2.SetPathValue("lease_uuid", leaseUUID)

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
		for i := range 2 {
			req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

// mockStatusChecker implements StatusChecker for testing.
type mockStatusChecker struct {
	hasPayload    map[string]bool
	isInFlight    map[string]bool
	inFlightCount int
}

func (m *mockStatusChecker) HasPayload(leaseUUID string) (bool, error) {
	if m.hasPayload == nil {
		return false, nil
	}
	return m.hasPayload[leaseUUID], nil
}

func (m *mockStatusChecker) IsInFlight(leaseUUID string) bool {
	if m.isInFlight == nil {
		return false
	}
	return m.isInFlight[leaseUUID]
}

func (m *mockStatusChecker) InFlightCount() int {
	return m.inFlightCount
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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
	})

	t.Run("active_with_provision_status", func(t *testing.T) {
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

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID: leaseUUID,
					Status:    backend.ProvisionStatusReady,
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
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Equal(t, "ready", response.ProvisionStatus)
	})

	t.Run("active_with_updating_provision", func(t *testing.T) {
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

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID: leaseUUID,
					Status:    backend.ProvisionStatusUpdating,
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
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Equal(t, "updating", response.ProvisionStatus)
	})

	t.Run("active_no_backend_router", func(t *testing.T) {
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
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Empty(t, response.ProvisionStatus)
	})

	t.Run("active_backend_error", func(t *testing.T) {
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

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Empty(t, response.ProvisionStatus)
	})

	t.Run("invalid_uuid_returns_400", func(t *testing.T) {
		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/not-a-uuid/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", "not-a-uuid")

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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

func (m *mockTokenTracker) Healthy() error {
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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
		req.SetPathValue("lease_uuid", leaseUUID)

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
				req.SetPathValue("lease_uuid", leaseUUID)

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

// TestGetLeaseProvision tests the GetLeaseProvision endpoint.
func TestGetLeaseProvision(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Chain client that returns a lease (any state)
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

	t.Run("happy_path_ready", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID:    leaseUUID,
					ProviderUUID: providerUUID,
					Status:       backend.ProvisionStatusReady,
					FailCount:    0,
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
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		assert.Equal(t, "ready", response.Status)
		assert.Equal(t, 0, response.FailCount)
		assert.Empty(t, response.LastError)
	})

	t.Run("happy_path_failed_with_error", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID:    leaseUUID,
					ProviderUUID: providerUUID,
					Status:       backend.ProvisionStatusFailed,
					FailCount:    3,
					LastError:    "container exited unexpectedly: exit_code=1",
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
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "failed", response.Status)
		assert.Equal(t, 3, response.FailCount)
		assert.Equal(t, "container exited unexpectedly: exit_code=1", response.LastError)
	})

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not provisioned"}`))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "provision not found", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("invalid_uuid_returns_400", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/not-a-uuid/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", "not-a-uuid")

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		mismatchClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:       mismatchClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("works_with_non_active_lease", func(t *testing.T) {
		// Provision diagnostics should work even for rejected/closed leases
		rejectedClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_REJECTED,
					}, nil
				}
				return nil, nil
			},
		}

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(backend.ProvisionInfo{
				LeaseUUID: leaseUUID,
				Status:    backend.ProvisionStatusFailed,
				FailCount: 1,
				LastError: "image pull failed",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        rejectedClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "failed", response.Status)
		assert.Equal(t, "image pull failed", response.LastError)
	})
}

// TestGetLeaseLogs tests the GetLeaseLogs endpoint.
func TestGetLeaseLogs(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

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

	t.Run("happy_path_single_container", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/logs/"+leaseUUID) && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{
					"0": "Starting server...\nListening on :8080\n",
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
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseLogsResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		require.Len(t, response.Logs, 1)
		assert.Contains(t, response.Logs["0"], "Listening on :8080")
	})

	t.Run("happy_path_multiple_containers", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"0": "web server logs\n",
				"1": "worker logs\n",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseLogsResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		require.Len(t, response.Logs, 2)
		assert.Equal(t, "web server logs\n", response.Logs["0"])
		assert.Equal(t, "worker logs\n", response.Logs["1"])
	})

	t.Run("tail_parameter_forwarded", func(t *testing.T) {
		var receivedTail string
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedTail = r.URL.Query().Get("tail")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"0": "logs\n"})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs?tail=50", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "50", receivedTail)
	})

	t.Run("tail_invalid_returns_400", func(t *testing.T) {
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: "http://unused",
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		tests := []struct {
			name  string
			query string
		}{
			{"negative", "?tail=-1"},
			{"zero", "?tail=0"},
			{"not_a_number", "?tail=abc"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
				req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs"+tt.query, nil)
				req.Header.Set("Authorization", "Bearer "+validToken)
				req.SetPathValue("lease_uuid", leaseUUID)

				rec := httptest.NewRecorder()
				h.GetLeaseLogs(rec, req)

				assert.Equal(t, http.StatusBadRequest, rec.Code, "query=%s", tt.query)

				var errResp ErrorResponse
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, "tail must be a positive integer", errResp.Error)
			})
		}
	})

	t.Run("tail_exceeds_max_returns_400", func(t *testing.T) {
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: "http://unused",
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs?tail=10001", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "tail must not exceed 10000", errResp.Error)
	})

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not provisioned"}`))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "logs not found", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		mismatchClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:       mismatchClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("default_tail_100", func(t *testing.T) {
		var receivedTail string
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedTail = r.URL.Query().Get("tail")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"0": "logs\n"})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// No ?tail= parameter — should default to 100
		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "100", receivedTail)
	})
}

// TestRestartLease_BackendIntegration tests the backend integration path
// in RestartLease using httptest.Server and a real backend.Router.
func TestRestartLease_BackendIntegration(t *testing.T) {
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

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusNotFound)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("invalid_state_returns_409", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusConflict)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusConflict, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid state for restart", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusInternalServerError)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_202", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusAccepted)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var response map[string]string
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "restarting", response["status"])
	})
}

// TestUpdateLease_BackendIntegration tests the backend integration path
// in UpdateLease using httptest.Server and a real backend.Router.
func TestUpdateLease_BackendIntegration(t *testing.T) {
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

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("missing_payload_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":""}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "payload is required", errResp.Error)
	})

	t.Run("invalid_body_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader("not json"))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid request body", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusNotFound)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("invalid_state_returns_409", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusConflict)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusConflict, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid state for update", errResp.Error)
	})

	t.Run("validation_error_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid manifest"})
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusInternalServerError)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_202", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusAccepted)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var response map[string]string
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "updating", response["status"])
	})
}

// TestGetLeaseReleases_BackendIntegration tests the backend integration path
// in GetLeaseReleases using httptest.Server and a real backend.Router.
func TestGetLeaseReleases_BackendIntegration(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// GetLeaseReleases uses requireActive=false, so it calls GetLease
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

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.WriteHeader(http.StatusNotFound)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.WriteHeader(http.StatusInternalServerError)
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_releases", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]backend.ReleaseInfo{
					{
						Version:   1,
						Image:     "nginx:1.25",
						Status:    "superseded",
						CreatedAt: now.Add(-1 * time.Hour),
					},
					{
						Version:   2,
						Image:     "nginx:1.26",
						Status:    "active",
						CreatedAt: now,
					},
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseReleasesResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		require.Len(t, response.Releases, 2)
		assert.Equal(t, 1, response.Releases[0].Version)
		assert.Equal(t, "nginx:1.25", response.Releases[0].Image)
		assert.Equal(t, "superseded", response.Releases[0].Status)
		assert.Equal(t, 2, response.Releases[1].Version)
		assert.Equal(t, "nginx:1.26", response.Releases[1].Image)
		assert.Equal(t, "active", response.Releases[1].Status)
	})

	t.Run("empty_releases_returns_empty_array", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]backend.ReleaseInfo{})
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

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseReleasesResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Empty(t, response.Releases)
	})
}

// --- Health endpoint stats tests ---

func TestHealthCheck_WithStatusChecker(t *testing.T) {
	sc := &mockStatusChecker{inFlightCount: 42}
	h := &Handlers{
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
		statusChecker: sc,
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	require.NotNil(t, response.Stats, "stats should be present when statusChecker is configured")
	assert.Equal(t, 42, response.Stats.InFlightProvisions)
}

func TestHealthCheck_WithoutStatusChecker(t *testing.T) {
	h := &Handlers{
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
		// statusChecker is nil
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	assert.Nil(t, response.Stats, "stats should be absent without statusChecker")
}
