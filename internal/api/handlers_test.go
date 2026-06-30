package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

// TestNewHandlers_AppliesWebSocketDefaults pins the WebSocket security
// defaults set by NewHandlers. StreamLeaseEvents has a defensive fallback
// that substitutes the production defaults and logs an slog.Error if either
// field is non-positive, so a future change that drops either field init
// from NewHandlers would NOT silently disable the mitigations — but it
// would force every /events connection onto the fallback path, emitting a
// per-connection error log and making the fallback the de-facto main code
// path instead of a safety net. This test keeps the constructor honest so
// the fallback stays reserved for genuinely misconfigured callers.
func TestNewHandlers_AppliesWebSocketDefaults(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		ProviderUUID: testutil.ValidUUID1,
		Bech32Prefix: "manifest",
	})

	assert.Equal(t, wsDefaultMaxMessageSize, h.wsMaxMessageSize,
		"NewHandlers must set wsMaxMessageSize so the production path doesn't fall through to StreamLeaseEvents' defensive default (which would log an slog.Error per connection)")
	assert.Equal(t, wsDefaultMaxConnLifetime, h.wsMaxConnLifetime,
		"NewHandlers must set wsMaxConnLifetime so the production path doesn't fall through to StreamLeaseEvents' defensive default (which would log an slog.Error per connection)")
}

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
				Host:     "test.example.com",
				Protocol: "https",
				Ports: map[string]backend.PortBinding{
					"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
				},
				Metadata: map[string]string{"key": "value"},
			},
			expectedHost:  "test.example.com",
			expectedProto: "https",
			expectedMeta:  map[string]string{"key": "value"},
			expectedPorts: map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 8080}},
		},
		{
			name:         "empty info",
			input:        backend.LeaseInfo{},
			expectedMeta: map[string]string{},
		},
		{
			name: "missing optional fields",
			input: backend.LeaseInfo{
				Host: "test.example.com",
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{},
		},
		{
			name: "metadata passed through",
			input: backend.LeaseInfo{
				Host:     "test.example.com",
				Metadata: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
		},
		{
			name: "single instance with ports",
			input: backend.LeaseInfo{
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "abc123def456",
						Image:         "nginx:latest",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"80/tcp": {HostIP: "0.0.0.0", HostPort: "32768"},
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
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "container1",
						Image:         "nginx:latest",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"80/tcp": {HostIP: "0.0.0.0", HostPort: "32768"},
						},
					},
					{
						InstanceIndex: 1,
						ContainerID:   "container2",
						Image:         "redis:alpine",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"6379/tcp": {HostIP: "0.0.0.0", HostPort: "32769"},
						},
					},
				},
				Metadata: map[string]string{"backend": "docker"},
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
			name: "instance without ports",
			input: backend.LeaseInfo{
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "abc123",
						Image:         "busybox:latest",
						Status:        "running",
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
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 0,
							ContainerID:   "abc123",
							Image:         "nginx:latest",
							Status:        "running",
							Ports: map[string]backend.PortBinding{
								"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
							},
						},
					},
				},
				"db": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 0,
							ContainerID:   "def456",
							Image:         "postgres:16",
							Status:        "running",
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

	t.Run("service with instance index", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 2,
							ContainerID:   "abc123",
							Ports: map[string]backend.PortBinding{
								"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
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
}

func TestExtractConnectionDetails_FQDN(t *testing.T) {
	t.Run("direct fqdn in lease info", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			FQDN: "myapp.example.com",
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "myapp.example.com", result.FQDN)
	})

	t.Run("fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123", FQDN: "inst.example.com"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "inst.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "inst.example.com", result.Instances[0].FQDN)
	})

	t.Run("direct fqdn takes precedence over instance fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			FQDN: "top-level.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123", FQDN: "instance-level.example.com"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "top-level.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "instance-level.example.com", result.Instances[0].FQDN)
	})

	t.Run("multi-instance each with unique fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "container0", FQDN: "0-abc1234.example.com"},
				{InstanceIndex: 1, ContainerID: "container1", FQDN: "1-def5678.example.com"},
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
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Empty(t, result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Empty(t, result.Instances[0].FQDN)
	})

	t.Run("service-level fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "abc123", FQDN: "web.example.com"},
				}},
				"db": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "def456", FQDN: "db.example.com"},
				}},
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
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "web0", FQDN: "web-0-abc.example.com"},
					{InstanceIndex: 1, ContainerID: "web1", FQDN: "web-1-def.example.com"},
				}},
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
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					FQDN: "explicit-web.example.com",
					Instances: []backend.LeaseInstance{
						{InstanceIndex: 0, ContainerID: "abc123", FQDN: "instance-web.example.com"},
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
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "abc123"},
				}},
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

// mockPlacementLookup implements PlacementLookup for testing.
type mockPlacementLookup struct {
	getFunc     func(leaseUUID string) string
	healthyFunc func() error
}

func (m *mockPlacementLookup) Get(leaseUUID string) string {
	if m.getFunc != nil {
		return m.getFunc(leaseUUID)
	}
	return ""
}

func (m *mockPlacementLookup) Healthy() error {
	if m.healthyFunc != nil {
		return m.healthyFunc()
	}
	return nil
}

// TestResolveBackend_PlacementRouting tests that resolveBackend checks
// the placement store first and falls back to SKU routing.
func TestResolveBackend_PlacementRouting(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

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

	t.Run("placement_routes_to_correct_backend", func(t *testing.T) {
		// Set up two backends — "placed-backend" has the lease via placement,
		// "default-backend" is the SKU fallback.
		placedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "placed-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer placedServer.Close()

		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "default-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		placedBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "placed-backend",
			BaseURL: placedServer.URL,
			Timeout: 5 * time.Second,
		})
		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: placedBackend},
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		placement := &mockPlacementLookup{
			getFunc: func(uuid string) string {
				if uuid == leaseUUID {
					return "placed-backend"
				}
				return ""
			},
		}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "placed-host.example.com", resp.Connection.Host,
			"should route to placement backend, not default")
	})

	t.Run("stale_placement_falls_back_to_sku", func(t *testing.T) {
		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "default-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		// Placement returns a backend name that doesn't exist in the router
		placement := &mockPlacementLookup{
			getFunc: func(uuid string) string {
				return "removed-backend"
			},
		}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "default-host.example.com", resp.Connection.Host,
			"should fall back to SKU routing when placement backend is missing")
	})

	t.Run("no_placement_uses_sku_routing", func(t *testing.T) {
		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "sku-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		// Placement returns empty string (no placement record)
		placement := &mockPlacementLookup{}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "sku-host.example.com", resp.Connection.Host)
	})
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

// --- GetWorkloads tests ---

// newFilteredProvisionServer returns an httptest.Server that serves provisions
// on GET /provisions, supporting the optional ?lease_uuid=... filter.
// All provisions are stored upfront; filter is applied per-request.
func newFilteredProvisionServer(t *testing.T, provisions []backend.ProvisionInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/provisions" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		filter := r.URL.Query()["lease_uuid"]
		out := provisions
		if len(filter) > 0 {
			wanted := make(map[string]struct{}, len(filter))
			for _, u := range filter {
				wanted[u] = struct{}{}
			}
			out = make([]backend.ProvisionInfo, 0, len(filter))
			for _, p := range provisions {
				if _, ok := wanted[p.LeaseUUID]; ok {
					out = append(out, p)
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(backend.ListProvisionsResponse{Provisions: out})
	}))
}

// newFailingServer returns an httptest.Server that responds 500 to all
// requests except /health.
func newFailingServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

// newWorkloadsHandler builds a Handlers with a backend router wired to the
// given backend entries.
func newWorkloadsHandler(t *testing.T, entries []backend.BackendEntry) *Handlers {
	t.Helper()
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	return &Handlers{
		providerUUID:  testutil.ValidUUID1,
		backendRouter: router,
	}
}

// callGetWorkloads invokes GetWorkloads with the given lease_uuid query params.
func callGetWorkloads(t *testing.T, h *Handlers, leaseUUIDs ...string) (int, WorkloadLookupResponse) {
	t.Helper()
	q := url.Values{"lease_uuid": leaseUUIDs}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)
	var resp WorkloadLookupResponse
	if rec.Code == http.StatusOK {
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	}
	return rec.Code, resp
}

func TestGetWorkloads_RequiresLeaseUUID(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	req := httptest.NewRequest("GET", "/workloads", nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "lease_uuid query parameter required")
}

func TestGetWorkloads_RejectsOverCap(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	tooMany := make([]string, backend.MaxLookupUUIDs+1)
	for i := range tooMany {
		tooMany[i] = testutil.ValidUUID2
	}
	q := url.Values{"lease_uuid": tooMany}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "too many lease_uuid values")
}

func TestGetWorkloads_RejectsBadUUID(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	q := url.Values{"lease_uuid": []string{"not-a-uuid"}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid lease_uuid")
}

func TestGetWorkloads_NilRouter(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, response.Workloads)
	assert.Empty(t, response.Workloads)
	assert.NotNil(t, response.Warnings)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_NonStackImageRoundTrip(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID:    testutil.ValidUUID2,
			ProviderUUID: testutil.ValidUUID1,
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
			Image:        "registry.local:5000/manifest-network/fred:v1.2.3@sha256:deadbeef",
			SKU:          "docker-micro",
			Quantity:     2,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	w, ok := response.Workloads[testutil.ValidUUID2]
	require.True(t, ok, "lease should appear keyed by UUID")
	assert.Equal(t, backend.ProvisionStatusReady, w.Status)
	assert.Equal(t, "test-backend", w.BackendName)

	require.Len(t, w.Items, 1)
	assert.Equal(t, "docker-micro", w.Items[0].SKU)
	// Backend reports a host:port + tag + digest reference; the handler must
	// strip the tag and digest while keeping the registry port intact. This
	// is the end-to-end proof that provisionToWorkloadEntry routes image
	// fields through stripImageTag — a future regression that inlines a
	// naive "split on last colon" would fail here by returning "registry".
	assert.Equal(t, "registry.local:5000/manifest-network/fred", w.Items[0].Image)
	assert.Equal(t, 2, w.Items[0].Count)
	assert.Empty(t, w.Items[0].ServiceName)
}

func TestGetWorkloads_StackImageRoundTrip(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID:    testutil.ValidUUID2,
			ProviderUUID: testutil.ValidUUID1,
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
			Quantity:     3,
			Items: []backend.LeaseItem{
				{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
				{SKU: "docker-large", Quantity: 1, ServiceName: "db"},
			},
			ServiceImages: map[string]string{
				"web": "nginx:1.25",
				"db":  "postgres:16",
			},
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	w, ok := response.Workloads[testutil.ValidUUID2]
	require.True(t, ok)
	require.Len(t, w.Items, 2)

	// Items preserve the backend's order — match by ServiceName.
	byName := make(map[string]WorkloadItem, 2)
	for _, item := range w.Items {
		byName[item.ServiceName] = item
	}
	require.Contains(t, byName, "web")
	require.Contains(t, byName, "db")
	assert.Equal(t, "docker-micro", byName["web"].SKU)
	// Tags stripped on the way out — see stripImageTag.
	assert.Equal(t, "nginx", byName["web"].Image)
	assert.Equal(t, 2, byName["web"].Count)
	assert.Equal(t, "docker-large", byName["db"].SKU)
	assert.Equal(t, "postgres", byName["db"].Image)
	assert.Equal(t, 1, byName["db"].Count)
}

// TestGetWorkloads_DedupesRepeatedLeaseUUIDs locks the input-dedupe behavior:
// a request with the same lease_uuid repeated must call each backend with the
// deduped set (so the backend doesn't iterate-and-emit-twice) and must return
// exactly one entry in the response. Without dedupe, the merge loop would log
// a misleading "lease reported by multiple backends" warning.
func TestGetWorkloads_DedupesRepeatedLeaseUUIDs(t *testing.T) {
	var capturedFilter []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		capturedFilter = r.URL.Query()["lease_uuid"]
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
			Provisions: []backend.ProvisionInfo{
				{
					LeaseUUID: testutil.ValidUUID2,
					Status:    backend.ProvisionStatusReady,
					Image:     "nginx:1.25",
					SKU:       "docker-micro",
					Quantity:  1,
				},
			},
		})
	}))
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Send the same UUID three times.
	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID2, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	// Backend was called with exactly one lease_uuid (the dedupe happens before fan-out).
	assert.Equal(t, []string{testutil.ValidUUID2}, capturedFilter,
		"backend should receive the deduped UUID list, not the raw input")

	// Response contains exactly one workload entry, no warnings.
	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_FanOutAcrossBackends(t *testing.T) {
	srv1 := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer srv1.Close()

	srv2 := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID3,
			Status:    backend.ProvisionStatusReady,
			Image:     "redis:7",
			SKU:       "docker-large",
			Quantity:  1,
		},
	})
	defer srv2.Close()

	client1 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-1", BaseURL: srv1.URL, Timeout: 5 * time.Second,
	})
	client2 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-2", BaseURL: srv2.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: client1, IsDefault: true},
		{Backend: client2},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID3)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 2)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.Contains(t, response.Workloads, testutil.ValidUUID3)
	assert.Equal(t, "backend-1", response.Workloads[testutil.ValidUUID2].BackendName)
	assert.Equal(t, "backend-2", response.Workloads[testutil.ValidUUID3].BackendName)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_BackendErrorWarning(t *testing.T) {
	// One healthy backend has the lease; another backend is failing.
	// Expect: 200 OK, lease present in workloads map, warning naming the failed backend.
	healthySrv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer healthySrv.Close()

	failingSrv := newFailingServer(t)
	defer failingSrv.Close()

	healthyClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "healthy-backend", BaseURL: healthySrv.URL, Timeout: 5 * time.Second,
	})
	failingClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "failing-backend", BaseURL: failingSrv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: healthyClient, IsDefault: true},
		{Backend: failingClient},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	// Healthy backend's data is still present despite the other backend failing
	// (errgroup must NOT cancel siblings).
	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)

	require.Len(t, response.Warnings, 1)
	assert.Contains(t, response.Warnings[0], `backend "failing-backend" unavailable`)
}

func TestGetWorkloads_UnknownLeasesOmitted(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Request both a known and an unknown lease.
	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID3)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.NotContains(t, response.Workloads, testutil.ValidUUID3)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_AllUnknownReturnsEmptyMap(t *testing.T) {
	srv := newFilteredProvisionServer(t, nil)
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Decode raw JSON to verify the wire format is `{}` and `[]`, not `null`.
	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"workloads":{}`, "empty workloads should serialize as object, not null")
	assert.Contains(t, body, `"warnings":[]`, "empty warnings should serialize as array, not null")
}

func TestGetWorkloads_AllBackendsFail(t *testing.T) {
	failingSrv1 := newFailingServer(t)
	defer failingSrv1.Close()
	failingSrv2 := newFailingServer(t)
	defer failingSrv2.Close()

	client1 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-1", BaseURL: failingSrv1.URL, Timeout: 5 * time.Second,
	})
	client2 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-2", BaseURL: failingSrv2.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: client1, IsDefault: true},
		{Backend: client2},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)
	assert.Empty(t, response.Workloads)
	assert.Len(t, response.Warnings, 2, "all backend failures should be surfaced as warnings")
}

func TestGetWorkloads_StackNilServiceImages(t *testing.T) {
	// Simulates cold restart where Items survive but StackManifest (and thus
	// ServiceImages) is nil. The "image" key should be absent from the JSON
	// (omitempty), not present as an empty string.
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Quantity:  2,
			Items: []backend.LeaseItem{
				{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
			},
			ServiceImages: nil,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&raw))

	workloads, ok := raw["workloads"].(map[string]any)
	require.True(t, ok, "workloads should be a JSON object")
	require.Contains(t, workloads, testutil.ValidUUID2)

	entry, ok := workloads[testutil.ValidUUID2].(map[string]any)
	require.True(t, ok)

	itemsAny, ok := entry["items"].([]any)
	require.True(t, ok, "items should be a JSON array")
	require.Len(t, itemsAny, 1)

	item, ok := itemsAny[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "web", item["service_name"])
	assert.Equal(t, "docker-micro", item["sku"])
	_, hasImage := item["image"]
	assert.False(t, hasImage, "image key should be absent from JSON when ServiceImages is nil")
}

// TestStripImageTag locks the redaction contract. Cases fall into three
// groups: valid references (must be stripped correctly while preserving
// host:port), deliberately-malformed inputs (pinned so a future refactor
// toward a grammar-aware parser is a conscious decision, not a silent
// behavior change), and degenerate edge cases (empty, bare digest).
func TestStripImageTag(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		// --- Valid references ---
		{"empty", "", ""},
		{"no tag", "nginx", "nginx"},
		{"simple tag", "nginx:1.25", "nginx"},
		{"patch tag", "nginx:1.25.3", "nginx"},
		{"latest tag", "nginx:latest", "nginx"},
		{"dash tag", "nginx:1.25-alpine3.18", "nginx"},
		// Mixed case: refs are spec-lowercase but the helper must not
		// silently normalize — that would corrupt the admin UI display.
		{"mixed case registry", "GHCR.IO/Org/Repo:v1", "GHCR.IO/Org/Repo"},
		{"registry with org", "ghcr.io/manifest-network/fred:v1.2.3", "ghcr.io/manifest-network/fred"},
		// host:port is the critical preservation case. Naive "strip after
		// last colon" would collapse these to "registry" or "localhost".
		{"registry with port no tag", "registry.local:5000/org/repo", "registry.local:5000/org/repo"},
		{"registry with port and tag", "registry.local:5000/org/repo:v1", "registry.local:5000/org/repo"},
		{"localhost with port and tag", "localhost:5000/foo:v1", "localhost:5000/foo"},
		// Bare localhost without a port is a legitimate reference and
		// distinguishes "first segment has a dot" from "has a slash".
		{"localhost no port with tag", "localhost/foo:v1", "localhost/foo"},
		// IPv6 literal host — the colons inside the bracketed host are
		// before the last "/" so they pass through untouched. Pinned so a
		// future "parse host properly" rewrite doesn't silently regress.
		{"ipv6 host with port and tag", "[::1]:5000/foo:v1", "[::1]:5000/foo"},
		// --- Digest handling ---
		{"digest only", "nginx@sha256:abc123", "nginx"},
		{"tag and digest", "nginx:1.25@sha256:abc123", "nginx"},
		{"registry port tag digest", "registry.local:5000/org/repo:v1@sha256:abc", "registry.local:5000/org/repo"},
		// Port + digest with *no* tag — exercises the @-strip leaving the
		// host:port intact and the colon-after-slash rule then correctly
		// not-stripping the remaining port colon.
		{"registry port digest no tag", "registry.local:5000/org/repo@sha256:abc", "registry.local:5000/org/repo"},
		// --- Malformed / degenerate inputs (contract pins) ---
		// Empty tag / empty digest: return the name. These are garbage-in,
		// but the behavior is locked so a defensive trim doesn't change it.
		{"empty tag", "nginx:", "nginx"},
		{"empty digest", "nginx@", "nginx"},
		// Double "@" — not grammar-valid. Pinned so the "strip from first
		// @" semantics documented in the helper stays stable.
		{"double at", "nginx@sha256:a@b", "nginx"},
		// Colon before slash: invalid Docker ref. The helper leaves it
		// untouched because the "tag colon" is not after the last "/".
		// Pinned as a non-behavior — garbage-in, garbage-out.
		{"colon before slash", "foo:1.25/bar", "foo:1.25/bar"},
		// Bare digest with no name — degenerate; returns empty. Pinned so
		// a future "require non-empty repo" check is a conscious decision.
		{"bare digest", "@sha256:abc", ""},
		// Trailing slash: no tag to strip, no crash on colon == -1.
		{"trailing slash", "nginx/", "nginx/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stripImageTag(tc.in))
		})
	}
}

func TestGetWorkloads_ContextCancelled(t *testing.T) {
	// Simulate a slow backend that blocks until context is cancelled,
	// then verify that GetWorkloads short-circuits with no body when the
	// request context is cancelled mid-fan-out.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		// Server side: context cancelled, just return.
	}))
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "slow-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	ctx, cancel := context.WithCancel(context.Background())
	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequestWithContext(ctx, "GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	// Cancel after a brief delay so the goroutine has time to start.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	h.GetWorkloads(rec, req)

	// Handler should bail without writing a body. We don't strictly enforce a
	// status code (the recorder defaults to 200 if no header is written), but
	// the body must NOT be a populated WorkloadLookupResponse.
	body := rec.Body.String()
	assert.NotContains(t, body, `"workloads":`,
		"handler should not write a body when request context is cancelled")
}

// fromLeaseUUID is a distinct UUID used as the "original retained lease" in
// RestoreLease tests. It must differ from leaseUUID (the new lease in the request
// path) so the handler can't accidentally confuse the two.
const fromLeaseUUID = "fedcba98-7654-3210-fedc-ba9876543210"

// TestRestoreLease_ForwardsAnd202 verifies the happy path: backend /restore
// returns 202 and the handler responds 202 {"status":"provisioning"}, forwarding
// the from_lease_uuid in the body sent to the backend.
func TestRestoreLease_ForwardsAnd202(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// PENDING lease — restore is called on fresh, not-yet-active leases.
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	var receivedBody []byte
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			var err error
			receivedBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
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
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it instead of guessing an arbitrary backend (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())

	var response map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	assert.Equal(t, "provisioning", response["status"])

	// Verify backend received the from_lease_uuid.
	require.NotNil(t, receivedBody, "backend should have received a request body")
	var backendReq map[string]any
	require.NoError(t, json.Unmarshal(receivedBody, &backendReq))
	assert.Equal(t, fromLeaseUUID, backendReq["from_lease_uuid"])
}

// TestRestoreLease_RejectsNonPendingLease verifies that restore refuses a target
// lease that is not PENDING (e.g. ACTIVE or CLOSED) with 409 and never reaches the
// backend — so a tenant cannot deploy onto an already-active or unbilled-closed
// lease, mirroring the provisioning path's LEASE_STATE_PENDING gate.
func TestRestoreLease_RejectsNonPendingLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state billingtypes.LeaseState
	}{
		{"active", billingtypes.LEASE_STATE_ACTIVE},
		{"closed", billingtypes.LEASE_STATE_CLOSED},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
							State:        tc.state, // NOT pending
						}, nil
					}
					return nil, nil
				},
			}

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("backend must NOT be called for a non-pending lease: %s %s", r.Method, r.URL.Path)
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
			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestRestoreLease_NoRetention404 verifies that a 422 from the backend
// (ErrNotRetained) is surfaced as a 404 to the caller.
func TestRestoreLease_NoRetention404(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			// 422 Unprocessable Entity → ErrNotRetained in the HTTP client.
			w.WriteHeader(http.StatusUnprocessableEntity)
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

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and exercises the ErrNotRetained path (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)
}

// TestRestoreLease_InsufficientResources503 verifies that a 503 from the backend
// (ErrInsufficientResources via the HTTP client's 503→sentinel mapping) is
// surfaced as a 503 to the tenant, matching how Provision surfaces capacity —
// NOT a 409.
func TestRestoreLease_InsufficientResources503(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			// 503 → ErrInsufficientResources in the HTTP client.
			w.WriteHeader(http.StatusServiceUnavailable)
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

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and exercises the ErrInsufficientResources path (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "insufficient resources to restore", errResp.Error)
}

// TestRestoreLease_DemoteExceedsTier_422 verifies that a backend Restore returning
// ErrDemoteDataExceedsTier is surfaced to the tenant as HTTP 422 Unprocessable
// Entity — distinct from the ErrValidation→400 mapping and the ErrNotRetained→404
// mapping. Uses a MockBackend (not an HTTP server) so the error is injected at the
// Go interface level without needing an HTTP status-code mapping for the sentinel.
// (ENG-438)
func TestRestoreLease_DemoteExceedsTier_422(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	// MockBackend with RestoreFn configured to return ErrDemoteDataExceedsTier
	// wrapped in an additional message, exactly as checkDemoteFit would return it.
	mockB := backend.NewMockBackend(backend.MockBackendConfig{Name: "test-backend"})
	mockB.RestoreFn = func(_ context.Context, _ backend.RestoreRequest) error {
		return fmt.Errorf("%w: service %q: 3000 bytes used exceeds disk_mb=2 cap (2097152 bytes)",
			backend.ErrDemoteDataExceedsTier, "app")
	}

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockB, IsDefault: true}},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", rec.Body.String())
}

// TestRestoreLease_PendingLeaseAuthenticates verifies that requireActive=false
// allows a PENDING lease to authenticate successfully (i.e. the handler reaches
// the backend call rather than 404-ing on auth).
func TestRestoreLease_PendingLeaseAuthenticates(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// GetLease returns a PENDING lease; GetActiveLease would return nil (not found).
	backendCalled := false
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			// Returning nil simulates "not active" — would cause 404 if called.
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			backendCalled = true
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
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and can verify requireActive=false auth (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	// The handler must NOT 404 on auth — it should reach the backend.
	assert.NotEqual(t, http.StatusNotFound, rec.Code, "handler must not 404 on PENDING lease auth; body: %s", rec.Body.String())
	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())
	assert.True(t, backendCalled, "backend should have been called for a PENDING lease")
}

// TestRestoreLease_MalformedFromLease400 verifies that a syntactically invalid
// from_lease_uuid is rejected with 400 before the backend is contacted.
func TestRestoreLease_MalformedFromLease400(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend should NOT be called for a malformed from_lease_uuid: %s %s", r.Method, r.URL.Path)
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

	invalidValues := []string{"not-a-uuid", "../etc/passwd", "short", "00000000000000000000000000000000x"}
	for _, bad := range invalidValues {
		t.Run(bad, func(t *testing.T) {
			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			reqBody := `{"from_lease_uuid":"` + bad + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code, "from_lease_uuid=%q body: %s", bad, rec.Body.String())

			var errResp ErrorResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Contains(t, errResp.Error, "uuid", "error message should mention uuid")
		})
	}
}

// TestRestoreLease_MissingFromLease400 verifies that an absent or empty
// from_lease_uuid is rejected with 400 before the backend is contacted.
func TestRestoreLease_MissingFromLease400(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend should NOT be called for missing from_lease_uuid: %s %s", r.Method, r.URL.Path)
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

	cases := []struct {
		name string
		body string
	}{
		{"empty_field", `{"from_lease_uuid":""}`},
		{"absent_field", `{}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code, "case=%q body: %s", tc.name, rec.Body.String())

			var errResp ErrorResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, "from_lease_uuid is required", errResp.Error)
		})
	}
}

// TestRestoreLease_RoutesToSourcePlacementBackend verifies that the handler
// routes the Restore call to the backend recorded in placement for the SOURCE
// lease (from_lease_uuid), not to an arbitrary backend resolved from the new
// lease (which has no placement). This is the core ENG-333 invariant: retained
// volumes live only on the backend that originally provisioned the source lease.
func TestRestoreLease_RoutesToSourcePlacementBackend(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	// Two named backends: only "backend-src" should receive the Restore call.
	srcCalled := false
	srcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			srcCalled = true
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request on src backend: %s %s", r.Method, r.URL.Path)
	}))
	defer srcServer.Close()

	otherCalled := false
	otherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherCalled = true
		t.Errorf("backend-other must NOT be called, got: %s %s", r.Method, r.URL.Path)
	}))
	defer otherServer.Close()

	srcBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-src",
		BaseURL: srcServer.URL,
		Timeout: 5 * time.Second,
	})
	otherBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-other",
		BaseURL: otherServer.URL,
		Timeout: 5 * time.Second,
	})
	// backend-other is the SKU-routing default; backend-src is reachable ONLY via
	// the source lease's placement. This makes the test a real regression guard:
	// if RestoreLease reverted to new-lease routing (resolveBackend → Route(sku)),
	// it would land on the default backend-other and the test would FAIL — not
	// silently pass as it would if backend-src were also the default.
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: srcBackend},                    // NOT default; reachable only via placement[fromLeaseUUID]
			{Backend: otherBackend, IsDefault: true}, // default; old new-lease routing would land here
		},
	})
	require.NoError(t, err)

	// Placement maps the SOURCE lease to "backend-src"; the new lease has no placement.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "backend-src"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())
	assert.True(t, srcCalled, "backend-src should have received the Restore call")
	assert.False(t, otherCalled, "backend-other must NOT have been called")
}

// TestRestoreLease_NoSourcePlacement_Returns404 verifies that when the
// placement lookup returns "" for the source lease (no retained data recorded
// on any backend), the handler responds 404 without contacting any backend.
func TestRestoreLease_NoSourcePlacement_Returns404(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend should be called when placement is missing: %s %s", r.Method, r.URL.Path)
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

	// placementLookup always returns "" — no recorded placement for the source lease.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string { return "" },
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)
}

// TestRestoreLease_PlacementDisabled_Returns503 verifies that when placement
// routing is disabled (placementLookup is nil), restore returns 503 "service
// not configured" — a service-misconfiguration condition — rather than a
// misleading 404 "no retained data" (Copilot review on PR #120). This matches
// how authenticateLease treats a nil backendRouter.
func TestRestoreLease_PlacementDisabled_Returns503(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend should be called when placement routing is disabled: %s %s", r.Method, r.URL.Path)
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

	// placementLookup is nil => placement routing disabled (service misconfig).
	// backendRouter is non-nil so authenticateLease passes; the nil placement
	// lookup must then surface as 503, not 404.
	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, errMsgServiceNotConfigured, errResp.Error)
}

// fakeRestoreRecorder captures the arguments passed to RecordRestorePlacement.
type fakeRestoreRecorder struct {
	newLease, backend string
	called            bool
}

func (f *fakeRestoreRecorder) RecordRestorePlacement(n, b string) {
	f.called, f.newLease, f.backend = true, n, b
}

// fakeRestoreTracker records how RestoreLease drives the in-flight tracker (ENG-358).
type fakeRestoreTracker struct {
	trackResult bool // what TryTrackRestoreInFlight returns

	tryCalled  bool
	tryLease   string
	tryTenant  string
	tryBackend string

	untrackCalled bool
	untrackLease  string
}

func (f *fakeRestoreTracker) TryTrackRestoreInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	f.tryCalled = true
	f.tryLease, f.tryTenant, f.tryBackend = leaseUUID, tenant, backendName
	return f.trackResult
}

func (f *fakeRestoreTracker) UntrackInFlight(leaseUUID string) {
	f.untrackCalled = true
	f.untrackLease = leaseUUID
}

// TestRestoreLease_RecorderCalledOnSuccess verifies that after a successful
// restore the handler calls RecordRestorePlacement(newLeaseUUID, backendName)
// on the injected RestorePlacementRecorder. The backend name must be that of
// the source-placement backend ("backend-src"), not any arbitrary backend
// (ENG-333).
func TestRestoreLease_RecorderCalledOnSuccess(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	// A backend named "backend-src" that accepts the restore and returns 202.
	srcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request on src backend: %s %s", r.Method, r.URL.Path)
	}))
	defer srcServer.Close()

	srcBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-src",
		BaseURL: srcServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: srcBackend, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Placement maps the SOURCE lease to "backend-src".
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "backend-src"
			}
			return ""
		},
	}

	recorder := &fakeRestoreRecorder{}

	h := NewHandlers(HandlersConfig{
		Client:          chainClient,
		BackendRouter:   router,
		PlacementLookup: placement,
		RestoreRecorder: recorder,
		ProviderUUID:    providerUUID,
		Bech32Prefix:    "manifest",
	})

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	resp := httptest.NewRecorder()
	h.RestoreLease(resp, req)

	require.Equal(t, http.StatusAccepted, resp.Code, "body: %s", resp.Body.String())
	assert.True(t, recorder.called, "RecordRestorePlacement should have been called on success")
	assert.Equal(t, leaseUUID, recorder.newLease, "recorder should receive the new lease UUID")
	assert.Equal(t, "backend-src", recorder.backend, "recorder should receive the source-placement backend name")
}

// TestRestoreLease_RecorderNotCalledOnMissingSourcePlacement verifies that when
// the source lease has no recorded placement (404, restore never reaches the
// success path), the RestorePlacementRecorder is NOT invoked. The recorder must
// only fire on a confirmed adopt, never on an error path (ENG-333).
func TestRestoreLease_RecorderNotCalledOnMissingSourcePlacement(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend should be called when placement is missing: %s %s", r.Method, r.URL.Path)
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

	// placementLookup always returns "" — no recorded placement for the source lease.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string { return "" },
	}

	recorder := &fakeRestoreRecorder{}

	h := NewHandlers(HandlersConfig{
		Client:          chainClient,
		BackendRouter:   router,
		PlacementLookup: placement,
		RestoreRecorder: recorder,
		ProviderUUID:    providerUUID,
		Bech32Prefix:    "manifest",
	})

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	resp := httptest.NewRecorder()
	h.RestoreLease(resp, req)

	require.Equal(t, http.StatusNotFound, resp.Code, "body: %s", resp.Body.String())
	assert.False(t, recorder.called, "RecordRestorePlacement must NOT be called when restore fails before the success path")
}

// restoreTrackerTestSetup builds a RestoreLease request whose source lease routes
// to a backend served by `backendHandler`, with `tracker` injected as the restore
// in-flight tracker. It returns the recorder after invoking the handler.
func restoreTrackerTestSetup(t *testing.T, tracker RestoreInFlightTracker, backendHandler http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
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
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	srcServer := httptest.NewServer(backendHandler)
	t.Cleanup(srcServer.Close)

	srcBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-src",
		BaseURL: srcServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: srcBackend, IsDefault: true}},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "backend-src"
			}
			return ""
		},
	}

	h := NewHandlers(HandlersConfig{
		Client:          chainClient,
		BackendRouter:   router,
		PlacementLookup: placement,
		RestoreTracker:  tracker,
		ProviderUUID:    providerUUID,
		Bech32Prefix:    "manifest",
	})

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	resp := httptest.NewRecorder()
	h.RestoreLease(resp, req)
	return resp
}

// TestRestoreLease_TracksRestoreInFlightOnSuccess is the API half of ENG-358: a
// successful restore must register the NEW lease in the in-flight tracker (so its
// provision callback is acked inline), keyed on the new lease and the SOURCE
// backend, and must NOT untrack on the success path.
func TestRestoreLease_TracksRestoreInFlightOnSuccess(t *testing.T) {
	tracker := &fakeRestoreTracker{trackResult: true}
	resp := restoreTrackerTestSetup(t, tracker, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	})

	require.Equal(t, http.StatusAccepted, resp.Code, "body: %s", resp.Body.String())
	assert.True(t, tracker.tryCalled, "restore must register the new lease in-flight")
	assert.Equal(t, testutil.ValidUUID1, tracker.tryLease, "must track the NEW lease UUID")
	assert.Equal(t, "backend-src", tracker.tryBackend, "must track against the SOURCE backend (ENG-333)")
	assert.NotEmpty(t, tracker.tryTenant, "must track the authenticated tenant")
	assert.False(t, tracker.untrackCalled, "must NOT untrack on the success path")
}

// TestRestoreLease_UntracksOnBackendError verifies the phantom-entry guard: if the
// synchronous Restore() call fails, the handler must untrack the in-flight entry
// it just registered, otherwise the TimeoutChecker would later reject a valid lease.
func TestRestoreLease_UntracksOnBackendError(t *testing.T) {
	tracker := &fakeRestoreTracker{trackResult: true}
	resp := restoreTrackerTestSetup(t, tracker, func(w http.ResponseWriter, r *http.Request) {
		// Backend rejects the restore — drives Restore() to return an error.
		w.WriteHeader(http.StatusInternalServerError)
	})

	require.NotEqual(t, http.StatusAccepted, resp.Code, "backend error must not yield 202")
	assert.True(t, tracker.tryCalled, "restore must have registered in-flight before calling the backend")
	assert.True(t, tracker.untrackCalled, "must untrack the in-flight entry when Restore() fails")
	assert.Equal(t, testutil.ValidUUID1, tracker.untrackLease, "must untrack the NEW lease UUID")
}

// TestRestoreLease_UntracksOnNonDefaultErrorBranch complements
// TestRestoreLease_UntracksOnBackendError (which drives the generic default
// branch). The untrack runs unconditionally BEFORE the error-classification
// switch, so it must fire on a SPECIFIC sentinel branch too — here a 422 from the
// backend → ErrNotRetained → 404. This locks the untrack-before-switch ordering
// against a regression that moves the untrack into a single error case.
func TestRestoreLease_UntracksOnNonDefaultErrorBranch(t *testing.T) {
	tracker := &fakeRestoreTracker{trackResult: true}
	resp := restoreTrackerTestSetup(t, tracker, func(w http.ResponseWriter, r *http.Request) {
		// 422 → backend.ErrNotRetained → the handler's first (non-default) error branch.
		w.WriteHeader(http.StatusUnprocessableEntity)
	})

	require.Equal(t, http.StatusNotFound, resp.Code, "422/ErrNotRetained must map to 404, body: %s", resp.Body.String())
	assert.True(t, tracker.untrackCalled, "must untrack on a non-default sync Restore() error branch too")
	assert.Equal(t, testutil.ValidUUID1, tracker.untrackLease, "must untrack the NEW lease UUID")
}

// TestRestoreLease_AlreadyInFlightReturns409 verifies that when the new lease is
// already being provisioned/restored (TryTrackRestoreInFlight==false — e.g. a
// duplicate POST or a racing reconciler fresh-provision), the handler returns 409,
// never calls the backend, and never untracks the foreign entry.
func TestRestoreLease_AlreadyInFlightReturns409(t *testing.T) {
	tracker := &fakeRestoreTracker{trackResult: false}
	resp := restoreTrackerTestSetup(t, tracker, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend must not be called when the lease is already in-flight: %s %s", r.Method, r.URL.Path)
	})

	require.Equal(t, http.StatusConflict, resp.Code, "already-in-flight restore must be 409, body: %s", resp.Body.String())
	assert.True(t, tracker.tryCalled, "handler must have attempted to track in-flight")
	assert.False(t, tracker.untrackCalled, "must NOT untrack a foreign in-flight entry it does not own")
}

// --- ENG-361: restore-route security gates (pre-mainnet) -------------------
//
// The headline cross-tenant data-theft gate (rec.Tenant != req.Tenant) is
// pinned at the backend layer by TestRestore_TenantMismatch_CollapsesToNotRetained
// (internal/backend/docker/restore_test.go). The three tests below pin the
// restore ROUTE's own enforcement so a refactor of RestoreLease — that skipped
// authenticateLease, or forwarded a tenant other than the ADR-036 signer to the
// backend — would be caught here, not only by inspecting that restore is wired
// with the same withAuthRL wrapper as restart/update.

// TestRestoreLease_RejectsUnauthenticated verifies PROPERTY 1 (ADR-036 auth) for
// the restore route: a missing, expired, or wrong-lease-bound token is rejected
// with 401 before any chain or backend work. Restore is authenticated exactly
// like restart/update — not an unauthenticated outlier.
func TestRestoreLease_RejectsUnauthenticated(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	for _, tc := range []struct {
		name        string
		authHeader  string // "" means no Authorization header
		description string
	}{
		{"missing_auth", "", "no Authorization header at all"},
		{"expired_token", "Bearer " + testutil.CreateExpiredToken(kp, leaseUUID), "token past MaxTokenAge"},
		// A token validly signed for a DIFFERENT lease must not authorize restore
		// onto leaseUUID: token.LeaseUUID is a signed field, so it cannot be
		// retargeted without the tenant's key (authenticateLeaseToken rejects a
		// token whose signed LeaseUUID does not match the path lease).
		{"wrong_target_lease_binding", "Bearer " + testutil.CreateTestToken(kp, testutil.ValidUUID3, time.Now()), "token bound to a different lease UUID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// client is nil and no backend is wired: auth must fail before either is
			// touched. A regression that reached them would nil-deref and fail loudly.
			h := &Handlers{
				client:       nil,
				providerUUID: providerUUID,
				bech32Prefix: "manifest",
			}

			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code, "%s: body: %s", tc.description, rec.Body.String())
		})
	}
}

// TestRestoreLease_RejectsNonOwnedTarget verifies PROPERTY 2a (caller owns the
// TARGET lease) for the restore route: even with a cryptographically valid token,
// a caller who does not own the path (target) lease — or whose lease belongs to a
// different provider — is rejected with 403 and the backend is never contacted.
func TestRestoreLease_RejectsNonOwnedTarget(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	for _, tc := range []struct {
		name  string
		lease *billingtypes.Lease
	}{
		{
			name: "tenant_mismatch",
			// Target lease is owned by a DIFFERENT tenant than the signer.
			lease: &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "manifest1different",
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			},
		},
		{
			name: "provider_mismatch",
			// Target lease belongs to a different provider.
			lease: &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID3,
				State:        billingtypes.LEASE_STATE_PENDING,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainClient := &mockChainClient{
				getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
					if uuid == leaseUUID {
						return tc.lease, nil
					}
					return nil, nil
				},
			}

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("backend must NOT be called when the caller does not own the target: %s %s", r.Method, r.URL.Path)
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
			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestRestoreLease_ForwardsSignerTenantOnCrossTenantSource verifies PROPERTY 2b
// plumbing at the restore route: the handler forwards the ADR-036 SIGNER's tenant
// (auth.Token.Tenant) to the backend — never a body- or path-derived value — and
// surfaces the backend's ErrNotRetained as an indistinguishable 404. This is what
// makes the backend's source-ownership gate (rec.Tenant != req.Tenant) effective:
// the caller owns the fresh TARGET lease but supplies another tenant's retained
// from_lease_uuid; the backend (here simulated with 422) sees the signer's tenant
// and rejects, and the caller cannot tell cross-tenant from not-found.
func TestRestoreLease_ForwardsSignerTenantOnCrossTenantSource(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// The caller legitimately owns the fresh PENDING target lease.
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	var receivedBody []byte
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			var err error
			receivedBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			// Simulate the docker backend's cross-tenant rejection
			// (rec.Tenant(other) != req.Tenant(signer) -> ErrNotRetained).
			w.WriteHeader(http.StatusUnprocessableEntity)
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

	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	// Cross-tenant source is indistinguishable from not-found: 404, same message.
	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)

	// The handler must have forwarded the SIGNER's tenant — the value the backend
	// gate compares against the retained record — not the body's from_lease_uuid
	// or any caller-supplied field. (RestoreRequest carries no tenant field for the
	// caller to set; this pins that RestoreLease sets the backend request's Tenant
	// from the authenticated token, i.e. Tenant: auth.Token.Tenant.)
	require.NotNil(t, receivedBody, "backend should have received a request body")
	var backendReq map[string]any
	require.NoError(t, json.Unmarshal(receivedBody, &backendReq))
	assert.Equal(t, kp.Address, backendReq["tenant"], "handler must forward the ADR-036 signer's tenant to the backend")
	assert.Equal(t, fromLeaseUUID, backendReq["from_lease_uuid"], "handler must forward the requested source lease")
}
