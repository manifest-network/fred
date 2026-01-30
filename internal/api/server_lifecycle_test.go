package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/provisioner"
)

// freePort binds :0, extracts the port, and returns "127.0.0.1:<port>".
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// newTestServer creates a Server with minimal mock deps for lifecycle tests.
func newTestServer(t *testing.T, addr string) *Server {
	t.Helper()

	s, err := NewServer(
		ServerConfig{
			Addr:           addr,
			ProviderUUID:   "01234567-89ab-cdef-0123-456789abcdef",
			Bech32Prefix:   "manifest",
			RateLimitRPS:   100,
			RateLimitBurst: 200,
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
			IdleTimeout:    30 * time.Second,
			RequestTimeout: 5 * time.Second,
		},
		&mockChainClient{},
		nil, // no backend router
		&mockCallbackPublisher{},
		nil, // no payload publisher
		&mockStatusChecker{},
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	return s
}

// startAndWaitForServer starts the server in a goroutine and polls /health
// until it responds. Returns the HTTP client for subsequent requests.
func startAndWaitForServer(t *testing.T, s *Server, addr string) *http.Client {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	healthURL := fmt.Sprintf("http://%s/health", addr)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for server to start")
		case err := <-errCh:
			t.Fatalf("server exited unexpectedly: %v", err)
		default:
		}

		resp, err := client.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			return client
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServer_StartAndHealth(t *testing.T) {
	addr := freePort(t)
	s := newTestServer(t, addr)

	client := startAndWaitForServer(t, s, addr)

	resp, err := client.Get(fmt.Sprintf("http://%s/health", addr))
	if err != nil {
		t.Fatalf("health check request failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health check status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	addr := freePort(t)
	s := newTestServer(t, addr)

	client := startAndWaitForServer(t, s, addr)

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	// Subsequent requests should fail
	_, err := client.Get(fmt.Sprintf("http://%s/health", addr))
	if err == nil {
		t.Error("request after shutdown should fail")
	}
}

func TestServer_RoutesRegistered(t *testing.T) {
	addr := freePort(t)
	s := newTestServer(t, addr)

	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/health"},
		{"GET", "/metrics"},
		{"POST", "/callbacks/provision"},
		{"GET", "/v1/leases/01234567-89ab-cdef-0123-456789abcdef/status"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			url := fmt.Sprintf("http://%s%s", addr, route.path)
			req, _ := http.NewRequest(route.method, url, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusNotFound {
				t.Errorf("route %s %s returned 404, expected route to be registered", route.method, route.path)
			}
		})
	}
}

func TestServer_ShutdownClosesTokenTracker(t *testing.T) {
	addr := freePort(t)
	dbPath := filepath.Join(t.TempDir(), "tokens.db")

	s, err := NewServer(
		ServerConfig{
			Addr:               addr,
			ProviderUUID:       "01234567-89ab-cdef-0123-456789abcdef",
			Bech32Prefix:       "manifest",
			RateLimitRPS:       100,
			RateLimitBurst:     200,
			ReadTimeout:        5 * time.Second,
			WriteTimeout:       5 * time.Second,
			IdleTimeout:        30 * time.Second,
			RequestTimeout:     5 * time.Second,
			TokenTrackerDBPath: dbPath,
		},
		&mockChainClient{},
		nil,
		&mockCallbackPublisher{},
		nil,
		&mockStatusChecker{},
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	if s.tokenTracker == nil {
		t.Fatal("expected token tracker to be configured")
	}

	startAndWaitForServer(t, s, addr)

	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

// testPayloadPublisher implements PayloadPublisher for server lifecycle tests.
type testPayloadPublisher struct{}

func (m *testPayloadPublisher) PublishPayload(event provisioner.PayloadEvent) error {
	return nil
}

func (m *testPayloadPublisher) StorePayload(leaseUUID string, payload []byte) bool {
	return true
}

func (m *testPayloadPublisher) DeletePayload(leaseUUID string) {}

func TestServer_HandlePayloadUpload(t *testing.T) {
	t.Run("returns_503_when_handler_not_configured", func(t *testing.T) {
		addr := freePort(t)
		s, err := NewServer(
			ServerConfig{
				Addr:           addr,
				ProviderUUID:   "01234567-89ab-cdef-0123-456789abcdef",
				Bech32Prefix:   "manifest",
				RateLimitRPS:   100,
				RateLimitBurst: 200,
				ReadTimeout:    5 * time.Second,
				WriteTimeout:   5 * time.Second,
				IdleTimeout:    30 * time.Second,
				RequestTimeout: 5 * time.Second,
			},
			&mockChainClient{},
			nil,
			&mockCallbackPublisher{},
			nil, // No payload publisher - handler won't be set
			&mockStatusChecker{},
		)
		if err != nil {
			t.Fatalf("NewServer() error = %v", err)
		}

		// Directly test the handler
		req := httptest.NewRequest(http.MethodPost, "/v1/leases/test-uuid/data", nil)
		rec := httptest.NewRecorder()

		s.handlePayloadUpload(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}

		// Verify error message
		body := rec.Body.String()
		if body == "" {
			t.Error("expected error response body")
		}
	})

	t.Run("delegates_to_configured_handler", func(t *testing.T) {
		addr := freePort(t)
		payloadPub := &testPayloadPublisher{}

		s, err := NewServer(
			ServerConfig{
				Addr:           addr,
				ProviderUUID:   "01234567-89ab-cdef-0123-456789abcdef",
				Bech32Prefix:   "manifest",
				RateLimitRPS:   100,
				RateLimitBurst: 200,
				ReadTimeout:    5 * time.Second,
				WriteTimeout:   5 * time.Second,
				IdleTimeout:    30 * time.Second,
				RequestTimeout: 5 * time.Second,
			},
			&mockChainClient{},
			nil,
			&mockCallbackPublisher{},
			payloadPub, // Provide publisher so handler is configured
			&mockStatusChecker{},
		)
		if err != nil {
			t.Fatalf("NewServer() error = %v", err)
		}

		// Verify handler was configured
		if s.payloadHandler == nil {
			t.Fatal("expected payload handler to be configured")
		}

		// The handler is configured - actual payload upload testing
		// is done in payload_test.go. Here we just verify the delegation path.
		req := httptest.NewRequest(http.MethodPost, "/v1/leases/test-uuid/data", nil)
		rec := httptest.NewRecorder()

		// This will fail auth but proves the handler is called
		s.handlePayloadUpload(rec, req)

		// Should get auth error (401), not service unavailable (503)
		if rec.Code == http.StatusServiceUnavailable {
			t.Error("handler should be configured and called, not return 503")
		}
	})
}

func TestServer_Start_ContextCancellation(t *testing.T) {
	addr := freePort(t)
	s := newTestServer(t, addr)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	client := &http.Client{Timeout: 2 * time.Second}
	healthURL := fmt.Sprintf("http://%s/health", addr)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for server to start")
		default:
		}

		resp, err := client.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel context - this should trigger graceful shutdown
	cancel()

	// Start should return with context.Canceled
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("Start() error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Start to return after context cancellation")
	}

	// Server should be shut down - port should be free
	// Verify by successfully binding to the same port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("port should be free after context cancellation, but got: %v", err)
	} else {
		ln.Close()
	}
}

func TestServer_StartBackground(t *testing.T) {
	t.Run("returns_immediately_when_listening", func(t *testing.T) {
		addr := freePort(t)
		s := newTestServer(t, addr)

		errChan, err := s.StartBackground()
		if err != nil {
			t.Fatalf("StartBackground() error = %v", err)
		}
		defer s.Shutdown(context.Background())

		// Server should be immediately ready to accept connections
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://%s/health", addr))
		if err != nil {
			t.Fatalf("health check failed immediately after StartBackground: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("health check status = %d, want %d", resp.StatusCode, http.StatusOK)
		}

		// Error channel should not have any errors yet
		select {
		case err := <-errChan:
			if err != nil {
				t.Errorf("unexpected error from errChan: %v", err)
			}
		default:
			// Expected - no error yet
		}
	})

	t.Run("error_channel_closed_on_shutdown", func(t *testing.T) {
		addr := freePort(t)
		s := newTestServer(t, addr)

		errChan, err := s.StartBackground()
		if err != nil {
			t.Fatalf("StartBackground() error = %v", err)
		}

		// Shutdown the server
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}

		// Error channel should close without error (graceful shutdown)
		select {
		case err, ok := <-errChan:
			if ok && err != nil {
				t.Errorf("expected channel to close cleanly, got error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("timed out waiting for error channel to close")
		}
	})

	t.Run("returns_error_for_invalid_address", func(t *testing.T) {
		s := newTestServer(t, "invalid:address:format")

		_, err := s.StartBackground()
		if err == nil {
			t.Error("expected error for invalid address")
		}
	})

	t.Run("returns_error_for_port_in_use", func(t *testing.T) {
		addr := freePort(t)

		// Start first server
		s1 := newTestServer(t, addr)
		_, err := s1.StartBackground()
		if err != nil {
			t.Fatalf("first StartBackground() error = %v", err)
		}
		defer s1.Shutdown(context.Background())

		// Try to start second server on same port
		s2 := newTestServer(t, addr)
		_, err = s2.StartBackground()
		if err == nil {
			t.Error("expected error when port is already in use")
		}
	})

	t.Run("returns_error_for_invalid_tls_certs", func(t *testing.T) {
		addr := freePort(t)

		s, err := NewServer(
			ServerConfig{
				Addr:           addr,
				ProviderUUID:   "01234567-89ab-cdef-0123-456789abcdef",
				Bech32Prefix:   "manifest",
				RateLimitRPS:   100,
				RateLimitBurst: 200,
				ReadTimeout:    5 * time.Second,
				WriteTimeout:   5 * time.Second,
				IdleTimeout:    30 * time.Second,
				RequestTimeout: 5 * time.Second,
				TLSCertFile:    "/nonexistent/cert.pem",
				TLSKeyFile:     "/nonexistent/key.pem",
			},
			&mockChainClient{},
			nil,
			&mockCallbackPublisher{},
			nil,
			&mockStatusChecker{},
		)
		if err != nil {
			t.Fatalf("NewServer() error = %v", err)
		}

		_, err = s.StartBackground()
		if err == nil {
			t.Error("expected error for invalid TLS certificates")
		}

		// Verify the port is free (listener was properly closed on error)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("port should be free after TLS cert error, but got: %v", err)
		} else {
			ln.Close()
		}
	})
}
