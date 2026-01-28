// Mock Backend for Fred
//
// This is a testing/development backend that simulates resource provisioning.
// It implements the full backend HTTP API but does NOT actually provision any
// real resources.
//
// IMPORTANT: The mock backend ignores the SKU field entirely. All provision
// requests create identical fake resources regardless of SKU. Connection
// details are deterministically generated from the lease UUID using SHA-256.
//
// For implementing a real backend, see BACKEND_GUIDE.md in the repository root.
//
// Usage:
//
//	MOCK_BACKEND_CALLBACK_SECRET="your-32-char-secret" ./mock-backend
//
// Environment Variables:
//
//	MOCK_BACKEND_ADDR             - Listen address (default: ":9000")
//	MOCK_BACKEND_NAME             - Backend name for logging (default: "mock-backend")
//	MOCK_BACKEND_DELAY            - Simulated provisioning delay (default: "0s")
//	MOCK_BACKEND_CALLBACK_SECRET  - HMAC secret for callbacks (required, min 32 chars)
//	MOCK_BACKEND_TLS_SKIP_VERIFY  - Skip TLS verification for callbacks (default: "false")
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

func main() {
	// Configure logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Get configuration from environment
	addr := os.Getenv("MOCK_BACKEND_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	name := os.Getenv("MOCK_BACKEND_NAME")
	if name == "" {
		name = "mock-backend"
	}

	delayStr := os.Getenv("MOCK_BACKEND_DELAY")
	var delay time.Duration
	if delayStr != "" {
		var err error
		delay, err = time.ParseDuration(delayStr)
		if err != nil {
			slog.Error("invalid MOCK_BACKEND_DELAY", "error", err)
			os.Exit(1)
		}
	}

	// Create mock backend
	mockBackend := backend.NewMockBackend(backend.MockBackendConfig{
		Name:           name,
		ProvisionDelay: delay,
	})

	// Create HTTP client for callbacks
	// By default, TLS verification is enabled. Set MOCK_BACKEND_TLS_SKIP_VERIFY=true
	// to skip verification (e.g., for self-signed certs in local testing).
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	tlsSkipVerify := os.Getenv("MOCK_BACKEND_TLS_SKIP_VERIFY")
	if tlsSkipVerify == "true" || tlsSkipVerify == "1" {
		slog.Warn("TLS certificate verification is DISABLED for callbacks - do not use in production",
			"env_var", "MOCK_BACKEND_TLS_SKIP_VERIFY",
		)
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	// Get callback secret for signing
	callbackSecret := os.Getenv("MOCK_BACKEND_CALLBACK_SECRET")
	if callbackSecret == "" {
		slog.Error("MOCK_BACKEND_CALLBACK_SECRET is required for callback authentication")
		os.Exit(1)
	}
	if len(callbackSecret) < 32 {
		slog.Error("MOCK_BACKEND_CALLBACK_SECRET must be at least 32 characters")
		os.Exit(1)
	}

	// Create HTTP server
	server := &MockBackendServer{
		backend:        mockBackend,
		httpClient:     httpClient,
		callbackSecret: callbackSecret,
		callbackURLs:   make(map[string]string),
	}

	// Set up a single callback function that routes by lease UUID
	mockBackend.SetCallbackFunc(server.handleCallback)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /provision", server.handleProvision)
	mux.HandleFunc("GET /info/{lease_uuid}", server.handleGetInfo)
	mux.HandleFunc("POST /deprovision", server.handleDeprovision)
	mux.HandleFunc("GET /provisions", server.handleListProvisions)
	mux.HandleFunc("GET /health", server.handleHealth)

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server
	go func() {
		slog.Info("starting mock backend server",
			"addr", addr,
			"name", name,
			"delay", delay,
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	slog.Info("mock backend stopped")
}

// MockBackendServer wraps the mock backend with HTTP handlers.
type MockBackendServer struct {
	backend        *backend.MockBackend
	httpClient     *http.Client
	callbackSecret string // HMAC secret for signing callbacks

	// Per-lease callback URLs to avoid race conditions with concurrent provisions
	callbackURLs   map[string]string
	callbackURLsMu sync.Mutex
}

func (s *MockBackendServer) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req backend.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.LeaseUUID == "" {
		http.Error(w, "lease_uuid is required", http.StatusBadRequest)
		return
	}
	if req.CallbackURL == "" {
		http.Error(w, "callback_url is required", http.StatusBadRequest)
		return
	}

	// Log provision request with payload details
	logAttrs := []any{
		"lease_uuid", req.LeaseUUID,
		"tenant", req.Tenant,
		"sku", req.SKU,
		"callback_url", req.CallbackURL,
		"has_payload", len(req.Payload) > 0,
	}
	if len(req.Payload) > 0 {
		logAttrs = append(logAttrs,
			"payload_size", len(req.Payload),
			"payload_base64", base64.StdEncoding.EncodeToString(req.Payload),
		)
		if req.PayloadHash != "" {
			logAttrs = append(logAttrs, "payload_hash", req.PayloadHash)
		}
	}
	slog.Info("provision request", logAttrs...)

	// Validate callback URL format and store for this lease (thread-safe)
	parsedURL, err := url.Parse(req.CallbackURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		http.Error(w, "invalid callback_url: must be a valid http/https URL", http.StatusBadRequest)
		return
	}
	s.callbackURLsMu.Lock()
	s.callbackURLs[req.LeaseUUID] = req.CallbackURL
	s.callbackURLsMu.Unlock()

	if err := s.backend.Provision(r.Context(), req); err != nil {
		// Clean up callback URL on error
		s.callbackURLsMu.Lock()
		delete(s.callbackURLs, req.LeaseUUID)
		s.callbackURLsMu.Unlock()

		if strings.Contains(err.Error(), "already provisioned") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(backend.ProvisionResponse{
		ProvisionID: req.LeaseUUID,
	}); err != nil {
		slog.Error("failed to encode provision response", "error", err)
	}
}

func (s *MockBackendServer) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		http.Error(w, "lease_uuid is required", http.StatusBadRequest)
		return
	}

	info, err := s.backend.GetInfo(r.Context(), leaseUUID)
	if err == backend.ErrNotProvisioned {
		http.Error(w, "not provisioned", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(info); err != nil {
		slog.Error("failed to encode info response", "error", err)
	}
}

func (s *MockBackendServer) handleDeprovision(w http.ResponseWriter, r *http.Request) {
	var req struct {
		LeaseUUID string `json:"lease_uuid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	slog.Info("deprovision request", "lease_uuid", req.LeaseUUID)

	if err := s.backend.Deprovision(r.Context(), req.LeaseUUID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Clean up callback URL
	s.callbackURLsMu.Lock()
	delete(s.callbackURLs, req.LeaseUUID)
	s.callbackURLsMu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (s *MockBackendServer) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"provisions": provisions,
	}); err != nil {
		slog.Error("failed to encode provisions response", "error", err)
	}
}

func (s *MockBackendServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleCallback is called by MockBackend when provisioning completes.
// It looks up the callback URL for the lease and sends the callback.
func (s *MockBackendServer) handleCallback(payload backend.CallbackPayload) {
	s.callbackURLsMu.Lock()
	callbackURL, exists := s.callbackURLs[payload.LeaseUUID]
	if exists {
		// Remove from map after retrieving (callback is one-time)
		delete(s.callbackURLs, payload.LeaseUUID)
	}
	s.callbackURLsMu.Unlock()

	if !exists || callbackURL == "" {
		slog.Debug("no callback URL for lease", "lease_uuid", payload.LeaseUUID)
		return
	}

	s.sendCallback(callbackURL, payload)
}

func (s *MockBackendServer) sendCallback(callbackURL string, payload backend.CallbackPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal callback payload", "error", err)
		return
	}

	// Compute HMAC signature
	signature := s.computeSignature(body)

	slog.Info("sending callback",
		"url", callbackURL,
		"lease_uuid", payload.LeaseUUID,
		"status", payload.Status,
	)

	req, err := http.NewRequest(http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		slog.Error("failed to create callback request", "error", err, "url", callbackURL)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Fred-Signature", signature)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		slog.Error("failed to send callback", "error", err, "url", callbackURL)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		slog.Error("callback returned error", "status", resp.StatusCode, "url", callbackURL)
		return
	}

	slog.Info("callback sent successfully", "lease_uuid", payload.LeaseUUID)
}

// computeSignature computes the HMAC-SHA256 signature for a payload.
func (s *MockBackendServer) computeSignature(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(s.callbackSecret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
