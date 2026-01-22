package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

	// Create HTTP client that skips TLS verification (for self-signed certs)
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Create HTTP server
	server := &MockBackendServer{
		backend:    mockBackend,
		httpClient: httpClient,
	}

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
	backend    *backend.MockBackend
	httpClient *http.Client
}

func (s *MockBackendServer) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req backend.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	slog.Info("provision request",
		"lease_uuid", req.LeaseUUID,
		"tenant", req.Tenant,
		"sku", req.SKU,
		"callback_url", req.CallbackURL,
		"has_payload", len(req.Payload) > 0,
	)

	// Set up callback function if URL provided
	if req.CallbackURL != "" {
		s.backend.SetCallbackFunc(func(payload backend.CallbackPayload) {
			s.sendCallback(req.CallbackURL, payload)
		})
	}

	if err := s.backend.Provision(r.Context(), req); err != nil {
		if strings.Contains(err.Error(), "already provisioned") {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(backend.ProvisionResponse{
		ProvisionID: req.LeaseUUID,
	})
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
	json.NewEncoder(w).Encode(info)
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

	w.WriteHeader(http.StatusOK)
}

func (s *MockBackendServer) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"provisions": provisions,
	})
}

func (s *MockBackendServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *MockBackendServer) sendCallback(callbackURL string, payload backend.CallbackPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal callback payload", "error", err)
		return
	}

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
