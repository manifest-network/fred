// docker-backend is an HTTP server that implements the Fred backend protocol
// for provisioning Docker containers.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func main() {
	configPath := flag.String("config", "docker-backend.yaml", "Path to configuration file")
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Apply environment variable overrides
	applyEnvOverrides(&cfg)

	// Log SKU mappings for visibility
	for uuid, profile := range cfg.SKUMapping {
		logger.Info("SKU mapping", "uuid", uuid, "profile", profile)
	}

	// Create backend
	b, err := docker.New(cfg, logger)
	if err != nil {
		logger.Error("failed to create backend", "error", err)
		os.Exit(1)
	}

	// Start backend
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := b.Start(ctx); err != nil {
		cancel()
		logger.Error("failed to start backend", "error", err)
		os.Exit(1)
	}
	cancel()

	// Create server
	server := NewServer(b, cfg.CallbackSecret, logger)

	// Setup HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start HTTP server
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down...")

	// Graceful shutdown
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP shutdown error", "error", err)
	}

	if err := b.Stop(); err != nil {
		logger.Error("backend shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
}

func loadConfig(path string) (docker.Config, error) {
	cfg := docker.DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, fmt.Errorf("config file not found: %s", path)
		}
		return cfg, fmt.Errorf("failed to read config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to parse config: %w", err)
	}

	return cfg, nil
}

func applyEnvOverrides(cfg *docker.Config) {
	if addr := os.Getenv("DOCKER_BACKEND_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if secret := os.Getenv("DOCKER_BACKEND_CALLBACK_SECRET"); secret != "" {
		cfg.CallbackSecret = secret
	}
	if host := os.Getenv("DOCKER_BACKEND_HOST_ADDRESS"); host != "" {
		cfg.HostAddress = host
	}
	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		cfg.DockerHost = dockerHost
	}
}

// Server handles HTTP requests for the Docker backend.
type Server struct {
	backend        *docker.Backend
	callbackSecret string
	logger         *slog.Logger
}

// NewServer creates a new HTTP server for the Docker backend.
func NewServer(b *docker.Backend, callbackSecret string, logger *slog.Logger) *Server {
	return &Server{
		backend:        b,
		callbackSecret: callbackSecret,
		logger:         logger,
	}
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	authMw := hmacAuthMiddleware(s.callbackSecret, s.logger)
	mux.Handle("POST /provision", authMw(http.HandlerFunc(s.handleProvision)))
	mux.Handle("POST /deprovision", authMw(http.HandlerFunc(s.handleDeprovision)))
	mux.Handle("GET /info/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetInfo)))
	mux.Handle("GET /logs/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetLogs)))
	mux.Handle("GET /provisions", authMw(http.HandlerFunc(s.handleListProvisions)))

	// Operational endpoints — no auth required (monitoring, health checks).
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.Handle("GET /metrics", promhttp.Handler())

	return mux
}

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req backend.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}
	if req.CallbackURL == "" {
		s.errorResponse(w, http.StatusBadRequest, "callback_url is required")
		return
	}
	if err := validateCallbackURL(req.CallbackURL); err != nil {
		s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid callback_url: %s", err))
		return
	}
	if len(req.Items) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "items is required")
		return
	}

	err := s.backend.Provision(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrAlreadyProvisioned) {
			s.errorResponse(w, http.StatusConflict, "lease already provisioned")
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			s.errorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, backend.ErrInsufficientResources) {
			s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Return 202 Accepted
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(backend.ProvisionResponse{
		ProvisionID: req.LeaseUUID,
	})
}

func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	info, err := s.backend.GetInfo(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	tail := 100 // default
	if v := r.URL.Query().Get("tail"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			s.errorResponse(w, http.StatusBadRequest, "tail must be a positive integer")
			return
		}
		if n > maxTailLines {
			s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("tail must not exceed %d", maxTailLines))
			return
		}
		tail = n
	}

	logs, err := s.backend.GetLogs(r.Context(), leaseUUID, tail)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

// DeprovisionRequest is the request body for /deprovision.
type DeprovisionRequest struct {
	LeaseUUID string `json:"lease_uuid"`
}

func (s *Server) handleDeprovision(w http.ResponseWriter, r *http.Request) {
	var req DeprovisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	if err := s.backend.Deprovision(r.Context(), req.LeaseUUID); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(StatusResponse{Status: "ok"})
}

// StatsResponse is the response body for /stats.
type StatsResponse struct {
	TotalCPUCores     float64 `json:"total_cpu_cores"`
	TotalMemoryMB     int64   `json:"total_memory_mb"`
	TotalDiskMB       int64   `json:"total_disk_mb"`
	AllocatedCPUCores float64 `json:"allocated_cpu_cores"`
	AllocatedMemoryMB int64   `json:"allocated_memory_mb"`
	AllocatedDiskMB   int64   `json:"allocated_disk_mb"`
	AvailableCPUCores float64 `json:"available_cpu_cores"`
	AvailableMemoryMB int64   `json:"available_memory_mb"`
	AvailableDiskMB   int64   `json:"available_disk_mb"`
	ActiveContainers  int     `json:"active_containers"`
}

// StatusResponse is a simple status response.
type StatusResponse struct {
	Status string `json:"status"`
}

func (s *Server) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
		Provisions: provisions,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.backend.Health(r.Context()); err != nil {
		s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(StatusResponse{Status: "healthy"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.backend.Stats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(StatsResponse{
		TotalCPUCores:     stats.TotalCPU,
		TotalMemoryMB:     stats.TotalMemoryMB,
		TotalDiskMB:       stats.TotalDiskMB,
		AllocatedCPUCores: stats.AllocatedCPU,
		AllocatedMemoryMB: stats.AllocatedMemory,
		AllocatedDiskMB:   stats.AllocatedDisk,
		AvailableCPUCores: stats.AvailableCPU(),
		AvailableMemoryMB: stats.AvailableMemoryMB(),
		AvailableDiskMB:   stats.AvailableDiskMB(),
		ActiveContainers:  stats.AllocationCount,
	})
}

// ErrorResponse is the response body for errors.
type ErrorResponse struct {
	Error string `json:"error"`
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

const maxRequestBodySize = 1 << 20 // 1 MiB
const maxTailLines = 10000         // Upper bound for log tail requests

// validateCallbackURL validates that a callback URL is safe to use.
// It rejects non-HTTP(S) schemes and private/internal IP addresses to prevent SSRF.
func validateCallbackURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("malformed URL")
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}

	if parsed.Host == "" {
		return fmt.Errorf("host is required")
	}

	// Extract hostname (without port)
	hostname := parsed.Hostname()

	// Reject localhost variants
	if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
		return fmt.Errorf("localhost is not allowed")
	}

	// Check if hostname is an IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		if ip.IsLoopback() {
			return fmt.Errorf("loopback addresses are not allowed")
		}
		if ip.IsPrivate() {
			return fmt.Errorf("private network addresses are not allowed")
		}
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("link-local addresses are not allowed")
		}
		// Block cloud metadata endpoints (169.254.x.x range)
		if ip.IsLinkLocalUnicast() || (ip.To4() != nil && ip.To4()[0] == 169 && ip.To4()[1] == 254) {
			return fmt.Errorf("metadata service addresses are not allowed")
		}
	}

	return nil
}

// hmacAuthMiddleware returns middleware that verifies HMAC-SHA256 signatures on requests.
func hmacAuthMiddleware(secret string, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Limit request body size
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

			sig := r.Header.Get(hmacauth.SignatureHeader)
			if sig == "" {
				logger.Warn("missing signature header", "remote", r.RemoteAddr, "path", r.URL.Path)
				http.Error(w, `{"error":"missing signature"}`, http.StatusUnauthorized)
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Warn("failed to read request body", "error", err)
				http.Error(w, `{"error":"request body too large"}`, http.StatusRequestEntityTooLarge)
				return
			}

			if err := hmacauth.Verify(secret, body, sig, 5*time.Minute); err != nil {
				logger.Warn("signature verification failed",
					"error", err,
					"remote", r.RemoteAddr,
					"path", r.URL.Path,
				)
				http.Error(w, `{"error":"invalid signature"}`, http.StatusUnauthorized)
				return
			}

			// Replace the body so handlers can read it
			r.Body = io.NopCloser(bytes.NewReader(body))
			next.ServeHTTP(w, r)
		})
	}
}
