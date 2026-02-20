// docker-backend is an HTTP server that implements the Fred backend protocol
// for provisioning Docker containers.
package main

import (
	"bytes"
	"cmp"
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
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "docker-backend.yaml", "Path to configuration file")
	flag.Parse()

	// Bootstrap logger for startup messages (before config is loaded).
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

	// Re-configure logger with the configured log level.
	logLevel, err := config.ParseLogLevel(cmp.Or(cfg.LogLevel, "info"))
	if err != nil {
		logger.Error("invalid log_level in config", "error", err)
		os.Exit(1)
	}
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)
	logger.Info("starting docker-backend", "version", version, "log_level", cfg.LogLevel)

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
	server := NewServer(b, string(cfg.CallbackSecret), logger)

	// Setup HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start HTTP server
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for shutdown signal or server error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
		logger.Info("shutting down...")
	case err := <-serverErr:
		logger.Error("HTTP server error, shutting down", "error", err)
	}

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
		cfg.CallbackSecret = config.Secret(secret)
	}
	if host := os.Getenv("DOCKER_BACKEND_HOST_ADDRESS"); host != "" {
		cfg.HostAddress = host
	}
	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		cfg.DockerHost = dockerHost
	}
}

// backendService defines the methods that handlers call on the backend.
// docker.Backend satisfies this interface structurally.
type backendService interface {
	Provision(ctx context.Context, req backend.ProvisionRequest) error
	Deprovision(ctx context.Context, leaseUUID string) error
	GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error)
	GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)
	GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error)
	Restart(ctx context.Context, req backend.RestartRequest) error
	Update(ctx context.Context, req backend.UpdateRequest) error
	GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error)
	Health(ctx context.Context) error
	Stats() shared.ResourceStats
}

// Server handles HTTP requests for the Docker backend.
type Server struct {
	backend        backendService
	callbackSecret string
	logger         *slog.Logger
}

// NewServer creates a new HTTP server for the Docker backend.
func NewServer(b backendService, callbackSecret string, logger *slog.Logger) *Server {
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
	mux.Handle("GET /provisions/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetProvision)))
	mux.Handle("GET /provisions", authMw(http.HandlerFunc(s.handleListProvisions)))
	mux.Handle("POST /restart", authMw(http.HandlerFunc(s.handleRestart)))
	mux.Handle("POST /update", authMw(http.HandlerFunc(s.handleUpdate)))
	mux.Handle("GET /releases/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetReleases)))

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
			s.validationErrorResponse(w, err)
			return
		}
		if errors.Is(err, backend.ErrInsufficientResources) {
			s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusAccepted, backend.ProvisionResponse{
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

	s.writeJSON(w, http.StatusOK, info)
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

	s.writeJSON(w, http.StatusOK, logs)
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

	s.writeJSON(w, http.StatusOK, StatusResponse{Status: "ok"})
}

func (s *Server) handleRestart(w http.ResponseWriter, r *http.Request) {
	var req backend.RestartRequest
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

	err := s.backend.Restart(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			s.errorResponse(w, http.StatusConflict, "invalid state for restart")
			return
		}
		s.logger.Error("restart failed", "lease_uuid", req.LeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusAccepted, StatusResponse{Status: "restarting"})
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req backend.UpdateRequest
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
	if len(req.Payload) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "payload is required")
		return
	}

	err := s.backend.Update(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			s.errorResponse(w, http.StatusConflict, "invalid state for update")
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			s.validationErrorResponse(w, err)
			return
		}
		s.logger.Error("update failed", "lease_uuid", req.LeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusAccepted, StatusResponse{Status: "updating"})
}

func (s *Server) handleGetReleases(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	releases, err := s.backend.GetReleases(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.logger.Error("get releases failed", "lease_uuid", leaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusOK, releases)
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

func (s *Server) handleGetProvision(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	info, err := s.backend.GetProvision(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, backend.ListProvisionsResponse{
		Provisions: provisions,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.backend.Health(r.Context()); err != nil {
		s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, StatusResponse{Status: "healthy"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.backend.Stats()

	s.writeJSON(w, http.StatusOK, StatsResponse{
		TotalCPUCores:     stats.TotalCPU,
		TotalMemoryMB:     stats.TotalMemoryMB,
		TotalDiskMB:       stats.TotalDiskMB,
		AllocatedCPUCores: stats.AllocatedCPU,
		AllocatedMemoryMB: stats.AllocatedMemoryMB,
		AllocatedDiskMB:   stats.AllocatedDiskMB,
		AvailableCPUCores: stats.AvailableCPU(),
		AvailableMemoryMB: stats.AvailableMemoryMB(),
		AvailableDiskMB:   stats.AvailableDiskMB(),
		ActiveContainers:  stats.AllocationCount,
	})
}

// ErrorResponse is the response body for errors.
type ErrorResponse struct {
	Error          string                 `json:"error"`
	ValidationCode backend.ValidationCode `json:"validation_code,omitempty"`
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, data any) {
	encoded, err := json.Marshal(data)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, `{"error":"internal encoding error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, writeErr := w.Write(encoded); writeErr != nil {
		s.logger.Debug("failed to write response body", "error", writeErr)
	}
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, ErrorResponse{Error: message})
}

// validationErrorResponse writes a 400 response with a validation_code field
// so the HTTPClient can reconstruct the correct sentinel error.
func (s *Server) validationErrorResponse(w http.ResponseWriter, err error) {
	s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
		Error:          err.Error(),
		ValidationCode: backend.ClassifyValidationError(err),
	})
}

const maxRequestBodySize = 1 << 20 // 1 MiB
const maxTailLines = 10000         // Upper bound for log tail requests

// validateCallbackURL validates that a callback URL is safe to use.
// It rejects non-HTTP(S) schemes and dangerous IP addresses to prevent SSRF.
//
// Note: This validation is defense-in-depth. The callback URL comes from Fred
// (providerd), not from untrusted tenants. Fred should validate its own
// callback_base_url configuration. We allow localhost and private IPs here
// since backends commonly run on private networks alongside Fred.
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

	// Check if hostname is an IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		// Block link-local addresses (169.254.0.0/16, fe80::/10) which
		// include cloud metadata endpoints like 169.254.169.254.
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("link-local addresses are not allowed")
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
