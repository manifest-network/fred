package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
)

const (
	// serverShutdownTimeout is the maximum time to wait for the server to shutdown gracefully.
	serverShutdownTimeout = 10 * time.Second
)

// CallbackPublisher publishes backend callbacks to the provisioner.
type CallbackPublisher interface {
	PublishCallback(callback backend.CallbackPayload) error
}

// StatusChecker provides status information about provisioning.
// Typically implemented by the provisioner.Manager.
type StatusChecker interface {
	HasPayload(leaseUUID string) bool
	IsInFlight(leaseUUID string) bool
}


// Server is the HTTP API server.
type Server struct {
	addr                  string
	server                *http.Server
	handlers              *Handlers
	payloadHandler        *PayloadHandler
	tokenTracker          *TokenTracker
	providerUUID          string
	bech32Prefix          string
	tlsCertFile           string
	tlsKeyFile            string
	rateLimiter           *RateLimiter
	tenantRateLimiter     *TenantRateLimiter
	callbackPublisher     CallbackPublisher
	callbackAuthenticator *CallbackAuthenticator
	statusChecker         StatusChecker
}

// ServerConfig holds configuration for the API server.
type ServerConfig struct {
	Addr                 string
	ProviderUUID         string
	Bech32Prefix         string
	TLSCertFile          string
	TLSKeyFile           string
	RateLimitRPS         float64
	RateLimitBurst       int
	TenantRateLimitRPS   float64  // Per-tenant rate limit (requests per second), 0 = disabled
	TenantRateLimitBurst int      // Per-tenant burst limit
	TrustedProxies       []string // CIDR blocks of trusted reverse proxies for X-Forwarded-For
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	MaxRequestBodySize   int64
	CallbackSecret       string // HMAC secret for callback authentication
	TokenTrackerDBPath   string // Path to token tracker database (enables replay protection)
}

// NewServer creates a new API server.
// Returns an error if token tracker initialization fails.
func NewServer(cfg ServerConfig, client ChainClient, backendRouter *backend.Router, callbackPublisher CallbackPublisher, payloadPublisher PayloadPublisher, statusChecker StatusChecker) (*Server, error) {
	// Create token tracker if path is configured (enables replay protection)
	var tokenTracker *TokenTracker
	if cfg.TokenTrackerDBPath != "" {
		var err error
		tokenTracker, err = NewTokenTracker(TokenTrackerConfig{
			DBPath: cfg.TokenTrackerDBPath,
			MaxAge: MaxTokenAge,
		})
		if err != nil {
			return nil, err
		}
		slog.Info("token replay protection enabled", "db_path", cfg.TokenTrackerDBPath)
	} else {
		slog.Warn("token replay protection disabled (no TokenTrackerDBPath configured)")
	}

	handlers := NewHandlers(client, backendRouter, tokenTracker, statusChecker, cfg.ProviderUUID, cfg.Bech32Prefix)

	// Parse trusted proxies for secure X-Forwarded-For handling
	var trustedProxies *TrustedProxyConfig
	if len(cfg.TrustedProxies) > 0 {
		trustedProxies = NewTrustedProxyConfig(cfg.TrustedProxies)
		slog.Info("trusted proxies configured for rate limiting", "count", len(cfg.TrustedProxies))
	}
	rateLimiter := NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, trustedProxies)

	// Create per-tenant rate limiter if configured
	var tenantRateLimiter *TenantRateLimiter
	if cfg.TenantRateLimitRPS > 0 {
		tenantRateLimiter = NewTenantRateLimiter(cfg.TenantRateLimitRPS, cfg.TenantRateLimitBurst)
		slog.Info("per-tenant rate limiting enabled",
			"rps", cfg.TenantRateLimitRPS,
			"burst", cfg.TenantRateLimitBurst,
		)
	}

	// Apply default for max request body size
	maxBodySize := cfg.MaxRequestBodySize
	if maxBodySize <= 0 {
		maxBodySize = config.DefaultMaxRequestBodySize
	}

	// Create callback authenticator if secret is provided
	var callbackAuth *CallbackAuthenticator
	if cfg.CallbackSecret != "" {
		callbackAuth = NewCallbackAuthenticator(cfg.CallbackSecret)
	}

	// Create payload handler if publisher is provided
	var payloadHandler *PayloadHandler
	if payloadPublisher != nil {
		payloadHandler = NewPayloadHandler(client, payloadPublisher, cfg.ProviderUUID, cfg.Bech32Prefix)
	}

	router := mux.NewRouter()

	s := &Server{
		addr:                  cfg.Addr,
		handlers:              handlers,
		payloadHandler:        payloadHandler,
		tokenTracker:          tokenTracker,
		providerUUID:          cfg.ProviderUUID,
		bech32Prefix:          cfg.Bech32Prefix,
		tlsCertFile:           cfg.TLSCertFile,
		tlsKeyFile:            cfg.TLSKeyFile,
		rateLimiter:           rateLimiter,
		tenantRateLimiter:     tenantRateLimiter,
		callbackPublisher:     callbackPublisher,
		callbackAuthenticator: callbackAuth,
		statusChecker:         statusChecker,
	}

	// Register routes - unauthenticated endpoints
	router.HandleFunc("/health", handlers.HealthCheck).Methods("GET")
	router.Handle("/metrics", promhttp.Handler()).Methods("GET")
	router.HandleFunc("/callbacks/provision", s.handleProvisionCallback).Methods("POST")

	// Create a subrouter for authenticated endpoints with tenant rate limiting
	authRouter := router.PathPrefix("/v1").Subrouter()
	authRouter.HandleFunc("/leases/{lease_uuid}/connection", handlers.GetLeaseConnection).Methods("GET")
	authRouter.HandleFunc("/leases/{lease_uuid}/status", handlers.GetLeaseStatus).Methods("GET")
	authRouter.HandleFunc("/leases/{lease_uuid}/data", s.handlePayloadUpload).Methods("POST")

	// Apply tenant rate limiting to authenticated endpoints only
	if tenantRateLimiter != nil {
		authRouter.Use(tenantRateLimiter.Middleware(cfg.Bech32Prefix))
	}

	// Add global middleware (order matters: rate limit first, then body size, then logging)
	router.Use(rateLimiter.Middleware)
	router.Use(maxBodySizeMiddleware(maxBodySize))
	router.Use(loggingMiddleware)

	s.server = &http.Server{
		Addr:         cfg.Addr,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return s, nil
}

// handleProvisionCallback handles POST /callbacks/provision from backends.
func (s *Server) handleProvisionCallback(w http.ResponseWriter, r *http.Request) {
	if s.callbackPublisher == nil {
		slog.Error("callback publisher not configured")
		writeError(w, "service not configured", http.StatusServiceUnavailable)
		return
	}

	// Verify callback authentication
	if s.callbackAuthenticator == nil {
		slog.Error("callback authenticator not configured")
		writeError(w, "service not configured", http.StatusServiceUnavailable)
		return
	}

	body, err := s.callbackAuthenticator.VerifyRequest(r)
	if err != nil {
		slog.Warn("callback authentication failed",
			"error", err,
			"remote_addr", r.RemoteAddr,
		)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var callback backend.CallbackPayload
	if err := decodeJSONBytes(body, &callback); err != nil {
		slog.Warn("invalid callback payload", "error", err)
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if callback.LeaseUUID == "" {
		writeError(w, "lease_uuid is required", http.StatusBadRequest)
		return
	}

	if !config.IsValidUUID(callback.LeaseUUID) {
		writeError(w, "lease_uuid must be a valid UUID", http.StatusBadRequest)
		return
	}

	if callback.Status != backend.CallbackStatusSuccess && callback.Status != backend.CallbackStatusFailed {
		writeError(w, "status must be 'success' or 'failed'", http.StatusBadRequest)
		return
	}

	// Idempotency check: if the lease is no longer in-flight, this is a duplicate callback.
	// Return 200 OK immediately to prevent duplicate Watermill messages.
	// This handles the case where a backend retries a callback after we've already processed it.
	if s.statusChecker != nil && !s.statusChecker.IsInFlight(callback.LeaseUUID) {
		slog.Debug("ignoring duplicate callback for processed lease",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
		metrics.DuplicateCallbacksTotal.Inc()
		w.WriteHeader(http.StatusOK)
		return
	}

	slog.Info("received provision callback",
		"lease_uuid", callback.LeaseUUID,
		"status", callback.Status,
	)

	if err := s.callbackPublisher.PublishCallback(callback); err != nil {
		slog.Error("failed to publish callback", "error", err)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePayloadUpload handles POST /v1/leases/{lease_uuid}/data from tenants.
func (s *Server) handlePayloadUpload(w http.ResponseWriter, r *http.Request) {
	if s.payloadHandler == nil {
		slog.Error("payload handler not configured")
		writeError(w, "service not configured", http.StatusServiceUnavailable)
		return
	}

	s.payloadHandler.HandlePayloadUpload(w, r)
}

// Start begins serving HTTP requests.
func (s *Server) Start(ctx context.Context) error {
	tlsEnabled := s.tlsCertFile != "" && s.tlsKeyFile != ""

	if tlsEnabled {
		slog.Info("starting API server with TLS", "addr", s.addr)
	} else {
		slog.Info("starting API server", "addr", s.addr)
	}

	errChan := make(chan error, 1)

	go func() {
		var err error
		if tlsEnabled {
			err = s.server.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile)
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down API server")

	shutdownCtx, cancel := context.WithTimeout(ctx, serverShutdownTimeout)
	defer cancel()

	// Shutdown HTTP server first
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	// Close token tracker
	if s.tokenTracker != nil {
		if err := s.tokenTracker.Close(); err != nil {
			slog.Error("failed to close token tracker", "error", err)
			return err
		}
	}

	return nil
}

// maxBodySizeMiddleware limits the size of request bodies.
func maxBodySizeMiddleware(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// loggingMiddleware logs incoming HTTP requests and records metrics.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		statusStr := strconv.Itoa(wrapped.statusCode)

		// Normalize path for metrics to avoid high cardinality
		// Replace UUIDs with placeholder
		path := normalizePath(r.URL.Path)

		// Record metrics
		metrics.APIRequestDuration.WithLabelValues(r.Method, path, statusStr).Observe(duration.Seconds())
		metrics.APIRequestsTotal.WithLabelValues(r.Method, path, statusStr).Inc()

		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", duration,
			"remote_addr", r.RemoteAddr,
		)
	})
}

// normalizePath replaces dynamic path segments (UUIDs) with placeholders
// to prevent high cardinality in metrics labels.
func normalizePath(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if _, err := uuid.Parse(segment); err == nil {
			segments[i] = "{uuid}"
		}
	}
	return strings.Join(segments, "/")
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
