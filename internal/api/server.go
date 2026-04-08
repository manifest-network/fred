package api

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
)

const (
	// defaultShutdownTimeout is the default maximum time to wait for the server to shutdown gracefully.
	defaultShutdownTimeout = 30 * time.Second

	// defaultRequestTimeout is the default timeout for individual request processing.
	// This is separate from HTTP server timeouts and applies to handler logic.
	defaultRequestTimeout = 30 * time.Second
)

// CallbackPublisher publishes backend callbacks to the provisioner.
type CallbackPublisher interface {
	PublishCallback(callback backend.CallbackPayload) error
}

// StatusChecker provides status information about provisioning.
// Typically implemented by the provisioner.Manager.
type StatusChecker interface {
	HasPayload(leaseUUID string) (bool, error)
	IsInFlight(leaseUUID string) bool
	InFlightCount() int
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
	shutdownTimeout       time.Duration
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
	CORSOrigins          []string // Allowed CORS origins; empty disables CORS middleware entirely
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	RequestTimeout       time.Duration // Timeout for individual request processing (default: 30s)
	ShutdownTimeout      time.Duration // Timeout for graceful shutdown (default: 30s)
	MaxRequestBodySize   int64
	CallbackSecret       string // HMAC secret for callback authentication
	TokenTrackerDBPath   string // Path to token tracker database (enables replay protection)
	CallbackBaseURL      string // Base URL for backend callbacks (used by restart/update)
}

// ServerDeps holds the runtime dependencies for the API server.
// These are the collaborators injected into the server at startup.
type ServerDeps struct {
	ChainClient       ChainClient
	BackendRouter     *backend.Router
	CallbackPublisher CallbackPublisher
	PayloadPublisher  PayloadPublisher
	StatusChecker     StatusChecker
	PlacementLookup   PlacementLookup // Optional — if nil, placement routing is disabled.
	EventBroker       *EventBroker    // Optional — if nil, the events endpoint returns 501.
}

// NewServer creates a new API server.
// Returns an error if token tracker initialization fails.
func NewServer(cfg ServerConfig, deps ServerDeps) (*Server, error) {
	client := deps.ChainClient
	backendRouter := deps.BackendRouter
	callbackPublisher := deps.CallbackPublisher
	payloadPublisher := deps.PayloadPublisher
	statusChecker := deps.StatusChecker
	placementLookup := deps.PlacementLookup
	eventBroker := deps.EventBroker
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

	// Avoid the nil-concrete-pointer-in-interface gotcha: only pass
	// the tracker as the interface when it is actually non-nil.
	var tracker TokenTrackerInterface
	if tokenTracker != nil {
		tracker = tokenTracker
	}
	handlers := NewHandlers(HandlersConfig{
		Client:          client,
		BackendRouter:   backendRouter,
		TokenTracker:    tracker,
		StatusChecker:   statusChecker,
		PlacementLookup: placementLookup,
		EventBroker:     eventBroker,
		ProviderUUID:    cfg.ProviderUUID,
		Bech32Prefix:    cfg.Bech32Prefix,
		CallbackBaseURL: cfg.CallbackBaseURL,
	})

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
		tenantRateLimiter = NewTenantRateLimiter(cfg.TenantRateLimitRPS, cfg.TenantRateLimitBurst, cfg.Bech32Prefix)
		slog.Info("per-tenant rate limiting enabled",
			"rps", cfg.TenantRateLimitRPS,
			"burst", cfg.TenantRateLimitBurst,
		)
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	maxBodySize := cmp.Or(max(cfg.MaxRequestBodySize, 0), config.DefaultMaxRequestBodySize)
	requestTimeout := cmp.Or(max(cfg.RequestTimeout, 0), defaultRequestTimeout)
	shutdownTimeout := cmp.Or(max(cfg.ShutdownTimeout, 0), defaultShutdownTimeout)

	// Create callback authenticator if secret is provided
	var callbackAuth *CallbackAuthenticator
	if cfg.CallbackSecret != "" {
		var err error
		callbackAuth, err = NewCallbackAuthenticator(cfg.CallbackSecret)
		if err != nil {
			return nil, fmt.Errorf("create callback authenticator: %w", err)
		}
	}

	// Create payload handler if publisher is provided
	var payloadHandler *PayloadHandler
	if payloadPublisher != nil {
		payloadHandler = NewPayloadHandler(client, payloadPublisher, cfg.ProviderUUID, cfg.Bech32Prefix)
	}

	s := &Server{
		addr:                  cfg.Addr,
		handlers:              handlers,
		payloadHandler:        payloadHandler,
		tokenTracker:          tokenTracker,
		providerUUID:          cfg.ProviderUUID,
		bech32Prefix:          cfg.Bech32Prefix,
		tlsCertFile:           cfg.TLSCertFile,
		tlsKeyFile:            cfg.TLSKeyFile,
		shutdownTimeout:       shutdownTimeout,
		rateLimiter:           rateLimiter,
		tenantRateLimiter:     tenantRateLimiter,
		callbackPublisher:     callbackPublisher,
		callbackAuthenticator: callbackAuth,
		statusChecker:         statusChecker,
	}

	mux := http.NewServeMux()

	// Per-route timeout wrapper. Applied explicitly to each route so that
	// streaming endpoints (WebSocket) can opt out. Routes without withTimeout
	// still have connection-level safety via http.Server.ReadTimeout/WriteTimeout.
	withTimeout := requestTimeoutMiddleware(requestTimeout)

	// Unauthenticated routes
	mux.Handle("GET /health", withTimeout(http.HandlerFunc(handlers.HealthCheck)))
	mux.Handle("GET /metrics", withTimeout(promhttp.Handler()))
	mux.Handle("GET /workloads", withTimeout(http.HandlerFunc(handlers.GetWorkloads)))
	mux.Handle("POST /callbacks/provision", withTimeout(http.HandlerFunc(s.handleProvisionCallback)))

	// Authenticated routes with optional tenant rate limiting.
	// AuthMiddleware validates AuthTokens; PayloadAuthMiddleware validates PayloadAuthTokens.
	// Both validate tokens cryptographically BEFORE consuming from the rate-limit bucket.
	withAuthRL := func(h http.HandlerFunc) http.Handler {
		if tenantRateLimiter != nil {
			return tenantRateLimiter.AuthMiddleware()(h)
		}
		return h
	}
	withPayloadRL := func(h http.HandlerFunc) http.Handler {
		if tenantRateLimiter != nil {
			return tenantRateLimiter.PayloadAuthMiddleware()(h)
		}
		return h
	}
	mux.Handle("GET /v1/leases/{lease_uuid}/connection", withTimeout(withAuthRL(handlers.GetLeaseConnection)))
	mux.Handle("GET /v1/leases/{lease_uuid}/status", withTimeout(withAuthRL(handlers.GetLeaseStatus)))
	mux.Handle("GET /v1/leases/{lease_uuid}/provision", withTimeout(withAuthRL(handlers.GetLeaseProvision)))
	mux.Handle("GET /v1/leases/{lease_uuid}/logs", withTimeout(withAuthRL(handlers.GetLeaseLogs)))
	mux.Handle("POST /v1/leases/{lease_uuid}/data", withTimeout(withPayloadRL(s.handlePayloadUpload)))
	mux.Handle("POST /v1/leases/{lease_uuid}/restart", withTimeout(withAuthRL(handlers.RestartLease)))
	mux.Handle("POST /v1/leases/{lease_uuid}/update", withTimeout(withAuthRL(handlers.UpdateLease)))
	mux.Handle("GET /v1/leases/{lease_uuid}/releases", withTimeout(withAuthRL(handlers.GetLeaseReleases)))

	// WebSocket endpoint: no request timeout. The WebSocket handler manages its own
	// lifecycle with ping/pong frames and per-write deadlines.
	// WSTokenPromoter promotes the "token" query param to the Authorization header
	// and strips it from the URL (WebSocket clients cannot set custom headers).
	mux.Handle("GET /v1/leases/{lease_uuid}/events", WSTokenPromoter(withAuthRL(handlers.StreamLeaseEvents)))

	// Apply global middleware. Each wrapper becomes the new outermost layer,
	// so the last-applied middleware runs first. Execution order:
	// cors → securityHeaders → rateLimiter → maxBody → logging → mux → [per-route timeout] → handler
	var handler http.Handler = mux
	handler = loggingMiddleware(handler)
	handler = maxBodySizeMiddleware(maxBodySize)(handler)
	handler = rateLimiter.Middleware(handler)
	handler = securityHeadersMiddleware(handler)
	if len(cfg.CORSOrigins) > 0 {
		// CORS must be the outermost layer so OPTIONS preflights short-circuit
		// before rateLimiter consumes a token. rs/cors handles preflight in its
		// own Handler without invoking next.ServeHTTP, so per-route withTimeout
		// wrappers are bypassed for preflight (correct behavior).
		handler = cors.New(cors.Options{
			AllowedOrigins: cfg.CORSOrigins,
			// rs/cors handles OPTIONS preflight implicitly; only list real methods.
			AllowedMethods: []string{http.MethodGet, http.MethodPost},
			// Authorization is required for the /v1/leases/* routes (Bearer tokens
			// extracted by handlers.go:extractBearerToken). Content-Type is required
			// for any POST with a JSON body (application/json is not a CORS-simple
			// type). /workloads itself is unauthenticated, but the CORS middleware
			// applies globally so we list every header any route may need.
			AllowedHeaders:   []string{"Authorization", "Content-Type"},
			AllowCredentials: false,
		}).Handler(handler)
	} else {
		slog.Warn("CORS disabled — browser clients will be blocked unless cors_origins is set")
	}

	s.server = &http.Server{
		Addr:         cfg.Addr,
		Handler:      handler,
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
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	// Verify callback authentication
	if s.callbackAuthenticator == nil {
		slog.Error("callback authenticator not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	body, err := s.callbackAuthenticator.VerifyRequest(r)
	if err != nil {
		slog.Warn("callback authentication failed",
			"error", err,
			"remote_addr", r.RemoteAddr,
		)
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
		return
	}

	var callback backend.CallbackPayload
	if err := json.Unmarshal(body, &callback); err != nil {
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

	// Note: we intentionally do NOT short-circuit non-in-flight callbacks here.
	// Restart/update operations don't register in the in-flight tracker (the lease
	// is already ACTIVE), so their completion callbacks arrive with IsInFlight==false.
	// The Watermill handler (HandleBackendCallback) handles both cases correctly:
	// in-flight callbacks trigger chain acknowledgement, while non-in-flight callbacks
	// publish the status event for WebSocket clients.

	slog.Info("received provision callback",
		"lease_uuid", callback.LeaseUUID,
		"status", callback.Status,
	)

	if err := s.callbackPublisher.PublishCallback(callback); err != nil {
		slog.Error("failed to publish callback", "error", err)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handlePayloadUpload handles POST /v1/leases/{lease_uuid}/data from tenants.
func (s *Server) handlePayloadUpload(w http.ResponseWriter, r *http.Request) {
	if s.payloadHandler == nil {
		slog.Error("payload handler not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	s.payloadHandler.HandlePayloadUpload(w, r)
}

// Start begins serving HTTP requests and blocks until context is canceled or error.
// When the context is canceled, the server is gracefully shut down before returning.
func (s *Server) Start(ctx context.Context) error {
	errChan, err := s.StartBackground()
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		// Context canceled - initiate graceful shutdown.
		// Use Shutdown() for full cleanup (HTTP server + token tracker).
		if err := s.Shutdown(context.Background()); err != nil {
			slog.Error("error during server shutdown", "error", err)
		}

		// Wait for serve goroutine to exit
		<-errChan
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// StartBackground starts the server in the background and returns immediately once
// the server is listening. Returns an error channel that will receive any server
// errors. This is useful when you need to ensure the server is ready before
// proceeding with other startup tasks (e.g., reconciliation that triggers callbacks).
func (s *Server) StartBackground() (<-chan error, error) {
	tlsEnabled := s.tlsCertFile != "" && s.tlsKeyFile != ""

	// Create listener first so we know when we're ready to accept connections
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	errChan := make(chan error, 1)

	if tlsEnabled {
		// Validate TLS certificates synchronously before starting the goroutine.
		// This ensures we fail fast on bad certs and don't leak the listener.
		// (ServeTLS does not close the listener if cert loading fails.)
		if _, err := tls.LoadX509KeyPair(s.tlsCertFile, s.tlsKeyFile); err != nil {
			_ = ln.Close()
			return nil, fmt.Errorf("failed to load TLS certificates: %w", err)
		}

		slog.Info("starting API server with TLS", "addr", ln.Addr().String())

		go func() {
			// ServeTLS wraps the listener with TLS and configures HTTP/2 automatically.
			// We pass the pre-created TCP listener so we can return immediately once listening.
			// Certs were already validated above, so this should not fail on cert loading.
			err := s.server.ServeTLS(ln, s.tlsCertFile, s.tlsKeyFile)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errChan <- err
			}
			close(errChan)
		}()
	} else {
		slog.Info("starting API server", "addr", ln.Addr().String())

		go func() {
			err := s.server.Serve(ln)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errChan <- err
			}
			close(errChan)
		}()
	}

	return errChan, nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down API server")

	shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
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

// Hijack implements http.Hijacker by delegating to the underlying writer.
// Required for WebSocket upgrades through the logging middleware.
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

// requestTimeoutMiddleware applies a timeout to request processing.
// This is separate from HTTP server timeouts (ReadTimeout/WriteTimeout) and applies
// to the handler logic itself. Uses http.TimeoutHandler which properly buffers the
// response and handles the timeout safely, avoiding race conditions with ResponseWriter.
//
// We pre-set Content-Type on the real ResponseWriter so that the timeout path
// (which writes directly to it, bypassing the buffered timeoutWriter) produces
// an application/json response matching the ErrorResponse envelope used by writeError.
// On the success path, the handler's own Content-Type overwrites this pre-set value.
func requestTimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		th := http.TimeoutHandler(next, timeout, `{"error":"request timeout","code":503}`)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			th.ServeHTTP(w, r)
		})
	}
}

// WSTokenPromoter is middleware that promotes a WebSocket "token" query
// parameter to the Authorization header and strips it from the URL so it
// does not leak into proxy access logs.
func WSTokenPromoter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := r.URL.Query().Get("token"); token != "" {
			if r.Header.Get("Authorization") == "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
			q := r.URL.Query()
			q.Del("token")
			r.URL.RawQuery = q.Encode()
		}
		next.ServeHTTP(w, r)
	})
}

// securityHeadersMiddleware adds security headers to all responses.
// These headers provide defense-in-depth against common web attacks.
func securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent MIME type sniffing
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Prevent clickjacking (API shouldn't be framed)
		w.Header().Set("X-Frame-Options", "DENY")

		// Enable XSS filtering (legacy, but still useful for older browsers)
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// Prevent caching of sensitive data
		w.Header().Set("Cache-Control", "no-store")

		next.ServeHTTP(w, r)
	})
}
