package api

import (
	"bufio"
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const (
	// defaultShutdownTimeout is the default maximum time to wait for the server to shutdown gracefully.
	defaultShutdownTimeout = 30 * time.Second

	// defaultRequestTimeout is the default timeout for individual request processing.
	// This is separate from HTTP server timeouts and applies to handler logic.
	defaultRequestTimeout = 30 * time.Second

	// callbackWriteDeadlineGrace leaves enough time for http.TimeoutHandler to
	// serialize its retryable 503 after canceling callback application. The
	// bundled backend's fresh first attempt normally has a larger delivery
	// window. A retry may have less of its shared delivery budget remaining and
	// cancel the request first.
	callbackWriteDeadlineGrace = 5 * time.Second

	// readHeaderTimeout caps how long the server waits for request headers.
	// Set independently of ReadTimeout to prevent Slowloris attacks even if
	// ReadTimeout is tuned to 0 for streaming endpoints.
	readHeaderTimeout = 5 * time.Second
)

// CallbackPublisher applies authenticated backend callbacks through the
// provisioner. Implementations return only after the callback has reached a
// terminal application result, so a backend can preserve per-lease ordering by
// waiting for the HTTP response before sending the next durable delivery.
type CallbackPublisher interface {
	PublishCallback(ctx context.Context, callback backend.CallbackPayload) error
}

// callbackRequestAuthenticator returns the exact DTO it authenticated. The
// production implementation selects a per-storage-lineage key from the bounded
// body; keeping decode inside this boundary prevents authentication and
// application from interpreting duplicate JSON differently.
type callbackRequestAuthenticator interface {
	VerifyCallbackRequest(*http.Request) (backend.CallbackPayload, error)
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
	callbackAuthenticator callbackRequestAuthenticator
	statusChecker         StatusChecker
}

// ServerConfig holds configuration for the API server.
type ServerConfig struct {
	Addr                        string
	ProviderUUID                string
	Bech32Prefix                string
	TLSCertFile                 string
	TLSKeyFile                  string
	RateLimitRPS                float64
	RateLimitBurst              int
	TenantRateLimitRPS          float64  // Per-tenant rate limit (requests per second), 0 = disabled
	TenantRateLimitBurst        int      // Per-tenant burst limit
	TrustedProxies              []string // CIDR blocks of trusted reverse proxies for X-Forwarded-For
	CORSOrigins                 []string // Allowed CORS origins (e.g. ["*"] for all). Empty or nil disables CORS middleware.
	ReadTimeout                 time.Duration
	WriteTimeout                time.Duration
	IdleTimeout                 time.Duration
	RequestTimeout              time.Duration // Timeout for individual request processing (default: 30s)
	CallbackApplicationTimeout  time.Duration // Timeout for terminal callback application (default: backend.DefaultCallbackApplicationTimeout)
	ShutdownTimeout             time.Duration // Timeout for graceful shutdown (default: 30s)
	MaxRequestBodySize          int64
	CallbackSecret              string                        // Non-production legacy single HMAC secret for isolated embeddings.
	CallbackHMACSecrets         map[backendidentity.ID]string // Production callback keys indexed by immutable backend storage identity.
	CallbackCanonicalPathPrefix string                        // Path prefix prepended to inbound URIs before HMAC verification (proxy stripPrefix compensation)
	TokenTrackerDBPath          string                        // Path to token tracker database (enables replay protection)
	CallbackBaseURL             string                        // Base URL for backend callbacks (used by restart/update)
}

// ServerDeps holds the runtime dependencies for the API server.
// These are the collaborators injected into the server at startup.
type ServerDeps struct {
	ChainClient        ChainClient
	BackendRouter      *backend.Router
	CallbackPublisher  CallbackPublisher
	PayloadPublisher   PayloadPublisher
	PayloadPersister   PayloadPersister   // Required — /update returns 500 without it (ENG-619).
	PayloadStoreHealth PayloadStoreHealth // Optional — health probe for the payload store's bbolt DB.
	StatusChecker      StatusChecker
	PlacementLookup    PlacementLookup            // Required by providerd; nil is supported only by isolated/test API embeddings.
	LifecycleCallbacks LifecycleCallbackAuthority // Required by providerd for typed restart/update callback routes.
	MaintenanceClaims  MaintenanceClaims          // Required by providerd for restart/update lifecycle exclusion.
	RestoreService     RestoreService             // Required by /restore; missing service returns 503.
	EventBroker        *EventBroker               // Optional — if nil, the events endpoint returns 501.
}

// NewServer creates a new API server.
// Returns an error if token tracker initialization fails.
func NewServer(cfg ServerConfig, deps ServerDeps) (*Server, error) {
	callbackApplicationTimeout, err := resolveCallbackApplicationTimeout(cfg.CallbackApplicationTimeout)
	if err != nil {
		return nil, err
	}

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
		Client:             client,
		BackendRouter:      backendRouter,
		TokenTracker:       tracker,
		StatusChecker:      statusChecker,
		PlacementLookup:    placementLookup,
		LifecycleCallbacks: deps.LifecycleCallbacks,
		MaintenanceClaims:  deps.MaintenanceClaims,
		RestoreService:     deps.RestoreService,
		PayloadPersister:   deps.PayloadPersister,
		PayloadStoreHealth: deps.PayloadStoreHealth,
		EventBroker:        eventBroker,
		ProviderUUID:       cfg.ProviderUUID,
		Bech32Prefix:       cfg.Bech32Prefix,
		CallbackBaseURL:    cfg.CallbackBaseURL,
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

	// Production uses a storage-lineage keyring. The single-key constructor stays
	// available for isolated non-production embeddings, but accepting both would
	// create an ambiguous authentication policy.
	var callbackAuth callbackRequestAuthenticator
	if cfg.CallbackSecret != "" && len(cfg.CallbackHMACSecrets) != 0 {
		return nil, fmt.Errorf("callback HMAC keyring cannot be combined with legacy callback secret")
	}
	if len(cfg.CallbackHMACSecrets) != 0 {
		keyring, err := NewCallbackKeyringAuthenticator(cfg.CallbackHMACSecrets)
		if err != nil {
			return nil, fmt.Errorf("create callback HMAC keyring: %w", err)
		}
		callbackAuth = keyring.WithCanonicalPathPrefix(cfg.CallbackCanonicalPathPrefix)
	} else if cfg.CallbackSecret != "" {
		var err error
		legacyAuth, err := NewCallbackAuthenticator(cfg.CallbackSecret)
		if err != nil {
			return nil, fmt.Errorf("create callback authenticator: %w", err)
		}
		callbackAuth = legacyAuth.WithCanonicalPathPrefix(cfg.CallbackCanonicalPathPrefix)
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
	withCallbackTimeout := callbackTimeoutMiddleware(callbackApplicationTimeout)

	// Unauthenticated routes
	mux.Handle("GET /health", withTimeout(http.HandlerFunc(handlers.HealthCheck)))
	// Deep readiness. Deliberately NOT what a load balancer should poll — see
	// the Readyz doc comment. /health is the liveness contract.
	mux.Handle("GET /readyz", withTimeout(http.HandlerFunc(handlers.Readyz)))
	mux.Handle("GET /metrics", withTimeout(promhttp.Handler()))
	mux.Handle("GET /workloads", withTimeout(http.HandlerFunc(handlers.GetWorkloads)))
	mux.Handle("POST /callbacks/provision", withCallbackTimeout(http.HandlerFunc(s.handleProvisionCallback)))

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
	mux.Handle("POST /v1/leases/{lease_uuid}/restore", withTimeout(withAuthRL(handlers.RestoreLease)))
	mux.Handle("POST /v1/leases/{lease_uuid}/update", withTimeout(withAuthRL(handlers.UpdateLease)))
	mux.Handle("GET /v1/leases/{lease_uuid}/releases", withTimeout(withAuthRL(handlers.GetLeaseReleases)))

	// WebSocket endpoint: no request timeout. The WebSocket handler manages its own
	// lifecycle with ping/pong frames and per-write deadlines.
	// WSTokenPromoter promotes the "token" query param to the Authorization header
	// and strips it from the URL (WebSocket clients cannot set custom headers).
	mux.Handle("GET /v1/leases/{lease_uuid}/events", WSTokenPromoter(withAuthRL(handlers.StreamLeaseEvents)))

	// Apply global middleware. Each wrapper becomes the new outermost layer,
	// so the last-applied middleware runs first. Execution order:
	// (cors, when enabled) → securityHeaders → rateLimiter → maxBody → logging → mux → [per-route timeout] → handler
	var handler http.Handler = mux
	handler = loggingMiddleware(handler)
	handler = maxBodySizeMiddleware(maxBodySize)(handler)
	handler = rateLimiter.Middleware(handler)
	handler = securityHeadersMiddleware(handler)
	if len(cfg.CORSOrigins) > 0 {
		slog.Info("CORS middleware enabled", "origins", cfg.CORSOrigins)
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
		slog.Info("CORS middleware disabled (cors_origins is empty)")
	}

	s.server = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	return s, nil
}

func resolveCallbackApplicationTimeout(configured time.Duration) (time.Duration, error) {
	timeout := cmp.Or(max(configured, 0), backend.DefaultCallbackApplicationTimeout)
	// The bundled sender's delivery deadline is a protocol constant, not a
	// paired runtime setting. Permit shorter application budgets for tests and
	// constrained deployments, but never let an embedding consume the fixed
	// response grace by stretching Fred's side past the protocol default.
	if timeout > backend.DefaultCallbackApplicationTimeout {
		return 0, fmt.Errorf(
			"callback application timeout %s must not exceed protocol default %s",
			timeout,
			backend.DefaultCallbackApplicationTimeout,
		)
	}
	return timeout, nil
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

	callback, err := s.callbackAuthenticator.VerifyCallbackRequest(r)
	if err != nil {
		if errors.Is(err, errInvalidCallbackPayload) {
			slog.Warn("invalid callback payload",
				"error", err,
				"remote_addr", r.RemoteAddr,
			)
			writeError(w, "invalid request body", http.StatusBadRequest)
			return
		}
		slog.Warn("callback authentication failed",
			"error", err,
			"remote_addr", r.RemoteAddr,
		)
		writeError(w, errMsgUnauthorized, http.StatusUnauthorized)
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

	// The backend posts to the callback URL fred supplied. RequestURI is part of
	// the verified HMAC, so either typed capability is authenticated even though
	// backends need not understand or copy it into their JSON payload. Parse the
	// raw query with error reporting: URL.Query silently drops malformed escapes,
	// which could otherwise downgrade a malformed typed route to legacy.
	callback.OperationID = ""
	callback.LifecycleID = ""
	callbackQuery, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, "callback query is malformed", http.StatusBadRequest)
		return
	}
	operationID, present, err := operation.ParseQuery(callbackQuery)
	if err != nil {
		writeError(w, "operation_id must be a single canonical UUIDv4", http.StatusBadRequest)
		return
	}
	if present {
		callback.OperationID = operationID.String()
	}
	lifecycleID, lifecyclePresent, err := lifecycle.ParseQuery(callbackQuery)
	if err != nil {
		writeError(w, "lifecycle_id must be a single canonical UUIDv4", http.StatusBadRequest)
		return
	}
	if present && lifecyclePresent {
		writeError(w, "callback URL must carry exactly one capability kind", http.StatusBadRequest)
		return
	}
	if lifecyclePresent {
		callback.LifecycleID = lifecycleID.String()
	}

	switch callback.Status {
	case backend.CallbackStatusSuccess, backend.CallbackStatusFailed, backend.CallbackStatusDeprovisioned:
		// ok
	default:
		writeError(w, "status must be 'success', 'failed', or 'deprovisioned'", http.StatusBadRequest)
		return
	}
	if present && callback.Status == backend.CallbackStatusDeprovisioned {
		writeError(w, "deprovisioned status requires lifecycle or legacy callback authority", http.StatusBadRequest)
		return
	}
	if callback.Retained && callback.Status != backend.CallbackStatusDeprovisioned {
		writeError(w, "retained requires deprovisioned status", http.StatusBadRequest)
		return
	}
	// Do not short-circuit callbacks that have no active operation-registry entry.
	// Restart and update use lifecycle authority for an already-active lease, so
	// their completion callbacks legitimately have no provision/restore operation.
	// The synchronous callback application selects settlement from the typed
	// callback authority instead of treating registry membership as authority.

	slog.Info("received provision callback",
		"lease_uuid", callback.LeaseUUID,
		"status", callback.Status,
	)

	if err := s.callbackPublisher.PublishCallback(r.Context(), callback); err != nil {
		// Callback delivery is owned by the backend's durable outbox. Any
		// application failure is retryable, including the brief startup and
		// shutdown windows where the provisioner is not accepting work.
		slog.Error("failed to apply callback", "error", err)
		writeError(w, errMsgServiceUnavailable, http.StatusServiceUnavailable)
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

		// Use the matched route template (bounded cardinality) for the metric
		// path label; the raw URL path still goes to the structured log below.
		path := metricPath(r)

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

// metricPath returns a bounded-cardinality label for the request: the matched
// ServeMux route template with its leading method/host stripped (e.g.
// "/v1/leases/{lease_uuid}/status"), or "unmatched" for a request that matched
// no route (e.g. an unauthenticated 404 path scan). Labeling metrics with the
// template rather than r.URL.Path bounds the {path} label to the finite set of
// registered routes + 1, closing an unauthenticated cardinality-DoS vector. (F28)
func metricPath(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	// Pattern is "[METHOD ][HOST]/path"; the path starts at the first '/'.
	if i := strings.IndexByte(r.Pattern, '/'); i >= 0 {
		return r.Pattern[i:]
	}
	return r.Pattern
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// Unwrap lets http.ResponseController reach the underlying net/http writer.
// The callback route uses it to extend the connection write deadline beyond
// the generic server default while a chain settlement is in progress.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
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

// callbackTimeoutMiddleware gives callback settlement its protocol-level
// budget rather than the generic request budget. Callback application may
// wait for a chain transaction, while the backend retains the durable outbox
// head until this handler returns 2xx. The connection write deadline must
// therefore outlive application cancellation long enough to return a 503;
// otherwise http.Server.WriteTimeout can silently win first and obscure the
// retry verdict from the backend.
func callbackTimeoutMiddleware(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		th := http.TimeoutHandler(next, timeout, `{"error":"callback application timeout","code":503}`)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			controller := http.NewResponseController(w)
			if err := controller.SetWriteDeadline(time.Now().Add(timeout + callbackWriteDeadlineGrace)); err != nil && !errors.Is(err, http.ErrNotSupported) {
				slog.Warn("failed to extend callback response write deadline", "error", err)
			}
			defer func() {
				if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
					slog.Warn("failed to clear callback response write deadline", "error", err)
				}
			}()

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
