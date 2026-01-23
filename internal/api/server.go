package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
)

const (
	// serverShutdownTimeout is the maximum time to wait for the server to shutdown gracefully.
	serverShutdownTimeout = 10 * time.Second
)

// CallbackPublisher publishes backend callbacks to the provisioner.
type CallbackPublisher interface {
	PublishCallback(callback backend.CallbackPayload) error
}

// Server is the HTTP API server.
type Server struct {
	addr                  string
	server                *http.Server
	handlers              *Handlers
	providerUUID          string
	tlsCertFile           string
	tlsKeyFile            string
	rateLimiter           *RateLimiter
	callbackPublisher     CallbackPublisher
	callbackAuthenticator *CallbackAuthenticator
}

// ServerConfig holds configuration for the API server.
type ServerConfig struct {
	Addr               string
	ProviderUUID       string
	Bech32Prefix       string
	TLSCertFile        string
	TLSKeyFile         string
	RateLimitRPS       float64
	RateLimitBurst     int
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	MaxRequestBodySize int64
	CallbackSecret     string // HMAC secret for callback authentication
}

// NewServer creates a new API server.
func NewServer(cfg ServerConfig, client *chain.Client, backendRouter *backend.Router, callbackPublisher CallbackPublisher) *Server {
	handlers := NewHandlers(client, backendRouter, cfg.ProviderUUID, cfg.Bech32Prefix)
	rateLimiter := NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst)

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

	router := mux.NewRouter()

	s := &Server{
		addr:                  cfg.Addr,
		handlers:              handlers,
		providerUUID:          cfg.ProviderUUID,
		tlsCertFile:           cfg.TLSCertFile,
		tlsKeyFile:            cfg.TLSKeyFile,
		rateLimiter:           rateLimiter,
		callbackPublisher:     callbackPublisher,
		callbackAuthenticator: callbackAuth,
	}

	// Register routes
	router.HandleFunc("/health", handlers.HealthCheck).Methods("GET")
	router.HandleFunc("/v1/leases/{lease_uuid}/connection", handlers.GetLeaseConnection).Methods("GET")
	router.HandleFunc("/callbacks/provision", s.handleProvisionCallback).Methods("POST")

	// Add middleware (order matters: rate limit first, then body size, then logging)
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

	return s
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

	if callback.Status != "success" && callback.Status != "failed" {
		writeError(w, "status must be 'success' or 'failed'", http.StatusBadRequest)
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

	return s.server.Shutdown(shutdownCtx)
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

// loggingMiddleware logs incoming HTTP requests.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		slog.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", time.Since(start),
			"remote_addr", r.RemoteAddr,
		)
	})
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
