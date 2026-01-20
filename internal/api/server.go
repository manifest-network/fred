package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/manifest-network/fred/internal/chain"
)

// Server is the HTTP API server.
type Server struct {
	addr         string
	server       *http.Server
	handlers     *Handlers
	providerUUID string
	tlsCertFile  string
	tlsKeyFile   string
	rateLimiter  *RateLimiter
}

// ServerConfig holds configuration for the API server.
type ServerConfig struct {
	Addr           string
	ProviderUUID   string
	Bech32Prefix   string
	TLSCertFile    string
	TLSKeyFile     string
	RateLimitRPS   float64
	RateLimitBurst int
}

// NewServer creates a new API server.
func NewServer(cfg ServerConfig, client *chain.Client) *Server {
	handlers := NewHandlers(client, cfg.ProviderUUID, cfg.Bech32Prefix)
	rateLimiter := NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst)

	router := mux.NewRouter()

	// Register routes
	router.HandleFunc("/health", handlers.HealthCheck).Methods("GET")
	router.HandleFunc("/v1/leases/{lease_uuid}/connection", handlers.GetLeaseConnection).Methods("GET")

	// Add middleware (order matters: rate limit first, then body size, then logging)
	router.Use(rateLimiter.Middleware)
	router.Use(maxBodySizeMiddleware(1 << 20)) // 1MB max request body
	router.Use(loggingMiddleware)

	server := &http.Server{
		Addr:         cfg.Addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		addr:         cfg.Addr,
		server:       server,
		handlers:     handlers,
		providerUUID: cfg.ProviderUUID,
		tlsCertFile:  cfg.TLSCertFile,
		tlsKeyFile:   cfg.TLSKeyFile,
		rateLimiter:  rateLimiter,
	}
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
		if err != nil && err != http.ErrServerClosed {
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
func (s *Server) Shutdown(ctx context.Context) {
	slog.Info("shutting down API server")

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shutdown server gracefully", "error", err)
	}
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
