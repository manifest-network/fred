package httpserver

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	// MaxRequestBodySize is the maximum size of an inbound request body (1 MiB).
	MaxRequestBodySize = 1 << 20
	// MaxTailLines is the upper bound for log tail requests.
	MaxTailLines = 10000
)

// HmacAuthMiddleware returns middleware that verifies HMAC-SHA256 signatures
// on requests. Requests without a valid signature are rejected with 401.
func HmacAuthMiddleware(secret string, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Limit request body size
			r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodySize)

			sig := r.Header.Get(hmacauth.SignatureHeader)
			if sig == "" {
				logger.Warn("missing signature header", "remote", r.RemoteAddr, "path", r.URL.Path)
				JSONError(w, http.StatusUnauthorized, "missing signature")
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Warn("failed to read request body", "error", err)
				JSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}

			if err := hmacauth.VerifyRequest(secret, r, body, sig, 5*time.Minute); err != nil {
				logger.Warn("signature verification failed",
					"error", err,
					"remote", r.RemoteAddr,
					"path", r.URL.Path,
				)
				JSONError(w, http.StatusUnauthorized, "invalid signature")
				return
			}

			// Replace the body so handlers can read it
			r.Body = io.NopCloser(bytes.NewReader(body))
			next.ServeHTTP(w, r)
		})
	}
}
