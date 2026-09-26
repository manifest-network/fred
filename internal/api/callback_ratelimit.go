package api

import (
	"context"
	"net/http"

	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
)

const (
	callbackRateLimitRPS   = 100
	callbackRateLimitBurst = 200
)

// Callback admission is applied at the route, inside its body/time bounds.
// Tenant traffic sharing the backend's NAT address cannot spend its tokens.
func callbackIngressRateLimit(tenant *RateLimiter, next http.Handler) http.Handler {
	tenantHandler := tenant.Middleware(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/callbacks/provision" {
			next.ServeHTTP(w, r)
			return
		}
		tenantHandler.ServeHTTP(w, r)
	})
}

type callbackProofContextKey struct{}

// An exhausted pre-authentication bucket must not let a same-NAT tenant starve
// a backend by flooding this endpoint itself. Such requests reach application
// only with a valid HMAC proof, and still spend the authenticated storage budget.
// Verification is bounded by the route's body limit and callback timeout.
func (s *Server) callbackPreauthBudget(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limiter := s.callbackIngressLimiter
		if limiter.getVisitor(limiter.getClientIP(r)).Allow() {
			next.ServeHTTP(w, r)
			return
		}
		if s.callbackAuthenticator != nil {
			proof, err := s.callbackAuthenticator.VerifyCallbackEvidence(r)
			if err == nil {
				ctx := context.WithValue(r.Context(), callbackProofContextKey{}, proof)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		metrics.RateLimitRejectionsTotal.WithLabelValues("callback_ingress").Inc()
		w.Header().Set("Retry-After", limiter.retryAfterSeconds())
		writeError(w, "callback rate limit exceeded", http.StatusTooManyRequests)
	})
}

func (s *Server) verifyCallback(r *http.Request) (hmacauth.VerifiedRequest, error) {
	if proof, ok := r.Context().Value(callbackProofContextKey{}).(hmacauth.VerifiedRequest); ok {
		return proof, nil
	}
	return s.callbackAuthenticator.VerifyCallbackEvidence(r)
}
