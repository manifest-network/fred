package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

const (
	// maxVisitors limits the number of tracked IPs to prevent memory exhaustion
	maxVisitors = 10000
	// visitorTTL is how long a visitor entry stays in the cache without access
	visitorTTL = 3 * time.Minute

	// maxTenants limits the number of tracked tenants to prevent memory exhaustion
	maxTenants = 10000
	// tenantTTL is how long a tenant entry stays in the cache without access
	tenantTTL = 5 * time.Minute
)

// RateLimiter implements per-IP rate limiting using a token bucket algorithm.
type RateLimiter struct {
	visitors *lru.LRU[string, *rate.Limiter]
	rate     rate.Limit // requests per second
	burst    int        // max burst size
}

// NewRateLimiter creates a new rate limiter.
// rps is requests per second, burst is the maximum burst size.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	// expirable.LRU handles both LRU eviction (when maxVisitors reached)
	// and TTL-based expiration (cleanup of stale entries)
	cache := lru.NewLRU[string, *rate.Limiter](maxVisitors, nil, visitorTTL)

	return &RateLimiter{
		visitors: cache,
		rate:     rate.Limit(rps),
		burst:    burst,
	}
}

// getVisitor retrieves or creates a rate limiter for the given IP.
func (rl *RateLimiter) getVisitor(ip string) *rate.Limiter {
	// Try to get existing limiter (also refreshes TTL)
	if limiter, ok := rl.visitors.Get(ip); ok {
		return limiter
	}

	// Create new limiter - LRU will automatically evict oldest if at capacity
	limiter := rate.NewLimiter(rl.rate, rl.burst)
	rl.visitors.Add(ip, limiter)
	return limiter
}

// Middleware returns an HTTP middleware that enforces rate limiting.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := getClientIP(r)

		limiter := rl.getVisitor(ip)
		if !limiter.Allow() {
			slog.Warn("rate limit exceeded", "ip", ip, "path", r.URL.Path)
			w.Header().Set("Retry-After", "1")
			writeError(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getClientIP extracts the client IP from the request.
// It checks X-Forwarded-For and X-Real-IP headers for proxied requests.
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (may contain multiple IPs)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP (original client)
		ip, _, _ := strings.Cut(xff, ",")
		ip = strings.TrimSpace(ip)
		// Validate it's a real IP address
		if validIP := net.ParseIP(ip); validIP != nil {
			return ip
		}
		// Invalid IP in header, fall through to RemoteAddr
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		ip := strings.TrimSpace(xri)
		// Validate it's a real IP address
		if validIP := net.ParseIP(ip); validIP != nil {
			return ip
		}
		// Invalid IP in header, fall through to RemoteAddr
	}

	// Fall back to RemoteAddr
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// TenantRateLimiter implements per-tenant rate limiting using a token bucket algorithm.
// This is used for authenticated endpoints where the tenant identity is known.
type TenantRateLimiter struct {
	tenants *lru.LRU[string, *rate.Limiter]
	rate    rate.Limit // requests per second
	burst   int        // max burst size
}

// NewTenantRateLimiter creates a new per-tenant rate limiter.
// rps is requests per second, burst is the maximum burst size per tenant.
func NewTenantRateLimiter(rps float64, burst int) *TenantRateLimiter {
	cache := lru.NewLRU[string, *rate.Limiter](maxTenants, nil, tenantTTL)

	return &TenantRateLimiter{
		tenants: cache,
		rate:    rate.Limit(rps),
		burst:   burst,
	}
}

// getLimiter retrieves or creates a rate limiter for the given tenant.
func (tl *TenantRateLimiter) getLimiter(tenant string) *rate.Limiter {
	if limiter, ok := tl.tenants.Get(tenant); ok {
		return limiter
	}

	limiter := rate.NewLimiter(tl.rate, tl.burst)
	tl.tenants.Add(tenant, limiter)
	return limiter
}

// Allow checks if a request from the tenant is allowed.
func (tl *TenantRateLimiter) Allow(tenant string) bool {
	return tl.getLimiter(tenant).Allow()
}

// TenantKey is the context key type for storing tenant info.
type tenantKey struct{}

// ContextWithTenant returns a new context with the tenant value set.
func ContextWithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// TenantFromContext extracts the tenant from the context.
// Returns empty string if not set.
func TenantFromContext(ctx context.Context) string {
	tenant, _ := ctx.Value(tenantKey{}).(string)
	return tenant
}

// TenantRateLimitMiddleware returns middleware that applies per-tenant rate limiting.
// It extracts the tenant from the Authorization header and applies the limit.
// If tenant cannot be extracted, the request proceeds without tenant-based limiting.
func (tl *TenantRateLimiter) Middleware(bech32Prefix string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to extract tenant from bearer token
			tenant := extractTenantFromAuth(r, bech32Prefix)
			if tenant == "" {
				// No tenant info available - proceed without tenant rate limiting
				// (IP-based rate limiting still applies)
				next.ServeHTTP(w, r)
				return
			}

			// Apply tenant rate limit
			if !tl.Allow(tenant) {
				slog.Warn("tenant rate limit exceeded",
					"tenant", tenant,
					"path", r.URL.Path,
				)
				w.Header().Set("Retry-After", "1")
				writeError(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			// Store tenant in context for downstream handlers
			ctx := ContextWithTenant(r.Context(), tenant)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractTenantFromAuth attempts to extract the tenant address from the Authorization header.
// Returns empty string if extraction fails.
func extractTenantFromAuth(r *http.Request, bech32Prefix string) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}

	token, err := ParseAuthToken(parts[1])
	if err != nil {
		return ""
	}

	// Validate the token to ensure it's properly signed
	if err := token.Validate(bech32Prefix); err != nil {
		return ""
	}

	return token.Tenant
}
