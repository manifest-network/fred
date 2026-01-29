package api

import (
	"context"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

// TrustedProxyConfig holds parsed trusted proxy CIDR ranges.
type TrustedProxyConfig struct {
	cidrs []*net.IPNet
}

// NewTrustedProxyConfig parses CIDR strings into a trusted proxy configuration.
// Invalid CIDR strings are logged and skipped.
func NewTrustedProxyConfig(cidrs []string) *TrustedProxyConfig {
	config := &TrustedProxyConfig{}
	for _, cidr := range cidrs {
		// Handle bare IPs by converting to CIDR notation
		if !strings.Contains(cidr, "/") {
			if strings.Contains(cidr, ":") {
				cidr = cidr + "/128" // IPv6
			} else {
				cidr = cidr + "/32" // IPv4
			}
		}
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			slog.Warn("invalid trusted proxy CIDR, skipping", "cidr", cidr, "error", err)
			continue
		}
		config.cidrs = append(config.cidrs, ipNet)
	}
	return config
}

// IsTrusted returns true if the given IP address is within a trusted proxy range.
func (c *TrustedProxyConfig) IsTrusted(ipStr string) bool {
	if c == nil || len(c.cidrs) == 0 {
		return false
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, cidr := range c.cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

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

// calcRetryAfterSeconds calculates the Retry-After header value from a rate limit.
// Returns the time until one token is available (1/rate), rounded up to at least 1 second.
func calcRetryAfterSeconds(r rate.Limit) string {
	if r <= 0 {
		return "1"
	}
	// Time for one token = 1/rate seconds, rounded up
	seconds := int(math.Ceil(1.0 / float64(r)))
	return strconv.Itoa(seconds)
}

// RateLimiter implements per-IP rate limiting using a token bucket algorithm.
type RateLimiter struct {
	visitors       *lru.LRU[string, *rate.Limiter]
	rate           rate.Limit // requests per second
	burst          int        // max burst size
	trustedProxies *TrustedProxyConfig
}

// NewRateLimiter creates a new rate limiter.
// rps is requests per second, burst is the maximum burst size.
// trustedProxies is optional - if nil or empty, X-Forwarded-For headers are ignored.
func NewRateLimiter(rps float64, burst int, trustedProxies *TrustedProxyConfig) *RateLimiter {
	// expirable.LRU handles both LRU eviction (when maxVisitors reached)
	// and TTL-based expiration (cleanup of stale entries)
	cache := lru.NewLRU[string, *rate.Limiter](maxVisitors, nil, visitorTTL)

	return &RateLimiter{
		visitors:       cache,
		rate:           rate.Limit(rps),
		burst:          burst,
		trustedProxies: trustedProxies,
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

// retryAfterSeconds calculates the Retry-After header value in seconds.
// Returns the time until one token is available, rounded up to at least 1 second.
func (rl *RateLimiter) retryAfterSeconds() string {
	return calcRetryAfterSeconds(rl.rate)
}

// Middleware returns an HTTP middleware that enforces rate limiting.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := rl.getClientIP(r)

		limiter := rl.getVisitor(ip)
		if !limiter.Allow() {
			slog.Warn("rate limit exceeded", "ip", ip, "path", r.URL.Path)
			w.Header().Set("Retry-After", rl.retryAfterSeconds())
			writeError(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getClientIP extracts the client IP from the request.
// It only trusts X-Forwarded-For and X-Real-IP headers if the request
// comes from a configured trusted proxy. This prevents IP spoofing attacks
// where malicious clients set these headers to bypass rate limiting.
func (rl *RateLimiter) getClientIP(r *http.Request) string {
	// Extract the direct connection IP first
	directIP := extractDirectIP(r.RemoteAddr)

	// Only trust forwarded headers if the direct connection is from a trusted proxy
	if rl.trustedProxies != nil && rl.trustedProxies.IsTrusted(directIP) {
		// Check X-Forwarded-For header (may contain multiple IPs)
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// Take the first IP (original client)
			ip, _, _ := strings.Cut(xff, ",")
			ip = strings.TrimSpace(ip)
			// Validate it's a real IP address
			if validIP := net.ParseIP(ip); validIP != nil {
				return ip
			}
			// Invalid IP in header, fall through to X-Real-IP
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
	}

	return directIP
}

// extractDirectIP extracts the IP address from RemoteAddr (host:port format).
func extractDirectIP(remoteAddr string) string {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
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

// retryAfterSeconds calculates the Retry-After header value in seconds.
// Returns the time until one token is available, rounded up to at least 1 second.
func (tl *TenantRateLimiter) retryAfterSeconds() string {
	return calcRetryAfterSeconds(tl.rate)
}

// TenantKey is the context key type for storing tenant info.
type tenantKey struct{}

// ContextWithTenant returns a new context with the tenant value set.
func ContextWithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
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
				w.Header().Set("Retry-After", tl.retryAfterSeconds())
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
	tokenStr, err := extractBearerToken(r)
	if err != nil {
		return ""
	}

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		return ""
	}

	// Validate the token to ensure it's properly signed
	if err := token.Validate(bech32Prefix); err != nil {
		return ""
	}

	return token.Tenant
}
