package api

import (
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
