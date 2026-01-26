package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	rl := NewRateLimiter(10, 10) // 10 requests per second, burst of 10

	// Should allow requests within limit
	for i := range 10 {
		limiter := rl.getVisitor("192.168.1.1")
		if !limiter.Allow() {
			t.Errorf("Request %d was blocked, should be allowed", i)
		}
	}
}

func TestRateLimiter_BlocksOverLimit(t *testing.T) {
	rl := NewRateLimiter(1, 1) // 1 request per second, burst of 1

	ip := "192.168.1.2"
	limiter := rl.getVisitor(ip)

	// First request should be allowed
	if !limiter.Allow() {
		t.Error("First request was blocked, should be allowed")
	}

	// Second request should be blocked (no tokens left)
	if limiter.Allow() {
		t.Error("Second request was allowed, should be blocked")
	}
}

func TestRateLimiter_BurstAllowed(t *testing.T) {
	rl := NewRateLimiter(1, 5) // 1 request per second, burst of 5

	ip := "192.168.1.3"
	limiter := rl.getVisitor(ip)

	// All burst requests should be allowed
	for i := range 5 {
		if !limiter.Allow() {
			t.Errorf("Burst request %d was blocked, should be allowed", i)
		}
	}

	// Next request should be blocked
	if limiter.Allow() {
		t.Error("Request after burst was allowed, should be blocked")
	}
}

func TestRateLimiter_PerIPIsolation(t *testing.T) {
	rl := NewRateLimiter(1, 1)

	// Exhaust limit for IP1
	limiter1 := rl.getVisitor("192.168.1.1")
	limiter1.Allow()

	// IP2 should still have its own limit
	limiter2 := rl.getVisitor("192.168.1.2")
	if !limiter2.Allow() {
		t.Error("Different IP was blocked, should have separate limit")
	}
}

func TestRateLimiter_Middleware(t *testing.T) {
	rl := NewRateLimiter(1, 1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := rl.Middleware(handler)

	// First request should pass
	req1 := httptest.NewRequest("GET", "/test", nil)
	req1.RemoteAddr = "192.168.1.1:12345"
	rec1 := httptest.NewRecorder()
	middleware.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("First request status = %d, want %d", rec1.Code, http.StatusOK)
	}

	// Second request should be rate limited
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.RemoteAddr = "192.168.1.1:12345"
	rec2 := httptest.NewRecorder()
	middleware.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("Second request status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}

	// Check Retry-After header
	if rec2.Header().Get("Retry-After") != "1" {
		t.Errorf("Retry-After header = %q, want %q", rec2.Header().Get("Retry-After"), "1")
	}
}

func TestGetClientIP_XForwardedFor(t *testing.T) {
	tests := []struct {
		name     string
		xff      string
		expected string
	}{
		{
			name:     "single IP",
			xff:      "203.0.113.195",
			expected: "203.0.113.195",
		},
		{
			name:     "multiple IPs",
			xff:      "203.0.113.195, 70.41.3.18, 150.172.238.178",
			expected: "203.0.113.195",
		},
		{
			name:     "with spaces",
			xff:      "  203.0.113.195  ,  70.41.3.18  ",
			expected: "203.0.113.195",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("X-Forwarded-For", tt.xff)
			req.RemoteAddr = "127.0.0.1:12345"

			ip := getClientIP(req)
			if ip != tt.expected {
				t.Errorf("getClientIP() = %q, want %q", ip, tt.expected)
			}
		})
	}
}

func TestGetClientIP_XRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Real-IP", "  203.0.113.100  ")
	req.RemoteAddr = "127.0.0.1:12345"

	ip := getClientIP(req)
	if ip != "203.0.113.100" {
		t.Errorf("getClientIP() = %q, want %q", ip, "203.0.113.100")
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		expected   string
	}{
		{
			name:       "with port",
			remoteAddr: "192.168.1.100:54321",
			expected:   "192.168.1.100",
		},
		{
			name:       "IPv6 with port",
			remoteAddr: "[::1]:54321",
			expected:   "::1",
		},
		{
			name:       "without port",
			remoteAddr: "192.168.1.100",
			expected:   "192.168.1.100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.RemoteAddr = tt.remoteAddr

			ip := getClientIP(req)
			if ip != tt.expected {
				t.Errorf("getClientIP() = %q, want %q", ip, tt.expected)
			}
		})
	}
}

func TestGetClientIP_Priority(t *testing.T) {
	// X-Forwarded-For should take priority over X-Real-IP
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Real-IP", "10.0.0.2")
	req.RemoteAddr = "10.0.0.3:12345"

	ip := getClientIP(req)
	if ip != "10.0.0.1" {
		t.Errorf("getClientIP() = %q, want %q (X-Forwarded-For should have priority)", ip, "10.0.0.1")
	}

	// X-Real-IP should take priority over RemoteAddr
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.Header.Set("X-Real-IP", "10.0.0.2")
	req2.RemoteAddr = "10.0.0.3:12345"

	ip2 := getClientIP(req2)
	if ip2 != "10.0.0.2" {
		t.Errorf("getClientIP() = %q, want %q (X-Real-IP should have priority)", ip2, "10.0.0.2")
	}
}

func TestGetClientIP_InvalidXForwardedFor(t *testing.T) {
	// Invalid IP in X-Forwarded-For should fall back to RemoteAddr
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "not-an-ip, 10.0.0.1")
	req.RemoteAddr = "192.168.1.1:12345"

	ip := getClientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("getClientIP() = %q, want %q (should fall back to RemoteAddr for invalid IP)", ip, "192.168.1.1")
	}
}

func TestGetClientIP_InvalidXRealIP(t *testing.T) {
	// Invalid IP in X-Real-IP should fall back to RemoteAddr
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Real-IP", "not-an-ip")
	req.RemoteAddr = "192.168.1.1:12345"

	ip := getClientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("getClientIP() = %q, want %q (should fall back to RemoteAddr for invalid IP)", ip, "192.168.1.1")
	}
}

func TestRateLimiter_MaxVisitors(t *testing.T) {
	rl := NewRateLimiter(10, 10)

	// Add maxVisitors entries
	for i := range maxVisitors {
		rl.getVisitor(string(rune('a'+i%26)) + string(rune(i)))
	}

	if rl.visitors.Len() != maxVisitors {
		t.Errorf("visitors count = %d, want %d", rl.visitors.Len(), maxVisitors)
	}

	// Adding one more should trigger eviction (LRU handles this automatically)
	rl.getVisitor("new-visitor")

	if rl.visitors.Len() != maxVisitors {
		t.Errorf("visitors count after eviction = %d, want %d", rl.visitors.Len(), maxVisitors)
	}
}

func TestRateLimiter_GetVisitorReturnsExisting(t *testing.T) {
	rl := NewRateLimiter(10, 10)

	ip := "192.168.1.1"

	// Get limiter first time
	limiter1 := rl.getVisitor(ip)

	// Get limiter second time - should return the same one
	limiter2 := rl.getVisitor(ip)

	if limiter1 != limiter2 {
		t.Error("getVisitor should return the same limiter for the same IP")
	}
}

// TenantRateLimiter tests

func TestTenantRateLimiter_AllowsWithinLimit(t *testing.T) {
	tl := NewTenantRateLimiter(10, 10) // 10 requests per second, burst of 10

	tenant := "manifest1abc123"
	for i := range 10 {
		if !tl.Allow(tenant) {
			t.Errorf("Request %d was blocked, should be allowed", i)
		}
	}
}

func TestTenantRateLimiter_BlocksOverLimit(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1) // 1 request per second, burst of 1

	tenant := "manifest1xyz789"

	// First request should be allowed
	if !tl.Allow(tenant) {
		t.Error("First request was blocked, should be allowed")
	}

	// Second request should be blocked (no tokens left)
	if tl.Allow(tenant) {
		t.Error("Second request was allowed, should be blocked")
	}
}

func TestTenantRateLimiter_PerTenantIsolation(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1)

	tenant1 := "manifest1tenant1"
	tenant2 := "manifest1tenant2"

	// Exhaust limit for tenant1
	tl.Allow(tenant1)

	// tenant2 should still have its own limit
	if !tl.Allow(tenant2) {
		t.Error("Different tenant was blocked, should have separate limit")
	}
}

func TestTenantRateLimiter_GetLimiterReturnsExisting(t *testing.T) {
	tl := NewTenantRateLimiter(10, 10)

	tenant := "manifest1test"

	// Get limiter first time
	limiter1 := tl.getLimiter(tenant)

	// Get limiter second time - should return the same one
	limiter2 := tl.getLimiter(tenant)

	if limiter1 != limiter2 {
		t.Error("getLimiter should return the same limiter for the same tenant")
	}
}

func TestContextWithTenant(t *testing.T) {
	ctx := context.Background()
	tenant := "manifest1test"

	// Initially should return empty
	if got := TenantFromContext(ctx); got != "" {
		t.Errorf("TenantFromContext() = %q, want empty string", got)
	}

	// After adding tenant, should return it
	ctx = ContextWithTenant(ctx, tenant)
	if got := TenantFromContext(ctx); got != tenant {
		t.Errorf("TenantFromContext() = %q, want %q", got, tenant)
	}
}
