package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil) // 10 requests per second, burst of 10

	// Should allow requests within limit
	for i := range 10 {
		limiter := rl.getVisitor("192.168.1.1")
		if !limiter.Allow() {
			t.Errorf("Request %d was blocked, should be allowed", i)
		}
	}
}

func TestRateLimiter_BlocksOverLimit(t *testing.T) {
	rl := NewRateLimiter(1, 1, nil) // 1 request per second, burst of 1

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
	rl := NewRateLimiter(1, 5, nil) // 1 request per second, burst of 5

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
	rl := NewRateLimiter(1, 1, nil)

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
	rl := NewRateLimiter(1, 1, nil)

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

func TestGetClientIP_WithTrustedProxy(t *testing.T) {
	// Create rate limiter with 127.0.0.1 as trusted proxy
	trustedProxies := NewTrustedProxyConfig([]string{"127.0.0.1"})
	rl := NewRateLimiter(10, 10, trustedProxies)

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
			req.RemoteAddr = "127.0.0.1:12345" // Trusted proxy

			ip := rl.getClientIP(req)
			if ip != tt.expected {
				t.Errorf("getClientIP() = %q, want %q", ip, tt.expected)
			}
		})
	}
}

func TestGetClientIP_WithoutTrustedProxy_IgnoresHeaders(t *testing.T) {
	// Without trusted proxies, X-Forwarded-For should be ignored
	rl := NewRateLimiter(10, 10, nil)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.195")
	req.Header.Set("X-Real-IP", "203.0.113.100")
	req.RemoteAddr = "192.168.1.1:12345"

	ip := rl.getClientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("getClientIP() = %q, want %q (should ignore headers without trusted proxy)", ip, "192.168.1.1")
	}
}

func TestGetClientIP_UntrustedSource_IgnoresHeaders(t *testing.T) {
	// With trusted proxies configured, but request from untrusted source
	trustedProxies := NewTrustedProxyConfig([]string{"10.0.0.0/8"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.195")
	req.RemoteAddr = "192.168.1.1:12345" // Not in 10.0.0.0/8

	ip := rl.getClientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("getClientIP() = %q, want %q (should ignore headers from untrusted source)", ip, "192.168.1.1")
	}
}

func TestGetClientIP_XRealIP_WithTrustedProxy(t *testing.T) {
	trustedProxies := NewTrustedProxyConfig([]string{"127.0.0.1"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Real-IP", "  203.0.113.100  ")
	req.RemoteAddr = "127.0.0.1:12345"

	ip := rl.getClientIP(req)
	if ip != "203.0.113.100" {
		t.Errorf("getClientIP() = %q, want %q", ip, "203.0.113.100")
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil)

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

			ip := rl.getClientIP(req)
			if ip != tt.expected {
				t.Errorf("getClientIP() = %q, want %q", ip, tt.expected)
			}
		})
	}
}

func TestGetClientIP_Priority_WithTrustedProxy(t *testing.T) {
	trustedProxies := NewTrustedProxyConfig([]string{"10.0.0.3"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	// X-Forwarded-For should take priority over X-Real-IP
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	req.Header.Set("X-Real-IP", "10.0.0.2")
	req.RemoteAddr = "10.0.0.3:12345" // Trusted proxy

	ip := rl.getClientIP(req)
	if ip != "10.0.0.1" {
		t.Errorf("getClientIP() = %q, want %q (X-Forwarded-For should have priority)", ip, "10.0.0.1")
	}

	// X-Real-IP should take priority over RemoteAddr when no XFF
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.Header.Set("X-Real-IP", "10.0.0.2")
	req2.RemoteAddr = "10.0.0.3:12345"

	ip2 := rl.getClientIP(req2)
	if ip2 != "10.0.0.2" {
		t.Errorf("getClientIP() = %q, want %q (X-Real-IP should have priority)", ip2, "10.0.0.2")
	}
}

func TestGetClientIP_InvalidXForwardedFor_WithTrustedProxy(t *testing.T) {
	trustedProxies := NewTrustedProxyConfig([]string{"192.168.1.1"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	// Invalid IP in X-Forwarded-For should fall back to RemoteAddr
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "not-an-ip, 10.0.0.1")
	req.RemoteAddr = "192.168.1.1:12345"

	ip := rl.getClientIP(req)
	if ip != "192.168.1.1" {
		t.Errorf("getClientIP() = %q, want %q (should fall back to RemoteAddr for invalid IP)", ip, "192.168.1.1")
	}
}

func TestTrustedProxyConfig(t *testing.T) {
	t.Run("parses CIDR blocks", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"10.0.0.0/8", "192.168.1.0/24"})

		if !config.IsTrusted("10.0.0.1") {
			t.Error("10.0.0.1 should be trusted")
		}
		if !config.IsTrusted("10.255.255.255") {
			t.Error("10.255.255.255 should be trusted")
		}
		if !config.IsTrusted("192.168.1.100") {
			t.Error("192.168.1.100 should be trusted")
		}
		if config.IsTrusted("192.168.2.1") {
			t.Error("192.168.2.1 should not be trusted")
		}
	})

	t.Run("parses bare IPs", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"127.0.0.1", "::1"})

		if !config.IsTrusted("127.0.0.1") {
			t.Error("127.0.0.1 should be trusted")
		}
		if !config.IsTrusted("::1") {
			t.Error("::1 should be trusted")
		}
		if config.IsTrusted("127.0.0.2") {
			t.Error("127.0.0.2 should not be trusted")
		}
	})

	t.Run("handles invalid CIDR gracefully", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"not-valid", "10.0.0.0/8"})

		// Should still work with valid entry
		if !config.IsTrusted("10.0.0.1") {
			t.Error("10.0.0.1 should be trusted")
		}
	})

	t.Run("nil config returns false", func(t *testing.T) {
		var config *TrustedProxyConfig
		if config.IsTrusted("127.0.0.1") {
			t.Error("nil config should not trust any IP")
		}
	})

	t.Run("empty config returns false", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{})
		if config.IsTrusted("127.0.0.1") {
			t.Error("empty config should not trust any IP")
		}
	})
}

func TestRateLimiter_MaxVisitors(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil)

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
	rl := NewRateLimiter(10, 10, nil)

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

func TestCalcRetryAfterSeconds(t *testing.T) {
	// Note: Only testing realistic values that could come from config validation.
	// Config validation ensures rate > 0, and viper/YAML parsing cannot produce
	// NaN/Inf, so those edge cases are not tested here.
	tests := []struct {
		name string
		rate float64
		want string
	}{
		{"10 RPS", 10.0, "1"},
		{"1 RPS", 1.0, "1"},
		{"0.5 RPS (2 second refill)", 0.5, "2"},
		{"0.1 RPS (10 second refill)", 0.1, "10"},
		{"0.33 RPS (rounds up to 4)", 0.33, "4"},
		{"zero rate fallback", 0, "1"},
		{"negative rate fallback", -1, "1"},
		{"100 RPS", 100.0, "1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.rate, 1, nil)
			got := rl.retryAfterSeconds()
			if got != tt.want {
				t.Errorf("retryAfterSeconds() = %q, want %q", got, tt.want)
			}

			// Also test TenantRateLimiter
			tl := NewTenantRateLimiter(tt.rate, 1)
			got = tl.retryAfterSeconds()
			if got != tt.want {
				t.Errorf("TenantRateLimiter.retryAfterSeconds() = %q, want %q", got, tt.want)
			}
		})
	}
}

