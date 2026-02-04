package api

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil) // 10 requests per second, burst of 10

	// Should allow requests within limit
	for i := range 10 {
		limiter := rl.getVisitor("192.168.1.1")
		assert.True(t, limiter.Allow(), "Request %d was blocked, should be allowed", i)
	}
}

func TestRateLimiter_BlocksOverLimit(t *testing.T) {
	rl := NewRateLimiter(1, 1, nil) // 1 request per second, burst of 1

	ip := "192.168.1.2"
	limiter := rl.getVisitor(ip)

	// First request should be allowed
	assert.True(t, limiter.Allow(), "First request was blocked, should be allowed")

	// Second request should be blocked (no tokens left)
	assert.False(t, limiter.Allow(), "Second request was allowed, should be blocked")
}

func TestRateLimiter_BurstAllowed(t *testing.T) {
	rl := NewRateLimiter(1, 5, nil) // 1 request per second, burst of 5

	ip := "192.168.1.3"
	limiter := rl.getVisitor(ip)

	// All burst requests should be allowed
	for i := range 5 {
		assert.True(t, limiter.Allow(), "Burst request %d was blocked, should be allowed", i)
	}

	// Next request should be blocked
	assert.False(t, limiter.Allow(), "Request after burst was allowed, should be blocked")
}

func TestRateLimiter_PerIPIsolation(t *testing.T) {
	rl := NewRateLimiter(1, 1, nil)

	// Exhaust limit for IP1
	limiter1 := rl.getVisitor("192.168.1.1")
	limiter1.Allow()

	// IP2 should still have its own limit
	limiter2 := rl.getVisitor("192.168.1.2")
	assert.True(t, limiter2.Allow(), "Different IP was blocked, should have separate limit")
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

	assert.Equal(t, http.StatusOK, rec1.Code)

	// Second request should be rate limited
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.RemoteAddr = "192.168.1.1:12345"
	rec2 := httptest.NewRecorder()
	middleware.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusTooManyRequests, rec2.Code)

	// Check Retry-After header
	assert.Equal(t, "1", rec2.Header().Get("Retry-After"))
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
			assert.Equal(t, tt.expected, ip)
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
	assert.Equal(t, "192.168.1.1", ip, "should ignore headers without trusted proxy")
}

func TestGetClientIP_UntrustedSource_IgnoresHeaders(t *testing.T) {
	// With trusted proxies configured, but request from untrusted source
	trustedProxies := NewTrustedProxyConfig([]string{"10.0.0.0/8"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.195")
	req.RemoteAddr = "192.168.1.1:12345" // Not in 10.0.0.0/8

	ip := rl.getClientIP(req)
	assert.Equal(t, "192.168.1.1", ip, "should ignore headers from untrusted source")
}

func TestGetClientIP_XRealIP_WithTrustedProxy(t *testing.T) {
	trustedProxies := NewTrustedProxyConfig([]string{"127.0.0.1"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Real-IP", "  203.0.113.100  ")
	req.RemoteAddr = "127.0.0.1:12345"

	ip := rl.getClientIP(req)
	assert.Equal(t, "203.0.113.100", ip)
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
			assert.Equal(t, tt.expected, ip)
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
	assert.Equal(t, "10.0.0.1", ip, "X-Forwarded-For should have priority")

	// X-Real-IP should take priority over RemoteAddr when no XFF
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.Header.Set("X-Real-IP", "10.0.0.2")
	req2.RemoteAddr = "10.0.0.3:12345"

	ip2 := rl.getClientIP(req2)
	assert.Equal(t, "10.0.0.2", ip2, "X-Real-IP should have priority")
}

func TestGetClientIP_InvalidXForwardedFor_WithTrustedProxy(t *testing.T) {
	trustedProxies := NewTrustedProxyConfig([]string{"192.168.1.1"})
	rl := NewRateLimiter(10, 10, trustedProxies)

	// Invalid IP in X-Forwarded-For should fall back to RemoteAddr
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "not-an-ip, 10.0.0.1")
	req.RemoteAddr = "192.168.1.1:12345"

	ip := rl.getClientIP(req)
	assert.Equal(t, "192.168.1.1", ip, "should fall back to RemoteAddr for invalid IP")
}

func TestTrustedProxyConfig(t *testing.T) {
	t.Run("parses CIDR blocks", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"10.0.0.0/8", "192.168.1.0/24"})

		assert.True(t, config.IsTrusted("10.0.0.1"))
		assert.True(t, config.IsTrusted("10.255.255.255"))
		assert.True(t, config.IsTrusted("192.168.1.100"))
		assert.False(t, config.IsTrusted("192.168.2.1"))
	})

	t.Run("parses bare IPs", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"127.0.0.1", "::1"})

		assert.True(t, config.IsTrusted("127.0.0.1"))
		assert.True(t, config.IsTrusted("::1"))
		assert.False(t, config.IsTrusted("127.0.0.2"))
	})

	t.Run("handles invalid CIDR gracefully", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{"not-valid", "10.0.0.0/8"})

		// Should still work with valid entry
		assert.True(t, config.IsTrusted("10.0.0.1"))
	})

	t.Run("nil config returns false", func(t *testing.T) {
		var config *TrustedProxyConfig
		assert.False(t, config.IsTrusted("127.0.0.1"))
	})

	t.Run("empty config returns false", func(t *testing.T) {
		config := NewTrustedProxyConfig([]string{})
		assert.False(t, config.IsTrusted("127.0.0.1"))
	})
}

func TestRateLimiter_MaxVisitors(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil)

	// Add maxVisitors entries
	for i := range maxVisitors {
		rl.getVisitor(string(rune('a'+i%26)) + string(rune(i)))
	}

	assert.Equal(t, maxVisitors, rl.visitors.Len())

	// Adding one more should trigger eviction (LRU handles this automatically)
	rl.getVisitor("new-visitor")

	assert.Equal(t, maxVisitors, rl.visitors.Len())
}

func TestRateLimiter_GetVisitorReturnsExisting(t *testing.T) {
	rl := NewRateLimiter(10, 10, nil)

	ip := "192.168.1.1"

	// Get limiter first time
	limiter1 := rl.getVisitor(ip)

	// Get limiter second time - should return the same one
	limiter2 := rl.getVisitor(ip)

	assert.Equal(t, limiter1, limiter2, "getVisitor should return the same limiter for the same IP")
}

// TenantRateLimiter tests

func TestTenantRateLimiter_AllowsWithinLimit(t *testing.T) {
	tl := NewTenantRateLimiter(10, 10) // 10 requests per second, burst of 10

	tenant := "manifest1abc123"
	for i := range 10 {
		assert.True(t, tl.Allow(tenant), "Request %d was blocked, should be allowed", i)
	}
}

func TestTenantRateLimiter_BlocksOverLimit(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1) // 1 request per second, burst of 1

	tenant := "manifest1xyz789"

	// First request should be allowed
	assert.True(t, tl.Allow(tenant), "First request was blocked, should be allowed")

	// Second request should be blocked (no tokens left)
	assert.False(t, tl.Allow(tenant), "Second request was allowed, should be blocked")
}

func TestTenantRateLimiter_PerTenantIsolation(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1)

	tenant1 := "manifest1tenant1"
	tenant2 := "manifest1tenant2"

	// Exhaust limit for tenant1
	tl.Allow(tenant1)

	// tenant2 should still have its own limit
	assert.True(t, tl.Allow(tenant2), "Different tenant was blocked, should have separate limit")
}

func TestTenantRateLimiter_GetLimiterReturnsExisting(t *testing.T) {
	tl := NewTenantRateLimiter(10, 10)

	tenant := "manifest1test"

	// Get limiter first time
	limiter1 := tl.getLimiter(tenant)

	// Get limiter second time - should return the same one
	limiter2 := tl.getLimiter(tenant)

	assert.Equal(t, limiter1, limiter2, "getLimiter should return the same limiter for the same tenant")
}

func TestTenantRateLimiter_Middleware(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1) // 1 request per second, burst of 1

	// Create a handler that verifies tenant was set in context
	var capturedTenant string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tenant, ok := r.Context().Value(tenantKey{}).(string); ok {
			capturedTenant = tenant
		}
		w.WriteHeader(http.StatusOK)
	})

	middleware := tl.Middleware("manifest")(handler)

	t.Run("rate_limits_per_tenant", func(t *testing.T) {
		// Create a valid auth token for testing
		kp := testutil.NewTestKeyPair("test-tenant")
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())

		// First request should pass
		req1 := httptest.NewRequest("GET", "/test", nil)
		req1.Header.Set("Authorization", "Bearer "+token)
		rec1 := httptest.NewRecorder()
		middleware.ServeHTTP(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code)

		// Second request with same tenant should be rate limited
		// Need a new token (different timestamp) but same tenant
		token2 := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now().Add(time.Second))
		req2 := httptest.NewRequest("GET", "/test", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		rec2 := httptest.NewRecorder()
		middleware.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusTooManyRequests, rec2.Code)

		// Check Retry-After header
		assert.Equal(t, "1", rec2.Header().Get("Retry-After"))
	})

	t.Run("different_tenants_independent", func(t *testing.T) {
		// Use fresh rate limiter
		tl2 := NewTenantRateLimiter(1, 1)
		middleware2 := tl2.Middleware("manifest")(handler)

		// Create tokens for two different tenants
		kp1 := testutil.NewTestKeyPair("tenant-1")
		kp2 := testutil.NewTestKeyPair("tenant-2")
		token1 := testutil.CreateTestToken(kp1, testutil.ValidUUID1, time.Now())
		token2 := testutil.CreateTestToken(kp2, testutil.ValidUUID1, time.Now())

		// Exhaust limit for tenant1
		req1 := httptest.NewRequest("GET", "/test", nil)
		req1.Header.Set("Authorization", "Bearer "+token1)
		rec1 := httptest.NewRecorder()
		middleware2.ServeHTTP(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code)

		// Tenant2 should still be allowed
		req2 := httptest.NewRequest("GET", "/test", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		rec2 := httptest.NewRecorder()
		middleware2.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code, "should have separate limit")
	})

	t.Run("sets_tenant_in_context", func(t *testing.T) {
		tl3 := NewTenantRateLimiter(10, 10)
		capturedTenant = "" // Reset

		contextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tenant, ok := r.Context().Value(tenantKey{}).(string); ok {
				capturedTenant = tenant
			}
			w.WriteHeader(http.StatusOK)
		})
		middleware3 := tl3.Middleware("manifest")(contextHandler)

		kp := testutil.NewTestKeyPair("context-test")
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		middleware3.ServeHTTP(rec, req)

		assert.Equal(t, kp.Address, capturedTenant)
	})

	t.Run("no_auth_header_proceeds_without_tenant_limiting", func(t *testing.T) {
		tl4 := NewTenantRateLimiter(1, 1)
		middleware4 := tl4.Middleware("manifest")(handler)

		// Request without auth header should proceed (IP limiting still applies separately)
		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		middleware4.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("invalid_token_proceeds_without_tenant_limiting", func(t *testing.T) {
		tl5 := NewTenantRateLimiter(1, 1)
		middleware5 := tl5.Middleware("manifest")(handler)

		// Request with invalid token should proceed (tenant extraction fails gracefully)
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer invalid-token-data")
		rec := httptest.NewRecorder()
		middleware5.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestContextWithTenant(t *testing.T) {
	ctx := context.Background()
	tenant := "manifest1test123"

	newCtx := ContextWithTenant(ctx, tenant)

	// Verify tenant can be retrieved
	got, ok := newCtx.Value(tenantKey{}).(string)
	require.True(t, ok, "tenant not found in context")
	assert.Equal(t, tenant, got)
}

func TestExtractTenantFromAuth(t *testing.T) {
	t.Run("valid_token_returns_tenant", func(t *testing.T) {
		kp := testutil.NewTestKeyPair("extract-test")
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		tenant := extractTenantFromAuth(req, "manifest")
		assert.Equal(t, kp.Address, tenant)
	})

	t.Run("missing_auth_returns_empty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)

		tenant := extractTenantFromAuth(req, "manifest")
		assert.Equal(t, "", tenant)
	})

	t.Run("invalid_token_returns_empty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer not-valid-base64-json")

		tenant := extractTenantFromAuth(req, "manifest")
		assert.Equal(t, "", tenant)
	})

	t.Run("wrong_prefix_returns_empty", func(t *testing.T) {
		kp := testutil.NewTestKeyPair("prefix-test")
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		// Use wrong prefix - validation should fail
		tenant := extractTenantFromAuth(req, "cosmos")
		assert.Equal(t, "", tenant, "wrong prefix")
	})

	t.Run("expired_token_returns_empty", func(t *testing.T) {
		kp := testutil.NewTestKeyPair("expired-test")
		// Create token with old timestamp
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now().Add(-1*time.Hour))

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)

		tenant := extractTenantFromAuth(req, "manifest")
		assert.Equal(t, "", tenant, "expired token")
	})
}

func TestCalcRetryAfterSeconds(t *testing.T) {
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
		// Non-finite edge cases: strconv.ParseFloat("NaN") succeeds and NaN
		// comparisons are always false, so explicit guards are needed.
		{"positive infinity", math.Inf(1), "1"},
		{"negative infinity", math.Inf(-1), "1"},
		{"NaN", math.NaN(), "1"},
		// Extremely small rates would overflow int; cap at 1 day.
		{"tiny rate capped at 1 day", 1e-10, "86400"},
		{"very small rate capped at 1 day", 1e-20, "86400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.rate, 1, nil)
			got := rl.retryAfterSeconds()
			assert.Equal(t, tt.want, got)

			// Also test TenantRateLimiter
			tl := NewTenantRateLimiter(tt.rate, 1)
			got = tl.retryAfterSeconds()
			assert.Equal(t, tt.want, got)
		})
	}
}
