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
	tl := NewTenantRateLimiter(10, 10, "manifest") // 10 requests per second, burst of 10

	tenant := "manifest1abc123"
	for i := range 10 {
		assert.True(t, tl.Allow(tenant), "Request %d was blocked, should be allowed", i)
	}
}

func TestTenantRateLimiter_BlocksOverLimit(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1, "manifest") // 1 request per second, burst of 1

	tenant := "manifest1xyz789"

	// First request should be allowed
	assert.True(t, tl.Allow(tenant), "First request was blocked, should be allowed")

	// Second request should be blocked (no tokens left)
	assert.False(t, tl.Allow(tenant), "Second request was allowed, should be blocked")
}

func TestTenantRateLimiter_PerTenantIsolation(t *testing.T) {
	tl := NewTenantRateLimiter(1, 1, "manifest")

	tenant1 := "manifest1tenant1"
	tenant2 := "manifest1tenant2"

	// Exhaust limit for tenant1
	tl.Allow(tenant1)

	// tenant2 should still have its own limit
	assert.True(t, tl.Allow(tenant2), "Different tenant was blocked, should have separate limit")
}

func TestTenantRateLimiter_GetLimiterReturnsExisting(t *testing.T) {
	tl := NewTenantRateLimiter(10, 10, "manifest")

	tenant := "manifest1test"

	// Get limiter first time
	limiter1 := tl.getLimiter(tenant)

	// Get limiter second time - should return the same one
	limiter2 := tl.getLimiter(tenant)

	assert.Equal(t, limiter1, limiter2, "getLimiter should return the same limiter for the same tenant")
}

func TestTenantRateLimiter_AuthMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("rate_limits_per_tenant", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest") // 1 request per second, burst of 1
		middleware := tl.AuthMiddleware()(handler)

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
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.AuthMiddleware()(handler)

		// Create tokens for two different tenants
		kp1 := testutil.NewTestKeyPair("tenant-1")
		kp2 := testutil.NewTestKeyPair("tenant-2")
		token1 := testutil.CreateTestToken(kp1, testutil.ValidUUID1, time.Now())
		token2 := testutil.CreateTestToken(kp2, testutil.ValidUUID1, time.Now())

		// Exhaust limit for tenant1
		req1 := httptest.NewRequest("GET", "/test", nil)
		req1.Header.Set("Authorization", "Bearer "+token1)
		rec1 := httptest.NewRecorder()
		middleware.ServeHTTP(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code)

		// Tenant2 should still be allowed
		req2 := httptest.NewRequest("GET", "/test", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		rec2 := httptest.NewRecorder()
		middleware.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code, "should have separate limit")
	})

	t.Run("sets_token_in_context", func(t *testing.T) {
		tl := NewTenantRateLimiter(10, 10, "manifest")

		var capturedToken *AuthToken
		contextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedToken = AuthTokenFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		})
		middleware := tl.AuthMiddleware()(contextHandler)

		kp := testutil.NewTestKeyPair("context-test")
		token := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		require.NotNil(t, capturedToken, "AuthToken should be in context")
		assert.Equal(t, kp.Address, capturedToken.Tenant)
		assert.Equal(t, testutil.ValidUUID1, capturedToken.LeaseUUID)
	})

	t.Run("no_auth_header_returns_401", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.AuthMiddleware()(handler)

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("invalid_token_returns_401", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.AuthMiddleware()(handler)

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer invalid-token-data")
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("expired_token_returns_401_without_consuming_bucket", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.AuthMiddleware()(handler)

		kp := testutil.NewTestKeyPair("expired-test")
		token := testutil.CreateExpiredToken(kp, testutil.ValidUUID1)

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		// Bucket should NOT have been consumed — a valid token should still pass
		validToken := testutil.CreateTestToken(kp, testutil.ValidUUID1, time.Now())
		req2 := httptest.NewRequest("GET", "/test", nil)
		req2.Header.Set("Authorization", "Bearer "+validToken)
		rec2 := httptest.NewRecorder()
		middleware.ServeHTTP(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code, "bucket should not have been consumed by expired token")
	})
}

func TestTenantRateLimiter_PayloadAuthMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("valid_payload_token_passes", func(t *testing.T) {
		tl := NewTenantRateLimiter(10, 10, "manifest")
		middleware := tl.PayloadAuthMiddleware()(handler)

		kp := testutil.NewTestKeyPair("payload-test")
		metaHash := testutil.ComputePayloadHash([]byte("test-payload"))
		token := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())

		req := httptest.NewRequest("POST", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("sets_payload_token_in_context", func(t *testing.T) {
		tl := NewTenantRateLimiter(10, 10, "manifest")

		var capturedToken *PayloadAuthToken
		contextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedToken = PayloadAuthTokenFromContext(r.Context())
			w.WriteHeader(http.StatusOK)
		})
		middleware := tl.PayloadAuthMiddleware()(contextHandler)

		kp := testutil.NewTestKeyPair("payload-ctx")
		metaHash := testutil.ComputePayloadHash([]byte("test-payload"))
		token := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())

		req := httptest.NewRequest("POST", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		require.NotNil(t, capturedToken, "PayloadAuthToken should be in context")
		assert.Equal(t, kp.Address, capturedToken.Tenant)
		assert.Equal(t, testutil.ValidUUID1, capturedToken.LeaseUUID)
		assert.Equal(t, metaHash, capturedToken.MetaHash)
	})

	t.Run("invalid_payload_token_returns_401", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.PayloadAuthMiddleware()(handler)

		req := httptest.NewRequest("POST", "/test", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		tl := NewTenantRateLimiter(1, 1, "manifest")
		middleware := tl.PayloadAuthMiddleware()(handler)

		req := httptest.NewRequest("POST", "/test", nil)
		rec := httptest.NewRecorder()
		middleware.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

func TestAuthTokenFromContext(t *testing.T) {
	t.Run("returns_nil_when_absent", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, AuthTokenFromContext(ctx))
	})

	t.Run("returns_token_when_present", func(t *testing.T) {
		token := &AuthToken{
			Tenant:    "manifest1test",
			LeaseUUID: testutil.ValidUUID1,
		}
		ctx := context.WithValue(context.Background(), authTokenKey{}, token)
		got := AuthTokenFromContext(ctx)
		require.NotNil(t, got)
		assert.Equal(t, token.Tenant, got.Tenant)
		assert.Equal(t, token.LeaseUUID, got.LeaseUUID)
	})
}

func TestPayloadAuthTokenFromContext(t *testing.T) {
	t.Run("returns_nil_when_absent", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, PayloadAuthTokenFromContext(ctx))
	})

	t.Run("returns_token_when_present", func(t *testing.T) {
		token := &PayloadAuthToken{
			Tenant:    "manifest1test",
			LeaseUUID: testutil.ValidUUID1,
			MetaHash:  "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
		}
		ctx := context.WithValue(context.Background(), payloadAuthTokenKey{}, token)
		got := PayloadAuthTokenFromContext(ctx)
		require.NotNil(t, got)
		assert.Equal(t, token.Tenant, got.Tenant)
		assert.Equal(t, token.MetaHash, got.MetaHash)
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
			tl := NewTenantRateLimiter(tt.rate, 1, "manifest")
			got = tl.retryAfterSeconds()
			assert.Equal(t, tt.want, got)
		})
	}
}
