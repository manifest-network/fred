package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// testCallbackSecret is a valid secret for testing (>= 32 bytes, ASCII).
const testCallbackSecret = "test-secret-that-is-at-least-32-bytes"

// newTestCallbackAuthenticator creates a CallbackAuthenticator for testing.
// It fails the test if creation fails.
func newTestCallbackAuthenticator(t *testing.T, secret string) *CallbackAuthenticator {
	t.Helper()
	auth, err := NewCallbackAuthenticator(secret)
	require.NoError(t, err)
	return auth
}

func TestNewCallbackAuthenticator_SecretValidation(t *testing.T) {
	tests := []struct {
		name      string
		secret    string
		wantError bool
	}{
		{
			name:      "valid secret - exactly 32 bytes",
			secret:    "12345678901234567890123456789012",
			wantError: false,
		},
		{
			name:      "valid secret - more than 32 bytes",
			secret:    testCallbackSecret,
			wantError: false,
		},
		{
			name:      "invalid secret - 31 bytes",
			secret:    "1234567890123456789012345678901",
			wantError: true,
		},
		{
			name:      "invalid secret - empty",
			secret:    "",
			wantError: true,
		},
		{
			name:      "invalid secret - short",
			secret:    "short",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth, err := NewCallbackAuthenticator(tt.secret)
			if tt.wantError {
				assert.Error(t, err)
				assert.Nil(t, auth)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, auth)
			}
		})
	}
}

func TestCallbackAuthenticator_ComputeSignature(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	signature := auth.ComputeSignature(payload)

	// Should have format "t=<timestamp>,sha256=<hex>"
	assert.True(t, strings.HasPrefix(signature, "t="), "signature should start with 't=', got %q", signature)
	assert.Contains(t, signature, ",sha256=")

	// Different payload should produce different signature
	differentPayload := []byte(`{"lease_uuid":"xyz-789","status":"failed"}`)
	differentSig := auth.ComputeSignature(differentPayload)
	assert.NotEqual(t, signature, differentSig)
}

func TestCallbackAuthenticator_ComputeSignatureWithTime(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	fixedTime := time.Unix(1700000000, 0)

	signature := auth.ComputeSignatureWithTime(payload, fixedTime)

	// Should have the fixed timestamp
	assert.True(t, strings.HasPrefix(signature, "t=1700000000,"), "signature should have fixed timestamp, got %q", signature)

	// Signature should be deterministic with same time
	signature2 := auth.ComputeSignatureWithTime(payload, fixedTime)
	assert.Equal(t, signature, signature2)
}

func TestCallbackAuthenticator_VerifySignature(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	now := time.Now()
	validSignature := auth.ComputeSignatureWithTime(payload, now)

	tests := []struct {
		name      string
		payload   []byte
		signature string
		refTime   time.Time
		wantValid bool
	}{
		{
			name:      "valid signature",
			payload:   payload,
			signature: validSignature,
			refTime:   now,
			wantValid: true,
		},
		{
			name:      "valid signature - within max age",
			payload:   payload,
			signature: validSignature,
			refTime:   now.Add(4 * time.Minute), // 4 minutes later, still within 5 min window
			wantValid: true,
		},
		{
			name:      "expired signature - too old",
			payload:   payload,
			signature: validSignature,
			refTime:   now.Add(6 * time.Minute), // 6 minutes later, outside 5 min window
			wantValid: false,
		},
		{
			name:      "future signature - too far ahead",
			payload:   payload,
			signature: auth.ComputeSignatureWithTime(payload, now.Add(2*time.Minute)),
			refTime:   now, // Signature is 2 minutes in the future (> 1 min tolerance)
			wantValid: false,
		},
		{
			name:      "future signature - within clock skew tolerance",
			payload:   payload,
			signature: auth.ComputeSignatureWithTime(payload, now.Add(30*time.Second)),
			refTime:   now, // Signature is 30 seconds in the future (< 1 min tolerance)
			wantValid: true,
		},
		{
			name:      "valid signature - whitespace after comma",
			payload:   payload,
			signature: strings.Replace(validSignature, ",sha256=", ", sha256=", 1),
			refTime:   now,
			wantValid: true,
		},
		{
			name:      "valid signature - whitespace around comma",
			payload:   payload,
			signature: strings.Replace(validSignature, ",sha256=", " , sha256=", 1),
			refTime:   now,
			wantValid: true,
		},
		{
			name:      "invalid signature - wrong hash",
			payload:   payload,
			signature: "t=1700000000,sha256=0000000000000000000000000000000000000000000000000000000000000000",
			refTime:   time.Unix(1700000000, 0),
			wantValid: false,
		},
		{
			name:      "invalid signature - missing timestamp",
			payload:   payload,
			signature: "sha256=abcdef",
			refTime:   now,
			wantValid: false,
		},
		{
			name:      "invalid signature - old format without timestamp",
			payload:   payload,
			signature: "sha256=" + strings.Split(validSignature, "sha256=")[1],
			refTime:   now,
			wantValid: false,
		},
		{
			name:      "invalid signature - not hex",
			payload:   payload,
			signature: "t=1700000000,sha256=not-valid-hex",
			refTime:   time.Unix(1700000000, 0),
			wantValid: false,
		},
		{
			name:      "invalid signature - tampered payload",
			payload:   []byte(`{"lease_uuid":"abc-123","status":"failed"}`),
			signature: validSignature,
			refTime:   now,
			wantValid: false,
		},
		{
			name:      "empty signature",
			payload:   payload,
			signature: "",
			refTime:   now,
			wantValid: false,
		},
		{
			name:      "invalid format - no comma",
			payload:   payload,
			signature: "t=1700000000sha256=abc",
			refTime:   now,
			wantValid: false,
		},
		{
			name:      "invalid format - wrong order",
			payload:   payload,
			signature: "sha256=abc,t=1700000000",
			refTime:   now,
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := auth.VerifySignatureWithTime(tt.payload, tt.signature, tt.refTime)
			assert.Equal(t, tt.wantValid, valid)
		})
	}
}

func TestCallbackAuthenticator_VerifyRequest(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	validSignature := auth.ComputeSignature(payload)

	tests := []struct {
		name       string
		body       []byte
		signature  string
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:      "valid request",
			body:      payload,
			signature: validSignature,
			wantErr:   false,
		},
		{
			name:       "missing signature header",
			body:       payload,
			signature:  "",
			wantErr:    true,
			wantErrMsg: "missing",
		},
		{
			name:       "invalid signature - bad encoding",
			body:       payload,
			signature:  fmt.Sprintf("t=%d,sha256=not-valid-hex!", time.Now().Unix()),
			wantErr:    true,
			wantErrMsg: "invalid signature encoding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", bytes.NewReader(tt.body))
			if tt.signature != "" {
				req.Header.Set(CallbackSignatureHeader, tt.signature)
			}

			body, err := auth.VerifyRequest(req)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.body, body)
		})
	}
}

func TestCallbackAuthenticator_DifferentSecrets(t *testing.T) {
	auth1 := newTestCallbackAuthenticator(t, "secret-one-that-is-at-least-32-chars")
	auth2 := newTestCallbackAuthenticator(t, "secret-two-that-is-at-least-32-chars")

	payload := []byte(`{"test":"data"}`)
	now := time.Now()

	sig1 := auth1.ComputeSignatureWithTime(payload, now)
	sig2 := auth2.ComputeSignatureWithTime(payload, now)

	// Different secrets should produce different signatures
	assert.NotEqual(t, sig1, sig2)

	// Each authenticator should only verify its own signatures
	assert.False(t, auth1.VerifySignatureWithTime(payload, sig2, now))
	assert.False(t, auth2.VerifySignatureWithTime(payload, sig1, now))
}

func TestCallbackAuthenticator_ReplayProtection(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)

	// Create a signature at time T
	signedAt := time.Now()
	signature := auth.ComputeSignatureWithTime(payload, signedAt)

	// Should be valid immediately
	assert.True(t, auth.VerifySignatureWithTime(payload, signature, signedAt))

	// Should still be valid at T+4 minutes
	assert.True(t, auth.VerifySignatureWithTime(payload, signature, signedAt.Add(4*time.Minute)))

	// Should be invalid at T+6 minutes (replay attack)
	assert.False(t, auth.VerifySignatureWithTime(payload, signature, signedAt.Add(6*time.Minute)))

	// Should be invalid at T+1 hour (definitely expired)
	assert.False(t, auth.VerifySignatureWithTime(payload, signature, signedAt.Add(time.Hour)))
}

func TestNewCallbackAuthenticatorWithMaxAge_Validation(t *testing.T) {
	tests := []struct {
		name    string
		secret  string
		maxAge  time.Duration
		wantErr bool
	}{
		{"valid", testCallbackSecret, time.Minute, false},
		{"valid - 1 second", testCallbackSecret, time.Second, false},
		{"valid - 30 minutes", testCallbackSecret, 30 * time.Minute, false},
		{"valid - exactly 1 hour", testCallbackSecret, time.Hour, false},
		{"invalid - zero maxAge", testCallbackSecret, 0, true},
		{"invalid - negative maxAge", testCallbackSecret, -time.Minute, true},
		{"invalid - exceeds max maxAge", testCallbackSecret, 2 * time.Hour, true},
		{"invalid - short secret", "short", time.Minute, true},
		{"invalid - empty secret", "", time.Minute, true},
		{"invalid - 31 byte secret", "1234567890123456789012345678901", time.Minute, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth, err := NewCallbackAuthenticatorWithMaxAge(tt.secret, tt.maxAge)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, auth)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, auth)
			}
		})
	}
}

func TestCallbackAuthenticator_CustomMaxAge(t *testing.T) {
	// Create authenticator with 1 minute max age
	auth, err := NewCallbackAuthenticatorWithMaxAge("test-secret-that-is-at-least-32-chars", time.Minute)
	require.NoError(t, err)

	payload := []byte(`{"test":"data"}`)
	signedAt := time.Now()
	signature := auth.ComputeSignatureWithTime(payload, signedAt)

	// Should be valid within 1 minute
	assert.True(t, auth.VerifySignatureWithTime(payload, signature, signedAt.Add(30*time.Second)))

	// Should be invalid after 1 minute
	assert.False(t, auth.VerifySignatureWithTime(payload, signature, signedAt.Add(2*time.Minute)))
}

func TestHandleProvisionCallback_Authentication(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	publishedCallback := &mockCallbackPublisher{}

	// Create server with callback auth
	server := &Server{
		callbackPublisher:     publishedCallback,
		callbackAuthenticator: auth,
	}

	tests := []struct {
		name           string
		body           string
		addSignature   bool
		wantStatusCode int
	}{
		{
			name:           "valid signed request",
			body:           `{"lease_uuid":"01234567-89ab-cdef-0123-456789abcdef","status":"success"}`,
			addSignature:   true,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing signature",
			body:           `{"lease_uuid":"01234567-89ab-cdef-0123-456789abcdef","status":"success"}`,
			addSignature:   false,
			wantStatusCode: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publishedCallback.reset()

			req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")

			if tt.addSignature {
				signature := auth.ComputeSignature([]byte(tt.body))
				req.Header.Set(CallbackSignatureHeader, signature)
			}

			// Need to reset body for handler since we read it for signature
			req.Body = io.NopCloser(strings.NewReader(tt.body))

			rr := httptest.NewRecorder()
			server.handleProvisionCallback(rr, req)

			assert.Equal(t, tt.wantStatusCode, rr.Code, "body: %s", rr.Body.String())

			// Verify callback was only published for successful auth
			if tt.wantStatusCode == http.StatusOK {
				assert.True(t, publishedCallback.called)
			} else {
				assert.False(t, publishedCallback.called)
			}
		})
	}
}

func TestHandleProvisionCallback_ReplayAttack(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars"

	// Use injectable time for deterministic testing (no time.Sleep needed)
	currentTime := time.Now()
	auth, err := NewCallbackAuthenticatorWithMaxAge(secret, time.Minute)
	require.NoError(t, err)
	auth.nowFunc = func() time.Time { return currentTime }

	publishedCallback := &mockCallbackPublisher{}
	server := &Server{
		callbackPublisher:     publishedCallback,
		callbackAuthenticator: auth,
	}

	body := `{"lease_uuid":"01234567-89ab-cdef-0123-456789abcdef","status":"success"}`

	// Create a signature at the current time
	signature := auth.ComputeSignatureWithTime([]byte(body), currentTime)

	// First request should succeed (time hasn't advanced)
	req1 := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set(CallbackSignatureHeader, signature)
	rr1 := httptest.NewRecorder()
	server.handleProvisionCallback(rr1, req1)

	assert.Equal(t, http.StatusOK, rr1.Code, "first request should succeed: %s", rr1.Body.String())

	// Advance time past the max age (deterministic, no sleep)
	currentTime = currentTime.Add(2 * time.Minute)

	// Replay attempt should fail (signature is now expired)
	publishedCallback.reset()
	req2 := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(CallbackSignatureHeader, signature)
	rr2 := httptest.NewRecorder()
	server.handleProvisionCallback(rr2, req2)

	assert.Equal(t, http.StatusUnauthorized, rr2.Code, "replay attack should be rejected: %s", rr2.Body.String())
}

// TestHandleProvisionCallback_IdempotencyResponse tests that duplicate callbacks
// for already-processed leases return a helpful response body.
func TestHandleProvisionCallback_IdempotencyResponse(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	publishedCallback := &mockCallbackPublisher{}

	// Create status checker that says the lease is NOT in-flight (already processed)
	statusChecker := &mockIdempotencyStatusChecker{
		isInFlight: map[string]bool{
			"01234567-89ab-cdef-0123-456789abcdef": false, // Already processed
			"11111111-1111-1111-1111-111111111111": true,  // Still in-flight
		},
	}

	server := &Server{
		callbackPublisher:     publishedCallback,
		callbackAuthenticator: auth,
		statusChecker:         statusChecker,
	}

	t.Run("already_processed_returns_helpful_body", func(t *testing.T) {
		publishedCallback.reset()

		body := `{"lease_uuid":"01234567-89ab-cdef-0123-456789abcdef","status":"success"}`
		signature := auth.ComputeSignature([]byte(body))

		req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(CallbackSignatureHeader, signature)

		rr := httptest.NewRecorder()
		server.handleProvisionCallback(rr, req)

		// Should still return 200 OK for idempotency
		assert.Equal(t, http.StatusOK, rr.Code)

		// But now with a helpful body
		var response CallbackResponse
		err := json.NewDecoder(rr.Body).Decode(&response)
		require.NoError(t, err, "body: %s", rr.Body.String())

		assert.Equal(t, "already_processed", response.Status)
		assert.Equal(t, "callback for this lease was already handled", response.Message)

		// Callback should NOT be published for already-processed leases
		assert.False(t, publishedCallback.called)
	})

	t.Run("in_flight_lease_is_published", func(t *testing.T) {
		publishedCallback.reset()

		body := `{"lease_uuid":"11111111-1111-1111-1111-111111111111","status":"success"}`
		signature := auth.ComputeSignature([]byte(body))

		req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(CallbackSignatureHeader, signature)

		rr := httptest.NewRecorder()
		server.handleProvisionCallback(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		// Callback should be published for in-flight leases
		assert.True(t, publishedCallback.called)
	})

	t.Run("no_status_checker_publishes_all", func(t *testing.T) {
		// Server without status checker should publish all callbacks
		serverNoChecker := &Server{
			callbackPublisher:     publishedCallback,
			callbackAuthenticator: auth,
			statusChecker:         nil,
		}

		publishedCallback.reset()

		body := `{"lease_uuid":"01234567-89ab-cdef-0123-456789abcdef","status":"success"}`
		signature := auth.ComputeSignature([]byte(body))

		req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(CallbackSignatureHeader, signature)

		rr := httptest.NewRecorder()
		serverNoChecker.handleProvisionCallback(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		// Without status checker, callback should be published
		assert.True(t, publishedCallback.called)
	})
}

// mockIdempotencyStatusChecker implements StatusChecker for idempotency testing.
type mockIdempotencyStatusChecker struct {
	isInFlight map[string]bool
}

func (m *mockIdempotencyStatusChecker) HasPayload(leaseUUID string) (bool, error) {
	return false, nil
}

func (m *mockIdempotencyStatusChecker) IsInFlight(leaseUUID string) bool {
	if m.isInFlight == nil {
		return true // Default to in-flight
	}
	return m.isInFlight[leaseUUID]
}

// TestCallbackAuthenticator_VerifySignature_Standalone tests the VerifySignature method
// that uses the injected time function (now()).
func TestCallbackAuthenticator_VerifySignature_Standalone(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)

	t.Run("uses_injected_time", func(t *testing.T) {
		// Inject a fixed time
		fixedTime := time.Unix(1700000000, 0)
		auth.nowFunc = func() time.Time { return fixedTime }

		// Create signature at that time
		signature := auth.ComputeSignatureWithTime(payload, fixedTime)

		// VerifySignature should use the injected time and succeed
		assert.True(t, auth.VerifySignature(payload, signature))
	})

	t.Run("rejects_expired_signature", func(t *testing.T) {
		// Create signature at old time
		oldTime := time.Unix(1600000000, 0)
		signature := auth.ComputeSignatureWithTime(payload, oldTime)

		// Set current time to much later
		auth.nowFunc = func() time.Time { return time.Unix(1700000000, 0) }

		// Should reject - signature is too old
		assert.False(t, auth.VerifySignature(payload, signature))
	})

	t.Run("accepts_valid_current_signature", func(t *testing.T) {
		// Reset to real time
		auth.nowFunc = nil

		// Create a fresh signature
		signature := auth.ComputeSignature(payload)

		// Should accept
		assert.True(t, auth.VerifySignature(payload, signature))
	})
}

func TestParseSignature(t *testing.T) {
	tests := []struct {
		name          string
		signature     string
		wantTimestamp int64
		wantSig       string
		wantOK        bool
	}{
		{
			name:          "valid signature",
			signature:     "t=1700000000,sha256=abcdef123456",
			wantTimestamp: 1700000000,
			wantSig:       "abcdef123456",
			wantOK:        true,
		},
		{
			name:      "missing timestamp prefix",
			signature: "1700000000,sha256=abcdef",
			wantOK:    false,
		},
		{
			name:      "missing sha256 prefix",
			signature: "t=1700000000,abcdef",
			wantOK:    false,
		},
		{
			name:      "no comma separator",
			signature: "t=1700000000sha256=abcdef",
			wantOK:    false,
		},
		{
			name:      "empty string",
			signature: "",
			wantOK:    false,
		},
		{
			name:      "invalid timestamp",
			signature: "t=notanumber,sha256=abcdef",
			wantOK:    false,
		},
		{
			name:          "extra data after signature",
			signature:     "t=1700000000,sha256=abc,extra=data",
			wantTimestamp: 1700000000,
			wantSig:       "abc,extra=data", // Cut only splits at first comma; hex decode will fail later
			wantOK:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timestamp, sig, ok := hmacauth.ParseSignature(tt.signature)
			assert.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantTimestamp, timestamp)
			assert.Equal(t, tt.wantSig, sig)
		})
	}
}

// mockCallbackPublisher is a mock implementation of CallbackPublisher for testing.
type mockCallbackPublisher struct {
	called bool
}

func (m *mockCallbackPublisher) PublishCallback(callback backend.CallbackPayload) error {
	m.called = true
	return nil
}

func (m *mockCallbackPublisher) reset() {
	m.called = false
}

// Example showing the signature format
func ExampleCallbackAuthenticator_ComputeSignature() {
	auth, err := NewCallbackAuthenticator("my-secret-key-at-least-32-characters")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	signature := auth.ComputeSignature(payload)
	// Output format: t=<unix-timestamp>,sha256=<hex-encoded-hmac>
	fmt.Println("Signature format:", strings.Split(signature, ",")[0][:2]) // "t="
	// Output: Signature format: t=
}
