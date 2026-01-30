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

	"github.com/manifest-network/fred/internal/backend"
)

// testCallbackSecret is a valid secret for testing (>= 32 bytes, ASCII).
const testCallbackSecret = "test-secret-that-is-at-least-32-bytes"

// newTestCallbackAuthenticator creates a CallbackAuthenticator for testing.
// It fails the test if creation fails.
func newTestCallbackAuthenticator(t *testing.T, secret string) *CallbackAuthenticator {
	t.Helper()
	auth, err := NewCallbackAuthenticator(secret)
	if err != nil {
		t.Fatalf("NewCallbackAuthenticator() error = %v", err)
	}
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
				if err == nil {
					t.Error("NewCallbackAuthenticator() error = nil, want error")
				}
				if auth != nil {
					t.Error("NewCallbackAuthenticator() returned non-nil auth with error")
				}
			} else {
				if err != nil {
					t.Errorf("NewCallbackAuthenticator() error = %v, want nil", err)
				}
				if auth == nil {
					t.Error("NewCallbackAuthenticator() returned nil auth without error")
				}
			}
		})
	}
}

func TestCallbackAuthenticator_ComputeSignature(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	signature := auth.ComputeSignature(payload)

	// Should have format "t=<timestamp>,sha256=<hex>"
	if !strings.HasPrefix(signature, "t=") {
		t.Errorf("signature should start with 't=', got %q", signature)
	}
	if !strings.Contains(signature, ",sha256=") {
		t.Errorf("signature should contain ',sha256=', got %q", signature)
	}

	// Different payload should produce different signature
	differentPayload := []byte(`{"lease_uuid":"xyz-789","status":"failed"}`)
	differentSig := auth.ComputeSignature(differentPayload)
	if signature == differentSig {
		t.Error("different payloads should produce different signatures")
	}
}

func TestCallbackAuthenticator_ComputeSignatureWithTime(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	fixedTime := time.Unix(1700000000, 0)

	signature := auth.ComputeSignatureWithTime(payload, fixedTime)

	// Should have the fixed timestamp
	if !strings.HasPrefix(signature, "t=1700000000,") {
		t.Errorf("signature should have fixed timestamp, got %q", signature)
	}

	// Signature should be deterministic with same time
	signature2 := auth.ComputeSignatureWithTime(payload, fixedTime)
	if signature != signature2 {
		t.Errorf("signature should be deterministic: %q != %q", signature, signature2)
	}
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
			if valid != tt.wantValid {
				t.Errorf("VerifySignature() = %v, want %v", valid, tt.wantValid)
			}
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
				if err == nil {
					t.Error("VerifyRequest() expected error, got nil")
				} else if tt.wantErrMsg != "" && !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf("VerifyRequest() error = %q, want error containing %q", err.Error(), tt.wantErrMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("VerifyRequest() unexpected error: %v", err)
				return
			}

			if !bytes.Equal(body, tt.body) {
				t.Errorf("VerifyRequest() body = %q, want %q", body, tt.body)
			}
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
	if sig1 == sig2 {
		t.Error("different secrets should produce different signatures")
	}

	// Each authenticator should only verify its own signatures
	if auth1.VerifySignatureWithTime(payload, sig2, now) {
		t.Error("auth1 should not verify sig2")
	}
	if auth2.VerifySignatureWithTime(payload, sig1, now) {
		t.Error("auth2 should not verify sig1")
	}
}

func TestCallbackAuthenticator_ReplayProtection(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)

	// Create a signature at time T
	signedAt := time.Now()
	signature := auth.ComputeSignatureWithTime(payload, signedAt)

	// Should be valid immediately
	if !auth.VerifySignatureWithTime(payload, signature, signedAt) {
		t.Error("signature should be valid immediately")
	}

	// Should still be valid at T+4 minutes
	if !auth.VerifySignatureWithTime(payload, signature, signedAt.Add(4*time.Minute)) {
		t.Error("signature should be valid at T+4 minutes")
	}

	// Should be invalid at T+6 minutes (replay attack)
	if auth.VerifySignatureWithTime(payload, signature, signedAt.Add(6*time.Minute)) {
		t.Error("signature should be invalid at T+6 minutes (replay protection)")
	}

	// Should be invalid at T+1 hour (definitely expired)
	if auth.VerifySignatureWithTime(payload, signature, signedAt.Add(time.Hour)) {
		t.Error("signature should be invalid at T+1 hour")
	}
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
				if err == nil {
					t.Error("expected error, got nil")
				}
				if auth != nil {
					t.Error("expected nil authenticator on error")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if auth == nil {
					t.Error("expected authenticator, got nil")
				}
			}
		})
	}
}

func TestCallbackAuthenticator_CustomMaxAge(t *testing.T) {
	// Create authenticator with 1 minute max age
	auth, err := NewCallbackAuthenticatorWithMaxAge("test-secret-that-is-at-least-32-chars", time.Minute)
	if err != nil {
		t.Fatalf("NewCallbackAuthenticatorWithMaxAge() error = %v", err)
	}

	payload := []byte(`{"test":"data"}`)
	signedAt := time.Now()
	signature := auth.ComputeSignatureWithTime(payload, signedAt)

	// Should be valid within 1 minute
	if !auth.VerifySignatureWithTime(payload, signature, signedAt.Add(30*time.Second)) {
		t.Error("signature should be valid within custom max age")
	}

	// Should be invalid after 1 minute
	if auth.VerifySignatureWithTime(payload, signature, signedAt.Add(2*time.Minute)) {
		t.Error("signature should be invalid after custom max age")
	}
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

			if rr.Code != tt.wantStatusCode {
				t.Errorf("status code = %d, want %d, body: %s", rr.Code, tt.wantStatusCode, rr.Body.String())
			}

			// Verify callback was only published for successful auth
			if tt.wantStatusCode == http.StatusOK && !publishedCallback.called {
				t.Error("callback should have been published")
			}
			if tt.wantStatusCode != http.StatusOK && publishedCallback.called {
				t.Error("callback should not have been published for failed auth")
			}
		})
	}
}

func TestHandleProvisionCallback_ReplayAttack(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars"

	// Use injectable time for deterministic testing (no time.Sleep needed)
	currentTime := time.Now()
	auth, err := NewCallbackAuthenticatorWithMaxAge(secret, time.Minute)
	if err != nil {
		t.Fatalf("NewCallbackAuthenticatorWithMaxAge() error = %v", err)
	}
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

	if rr1.Code != http.StatusOK {
		t.Errorf("first request should succeed, got status %d: %s", rr1.Code, rr1.Body.String())
	}

	// Advance time past the max age (deterministic, no sleep)
	currentTime = currentTime.Add(2 * time.Minute)

	// Replay attempt should fail (signature is now expired)
	publishedCallback.reset()
	req2 := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(CallbackSignatureHeader, signature)
	rr2 := httptest.NewRecorder()
	server.handleProvisionCallback(rr2, req2)

	if rr2.Code != http.StatusUnauthorized {
		t.Errorf("replay attack should be rejected, got status %d: %s", rr2.Code, rr2.Body.String())
	}
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
		if rr.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rr.Code, http.StatusOK)
		}

		// But now with a helpful body
		var response CallbackResponse
		if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v, body: %s", err, rr.Body.String())
		}

		if response.Status != "already_processed" {
			t.Errorf("Status = %q, want %q", response.Status, "already_processed")
		}
		if response.Message != "callback for this lease was already handled" {
			t.Errorf("Message = %q, want %q", response.Message, "callback for this lease was already handled")
		}

		// Callback should NOT be published for already-processed leases
		if publishedCallback.called {
			t.Error("callback should not be published for already-processed lease")
		}
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

		if rr.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rr.Code, http.StatusOK)
		}

		// Callback should be published for in-flight leases
		if !publishedCallback.called {
			t.Error("callback should be published for in-flight lease")
		}
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

		if rr.Code != http.StatusOK {
			t.Errorf("status code = %d, want %d", rr.Code, http.StatusOK)
		}

		// Without status checker, callback should be published
		if !publishedCallback.called {
			t.Error("callback should be published when no status checker")
		}
	})
}

// mockIdempotencyStatusChecker implements StatusChecker for idempotency testing.
type mockIdempotencyStatusChecker struct {
	isInFlight map[string]bool
}

func (m *mockIdempotencyStatusChecker) HasPayload(leaseUUID string) bool {
	return false
}

func (m *mockIdempotencyStatusChecker) IsInFlight(leaseUUID string) bool {
	if m.isInFlight == nil {
		return true // Default to in-flight
	}
	return m.isInFlight[leaseUUID]
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
			timestamp, sig, ok := parseSignature(tt.signature)
			if ok != tt.wantOK {
				t.Errorf("parseSignature() ok = %v, want %v", ok, tt.wantOK)
				return
			}
			if !tt.wantOK {
				return
			}
			if timestamp != tt.wantTimestamp {
				t.Errorf("parseSignature() timestamp = %d, want %d", timestamp, tt.wantTimestamp)
			}
			if sig != tt.wantSig {
				t.Errorf("parseSignature() sig = %q, want %q", sig, tt.wantSig)
			}
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
