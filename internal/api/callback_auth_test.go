package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/manifest-network/fred/internal/backend"
)

func TestCallbackAuthenticator_ComputeSignature(t *testing.T) {
	auth := NewCallbackAuthenticator("test-secret-that-is-at-least-32-chars")

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	signature := auth.ComputeSignature(payload)

	if !strings.HasPrefix(signature, "sha256=") {
		t.Errorf("signature should start with 'sha256=', got %q", signature)
	}

	// Signature should be deterministic
	signature2 := auth.ComputeSignature(payload)
	if signature != signature2 {
		t.Errorf("signature should be deterministic: %q != %q", signature, signature2)
	}

	// Different payload should produce different signature
	differentPayload := []byte(`{"lease_uuid":"xyz-789","status":"failed"}`)
	differentSig := auth.ComputeSignature(differentPayload)
	if signature == differentSig {
		t.Error("different payloads should produce different signatures")
	}
}

func TestCallbackAuthenticator_VerifySignature(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars"
	auth := NewCallbackAuthenticator(secret)

	payload := []byte(`{"lease_uuid":"abc-123","status":"success"}`)
	validSignature := auth.ComputeSignature(payload)

	tests := []struct {
		name      string
		payload   []byte
		signature string
		wantValid bool
	}{
		{
			name:      "valid signature",
			payload:   payload,
			signature: validSignature,
			wantValid: true,
		},
		{
			name:      "invalid signature - wrong hash",
			payload:   payload,
			signature: "sha256=0000000000000000000000000000000000000000000000000000000000000000",
			wantValid: false,
		},
		{
			name:      "invalid signature - missing prefix",
			payload:   payload,
			signature: strings.TrimPrefix(validSignature, "sha256="),
			wantValid: false,
		},
		{
			name:      "invalid signature - wrong prefix",
			payload:   payload,
			signature: "sha512=" + strings.TrimPrefix(validSignature, "sha256="),
			wantValid: false,
		},
		{
			name:      "invalid signature - not hex",
			payload:   payload,
			signature: "sha256=not-valid-hex",
			wantValid: false,
		},
		{
			name:      "invalid signature - tampered payload",
			payload:   []byte(`{"lease_uuid":"abc-123","status":"failed"}`),
			signature: validSignature,
			wantValid: false,
		},
		{
			name:      "empty signature",
			payload:   payload,
			signature: "",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := auth.VerifySignature(tt.payload, tt.signature)
			if valid != tt.wantValid {
				t.Errorf("VerifySignature() = %v, want %v", valid, tt.wantValid)
			}
		})
	}
}

func TestCallbackAuthenticator_VerifyRequest(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars"
	auth := NewCallbackAuthenticator(secret)

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
			name:       "invalid signature",
			body:       payload,
			signature:  "sha256=invalid",
			wantErr:    true,
			wantErrMsg: "invalid signature",
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
	auth1 := NewCallbackAuthenticator("secret-one-that-is-at-least-32-chars")
	auth2 := NewCallbackAuthenticator("secret-two-that-is-at-least-32-chars")

	payload := []byte(`{"test":"data"}`)

	sig1 := auth1.ComputeSignature(payload)
	sig2 := auth2.ComputeSignature(payload)

	// Different secrets should produce different signatures
	if sig1 == sig2 {
		t.Error("different secrets should produce different signatures")
	}

	// Each authenticator should only verify its own signatures
	if auth1.VerifySignature(payload, sig2) {
		t.Error("auth1 should not verify sig2")
	}
	if auth2.VerifySignature(payload, sig1) {
		t.Error("auth2 should not verify sig1")
	}
}

func TestHandleProvisionCallback_Authentication(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars"
	auth := NewCallbackAuthenticator(secret)

	// Create a mock callback publisher
	var publishedCallback *mockCallbackPublisher
	publishedCallback = &mockCallbackPublisher{}

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
			body:           `{"lease_uuid":"abc-123","status":"success"}`,
			addSignature:   true,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "missing signature",
			body:           `{"lease_uuid":"abc-123","status":"success"}`,
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
