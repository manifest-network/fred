package hmacauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSignAndVerify(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)
	const method = "POST"
	const uri = "/provision"

	t.Run("valid signature", func(t *testing.T) {
		sig := Sign(secret, method, uri, body)
		assert.NoError(t, Verify(secret, method, uri, body, sig, 5*time.Minute))
	})

	t.Run("expired timestamp", func(t *testing.T) {
		sig := SignWithTime(secret, method, uri, body, time.Now().Add(-10*time.Minute))
		assert.Error(t, Verify(secret, method, uri, body, sig, 5*time.Minute))
	})

	t.Run("wrong secret", func(t *testing.T) {
		sig := Sign("wrong-secret-wrong-secret-wrong!", method, uri, body)
		assert.Error(t, Verify(secret, method, uri, body, sig, 5*time.Minute))
	})

	t.Run("tampered body", func(t *testing.T) {
		sig := Sign(secret, method, uri, body)
		assert.Error(t, Verify(secret, method, uri, []byte(`{"lease_uuid":"TAMPERED"}`), sig, 5*time.Minute))
	})

	t.Run("malformed signature missing sha256", func(t *testing.T) {
		sig := "t=1234567890"
		assert.Error(t, Verify(secret, method, uri, body, sig, 5*time.Minute))
	})

	t.Run("malformed signature missing timestamp", func(t *testing.T) {
		assert.Error(t, Verify(secret, method, uri, body, "sha256=abc123", 5*time.Minute))
	})

	t.Run("future timestamp rejected", func(t *testing.T) {
		sig := SignWithTime(secret, method, uri, body, time.Now().Add(10*time.Minute))
		err := Verify(secret, method, uri, body, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "future")
	})
}

// TestSignAndVerify_MethodURIBinding asserts the ENG-191 properties: the
// method and URI must match between sign and verify, the body hash is
// computed correctly for empty/nil bodies, and the old "<timestamp>.<body>"
// wire format is rejected.
func TestSignAndVerify_MethodURIBinding(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"

	t.Run("round trip with method and URI", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"x"}`)
		sig := Sign(secret, "POST", "/provision", body)
		assert.NoError(t, Verify(secret, "POST", "/provision", body, sig, 5*time.Minute))
	})

	t.Run("wrong method (POST→DELETE) is rejected", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"x"}`)
		sig := Sign(secret, "POST", "/provision", body)
		err := Verify(secret, "DELETE", "/provision", body, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("cross-endpoint replay (same method, different URI) is rejected", func(t *testing.T) {
		body := []byte(`{"lease_uuid":"x"}`)
		sig := Sign(secret, "POST", "/provision", body)
		err := Verify(secret, "POST", "/deprovision", body, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("cross-resource GET replay is rejected", func(t *testing.T) {
		sig := Sign(secret, "GET", "/info/abc", nil)
		err := Verify(secret, "GET", "/logs/abc", nil, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("wrong query string is rejected", func(t *testing.T) {
		sig := Sign(secret, "GET", "/logs/abc?tail=10", nil)
		err := Verify(secret, "GET", "/logs/abc?tail=100", nil, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("method case sensitivity (POST vs post)", func(t *testing.T) {
		body := []byte(`{}`)
		sig := Sign(secret, "POST", "/provision", body)
		err := Verify(secret, "post", "/provision", body, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})

	t.Run("empty-body GET round trip", func(t *testing.T) {
		sig := Sign(secret, "GET", "/info/x", nil)
		assert.NoError(t, Verify(secret, "GET", "/info/x", nil, sig, 5*time.Minute))
	})

	t.Run("nil vs empty body parity", func(t *testing.T) {
		sig := Sign(secret, "GET", "/info/x", nil)
		assert.NoError(t, Verify(secret, "GET", "/info/x", []byte{}, sig, 5*time.Minute))
	})

	t.Run("binary-safe body", func(t *testing.T) {
		body := []byte{0x00, 0x0A, 0xFF, 0x7F, 0x80}
		sig := Sign(secret, "POST", "/raw", body)
		assert.NoError(t, Verify(secret, "POST", "/raw", body, sig, 5*time.Minute))
	})

	t.Run("old wire format rejected", func(t *testing.T) {
		// Construct a signature using the pre-fix "<timestamp>.<body>" format
		// and verify with the new function — must be rejected.
		body := []byte(`{"lease_uuid":"x"}`)
		ts := time.Now().Unix()
		oldSignedPayload := fmt.Sprintf("%d.%s", ts, body)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(oldSignedPayload))
		oldSig := fmt.Sprintf("t=%d,sha256=%s", ts, hex.EncodeToString(mac.Sum(nil)))

		err := Verify(secret, "POST", "/provision", body, oldSig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})
}

func TestParseSignature(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890,sha256=abcdef")
		assert.True(t, ok)
		assert.Equal(t, int64(1234567890), ts)
		assert.Equal(t, "abcdef", hex)
	})

	t.Run("with whitespace", func(t *testing.T) {
		ts, hex, ok := ParseSignature("t=1234567890, sha256=abcdef")
		assert.True(t, ok)
		assert.Equal(t, int64(1234567890), ts)
		assert.Equal(t, "abcdef", hex)
	})

	t.Run("no comma", func(t *testing.T) {
		_, _, ok := ParseSignature("t=1234567890sha256=abcdef")
		assert.False(t, ok)
	})

	t.Run("empty string", func(t *testing.T) {
		_, _, ok := ParseSignature("")
		assert.False(t, ok)
	})
}

func TestSignWithTime(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`hello`)
	ts := time.Unix(1700000000, 0)

	sig := SignWithTime(secret, "POST", "/echo", body, ts)

	// Verify round-trips correctly
	parsedTs, _, ok := ParseSignature(sig)
	assert.True(t, ok)
	assert.Equal(t, int64(1700000000), parsedTs)
}

// TestSignRequest_ParityWithSign locks the contract that the *http.Request
// wrapper is a thin delegation: it must produce the exact same signature
// as the primitive given req.Method and req.URL.RequestURI(). A test failure
// here means the wrapper has drifted from "trivial delegation" — fix the
// wrapper, not the test.
func TestSignRequest_ParityWithSign(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)
	ts := time.Unix(1700000000, 0)

	cases := []struct {
		name   string
		method string
		target string // passed to httptest.NewRequest
	}{
		{"POST with body, no query", http.MethodPost, "/provision"},
		{"GET, empty body, no query", http.MethodGet, "/info/abc"},
		{"GET with query string", http.MethodGet, "/logs/abc?tail=100"},
		{"DELETE with body", http.MethodDelete, "/provisions/abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			wrapper := SignRequestWithTime(secret, req, body, ts)
			primitive := SignWithTime(secret, req.Method, req.URL.RequestURI(), body, ts)
			assert.Equal(t, primitive, wrapper,
				"SignRequest must be exact delegation of Sign(req.Method, req.URL.RequestURI())")
		})
	}
}

// TestVerifyRequest_ParityWithVerify is the symmetric assertion: signatures
// produced by either layer must verify under either layer.
func TestVerifyRequest_ParityWithVerify(t *testing.T) {
	secret := "test-secret-that-is-at-least-32-chars!"
	body := []byte(`{"lease_uuid":"abc-123"}`)

	req := httptest.NewRequest(http.MethodPost, "/provision?dryrun=true", nil)

	t.Run("primitive sig verifies through wrapper", func(t *testing.T) {
		sig := Sign(secret, req.Method, req.URL.RequestURI(), body)
		assert.NoError(t, VerifyRequest(secret, req, body, sig, 5*time.Minute))
	})

	t.Run("wrapper sig verifies through primitive", func(t *testing.T) {
		sig := SignRequest(secret, req, body)
		assert.NoError(t, Verify(secret, req.Method, req.URL.RequestURI(), body, sig, 5*time.Minute))
	})

	t.Run("wrapper rejects cross-endpoint replay", func(t *testing.T) {
		// Sign for /provision, verify against /deprovision via wrapper.
		srcReq := httptest.NewRequest(http.MethodPost, "/provision", nil)
		sig := SignRequest(secret, srcReq, body)

		replayReq := httptest.NewRequest(http.MethodPost, "/deprovision", nil)
		err := VerifyRequest(secret, replayReq, body, sig, 5*time.Minute)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch")
	})
}
