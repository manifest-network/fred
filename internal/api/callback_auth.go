package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// CallbackSignatureHeader is the header name for HMAC signatures on callbacks.
	// Format: "t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
	CallbackSignatureHeader = "X-Fred-Signature"

	// DefaultCallbackMaxAge is the default maximum age for callback timestamps.
	// Callbacks older than this are rejected to prevent replay attacks.
	DefaultCallbackMaxAge = 5 * time.Minute

	// callbackClockSkewTolerance is the maximum allowed clock skew for future timestamps.
	// This allows for minor clock differences between backend and Fred servers.
	callbackClockSkewTolerance = 1 * time.Minute
)

// CallbackAuthenticator verifies HMAC signatures on backend callbacks.
// It includes timestamp-based replay protection following the Stripe pattern.
type CallbackAuthenticator struct {
	secret []byte
	maxAge time.Duration
}

// NewCallbackAuthenticator creates a new callback authenticator with the given secret.
// Uses DefaultCallbackMaxAge for replay protection.
func NewCallbackAuthenticator(secret string) *CallbackAuthenticator {
	return &CallbackAuthenticator{
		secret: []byte(secret),
		maxAge: DefaultCallbackMaxAge,
	}
}

// NewCallbackAuthenticatorWithMaxAge creates a callback authenticator with a custom max age.
func NewCallbackAuthenticatorWithMaxAge(secret string, maxAge time.Duration) *CallbackAuthenticator {
	return &CallbackAuthenticator{
		secret: []byte(secret),
		maxAge: maxAge,
	}
}

// ComputeSignature computes the HMAC-SHA256 signature for a payload with timestamp.
// Returns the signature in the format "t=<timestamp>,sha256=<hex>".
func (a *CallbackAuthenticator) ComputeSignature(payload []byte) string {
	return a.ComputeSignatureWithTime(payload, time.Now())
}

// ComputeSignatureWithTime computes the signature with a specific timestamp (for testing).
func (a *CallbackAuthenticator) ComputeSignatureWithTime(payload []byte, t time.Time) string {
	timestamp := t.Unix()
	signedPayload := fmt.Sprintf("%d.%s", timestamp, payload)

	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(signedPayload))
	sig := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("t=%d,sha256=%s", timestamp, sig)
}

// VerifySignature verifies that the provided signature matches the payload.
// The signature should be in the format "t=<timestamp>,sha256=<hex>".
// Returns false if the signature is invalid or the timestamp is too old.
func (a *CallbackAuthenticator) VerifySignature(payload []byte, signature string) bool {
	return a.VerifySignatureWithTime(payload, signature, time.Now())
}

// VerifySignatureWithTime verifies the signature against a reference time (for testing).
func (a *CallbackAuthenticator) VerifySignatureWithTime(payload []byte, signature string, now time.Time) bool {
	return a.verifySignatureWithError(payload, signature, now) == nil
}

// parseSignature parses "t=<timestamp>,sha256=<hex>" format.
// Returns timestamp, signature hex, and success flag.
func parseSignature(signature string) (int64, string, bool) {
	// Split into parts
	parts := strings.Split(signature, ",")
	if len(parts) != 2 {
		return 0, "", false
	}

	// Parse timestamp
	if !strings.HasPrefix(parts[0], "t=") {
		return 0, "", false
	}
	timestamp, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "t="), 10, 64)
	if err != nil {
		return 0, "", false
	}

	// Parse signature
	if !strings.HasPrefix(parts[1], "sha256=") {
		return 0, "", false
	}
	sigHex := strings.TrimPrefix(parts[1], "sha256=")

	return timestamp, sigHex, true
}

// VerifyRequest reads the request body, verifies the signature, and returns the body bytes.
// Returns an error if verification fails or the timestamp is too old.
func (a *CallbackAuthenticator) VerifyRequest(r *http.Request) ([]byte, error) {
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		return nil, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	if err := a.verifySignatureWithError(body, signature, time.Now()); err != nil {
		return nil, err
	}

	return body, nil
}

// verifySignatureWithError is like VerifySignature but returns a descriptive error.
func (a *CallbackAuthenticator) verifySignatureWithError(payload []byte, signature string, now time.Time) error {
	timestamp, sigHex, ok := parseSignature(signature)
	if !ok {
		return fmt.Errorf("invalid signature format: expected t=<timestamp>,sha256=<hex>")
	}

	signedAt := time.Unix(timestamp, 0)
	if now.Sub(signedAt) > a.maxAge {
		return fmt.Errorf("signature expired: signed %v ago, max age is %v", now.Sub(signedAt).Round(time.Second), a.maxAge)
	}
	if signedAt.After(now.Add(callbackClockSkewTolerance)) {
		return fmt.Errorf("signature timestamp too far in future: %v ahead", signedAt.Sub(now).Round(time.Second))
	}

	providedSig, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}

	signedPayload := fmt.Sprintf("%d.%s", timestamp, payload)
	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(signedPayload))
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(providedSig, expectedSig) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}
