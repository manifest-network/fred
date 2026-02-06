package api

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	// CallbackSignatureHeader is the header name for HMAC signatures on callbacks.
	// Format: "t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
	CallbackSignatureHeader = hmacauth.SignatureHeader

	// DefaultCallbackMaxAge is the default maximum age for callback timestamps.
	// Callbacks older than this are rejected to prevent replay attacks.
	DefaultCallbackMaxAge = 5 * time.Minute

	// MaxCallbackMaxAge is the maximum allowed value for callback max age.
	// Values larger than this would undermine replay protection.
	MaxCallbackMaxAge = 1 * time.Hour

	// MinCallbackSecretLength is the minimum required length for callback secrets.
	// For full 256-bit security with HMAC-SHA256, use at least 32 bytes.
	MinCallbackSecretLength = 32

	// callbackClockSkewTolerance is the maximum allowed clock skew for future timestamps.
	// This allows for minor clock differences between backend and Fred servers.
	callbackClockSkewTolerance = 1 * time.Minute
)

// CallbackAuthenticator verifies HMAC signatures on backend callbacks.
// It includes timestamp-based replay protection following the Stripe pattern.
//
// Performance note: The current implementation uses fmt.Sprintf to build the signed
// payload, which allocates an intermediate string. This is acceptable because callback
// payloads are small by design (~100 bytes: lease_uuid, status, error). The large
// Payload field (potentially megabytes) is in ProvisionRequest going TO backends,
// not in CallbackPayload coming back. If signing large data becomes necessary,
// consider writing to the HMAC incrementally to avoid copying the payload.
type CallbackAuthenticator struct {
	secret  string
	maxAge  time.Duration
	nowFunc func() time.Time // For testing; defaults to time.Now
}

// validateCallbackSecret checks that the secret meets minimum length requirements.
func validateCallbackSecret(secret string) error {
	if len(secret) < MinCallbackSecretLength {
		return fmt.Errorf("callback secret must be at least %d bytes, got %d", MinCallbackSecretLength, len(secret))
	}
	return nil
}

// NewCallbackAuthenticator creates a new callback authenticator with the given secret.
// Uses DefaultCallbackMaxAge for replay protection.
// Returns an error if the secret is shorter than MinCallbackSecretLength bytes.
func NewCallbackAuthenticator(secret string) (*CallbackAuthenticator, error) {
	if err := validateCallbackSecret(secret); err != nil {
		return nil, err
	}
	return &CallbackAuthenticator{
		secret:  secret,
		maxAge:  DefaultCallbackMaxAge,
		nowFunc: time.Now,
	}, nil
}

// NewCallbackAuthenticatorWithMaxAge creates a callback authenticator with a custom max age.
// Returns an error if the secret is shorter than MinCallbackSecretLength bytes,
// maxAge is not positive, or maxAge exceeds MaxCallbackMaxAge.
func NewCallbackAuthenticatorWithMaxAge(secret string, maxAge time.Duration) (*CallbackAuthenticator, error) {
	if err := validateCallbackSecret(secret); err != nil {
		return nil, err
	}
	if maxAge <= 0 {
		return nil, errors.New("callback max age must be positive")
	}
	if maxAge > MaxCallbackMaxAge {
		return nil, fmt.Errorf("callback max age %v exceeds maximum allowed %v", maxAge, MaxCallbackMaxAge)
	}
	return &CallbackAuthenticator{
		secret:  secret,
		maxAge:  maxAge,
		nowFunc: time.Now,
	}, nil
}

// ComputeSignature computes the HMAC-SHA256 signature for a payload with timestamp.
// Returns the signature in the format "t=<timestamp>,sha256=<hex>".
func (a *CallbackAuthenticator) ComputeSignature(payload []byte) string {
	return hmacauth.SignWithTime(a.secret, payload, a.now())
}

// ComputeSignatureWithTime computes the signature with a specific timestamp (for testing).
func (a *CallbackAuthenticator) ComputeSignatureWithTime(payload []byte, t time.Time) string {
	return hmacauth.SignWithTime(a.secret, payload, t)
}

// now returns the current time, using the injected time function if set.
func (a *CallbackAuthenticator) now() time.Time {
	if a.nowFunc != nil {
		return a.nowFunc()
	}
	return time.Now()
}

// VerifySignature verifies that the provided signature matches the payload.
// The signature should be in the format "t=<timestamp>,sha256=<hex>".
// Returns false if the signature is invalid, the timestamp is too old, or the timestamp is too far in the future.
func (a *CallbackAuthenticator) VerifySignature(payload []byte, signature string) bool {
	return a.VerifySignatureWithTime(payload, signature, a.now())
}

// VerifySignatureWithTime verifies the signature against a reference time (for testing).
func (a *CallbackAuthenticator) VerifySignatureWithTime(payload []byte, signature string, now time.Time) bool {
	return a.verifySignatureWithError(payload, signature, now) == nil
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

	if err := a.verifySignatureWithError(body, signature, a.now()); err != nil {
		return nil, err
	}

	return body, nil
}

// verifySignatureWithError is like VerifySignature but returns a descriptive error.
func (a *CallbackAuthenticator) verifySignatureWithError(payload []byte, signature string, now time.Time) error {
	timestamp, sigHex, ok := hmacauth.ParseSignature(signature)
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

	expectedSig := hmacauth.ComputeMAC(a.secret, timestamp, payload)

	providedSig, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}

	if !hmac.Equal(providedSig, expectedSig) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}
