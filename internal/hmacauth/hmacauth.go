// Package hmacauth provides HMAC-SHA256 signing and verification
// for request authentication between Fred components.
//
// Signature format: "t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
// Signed payload: "<timestamp>.<body>"
package hmacauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader is the HTTP header name for HMAC signatures.
const SignatureHeader = "X-Fred-Signature"

// ComputeMAC computes the HMAC-SHA256 for the given secret, timestamp, and payload.
// The signed message is "<timestamp>.<payload>".
func ComputeMAC(secret string, timestamp int64, payload []byte) []byte {
	signedPayload := fmt.Sprintf("%d.%s", timestamp, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signedPayload))
	return mac.Sum(nil)
}

// ParseSignature parses a signature in the format "t=<timestamp>,sha256=<hex>".
// Returns the timestamp, hex-encoded signature, and whether parsing succeeded.
func ParseSignature(signature string) (int64, string, bool) {
	timestampPart, sigPart, ok := strings.Cut(signature, ",")
	if !ok {
		return 0, "", false
	}

	timestampPart = strings.TrimSpace(timestampPart)
	sigPart = strings.TrimSpace(sigPart)

	timestampStr, ok := strings.CutPrefix(timestampPart, "t=")
	if !ok {
		return 0, "", false
	}
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, "", false
	}

	sigHex, ok := strings.CutPrefix(sigPart, "sha256=")
	if !ok {
		return 0, "", false
	}

	return timestamp, sigHex, true
}

// Sign computes an HMAC-SHA256 signature for payload using the current time.
// Returns the signature in the format "t=<timestamp>,sha256=<hex>".
func Sign(secret string, payload []byte) string {
	return SignWithTime(secret, payload, time.Now())
}

// SignWithTime computes an HMAC-SHA256 signature for payload with a specific timestamp.
// Returns the signature in the format "t=<timestamp>,sha256=<hex>".
func SignWithTime(secret string, payload []byte, t time.Time) string {
	timestamp := t.Unix()
	sig := ComputeMAC(secret, timestamp, payload)
	return fmt.Sprintf("t=%d,sha256=%s", timestamp, hex.EncodeToString(sig))
}

// Verify verifies an HMAC-SHA256 signature against the payload.
// It checks signature format, timestamp freshness (within maxAge), and HMAC correctness.
// Uses hmac.Equal on raw bytes for constant-time comparison.
// Clock skew tolerance for future timestamps is 1 minute.
func Verify(secret string, payload []byte, signature string, maxAge time.Duration) error {
	return VerifyWithTime(secret, payload, signature, maxAge, time.Minute, time.Now())
}

// VerifyWithTime verifies an HMAC-SHA256 signature with an explicit reference time
// and configurable clock skew tolerance for future timestamps.
func VerifyWithTime(secret string, payload []byte, signature string, maxAge, clockSkew time.Duration, now time.Time) error {
	timestamp, sigHex, ok := ParseSignature(signature)
	if !ok {
		return fmt.Errorf("invalid signature format: expected t=<timestamp>,sha256=<hex>")
	}

	signedAt := time.Unix(timestamp, 0)
	if now.Sub(signedAt) > maxAge {
		return fmt.Errorf("signature expired: signed %v ago, max age is %v", now.Sub(signedAt).Round(time.Second), maxAge)
	}
	if signedAt.After(now.Add(clockSkew)) {
		return fmt.Errorf("signature timestamp too far in future: %v ahead", signedAt.Sub(now).Round(time.Second))
	}

	providedSig, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}

	expectedSig := ComputeMAC(secret, timestamp, payload)
	if !hmac.Equal(providedSig, expectedSig) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}
