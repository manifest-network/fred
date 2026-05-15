// Package hmacauth provides HMAC-SHA256 signing and verification for
// request authentication between Fred components (Fred ↔ backends).
//
// # Wire format
//
// Header: "X-Fred-Signature: t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
//
// # Signed payload
//
// "<timestamp>.<body>" — the timestamp is bound to the body so an attacker
// cannot substitute a different timestamp on a captured request.
//
// # Replay protection
//
// Verify takes a maxAge parameter — callers choose the freshness window for
// their use case (the callback path uses 5 minutes via DefaultCallbackMaxAge).
// The 1-minute clock-skew tolerance for future timestamps is hardcoded in
// Verify; use VerifyWithTime to override it.
//
// Combined with the body binding above, the timestamp check prevents both
// replay and timestamp substitution attacks. The pattern is the same one
// used by Stripe, GitHub, and Slack webhooks.
//
// # Constant-time comparison
//
// Verification uses hmac.Equal (constant-time) to prevent timing attacks.
//
// # Known limitation: cross-endpoint replay within the freshness window
//
// The signed payload is "<timestamp>.<body>" — the HTTP method and request
// URI are NOT bound. An attacker who captures a signed request can replay
// it within the freshness window (5 minutes on the callback path) against
// any other endpoint that accepts the same JSON fields. Two concrete
// shapes in this codebase:
//
//   - A signed POST /provision body can be replayed against POST
//     /deprovision: deprovision uses only lease_uuid and ignores every
//     other field on the body, so the signature still verifies and the
//     handler still acts.
//   - Every empty-body GET shares the same signed payload (only the
//     timestamp varies). A captured GET /info/{lease_uuid} signature can
//     therefore be replayed against GET /logs/{lease_uuid} (and vice
//     versa) for the same lease within the freshness window.
//
// This pattern (timestamp + body, no method/URI binding) matches
// Stripe/GitHub/Slack webhook signing — correct for single-endpoint
// webhook delivery, but too narrow for this codebase's multi-endpoint
// shared-JSON-shape HTTP contract. The right fix is to rebind the
// signature to "<timestamp>.<METHOD>.<uri>.<body>" across all four
// senders (Fred → backend HTTP client, both backends' callback
// senders, Fred's callback receiver) and all four verifiers in a
// coordinated rollout. That work is tracked in ENG-191; the back-
// compat strategy (header version + dual-verify window) lives in
// the Linear issue.
//
// Until ENG-191 lands: callers should treat the 5-minute freshness
// window as the effective replay attack surface for any captured
// signature, and assume the JSON payload alone gates which method/URI
// pair the attacker can substitute.
//
// # Used by
//
//   - Backend → Fred callbacks (internal/api/callback_auth.go)
//   - Fred → backend HTTP requests (internal/backend/client.go)
//   - Backend self-verification middleware (cmd/docker-backend/main.go)
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
