// Package hmacauth provides HMAC-SHA256 signing and verification for
// request authentication between Fred components (Fred ↔ backends).
//
// # Wire format
//
// Header: "X-Fred-Signature: t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
//
// # Signed canonical string
//
// The HMAC is computed over a four-field canonical string:
//
//	"<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>"
//
// Binding the HTTP method and request URI into the signed envelope prevents
// cross-endpoint replay: a signature captured on (e.g.) POST /provision can
// no longer be replayed against POST /deprovision, nor can a signature on
// GET /info/<id> be replayed against GET /logs/<id>. The body is included
// via its SHA-256 hash (bounded length, binary-safe) rather than as a
// literal string, so arbitrary bytes — including NUL, newlines, or non-
// UTF-8 — cannot influence the canonical string.
//
// The timestamp is bound to the same envelope so an attacker cannot
// substitute a different timestamp on a captured request.
//
// # Replay protection
//
// Verify takes a maxAge parameter — callers choose the freshness window for
// their use case (the callback path uses 5 minutes via DefaultCallbackMaxAge).
// The 1-minute clock-skew tolerance for future timestamps is hardcoded in
// Verify; use VerifyWithTime to override it.
//
// Combined with the method+URI+body binding above, the timestamp check
// prevents replay, timestamp substitution, and cross-endpoint replay
// attacks. The canonical-string approach is conceptually similar to
// AWS SigV4's canonical request, simplified to the fields this codebase
// needs.
//
// # Constant-time comparison
//
// Verification uses hmac.Equal (constant-time) to prevent timing attacks.
//
// # Method and URI conventions
//
// Method is case-sensitive (RFC 9110 §9.1) — callers pass it as-is, and no
// normalization is performed. Sender and verifier must agree on case;
// Go's net/http does NOT canonicalize method case (http.NewRequest("post",
// ...) preserves "post"; server parsing preserves whatever the client put
// on the wire). Agreement in this codebase comes from convention: every
// call site uses the http.MethodGet / http.MethodPost / ... named
// constants, which are uppercase. A caller that passes a literal "post"
// will produce a signature that does not verify against a verifier seeing
// "POST" — that is the intended behavior, since silent normalization would
// mask method-confusion bugs.
//
// URI is the request-target: path plus "?<raw-query>" if a query string is
// present. On the verifier side, use r.URL.RequestURI(). On the sender
// side, use req.URL.RequestURI() after constructing the *http.Request —
// both sides then sign the bytes that go on the wire.
//
// # Used by
//
//   - Backend → Fred callbacks (internal/api/callback_auth.go)
//   - Fred → backend HTTP requests (internal/backend/client.go)
//   - docker-backend self-verification middleware (cmd/docker-backend/main.go)
//   - k3s-backend self-verification middleware (cmd/k3s-backend/server.go)
package hmacauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader is the HTTP header name for HMAC signatures.
const SignatureHeader = "X-Fred-Signature"

// ComputeMAC computes the HMAC-SHA256 over the canonical string
// "<timestamp>\n<method>\n<uri>\n<hex(sha256(body))>".
//
// body may be nil; sha256.Sum256(nil) is well-defined and produces the
// stable empty-body hash, so both sender and verifier always emit the
// hash field.
func ComputeMAC(secret string, timestamp int64, method, uri string, body []byte) []byte {
	bodyHash := sha256.Sum256(body)
	signed := fmt.Sprintf("%d\n%s\n%s\n%x", timestamp, method, uri, bodyHash[:])
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signed))
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

// Sign computes an HMAC-SHA256 signature for the given request shape using
// the current time. Returns the signature in the format
// "t=<timestamp>,sha256=<hex>".
//
// Callers building an outbound *http.Request should pass req.Method and
// req.URL.RequestURI() — both fields are populated by the stdlib request
// constructor and match what the verifier will see on the wire.
func Sign(secret, method, uri string, body []byte) string {
	return SignWithTime(secret, method, uri, body, time.Now())
}

// SignWithTime is Sign with an explicit timestamp (for tests and deterministic flows).
func SignWithTime(secret, method, uri string, body []byte, t time.Time) string {
	timestamp := t.Unix()
	sig := ComputeMAC(secret, timestamp, method, uri, body)
	return fmt.Sprintf("t=%d,sha256=%s", timestamp, hex.EncodeToString(sig))
}

// Verify verifies an HMAC-SHA256 signature against the given request shape.
// It checks signature format, timestamp freshness (within maxAge), and HMAC
// correctness over "<timestamp>\n<method>\n<uri>\n<hex(sha256(body))>".
// Uses hmac.Equal on raw bytes for constant-time comparison.
// Clock skew tolerance for future timestamps is 1 minute.
//
// Verifiers should pass r.Method and r.URL.RequestURI() from the incoming
// *http.Request.
func Verify(secret, method, uri string, body []byte, signature string, maxAge time.Duration) error {
	return VerifyWithTime(secret, method, uri, body, signature, maxAge, time.Minute, time.Now())
}

// VerifyWithTime verifies an HMAC-SHA256 signature with an explicit reference time
// and configurable clock skew tolerance for future timestamps.
func VerifyWithTime(
	secret, method, uri string,
	body []byte,
	signature string,
	maxAge, clockSkew time.Duration,
	now time.Time,
) error {
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

	expectedSig := ComputeMAC(secret, timestamp, method, uri, body)
	if !hmac.Equal(providedSig, expectedSig) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

// --- *http.Request convenience wrappers ---------------------------------
//
// These thin wrappers extract method + URI from a *http.Request and
// delegate to the primitive Sign/Verify functions. They exist so the
// many call sites that already hold an *http.Request in scope (HTTP
// clients, middleware verifiers) can avoid repeating
// (req.Method, req.URL.RequestURI()) at every call. The contract
// (canonical string, hash, comparison) lives entirely in the primitive
// layer — these wrappers add zero logic of their own.

// SignRequest signs the given request shape using the current time.
// Equivalent to Sign(secret, req.Method, req.URL.RequestURI(), body).
func SignRequest(secret string, req *http.Request, body []byte) string {
	return Sign(secret, req.Method, req.URL.RequestURI(), body)
}

// SignRequestWithTime is SignRequest with an explicit timestamp.
func SignRequestWithTime(secret string, req *http.Request, body []byte, t time.Time) string {
	return SignWithTime(secret, req.Method, req.URL.RequestURI(), body, t)
}

// VerifyRequest verifies a signature against the given request shape.
// The caller must read r.Body first and pass it; this wrapper does not
// touch r.Body. Equivalent to
// Verify(secret, r.Method, r.URL.RequestURI(), body, signature, maxAge).
//
// Note: (*api.CallbackAuthenticator).VerifyRequest has different
// semantics — it reads the body itself before verifying. The names do
// not collide at the type system level (different packages) but be
// aware which one you are calling.
func VerifyRequest(secret string, r *http.Request, body []byte, signature string, maxAge time.Duration) error {
	return Verify(secret, r.Method, r.URL.RequestURI(), body, signature, maxAge)
}

// VerifyRequestWithTime is VerifyRequest with explicit reference time and
// clock-skew tolerance for future timestamps.
func VerifyRequestWithTime(
	secret string,
	r *http.Request,
	body []byte,
	signature string,
	maxAge, clockSkew time.Duration,
	now time.Time,
) error {
	return VerifyWithTime(secret, r.Method, r.URL.RequestURI(), body, signature, maxAge, clockSkew, now)
}
