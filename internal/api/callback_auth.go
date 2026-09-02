package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	// CallbackSignatureHeader is the header name for HMAC signatures on callbacks.
	// Format: "t=<unix-timestamp>,sha256=<hex-encoded-hmac>"
	CallbackSignatureHeader = hmacauth.SignatureHeader

	// DefaultCallbackMaxAge is the default maximum age for callback timestamps.
	// Callbacks older than this are rejected, bounding same-endpoint replay to
	// this freshness window. The callback protocol has no nonce cache.
	DefaultCallbackMaxAge = 5 * time.Minute

	// MinCallbackSecretLength is the minimum required length for callback secrets.
	// It aliases the shared signing-boundary contract so senders and verifiers
	// cannot silently drift apart.
	MinCallbackSecretLength = hmacauth.MinSecretLength

	// callbackClockSkewTolerance is the maximum allowed clock skew for future timestamps.
	// This allows for minor clock differences between backend and Fred servers.
	callbackClockSkewTolerance = 1 * time.Minute
)

// errInvalidCallbackPayload distinguishes an authenticated-but-malformed
// callback from an authentication failure at the HTTP boundary. Keeping the
// distinction typed preserves the API's existing 400 response without making
// handlers parse error strings.
var errInvalidCallbackPayload = errors.New("invalid callback payload")

// CallbackAuthenticator verifies HMAC signatures on backend callbacks.
// Its timestamp limits same-endpoint replay to a bounded freshness window;
// method and URI binding prevent cross-endpoint replay. There is intentionally
// no nonce cache for backend protocol messages.
//
// Performance note: The current implementation uses fmt.Sprintf to build the signed
// payload, which allocates an intermediate string. This is acceptable because callback
// payloads are small by design (~100 bytes: lease_uuid, status, error). The large
// Payload field (potentially megabytes) is in ProvisionRequest going TO backends,
// not in CallbackPayload coming back. If signing large data becomes necessary,
// consider writing to the HMAC incrementally to avoid copying the payload.
type CallbackAuthenticator struct {
	secret string
	maxAge time.Duration
	// canonicalPathPrefix is prepended to r.URL.RequestURI() before HMAC
	// verification. Set this when fred sits behind a path-stripping reverse
	// proxy (e.g., Traefik stripPrefix) so the verifier's canonical URI
	// matches what the signer used. Empty means no prepend — byte-identical
	// to the pre-prefix behavior.
	canonicalPathPrefix string
	// nowFunc is the authenticator's clock. NewCallbackAuthenticator is
	// the only constructor and always sets it to time.Now, so it is never
	// nil; now() calls it unconditionally.
	nowFunc func() time.Time
}

// CallbackKeyringAuthenticator verifies callbacks with the key assigned to the
// immutable storage lineage named in the HMAC-covered payload. The map is copied
// at construction so callers cannot rotate authority behind an in-flight
// verification. Its zero value is invalid.
type CallbackKeyringAuthenticator struct {
	secrets             map[backendidentity.ID]string
	maxAge              time.Duration
	canonicalPathPrefix string
	nowFunc             func() time.Time
}

// NewCallbackKeyringAuthenticator constructs the production callback verifier.
// Identities and secrets must both be unique: accepting the same key for two
// lineages would silently restore fleet-wide callback authority.
func NewCallbackKeyringAuthenticator(
	secrets map[backendidentity.ID]string,
) (*CallbackKeyringAuthenticator, error) {
	if len(secrets) == 0 {
		return nil, fmt.Errorf("callback HMAC keyring is required")
	}
	ownedSecrets := make(map[backendidentity.ID]string, len(secrets))
	secretOwners := make(map[string]backendidentity.ID, len(secrets))
	for storageID, secret := range secrets {
		if !storageID.Valid() {
			return nil, fmt.Errorf("callback HMAC keyring contains an invalid backend storage identity")
		}
		if err := validateCallbackSecret(secret); err != nil {
			return nil, fmt.Errorf("callback HMAC key for storage %s: %w", storageID, err)
		}
		if owner, duplicate := secretOwners[secret]; duplicate {
			return nil, fmt.Errorf(
				"callback HMAC key for storage %s duplicates storage %s",
				storageID, owner,
			)
		}
		ownedSecrets[storageID] = secret
		secretOwners[secret] = storageID
	}
	return &CallbackKeyringAuthenticator{
		secrets: ownedSecrets,
		maxAge:  DefaultCallbackMaxAge,
		nowFunc: time.Now,
	}, nil
}

// WithCanonicalPathPrefix applies the same reverse-proxy canonicalization
// contract as CallbackAuthenticator.
func (a *CallbackKeyringAuthenticator) WithCanonicalPathPrefix(
	prefix string,
) *CallbackKeyringAuthenticator {
	a.canonicalPathPrefix = prefix
	return a
}

// validateCallbackSecret checks that the secret meets minimum length requirements.
func validateCallbackSecret(secret string) error {
	if len(secret) < MinCallbackSecretLength {
		return fmt.Errorf("callback secret must be at least %d bytes, got %d", MinCallbackSecretLength, len(secret))
	}
	return nil
}

// NewCallbackAuthenticator creates a new callback authenticator with the given secret.
// Uses DefaultCallbackMaxAge as its replay freshness bound.
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

// WithCanonicalPathPrefix configures a static path prefix that is prepended to
// r.URL.RequestURI() before HMAC verification. Set this when fred is deployed
// behind a path-stripping reverse proxy (e.g., Traefik stripPrefix mapping
// /api/fred/* → /*) so the verifier's canonical URI matches what the signer
// used. Passing the empty string is a no-op and preserves the default direct-
// call behavior. Returns the receiver for chaining.
func (a *CallbackAuthenticator) WithCanonicalPathPrefix(prefix string) *CallbackAuthenticator {
	a.canonicalPathPrefix = prefix
	return a
}

// ComputeSignature computes the HMAC-SHA256 signature for a request shape with
// the current timestamp. method and uri must match what the verifier will see
// on the wire (typically req.Method and req.URL.RequestURI()).
// Returns the signature in the format "t=<timestamp>,sha256=<hex>".
func (a *CallbackAuthenticator) ComputeSignature(method, uri string, payload []byte) string {
	return hmacauth.SignWithTime(a.secret, method, uri, payload, a.now())
}

// now returns the current time through the authenticator's clock.
func (a *CallbackAuthenticator) now() time.Time {
	return a.nowFunc()
}

// VerifySignature verifies that the provided signature matches the request shape.
// method and uri must match what the sender used (typically r.Method and
// r.URL.RequestURI() on the inbound request). The signature should be in the
// format "t=<timestamp>,sha256=<hex>".
// Returns false if the signature is invalid, the timestamp is too old, or the timestamp is too far in the future.
func (a *CallbackAuthenticator) VerifySignature(method, uri string, payload []byte, signature string) bool {
	return a.VerifySignatureWithTime(method, uri, payload, signature, a.now())
}

// VerifySignatureWithTime verifies the signature against an explicit
// reference time. VerifySignature is the production entry point and
// delegates here with a.now(); tests call it directly to pin the clock
// and drive the replay window deterministically.
func (a *CallbackAuthenticator) VerifySignatureWithTime(method, uri string, payload []byte, signature string, now time.Time) bool {
	return a.verifySignatureWithError(method, uri, payload, signature, now) == nil
}

// VerifyRequest reads the request body, verifies the signature, and returns the body bytes.
// Returns an error if verification fails or the timestamp is too old.
//
// Note: do not confuse this method with hmacauth.VerifyRequest. The names
// live in different packages and have different semantics:
//
//   - This method reads r.Body itself (callers pass the bare *http.Request
//     and receive the body back) and applies the callback-specific maxAge
//     and clock-skew tolerance configured on the CallbackAuthenticator.
//   - hmacauth.VerifyRequest is a low-level wrapper that takes a
//     pre-read body and an explicit maxAge; r.Body is untouched.
//
// Internally, this method delegates the canonical-string check to
// hmacauth.VerifyWithTime via verifySignatureWithError.
func (a *CallbackAuthenticator) VerifyRequest(r *http.Request) ([]byte, error) {
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		return nil, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	uri := a.canonicalPathPrefix + r.URL.RequestURI()
	if err := a.verifySignatureWithError(r.Method, uri, body, signature, a.now()); err != nil {
		return nil, err
	}

	return body, nil
}

// VerifyCallbackRequest authenticates and decodes one callback DTO. It keeps
// the legacy single-secret constructor useful for isolated tests and
// non-production embeddings while sharing the strict wire decoder used by the
// production keyring.
func (a *CallbackAuthenticator) VerifyCallbackRequest(
	r *http.Request,
) (backend.CallbackPayload, error) {
	body, err := a.VerifyRequest(r)
	if err != nil {
		return backend.CallbackPayload{}, err
	}
	callback, err := decodeCallbackPayload(body)
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("%w: %w", errInvalidCallbackPayload, err)
	}
	return callback, nil
}

// VerifyCallbackRequest reads the already server-bounded body once, decodes the
// exact DTO that application code will receive, selects its lineage key, then
// authenticates the original bytes. The backend storage ID is only an untrusted
// key selector until HMAC verification succeeds; callback application later
// binds that same signed ID to operation/lifecycle-owned placement authority.
func (a *CallbackKeyringAuthenticator) VerifyCallbackRequest(
	r *http.Request,
) (backend.CallbackPayload, error) {
	if a == nil || len(a.secrets) == 0 || a.nowFunc == nil {
		return backend.CallbackPayload{}, fmt.Errorf("callback HMAC keyring is unavailable")
	}
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		return backend.CallbackPayload{}, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("failed to read request body: %w", err)
	}
	callback, err := decodeCallbackPayload(body)
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("%w: %w", errInvalidCallbackPayload, err)
	}
	storageID, err := backendidentity.Parse(callback.BackendStorageID)
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("callback backend storage identity: %w", err)
	}
	secret, exists := a.secrets[storageID]
	if !exists {
		return backend.CallbackPayload{}, fmt.Errorf("callback backend storage identity is not configured")
	}
	uri := a.canonicalPathPrefix + r.URL.RequestURI()
	if err := hmacauth.VerifyWithTime(
		secret, r.Method, uri, body, signature,
		a.maxAge, callbackClockSkewTolerance, a.nowFunc(),
	); err != nil {
		return backend.CallbackPayload{}, err
	}
	return callback, nil
}

var callbackPayloadFields = [...]string{
	"lease_uuid",
	"status",
	"error",
	"backend_storage_id",
	"backend",
	"operation_id",
	"lifecycle_id",
	"retained",
}

// decodeCallbackPayload decodes the callback exactly once and rejects any JSON
// object whose field names encoding/json could interpret ambiguously. Unknown
// fields remain forward-compatible, but duplicate names and case variants of a
// known protocol name are rejected instead of allowing last-value-wins parsing.
func decodeCallbackPayload(body []byte) (backend.CallbackPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("decode callback payload: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return backend.CallbackPayload{}, fmt.Errorf("callback payload must be a JSON object")
	}

	var callback backend.CallbackPayload
	seen := make(map[string]struct{}, len(callbackPayloadFields))
	for decoder.More() {
		fieldToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return backend.CallbackPayload{}, fmt.Errorf("decode callback field name: %w", tokenErr)
		}
		field, ok := fieldToken.(string)
		if !ok {
			return backend.CallbackPayload{}, fmt.Errorf("callback field name must be a string")
		}
		if _, duplicate := seen[field]; duplicate {
			return backend.CallbackPayload{}, fmt.Errorf("callback payload contains duplicate field %q", field)
		}
		seen[field] = struct{}{}
		for _, canonical := range callbackPayloadFields {
			if strings.EqualFold(field, canonical) && field != canonical {
				return backend.CallbackPayload{}, fmt.Errorf(
					"callback payload contains ambiguous field %q; use %q",
					field, canonical,
				)
			}
		}

		var target any
		switch field {
		case "lease_uuid":
			target = &callback.LeaseUUID
		case "status":
			target = &callback.Status
		case "error":
			target = &callback.Error
		case "backend_storage_id":
			target = &callback.BackendStorageID
		case "backend":
			target = &callback.Backend
		case "operation_id":
			target = &callback.OperationID
		case "lifecycle_id":
			target = &callback.LifecycleID
		case "retained":
			target = &callback.Retained
		default:
			target = new(json.RawMessage)
		}
		if err := decoder.Decode(target); err != nil {
			return backend.CallbackPayload{}, fmt.Errorf("decode callback field %q: %w", field, err)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return backend.CallbackPayload{}, fmt.Errorf("close callback payload: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return backend.CallbackPayload{}, fmt.Errorf("callback payload must end with a JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return backend.CallbackPayload{}, fmt.Errorf("callback payload contains trailing JSON data")
		}
		return backend.CallbackPayload{}, fmt.Errorf("decode callback payload trailer: %w", err)
	}
	return callback, nil
}

// verifySignatureWithError is like VerifySignature but returns a descriptive error.
func (a *CallbackAuthenticator) verifySignatureWithError(method, uri string, payload []byte, signature string, now time.Time) error {
	return hmacauth.VerifyWithTime(a.secret, method, uri, payload, signature, a.maxAge, callbackClockSkewTolerance, now)
}
