package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
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
	secret        string
	maxAge        time.Duration
	proofVerifier hmacauth.CallbackProofVerifier
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

// CallbackKey is one backend's callback authentication: its configured name,
// which labels metrics only after its key verified a callback, and the keys it
// may sign with (its current key and, during a rotation, its previous key).
type CallbackKey struct {
	Backend string
	Keys    hmacauth.VerifyKeys
}

// CallbackKeyringAuthenticator verifies callbacks with the keys assigned to the
// immutable storage lineage named in the HMAC-covered payload. The map is copied
// at construction so callers cannot rotate authority behind an in-flight
// verification. Its zero value is invalid.
type CallbackKeyringAuthenticator struct {
	keys                map[backendidentity.ID]CallbackKey
	maxAge              time.Duration
	canonicalPathPrefix string
	nowFunc             func() time.Time
	proofVerifier       hmacauth.CallbackProofVerifier
}

// NewCallbackKeyringAuthenticator constructs the production callback verifier.
// Identities, backend names and every key must be unique, keys compared by
// HMAC equivalence: two lineages accepting a common key, even a previous key
// during a rotation, would silently restore fleet-wide callback authority.
func NewCallbackKeyringAuthenticator(
	keys map[backendidentity.ID]CallbackKey,
	proofVerifier hmacauth.CallbackProofVerifier,
) (*CallbackKeyringAuthenticator, error) {
	if !proofVerifier.Valid() {
		return nil, fmt.Errorf("callback proof verifier is required")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("callback HMAC keyring is required")
	}
	owned := make(map[backendidentity.ID]CallbackKey, len(keys))
	names := make(map[string]backendidentity.ID, len(keys))
	for storageID, key := range keys {
		if !storageID.Valid() {
			return nil, fmt.Errorf("callback HMAC keyring contains an invalid backend storage identity")
		}
		if key.Backend == "" {
			return nil, fmt.Errorf("callback HMAC key for storage %s has no backend name", storageID)
		}
		if !key.Keys.Valid() {
			return nil, fmt.Errorf("callback HMAC key for storage %s is invalid", storageID)
		}
		if owner, duplicate := names[key.Backend]; duplicate {
			return nil, fmt.Errorf(
				"callback backend %q is bound to storage %s and %s", key.Backend, owner, storageID,
			)
		}
		for ownerID, other := range owned {
			if key.Keys.SharesKeyWith(other.Keys) {
				return nil, fmt.Errorf(
					"callback HMAC key for storage %s duplicates storage %s", storageID, ownerID,
				)
			}
		}
		owned[storageID] = key
		names[key.Backend] = storageID
	}
	for _, key := range owned {
		metrics.APICallbackSignatureKeyTotal.WithLabelValues(key.Backend, metrics.CallbackKeySlotCurrent)
		configured := 0.0
		if key.Keys.HasRotation() {
			metrics.APICallbackSignatureKeyTotal.WithLabelValues(key.Backend, metrics.CallbackKeySlotPrevious)
			configured = 1
		}
		metrics.APICallbackPreviousKeyConfigured.WithLabelValues(key.Backend).Set(configured)
	}
	for _, failure := range callbackAuthFailures {
		metrics.APICallbackAuthFailuresTotal.WithLabelValues(failure.label())
	}
	return &CallbackKeyringAuthenticator{
		keys:          owned,
		maxAge:        DefaultCallbackMaxAge,
		nowFunc:       time.Now,
		proofVerifier: proofVerifier,
	}, nil
}

// callbackAuthFailure is the closed cause of a refused callback signature. The
// zero value is invalid and never counted.
type callbackAuthFailure uint8

const (
	callbackAuthFailureInvalid callbackAuthFailure = iota
	callbackAuthFailureMissing
	callbackAuthFailureFormat
	callbackAuthFailureExpired
	callbackAuthFailureFuture
	callbackAuthFailureMismatch
	callbackAuthFailureUnknownStorage
)

var callbackAuthFailures = [...]callbackAuthFailure{
	callbackAuthFailureMissing,
	callbackAuthFailureFormat,
	callbackAuthFailureExpired,
	callbackAuthFailureFuture,
	callbackAuthFailureMismatch,
	callbackAuthFailureUnknownStorage,
}

func (failure callbackAuthFailure) label() string {
	switch failure {
	case callbackAuthFailureMissing:
		return metrics.CallbackAuthFailureMissing
	case callbackAuthFailureFormat:
		return metrics.CallbackAuthFailureFormat
	case callbackAuthFailureExpired:
		return metrics.CallbackAuthFailureExpired
	case callbackAuthFailureFuture:
		return metrics.CallbackAuthFailureFuture
	case callbackAuthFailureMismatch:
		return metrics.CallbackAuthFailureMismatch
	case callbackAuthFailureUnknownStorage:
		return metrics.CallbackAuthFailureUnknownStorage
	default:
		return ""
	}
}

// callbackAuthFailureOf maps a verification failure to its counted cause; any
// other error, such as an unavailable verifier, is not an authentication
// failure and maps to the invalid zero value.
func callbackAuthFailureOf(err error) callbackAuthFailure {
	switch hmacauth.FailureReasonOf(err) {
	case hmacauth.FailureFormat:
		return callbackAuthFailureFormat
	case hmacauth.FailureExpired:
		return callbackAuthFailureExpired
	case hmacauth.FailureFuture:
		return callbackAuthFailureFuture
	case hmacauth.FailureMismatch:
		return callbackAuthFailureMismatch
	default:
		return callbackAuthFailureInvalid
	}
}

func (failure callbackAuthFailure) count() {
	if label := failure.label(); label != "" {
		metrics.APICallbackAuthFailuresTotal.WithLabelValues(label).Inc()
	}
}

func callbackKeySlotLabel(slot hmacauth.KeySlot) string {
	if slot == hmacauth.KeySlotRotation {
		return metrics.CallbackKeySlotPrevious
	}
	return metrics.CallbackKeySlotCurrent
}

// WithCanonicalPathPrefix applies the same reverse-proxy canonicalization
// contract as CallbackAuthenticator.
func (a *CallbackKeyringAuthenticator) WithCanonicalPathPrefix(
	prefix string,
) *CallbackKeyringAuthenticator {
	if a == nil {
		return nil
	}
	clone := *a
	clone.canonicalPathPrefix = prefix
	return &clone
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
func NewCallbackAuthenticator(
	secret string,
	proofVerifier hmacauth.CallbackProofVerifier,
) (*CallbackAuthenticator, error) {
	if !proofVerifier.Valid() {
		return nil, fmt.Errorf("callback proof verifier is required")
	}
	if err := validateCallbackSecret(secret); err != nil {
		return nil, err
	}
	return &CallbackAuthenticator{
		secret:        secret,
		maxAge:        DefaultCallbackMaxAge,
		nowFunc:       time.Now,
		proofVerifier: proofVerifier,
	}, nil
}

// WithCanonicalPathPrefix configures a static path prefix that is prepended to
// r.URL.RequestURI() before HMAC verification. Set this when fred is deployed
// behind a path-stripping reverse proxy (e.g., Traefik stripPrefix mapping
// /api/fred/* → /*) so the verifier's canonical URI matches what the signer
// used. Passing the empty string is a no-op and preserves the default direct-
// call behavior. Returns the receiver for chaining.
func (a *CallbackAuthenticator) WithCanonicalPathPrefix(prefix string) *CallbackAuthenticator {
	if a == nil {
		return nil
	}
	clone := *a
	clone.canonicalPathPrefix = prefix
	return &clone
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

// VerifyCallbackEvidence authenticates the exact callback request envelope and
// returns opaque evidence rather than a caller-constructible DTO.
func (a *CallbackAuthenticator) VerifyCallbackEvidence(
	r *http.Request,
) (hmacauth.VerifiedRequest, error) {
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		return hmacauth.VerifiedRequest{}, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}
	body, err := callbackwire.ReadEnvelope(r.Body)
	if err != nil {
		return hmacauth.VerifiedRequest{}, fmt.Errorf("failed to read request body: %w", err)
	}
	uri := a.canonicalPathPrefix + r.URL.RequestURI()
	proof, err := a.proofVerifier.VerifyRoutedWithTime(
		a.secret, r.Method, uri, body, signature, "",
		a.canonicalPathPrefix+"/callbacks/provision",
		a.maxAge, callbackClockSkewTolerance, a.now(),
	)
	if err != nil {
		return hmacauth.VerifiedRequest{}, err
	}
	return proof, nil
}

// VerifyCallbackEvidence authenticates with the immutable storage-lineage key
// selected by the signed body and binds that route into the returned proof. The
// backend storage ID is only an untrusted key selector until HMAC verification
// succeeds; callback application later binds the same signed ID to
// operation/lifecycle-owned placement authority.
func (a *CallbackKeyringAuthenticator) VerifyCallbackEvidence(
	r *http.Request,
) (hmacauth.VerifiedRequest, error) {
	if a == nil || len(a.keys) == 0 || a.nowFunc == nil || !a.proofVerifier.Valid() {
		return hmacauth.VerifiedRequest{}, fmt.Errorf("callback HMAC keyring is unavailable")
	}
	signature := r.Header.Get(CallbackSignatureHeader)
	if signature == "" {
		callbackAuthFailureMissing.count()
		return hmacauth.VerifiedRequest{}, fmt.Errorf("missing %s header", CallbackSignatureHeader)
	}
	body, err := callbackwire.ReadEnvelope(r.Body)
	if err != nil {
		return hmacauth.VerifiedRequest{}, fmt.Errorf("failed to read request body: %w", err)
	}
	storageID, err := callbackwire.SelectUntrustedStorageRoute(body)
	if err != nil {
		return hmacauth.VerifiedRequest{}, fmt.Errorf("%w: %w", errInvalidCallbackPayload, err)
	}
	key, exists := a.keys[storageID]
	if !exists {
		callbackAuthFailureUnknownStorage.count()
		return hmacauth.VerifiedRequest{}, fmt.Errorf("callback backend storage identity is not configured")
	}
	uri := a.canonicalPathPrefix + r.URL.RequestURI()
	proof, slot, err := a.proofVerifier.VerifyRoutedKeysWithTime(
		key.Keys, r.Method, uri, body, signature,
		storageID.String(),
		a.canonicalPathPrefix+"/callbacks/provision",
		a.maxAge, callbackClockSkewTolerance, a.nowFunc(),
	)
	if err != nil {
		callbackAuthFailureOf(err).count()
		return hmacauth.VerifiedRequest{}, err
	}
	metrics.APICallbackSignatureKeyTotal.WithLabelValues(key.Backend, callbackKeySlotLabel(slot)).Inc()
	return proof, nil
}

// verifySignatureWithError is like VerifySignature but returns a descriptive error.
func (a *CallbackAuthenticator) verifySignatureWithError(method, uri string, payload []byte, signature string, now time.Time) error {
	return hmacauth.VerifyWithTime(a.secret, method, uri, payload, signature, a.maxAge, callbackClockSkewTolerance, now)
}
