package hmacauth

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// KeySlot names which configured key verified a request. The zero value is
// invalid and never describes a successful verification.
type KeySlot uint8

const (
	keySlotInvalid KeySlot = iota
	// KeySlotCurrent is the key this side also signs with.
	KeySlotCurrent
	// KeySlotRotation is the one verify-only key a side may hold while its
	// shared key rotates.
	KeySlotRotation
)

// Valid reports whether slot names a configured key.
func (slot KeySlot) Valid() bool {
	return slot == KeySlotCurrent || slot == KeySlotRotation
}

// FailureReason is the closed cause of a failed verification. The zero value
// is invalid and never describes a failure.
type FailureReason uint8

const (
	failureReasonInvalid FailureReason = iota
	// FailureFormat is a malformed signature header or a request envelope that
	// does not match the endpoint.
	FailureFormat
	// FailureExpired is a timestamp older than the freshness window.
	FailureExpired
	// FailureFuture is a timestamp beyond the allowed clock skew.
	FailureFuture
	// FailureMismatch is a well-formed, fresh signature no configured key made.
	FailureMismatch
)

// String returns the reason's metric label; the invalid zero value has none.
func (reason FailureReason) String() string {
	switch reason {
	case FailureFormat:
		return "format"
	case FailureExpired:
		return "expired"
	case FailureFuture:
		return "future"
	case FailureMismatch:
		return "mismatch"
	default:
		return ""
	}
}

// VerificationError is a failed verification and its closed cause. Its message
// is the text the untyped verification errors carried, so callers and logs see
// the same diagnostics.
type VerificationError struct {
	reason  FailureReason
	message string
}

func (err *VerificationError) Error() string { return err.message }

// FailureReasonOf returns the closed cause of a verification failure, or the
// invalid zero value for any other error.
func FailureReasonOf(err error) FailureReason {
	var verification *VerificationError
	if errors.As(err, &verification) {
		return verification.reason
	}
	return failureReasonInvalid
}

func verificationFailure(reason FailureReason, format string, args ...any) error {
	return &VerificationError{reason: reason, message: fmt.Sprintf(format, args...)}
}

// VerifyKeys is the set of keys one side accepts: the key it signs with and,
// during a rotation, at most one verify-only key. It can verify but never
// sign, and it cannot hold a third key. The zero value is invalid.
type VerifyKeys struct {
	current  []byte
	rotation []byte
}

// NewVerifyKeys builds the keys one side accepts. rotation is optional; when
// set it must be as long as a signing key and must not be equivalent to
// current, since an equivalent key would add no new authority and hide a
// configuration mistake.
func NewVerifyKeys(current, rotation string) (VerifyKeys, error) {
	if len(current) < MinSecretLength {
		return VerifyKeys{}, fmt.Errorf("current HMAC key must be at least %d bytes, got %d", MinSecretLength, len(current))
	}
	keys := VerifyKeys{current: []byte(current)}
	if rotation == "" {
		return keys, nil
	}
	if len(rotation) < MinSecretLength {
		return VerifyKeys{}, fmt.Errorf("rotation HMAC key must be at least %d bytes, got %d", MinSecretLength, len(rotation))
	}
	if Equivalent(current, rotation) {
		return VerifyKeys{}, errors.New("rotation HMAC key is equivalent to the current key")
	}
	keys.rotation = []byte(rotation)
	return keys, nil
}

// Valid reports whether keys was built by NewVerifyKeys.
func (keys VerifyKeys) Valid() bool {
	return len(keys.current) >= MinSecretLength
}

// HasRotation reports whether a verify-only rotation key is configured.
func (keys VerifyKeys) HasRotation() bool {
	return keys.Valid() && keys.rotation != nil
}

// SharesKeyWith reports whether any key of keys is equivalent to any key of
// other. Two backends must never accept a common key.
func (keys VerifyKeys) SharesKeyWith(other VerifyKeys) bool {
	for _, mine := range [][]byte{keys.current, keys.rotation} {
		for _, theirs := range [][]byte{other.current, other.rotation} {
			if mine != nil && theirs != nil && equivalentKeys(mine, theirs) {
				return true
			}
		}
	}
	return false
}

// match reports which key made the provided MAC over canonical. When a
// rotation key is configured both MACs are always computed, so the time taken
// does not reveal which key matched.
func (keys VerifyKeys) match(provided, canonical []byte) (KeySlot, error) {
	if !keys.Valid() {
		return keySlotInvalid, errors.New("HMAC verification keys are unavailable")
	}
	currentMatches := hmac.Equal(provided, macOf(keys.current, canonical))
	rotationMatches := keys.rotation != nil && hmac.Equal(provided, macOf(keys.rotation, canonical))
	switch {
	case currentMatches:
		return KeySlotCurrent, nil
	case rotationMatches:
		return KeySlotRotation, nil
	default:
		return keySlotInvalid, verificationFailure(FailureMismatch, "signature mismatch")
	}
}

// VerifyRequestKeys verifies r against keys with a one-minute future clock
// skew and reports which key matched. The caller must read r.Body first and
// pass it.
func VerifyRequestKeys(
	keys VerifyKeys,
	r *http.Request,
	body []byte,
	signature string,
	maxAge time.Duration,
) (KeySlot, error) {
	envelope, err := parseEnvelope(r.Method, r.URL.RequestURI(), body, signature, maxAge, time.Minute, time.Now())
	if err != nil {
		return keySlotInvalid, err
	}
	return keys.match(envelope.provided, envelope.canonical)
}

// Key identity labels. Changing either changes every equivalence or key ID.
const (
	equivalenceLabel = "fred-hmac-key-equivalence/v1"
	keyIDLabel       = "fred-hmac-key-id/v1"
)

// keyFingerprint is HMAC-SHA256 keyed by key over label. HMAC treats keys that
// differ only by trailing zero bytes, and a key longer than the hash block and
// its SHA-256 digest, as the same key, and so does this fingerprint; a string
// comparison does not.
func keyFingerprint(key []byte, label string) []byte {
	return macOf(key, []byte(label))
}

func equivalentKeys(a, b []byte) bool {
	return hmac.Equal(keyFingerprint(a, equivalenceLabel), keyFingerprint(b, equivalenceLabel))
}

// Equivalent reports whether a and b authenticate exactly the same messages.
func Equivalent(a, b string) bool {
	return equivalentKeys([]byte(a), []byte(b))
}

// KeyID returns a short non-secret identifier for key, so operators can
// confirm two processes hold the same key without revealing it. Print it only
// on request: anyone holding an ID can test guesses of a weak key against it.
func KeyID(key string) string {
	return hex.EncodeToString(keyFingerprint([]byte(key), keyIDLabel)[:6])
}

// CurrentKeyID returns the KeyID of the signing key.
func (keys VerifyKeys) CurrentKeyID() string {
	return KeyID(string(keys.current))
}

// RotationKeyID returns the KeyID of the verify-only key, if configured.
func (keys VerifyKeys) RotationKeyID() (string, bool) {
	if !keys.HasRotation() {
		return "", false
	}
	return KeyID(string(keys.rotation)), true
}
