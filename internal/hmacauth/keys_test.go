package hmacauth

import (
	"crypto/sha256"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	currentTestKey  = "current-key-that-is-at-least-32-bytes!!"
	rotationTestKey = "rotation-key-that-is-at-least-32-bytes!"
	foreignTestKey  = "foreign-key-that-is-at-least-32-bytes!!"
)

func rotatingKeys(t *testing.T) VerifyKeys {
	t.Helper()
	keys, err := NewVerifyKeys(currentTestKey, rotationTestKey)
	require.NoError(t, err)
	return keys
}

func TestVerifyKeysAcceptsEitherKeyAndReportsWhichMatched(t *testing.T) {
	keys := rotatingKeys(t)
	body := []byte(`{"lease_uuid":"abc"}`)
	for key, want := range map[string]KeySlot{currentTestKey: KeySlotCurrent, rotationTestKey: KeySlotRotation} {
		req := httptest.NewRequest("POST", "/provision?x=1", nil)
		slot, err := VerifyRequestKeys(keys, req, body, SignRequest(key, req, body), time.Minute)
		require.NoError(t, err)
		assert.Equal(t, want, slot)
	}

	req := httptest.NewRequest("POST", "/provision", nil)
	slot, err := VerifyRequestKeys(keys, req, body, SignRequest(foreignTestKey, req, body), time.Minute)
	assert.False(t, slot.Valid())
	assert.Equal(t, FailureMismatch, FailureReasonOf(err))
	assert.EqualError(t, err, "signature mismatch")

	single, err := NewVerifyKeys(currentTestKey, "")
	require.NoError(t, err)
	assert.False(t, single.HasRotation())
	_, err = VerifyRequestKeys(single, req, body, SignRequest(rotationTestKey, req, body), time.Minute)
	assert.Equal(t, FailureMismatch, FailureReasonOf(err), "without a rotation key only the current key verifies")
}

func TestVerifyKeysRejectsStaleAndMalformedSignaturesWhateverTheKey(t *testing.T) {
	keys := rotatingKeys(t)
	body := []byte(`{}`)
	now := time.Now()
	cases := map[string]struct {
		signature string
		reason    FailureReason
		message   string
	}{
		"expired": {SignWithTime(rotationTestKey, "POST", "/x", body, now.Add(-10*time.Minute)), FailureExpired, "signature expired"},
		"future":  {SignWithTime(rotationTestKey, "POST", "/x", body, now.Add(10*time.Minute)), FailureFuture, "signature timestamp too far in future"},
		"format":  {"sha256=deadbeef", FailureFormat, "invalid signature format"},
		"encoding": {
			strings.Replace(SignWithTime(rotationTestKey, "POST", "/x", body, now), "sha256=", "sha256=zz", 1),
			FailureFormat, "invalid signature encoding",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			envelope, err := parseEnvelope("POST", "/x", body, tc.signature, 5*time.Minute, time.Minute, now)
			require.Error(t, err)
			assert.Equal(t, tc.reason, FailureReasonOf(err))
			assert.ErrorContains(t, err, tc.message, "messages are unchanged from the untyped errors")
			assert.Nil(t, envelope.provided)
			_, matchErr := keys.match(nil, nil)
			assert.Error(t, matchErr)
		})
	}
}

func TestNewVerifyKeysRefusesWeakOrRedundantRotationKeys(t *testing.T) {
	_, err := NewVerifyKeys("short", "")
	assert.Error(t, err)
	_, err = NewVerifyKeys(currentTestKey, "short")
	assert.Error(t, err)
	_, err = NewVerifyKeys(currentTestKey, currentTestKey)
	assert.ErrorContains(t, err, "equivalent")
	_, err = NewVerifyKeys(currentTestKey, currentTestKey+"\x00")
	assert.ErrorContains(t, err, "equivalent", "a zero-padded copy is the same HMAC key")

	var zero VerifyKeys
	assert.False(t, zero.Valid())
	assert.False(t, zero.HasRotation())
	_, err = zero.match([]byte{1}, []byte{2})
	assert.Error(t, err)
}

func TestEquivalentFollowsHMACKeyEquivalence(t *testing.T) {
	body := []byte("payload")
	signed := SignWithTime(currentTestKey, "POST", "/x", body, time.Now())
	padded := currentTestKey + "\x00\x00"
	require.NoError(t, Verify(padded, "POST", "/x", body, signed, time.Minute),
		"HMAC really treats the zero-padded key as the same key")
	assert.True(t, Equivalent(currentTestKey, padded))

	long := strings.Repeat("k", 100)
	digest := sha256.Sum256([]byte(long))
	require.NoError(t, Verify(string(digest[:]), "POST", "/x", body,
		SignWithTime(long, "POST", "/x", body, time.Now()), time.Minute),
		"HMAC really treats a long key and its digest as the same key")
	assert.True(t, Equivalent(long, string(digest[:])))

	assert.False(t, Equivalent(currentTestKey, rotationTestKey))
}

func TestSharesKeyWithComparesEveryKeyByEquivalence(t *testing.T) {
	keys := rotatingKeys(t)
	other, err := NewVerifyKeys(foreignTestKey, rotationTestKey+"\x00")
	require.NoError(t, err)
	assert.True(t, keys.SharesKeyWith(other), "an equivalent rotation key is a shared key")
	assert.True(t, other.SharesKeyWith(keys))

	disjoint, err := NewVerifyKeys(foreignTestKey, "")
	require.NoError(t, err)
	assert.False(t, keys.SharesKeyWith(disjoint))
}

func TestKeyIDIdentifiesAKeyWithoutRevealingIt(t *testing.T) {
	id := KeyID(currentTestKey)
	assert.Len(t, id, 12)
	assert.Equal(t, id, KeyID(currentTestKey))
	assert.Equal(t, id, KeyID(currentTestKey+"\x00"), "equivalent keys share an ID")
	assert.NotEqual(t, id, KeyID(rotationTestKey))
	assert.NotContains(t, currentTestKey, id)

	keys := rotatingKeys(t)
	assert.Equal(t, id, keys.CurrentKeyID())
	rotationID, ok := keys.RotationKeyID()
	require.True(t, ok)
	assert.Equal(t, KeyID(rotationTestKey), rotationID)
}

func TestVerifyRoutedKeysMintsAProofBoundToItsRoute(t *testing.T) {
	verifier, consumer := NewCallbackProofBoundary()
	keys := rotatingKeys(t)
	body := []byte(`{"lease_uuid":"abc"}`)
	now := time.Now()
	const path = "/callbacks/provision"

	proof, slot, err := verifier.VerifyRoutedKeysWithTime(
		keys, "POST", path, body, SignWithTime(rotationTestKey, "POST", path, body, now),
		"route-a", path, 5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)
	assert.Equal(t, KeySlotRotation, slot)
	assert.True(t, consumer.Accepts(proof))
	assert.Equal(t, "route-a", proof.KeyRoute())
	assert.Equal(t, body, proof.Body())

	for name, call := range map[string]func() error{
		"method": func() error {
			_, _, err := verifier.VerifyRoutedKeysWithTime(keys, "GET", path, body,
				SignWithTime(currentTestKey, "GET", path, body, now), "r", path, time.Minute, time.Minute, now)
			return err
		},
		"path": func() error {
			_, _, err := verifier.VerifyRoutedKeysWithTime(keys, "POST", "/elsewhere", body,
				SignWithTime(currentTestKey, "POST", "/elsewhere", body, now), "r", path, time.Minute, time.Minute, now)
			return err
		},
	} {
		assert.Equal(t, FailureFormat, FailureReasonOf(call()), name)
	}

	var unbound CallbackProofVerifier
	_, slot, err = unbound.VerifyRoutedKeysWithTime(keys, "POST", path, body, "", "r", path, time.Minute, time.Minute, now)
	assert.Error(t, err)
	assert.False(t, slot.Valid())
}

func TestMatchCallbackKeysChecksLikeTheVerifierWithoutAProof(t *testing.T) {
	keys := rotatingKeys(t)
	body := []byte(`{"lease_uuid":"abc"}`)
	now := time.Now()
	const path = "/callbacks/provision"

	require.NoError(t, MatchCallbackKeys(keys, "POST", path, body,
		SignWithTime(rotationTestKey, "POST", path, body, now), path, 5*time.Minute, time.Minute, now))
	require.NoError(t, MatchCallbackKeys(keys, "POST", path, body,
		SignWithTime(currentTestKey, "POST", path, body, now), path, 5*time.Minute, time.Minute, now))

	for name, reason := range map[string]struct {
		err  error
		want FailureReason
	}{
		"mismatch": {MatchCallbackKeys(keys, "POST", path, body,
			SignWithTime("another-key-0123456789abcdef0123", "POST", path, body, now), path, time.Minute, time.Minute, now), FailureMismatch},
		"expired": {MatchCallbackKeys(keys, "POST", path, body,
			SignWithTime(currentTestKey, "POST", path, body, now.Add(-time.Hour)), path, time.Minute, time.Minute, now), FailureExpired},
		"path": {MatchCallbackKeys(keys, "POST", "/elsewhere", body,
			SignWithTime(currentTestKey, "POST", "/elsewhere", body, now), path, time.Minute, time.Minute, now), FailureFormat},
		"method": {MatchCallbackKeys(keys, "GET", path, body,
			SignWithTime(currentTestKey, "GET", path, body, now), path, time.Minute, time.Minute, now), FailureFormat},
	} {
		assert.Equal(t, reason.want, FailureReasonOf(reason.err), name)
	}
}

func TestFailureReasonsAreClosed(t *testing.T) {
	assert.Equal(t, failureReasonInvalid, FailureReasonOf(errors.New("not a verification failure")))
	assert.Empty(t, failureReasonInvalid.String())
	labels := map[string]struct{}{}
	for _, reason := range []FailureReason{FailureFormat, FailureExpired, FailureFuture, FailureMismatch} {
		assert.NotEmpty(t, reason.String())
		assert.NotContains(t, labels, reason.String())
		labels[reason.String()] = struct{}{}
	}
	assert.False(t, keySlotInvalid.Valid())
}
