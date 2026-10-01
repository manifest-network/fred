package api

import (
	"errors"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
)

// fencedCallbackKeyring is live backend A beside fenced backend B.
func fencedCallbackKeyring(t *testing.T) *CallbackKeyringAuthenticator {
	t.Helper()
	keysA, err := hmacauth.NewVerifyKeys(callbackKeyringSecretA, "")
	require.NoError(t, err)
	keysB, err := hmacauth.NewVerifyKeys(callbackKeyringSecretB, "")
	require.NoError(t, err)
	verifier, _ := hmacauth.NewCallbackProofBoundary()
	keyring, err := NewCallbackKeyringAuthenticator(map[backendidentity.ID]CallbackKey{
		callbackKeyringID(t, callbackKeyringStorageA): {Backend: "live-a", Keys: keysA},
		callbackKeyringID(t, callbackKeyringStorageB): {Backend: "fenced-b", Keys: keysB, Fenced: true},
	}, verifier)
	require.NoError(t, err)
	return keyring
}

// Not parallel: these read process-global counters.
func TestCallbackKeyringRefusesAFencedBackendsOwnCallback(t *testing.T) {
	keyring := fencedCallbackKeyring(t)
	fenced := metrics.APICallbackAuthFailuresTotal.WithLabelValues(metrics.CallbackAuthFailureFenced)
	mismatch := metrics.APICallbackAuthFailuresTotal.WithLabelValues(metrics.CallbackAuthFailureMismatch)
	verified := metrics.APICallbackSignatureKeyTotal.WithLabelValues("fenced-b", metrics.CallbackKeySlotCurrent)
	fencedBefore, mismatchBefore, verifiedBefore :=
		promtestutil.ToFloat64(fenced), promtestutil.ToFloat64(mismatch), promtestutil.ToFloat64(verified)

	bodyB := callbackBodyFor(callbackKeyringStorageB)
	proof, err := keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(bodyB, callbackKeyringSecretB))
	require.ErrorIs(t, err, errCallbackBackendFenced, "a correctly signed fenced callback is refused")
	require.False(t, proof.Valid(), "a fenced key never yields a proof")
	require.Equal(t, fencedBefore+1, promtestutil.ToFloat64(fenced))
	require.Equal(t, verifiedBefore, promtestutil.ToFloat64(verified))

	_, err = keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(bodyB, callbackKeyringSecretA))
	require.Error(t, err)
	require.False(t, errors.Is(err, errCallbackBackendFenced),
		"a forgery that names the fenced storage is a mismatch, not the fenced backend")
	require.Equal(t, mismatchBefore+1, promtestutil.ToFloat64(mismatch))
	require.Equal(t, fencedBefore+1, promtestutil.ToFloat64(fenced))

	bodyA := callbackBodyFor(callbackKeyringStorageA)
	proof, err = keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(bodyA, callbackKeyringSecretA))
	require.NoError(t, err, "the live backend is unaffected")
	require.True(t, proof.Valid())
}

func TestCallbackKeyringRejectsFencedKeyReuseAndAnAllFencedKeyring(t *testing.T) {
	keysA, err := hmacauth.NewVerifyKeys(callbackKeyringSecretA, "")
	require.NoError(t, err)
	verifier, _ := hmacauth.NewCallbackProofBoundary()

	// Map order is random; repeat so both visiting orders are checked.
	for range 16 {
		_, err = NewCallbackKeyringAuthenticator(map[backendidentity.ID]CallbackKey{
			callbackKeyringID(t, callbackKeyringStorageA): {Backend: "live-a", Keys: keysA},
			callbackKeyringID(t, callbackKeyringStorageB): {Backend: "fenced-b", Keys: keysA, Fenced: true},
		}, verifier)
		require.ErrorContains(t, err, "duplicates", "a fenced key shared with a live backend is refused")
	}

	_, err = NewCallbackKeyringAuthenticator(map[backendidentity.ID]CallbackKey{
		callbackKeyringID(t, callbackKeyringStorageA): {Backend: "fenced-a", Keys: keysA, Fenced: true},
	}, verifier)
	require.ErrorContains(t, err, "no unfenced backend")
}
