package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
)

const callbackKeyringPreviousB = "backend-b-old-secret-0123456789ab"

// rotatingCallbackKeyring is backend A with one key and backend B mid-rotation:
// its new key current, its old key previous.
func rotatingCallbackKeyring(t *testing.T) *CallbackKeyringAuthenticator {
	t.Helper()
	keysA, err := hmacauth.NewVerifyKeys(callbackKeyringSecretA, "")
	require.NoError(t, err)
	keysB, err := hmacauth.NewVerifyKeys(callbackKeyringSecretB, callbackKeyringPreviousB)
	require.NoError(t, err)
	verifier, _ := hmacauth.NewCallbackProofBoundary()
	keyring, err := NewCallbackKeyringAuthenticator(map[backendidentity.ID]CallbackKey{
		callbackKeyringID(t, callbackKeyringStorageA): {Backend: "rotation-a", Keys: keysA},
		callbackKeyringID(t, callbackKeyringStorageB): {Backend: "rotation-b", Keys: keysB},
	}, verifier)
	require.NoError(t, err)
	return keyring
}

func callbackBodyFor(storage string) []byte {
	return []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + storage + `"}`)
}

// Not parallel: these read process-global counters.
func TestCallbackKeyringAcceptsAPreviousKeyAndCountsItsSlot(t *testing.T) {
	keyring := rotatingCallbackKeyring(t)
	current := metrics.APICallbackSignatureKeyTotal.WithLabelValues("rotation-b", metrics.CallbackKeySlotCurrent)
	previous := metrics.APICallbackSignatureKeyTotal.WithLabelValues("rotation-b", metrics.CallbackKeySlotPrevious)
	currentBefore, previousBefore := promtestutil.ToFloat64(current), promtestutil.ToFloat64(previous)

	body := callbackBodyFor(callbackKeyringStorageB)
	_, err := keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(body, callbackKeyringPreviousB))
	require.NoError(t, err, "a callback the backend still signs with its old key verifies")
	assert.Equal(t, previousBefore+1, promtestutil.ToFloat64(previous))
	assert.Equal(t, currentBefore, promtestutil.ToFloat64(current))

	_, err = keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(body, callbackKeyringSecretB))
	require.NoError(t, err)
	assert.Equal(t, currentBefore+1, promtestutil.ToFloat64(current))

	_, err = keyring.VerifyCallbackEvidence(signedKeyringCallbackRequest(callbackBodyFor(callbackKeyringStorageA), callbackKeyringPreviousB))
	require.Error(t, err, "backend B's previous key never authenticates backend A")

	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.APICallbackPreviousKeyConfigured.WithLabelValues("rotation-b")))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.APICallbackPreviousKeyConfigured.WithLabelValues("rotation-a")))
}

func TestCallbackKeyringCountsEachRefusalByReason(t *testing.T) {
	keyring := rotatingCallbackKeyring(t)
	bodyB := callbackBodyFor(callbackKeyringStorageB)
	unknownStorage := callbackBodyFor(callbackKeyringStorageC)
	expired := httptest.NewRequest(http.MethodPost, "https://fred.example.test/callbacks/provision", bytes.NewReader(bodyB))
	expired.Header.Set(hmacauth.SignatureHeader,
		hmacauth.SignWithTime(callbackKeyringSecretB, http.MethodPost, "/callbacks/provision", bodyB, time.Now().Add(-time.Hour)))
	missing := httptest.NewRequest(http.MethodPost, "https://fred.example.test/callbacks/provision", bytes.NewReader(bodyB))
	malformed := httptest.NewRequest(http.MethodPost, "https://fred.example.test/callbacks/provision", bytes.NewReader(bodyB))
	malformed.Header.Set(hmacauth.SignatureHeader, "not-a-signature")

	for reason, request := range map[string]*http.Request{
		metrics.CallbackAuthFailureMissing:        missing,
		metrics.CallbackAuthFailureFormat:         malformed,
		metrics.CallbackAuthFailureExpired:        expired,
		metrics.CallbackAuthFailureMismatch:       signedKeyringCallbackRequest(bodyB, callbackKeyringSecretA),
		metrics.CallbackAuthFailureUnknownStorage: signedKeyringCallbackRequest(unknownStorage, callbackKeyringSecretA),
	} {
		counter := metrics.APICallbackAuthFailuresTotal.WithLabelValues(reason)
		before := promtestutil.ToFloat64(counter)
		_, err := keyring.VerifyCallbackEvidence(request)
		require.Error(t, err, reason)
		assert.Equal(t, before+1, promtestutil.ToFloat64(counter), reason)
	}
}
