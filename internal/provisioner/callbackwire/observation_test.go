package callbackwire

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	testSecret    = "callback-wire-test-secret-0123456789abcdef"
	testStorageID = "550e8400-e29b-41d4-a716-446655440000"
	testOperation = "2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a"
	testLifecycle = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
)

var callbackWireProofVerifier, _ = hmacauth.NewCallbackProofBoundary()

func verifiedCallback(t *testing.T, uri string, body []byte, route string) hmacauth.VerifiedRequest {
	t.Helper()
	now := time.Unix(1700000000, 0)
	signature := hmacauth.SignWithTime(testSecret, http.MethodPost, uri, body, now)
	proof, err := callbackWireProofVerifier.VerifyRoutedWithTime(
		testSecret, http.MethodPost, uri, body, signature, route,
		"/callbacks/provision", 5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)
	return proof
}

func TestDecodeVerifiedDerivesAuthorityOnlyFromSignedQuery(t *testing.T) {
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + testStorageID + `","operation_id":"` + testLifecycle + `","lifecycle_id":"` + testLifecycle + `"}`)
	proof := verifiedCallback(t, "/callbacks/provision?operation_id="+testOperation, body, testStorageID)

	observation, err := DecodeVerified(proof)
	require.NoError(t, err)
	assert.Equal(t, SelectorOperation, observation.Selector())
	assert.Equal(t, testOperation, observation.OperationID().String())
	assert.False(t, observation.LifecycleID().Valid())
}

func TestDecodeVerifiedRejectsForeignKeyRoute(t *testing.T) {
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + testStorageID + `"}`)
	proof := verifiedCallback(t, "/callbacks/provision", body, testLifecycle)

	_, err := DecodeVerified(proof)
	require.ErrorIs(t, err, ErrInvalidPayload)
}

func TestSelectUntrustedStorageRouteSharesStrictPayloadGrammar(t *testing.T) {
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + testStorageID + `"}`)
	storageID, err := SelectUntrustedStorageRoute(body)
	require.NoError(t, err)
	assert.Equal(t, testStorageID, storageID.String())

	_, err = SelectUntrustedStorageRoute([]byte(`{"backend_storage_id":"` + testStorageID + `","Backend_Storage_ID":"` + testStorageID + `"}`))
	require.ErrorContains(t, err, "ambiguous field")
	_, err = SelectUntrustedStorageRoute([]byte(`{"backend_storage_id":"` + testStorageID + `"}{}`))
	require.ErrorContains(t, err, "trailing JSON data")
}

func TestCallbackProofCannotBeMintedForAnotherPath(t *testing.T) {
	now := time.Unix(1700000000, 0)
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success"}`)
	uri := "/callbacks/maintenance?operation_id=" + testOperation
	signature := hmacauth.SignWithTime(testSecret, http.MethodPost, uri, body, now)
	_, err := callbackWireProofVerifier.VerifyRoutedWithTime(
		testSecret, http.MethodPost, uri, body, signature, "",
		"/callbacks/provision", 5*time.Minute, time.Minute, now,
	)
	require.Error(t, err)
}
