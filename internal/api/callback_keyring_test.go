package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	callbackKeyringStorageA = "550e8400-e29b-41d4-a716-446655440000"
	callbackKeyringStorageB = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	callbackKeyringStorageC = "d9428888-122b-41e1-b85c-61c67afba0c6"
	callbackKeyringSecretA  = "backend-a-secret-0123456789abcdef"
	callbackKeyringSecretB  = "backend-b-secret-0123456789abcdef"
)

func callbackKeyringID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func testCallbackKeyring(t *testing.T) *CallbackKeyringAuthenticator {
	t.Helper()
	keyring, err := NewCallbackKeyringAuthenticator(map[backendidentity.ID]string{
		callbackKeyringID(t, callbackKeyringStorageA): callbackKeyringSecretA,
		callbackKeyringID(t, callbackKeyringStorageB): callbackKeyringSecretB,
	})
	require.NoError(t, err)
	return keyring
}

func signedKeyringCallbackRequest(body []byte, secret string) *http.Request {
	request := httptest.NewRequest(
		http.MethodPost, "https://fred.example.test/callbacks/provision", bytes.NewReader(body),
	)
	request.Header.Set(
		hmacauth.SignatureHeader,
		hmacauth.Sign(secret, request.Method, request.URL.RequestURI(), body),
	)
	return request
}

func TestCallbackKeyringAuthenticatorSelectsExactStorageKey(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `","backend":"backend-a"}`)

	callback, err := keyring.VerifyCallbackRequest(
		signedKeyringCallbackRequest(body, callbackKeyringSecretA),
	)
	require.NoError(t, err)
	assert.Equal(t, backend.CallbackPayload{
		LeaseUUID:        "d144291f-a36f-47a4-8ccf-48afe590e29d",
		Status:           backend.CallbackStatusSuccess,
		BackendStorageID: callbackKeyringStorageA,
		Backend:          "backend-a",
	}, callback)
}

func TestCallbackKeyringAuthenticatorRejectsCrossBackendForgery(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	bodyForA := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"deprovisioned","backend_storage_id":"` + callbackKeyringStorageA + `"}`)
	bodyForB := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"deprovisioned","backend_storage_id":"` + callbackKeyringStorageB + `"}`)

	_, err := keyring.VerifyCallbackRequest(
		signedKeyringCallbackRequest(bodyForB, callbackKeyringSecretA),
	)
	require.Error(t, err, "backend A's key must not authenticate storage B")
	_, err = keyring.VerifyCallbackRequest(
		signedKeyringCallbackRequest(bodyForA, callbackKeyringSecretB),
	)
	require.Error(t, err, "backend B's key must not authenticate storage A")
}

func TestCallbackKeyringAuthenticatorRejectsUnselectableStorageIdentity(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	tests := []struct {
		name      string
		storageID string
		want      string
	}{
		{name: "missing", want: "backend storage ID"},
		{name: "malformed", storageID: "not-a-uuid", want: "backend storage ID"},
		{name: "noncanonical", storageID: "550E8400-E29B-41D4-A716-446655440000", want: "backend storage ID"},
		{name: "unknown", storageID: callbackKeyringStorageC, want: "is not configured"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + test.storageID + `"}`)
			_, err := keyring.VerifyCallbackRequest(
				signedKeyringCallbackRequest(body, callbackKeyringSecretA),
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestCallbackKeyringAuthenticatorRejectsAmbiguousCallbackJSON(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "duplicate storage selector",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `","backend_storage_id":"` + callbackKeyringStorageB + `"}`,
			want: "duplicate field",
		},
		{
			name: "escaped duplicate storage selector",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `","backend_storage_\u0069d":"` + callbackKeyringStorageB + `"}`,
			want: "duplicate field",
		},
		{
			name: "case-ambiguous storage selector",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","BACKEND_STORAGE_ID":"` + callbackKeyringStorageA + `"}`,
			want: "ambiguous field",
		},
		{
			name: "duplicate application field",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","status":"failed","backend_storage_id":"` + callbackKeyringStorageA + `"}`,
			want: "duplicate field",
		},
		{
			name: "duplicate unknown field",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `","future":1,"future":2}`,
			want: "duplicate field",
		},
		{
			name: "malformed object",
			body: `{"lease_uuid":`,
			want: "decode callback field",
		},
		{
			name: "trailing object",
			body: `{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `"}{}`,
			want: "trailing JSON data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := []byte(test.body)
			_, err := keyring.VerifyCallbackRequest(
				signedKeyringCallbackRequest(body, callbackKeyringSecretA),
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestCallbackKeyringAuthenticatorConstructionIsClosed(t *testing.T) {
	t.Parallel()
	idA := callbackKeyringID(t, callbackKeyringStorageA)
	idB := callbackKeyringID(t, callbackKeyringStorageB)

	_, err := NewCallbackKeyringAuthenticator(nil)
	require.ErrorContains(t, err, "keyring is required")
	_, err = NewCallbackKeyringAuthenticator(map[backendidentity.ID]string{{}: callbackKeyringSecretA})
	require.ErrorContains(t, err, "invalid backend storage identity")
	_, err = NewCallbackKeyringAuthenticator(map[backendidentity.ID]string{idA: "short"})
	require.ErrorContains(t, err, "at least")
	_, err = NewCallbackKeyringAuthenticator(map[backendidentity.ID]string{
		idA: callbackKeyringSecretA,
		idB: callbackKeyringSecretA,
	})
	require.ErrorContains(t, err, "duplicates storage")

	configured := map[backendidentity.ID]string{idA: callbackKeyringSecretA}
	keyring, err := NewCallbackKeyringAuthenticator(configured)
	require.NoError(t, err)
	configured[idA] = callbackKeyringSecretB
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `"}`)
	_, err = keyring.VerifyCallbackRequest(signedKeyringCallbackRequest(body, callbackKeyringSecretA))
	require.NoError(t, err, "construction must detach the caller's mutable map")
}

func TestNewServerRejectsAmbiguousCallbackAuthenticationModes(t *testing.T) {
	t.Parallel()
	server, err := NewServer(ServerConfig{
		CallbackSecret: testCallbackSecret,
		CallbackHMACSecrets: map[backendidentity.ID]string{
			callbackKeyringID(t, callbackKeyringStorageA): callbackKeyringSecretA,
		},
	}, ServerDeps{})
	assert.Nil(t, server)
	require.ErrorContains(t, err, "cannot be combined")
}

func TestNewServerConstructsStorageIdentityKeyring(t *testing.T) {
	t.Parallel()
	server, err := NewServer(ServerConfig{
		CallbackHMACSecrets: map[backendidentity.ID]string{
			callbackKeyringID(t, callbackKeyringStorageA): callbackKeyringSecretA,
			callbackKeyringID(t, callbackKeyringStorageB): callbackKeyringSecretB,
		},
	}, ServerDeps{})
	require.NoError(t, err)
	require.IsType(t, &CallbackKeyringAuthenticator{}, server.callbackAuthenticator)

	bodyForB := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageB + `"}`)
	_, err = server.callbackAuthenticator.VerifyCallbackRequest(
		signedKeyringCallbackRequest(bodyForB, callbackKeyringSecretB),
	)
	require.NoError(t, err)
	_, err = server.callbackAuthenticator.VerifyCallbackRequest(
		signedKeyringCallbackRequest(bodyForB, callbackKeyringSecretA),
	)
	require.Error(t, err, "backend A's key must not authenticate storage B through server wiring")
}

func TestHandleProvisionCallbackUsesKeyringDecodedPayload(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	publisher := &capturingCallbackPublisher{}
	server := &Server{callbackPublisher: publisher, callbackAuthenticator: keyring}
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `","backend":"backend-a","operation_id":"6ba7b811-9dad-41d1-80b4-00c04fd430c8"}`)
	request := httptest.NewRequest(
		http.MethodPost,
		"/callbacks/provision?operation_id=d9428888-122b-41e1-b85c-61c67afba0c6",
		bytes.NewReader(body),
	)
	request.Header.Set(
		hmacauth.SignatureHeader,
		hmacauth.Sign(callbackKeyringSecretA, request.Method, request.URL.RequestURI(), body),
	)
	response := httptest.NewRecorder()

	server.handleProvisionCallback(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	require.True(t, publisher.called)
	assert.Equal(t, callbackKeyringStorageA, publisher.callback.BackendStorageID)
	assert.Equal(t, "backend-a", publisher.callback.Backend)
	assert.Equal(t, "d9428888-122b-41e1-b85c-61c67afba0c6", publisher.callback.OperationID,
		"authenticated URL authority must overwrite body metadata")
}

func TestHandleProvisionCallbackRejectsCrossBackendKeyBeforePublish(t *testing.T) {
	t.Parallel()
	keyring := testCallbackKeyring(t)
	publisher := &capturingCallbackPublisher{}
	server := &Server{callbackPublisher: publisher, callbackAuthenticator: keyring}
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageB + `"}`)
	request := signedKeyringCallbackRequest(body, callbackKeyringSecretA)
	response := httptest.NewRecorder()

	server.handleProvisionCallback(response, request)

	assert.Equal(t, http.StatusUnauthorized, response.Code)
	assert.False(t, publisher.called)
}
