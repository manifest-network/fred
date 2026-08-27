package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

const canonicalTestLifecycleID = "123e4567-e89b-42d3-a456-426614174000"

func signedLifecycleCallbackRequest(
	t *testing.T,
	auth *CallbackAuthenticator,
	requestURI string,
	body string,
) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, requestURI, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		CallbackSignatureHeader,
		auth.ComputeSignature(req.Method, req.URL.RequestURI(), []byte(body)),
	)
	return req
}

func TestHandleProvisionCallback_LifecycleCapabilityComesOnlyFromAuthenticatedURL(t *testing.T) {
	tests := []struct {
		name            string
		requestURI      string
		wantOperationID string
		wantLifecycleID string
	}{
		{
			name:            "canonical lifecycle capability is injected",
			requestURI:      "/callbacks/provision?trace=keep&lifecycle_id=" + canonicalTestLifecycleID,
			wantLifecycleID: canonicalTestLifecycleID,
		},
		{
			name:       "tokenless URL clears body supplied authority",
			requestURI: "/callbacks/provision?trace=keep",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := newTestCallbackAuthenticator(t, testCallbackSecret)
			publisher := &capturingCallbackPublisher{}
			server := &Server{
				callbackPublisher:     publisher,
				callbackAuthenticator: auth,
			}
			body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"failed",` +
				`"operation_id":"d9428888-122b-41e1-b85c-61c67afba0c6",` +
				`"lifecycle_id":"550e8400-e29b-41d4-a716-446655440000"}`
			req := signedLifecycleCallbackRequest(t, auth, test.requestURI, body)

			recorder := httptest.NewRecorder()
			server.handleProvisionCallback(recorder, req)

			assert.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.True(t, publisher.called)
			assert.Equal(t, test.wantOperationID, publisher.callback.OperationID)
			assert.Equal(t, test.wantLifecycleID, publisher.callback.LifecycleID)
		})
	}
}

func TestHandleProvisionCallback_RejectsInvalidLifecycleCapability(t *testing.T) {
	const otherLifecycleID = "d9428888-122b-41e1-b85c-61c67afba0c6"
	tests := []struct {
		name      string
		query     string
		wantError string
	}{
		{
			name:      "duplicate different lifecycle IDs",
			query:     "lifecycle_id=" + canonicalTestLifecycleID + "&lifecycle_id=" + otherLifecycleID,
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name:      "duplicate identical lifecycle IDs",
			query:     "lifecycle_id=" + canonicalTestLifecycleID + "&lifecycle_id=" + canonicalTestLifecycleID,
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name:      "encoded duplicate lifecycle key",
			query:     "lifecycle_id=" + canonicalTestLifecycleID + "&lifecycle%5Fid=" + otherLifecycleID,
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name:      "malformed lifecycle ID",
			query:     "lifecycle_id=not-a-uuid",
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name:      "non-v4 lifecycle ID",
			query:     "lifecycle_id=6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name:      "uppercase lifecycle ID",
			query:     "lifecycle_id=123E4567-E89B-42D3-A456-426614174000",
			wantError: "lifecycle_id must be a single canonical UUIDv4",
		},
		{
			name: "operation and lifecycle capabilities together",
			query: "operation_id=" + otherLifecycleID +
				"&lifecycle_id=" + canonicalTestLifecycleID,
			wantError: "callback URL must carry exactly one capability kind",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := newTestCallbackAuthenticator(t, testCallbackSecret)
			publisher := &capturingCallbackPublisher{}
			server := &Server{
				callbackPublisher:     publisher,
				callbackAuthenticator: auth,
			}
			body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"failed"}`
			req := signedLifecycleCallbackRequest(
				t,
				auth,
				"/callbacks/provision?"+test.query,
				body,
			)

			recorder := httptest.NewRecorder()
			server.handleProvisionCallback(recorder, req)

			assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			assertErrorBody(t, recorder, test.wantError)
			assert.False(t, publisher.called, "invalid callback authority must fail before publication")
		})
	}
}

func TestHandleProvisionCallback_RejectsMalformedRawCapabilityQuery(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	publisher := &capturingCallbackPublisher{}
	server := &Server{
		callbackPublisher:     publisher,
		callbackAuthenticator: auth,
	}
	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"failed"}`
	req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req.URL.RawQuery = "lifecycle_id=%zz"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		CallbackSignatureHeader,
		auth.ComputeSignature(req.Method, req.URL.RequestURI(), []byte(body)),
	)

	recorder := httptest.NewRecorder()
	server.handleProvisionCallback(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	assertErrorBody(t, recorder, "callback query is malformed")
	assert.False(t, publisher.called)
}
