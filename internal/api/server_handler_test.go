package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

// errCallbackPublisher is a CallbackPublisher that always returns an error.
type errCallbackPublisher struct{ err error }

func (p *errCallbackPublisher) PublishCallback(context.Context, backend.CallbackPayload) error {
	return p.err
}

// capturingCallbackPublisher records the last published callback.
type capturingCallbackPublisher struct {
	called   bool
	callback backend.CallbackPayload
}

func (p *capturingCallbackPublisher) PublishCallback(_ context.Context, cb backend.CallbackPayload) error {
	p.called = true
	p.callback = cb
	return nil
}

// signedRequest creates a POST request with a valid HMAC signature.
func signedRequest(t *testing.T, auth *CallbackAuthenticator, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	sig := auth.ComputeSignature(req.Method, req.URL.RequestURI(), []byte(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(CallbackSignatureHeader, sig)
	return req
}

func TestHandleProvisionCallback_Success(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success",` +
		`"operation_id":"d9428888-122b-41e1-b85c-61c67afba0c6"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusOK, rr.Code)
	require.True(t, pub.called)
	assert.Equal(t, testutil.ValidUUID1, pub.callback.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, pub.callback.Status)
	assert.Empty(t, pub.callback.OperationID, "JSON metadata cannot manufacture callback authority")
}

func TestHandleProvisionCallback_PropagatesAuthenticatedOperationID(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}
	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success",` +
		`"operation_id":"d9428888-122b-41e1-b85c-61c67afba0c6"}`
	req := httptest.NewRequest(http.MethodPost,
		"/callbacks/provision?operation_id=123e4567-e89b-42d3-a456-426614174000", strings.NewReader(body))
	req.Header.Set(CallbackSignatureHeader,
		auth.ComputeSignature(req.Method, req.URL.RequestURI(), []byte(body)))

	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	require.True(t, pub.called)
	assert.Equal(t, "123e4567-e89b-42d3-a456-426614174000", pub.callback.OperationID)
}

func TestHandleProvisionCallback_RejectsInvalidAuthenticatedOperationID(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "empty", query: "operation_id="},
		{name: "malformed", query: "operation_id=not-a-uuid"},
		{name: "uppercase non-canonical", query: "operation_id=123E4567-E89B-42D3-A456-426614174000"},
		{name: "non-v4", query: "operation_id=6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{name: "duplicate", query: "operation_id=123e4567-e89b-42d3-a456-426614174000&operation_id=d9428888-122b-41e1-b85c-61c67afba0c6"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := newTestCallbackAuthenticator(t, testCallbackSecret)
			pub := &capturingCallbackPublisher{}
			srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}
			body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
			req := httptest.NewRequest(http.MethodPost,
				"/callbacks/provision?"+tt.query, strings.NewReader(body))
			req.Header.Set(CallbackSignatureHeader,
				auth.ComputeSignature(req.Method, req.URL.RequestURI(), []byte(body)))

			rr := httptest.NewRecorder()
			srv.handleProvisionCallback(rr, req)

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assertErrorBody(t, rr, "operation_id must be a single canonical UUIDv4")
			assert.False(t, pub.called, "an invalid operation ID must fail before publication")
		})
	}
}

func TestHandleProvisionCallback_SuccessFailedStatus(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"failed"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusOK, rr.Code)
	require.True(t, pub.called)
	assert.Equal(t, backend.CallbackStatusFailed, pub.callback.Status)
}

func TestHandleProvisionCallback_NilPublisher(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	srv := &Server{callbackPublisher: nil, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assertErrorBody(t, rr, errMsgServiceNotConfigured)
}

func TestHandleProvisionCallback_NilAuthenticator(t *testing.T) {
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: nil}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.False(t, pub.called)
	assertErrorBody(t, rr, errMsgServiceNotConfigured)
}

func TestHandleProvisionCallback_InvalidSignature(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(CallbackSignatureHeader, "t=0,sha256=0000000000000000000000000000000000000000000000000000000000000000")

	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.False(t, pub.called)
}

func TestHandleProvisionCallback_InvalidJSON(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `not json at all`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.False(t, pub.called)
	assertErrorBody(t, rr, "invalid request body")
}

func TestHandleProvisionCallback_MissingLeaseUUID(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.False(t, pub.called)
	assertErrorBody(t, rr, "lease_uuid is required")
}

func TestHandleProvisionCallback_InvalidUUIDFormat(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.InvalidUUID1 + `","status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.False(t, pub.called)
	assertErrorBody(t, rr, "lease_uuid must be a valid UUID")
}

func TestHandleProvisionCallback_InvalidStatus(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"unknown"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.False(t, pub.called)
	assertErrorBody(t, rr, "status must be 'success', 'failed', or 'deprovisioned'")
}

// TestHandleProvisionCallback_AcceptsDeprovisioned verifies that the HTTP
// validator lets the new CallbackStatusDeprovisioned reach the callback
// application rather than returning 400. Without this, every deprovisioned
// callback fails at the API boundary.
func TestHandleProvisionCallback_AcceptsDeprovisioned(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"deprovisioned"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, pub.called)
}

func TestHandleProvisionCallback_PublishError(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &errCallbackPublisher{err: errors.New("broker down")}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assertErrorBody(t, rr, errMsgServiceUnavailable)
}

func TestHandleProvisionCallback_WithErrorField(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"failed","error":"container exited"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusOK, rr.Code)
	require.True(t, pub.called)
	assert.Equal(t, "container exited", pub.callback.Error)
}

// assertErrorBody checks that the response body contains the expected error message.
func assertErrorBody(t *testing.T, rr *httptest.ResponseRecorder, wantMsg string) {
	t.Helper()
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, wantMsg, resp.Error)
}
