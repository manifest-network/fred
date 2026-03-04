package api

import (
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

func (p *errCallbackPublisher) PublishCallback(backend.CallbackPayload) error { return p.err }

// capturingCallbackPublisher records the last published callback.
type capturingCallbackPublisher struct {
	called   bool
	callback backend.CallbackPayload
}

func (p *capturingCallbackPublisher) PublishCallback(cb backend.CallbackPayload) error {
	p.called = true
	p.callback = cb
	return nil
}

// signedRequest creates a POST request with a valid HMAC signature.
func signedRequest(t *testing.T, auth *CallbackAuthenticator, body string) *http.Request {
	t.Helper()
	sig := auth.ComputeSignature([]byte(body))
	req := httptest.NewRequest(http.MethodPost, "/callbacks/provision", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(CallbackSignatureHeader, sig)
	return req
}

func TestHandleProvisionCallback_Success(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &capturingCallbackPublisher{}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusOK, rr.Code)
	require.True(t, pub.called)
	assert.Equal(t, testutil.ValidUUID1, pub.callback.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, pub.callback.Status)
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
	assertErrorBody(t, rr, "status must be 'success' or 'failed'")
}

func TestHandleProvisionCallback_PublishError(t *testing.T) {
	auth := newTestCallbackAuthenticator(t, testCallbackSecret)
	pub := &errCallbackPublisher{err: errors.New("broker down")}
	srv := &Server{callbackPublisher: pub, callbackAuthenticator: auth}

	body := `{"lease_uuid":"` + testutil.ValidUUID1 + `","status":"success"}`
	rr := httptest.NewRecorder()
	srv.handleProvisionCallback(rr, signedRequest(t, auth, body))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assertErrorBody(t, rr, errMsgInternalServerError)
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
