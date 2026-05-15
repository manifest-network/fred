package main

// ENG-191 QA — HTTP-server-level replay-attack tests against the k3s-backend.
//
// These spin up the k3s-backend HTTP handler via httptest.NewServer (a real
// listening socket, not just handler.ServeHTTP into a recorder) and assert
// that the exact attacker scenarios from the team brief are rejected:
//
//   - State-change replay: a captured signed POST /provision request, replayed
//     verbatim (same Authorization header, same body) against POST /deprovision,
//     must return 401 — the /deprovision handler reads lease_uuid and would
//     happily tear down the lease if the signature verified.
//
//   - Confidentiality replay: a captured signed GET /info/<uuid> (empty body),
//     replayed against GET /logs/<uuid> (also empty body), must return 401 —
//     pre-fix both share the same canonical string and the leak would succeed.
//
// These are companion to TestHMACMiddleware_RejectsCrossEndpointReplay /
// ..._RejectsCrossPathReplay (handler-level via httptest.NewRecorder) but
// exercise the live HTTP stack end-to-end via net/http.Client.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/hmacauth"
)

// TestQA_HMACReplay_CrossEndpoint_POST_RejectsProvisionAsDeprovision is the
// headline state-change replay test from the team brief. A signature captured
// on POST /provision must NOT verify when the same X-Fred-Signature header is
// replayed against POST /deprovision with an identical body.
func TestQA_HMACReplay_CrossEndpoint_POST_RejectsProvisionAsDeprovision(t *testing.T) {
	srv := httptest.NewServer(newTestHandler())
	defer srv.Close()

	body := []byte(`{"lease_uuid":"qa-cross-endpoint","callback_url":"http://fred.test/callbacks/provision","items":[{"sku":"k3s-micro","quantity":1}]}`)

	// Capture: sign a POST /provision request shape.
	srcReq := httptest.NewRequest("POST", "/provision", bytes.NewReader(body))
	capturedSig := hmacauth.Sign(testSecret, srcReq.Method, srcReq.URL.RequestURI(), body)

	// Replay: send a real HTTP request to /deprovision carrying the
	// captured signature header and the same body.
	replayReq, err := http.NewRequest("POST", srv.URL+"/deprovision", bytes.NewReader(body))
	require.NoError(t, err)
	replayReq.Header.Set(hmacauth.SignatureHeader, capturedSig)
	replayReq.Header.Set("Content-Type", "application/json")

	resp, err := srv.Client().Do(replayReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"captured /provision signature MUST NOT verify when replayed against /deprovision (body=%q)", respBody)
	assert.Contains(t, string(respBody), "invalid signature",
		"response body must indicate signature rejection, not a downstream handler error")
}

// TestQA_HMACReplay_CrossEndpoint_GET_RejectsInfoAsLogs is the headline
// confidentiality replay test from the team brief. A signature captured on
// GET /info/<uuid> must NOT verify when replayed against GET /logs/<uuid>.
// Both have empty bodies, so without method+URI binding their canonical
// strings would be identical.
func TestQA_HMACReplay_CrossEndpoint_GET_RejectsInfoAsLogs(t *testing.T) {
	srv := httptest.NewServer(newTestHandler())
	defer srv.Close()

	const leaseID = "qa-cross-resource"

	// Capture: sign a GET /info/<id> request shape with empty body.
	srcReq := httptest.NewRequest("GET", "/info/"+leaseID, nil)
	capturedSig := hmacauth.Sign(testSecret, srcReq.Method, srcReq.URL.RequestURI(), nil)

	// Replay: send a GET /logs/<id> request with the captured signature.
	replayReq, err := http.NewRequest("GET", srv.URL+"/logs/"+leaseID, nil)
	require.NoError(t, err)
	replayReq.Header.Set(hmacauth.SignatureHeader, capturedSig)

	resp, err := srv.Client().Do(replayReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"captured /info signature MUST NOT verify when replayed against /logs (body=%q)", respBody)
	assert.Contains(t, string(respBody), "invalid signature",
		"response body must indicate signature rejection")
}

// TestQA_HMACReplay_QueryString_Tenant_Swap exercises the query-string
// binding property at the HTTP level. The exact request line is what gets
// signed; flipping a query value must invalidate the signature even when
// path and method match.
func TestQA_HMACReplay_QueryString_Tenant_Swap(t *testing.T) {
	srv := httptest.NewServer(newTestHandler())
	defer srv.Close()

	// Capture: sign GET /logs/qa-lease?tail=10 (tail is the closest real
	// query-bearing endpoint shape in this backend).
	srcReq := httptest.NewRequest("GET", "/logs/qa-lease?tail=10", nil)
	capturedSig := hmacauth.Sign(testSecret, srcReq.Method, srcReq.URL.RequestURI(), nil)

	// Replay: same path but different query value.
	replayReq, err := http.NewRequest("GET", srv.URL+"/logs/qa-lease?tail=10000", nil)
	require.NoError(t, err)
	replayReq.Header.Set(hmacauth.SignatureHeader, capturedSig)

	resp, err := srv.Client().Do(replayReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
		"captured tail=10 signature MUST NOT verify when replayed with tail=10000 (body=%q)", respBody)
	assert.Contains(t, string(respBody), "invalid signature")
}

// TestQA_HMACRoundTrip_LiveHTTP confirms the new format still completes the
// happy path end-to-end against the live HTTP server. This is the regression
// guard: if the canonical-string contract drifts between sender and verifier,
// the round trip breaks.
func TestQA_HMACRoundTrip_LiveHTTP(t *testing.T) {
	srv := httptest.NewServer(newTestHandler())
	defer srv.Close()

	// GET /info/<lease> with a properly signed request — the middleware
	// must accept the signature. newTestHandler() backs the server with
	// nil backend, so a successful auth pass-through reaches the handler
	// which then panics on nil dereference. We catch the panic by
	// observing that the connection is closed (500-ish) — but we
	// specifically must NOT see 401.
	srcReq, err := http.NewRequest("GET", srv.URL+"/info/qa-round-trip", nil)
	require.NoError(t, err)
	// httptest.NewRequest builds a request with the same URL.RequestURI()
	// as the live client's *http.Request will have, so this signs over
	// the same canonical string the verifier will reconstruct.
	canonReq := httptest.NewRequest("GET", "/info/qa-round-trip", nil)
	srcReq.Header.Set(
		hmacauth.SignatureHeader,
		hmacauth.Sign(testSecret, canonReq.Method, canonReq.URL.RequestURI(), nil),
	)

	resp, err := srv.Client().Do(srcReq)
	// A panic in the handler closes the connection. Both "no error +
	// 500" and "transport error" are acceptable signals that we got past
	// the middleware. What is NOT acceptable is a clean 401.
	if err == nil {
		defer resp.Body.Close()
		assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode,
			"validly signed request was rejected as unauthorized — sender/verifier disagree on the canonical string")
	}
}
