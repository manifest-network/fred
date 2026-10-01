package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const callbackAdmissionClientIP = "192.0.2.5"

func newCallbackAdmissionServer(t *testing.T) *Server {
	t.Helper()
	verifier, _ := hmacauth.NewCallbackProofBoundary()
	server, err := NewServer(ServerConfig{
		ProviderUUID: "01234567-89ab-cdef-0123-456789abcdef",
		Bech32Prefix: "manifest", RateLimitRPS: 0.001, RateLimitBurst: 1,
		TrustedProxies: []string{"127.0.0.1/32"},
		CallbackHMACSecrets: map[backendidentity.ID]string{
			callbackKeyringID(t, callbackKeyringStorageA): callbackKeyringSecretA,
			callbackKeyringID(t, callbackKeyringStorageB): callbackKeyringSecretB,
		},
	}, ServerDeps{
		ChainClient: &mockChainClient{}, CallbackPublisher: &mockCallbackPublisher{},
		CallbackProofVerifier: verifier,
	})
	require.NoError(t, err)
	// Freeze the constructor-owned buckets at one token. No middleware or
	// limiter is replaced: every request goes through the production handler.
	require.NotNil(t, server.callbackIngressLimiter)
	server.callbackIngressLimiter.getVisitor(callbackAdmissionClientIP).SetLimit(0)
	server.callbackIngressLimiter.getVisitor(callbackAdmissionClientIP).SetBurst(1)
	require.NotNil(t, server.callbackRateLimiter)
	for _, storage := range []string{callbackKeyringStorageA, callbackKeyringStorageB} {
		server.callbackRateLimiter.get(storage).SetLimit(0)
		server.callbackRateLimiter.get(storage).SetBurst(1)
	}
	return server
}

func serveCallbackAdmission(server *Server, request *http.Request) *httptest.ResponseRecorder {
	request.RemoteAddr = "127.0.0.1:4321"
	request.Header.Set("X-Forwarded-For", callbackAdmissionClientIP)
	response := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(response, request)
	return response
}

func callbackAdmissionRequest(storage, secret string, sequence int) *http.Request {
	// Distinct signed bodies exercise the storage budget independently of any
	// duplicate-delivery handling.
	body := []byte(fmt.Sprintf(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"failed","error":"failure %d","backend_storage_id":%q}`, sequence, storage))
	return signedKeyringCallbackRequest(body, secret)
}

func TestNewServerCallbackAdmissionSurvivesTenantAndIngressExhaustion(t *testing.T) {
	server := newCallbackAdmissionServer(t)
	response := serveCallbackAdmission(server, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = serveCallbackAdmission(server, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusTooManyRequests, response.Code, "exhaust the real tenant middleware behind the same proxy/NAT")

	response = serveCallbackAdmission(server, httptest.NewRequest(http.MethodPost, "/callbacks/provision", nil))
	require.Equal(t, http.StatusUnauthorized, response.Code, "callback has an independent pre-auth bucket")
	response = serveCallbackAdmission(server, httptest.NewRequest(http.MethodPost, "/callbacks/provision", nil))
	require.Equal(t, http.StatusTooManyRequests, response.Code, "production route must retain pre-auth admission")
	require.NotEmpty(t, response.Header().Get("Retry-After"))

	response = serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretA, 1))
	require.Equal(t, http.StatusOK, response.Code, "valid HMAC bypasses the exhausted ingress bucket: %s", response.Body.String())
	response = serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretA, 2))
	require.Equal(t, http.StatusTooManyRequests, response.Code, "verified callbacks still spend their storage bucket")
	require.NotEmpty(t, response.Header().Get("Retry-After"))
	response = serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageB, callbackKeyringSecretB, 1))
	require.Equal(t, http.StatusOK, response.Code, "another storage lineage has its own budget: %s", response.Body.String())
}

func TestNewServerCallbackAdmissionDoesNotChargeClaimedStorageUntilAuthenticated(t *testing.T) {
	server := newCallbackAdmissionServer(t)
	for index := range 3 {
		response := serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretB, index))
		if index == 0 {
			require.Equal(t, http.StatusUnauthorized, response.Code)
		} else {
			require.Equal(t, http.StatusTooManyRequests, response.Code)
		}
	}
	response := serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretA, 4))
	require.Equal(t, http.StatusOK, response.Code, "forged storage selectors must not consume the victim's budget: %s", response.Body.String())
}

type callbackAdmissionBody struct {
	io.Reader
	read int
}

func (b *callbackAdmissionBody) Read(value []byte) (int, error) {
	n, err := b.Reader.Read(value)
	b.read += n
	return n, err
}

func (*callbackAdmissionBody) Close() error { return nil }

func TestNewServerCallbackAdmissionBoundsUntrustedStructureBeforeAndAfterExhaustion(t *testing.T) {
	server := newCallbackAdmissionServer(t)
	payload := []byte(`{"backend_storage_id":"` + callbackKeyringStorageA + `",` + strings.Repeat(`"junk":0,`, 100_000) + `"last":0}`)
	for index := range 2 {
		request := signedKeyringCallbackRequest(payload, callbackKeyringSecretB)
		body := &callbackAdmissionBody{Reader: bytes.NewReader(payload)}
		request.Body = body
		response := serveCallbackAdmission(server, request)
		if index == 0 {
			require.Equal(t, http.StatusUnauthorized, response.Code)
		} else {
			require.Equal(t, http.StatusTooManyRequests, response.Code)
		}
		require.LessOrEqual(t, body.read, 2048, "authentication must reject the structure before reading/decoding the complete body")
	}
	response := serveCallbackAdmission(server, callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretA, 1))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}
