package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallbackBudgetIsIndependentOfTenantTrafficBehindSameNAT(t *testing.T) {
	server := &Server{
		callbackAuthenticator:  testCallbackKeyring(t),
		callbackIngressLimiter: NewRateLimiter(0.001, 1, nil),
		callbackPublisher:      &mockCallbackPublisher{},
		callbackRateLimiter:    newLimiterCache(maxVisitors, visitorTTL, 0.001, 1),
	}
	mux := http.NewServeMux()
	mux.Handle("POST /callbacks/provision", server.callbackPreauthBudget(http.HandlerFunc(server.handleProvisionCallback)))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := callbackIngressRateLimit(NewRateLimiter(0.001, 1, nil), mux)
	for range 100 {
		request := httptest.NewRequest(http.MethodGet, "/workloads", nil)
		request.RemoteAddr = "192.0.2.5:1234"
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `"}`)
	request := signedKeyringCallbackRequest(body, callbackKeyringSecretA)
	request.RemoteAddr = "192.0.2.5:4321"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

func TestCallbackStorageBudgetRequiresVerifiedKeyAndIsolatesBackends(t *testing.T) {
	server := &Server{
		callbackAuthenticator:  testCallbackKeyring(t),
		callbackIngressLimiter: NewRateLimiter(0.001, 1, nil),
		callbackPublisher:      &mockCallbackPublisher{},
		callbackRateLimiter:    newLimiterCache(maxVisitors, visitorTTL, 0.001, 1),
	}
	call := func(storage, secret string) *httptest.ResponseRecorder {
		body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + storage + `"}`)
		response := httptest.NewRecorder()
		server.handleProvisionCallback(response, signedKeyringCallbackRequest(body, secret))
		return response
	}
	for range 10 {
		require.Equal(t, http.StatusUnauthorized, call(callbackKeyringStorageA, callbackKeyringSecretB).Code)
	}
	require.Equal(t, http.StatusOK, call(callbackKeyringStorageA, callbackKeyringSecretA).Code)
	response := call(callbackKeyringStorageA, callbackKeyringSecretA)
	require.Equal(t, http.StatusTooManyRequests, response.Code)
	require.NotEmpty(t, response.Header().Get("Retry-After"))
	require.Equal(t, http.StatusOK, call(callbackKeyringStorageB, callbackKeyringSecretB).Code)
}

func TestExhaustedCallbackIngressStillAdmitsVerifiedBackend(t *testing.T) {
	server := &Server{
		callbackAuthenticator:  testCallbackKeyring(t),
		callbackPublisher:      &mockCallbackPublisher{},
		callbackIngressLimiter: NewRateLimiter(0.001, 1, nil),
		callbackRateLimiter:    newLimiterCache(maxVisitors, visitorTTL, 0.001, 1),
	}
	handler := server.callbackPreauthBudget(http.HandlerFunc(server.handleProvisionCallback))
	for i := range 10 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/callbacks/provision", nil))
		if i == 0 {
			require.Equal(t, http.StatusUnauthorized, response.Code)
		} else {
			require.Equal(t, http.StatusTooManyRequests, response.Code)
			require.NotEmpty(t, response.Header().Get("Retry-After"))
		}
	}
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"success","backend_storage_id":"` + callbackKeyringStorageA + `"}`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, signedKeyringCallbackRequest(body, callbackKeyringSecretA))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, signedKeyringCallbackRequest(body, callbackKeyringSecretA))
	require.Equal(t, http.StatusTooManyRequests, response.Code, "authenticated storage budget still applies")
}
