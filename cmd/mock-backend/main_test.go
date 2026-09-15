package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	testUUID1         = "01234567-89ab-cdef-0123-456789abcdef"
	testUUID2         = "abcdef01-2345-6789-abcd-ef0123456789"
	testUUID3         = "12345678-1234-1234-1234-123456789abc"
	testStorageID     = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	testBackendSecret = "test-secret-that-is-at-least-32-chars!"
)

func mustTestStorageID(t testing.TB) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(testStorageID)
	require.NoError(t, err)
	return id
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func signedTestRequest(method, target string, body []byte) *http.Request {
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	request.Header.Set(
		hmacauth.SignatureHeader,
		hmacauth.SignRequest(testBackendSecret, request, body),
	)
	return request
}

// newTestServer wires a real MockBackend behind a MockBackendServer for handler
// testing. Provisions are seeded via the backend's Provision API so they live in
// the same in-memory state the handler queries.
func newTestServer(t *testing.T, leases []string) *MockBackendServer {
	t.Helper()
	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "mock"})
	for _, uuid := range leases {
		require.NoError(t, mb.Provision(context.Background(), backend.ProvisionRequest{
			LeaseUUID:    uuid,
			ProviderUUID: "provider-1",
			Items:        []backend.LeaseItem{{SKU: "test-sku", Quantity: 1}},
		}))
	}
	return &MockBackendServer{
		backend:        mb,
		callbackSecret: testBackendSecret,
		name:           "mock",
		storageID:      mustTestStorageID(t),
		callbackURLs:   make(map[string]string),
	}
}

func TestMockBackend_HandlerBindsResponsesAndSideEffectsToStorageIdentity(t *testing.T) {
	srv := newTestServer(t, nil)
	handler := srv.Handler()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusOK, health.Code)
	assert.Equal(t, testStorageID, health.Header().Get(backendidentity.ResponseHeader))

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	body := []byte(`{"lease_uuid":"` + leaseUUID + `","tenant":"tenant","provider_uuid":"provider","items":[{"sku":"test","quantity":1}],"callback_url":"http://fred.local/callbacks/provision"}`)
	boundPath, err := backendidentity.BoundPath(srv.storageID, "/provision")
	require.NoError(t, err)
	boundPath += "?" + backendidentity.QueryParameter + "=" + testStorageID

	accepted := httptest.NewRecorder()
	handler.ServeHTTP(accepted, signedTestRequest(http.MethodPost, boundPath, body))
	require.Equal(t, http.StatusAccepted, accepted.Code, accepted.Body.String())
	assert.Equal(t, testStorageID, accepted.Header().Get(backendidentity.ResponseHeader))
	_, err = srv.backend.GetProvision(context.Background(), leaseUUID)
	require.NoError(t, err)

	const rejectedLease = "86cda7d7-ec70-4377-8a69-12a4a5ef4f46"
	rejectedBody := bytes.ReplaceAll(body, []byte(leaseUUID), []byte(rejectedLease))
	wrongPath := "/_fred/storage/550e8400-e29b-41d4-a716-446655440000/provision"
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, signedTestRequest(http.MethodPost, wrongPath, rejectedBody))
	require.Equal(t, http.StatusNotFound, rejected.Code)
	_, err = srv.backend.GetProvision(context.Background(), rejectedLease)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestMockBackend_HandlerRejectsMismatchedExpectedIdentityBeforeHandler(t *testing.T) {
	srv := newTestServer(t, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet,
		"/health?"+backendidentity.QueryParameter+"=550e8400-e29b-41d4-a716-446655440000", nil)

	srv.Handler().ServeHTTP(recorder, request)

	require.Equal(t, http.StatusConflict, recorder.Code)
	assert.Equal(t, testStorageID, recorder.Header().Get(backendidentity.ResponseHeader))
}

func TestMockBackend_HandlerAuthenticatesContractRoutesAndKeepsOperationalRoutesPublic(t *testing.T) {
	srv := newTestServer(t, nil)
	handler := srv.Handler()
	boundProvision, err := backendidentity.BoundPath(srv.storageID, "/provision")
	require.NoError(t, err)
	boundDeprovision, err := backendidentity.BoundPath(srv.storageID, "/deprovision")
	require.NoError(t, err)

	for _, route := range []struct {
		method string
		target string
	}{
		{http.MethodPost, "/provision"},
		{http.MethodPost, "/deprovision"},
		{http.MethodGet, "/info/" + testUUID1},
		{http.MethodGet, "/logs/" + testUUID1},
		{http.MethodGet, "/provisions"},
		{http.MethodGet, "/provisions/" + testUUID1},
		{http.MethodGet, "/retentions"},
		{http.MethodPost, boundProvision},
		{http.MethodPost, boundDeprovision},
	} {
		t.Run(route.method+" "+route.target, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(route.method, route.target, nil))

			assert.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
			assert.JSONEq(t, `{"error":"missing signature"}`, response.Body.String())
		})
	}

	for _, target := range []string{"/health", "/stats"} {
		t.Run("public "+target, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))

			assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
		})
	}

	t.Run("valid signed inventory request", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, signedTestRequest(http.MethodGet, "/provisions", nil))

		assert.Equal(t, http.StatusOK, response.Code, response.Body.String())
	})
}

func TestMockBackend_HandlerRejectsUnsignedMutationWithoutSideEffect(t *testing.T) {
	srv := newTestServer(t, nil)
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	body := []byte(`{"lease_uuid":"` + leaseUUID + `","tenant":"tenant","provider_uuid":"provider","items":[{"sku":"test","quantity":1}],"callback_url":"http://fred.local/callbacks/provision"}`)

	response := httptest.NewRecorder()
	srv.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/provision", bytes.NewReader(body)))

	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	_, err := srv.backend.GetProvision(context.Background(), leaseUUID)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestMockBackend_HandlerCapsAuthenticatedRequestBodies(t *testing.T) {
	srv := newTestServer(t, nil)
	body := bytes.Repeat([]byte{'x'}, int(mockBackendMaxRequestBodySize)+1)
	request := httptest.NewRequest(http.MethodPost, "/provision", bytes.NewReader(body))
	request.Header.Set(hmacauth.SignatureHeader, "present-but-invalid")
	response := httptest.NewRecorder()

	srv.Handler().ServeHTTP(response, request)

	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code, response.Body.String())
}

func TestMockBackendAddrDefaultsToLoopback(t *testing.T) {
	t.Setenv("MOCK_BACKEND_ADDR", "")
	assert.Equal(t, "127.0.0.1:9000", mockBackendAddr())

	t.Setenv("MOCK_BACKEND_ADDR", "[::1]:9001")
	assert.Equal(t, "[::1]:9001", mockBackendAddr())
}

func TestMockBackend_ProvisionRejectsUnusableCallbackAuthority(t *testing.T) {
	t.Parallel()

	for _, callbackURL := range []string{
		"https://:443/callbacks/provision",
		"https://./callbacks/provision",
		"https://fred.example:/callbacks/provision",
		"https://fred.example:0/callbacks/provision",
		"https://fred.example:65536/callbacks/provision",
		"https://user@fred.example/callbacks/provision",
		"https://fred.example/callbacks/provision#",
		"https://fred.example/callbacks/provision?",
		"https://fred.example/api/../callbacks/provision",
		"https://fred.example/api%2Fcallback",
		"https://fred.example/callbacks/provision?trace=a b",
	} {
		t.Run(callbackURL, func(t *testing.T) {
			t.Parallel()
			srv := newTestServer(t, nil)
			body, err := json.Marshal(backend.ProvisionRequest{
				LeaseUUID:    testUUID1,
				Tenant:       "tenant",
				ProviderUUID: "provider",
				Items:        []backend.LeaseItem{{SKU: "test", Quantity: 1}},
				CallbackURL:  callbackURL,
			})
			require.NoError(t, err)
			boundPath, err := backendidentity.BoundPath(srv.storageID, "/provision")
			require.NoError(t, err)
			boundPath += "?" + backendidentity.QueryParameter + "=" + testStorageID

			response := httptest.NewRecorder()
			srv.Handler().ServeHTTP(response, signedTestRequest(http.MethodPost, boundPath, body))

			assert.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		})
	}
}

func TestMockBackend_CallbackCapabilityNeverEntersLogs(t *testing.T) {
	const capability = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + capability

	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv := &MockBackendServer{
		name:      "mock",
		storageID: mustTestStorageID(t),
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("transport failed for %s", request.URL.String())
		})},
	}
	srv.sendCallback(callbackURL, backend.CallbackPayload{
		LeaseUUID: testUUID1,
		Status:    backend.CallbackStatusFailed,
	})

	assert.NotContains(t, output.String(), capability)
	assert.NotContains(t, output.String(), callbackURL)
}

func TestMockBackend_CallbackIncludesStorageIdentity(t *testing.T) {
	var received backend.CallbackPayload
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		defer request.Body.Close()
		require.NoError(t, json.NewDecoder(request.Body).Decode(&received))
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})}

	srv := newTestServer(t, nil)
	srv.httpClient = client
	srv.callbackSecret = testBackendSecret
	srv.callbackURLs[testUUID1] = "http://fred.local/callbacks/provision"

	srv.handleCallback(backend.CallbackPayload{
		LeaseUUID: testUUID1,
		Status:    backend.CallbackStatusSuccess,
	})

	assert.Equal(t, testStorageID, received.BackendStorageID)
	assert.Equal(t, "mock", received.Backend)
}

func TestMockBackend_HandleListProvisions_Filtered_OK(t *testing.T) {
	srv := newTestServer(t, []string{testUUID1, testUUID2, testUUID3})

	// Filtered request asking for two of the three.
	req := httptest.NewRequest("GET", "/provisions?lease_uuid="+testUUID1+"&lease_uuid="+testUUID3, nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp backend.ListProvisionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Provisions, 2)

	got := make(map[string]bool)
	for _, p := range resp.Provisions {
		got[p.LeaseUUID] = true
	}
	assert.True(t, got[testUUID1])
	assert.True(t, got[testUUID3])
	assert.False(t, got[testUUID2])
}

func TestMockBackend_HandleListProvisions_UnknownOmitted(t *testing.T) {
	srv := newTestServer(t, []string{testUUID1})

	req := httptest.NewRequest("GET", "/provisions?lease_uuid="+testUUID2, nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// Empty result must serialize as `[]` not `null`.
	assert.Contains(t, w.Body.String(), `"provisions":[]`)
}

func TestMockBackend_HandleListProvisions_RejectsBadUUID(t *testing.T) {
	srv := newTestServer(t, nil)

	req := httptest.NewRequest("GET", "/provisions?lease_uuid=not-a-uuid", nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid lease_uuid")
}

func TestMockBackend_HandleListProvisions_Unfiltered_StillWorks(t *testing.T) {
	// Confirm the existing reconciler path (no filter) still returns the full set.
	srv := newTestServer(t, []string{testUUID1, testUUID2})

	req := httptest.NewRequest("GET", "/provisions", nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp backend.ListProvisionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Len(t, resp.Provisions, 2)
}

func TestMockBackend_HandleListProvisions_Paginates(t *testing.T) {
	srv := newTestServer(t, []string{testUUID1, testUUID2, testUUID3})

	req := httptest.NewRequest("GET", "/provisions?limit=2", nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var p backend.ListProvisionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
	require.Len(t, p.Provisions, 2)
	assert.Equal(t, testUUID1, p.Provisions[0].LeaseUUID) // sorted: testUUID1 < testUUID3 < testUUID2
	assert.Equal(t, testUUID3, p.Provisions[1].LeaseUUID)
	assert.Equal(t, testUUID3, p.Continue)
}

func TestMockBackend_HandleListProvisions_MalformedContinueIs400(t *testing.T) {
	srv := newTestServer(t, []string{testUUID1})
	req := httptest.NewRequest("GET", "/provisions?limit=2&continue=not-a-uuid", nil)
	w := httptest.NewRecorder()
	srv.handleListProvisions(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// newTestServerWithRetentions wires a MockBackendServer whose backend reports
// the given leases as retained.
func newTestServerWithRetentions(t *testing.T, retained []string) *MockBackendServer {
	t.Helper()
	srv := newTestServer(t, nil)
	r := make([]backend.RetainedLease, 0, len(retained))
	for _, uuid := range retained {
		r = append(r, backend.RetainedLease{LeaseUUID: uuid})
	}
	srv.backend.SetRetentions(r)
	return srv
}

// The /retentions handler mirrors /provisions, and it is load-bearing in a way
// that is easy to lose: fred treats any non-200 here as a failure, so if this
// endpoint regresses the reconciler's retention sweep is permanently
// incomplete and the placement pruner silently short-circuits. Nothing else
// fails loudly when that happens, hence these tests.
func TestMockBackend_HandleListRetentions_OK(t *testing.T) {
	srv := newTestServerWithRetentions(t, []string{testUUID1, testUUID2})

	req := httptest.NewRequest("GET", "/retentions", nil)
	w := httptest.NewRecorder()
	srv.handleListRetentions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp backend.ListRetentionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Retentions, 2)
	assert.Empty(t, resp.Continue)
}

func TestMockBackend_HandleListRetentions_EmptySerializesAsArray(t *testing.T) {
	srv := newTestServerWithRetentions(t, nil)

	req := httptest.NewRequest("GET", "/retentions", nil)
	w := httptest.NewRecorder()
	srv.handleListRetentions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	// `null` would decode into a nil slice; the contract is an empty array.
	assert.Contains(t, w.Body.String(), `"retentions":[]`)
}

func TestMockBackend_HandleListRetentions_Paginates(t *testing.T) {
	srv := newTestServerWithRetentions(t, []string{testUUID1, testUUID2, testUUID3})

	req := httptest.NewRequest("GET", "/retentions?limit=2", nil)
	w := httptest.NewRecorder()
	srv.handleListRetentions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp backend.ListRetentionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Retentions, 2)
	// Sorted by lease UUID: testUUID1 < testUUID3 < testUUID2.
	assert.Equal(t, testUUID1, resp.Retentions[0].LeaseUUID)
	assert.Equal(t, testUUID3, resp.Retentions[1].LeaseUUID)
	assert.Equal(t, testUUID3, resp.Continue)
}

func TestMockBackend_HandleListRetentions_MalformedContinueIs400(t *testing.T) {
	srv := newTestServerWithRetentions(t, []string{testUUID1})

	req := httptest.NewRequest("GET", "/retentions?limit=2&continue=not-a-uuid", nil)
	w := httptest.NewRecorder()
	srv.handleListRetentions(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// With no snapshot configured the backend reports nil, and the handler must
// still answer 200 with a zero-valued snapshot. A 404 or a `null` body would be
// read by fred as a failed call rather than as "no usable load signal".
func TestMockBackend_HandleStats_NilSnapshotBecomesZeroValued(t *testing.T) {
	srv := newTestServer(t, nil)

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	srv.handleStats(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var stats backend.LoadStats
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &stats))
	assert.Zero(t, stats.TotalCPUCores)

	_, ok := stats.CPUAllocatedRatio()
	assert.False(t, ok, "a zero-valued snapshot must read as no usable load signal")
}

func TestMockBackend_HandleStats_ReturnsConfiguredSnapshot(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.backend.SetLoadStats(&backend.LoadStats{
		TotalCPUCores:     8,
		AllocatedCPUCores: 2,
		ActiveContainers:  3,
	})

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	srv.handleStats(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var stats backend.LoadStats
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &stats))
	assert.Equal(t, 8.0, stats.TotalCPUCores)
	assert.Equal(t, 2.0, stats.AllocatedCPUCores)
	assert.Equal(t, 3, stats.ActiveContainers)

	ratio, ok := stats.CPUAllocatedRatio()
	require.True(t, ok)
	assert.InDelta(t, 0.25, ratio, 1e-9)
}

func TestMockBackend_HandleStats_ErrorIs500(t *testing.T) {
	srv := newTestServer(t, nil)
	srv.backend.SetGetLoadStatsErr(errors.New("stats unavailable"))

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	srv.handleStats(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// TestComputeSignature_VerifiesAgainstHmacauth proves the standalone
// reference implementation in computeSignature produces signatures that
// the canonical internal/hmacauth.VerifyRequest accepts. Without this,
// mock-backend's deliberately-decoupled signer could silently drift
// from the canonical contract (e.g., wrong field separator, wrong body
// encoding) and only fail in production once it talks to a real Fred.
//
// The standalone implementation is intentional — it is the reference
// for external backend authors who cannot import internal/hmacauth.
// This test is the safety net that locks the two implementations to
// the same canonical string.
func TestComputeSignature_VerifiesAgainstHmacauth(t *testing.T) {
	const secret = testBackendSecret
	srv := &MockBackendServer{callbackSecret: secret}

	cases := []struct {
		name string
		// callbackURL is the URL the mock backend will POST to; computeSignature
		// is called with the request's method and URL.RequestURI() (i.e. path+query).
		callbackURL string
		body        []byte
	}{
		{"typical callback body", "http://fred.local/callbacks/provision", []byte(`{"lease_uuid":"abc","status":"success"}`)},
		{"empty body", "http://fred.local/callbacks/provision", nil},
		{"callback URL with query", "http://fred.local/callbacks/provision?retry=1", []byte(`{"lease_uuid":"abc"}`)},
		{"binary-safe body", "http://fred.local/callbacks/provision", []byte{0x00, 0x0A, 0xFF}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, tc.callbackURL, nil)
			require.NoError(t, err)

			sig := srv.computeSignature(req.Method, req.URL.RequestURI(), tc.body)
			require.NotEmpty(t, sig)

			// The canonical Fred-side verifier must accept it.
			assert.NoError(t,
				hmacauth.VerifyRequest(secret, req, tc.body, sig, 5*time.Minute),
				"standalone mock-backend signer drifted from the canonical contract")
		})
	}
}
