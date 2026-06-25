package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	testUUID1 = "01234567-89ab-cdef-0123-456789abcdef"
	testUUID2 = "abcdef01-2345-6789-abcd-ef0123456789"
	testUUID3 = "12345678-1234-1234-1234-123456789abc"
)

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
		backend:      mb,
		callbackURLs: make(map[string]string),
	}
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
	const secret = "test-secret-that-is-at-least-32-chars!"
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
