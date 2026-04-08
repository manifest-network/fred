package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
