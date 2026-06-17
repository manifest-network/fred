package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

// retainedProvisionServer returns an httptest server whose GET
// /provisions/{lease} responds with a retained ProvisionInfo for matchLease and
// 404 (ErrNotProvisioned) otherwise. The Tenant field crosses the wire
// (json:"tenant,omitempty") so the authz fallback can read it.
func retainedProvisionServer(t *testing.T, matchLease, tenant string, retainedUntil time.Time) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/provisions/"+matchLease {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.ProvisionInfo{
			LeaseUUID:     matchLease,
			ProviderUUID:  testutil.ValidUUID2,
			Status:        backend.ProvisionStatusRetained,
			CreatedAt:     retainedUntil.Add(-90 * 24 * time.Hour),
			RetainedUntil: retainedUntil,
			Tenant:        tenant,
			Items: []backend.LeaseItem{
				{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// notProvisionedServer always 404s (a backend that does not hold the lease).
func notProvisionedServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// erroringBackendServer always 500s — a transiently-unhealthy backend whose
// GetProvision returns a non-ErrNotProvisioned error.
func erroringBackendServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func httpBackend(t *testing.T, name, url string) backend.Backend {
	t.Helper()
	return backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    name,
		BaseURL: url,
		Timeout: 5 * time.Second,
	})
}

// TestGetLeaseStatus_Retained_NonActiveLease verifies (#3) that a non-ACTIVE
// chain lease whose data was retained surfaces provision_status=retained plus
// retained_until + items + restore_hint — the former ACTIVE-only gate is lifted.
func TestGetLeaseStatus_Retained_NonActiveLease(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(60 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_CLOSED, // non-ACTIVE
				}, nil
			}
			return nil, nil
		},
	}

	srv := retainedProvisionServer(t, leaseUUID, kp.Address, retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseStatusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "retained", resp.ProvisionStatus)
	assert.Equal(t, retainedUntil.Format(time.RFC3339), resp.RetainedUntil)
	assert.NotEmpty(t, resp.RestoreHint)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "web", resp.Items[0].ServiceName)
	assert.Equal(t, "docker-micro", resp.Items[0].SKU)
	assert.Equal(t, 2, resp.Items[0].Quantity)
}

// TestGetLeaseProvision_Retained_WithinGraceWindow verifies (#3) that GET
// /provision returns status=retained (not 404) for a retained lease.
func TestGetLeaseProvision_Retained_WithinGraceWindow(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(60 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_CLOSED}, nil
			}
			return nil, nil
		},
	}
	srv := retainedProvisionServer(t, leaseUUID, kp.Address, retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}
	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.GetLeaseProvision(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseProvisionResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "retained", resp.Status)
	assert.Equal(t, retainedUntil.Format(time.RFC3339), resp.RetainedUntil)
	assert.NotEmpty(t, resp.RestoreHint)
	require.Len(t, resp.Items, 1)
}

// TestGetLeaseStatus_ChainPruned_AuthzFallback verifies (#5) the closed-lease
// authz fallback: when GetLease returns nil, the owner is authorized via the
// retained record's Tenant, and a cross-tenant caller is rejected (404).
func TestGetLeaseStatus_ChainPruned_AuthzFallback(t *testing.T) {
	owner := testutil.NewTestKeyPair("owner-tenant")
	attacker := testutil.NewTestKeyPair("attacker-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)

	// Chain has pruned the closed lease: GetLease returns nil for everyone.
	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, _ string) (*billingtypes.Lease, error) { return nil, nil },
	}
	srv := retainedProvisionServer(t, leaseUUID, owner.Address, retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	t.Run("owner_authorized_via_retained_tenant", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(owner, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var resp LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "retained", resp.ProvisionStatus)
		assert.Equal(t, owner.Address, resp.Tenant, "response tenant is the authenticated caller, never leaked from the record")
		assert.Equal(t, retainedUntil.Format(time.RFC3339), resp.RetainedUntil)
	})

	t.Run("cross_tenant_rejected", func(t *testing.T) {
		// The attacker presents a validly-signed token for THEIR tenant but the
		// same lease UUID. The retained record is owned by `owner`, so the tenant
		// binding fails and the request is rejected as not-found.
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(attacker, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code, "cross-tenant caller must not be authorized against another tenant's retained record")
	})
}

// TestGetLeaseProvision_ChainPruned_AuthzFallback mirrors the status fallback for
// GET /provision.
func TestGetLeaseProvision_ChainPruned_AuthzFallback(t *testing.T) {
	owner := testutil.NewTestKeyPair("owner-tenant")
	attacker := testutil.NewTestKeyPair("attacker-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{getLeaseFunc: func(_ context.Context, _ string) (*billingtypes.Lease, error) { return nil, nil }}
	srv := retainedProvisionServer(t, leaseUUID, owner.Address, retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	t.Run("owner_authorized", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(owner, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)
		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var resp LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "retained", resp.Status)
	})

	t.Run("cross_tenant_rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(attacker, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)
		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestGetLeaseProvision_ChainPruned_BackendError_Returns404 pins the asymmetry
// fix (FIX 2): a chain-pruned caller (not yet authorized) hitting a transiently
// erroring backend must get 404, NOT 500 — surfacing a 500 would leak
// backend-health/existence to a caller who has not proven ownership. The 500
// path is reserved for the chain-present, already-authorized branch.
func TestGetLeaseProvision_ChainPruned_BackendError_Returns404(t *testing.T) {
	caller := testutil.NewTestKeyPair("some-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Chain pruned the lease (GetLease → nil); the single backend 500s.
	chainClient := &mockChainClient{getLeaseFunc: func(_ context.Context, _ string) (*billingtypes.Lease, error) { return nil, nil }}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", erroringBackendServer(t).URL), IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(caller, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseProvision(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code,
		"chain-pruned + erroring backend must 404, not leak a 500 to an unauthorized caller")
}

// TestGetLeaseProvision_ChainPresent_BackendError_Returns500 is the symmetric
// case: when the chain still vouches for the lease (caller already authorized),
// a genuine backend error surfaces as 500 rather than masking it as a 404.
func TestGetLeaseProvision_ChainPresent_BackendError_Returns500(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
			}
			return nil, nil
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", erroringBackendServer(t).URL), IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseProvision(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code,
		"chain-present + erroring backend must surface a 500 to the authorized owner")
}

// TestGetLeaseStatus_MultiBackendFanOut verifies (#4) that with two backends,
// only the holder returns retained; the fan-out finds it while the non-holder
// returns ErrNotProvisioned and is skipped. This is the bug a single statically-
// routed pick would cause: 404-ing a genuinely retained lease.
func TestGetLeaseStatus_MultiBackendFanOut(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(45 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			// Closed lease with no items → ExtractRoutingSKU == "" → fan out over Backends().
			if uuid == leaseUUID {
				return &billingtypes.Lease{Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_CLOSED}, nil
			}
			return nil, nil
		},
	}

	holder := retainedProvisionServer(t, leaseUUID, kp.Address, retainedUntil)
	nonHolder := notProvisionedServer(t)
	// Register the non-holder FIRST so the fan-out must skip it and continue.
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: httpBackend(t, "non-holder", nonHolder.URL), IsDefault: true},
			{Backend: httpBackend(t, "holder", holder.URL)},
		},
	})
	require.NoError(t, err)

	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}
	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseStatusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "retained", resp.ProvisionStatus, "fan-out must find the holder past the non-holder")
}
