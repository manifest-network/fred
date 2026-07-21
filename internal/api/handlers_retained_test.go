package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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
// (json:"tenant,omitempty") so the authz fallback can read it. Partition is left
// empty (omitempty absent), so every existing caller's response is byte-identical
// to before partitioning; retainedProvisionServerWithPartition sets it.
func retainedProvisionServer(t *testing.T, matchLease, tenant string, retainedUntil time.Time) *httptest.Server {
	t.Helper()
	return retainedProvisionServerWithPartition(t, matchLease, tenant, "", retainedUntil)
}

// retainedProvisionServerWithPartition is retainedProvisionServer with an
// explicit retention partition on the retained ProvisionInfo, so tests can prove
// the partition flows through to the owner-only retained API responses.
func retainedProvisionServerWithPartition(t *testing.T, matchLease, tenant, partition string, retainedUntil time.Time) *httptest.Server {
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
			Partition:     partition,
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

// TestRetainedResponses_Partition proves the retention partition flows to the
// owner-only retained responses on BOTH endpoints (/status via
// applyRetentionFields, /provision via its retained branch) when set, and is
// omitted (omitempty) when the record has no partition — so a non-participant's
// retained response is byte-identical to before partitioning. It also implicitly
// re-affirms the owner-only exposure: this response is served only after the
// caller's token authorizes them as the lease owner.
func TestRetainedResponses_Partition(t *testing.T) {
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

	// serve issues an owner-authenticated GET to the given endpoint against a
	// backend whose retained ProvisionInfo carries `partition`, returning the raw
	// response body.
	serve := func(t *testing.T, partition, endpoint string) string {
		t.Helper()
		srv := retainedProvisionServerWithPartition(t, leaseUUID, kp.Address, partition, retainedUntil)
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
		})
		require.NoError(t, err)
		h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+endpoint, nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)
		rec := httptest.NewRecorder()
		if endpoint == "/status" {
			h.GetLeaseStatus(rec, req)
		} else {
			h.GetLeaseProvision(rec, req)
		}
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		return rec.Body.String()
	}

	t.Run("present_status", func(t *testing.T) {
		assert.Contains(t, serve(t, "cust-a", "/status"), `"partition":"cust-a"`)
	})
	t.Run("present_provision", func(t *testing.T) {
		assert.Contains(t, serve(t, "cust-a", "/provision"), `"partition":"cust-a"`)
	})
	t.Run("absent_status", func(t *testing.T) {
		assert.NotContains(t, serve(t, "", "/status"), `"partition"`,
			"omitempty: a non-partitioned retained record's response must not carry the field")
	})
	t.Run("absent_provision", func(t *testing.T) {
		assert.NotContains(t, serve(t, "", "/provision"), `"partition"`,
			"omitempty: a non-partitioned retained record's response must not carry the field")
	})
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

// TestAuthorizeRetained_EmptyTenantNeverAuthorizes directly exercises the
// isolation-boundary reject branch (GAP 2a): authorizeRetained must NEVER
// authorize when the caller tenant OR the retained record's tenant is empty —
// even if both happen to be the empty string. This is the branch the
// handler-level happy-path tests don't reach.
func TestAuthorizeRetained_EmptyTenantNeverAuthorizes(t *testing.T) {
	retained := func(tenant string) *backend.ProvisionInfo {
		return &backend.ProvisionInfo{Status: backend.ProvisionStatusRetained, Tenant: tenant}
	}
	cases := []struct {
		name         string
		info         *backend.ProvisionInfo
		callerTenant string
		want         bool
	}{
		{"nil info", nil, "manifest1abc", false},
		{"non-retained status", &backend.ProvisionInfo{Status: backend.ProvisionStatusReady, Tenant: "manifest1abc"}, "manifest1abc", false},
		{"empty record tenant", retained(""), "manifest1abc", false},
		{"empty caller tenant", retained("manifest1abc"), "", false},
		{"both empty", retained(""), "", false},
		{"mismatched tenants", retained("manifest1owner"), "manifest1other", false},
		{"matching non-empty tenants", retained("manifest1abc"), "manifest1abc", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, authorizeRetained(tc.info, tc.callerTenant))
		})
	}
}

// TestGetLeaseStatus_ChainPruned_EmptyRecordTenant_Returns404 is the handler-level
// counterpart (GAP 2a): a chain-pruned lease whose retained record carries an
// EMPTY tenant must NOT authorize the caller (404), never binding on "" == "".
func TestGetLeaseStatus_ChainPruned_EmptyRecordTenant_Returns404(t *testing.T) {
	caller := testutil.NewTestKeyPair("some-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{getLeaseFunc: func(_ context.Context, _ string) (*billingtypes.Lease, error) { return nil, nil }}
	// Retained record with an EMPTY Tenant (e.g. a corrupt/legacy record).
	srv := retainedProvisionServer(t, leaseUUID, "", retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: httpBackend(t, "b1", srv.URL), IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{client: chainClient, backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(caller, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code,
		"an empty retained-record tenant must never authorize a caller (no empty-tenant binding)")
}

// TestGetLeaseStatus_Retained_ExpiredLease verifies the EXPIRED-state authz
// variant (GAP 2b): a chain lease in EXPIRED state (not just CLOSED/pruned) whose
// data was retained → the owner is authorized and GET /status returns retained.
func TestGetLeaseStatus_Retained_ExpiredLease(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_EXPIRED, // expired, not closed/pruned
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
	assert.Equal(t, "LEASE_STATE_EXPIRED", resp.State)
	assert.Equal(t, "retained", resp.ProvisionStatus, "an EXPIRED lease with retained data must surface retained")
	assert.Equal(t, retainedUntil.Format(time.RFC3339), resp.RetainedUntil)
	require.NotEmpty(t, resp.Items)
}

// TestFindProvision_StampsBackendName: ProvisionInfo.BackendName is json:"-" so
// HTTP backends leave it empty on the wire; both the placement fast-path and the
// fan-out must stamp the answering backend so downstream logging identifies it.
func TestFindProvision_StampsBackendName(t *testing.T) {
	leaseUUID := testutil.ValidUUID1
	placed := readyProvisionServer(t, leaseUUID)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: httpBackend(t, "placed", placed.URL), IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Fast path (placement hit).
	hFast := &Handlers{
		backendRouter:   router,
		placementLookup: &mockPlacementLookup{getFunc: func(_ string) string { return "placed" }},
	}
	info, err := hFast.findProvision(context.Background(), leaseUUID, "")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "placed", info.BackendName, "fast-path must stamp the answering backend")

	// Fan-out (no placement).
	hFan := &Handlers{backendRouter: router}
	info, err = hFan.findProvisionAcrossBackends(context.Background(), leaseUUID, "")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "placed", info.BackendName, "fan-out must stamp the answering backend")
}

// readyProvisionServer serves a ready (live, ACTIVE) ProvisionInfo for matchLease.
func readyProvisionServer(t *testing.T, matchLease string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/provisions/"+matchLease {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.ProvisionInfo{
			LeaseUUID:    matchLease,
			ProviderUUID: testutil.ValidUUID2,
			Status:       backend.ProvisionStatusReady,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// countingNotProvisionedServer 404s every request and increments hits so a test
// can assert the backend was (or was NOT) queried.
func countingNotProvisionedServer(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestGetLeaseStatus_ActiveLease_PlacementFastPath verifies FIX A: for an ACTIVE
// lease WITH a placement record, findProvision takes the O(1) placement
// fast-path — only the placed backend's GetProvision is invoked; the OTHER
// backend is NOT queried (no full fan-out on the read-heavy common case).
func TestGetLeaseStatus_ActiveLease_PlacementFastPath(t *testing.T) {
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

	placed := readyProvisionServer(t, leaseUUID)
	var otherHits atomic.Int64
	other := countingNotProvisionedServer(t, &otherHits)

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			// "other" is the default/first backend — a naive fan-out over Backends()
			// would query it; the placement fast-path must avoid that.
			{Backend: httpBackend(t, "other", other.URL), IsDefault: true},
			{Backend: httpBackend(t, "placed", placed.URL)},
		},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{getFunc: func(_ string) string { return "placed" }}
	h := &Handlers{client: chainClient, backendRouter: router, placementLookup: placement, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseStatusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "ready", resp.ProvisionStatus, "placement fast-path must resolve the placed backend's live status")
	assert.Equal(t, int64(0), otherHits.Load(), "the non-placed backend must NOT be queried when placement hits (no fan-out)")
}

// TestGetLeaseProvision_ActiveLease_PlacementFastPath is the GET /provision
// counterpart of the placement fast-path.
func TestGetLeaseProvision_ActiveLease_PlacementFastPath(t *testing.T) {
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

	placed := readyProvisionServer(t, leaseUUID)
	var otherHits atomic.Int64
	other := countingNotProvisionedServer(t, &otherHits)

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: httpBackend(t, "other", other.URL), IsDefault: true},
			{Backend: httpBackend(t, "placed", placed.URL)},
		},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{getFunc: func(_ string) string { return "placed" }}
	h := &Handlers{client: chainClient, backendRouter: router, placementLookup: placement, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseProvision(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseProvisionResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "ready", resp.Status)
	assert.Equal(t, int64(0), otherHits.Load(), "the non-placed backend must NOT be queried when placement hits (no fan-out)")
}

// TestGetLeaseStatus_StalePlacement_FallsBackToFanOut verifies the fast-path
// degrades gracefully: when the placed backend returns ErrNotProvisioned (stale
// placement — e.g. a closed/retained lease whose placement wasn't cleaned), the
// fan-out still finds the holder.
func TestGetLeaseStatus_StalePlacement_FallsBackToFanOut(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	retainedUntil := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_CLOSED}, nil
			}
			return nil, nil
		},
	}

	// Placement points at "stale", which no longer holds the lease (404). The
	// holder "holder" is found by the fan-out fallback.
	stale := notProvisionedServer(t)
	holder := retainedProvisionServer(t, leaseUUID, kp.Address, retainedUntil)
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: httpBackend(t, "stale", stale.URL), IsDefault: true},
			{Backend: httpBackend(t, "holder", holder.URL)},
		},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{getFunc: func(_ string) string { return "stale" }}
	h := &Handlers{client: chainClient, backendRouter: router, placementLookup: placement, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp LeaseStatusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "retained", resp.ProvisionStatus, "stale placement must fall back to the fan-out and find the holder")
}

// TestGetLeaseProvision_PlacedBackendError_NotInFanOut_Returns500 verifies FIX 1:
// findProvision must NOT swallow a genuine placed-backend error. The placed
// backend (resolved via GetBackendByName) returns a real 500, and it is NOT in
// the lease SKU's RouteAll set — so the fan-out queries a different backend that
// only 404s and finds nothing. The placed-backend error must be surfaced
// (handler → 500), not masked as (nil,nil) → 404.
func TestGetLeaseProvision_PlacedBackendError_NotInFanOut_Returns500(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Chain lease carries an item whose SKU routes ONLY to "fanout" — so the
	// erroring "placed" backend (a different SKU) is excluded from RouteAll(sku)
	// and is never re-queried by the fan-out.
	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
					Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-fanout"}},
				}, nil
			}
			return nil, nil
		},
	}

	placed := erroringBackendServer(t) // genuine 500 on GetProvision
	var fanoutHits atomic.Int64
	fanout := countingNotProvisionedServer(t, &fanoutHits) // 404s; finds nothing

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			// "placed" matches a DIFFERENT sku → excluded from RouteAll("sku-fanout").
			{Backend: httpBackend(t, "placed", placed.URL), Match: backend.MatchCriteria{SKUs: []string{"sku-placed"}}},
			// "fanout" is the only RouteAll("sku-fanout") candidate.
			{Backend: httpBackend(t, "fanout", fanout.URL), Match: backend.MatchCriteria{SKUs: []string{"sku-fanout"}}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{getFunc: func(_ string) string { return "placed" }}
	h := &Handlers{client: chainClient, backendRouter: router, placementLookup: placement, providerUUID: providerUUID, bech32Prefix: "manifest"}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()
	h.GetLeaseProvision(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code,
		"a genuine placed-backend error must surface as 500 when the fan-out finds nothing, not be masked as 404")
	// Sanity: the fan-out ran (and only 404'd) — the placed backend was not in it.
	assert.Equal(t, int64(1), fanoutHits.Load(), "the fan-out candidate must have been queried exactly once")
}
