package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

const testBillingOnlySKU = "01a126cc-7edd-700d-9ac6-02ed34fa4bff"

func testBillingOnlyPolicy(t *testing.T) *placement.BillingPolicy {
	t.Helper()
	policy, err := placement.NewBillingPolicy([]string{testBillingOnlySKU})
	require.NoError(t, err)
	return policy
}

// assertBillingOnlyRefusal checks a refusal carries reason billing_only. The
// message is pinned too: clients released before the reason code match it.
func assertBillingOnlyRefusal(t *testing.T, rec *httptest.ResponseRecorder, message string) {
	t.Helper()
	assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
	var body ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, ErrorResponse{Error: message, Code: http.StatusBadRequest, Reason: "billing_only"}, body)
}

func TestNewServer_RequiresBillingPolicy(t *testing.T) {
	// No implicit default: a provider without billing-only SKUs passes
	// placement.NoBillingOnlySKUs() explicitly.
	_, err := NewServer(ServerConfig{Addr: freePort(t), ProviderUUID: testutil.ValidUUID2}, ServerDeps{
		ChainClient: &mockChainClient{},
	})
	require.ErrorContains(t, err, "billing policy is required")
}

func TestPayloadHandler_BillingOnlyLeaseRefused(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-billing-only")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
				Items:        []billingtypes.LeaseItem{{SkuUuid: testBillingOnlySKU, Quantity: 1}},
			}, nil
		},
	}
	publisher := newMockPayloadPublisher()
	handler := NewPayloadHandler(client, publisher, testutil.ValidUUID1, "manifest", testBillingOnlyPolicy(t))

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assertBillingOnlyRefusal(t, rr, "lease is billing-only and does not accept a payload")
	assert.Empty(t, publisher.storedPayloads, "a billing-only lease's payload is never stored")
	assert.Empty(t, publisher.publishedEvents, "nothing is queued for provisioning")
}

// TestRestoreLease_BillingOnlyTargetRefused drives the restore route through
// the restore service and the placement coordinator bound with a billing-only
// policy. A restore deploys a workload exactly as a provision does, so a
// billing-only target must never reach the backend, even one that would
// accept it.
func TestRestoreLease_BillingOnlyTargetRefused(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
					Items:        []billingtypes.LeaseItem{{SkuUuid: testBillingOnlySKU, Quantity: 1}},
				}, nil
			}
			return nil, nil
		},
	}

	var restoreCalls atomic.Int32
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" {
			restoreCalls.Add(1)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()
	backendClient := newBackendHTTPClientForTest(t, backendHTTPClientConfig{
		Name: "test-backend", BaseURL: backendServer.URL, Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	placements := &mockPlacementLookup{getFunc: func(uuid string) string {
		if uuid == fromLeaseUUID {
			return "test-backend"
		}
		return ""
	}}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placements,
		restoreService: newRestoreServiceForTest(
			t, providerUUID, kp.Address, chainClient, router, placements, nil, testBillingOnlyPolicy(t),
		),
		providerUUID: providerUUID,
		bech32Prefix: "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore",
		strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)
	rec := httptest.NewRecorder()

	h.RestoreLease(rec, req)

	assertBillingOnlyRefusal(t, rec, "lease is billing-only and cannot be restored into")
	assert.Zero(t, restoreCalls.Load(), "a billing-only lease never reaches the backend")
}

// TestGetLeaseStatus_MarksBillingOnlyLease checks /status tells a billing-only
// lease, which never gets a provision_status, apart from one whose backend
// Fred could not read. A lease that mixes in a deployable SKU is not
// billing-only.
func TestGetLeaseStatus_MarksBillingOnlyLease(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	for name, tc := range map[string]struct {
		policy      *placement.BillingPolicy
		skus        []string
		billingOnly bool
	}{
		"billing-only":                {policy: testBillingOnlyPolicy(t), skus: []string{testBillingOnlySKU}, billingOnly: true},
		"mixed with a deployable SKU": {policy: testBillingOnlyPolicy(t), skus: []string{testBillingOnlySKU, testutil.ValidUUID3}},
		"deployable":                  {policy: testBillingOnlyPolicy(t), skus: []string{testutil.ValidUUID3}},
		"no billing-only SKUs":        {policy: placement.NoBillingOnlySKUs(), skus: []string{testBillingOnlySKU}},
	} {
		t.Run(name, func(t *testing.T) {
			lease := &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}
			for _, sku := range tc.skus {
				lease.Items = append(lease.Items, billingtypes.LeaseItem{SkuUuid: sku, Quantity: 1})
			}
			h := NewHandlers(HandlersConfig{
				Client: &mockChainClient{getLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					return lease, nil
				}},
				BillingPolicy: tc.policy,
				ProviderUUID:  providerUUID,
				Bech32Prefix:  "manifest",
			})

			req := httptest.NewRequest(http.MethodGet, "/v1/leases/"+leaseUUID+"/status", nil)
			req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
			req.SetPathValue("lease_uuid", leaseUUID)
			rec := httptest.NewRecorder()
			h.GetLeaseStatus(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			var fields map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fields))
			if tc.billingOnly {
				assert.Equal(t, true, fields["billing_only"])
			} else {
				assert.NotContains(t, fields, "billing_only", "omitted unless the lease is billing-only")
			}
		})
	}
}

// TestGetBillingOnlySKUs_Unauthenticated drives the route through the server,
// which serves it without a token so a client can check a SKU before it
// creates a lease.
func TestGetBillingOnlySKUs_Unauthenticated(t *testing.T) {
	const otherSKU = "00f4c1de-1b2a-4c3d-8e9f-0a1b2c3d4e5f"
	for name, tc := range map[string]struct {
		skus []string
		want string
	}{
		"listed in ascending order": {
			skus: []string{testBillingOnlySKU, otherSKU},
			want: `{"provider_uuid":"` + testutil.ValidUUID2 + `","billing_only_skus":["` + otherSKU + `","` + testBillingOnlySKU + `"]}`,
		},
		"none is an empty list": {
			want: `{"provider_uuid":"` + testutil.ValidUUID2 + `","billing_only_skus":[]}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			policy, err := placement.NewBillingPolicy(tc.skus)
			require.NoError(t, err)
			server, err := NewServer(ServerConfig{
				BillingPolicy:  policy,
				ProviderUUID:   testutil.ValidUUID2,
				Bech32Prefix:   "manifest",
				RateLimitRPS:   100,
				RateLimitBurst: 200,
			}, ServerDeps{ChainClient: &mockChainClient{}})
			require.NoError(t, err)

			rec := httptest.NewRecorder()
			server.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/billing-only-skus", nil))

			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			assert.JSONEq(t, tc.want, rec.Body.String())
		})
	}
}

func TestGetBillingOnlySKUs_WithoutPolicyIsRefused(t *testing.T) {
	h := NewHandlers(HandlersConfig{ProviderUUID: testutil.ValidUUID2})
	rec := httptest.NewRecorder()
	h.GetBillingOnlySKUs(rec, httptest.NewRequest(http.MethodGet, "/v1/billing-only-skus", nil))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "an empty list would claim every SKU is deployable")
	assert.NotContains(t, rec.Body.String(), "billing_only_skus")
}
