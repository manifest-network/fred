package api

import (
	"context"
	"encoding/hex"
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

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "billing-only")
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

	assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "billing-only")
	assert.Zero(t, restoreCalls.Load(), "a billing-only lease never reaches the backend")
}
