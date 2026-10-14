package api

import (
	"context"
	"fmt"
	"io"
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
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/testutil"
)

const testBillingOnlySKU = "01a126cc-7edd-700d-9ac6-02ed34fa4bff"

func TestNewServer_RequiresBillingPolicy(t *testing.T) {
	// No implicit default: a provider without billing-only SKUs passes
	// provisioner.NoBillingOnlySKUs() explicitly.
	_, err := NewServer(ServerConfig{Addr: freePort(t), ProviderUUID: testutil.ValidUUID2}, ServerDeps{
		ChainClient: &mockChainClient{},
	})
	require.ErrorContains(t, err, "billing policy is required")
}

// TestServer_RestoreRefusesBillingOnlyLease drives the restore route through
// NewServer. A restore deploys a workload exactly as a provision does, so a
// billing-only lease must never reach the backend, even one that would accept it.
func TestServer_RestoreRefusesBillingOnlyLease(t *testing.T) {
	kp := testutil.NewTestKeyPair("billing-only-restore")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	var restoreCalls atomic.Int32
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" {
			restoreCalls.Add(1)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backendServer.Close()
	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: backendServer.URL, Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	policy, err := provisioner.NewBillingPolicy([]string{testBillingOnlySKU})
	require.NoError(t, err)

	addr := freePort(t)
	s, err := NewServer(ServerConfig{
		BillingPolicy:  policy,
		Addr:           addr,
		ProviderUUID:   providerUUID,
		Bech32Prefix:   "manifest",
		RateLimitRPS:   100,
		RateLimitBurst: 200,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    30 * time.Second,
		RequestTimeout: 5 * time.Second,
	}, ServerDeps{
		ChainClient: &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         uuid,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
					Items:        []billingtypes.LeaseItem{{SkuUuid: testBillingOnlySKU, Quantity: 1}},
				}, nil
			},
		},
		BackendRouter:     router,
		CallbackPublisher: &mockCallbackPublisher{},
		StatusChecker:     &mockStatusChecker{},
		PlacementLookup: &mockPlacementLookup{getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		}},
	})
	require.NoError(t, err)
	client := startAndWaitForServer(t, s, addr)
	defer s.Shutdown(context.Background())

	token := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s/v1/leases/%s/restore", addr, leaseUUID),
		strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
	assert.Contains(t, string(body), "billing-only")
	assert.Zero(t, restoreCalls.Load(), "a billing-only lease never reaches the backend")
}
