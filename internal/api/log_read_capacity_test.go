package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

func TestGetLeaseLogsBackendCapacityPreservesTenantRetryAndSharedCircuit(t *testing.T) {
	leaseUUID, providerUUID := testutil.ValidUUID1, testutil.ValidUUID2
	kp := testutil.NewTestKeyPair("log-capacity")
	finish := make(chan struct{})
	defer close(finish)
	logsHandler := backend.NewLogsHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		<-finish // The worker retains its allocation after its request times out.
	}), 20*time.Millisecond)
	first := httptest.NewRecorder()
	logsHandler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/logs/first-tenant", nil))
	require.Equal(t, http.StatusServiceUnavailable, first.Code)
	require.JSONEq(t, `{"error":"request timeout"}`, first.Body.String())

	var infoCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/logs/") {
			logsHandler.ServeHTTP(w, r)
			return
		}
		infoCalls.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := newBackendHTTPClientForTest(t, backendHTTPClientConfig{
		Name: "log-capacity", BaseURL: server.URL, Timeout: 10 * time.Second, CBFailureThresh: 1,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	h := &Handlers{
		client: &mockChainClient{getLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		}},
		backendRouter: router, providerUUID: providerUUID, bech32Prefix: "manifest",
	}
	route := backend.NewTenantLogsHandler(http.HandlerFunc(h.GetLeaseLogs), 10*time.Second)
	for range 3 {
		req := httptest.NewRequest(http.MethodGet, "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
		req.SetPathValue("lease_uuid", leaseUUID)
		rec := httptest.NewRecorder()
		route.ServeHTTP(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, "%s", rec.Body.String())
		require.Equal(t, "1", rec.Header().Get("Retry-After"))
		require.JSONEq(t, `{"error":"log response capacity exhausted","code":503}`, rec.Body.String())
	}
	_, err = client.GetInfo(t.Context(), leaseUUID)
	require.NoError(t, err, "repeated backend log pressure must leave unrelated reads and mutation admission available")
	require.EqualValues(t, 1, infoCalls.Load())
}
