package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/testutil"
)

type apiLogAdmissionBackend struct {
	backend.Backend
	getLogs func(context.Context, string, int) (map[string]string, error)
}

func (*apiLogAdmissionBackend) Name() string { return "log-admission" }

func (b *apiLogAdmissionBackend) GetLogs(ctx context.Context, lease string, tail int) (map[string]string, error) {
	return b.getLogs(ctx, lease, tail)
}

func newLogAdmissionAPI(t *testing.T, timeout time.Duration, tenantRate float64, client *apiLogAdmissionBackend) (http.Handler, func() [2]*http.Request) {
	t.Helper()
	keys := [2]*testutil.TestKeyPair{testutil.NewTestKeyPair("logs-first"), testutil.NewTestKeyPair("logs-second")}
	leaseIDs := [2]string{testutil.ValidUUID1, testutil.ValidUUID3}
	leases := make(map[string]*billingtypes.Lease, len(leaseIDs))
	for i, leaseID := range leaseIDs {
		leases[leaseID] = &billingtypes.Lease{
			Uuid: leaseID, Tenant: keys[i].Address, ProviderUuid: testutil.ValidUUID2,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	server, err := NewServer(ServerConfig{
		ProviderUUID: testutil.ValidUUID2, Bech32Prefix: "manifest", RequestTimeout: timeout,
		WriteTimeout: 7 * time.Second,
		RateLimitRPS: 100, RateLimitBurst: 100, TenantRateLimitRPS: tenantRate, TenantRateLimitBurst: 100,
	}, ServerDeps{
		BackendRouter: router,
		ChainClient: &mockChainClient{getLeaseFunc: func(_ context.Context, leaseID string) (*billingtypes.Lease, error) {
			return leases[leaseID], nil
		}},
	})
	require.NoError(t, err)
	return server.server.Handler, func() [2]*http.Request {
		var requests [2]*http.Request
		for i, leaseID := range leaseIDs {
			requests[i] = httptest.NewRequest(http.MethodGet, "/v1/leases/"+leaseID+"/logs", nil)
			requests[i].Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(keys[i], leaseID, time.Now()))
		}
		return requests
	}
}

type apiHeldLogWriter struct {
	*httptest.ResponseRecorder
	writeStarted chan struct{}
	release      <-chan struct{}
	started      sync.Once
}

func (w *apiHeldLogWriter) Write(p []byte) (int, error) {
	w.started.Do(func() { close(w.writeStarted) })
	<-w.release
	return w.ResponseRecorder.Write(p)
}

func assertLogAdmissionRefusal(t *testing.T, handler http.Handler, request *http.Request) {
	t.Helper()
	for _, authorization := range []string{"", "Bearer invalid-signature"} {
		unauthorized := request.Clone(t.Context())
		unauthorized.Header.Set("Authorization", authorization)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, unauthorized)
		require.Equal(t, http.StatusUnauthorized, w.Code, "authentication remains outside the shared log gate")
	}

	w := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // A canceled waiter cannot hold the bounded admission queue.
	handler.ServeHTTP(w, request.Clone(ctx))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	require.JSONEq(t, `{"error":"log response capacity exhausted","code":503}`, w.Body.String())
}

func TestLogsRouteAdmissionSharedAcrossTenantsThroughResponseWrite(t *testing.T) {
	for _, scenario := range []struct {
		holdBackend bool
		tenantRate  float64
	}{{true, 0}, {false, 0}, {true, 100}, {false, 100}} {
		holdBackend := scenario.holdBackend
		name := "final_client_write"
		if holdBackend {
			name = "backend_read"
		}
		t.Run(fmt.Sprintf("%s/tenant_rate=%g", name, scenario.tenantRate), func(t *testing.T) {
			// Server-owned LRU cleanup goroutines outlive requests. Construct the
			// server outside fake time; only request lifetimes belong to the bubble.
			client := new(apiLogAdmissionBackend)
			handler, newRequests := newLogAdmissionAPI(t, time.Second, scenario.tenantRate, client)
			synctest.Test(t, func(t *testing.T) {
				requests := newRequests()
				release := make(chan struct{})
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				backendStarted := make(chan struct{})
				var calls atomic.Int32
				client.getLogs = func(_ context.Context, _ string, _ int) (map[string]string, error) {
					if calls.Add(1) == 1 {
						close(backendStarted)
						if holdBackend {
							<-release
						}
					}
					return map[string]string{"web/0": "live\n", "failed/web/0": "previous failure\n"}, nil
				}
				first := httptest.NewRecorder()
				var writer http.ResponseWriter = first
				if !holdBackend {
					writer = &apiHeldLogWriter{ResponseRecorder: first, writeStarted: make(chan struct{}), release: release}
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					handler.ServeHTTP(writer, requests[0])
				}()
				synctest.Wait()
				select {
				case <-backendStarted:
				default:
					t.Fatal("authenticated request did not reach its backend")
				}
				if held, ok := writer.(*apiHeldLogWriter); ok {
					select {
					case <-held.writeStarted:
					default:
						t.Fatal("completed backend response did not reach the client writer")
					}
				}
				assertLogAdmissionRefusal(t, handler, requests[1])
				require.EqualValues(t, 1, calls.Load(), "another tenant cannot materialize another log response")
				finish()
				<-done
				require.Equal(t, http.StatusOK, first.Code)
				second := httptest.NewRecorder()
				handler.ServeHTTP(second, requests[1].Clone(t.Context()))
				require.Equal(t, http.StatusOK, second.Code, "idempotent read token remains usable after admission refusal")
				require.EqualValues(t, 2, calls.Load())
				tenant := testutil.NewTestKeyPair("logs-second").Address
				require.JSONEq(t, fmt.Sprintf(`{"lease_uuid":%q,"tenant":%q,"provider_uuid":%q,"logs":{"web/0":"live\n","failed/web/0":"previous failure\n"}}`,
					testutil.ValidUUID3, tenant, testutil.ValidUUID2), second.Body.String())
			})
		})
	}
}

func TestLogsRouteConfiguredTimeoutRetainsAdmissionUntilBackendExits(t *testing.T) {
	const timeout = 37 * time.Millisecond
	client := new(apiLogAdmissionBackend)
	handler, newRequests := newLogAdmissionAPI(t, timeout, 0, client)
	synctest.Test(t, func(t *testing.T) {
		requests := newRequests()
		release := make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		defer finish()
		var calls atomic.Int32
		observedDeadline := make(chan time.Time, 1)
		client.getLogs = func(ctx context.Context, _ string, _ int) (map[string]string, error) {
			if calls.Add(1) == 1 {
				deadline, _ := ctx.Deadline()
				observedDeadline <- deadline
				<-ctx.Done()
				<-release // A canceled request does not prove the backend released its response.
				return nil, ctx.Err()
			}
			return map[string]string{"web/0": "ready"}, nil
		}
		start := time.Now()
		first := httptest.NewRecorder()
		handler.ServeHTTP(first, requests[0])
		require.Equal(t, start.Add(timeout), <-observedDeadline, "route must pass the configured timeout to its response owner")
		require.Equal(t, timeout, time.Since(start))
		require.Equal(t, http.StatusServiceUnavailable, first.Code)
		require.Contains(t, first.Body.String(), "request timeout")
		assertLogAdmissionRefusal(t, handler, requests[1])
		require.EqualValues(t, 1, calls.Load())
		finish()
		synctest.Wait()
		second := httptest.NewRecorder()
		handler.ServeHTTP(second, requests[1].Clone(t.Context()))
		require.Equal(t, http.StatusOK, second.Code)
		require.EqualValues(t, 2, calls.Load())
	})
}

func TestLogsRouteUnauthenticatedResponseDoesNotOwnAdmission(t *testing.T) {
	for _, tenantRate := range []float64{0, 100} {
		t.Run(fmt.Sprintf("tenant_rate=%g", tenantRate), func(t *testing.T) {
			var calls atomic.Int32
			client := &apiLogAdmissionBackend{getLogs: func(context.Context, string, int) (map[string]string, error) {
				calls.Add(1)
				return map[string]string{"web/0": "ready"}, nil
			}}
			handler, newRequests := newLogAdmissionAPI(t, time.Second, tenantRate, client)
			synctest.Test(t, func(t *testing.T) {
				requests := newRequests()
				requests[0].Header.Del("Authorization")
				release := make(chan struct{})
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				writer := &apiHeldLogWriter{ResponseRecorder: httptest.NewRecorder(), writeStarted: make(chan struct{}), release: release}
				done := make(chan struct{})
				go func() { defer close(done); handler.ServeHTTP(writer, requests[0]) }()
				synctest.Wait()
				select {
				case <-writer.writeStarted:
				default:
					t.Fatal("unauthenticated response did not reach the client writer")
				}
				require.Zero(t, calls.Load(), "unauthenticated request must not query backend logs")
				second := httptest.NewRecorder()
				handler.ServeHTTP(second, requests[1])
				require.Equal(t, http.StatusOK, second.Code, "a held unauthorized response must not consume log admission")
				require.EqualValues(t, 1, calls.Load())
				finish()
				<-done
				require.Equal(t, http.StatusUnauthorized, writer.Code)
			})
		})
	}
}

func TestLogsRouteQueuedTenantPrecedesRepeatingCurrentTenant(t *testing.T) {
	client := new(apiLogAdmissionBackend)
	handler, newRequests := newLogAdmissionAPI(t, time.Second, 0, client)
	synctest.Test(t, func(t *testing.T) {
		requests := newRequests()
		firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
		finishFirst := sync.OnceFunc(func() { close(firstRelease) })
		finishSecond := sync.OnceFunc(func() { close(secondRelease) })
		defer finishFirst()
		defer finishSecond()
		entered := make(chan string, 3)
		var calls atomic.Int32
		client.getLogs = func(_ context.Context, lease string, _ int) (map[string]string, error) {
			call := calls.Add(1)
			entered <- lease
			switch call {
			case 1:
				<-firstRelease
			case 2:
				<-secondRelease
			}
			return map[string]string{"web/0": "ready"}, nil
		}
		responses := make(chan *httptest.ResponseRecorder, 3)
		request := func(r *http.Request) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r.Clone(t.Context()))
			responses <- w
		}
		go request(requests[0])
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load())
		require.Equal(t, testutil.ValidUUID1, <-entered)
		go request(requests[1])
		synctest.Wait() // The other tenant has reached admission before a repeat.
		go request(requests[0])
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load(), "queued requests must not materialize more log maps")
		require.Empty(t, responses, "requests wait in the bounded queue while their deadlines permit")
		finishFirst()
		synctest.Wait()
		require.EqualValues(t, 2, calls.Load())
		require.Equal(t, testutil.ValidUUID3, <-entered, "the waiting tenant must precede a repeat by the current tenant")
		finishSecond()
		synctest.Wait()
		require.EqualValues(t, 3, calls.Load())
		require.Equal(t, testutil.ValidUUID1, <-entered)
		for range 3 {
			require.Equal(t, http.StatusOK, (<-responses).Code)
		}
	})
}

type apiLogDeadlineWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *apiLogDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestLogsRoutePreservesConfiguredWriteBudget(t *testing.T) {
	client := &apiLogAdmissionBackend{getLogs: func(context.Context, string, int) (map[string]string, error) {
		return map[string]string{"web/0": "ready"}, nil
	}}
	handler, newRequests := newLogAdmissionAPI(t, time.Second, 0, client)
	synctest.Test(t, func(t *testing.T) {
		requests := newRequests()
		writer := &apiLogDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
		start := time.Now()
		handler.ServeHTTP(writer, requests[0])
		require.Equal(t, http.StatusOK, writer.Code)
		require.Equal(t, []time.Time{start.Add(8 * time.Second), start.Add(7 * time.Second)}, writer.deadlines,
			"allow request processing before tightening the final response write to its configured budget")
	})
}
