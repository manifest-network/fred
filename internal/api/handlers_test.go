package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
	"github.com/manifest-network/fred/internal/testutil"
)

type restoreServiceFunc func(context.Context, restoreapp.Command) restoreapp.Result

func (execute restoreServiceFunc) Execute(
	ctx context.Context,
	command restoreapp.Command,
) restoreapp.Result {
	return execute(ctx, command)
}

// TestNewHandlers_AppliesWebSocketDefaults pins the WebSocket security
// defaults set by NewHandlers. StreamLeaseEvents has a defensive fallback
// that substitutes the production defaults and logs an slog.Error if either
// field is non-positive, so a future change that drops either field init
// from NewHandlers would NOT silently disable the mitigations — but it
// would force every /events connection onto the fallback path, emitting a
// per-connection error log and making the fallback the de-facto main code
// path instead of a safety net. This test keeps the constructor honest so
// the fallback stays reserved for genuinely misconfigured callers.
func TestNewHandlers_AppliesWebSocketDefaults(t *testing.T) {
	h := NewHandlers(HandlersConfig{
		ProviderUUID: testutil.ValidUUID1,
		Bech32Prefix: "manifest",
	})

	assert.Equal(t, wsDefaultMaxMessageSize, h.wsMaxMessageSize,
		"NewHandlers must set wsMaxMessageSize so the production path doesn't fall through to StreamLeaseEvents' defensive default (which would log an slog.Error per connection)")
	assert.Equal(t, wsDefaultMaxConnLifetime, h.wsMaxConnLifetime,
		"NewHandlers must set wsMaxConnLifetime so the production path doesn't fall through to StreamLeaseEvents' defensive default (which would log an slog.Error per connection)")
}

func TestNewHandlers_NormalizesTypedNilRestoreService(t *testing.T) {
	t.Parallel()
	var typedNil restoreServiceFunc
	handlers := NewHandlers(HandlersConfig{RestoreService: typedNil})
	assert.Nil(t, handlers.restoreService,
		"a typed-nil optional service must behave like an unwired restore endpoint")
}

func TestHealthCheck(t *testing.T) {
	h := &Handlers{
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
		// client is nil - health check should still work
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	assert.Equal(t, testutil.ValidUUID1, response.ProviderUUID)

	// Check Content-Type header
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

// TestHealthCheck_ChainUnavailable tests health check when chain ping fails.
//
// This test used to assert 503. It asserts 200 now, deliberately: that 503 is
// the mainnet-morpheus ENG-522 incident in miniature. A single failed chain Ping
// flipped /health to 503, the load balancer dropped providerd's only server, and
// a backend's provision-success callback — which needs no chain at all — got
// "no available server" three times and was dropped, orphaning the workload.
// The chain being unreachable is a degradation of a shared remote dependency,
// never a reason to take providerd out of rotation.
func TestHealthCheck_ChainUnavailable(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return fmt.Errorf("connection refused")
		},
	}

	h := &Handlers{
		client:       chainClient,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code,
		"an unreachable chain must not de-register providerd from its load balancer (ENG-522)")

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "degraded", response.Status)

	// Check that chain check shows unhealthy with sanitized message
	// (raw error details are logged server-side, not exposed to clients)
	chainCheck, ok := response.Checks["chain"]
	require.True(t, ok, "missing chain check in response")
	assert.Equal(t, "unhealthy", chainCheck.Status)
	assert.Equal(t, "chain connectivity failed", chainCheck.Message)
}

// TestHealthCheck_ChainHealthy tests health check when chain is available.
func TestHealthCheck_ChainHealthy(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return nil // Healthy
		},
	}

	h := &Handlers{
		client:       chainClient,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)

	chainCheck, ok := response.Checks["chain"]
	require.True(t, ok, "missing chain check in response")
	assert.Equal(t, "healthy", chainCheck.Status)
}

// TestHealthCheck_BackendUnhealthy tests health check when a backend is unavailable.
func TestHealthCheck_BackendUnhealthy(t *testing.T) {
	// Create a backend server that returns unhealthy
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("backend down"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
		},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        nil, // No chain client
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	// Was 503. Now 200 — and note this router has exactly ONE backend, so this
	// is the "every backend is down" case, not merely "one of N". A fleet-wide
	// backend outage is precisely when de-registering helps least (there is
	// nowhere to route) and hurts most (the callback route dies with the tenant
	// API, so a backend that recovers cannot report what it finished).
	assert.Equal(t, http.StatusOK, rec.Code,
		"an unreachable backend must not de-register providerd from its load balancer (ENG-522)")

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "degraded", response.Status)

	// Check backend health status
	backendCheck, ok := response.Checks["backend:test-backend"]
	require.True(t, ok, "missing backend check in response, got: %v", response.Checks)
	assert.Equal(t, "unhealthy", backendCheck.Status)
}

// TestHealthCheck_AllHealthy tests health check when both chain and backend are healthy.
func TestHealthCheck_AllHealthy(t *testing.T) {
	chainClient := &mockChainClient{
		pingFunc: func(ctx context.Context) error {
			return nil
		},
	}

	// Create a healthy backend
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "healthy-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
		},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)

	// Both checks should be healthy
	assert.Equal(t, "healthy", response.Checks["chain"].Status)
	assert.Equal(t, "healthy", response.Checks["backend:healthy-backend"].Status)
}

// mockPayloadStoreHealth implements PayloadStoreHealth for testing.
type mockPayloadStoreHealth struct {
	healthyFunc func() error
}

func (m *mockPayloadStoreHealth) Healthy() error {
	if m.healthyFunc != nil {
		return m.healthyFunc()
	}
	return nil
}

// unhealthyStore is the error a broken bbolt store reports.
func unhealthyStore() error { return errors.New("bbolt: database not open") }

// localStoreCases enumerates the three process-owned bbolt stores, each wired to
// fail. They share a verdict ("unhealthy") that the two remote dependencies do
// not, so every test that cares about the local/remote split runs over this set.
func localStoreCases() []struct {
	name      string
	checkKey  string
	message   string
	configure func(h *Handlers)
} {
	return []struct {
		name      string
		checkKey  string
		message   string
		configure func(h *Handlers)
	}{
		{
			name:      "token tracker",
			checkKey:  "token_tracker",
			message:   "token tracker unavailable",
			configure: func(h *Handlers) { h.tokenTracker = &mockTokenTracker{healthyFunc: unhealthyStore} },
		},
		{
			name:      "placement store",
			checkKey:  "placement_store",
			message:   "placement store unavailable",
			configure: func(h *Handlers) { h.placementLookup = &mockPlacementLookup{healthyFunc: unhealthyStore} },
		},
		{
			name:      "payload store",
			checkKey:  "payload_store",
			message:   "payload store unavailable",
			configure: func(h *Handlers) { h.payloadStoreHealth = &mockPayloadStoreHealth{healthyFunc: unhealthyStore} },
		},
	}
}

// TestHealthCheck_LocalStoreUnhealthyStillServes pins the third verdict tier.
//
// A broken bbolt store is a genuinely local fault — the class a load balancer is
// normally sanctioned to act on — so it reports "unhealthy" rather than
// "degraded". /health still answers 200 anyway, and that is not a contradiction:
// accepting a backend callback writes to no store at all (server.go registers
// POST /callbacks/provision with only a timeout wrapper), so de-registering
// providerd's single server would sever a working path to fix a broken one. A
// supervisor can restart this process; a load balancer cannot. /readyz is where
// the tier reaches the wire.
func TestHealthCheck_LocalStoreUnhealthyStillServes(t *testing.T) {
	for _, tt := range localStoreCases() {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handlers{
				providerUUID: testutil.ValidUUID1,
				bech32Prefix: "manifest",
			}
			tt.configure(h)

			req := httptest.NewRequest("GET", "/health", nil)
			rec := httptest.NewRecorder()

			h.HealthCheck(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code,
				"/health is a liveness contract and must never 503 on a dependency probe")

			var response HealthResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

			assert.Equal(t, "unhealthy", response.Status,
				"a process-owned store is not merely degraded")

			check, ok := response.Checks[tt.checkKey]
			require.True(t, ok, "missing %s check, got: %v", tt.checkKey, response.Checks)
			assert.Equal(t, "unhealthy", check.Status)
			assert.Equal(t, tt.message, check.Message,
				"raw store errors must stay server-side")
		})
	}
}

// TestHealthCheck_LocalFailureOutranksRemote guards the precedence in the
// verdict switch. With both a remote and a local fault present the answer must
// be the more severe one — a naive if/else chain that checked remote first would
// report "degraded" and hide the restart-worthy fault.
func TestHealthCheck_LocalFailureOutranksRemote(t *testing.T) {
	h := &Handlers{
		client: &mockChainClient{
			pingFunc: func(ctx context.Context) error { return errors.New("connection refused") },
		},
		tokenTracker: &mockTokenTracker{healthyFunc: unhealthyStore},
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "unhealthy", response.Status,
		"local failure must outrank remote degradation")
	assert.Equal(t, "unhealthy", response.Checks["chain"].Status)
	assert.Equal(t, "unhealthy", response.Checks["token_tracker"].Status)
}

// TestHealthCheck_UnconfiguredChecksAreAbsent pins that an absent key means "not
// configured", never "passed". A dev-mode providerd runs without the bbolt
// stores, and folding those into the verdict would make it permanently
// unhealthy.
func TestHealthCheck_UnconfiguredChecksAreAbsent(t *testing.T) {
	h := &Handlers{
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	for _, key := range []string{"chain", "token_tracker", "placement_store", "payload_store"} {
		assert.NotContains(t, response.Checks, key,
			"%s is not configured and must be omitted, not reported as passing", key)
	}
}

// TestHealthCheck_PayloadStoreProbed covers the store that had a Healthy method
// nothing ever called: a lost or truncated payloads.db was invisible everywhere while
// /update returned 500 and reprovisions reverted tenants to their as-created
// manifest (ENG-619).
func TestHealthCheck_PayloadStoreProbed(t *testing.T) {
	called := false
	h := &Handlers{
		payloadStoreHealth: &mockPayloadStoreHealth{
			healthyFunc: func() error {
				called = true
				return nil
			},
		},
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.True(t, called, "the payload store's Healthy() must actually be invoked")

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	require.Contains(t, response.Checks, "payload_store")
	assert.Equal(t, "healthy", response.Checks["payload_store"].Status)
}

// TestHealthCheck_RecordsCheckGauges pins the observability half of the fix.
// Removing dependency state from the status code is only safe because it lands
// in a metric instead: before this, chain, token_tracker and placement_store had
// no HEALTH metric at all — the chain has latency and transaction counters, but
// nothing answering "is it reachable now" — and the status code was their sole
// reachability signal.
//
// Asserts both polarities in one test so the ordering of other tests touching
// these global collectors cannot make it pass vacuously.
func TestHealthCheck_RecordsCheckGauges(t *testing.T) {
	checkNames := []string{"chain", "token_tracker", "placement_store", "payload_store"}

	probe := func(t *testing.T, healthy bool) {
		t.Helper()

		// Seed every series to the OPPOSITE value first. Without this the
		// "unhealthy sets 0" half is vacuous: WithLabelValues auto-creates a
		// gauge at 0, so an implementation that never writes at all reads as 0
		// and passes. Seeding makes a missing write observable in both
		// directions.
		seed := 1.0
		if healthy {
			seed = 0.0
		}
		for _, check := range checkNames {
			metrics.HealthCheckHealthy.WithLabelValues(check).Set(seed)
		}

		result := func() error {
			if healthy {
				return nil
			}
			return unhealthyStore()
		}

		h := &Handlers{
			client: &mockChainClient{
				pingFunc: func(ctx context.Context) error { return result() },
			},
			tokenTracker:       &mockTokenTracker{healthyFunc: result},
			placementLookup:    &mockPlacementLookup{healthyFunc: result},
			payloadStoreHealth: &mockPayloadStoreHealth{healthyFunc: result},
			providerUUID:       testutil.ValidUUID1,
			bech32Prefix:       "manifest",
		}

		rec := httptest.NewRecorder()
		h.HealthCheck(rec, httptest.NewRequest("GET", "/health", nil))

		want := 0.0
		if healthy {
			want = 1.0
		}
		for _, check := range checkNames {
			assert.Equal(t, want,
				promtestutil.ToFloat64(metrics.HealthCheckHealthy.WithLabelValues(check)),
				"fred_health_check_healthy{check=%q} should be %v (seeded to %v, so a missing write fails here)", check, want, seed)
		}
	}

	t.Run("unhealthy sets 0", func(t *testing.T) { probe(t, false) })
	t.Run("healthy sets 1", func(t *testing.T) { probe(t, true) })
}

// TestHealthCheck_KeepsProbingBackends is a regression guard on a subtle
// coupling. Router.HealthCheck is the ONLY writer of fred_backend_healthy, and
// this handler is its only non-test caller, so the deployment's 30s poll of
// /health is the clock driving that gauge, the BackendUnhealthy alert and two
// dashboards. A future "backends no longer affect the status code, so stop
// probing them" simplification would freeze all four at a plausible wrong value
// rather than making them go absent.
func TestHealthCheck_KeepsProbingBackends(t *testing.T) {
	probed := make(chan struct{}, 1)
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			select {
			case probed <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backendServer.Close()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "gauge-backend",
				BaseURL: backendServer.URL,
				Timeout: 5 * time.Second,
			}),
			IsDefault: true,
		}},
		BackendHealthy: metrics.BackendHealthy,
	})
	require.NoError(t, err)

	h := &Handlers{
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	rec := httptest.NewRecorder()
	h.HealthCheck(rec, httptest.NewRequest("GET", "/health", nil))

	assert.Len(t, probed, 1, "/health must still probe every backend")
	assert.Equal(t, 1.0,
		promtestutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("gauge-backend")),
		"fred_backend_healthy has no other writer; /health must keep driving it")
}

// TestHealthProbeBudget_FitsInsideEveryDeadline pins the inequality the whole
// liveness contract rests on. Three independent deadlines can each recreate the
// ENG-522 cascade if the budget outgrows them, and the binding one is NOT
// fred's — it belongs to the prober.
func TestHealthProbeBudget_FitsInsideEveryDeadline(t *testing.T) {
	// Traefik's loadBalancer healthCheck timeout. Documented default 5s, and
	// the reference deployment's stanza sets only `path` and `interval`
	// (manifest-deploy roles/traefik/templates/dynamic.yml.j2). This is the
	// SMALLEST of the three and therefore the one that actually binds: a probe
	// slower than this marks the only server DOWN regardless of what the
	// handler would eventually have answered.
	const traefikProbeTimeout = 5 * time.Second
	// config.go: v.SetDefault("http_write_timeout", "15s") — the server severs
	// the response write at this point.
	const defaultWriteTimeout = 15 * time.Second

	assert.Less(t, healthProbeBudget, traefikProbeTimeout,
		"healthProbeBudget must stay under Traefik's 5s healthCheck timeout — the prober gives up first, so a longer budget marks the only server DOWN no matter what we answer")
	assert.Less(t, healthProbeBudget, defaultWriteTimeout,
		"healthProbeBudget must stay under the default http_write_timeout or the connection is severed mid-probe")
	assert.Less(t, healthProbeBudget, defaultRequestTimeout,
		"healthProbeBudget must stay under the request timeout or http.TimeoutHandler answers 503")
}

// TestServer_HealthNeverReturns503ThroughTheStack is the test whose absence let
// the http.TimeoutHandler gap through review: every other health test calls
// Handlers.HealthCheck directly, which bypasses requestTimeoutMiddleware — the
// one component that can turn this endpoint into a 503 without any verdict
// saying so.
//
// Exercised through the real mux and the real middleware chain, against a
// backend that accepts the connection and never answers.
func TestServer_HealthNeverReturns503ThroughTheStack(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer hung.Close()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: backend.NewHTTPClient(backend.HTTPClientConfig{
				Name: "hung-backend",
				// Deliberately LONGER than healthProbeBudget: the budget, not
				// the client timeout, must be what bounds this.
				BaseURL: hung.URL,
				Timeout: 30 * time.Second,
			}),
			IsDefault: true,
		}},
	})
	require.NoError(t, err)

	addr := freePort(t)
	srv, err := NewServer(ServerConfig{
		Addr:           addr,
		ProviderUUID:   testutil.ValidUUID1,
		Bech32Prefix:   "manifest",
		RateLimitRPS:   100,
		RateLimitBurst: 200,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    30 * time.Second,
		// Left at the default so the assertion below is about the real
		// production relationship, not a test-tuned one.
	}, ServerDeps{BackendRouter: router})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	// Wait on the LISTENER, not on /health. startAndWaitForServer polls /health
	// with a 2s client timeout, which this test deliberately makes slow — using
	// it here would time out on the very condition under test.
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 20*time.Millisecond, "server never started listening")

	client := &http.Client{Timeout: defaultRequestTimeout}

	for _, path := range []string{"/health", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			start := time.Now()
			resp, err := client.Get(fmt.Sprintf("http://%s%s", addr, path))
			require.NoError(t, err)
			defer resp.Body.Close()
			elapsed := time.Since(start)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusOK, resp.StatusCode,
				"a hung backend must not produce a non-200 through the real middleware; got body %q", string(body))
			assert.NotContains(t, string(body), "request timeout",
				"http.TimeoutHandler must never win the race against healthProbeBudget")
			assert.Less(t, elapsed, defaultRequestTimeout,
				"the probe must finish inside the request budget")

			var response HealthResponse
			require.NoError(t, json.Unmarshal(body, &response))
			assert.Equal(t, "degraded", response.Status)
		})
	}
}

// TestHealthCheck_HungBackendStillServes covers the case that a *stopped*
// backend does not: one that accepts the TCP connection and then never answers.
// Connection-refused fails in milliseconds, which is why both real incidents
// stayed inside any budget; a hung backend instead burns its full client
// timeout, and serially that used to blow the request budget and turn /health
// into a 503 by way of http.TimeoutHandler.
//
// The backend client timeout is set short here purely to keep the test fast —
// the property under test is that a non-answering backend yields 200 + degraded
// rather than a timeout, which the request-scoped budget and the router's
// concurrent probing together guarantee.
func TestHealthCheck_HungBackendStillServes(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer backendServer.Close()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "hung-backend",
				BaseURL: backendServer.URL,
				Timeout: 200 * time.Millisecond,
			}),
			IsDefault: true,
		}},
	})
	require.NoError(t, err)

	h := &Handlers{
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	rec := httptest.NewRecorder()
	start := time.Now()
	h.HealthCheck(rec, httptest.NewRequest("GET", "/health", nil))
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusOK, rec.Code,
		"a hung backend must not turn /health into a 503")
	assert.Less(t, elapsed, defaultRequestTimeout,
		"the probe must finish inside the request budget so http.TimeoutHandler never fires")

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	assert.Equal(t, "degraded", response.Status)
	assert.Equal(t, "unhealthy", response.Checks["backend:hung-backend"].Status)
}

// TestReadyz_RemoteDegradationStays200 is what makes a second, 503-capable
// endpoint safe to add at all. Neither real incident — the mainnet chain blip
// nor the 2026-08-17 dev backend stop — would take /readyz down either, so
// pointing a load balancer at it by mistake could not reproduce them.
func TestReadyz_RemoteDegradationStays200(t *testing.T) {
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backendServer.Close()

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "down-backend",
				BaseURL: backendServer.URL,
				Timeout: 5 * time.Second,
			}),
			IsDefault: true,
		}},
	})
	require.NoError(t, err)

	h := &Handlers{
		client: &mockChainClient{
			pingFunc: func(ctx context.Context) error { return errors.New("connection refused") },
		},
		backendRouter: router,
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	rec := httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest("GET", "/readyz", nil))

	assert.Equal(t, http.StatusOK, rec.Code,
		"degraded means providerd is still ready to serve")

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	assert.Equal(t, "degraded", response.Status)
}

// TestReadyz_LocalStoreUnhealthyReturns503 is the one place the third tier
// reaches the wire.
func TestReadyz_LocalStoreUnhealthyReturns503(t *testing.T) {
	for _, tt := range localStoreCases() {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handlers{
				providerUUID: testutil.ValidUUID1,
				bech32Prefix: "manifest",
			}
			tt.configure(h)

			rec := httptest.NewRecorder()
			h.Readyz(rec, httptest.NewRequest("GET", "/readyz", nil))

			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

			var response HealthResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
			assert.Equal(t, "unhealthy", response.Status)
			assert.Equal(t, "unhealthy", response.Checks[tt.checkKey].Status)
		})
	}
}

// TestReadyz_AllHealthy covers the ordinary case and pins that /readyz serves
// the same body shape as /health rather than a reduced one.
func TestReadyz_AllHealthy(t *testing.T) {
	h := &Handlers{
		client: &mockChainClient{
			pingFunc: func(ctx context.Context) error { return nil },
		},
		tokenTracker:  &mockTokenTracker{},
		statusChecker: &mockStatusChecker{inFlightCount: 7},
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
	}

	rec := httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest("GET", "/readyz", nil))

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	assert.Equal(t, testutil.ValidUUID1, response.ProviderUUID)
	assert.Equal(t, "healthy", response.Checks["chain"].Status)
	assert.Equal(t, "healthy", response.Checks["token_tracker"].Status)
	require.NotNil(t, response.Stats)
	assert.Equal(t, 7, response.Stats.InFlightProvisions)
}

func TestReadyz_WaitsForFirstAuthoritativePlacementInventory(t *testing.T) {
	for _, tt := range []struct {
		name         string
		bootstrapped bool
		wantStatus   int
		wantCheck    string
	}{
		{name: "startup inventory pending", wantStatus: http.StatusServiceUnavailable, wantCheck: "unhealthy"},
		{name: "startup inventory complete", bootstrapped: true, wantStatus: http.StatusOK, wantCheck: "healthy"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandlers(HandlersConfig{
				PlacementLookup: &mockBootstrapPlacementLookup{bootstrapped: tt.bootstrapped},
				ProviderUUID:    testutil.ValidUUID1,
				Bech32Prefix:    "manifest",
			})

			rec := httptest.NewRecorder()
			h.Readyz(rec, httptest.NewRequest("GET", "/readyz", nil))

			assert.Equal(t, tt.wantStatus, rec.Code)
			var response HealthResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
			assert.Equal(t, tt.wantCheck, response.Checks["placement_inventory"].Status)
			assert.Equal(t, "healthy", response.Checks["placement_store"].Status,
				"database health and startup authority are independent checks")
		})
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, "test error", http.StatusBadRequest)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var response ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "test error", response.Error)
	assert.Equal(t, http.StatusBadRequest, response.Code)
}

func TestWriteJSON(t *testing.T) {
	data := map[string]string{"key": "value"}

	rec := httptest.NewRecorder()
	writeJSON(rec, data, http.StatusOK)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var response map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "value", response["key"])
}

func TestExtractToken_MissingAuth(t *testing.T) {
	h := &Handlers{}

	req := httptest.NewRequest("GET", "/test", nil)
	// No Authorization header

	_, err := h.extractToken(req)
	assert.Error(t, err)
	assert.Equal(t, errMissingAuth, err)
}

func TestExtractToken_InvalidFormat(t *testing.T) {
	h := &Handlers{}

	tests := []struct {
		name   string
		header string
	}{
		{"no bearer", "token123"},
		{"wrong scheme", "Basic token123"},
		{"bearer only", "Bearer"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			_, err := h.extractToken(req)
			assert.Error(t, err, "extractToken() = nil error for header %q", tt.header)
		})
	}
}

// TestExtractToken_ValidFormat is covered by TestExtractToken_ValidToken below

func TestExtractToken_ValidToken(t *testing.T) {
	h := &Handlers{}

	// Pre-encoded valid token JSON for testing extraction (not signature validation)
	tokenB64 := "eyJ0ZW5hbnQiOiJtYW5pZmVzdDFhYmMiLCJsZWFzZV91dWlkIjoiMDEyMzQ1NjctODlhYi1jZGVmLTAxMjMtNDU2Nzg5YWJjZGVmIiwidGltZXN0YW1wIjoxMjM0NTY3ODkwLCJwdWJfa2V5IjoiZEdWemRBPT0iLCJzaWduYXR1cmUiOiJkR1Z6ZEE9PSJ9"

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB64)

	token, err := h.extractToken(req)
	require.NoError(t, err)

	assert.Equal(t, "manifest1abc", token.Tenant)
	assert.Equal(t, "01234567-89ab-cdef-0123-456789abcdef", token.LeaseUUID)
}

func TestExtractToken_CaseInsensitiveBearer(t *testing.T) {
	h := &Handlers{}

	tokenB64 := "eyJ0ZW5hbnQiOiJtYW5pZmVzdDFhYmMiLCJsZWFzZV91dWlkIjoiMDEyMzQ1NjctODlhYi1jZGVmLTAxMjMtNDU2Nzg5YWJjZGVmIiwidGltZXN0YW1wIjoxMjM0NTY3ODkwLCJwdWJfa2V5IjoiZEdWemRBPT0iLCJzaWduYXR1cmUiOiJkR1Z6ZEE9PSJ9"

	cases := []string{"Bearer", "bearer", "BEARER", "BeArEr"}

	for _, prefix := range cases {
		t.Run(prefix, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Authorization", prefix+" "+tokenB64)

			_, err := h.extractToken(req)
			assert.NoError(t, err, "extractToken() with %q error = %v", prefix, err)
		})
	}
}

func TestConnectionResponse_JSON(t *testing.T) {
	response := ConnectionResponse{
		LeaseUUID:    testutil.ValidUUID1,
		Tenant:       "manifest1abc",
		ProviderUUID: testutil.ValidUUID2,
		Connection: ConnectionDetails{
			Host: "compute-alpha.example.com",
			Ports: map[string]PortMapping{
				"443/tcp": {HostIP: "0.0.0.0", HostPort: 8443},
			},
			Protocol: "https",
			Metadata: map[string]string{
				"region": "us-east-1",
			},
		},
	}

	jsonBytes, err := json.Marshal(response)
	require.NoError(t, err)

	var decoded ConnectionResponse
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))

	assert.Equal(t, response.LeaseUUID, decoded.LeaseUUID)
	assert.Equal(t, response.Connection.Host, decoded.Connection.Host)
}

// mockChainClient implements ChainClient for testing.
type mockChainClient struct {
	getLeaseFunc       func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	getActiveLeaseFunc func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	pingFunc           func(ctx context.Context) error
}

func (m *mockChainClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getLeaseFunc != nil {
		return m.getLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

func (m *mockChainClient) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getActiveLeaseFunc != nil {
		return m.getActiveLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

func (m *mockChainClient) Ping(ctx context.Context) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

// TestGetLeaseConnection_BackendIntegration tests the backend integration path
// in GetLeaseConnection using httptest.Server and a real backend.Router.
func TestGetLeaseConnection_BackendIntegration(t *testing.T) {
	// Create a test key pair for signing tokens
	kp := testutil.NewTestKeyPair("test-tenant")

	// Test lease details
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Create a valid auth token
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	// Mock chain client that returns an active lease
	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil, // No backend router configured
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		// Create backend server that returns 404 (not provisioned)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("not found"))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		// Create real backend client and router
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		// Create backend server that returns 500
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_extracts_connection_details", func(t *testing.T) {
		// Create backend server that returns valid lease info
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"host":     "compute-alpha.example.com",
					"protocol": "https",
					"ports": map[string]any{
						"443/tcp": map[string]any{"host_ip": "0.0.0.0", "host_port": "8443"},
					},
					"metadata": map[string]any{
						"region":  "us-east-1",
						"backend": "test-backend",
					},
					"credentials": map[string]any{"token": "secret"}, // non-string map, ignored
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		// Verify response fields
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)

		// Verify connection details extraction
		assert.Equal(t, "compute-alpha.example.com", response.Connection.Host)
		assert.Equal(t, 8443, response.Connection.Ports["443/tcp"].HostPort)
		assert.Equal(t, "https", response.Connection.Protocol)
		assert.Equal(t, "us-east-1", response.Connection.Metadata["region"])
		assert.Equal(t, "test-backend", response.Connection.Metadata["backend"])
	})

	t.Run("happy_path_with_multiple_ports", func(t *testing.T) {
		// Test that multiple port mappings are handled correctly
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/info/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"host":"test.example.com","ports":{"80/tcp":{"host_ip":"0.0.0.0","host_port":"8080"},"443/tcp":{"host_ip":"0.0.0.0","host_port":"8443"}},"protocol":"grpc"}`))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Len(t, response.Connection.Ports, 2)
		assert.Equal(t, 8080, response.Connection.Ports["80/tcp"].HostPort)
		assert.Equal(t, 8443, response.Connection.Ports["443/tcp"].HostPort)
	})
}

// TestExtractConnectionDetails tests the extractConnectionDetails helper function.
func TestExtractConnectionDetails(t *testing.T) {
	tests := []struct {
		name              string
		input             backend.LeaseInfo
		expectedHost      string
		expectedProto     string
		expectedMeta      map[string]string
		expectedPorts     map[string]PortMapping
		expectedInstances []InstanceInfo
	}{
		{
			name: "full info with ports and metadata",
			input: backend.LeaseInfo{
				Host:     "test.example.com",
				Protocol: "https",
				Ports: map[string]backend.PortBinding{
					"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
				},
				Metadata: map[string]string{"key": "value"},
			},
			expectedHost:  "test.example.com",
			expectedProto: "https",
			expectedMeta:  map[string]string{"key": "value"},
			expectedPorts: map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 8080}},
		},
		{
			name:         "empty info",
			input:        backend.LeaseInfo{},
			expectedMeta: map[string]string{},
		},
		{
			name: "missing optional fields",
			input: backend.LeaseInfo{
				Host: "test.example.com",
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{},
		},
		{
			name: "metadata passed through",
			input: backend.LeaseInfo{
				Host:     "test.example.com",
				Metadata: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
			},
			expectedHost: "test.example.com",
			expectedMeta: map[string]string{"region": "us-east-1", "backend": "kubernetes"},
		},
		{
			name: "single instance with ports",
			input: backend.LeaseInfo{
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "abc123def456",
						Image:         "nginx:latest",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"80/tcp": {HostIP: "0.0.0.0", HostPort: "32768"},
						},
					},
				},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123def456",
					Image:         "nginx:latest",
					Status:        "running",
					Ports:         map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 32768}},
				},
			},
		},
		{
			name: "multiple instances",
			input: backend.LeaseInfo{
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "container1",
						Image:         "nginx:latest",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"80/tcp": {HostIP: "0.0.0.0", HostPort: "32768"},
						},
					},
					{
						InstanceIndex: 1,
						ContainerID:   "container2",
						Image:         "redis:alpine",
						Status:        "running",
						Ports: map[string]backend.PortBinding{
							"6379/tcp": {HostIP: "0.0.0.0", HostPort: "32769"},
						},
					},
				},
				Metadata: map[string]string{"backend": "docker"},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{"backend": "docker"},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "container1",
					Image:         "nginx:latest",
					Status:        "running",
					Ports:         map[string]PortMapping{"80/tcp": {HostIP: "0.0.0.0", HostPort: 32768}},
				},
				{
					InstanceIndex: 1,
					ContainerID:   "container2",
					Image:         "redis:alpine",
					Status:        "running",
					Ports:         map[string]PortMapping{"6379/tcp": {HostIP: "0.0.0.0", HostPort: 32769}},
				},
			},
		},
		{
			name: "instance without ports",
			input: backend.LeaseInfo{
				Host: "docker-host.example.com",
				Instances: []backend.LeaseInstance{
					{
						InstanceIndex: 0,
						ContainerID:   "abc123",
						Image:         "busybox:latest",
						Status:        "running",
					},
				},
			},
			expectedHost: "docker-host.example.com",
			expectedMeta: map[string]string{},
			expectedInstances: []InstanceInfo{
				{
					InstanceIndex: 0,
					ContainerID:   "abc123",
					Image:         "busybox:latest",
					Status:        "running",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractConnectionDetails(tt.input)

			assert.Equal(t, tt.expectedHost, result.Host)
			assert.Equal(t, tt.expectedProto, result.Protocol)
			assert.Len(t, result.Metadata, len(tt.expectedMeta))
			for k, v := range tt.expectedMeta {
				assert.Equal(t, v, result.Metadata[k])
			}
			assert.Len(t, result.Ports, len(tt.expectedPorts))
			for k, v := range tt.expectedPorts {
				assert.Equal(t, v, result.Ports[k])
			}

			// Verify instances
			assert.Len(t, result.Instances, len(tt.expectedInstances))
			for i, expected := range tt.expectedInstances {
				actual := result.Instances[i]
				assert.Equal(t, expected.InstanceIndex, actual.InstanceIndex)
				assert.Equal(t, expected.ContainerID, actual.ContainerID)
				assert.Equal(t, expected.Image, actual.Image)
				assert.Equal(t, expected.Status, actual.Status)
				assert.Equal(t, expected.FQDN, actual.FQDN)
				assert.Len(t, actual.Ports, len(expected.Ports))
				for k, v := range expected.Ports {
					assert.Equal(t, v, actual.Ports[k])
				}
			}
		})
	}
}

func TestExtractConnectionDetails_Services(t *testing.T) {
	t.Run("stack with two services", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 0,
							ContainerID:   "abc123",
							Image:         "nginx:latest",
							Status:        "running",
							Ports: map[string]backend.PortBinding{
								"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
							},
						},
					},
				},
				"db": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 0,
							ContainerID:   "def456",
							Image:         "postgres:16",
							Status:        "running",
						},
					},
				},
			},
		}

		result := extractConnectionDetails(input)
		assert.Equal(t, "docker-host.example.com", result.Host)
		assert.Nil(t, result.Instances, "stack response should not have flat instances")
		require.Len(t, result.Services, 2)

		// web service
		webSvc := result.Services["web"]
		require.Len(t, webSvc.Instances, 1)
		assert.Equal(t, 0, webSvc.Instances[0].InstanceIndex)
		assert.Equal(t, "abc123", webSvc.Instances[0].ContainerID)
		assert.Equal(t, "nginx:latest", webSvc.Instances[0].Image)
		assert.Equal(t, "running", webSvc.Instances[0].Status)
		require.Len(t, webSvc.Instances[0].Ports, 1)
		assert.Equal(t, PortMapping{HostIP: "0.0.0.0", HostPort: 8080}, webSvc.Instances[0].Ports["80/tcp"])

		// db service
		dbSvc := result.Services["db"]
		require.Len(t, dbSvc.Instances, 1)
		assert.Equal(t, "postgres:16", dbSvc.Instances[0].Image)
	})

	t.Run("service with instance index", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					Instances: []backend.LeaseInstance{
						{
							InstanceIndex: 2,
							ContainerID:   "abc123",
							Ports: map[string]backend.PortBinding{
								"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
							},
						},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		webSvc := result.Services["web"]
		require.Len(t, webSvc.Instances, 1)
		assert.Equal(t, 2, webSvc.Instances[0].InstanceIndex)
		require.Len(t, webSvc.Instances[0].Ports, 1)
		assert.Equal(t, PortMapping{HostIP: "0.0.0.0", HostPort: 8080}, webSvc.Instances[0].Ports["80/tcp"])
	})
}

func TestExtractConnectionDetails_FQDN(t *testing.T) {
	t.Run("direct fqdn in lease info", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			FQDN: "myapp.example.com",
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "myapp.example.com", result.FQDN)
	})

	t.Run("fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123", FQDN: "inst.example.com"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "inst.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "inst.example.com", result.Instances[0].FQDN)
	})

	t.Run("direct fqdn takes precedence over instance fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			FQDN: "top-level.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123", FQDN: "instance-level.example.com"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "top-level.example.com", result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Equal(t, "instance-level.example.com", result.Instances[0].FQDN)
	})

	t.Run("multi-instance each with unique fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "container0", FQDN: "0-abc1234.example.com"},
				{InstanceIndex: 1, ContainerID: "container1", FQDN: "1-def5678.example.com"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Equal(t, "0-abc1234.example.com", result.FQDN, "top-level propagated from first instance")
		require.Len(t, result.Instances, 2)
		assert.Equal(t, "0-abc1234.example.com", result.Instances[0].FQDN)
		assert.Equal(t, "1-def5678.example.com", result.Instances[1].FQDN)
	})

	t.Run("no fqdn anywhere", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Instances: []backend.LeaseInstance{
				{InstanceIndex: 0, ContainerID: "abc123"},
			},
		}
		result := extractConnectionDetails(input)
		assert.Empty(t, result.FQDN)
		require.Len(t, result.Instances, 1)
		assert.Empty(t, result.Instances[0].FQDN)
	})

	t.Run("service-level fqdn propagated from first instance", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "abc123", FQDN: "web.example.com"},
				}},
				"db": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "def456", FQDN: "db.example.com"},
				}},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 2)
		assert.Equal(t, "web.example.com", result.Services["web"].FQDN)
		assert.Equal(t, "db.example.com", result.Services["db"].FQDN)
		assert.Equal(t, "web.example.com", result.Services["web"].Instances[0].FQDN)
		assert.Equal(t, "db.example.com", result.Services["db"].Instances[0].FQDN)
	})

	t.Run("service with multi-instance unique fqdns", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "web0", FQDN: "web-0-abc.example.com"},
					{InstanceIndex: 1, ContainerID: "web1", FQDN: "web-1-def.example.com"},
				}},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		webSvc := result.Services["web"]
		assert.Equal(t, "web-0-abc.example.com", webSvc.FQDN, "service-level propagated from first instance")
		require.Len(t, webSvc.Instances, 2)
		assert.Equal(t, "web-0-abc.example.com", webSvc.Instances[0].FQDN)
		assert.Equal(t, "web-1-def.example.com", webSvc.Instances[1].FQDN)
	})

	t.Run("explicit service fqdn takes precedence over instance fqdn", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {
					FQDN: "explicit-web.example.com",
					Instances: []backend.LeaseInstance{
						{InstanceIndex: 0, ContainerID: "abc123", FQDN: "instance-web.example.com"},
					},
				},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		assert.Equal(t, "explicit-web.example.com", result.Services["web"].FQDN)
		assert.Equal(t, "instance-web.example.com", result.Services["web"].Instances[0].FQDN)
	})

	t.Run("service without fqdn in instances", func(t *testing.T) {
		input := backend.LeaseInfo{
			Host: "docker-host.example.com",
			Services: map[string]backend.LeaseService{
				"web": {Instances: []backend.LeaseInstance{
					{InstanceIndex: 0, ContainerID: "abc123"},
				}},
			},
		}
		result := extractConnectionDetails(input)
		require.Len(t, result.Services, 1)
		assert.Empty(t, result.Services["web"].FQDN)
		assert.Empty(t, result.Services["web"].Instances[0].FQDN)
	})
}

// TestGetLeaseConnection_TokenReplayProtection tests the token replay protection.
func TestGetLeaseConnection_TokenReplayProtection(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Mock chain client that returns an active lease
	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("replayed_token_rejected", func(t *testing.T) {
		dbPath := t.TempDir() + "/tokens.db"
		tokenTracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tokenTracker.Close()

		// Create a backend server
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  tokenTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// Create a valid token
		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		// First request should succeed
		req1 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req1.Header.Set("Authorization", "Bearer "+validToken)
		req1.SetPathValue("lease_uuid", leaseUUID)

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code, "first request status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		// Second request with same token should be rejected
		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+validToken)
		req2.SetPathValue("lease_uuid", leaseUUID)

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		assert.Equal(t, http.StatusUnauthorized, rec2.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec2.Body).Decode(&errResp))
		assert.Equal(t, "unauthorized", errResp.Error)
	})

	t.Run("different_tokens_both_succeed", func(t *testing.T) {
		dbPath := t.TempDir() + "/tokens.db"
		tokenTracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tokenTracker.Close()

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  tokenTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// Create two different tokens (different timestamps = different signatures)
		// Timestamps are Unix seconds, so we need different seconds for different signatures
		now := time.Now()
		token1 := testutil.CreateTestToken(kp, leaseUUID, now)
		token2 := testutil.CreateTestToken(kp, leaseUUID, now.Add(1*time.Second))

		// Both requests should succeed since they're different tokens
		req1 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req1.Header.Set("Authorization", "Bearer "+token1)
		req1.SetPathValue("lease_uuid", leaseUUID)

		rec1 := httptest.NewRecorder()
		h.GetLeaseConnection(rec1, req1)

		assert.Equal(t, http.StatusOK, rec1.Code, "first token status = %d, want %d; body: %s", rec1.Code, http.StatusOK, rec1.Body.String())

		req2 := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req2.Header.Set("Authorization", "Bearer "+token2)
		req2.SetPathValue("lease_uuid", leaseUUID)

		rec2 := httptest.NewRecorder()
		h.GetLeaseConnection(rec2, req2)

		assert.Equal(t, http.StatusOK, rec2.Code, "second token status = %d, want %d; body: %s", rec2.Code, http.StatusOK, rec2.Body.String())
	})

	t.Run("no_tracker_allows_replay", func(t *testing.T) {
		// When no token tracker is configured, replays should be allowed
		// (graceful degradation)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host": "test.example.com",
				"ports": map[string]any{
					"443/tcp": map[string]any{
						"host_ip":   "0.0.0.0",
						"host_port": "8443",
					},
				},
				"protocol": "https",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  nil, // No tracker
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		// Both requests should succeed without tracker
		for i := range 2 {
			req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.GetLeaseConnection(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code, "request %d status = %d, want %d; body: %s", i+1, rec.Code, http.StatusOK, rec.Body.String())
		}
	})
}

// TestGetLeaseConnection_ChainErrors tests chain-related error paths.
func TestGetLeaseConnection_ChainErrors(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	t.Run("chain_error_returns_500", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("lease_not_found_returns_404", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, nil // Lease not found
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different", // Different tenant
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("provider_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: testutil.ValidUUID3, // Different provider
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

// mockStatusChecker implements StatusChecker for testing.
type mockStatusChecker struct {
	hasPayload    map[string]bool
	isInFlight    map[string]bool
	inFlightCount int
}

func (m *mockStatusChecker) HasPayload(leaseUUID string) (bool, error) {
	if m.hasPayload == nil {
		return false, nil
	}
	return m.hasPayload[leaseUUID], nil
}

func (m *mockStatusChecker) IsInFlight(leaseUUID string) bool {
	if m.isInFlight == nil {
		return false
	}
	return m.isInFlight[leaseUUID]
}

func (m *mockStatusChecker) InFlightCount() int {
	return m.inFlightCount
}

// TestGetLeaseStatus tests the GetLeaseStatus endpoint.
func TestGetLeaseStatus(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	t.Run("pending_without_meta_hash", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     nil, // No payload required
					}, nil
				}
				return nil, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, "LEASE_STATE_PENDING", response.State)
		assert.False(t, response.RequiresPayload)
	})

	t.Run("pending_with_meta_hash_no_payload", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     []byte{1, 2, 3, 4}, // Has meta hash - requires payload
					}, nil
				}
				return nil, nil
			},
		}

		statusChecker := &mockStatusChecker{
			hasPayload: map[string]bool{leaseUUID: false},
			isInFlight: map[string]bool{leaseUUID: false},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: statusChecker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.True(t, response.RequiresPayload)
		assert.False(t, response.PayloadReceived)
		assert.False(t, response.ProvisioningStarted)
	})

	t.Run("pending_with_payload_received", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_PENDING,
						MetaHash:     []byte{1, 2, 3, 4},
					}, nil
				}
				return nil, nil
			},
		}

		statusChecker := &mockStatusChecker{
			hasPayload: map[string]bool{leaseUUID: true},
			isInFlight: map[string]bool{leaseUUID: true},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: statusChecker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.True(t, response.PayloadReceived)
		assert.True(t, response.ProvisioningStarted)
	})

	t.Run("active_lease", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			statusChecker: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
	})

	t.Run("active_with_provision_status", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID: leaseUUID,
					Status:    backend.ProvisionStatusReady,
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Equal(t, "ready", response.ProvisionStatus)
	})

	t.Run("active_with_updating_provision", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID: leaseUUID,
					Status:    backend.ProvisionStatusUpdating,
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Equal(t, "updating", response.ProvisionStatus)
	})

	t.Run("active_no_backend_router", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Empty(t, response.ProvisionStatus)
	})

	t.Run("active_backend_error", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_ACTIVE,
					}, nil
				}
				return nil, nil
			},
		}

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseStatusResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, "LEASE_STATE_ACTIVE", response.State)
		assert.Empty(t, response.ProvisionStatus)
	})

	t.Run("invalid_uuid_returns_400", func(t *testing.T) {
		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/not-a-uuid/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", "not-a-uuid")

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		// No Authorization header
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("lease_uuid_mismatch_returns_401", func(t *testing.T) {
		// Token is for different lease UUID
		differentLeaseToken := testutil.CreateTestToken(kp, testutil.ValidUUID3, time.Now())

		h := &Handlers{
			client:       nil,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+differentLeaseToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("lease_not_found_returns_404", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, nil // Lease not found
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("provider_mismatch_returns_403", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: testutil.ValidUUID3, // Different provider
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("chain_error_returns_500", func(t *testing.T) {
		chainClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return nil, fmt.Errorf("chain unavailable")
			},
		}

		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseStatus(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

// mockTokenTracker implements a mock TokenTracker for testing.
type mockTokenTracker struct {
	tryUseFunc  func(signature string) error
	healthyFunc func() error
}

func (m *mockTokenTracker) TryUse(signature string) error {
	if m.tryUseFunc != nil {
		return m.tryUseFunc(signature)
	}
	return nil
}

func (m *mockTokenTracker) Healthy() error {
	if m.healthyFunc != nil {
		return m.healthyFunc()
	}
	return nil
}

func (m *mockTokenTracker) Close() error {
	return nil
}

// TestTokenTracker_FailClosed tests that database errors result in 503 Service Unavailable.
func TestTokenTracker_FailClosed(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"host": "test.example.com",
			"ports": map[string]any{
				"443/tcp": map[string]any{
					"host_ip":   "0.0.0.0",
					"host_port": "8443",
				},
			},
			"protocol": "https",
		})
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
		},
	})
	require.NoError(t, err)

	t.Run("database_error_returns_503", func(t *testing.T) {
		// Create a mock token tracker that returns a database error
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return fmt.Errorf("bbolt: database not open")
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service temporarily unavailable", errResp.Error)
	})

	t.Run("replay_detected_returns_401", func(t *testing.T) {
		// Ensure replay detection still returns 401, not 503
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return ErrTokenAlreadyUsed
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "unauthorized", errResp.Error)
	})

	t.Run("success_returns_200", func(t *testing.T) {
		// Ensure successful token use still works
		mockTracker := &mockTokenTracker{
			tryUseFunc: func(signature string) error {
				return nil // Success
			},
		}

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			tokenTracker:  mockTracker,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	})

	t.Run("various_database_errors_return_503", func(t *testing.T) {
		dbErrors := []error{
			fmt.Errorf("bbolt: database not open"),
			fmt.Errorf("disk full"),
			fmt.Errorf("i/o timeout"),
			fmt.Errorf("database is locked"),
		}

		for _, dbErr := range dbErrors {
			t.Run(dbErr.Error(), func(t *testing.T) {
				mockTracker := &mockTokenTracker{
					tryUseFunc: func(signature string) error {
						return dbErr
					},
				}

				h := &Handlers{
					client:        chainClient,
					backendRouter: router,
					tokenTracker:  mockTracker,
					providerUUID:  providerUUID,
					bech32Prefix:  "manifest",
				}

				validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

				req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
				req.Header.Set("Authorization", "Bearer "+validToken)
				req.SetPathValue("lease_uuid", leaseUUID)

				rec := httptest.NewRecorder()
				h.GetLeaseConnection(rec, req)

				assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "status = %d, want %d for error %q", rec.Code, http.StatusServiceUnavailable, dbErr)
			})
		}
	})
}

// TestCallbackResponse_JSON tests the CallbackResponse type serialization.
func TestCallbackResponse_JSON(t *testing.T) {
	response := CallbackResponse{
		Status:  "already_processed",
		Message: "callback for this lease was already handled",
	}

	jsonBytes, err := json.Marshal(response)
	require.NoError(t, err)

	var decoded CallbackResponse
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))

	assert.Equal(t, response.Status, decoded.Status)
	assert.Equal(t, response.Message, decoded.Message)
}

// TestCallbackResponse_OmitEmptyMessage tests that empty message is omitted.
func TestCallbackResponse_OmitEmptyMessage(t *testing.T) {
	response := CallbackResponse{
		Status:  "ok",
		Message: "", // Should be omitted
	}

	jsonBytes, err := json.Marshal(response)
	require.NoError(t, err)

	jsonStr := string(jsonBytes)
	assert.False(t, strings.Contains(jsonStr, "message"), "JSON should not contain 'message' when empty, got %s", jsonStr)
}

// TestGetLeaseStatus_RedactsVerboseError_SurfacesReasonMessage pins the ENG-508
// security cut: a FAILED provision surfaces the curated machine reason + human
// message to the tenant and NEVER the verbose operator detail (host paths,
// stack traces). The wire type no longer carries last_error, so no host path or
// last_error key can appear in the tenant response.
func TestGetLeaseStatus_RedactsVerboseError_SurfacesReasonMessage(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(backend.ProvisionInfo{
				LeaseUUID:    leaseUUID,
				ProviderUUID: providerUUID,
				Status:       backend.ProvisionStatusFailed,
				FailCount:    1,
				Reason:       backend.ReasonContainerExited,
				Message:      "container exited unexpectedly",
			})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, "/data/fred/volumes", "tenant response must not leak host paths")
	assert.NotContains(t, body, "last_error", "the verbose last_error field must be gone from the wire")
	assert.Contains(t, body, `"reason":"ContainerExited"`)
	assert.Contains(t, body, `"message":"container exited unexpectedly"`)
}

// TestGetLeaseStatus_FailedEmptyReason_DefaultsUnknown pins the read-boundary
// Unknown default: a FAILED provision that reaches the API with no authored
// reason (legacy/pre-ENG-508 backend) must still surface a machine reason to the
// tenant, defaulting to "Unknown" rather than an empty string.
func TestGetLeaseStatus_FailedEmptyReason_DefaultsUnknown(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(backend.ProvisionInfo{
				LeaseUUID:    leaseUUID,
				ProviderUUID: providerUUID,
				Status:       backend.ProvisionStatusFailed,
				FailCount:    1,
				Reason:       "",
				Message:      "",
			})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.GetLeaseStatus(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"reason":"Unknown"`)
}

// TestGetLeaseProvision tests the GetLeaseProvision endpoint.
func TestGetLeaseProvision(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// Chain client that returns a lease (any state)
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("happy_path_ready", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID:    leaseUUID,
					ProviderUUID: providerUUID,
					Status:       backend.ProvisionStatusReady,
					FailCount:    0,
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		assert.Equal(t, "ready", response.Status)
		assert.Equal(t, 0, response.FailCount)
		assert.Empty(t, response.Reason)
		assert.Empty(t, response.Message)
	})

	t.Run("drained_placement_candidates_remain_readable", func(t *testing.T) {
		var placedCalls, routedCalls atomic.Int32
		placedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			placedCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(backend.ProvisionInfo{
				LeaseUUID: leaseUUID,
				Status:    backend.ProvisionStatusReady,
			})
		}))
		defer placedServer.Close()
		routedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			routedCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer routedServer.Close()

		placedBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name: "placed-drained", BaseURL: placedServer.URL, Timeout: 5 * time.Second,
		})
		routedBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name: "sku-routed", BaseURL: routedServer.URL, Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
			{Backend: placedBackend},
			{Backend: routedBackend, Match: backend.MatchCriteria{SKUs: []string{"sku-1"}}, IsDefault: true},
		}})
		require.NoError(t, err)

		readChain := &mockChainClient{getLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: kp.Address, ProviderUuid: providerUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		}}
		currentPlacement := placement.Placement{}
		h := &Handlers{
			client: readChain, backendRouter: router,
			placementLookup: &mockPlacementLookup{lookupFunc: func(string) placement.Placement {
				return currentPlacement
			}},
			providerUUID: providerUUID, bech32Prefix: "manifest",
		}
		request := func() *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, "/v1/leases/"+leaseUUID+"/provision", nil)
			req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
			req.SetPathValue("lease_uuid", leaseUUID)
			rec := httptest.NewRecorder()
			h.GetLeaseProvision(rec, req)
			return rec
		}

		for _, tc := range []struct {
			name      string
			placement placement.Placement
		}{
			{
				name: "confirmed owner with unresolved attempt",
				placement: placement.Placement{
					Backend: placedBackend.Name(), Attempt: placedBackend.Name(),
				},
			},
			{
				name:      "attempt-only candidate",
				placement: placement.Placement{Attempt: placedBackend.Name()},
			},
			{
				name: "conflict candidate",
				placement: placement.Placement{
					Conflict: true, ConflictBackends: []string{placedBackend.Name(), routedBackend.Name()},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				placedCalls.Store(0)
				routedCalls.Store(0)
				currentPlacement = tc.placement

				rec := request()

				require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
				assert.Equal(t, int32(1), placedCalls.Load())
				assert.Zero(t, routedCalls.Load(),
					"SKU fan-out must not skip a known placement candidate")
			})
		}

		placedCalls.Store(0)
		routedCalls.Store(0)
		currentPlacement = placement.Placement{Attempt: "removed-backend"}
		rec := request()
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())
		assert.Zero(t, placedCalls.Load())
		assert.Equal(t, int32(1), routedCalls.Load())
	})

	t.Run("happy_path_failed_with_error", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/provisions/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(backend.ProvisionInfo{
					LeaseUUID:    leaseUUID,
					ProviderUUID: providerUUID,
					Status:       backend.ProvisionStatusFailed,
					FailCount:    3,
					Reason:       backend.ReasonContainerExited,
					Message:      "container exited unexpectedly",
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "failed", response.Status)
		assert.Equal(t, 3, response.FailCount)
		assert.Equal(t, "ContainerExited", response.Reason)
		assert.Equal(t, "container exited unexpectedly", response.Message)
	})

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not provisioned"}`))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "provision not found", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("invalid_uuid_returns_400", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/not-a-uuid/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", "not-a-uuid")

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		mismatchClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:       mismatchClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("works_with_non_active_lease", func(t *testing.T) {
		// Provision diagnostics should work even for rejected/closed leases
		rejectedClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				if uuid == leaseUUID {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						Tenant:       kp.Address,
						ProviderUuid: providerUUID,
						State:        billingtypes.LEASE_STATE_REJECTED,
					}, nil
				}
				return nil, nil
			},
		}

		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(backend.ProvisionInfo{
				LeaseUUID: leaseUUID,
				Status:    backend.ProvisionStatusFailed,
				FailCount: 1,
				Reason:    backend.ReasonImagePullFailed,
				Message:   "image pull failed",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        rejectedClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/provision", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseProvision(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseProvisionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "failed", response.Status)
		assert.Equal(t, "ImagePullFailed", response.Reason)
		assert.Equal(t, "image pull failed", response.Message)
	})
}

// TestGetLeaseLogs tests the GetLeaseLogs endpoint.
func TestGetLeaseLogs(t *testing.T) {
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
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("happy_path_single_container", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/logs/"+leaseUUID) && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{
					"0": "Starting server...\nListening on :8080\n",
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseLogsResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		require.Len(t, response.Logs, 1)
		assert.Contains(t, response.Logs["0"], "Listening on :8080")
	})

	t.Run("happy_path_multiple_containers", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"0": "web server logs\n",
				"1": "worker logs\n",
			})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response LeaseLogsResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		require.Len(t, response.Logs, 2)
		assert.Equal(t, "web server logs\n", response.Logs["0"])
		assert.Equal(t, "worker logs\n", response.Logs["1"])
	})

	t.Run("tail_parameter_forwarded", func(t *testing.T) {
		var receivedTail string
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedTail = r.URL.Query().Get("tail")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"0": "logs\n"})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs?tail=50", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "50", receivedTail)
	})

	t.Run("tail_invalid_returns_400", func(t *testing.T) {
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: "http://unused",
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		tests := []struct {
			name  string
			query string
		}{
			{"negative", "?tail=-1"},
			{"zero", "?tail=0"},
			{"not_a_number", "?tail=abc"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
				req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs"+tt.query, nil)
				req.Header.Set("Authorization", "Bearer "+validToken)
				req.SetPathValue("lease_uuid", leaseUUID)

				rec := httptest.NewRecorder()
				h.GetLeaseLogs(rec, req)

				assert.Equal(t, http.StatusBadRequest, rec.Code, "query=%s", tt.query)

				var errResp ErrorResponse
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, "tail must be a positive integer", errResp.Error)
			})
		}
	})

	t.Run("tail_exceeds_max_returns_400", func(t *testing.T) {
		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: "http://unused",
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs?tail=10001", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "tail must not exceed 10000", errResp.Error)
	})

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not provisioned"}`))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "logs not found", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("missing_auth_returns_401", func(t *testing.T) {
		h := &Handlers{
			client:       chainClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("tenant_mismatch_returns_403", func(t *testing.T) {
		mismatchClient := &mockChainClient{
			getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       "manifest1different",
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			},
		}

		h := &Handlers{
			client:       mismatchClient,
			providerUUID: providerUUID,
			bech32Prefix: "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("default_tail_100", func(t *testing.T) {
		var receivedTail string
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedTail = r.URL.Query().Get("tail")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"0": "logs\n"})
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		// No ?tail= parameter — should default to 100
		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/logs", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseLogs(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "100", receivedTail)
	})
}

// TestRestartLease_BackendIntegration tests the backend integration path
// in RestartLease using httptest.Server and a real backend.Router.
func TestRestartLease_BackendIntegration(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("invalid_state_returns_409", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusConflict)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusConflict, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid state for restart", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_202", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/restart" && r.Method == "POST" {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restart", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.RestartLease(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var response map[string]string
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "restarting", response["status"])
	})
}

// TestUpdateLease_BackendIntegration tests the backend integration path
// in UpdateLease using httptest.Server and a real backend.Router.
// mockPersistCall records one OverwritePayload invocation.
type mockPersistCall struct {
	leaseUUID string
	payload   []byte
}

// mockPayloadPersister is the api-side fake for the ENG-619 persistence seam.
type mockPayloadPersister struct {
	calls []mockPersistCall
	err   error // when set, OverwritePayload fails
}

func (m *mockPayloadPersister) OverwritePayload(leaseUUID string, payload []byte) error {
	m.calls = append(m.calls, mockPersistCall{leaseUUID: leaseUUID, payload: append([]byte(nil), payload...)})
	return m.err
}

// updateTestBackend returns a router whose single backend answers /update with
// the given status, plus a pointer to the number of /update requests it saw.
func updateTestBackend(t *testing.T, status int, body string) (*backend.Router, *int) {
	t.Helper()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/update" && r.Method == "POST" {
			calls++
			w.WriteHeader(status)
			if body != "" {
				_, _ = w.Write([]byte(body))
			}
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(server.Close)

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	return router, &calls
}

func TestUpdateLease_BackendIntegration(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:           chainClient,
			backendRouter:    nil,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("missing_payload_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":""}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "payload is required", errResp.Error)
	})

	t.Run("invalid_body_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader("not json"))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid request body", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("invalid_state_returns_409", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusConflict)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusConflict, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "invalid state for update", errResp.Error)
	})

	t.Run("validation_error_returns_400", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid manifest"})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_202", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/update" && r.Method == "POST" {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}`
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var response map[string]string
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
		assert.Equal(t, "updating", response["status"])
	})

	// --- ENG-619: an update that reaches the backend must also reach the store ---

	t.Run("persists_updated_payload_after_backend_accepts", func(t *testing.T) {
		router, _ := updateTestBackend(t, http.StatusAccepted, "")
		persister := &mockPayloadPersister{}

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: persister,
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		body := `{"payload":"dGVzdA=="}` // "test"
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		require.Len(t, persister.calls, 1, "a successful update must persist the new payload")
		assert.Equal(t, leaseUUID, persister.calls[0].leaseUUID)
		assert.Equal(t, []byte("test"), persister.calls[0].payload,
			"the persisted payload must be the one sent to the backend")
	})

	t.Run("backend_rejection_does_not_persist", func(t *testing.T) {
		// Persist-after-success: a payload the backend refused must never reach
		// the store, or the next reprovision would replay a manifest that was
		// never deployed.
		router, _ := updateTestBackend(t, http.StatusBadRequest, `{"error":"invalid manifest"}`)
		persister := &mockPayloadPersister{}

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: persister,
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(`{"payload":"dGVzdA=="}`))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Empty(t, persister.calls, "a rejected update must not be persisted")
	})

	t.Run("persist_failure_returns_500", func(t *testing.T) {
		// The error branch: the backend is now running the new manifest but
		// nothing durable records it. Answering 202 here is the silent-revert
		// bug, so the tenant is told the update did not fully land.
		router, updateCalls := updateTestBackend(t, http.StatusAccepted, "")
		persister := &mockPayloadPersister{err: errors.New("disk full")}

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: persister,
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(`{"payload":"dGVzdA=="}`))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Equal(t, 1, *updateCalls, "the backend was still called — the tenant retries to re-apply and re-persist")
		require.Len(t, persister.calls, 1)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("missing_persister_returns_500_without_calling_backend", func(t *testing.T) {
		// Fail before touching the backend: a lease left running a manifest fred
		// has no durable record of is worse than an update that never happened.
		router, updateCalls := updateTestBackend(t, http.StatusAccepted, "")

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
			// payloadPersister deliberately nil
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(`{"payload":"dGVzdA=="}`))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Zero(t, *updateCalls, "the update must not be half-applied")
	})

	t.Run("sends_payload_hash_matching_the_payload", func(t *testing.T) {
		// payload_hash is part of the documented /update request; it was never
		// populated before ENG-619.
		var got backend.UpdateRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()

		client := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name: "test-backend", BaseURL: server.URL, Timeout: 5 * time.Second,
		})
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:           chainClient,
			backendRouter:    router,
			providerUUID:     providerUUID,
			bech32Prefix:     "manifest",
			payloadPersister: &mockPayloadPersister{},
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update", strings.NewReader(`{"payload":"dGVzdA=="}`))
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.UpdateLease(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		want := sha256.Sum256([]byte("test"))
		assert.Equal(t, hex.EncodeToString(want[:]), got.PayloadHash)
		assert.Equal(t, []byte("test"), got.Payload)
	})
}

// TestGetLeaseReleases_BackendIntegration tests the backend integration path
// in GetLeaseReleases using httptest.Server and a real backend.Router.
func TestGetLeaseReleases_BackendIntegration(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// GetLeaseReleases uses requireActive=false, so it calls GetLease
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("router_missing_returns_503", func(t *testing.T) {
		h := &Handlers{
			client:        chainClient,
			backendRouter: nil,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "service not configured", errResp.Error)
	})

	t.Run("not_provisioned_returns_404", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "lease not yet provisioned", errResp.Error)
	})

	t.Run("backend_error_returns_500", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "internal server error", errResp.Error)
	})

	t.Run("happy_path_returns_releases", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]backend.ReleaseInfo{
					{
						Version:   1,
						Image:     "nginx:1.25",
						Status:    "superseded",
						CreatedAt: now.Add(-1 * time.Hour),
					},
					{
						Version:   2,
						Image:     "nginx:1.26",
						Status:    "active",
						CreatedAt: now,
					},
				})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())

		var response LeaseReleasesResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Equal(t, leaseUUID, response.LeaseUUID)
		assert.Equal(t, kp.Address, response.Tenant)
		assert.Equal(t, providerUUID, response.ProviderUUID)
		require.Len(t, response.Releases, 2)
		assert.Equal(t, 1, response.Releases[0].Version)
		assert.Equal(t, "nginx:1.25", response.Releases[0].Image)
		assert.Equal(t, "superseded", response.Releases[0].Status)
		assert.Equal(t, 2, response.Releases[1].Version)
		assert.Equal(t, "nginx:1.26", response.Releases[1].Image)
		assert.Equal(t, "active", response.Releases[1].Status)
	})

	t.Run("empty_releases_returns_empty_array", func(t *testing.T) {
		backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/releases/"+leaseUUID && r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode([]backend.ReleaseInfo{})
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}))
		defer backendServer.Close()

		backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "test-backend",
			BaseURL: backendServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: backendClient, IsDefault: true},
			},
		})
		require.NoError(t, err)

		h := &Handlers{
			client:        chainClient,
			backendRouter: router,
			providerUUID:  providerUUID,
			bech32Prefix:  "manifest",
		}

		validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/releases", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseReleases(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var response LeaseReleasesResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

		assert.Empty(t, response.Releases)
	})
}

// --- Health endpoint stats tests ---

func TestHealthCheck_WithStatusChecker(t *testing.T) {
	sc := &mockStatusChecker{inFlightCount: 42}
	h := &Handlers{
		providerUUID:  testutil.ValidUUID1,
		bech32Prefix:  "manifest",
		statusChecker: sc,
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	require.NotNil(t, response.Stats, "stats should be present when statusChecker is configured")
	assert.Equal(t, 42, response.Stats.InFlightProvisions)
}

// mockPlacementLookup implements PlacementLookup for testing.
type mockPlacementLookup struct {
	lookupFunc  func(leaseUUID string) placement.Placement
	getFunc     func(leaseUUID string) string
	healthyFunc func() error
}

type mockBootstrapPlacementLookup struct {
	mockPlacementLookup
	bootstrapped bool
}

func (m *mockBootstrapPlacementLookup) InventoryBootstrapped() bool {
	return m.bootstrapped
}

func (m *mockPlacementLookup) Lookup(leaseUUID string) placement.Placement {
	if m.lookupFunc != nil {
		return m.lookupFunc(leaseUUID)
	}
	if m.getFunc != nil {
		if backendName := m.getFunc(leaseUUID); backendName != "" {
			return placement.Placement{Backend: backendName}
		}
	}
	return placement.Placement{}
}

func (m *mockPlacementLookup) Healthy() error {
	if m.healthyFunc != nil {
		return m.healthyFunc()
	}
	return nil
}

// TestResolveBackend_PlacementRouting tests that resolveBackend checks
// the placement store first and falls back to SKU routing.
func TestResolveBackend_PlacementRouting(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_ACTIVE,
				}, nil
			}
			return nil, nil
		},
	}

	t.Run("placement_routes_to_correct_backend", func(t *testing.T) {
		// Set up two backends — "placed-backend" has the lease via placement,
		// "default-backend" is the SKU fallback.
		placedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "placed-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer placedServer.Close()

		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "default-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		placedBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "placed-backend",
			BaseURL: placedServer.URL,
			Timeout: 5 * time.Second,
		})
		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: placedBackend},
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		placement := &mockPlacementLookup{
			getFunc: func(uuid string) string {
				if uuid == leaseUUID {
					return "placed-backend"
				}
				return ""
			},
		}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "placed-host.example.com", resp.Connection.Host,
			"should route to placement backend, not default")
	})

	// ENG-635: a placement record naming a backend the router does not know is
	// refused, not re-routed. Previously this fell through to SKU routing and
	// answered 200 with the DEFAULT backend's connection details — a confident
	// answer from a machine that never held the lease. The tenant would then act
	// on a host that does not serve their deployment.
	//
	// 503 rather than 404 is deliberate: a backend is usually absent because it
	// was paused, renamed or is mid-redeploy. "Temporarily unavailable" is true
	// and recoverable; "not found" invites the tenant to destroy and recreate,
	// turning an outage into real data loss.
	t.Run("unresolvable_placement_refuses_with_503", func(t *testing.T) {
		var defaultQueried atomic.Int32
		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defaultQueried.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "default-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		// Placement returns a backend name that doesn't exist in the router
		placement := &mockPlacementLookup{
			getFunc: func(uuid string) string {
				return "removed-backend"
			},
		}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
			"an unresolvable placement must refuse, not answer from another backend")
		assert.Zero(t, defaultQueried.Load(),
			"no substitute backend may be queried — asserting the status alone would "+
				"pass even if the wrong backend had been asked and its answer discarded")
	})

	t.Run("unresolved_attempt_refuses_with_503", func(t *testing.T) {
		var defaultQueried atomic.Int32
		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defaultQueried.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "default-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		placementLookup := &mockPlacementLookup{
			lookupFunc: func(uuid string) placement.Placement {
				return placement.Placement{Attempt: "attempted-backend"}
			},
		}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placementLookup,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
			"an unresolved attempt must remain unavailable until inventory resolves it")
		assert.Zero(t, defaultQueried.Load(),
			"SKU routing must not query a potentially different backend")
	})

	t.Run("no_placement_uses_sku_routing", func(t *testing.T) {
		defaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"host":     "sku-host.example.com",
				"protocol": "https",
				"ports":    map[string]any{},
			})
		}))
		defer defaultServer.Close()

		defaultBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:    "default-backend",
			BaseURL: defaultServer.URL,
			Timeout: 5 * time.Second,
		})

		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{
				{Backend: defaultBackend, IsDefault: true},
			},
		})
		require.NoError(t, err)

		// Placement returns empty string (no placement record)
		placement := &mockPlacementLookup{}

		h := &Handlers{
			client:          chainClient,
			backendRouter:   router,
			placementLookup: placement,
			providerUUID:    providerUUID,
			bech32Prefix:    "manifest",
		}

		req := httptest.NewRequest("GET", "/v1/leases/"+leaseUUID+"/connection", nil)
		req.Header.Set("Authorization", "Bearer "+validToken)
		req.SetPathValue("lease_uuid", leaseUUID)

		rec := httptest.NewRecorder()
		h.GetLeaseConnection(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ConnectionResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "sku-host.example.com", resp.Connection.Host)
	})
}

func TestHealthCheck_WithoutStatusChecker(t *testing.T) {
	h := &Handlers{
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
		// statusChecker is nil
	}

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	h.HealthCheck(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var response HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))

	assert.Equal(t, "healthy", response.Status)
	assert.Nil(t, response.Stats, "stats should be absent without statusChecker")
}

// --- GetWorkloads tests ---

// newFilteredProvisionServer returns an httptest.Server that serves provisions
// on GET /provisions, supporting the optional ?lease_uuid=... filter.
// All provisions are stored upfront; filter is applied per-request.
func newFilteredProvisionServer(t *testing.T, provisions []backend.ProvisionInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/provisions" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		filter := r.URL.Query()["lease_uuid"]
		out := provisions
		if len(filter) > 0 {
			wanted := make(map[string]struct{}, len(filter))
			for _, u := range filter {
				wanted[u] = struct{}{}
			}
			out = make([]backend.ProvisionInfo, 0, len(filter))
			for _, p := range provisions {
				if _, ok := wanted[p.LeaseUUID]; ok {
					out = append(out, p)
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(backend.ListProvisionsResponse{Provisions: out})
	}))
}

// newFailingServer returns an httptest.Server that responds 500 to all
// requests except /health.
func newFailingServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

// newWorkloadsHandler builds a Handlers with a backend router wired to the
// given backend entries.
func newWorkloadsHandler(t *testing.T, entries []backend.BackendEntry) *Handlers {
	t.Helper()
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	return &Handlers{
		providerUUID:  testutil.ValidUUID1,
		backendRouter: router,
	}
}

// callGetWorkloads invokes GetWorkloads with the given lease_uuid query params.
func callGetWorkloads(t *testing.T, h *Handlers, leaseUUIDs ...string) (int, WorkloadLookupResponse) {
	t.Helper()
	q := url.Values{"lease_uuid": leaseUUIDs}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)
	var resp WorkloadLookupResponse
	if rec.Code == http.StatusOK {
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	}
	return rec.Code, resp
}

func TestGetWorkloads_RequiresLeaseUUID(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	req := httptest.NewRequest("GET", "/workloads", nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "lease_uuid query parameter required")
}

func TestGetWorkloads_RejectsOverCap(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	tooMany := make([]string, backend.MaxLookupUUIDs+1)
	for i := range tooMany {
		tooMany[i] = testutil.ValidUUID2
	}
	q := url.Values{"lease_uuid": tooMany}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "too many lease_uuid values")
}

func TestGetWorkloads_RejectsBadUUID(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	q := url.Values{"lease_uuid": []string{"not-a-uuid"}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid lease_uuid")
}

func TestGetWorkloads_NilRouter(t *testing.T) {
	h := &Handlers{providerUUID: testutil.ValidUUID1}

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, response.Workloads)
	assert.Empty(t, response.Workloads)
	assert.NotNil(t, response.Warnings)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_NonStackImageRoundTrip(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID:    testutil.ValidUUID2,
			ProviderUUID: testutil.ValidUUID1,
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
			Image:        "registry.local:5000/manifest-network/fred:v1.2.3@sha256:deadbeef",
			SKU:          "docker-micro",
			Quantity:     2,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	w, ok := response.Workloads[testutil.ValidUUID2]
	require.True(t, ok, "lease should appear keyed by UUID")
	assert.Equal(t, backend.ProvisionStatusReady, w.Status)
	assert.Equal(t, "test-backend", w.BackendName)

	require.Len(t, w.Items, 1)
	assert.Equal(t, "docker-micro", w.Items[0].SKU)
	// Backend reports a host:port + tag + digest reference; the handler must
	// strip the tag and digest while keeping the registry port intact. This
	// is the end-to-end proof that provisionToWorkloadEntry routes image
	// fields through stripImageTag — a future regression that inlines a
	// naive "split on last colon" would fail here by returning "registry".
	assert.Equal(t, "registry.local:5000/manifest-network/fred", w.Items[0].Image)
	assert.Equal(t, 2, w.Items[0].Count)
	assert.Empty(t, w.Items[0].ServiceName)
}

func TestGetWorkloads_StackImageRoundTrip(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID:    testutil.ValidUUID2,
			ProviderUUID: testutil.ValidUUID1,
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
			Quantity:     3,
			Items: []backend.LeaseItem{
				{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
				{SKU: "docker-large", Quantity: 1, ServiceName: "db"},
			},
			ServiceImages: map[string]string{
				"web": "nginx:1.25",
				"db":  "postgres:16",
			},
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	w, ok := response.Workloads[testutil.ValidUUID2]
	require.True(t, ok)
	require.Len(t, w.Items, 2)

	// Items preserve the backend's order — match by ServiceName.
	byName := make(map[string]WorkloadItem, 2)
	for _, item := range w.Items {
		byName[item.ServiceName] = item
	}
	require.Contains(t, byName, "web")
	require.Contains(t, byName, "db")
	assert.Equal(t, "docker-micro", byName["web"].SKU)
	// Tags stripped on the way out — see stripImageTag.
	assert.Equal(t, "nginx", byName["web"].Image)
	assert.Equal(t, 2, byName["web"].Count)
	assert.Equal(t, "docker-large", byName["db"].SKU)
	assert.Equal(t, "postgres", byName["db"].Image)
	assert.Equal(t, 1, byName["db"].Count)
}

// TestGetWorkloads_DedupesRepeatedLeaseUUIDs locks the input-dedupe behavior:
// a request with the same lease_uuid repeated must call each backend with the
// deduped set (so the backend doesn't iterate-and-emit-twice) and must return
// exactly one entry in the response. Without dedupe, the merge loop would log
// a misleading "lease reported by multiple backends" warning.
func TestGetWorkloads_DedupesRepeatedLeaseUUIDs(t *testing.T) {
	var capturedFilter []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		capturedFilter = r.URL.Query()["lease_uuid"]
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
			Provisions: []backend.ProvisionInfo{
				{
					LeaseUUID: testutil.ValidUUID2,
					Status:    backend.ProvisionStatusReady,
					Image:     "nginx:1.25",
					SKU:       "docker-micro",
					Quantity:  1,
				},
			},
		})
	}))
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Send the same UUID three times.
	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID2, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	// Backend was called with exactly one lease_uuid (the dedupe happens before fan-out).
	assert.Equal(t, []string{testutil.ValidUUID2}, capturedFilter,
		"backend should receive the deduped UUID list, not the raw input")

	// Response contains exactly one workload entry, no warnings.
	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_FanOutAcrossBackends(t *testing.T) {
	srv1 := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer srv1.Close()

	srv2 := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID3,
			Status:    backend.ProvisionStatusReady,
			Image:     "redis:7",
			SKU:       "docker-large",
			Quantity:  1,
		},
	})
	defer srv2.Close()

	client1 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-1", BaseURL: srv1.URL, Timeout: 5 * time.Second,
	})
	client2 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-2", BaseURL: srv2.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: client1, IsDefault: true},
		{Backend: client2},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID3)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 2)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.Contains(t, response.Workloads, testutil.ValidUUID3)
	assert.Equal(t, "backend-1", response.Workloads[testutil.ValidUUID2].BackendName)
	assert.Equal(t, "backend-2", response.Workloads[testutil.ValidUUID3].BackendName)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_BackendErrorWarning(t *testing.T) {
	// One healthy backend has the lease; another backend is failing.
	// Expect: 200 OK, lease present in workloads map, warning naming the failed backend.
	healthySrv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer healthySrv.Close()

	failingSrv := newFailingServer(t)
	defer failingSrv.Close()

	healthyClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "healthy-backend", BaseURL: healthySrv.URL, Timeout: 5 * time.Second,
	})
	failingClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "failing-backend", BaseURL: failingSrv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: healthyClient, IsDefault: true},
		{Backend: failingClient},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)

	// Healthy backend's data is still present despite the other backend failing
	// (errgroup must NOT cancel siblings).
	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)

	require.Len(t, response.Warnings, 1)
	assert.Contains(t, response.Warnings[0], `backend "failing-backend" unavailable`)
}

func TestGetWorkloads_UnknownLeasesOmitted(t *testing.T) {
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Image:     "nginx:1.25",
			SKU:       "docker-micro",
			Quantity:  1,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Request both a known and an unknown lease.
	code, response := callGetWorkloads(t, h, testutil.ValidUUID2, testutil.ValidUUID3)
	assert.Equal(t, http.StatusOK, code)

	require.Len(t, response.Workloads, 1)
	assert.Contains(t, response.Workloads, testutil.ValidUUID2)
	assert.NotContains(t, response.Workloads, testutil.ValidUUID3)
	assert.Empty(t, response.Warnings)
}

func TestGetWorkloads_AllUnknownReturnsEmptyMap(t *testing.T) {
	srv := newFilteredProvisionServer(t, nil)
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	// Decode raw JSON to verify the wire format is `{}` and `[]`, not `null`.
	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"workloads":{}`, "empty workloads should serialize as object, not null")
	assert.Contains(t, body, `"warnings":[]`, "empty warnings should serialize as array, not null")
}

func TestGetWorkloads_AllBackendsFail(t *testing.T) {
	failingSrv1 := newFailingServer(t)
	defer failingSrv1.Close()
	failingSrv2 := newFailingServer(t)
	defer failingSrv2.Close()

	client1 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-1", BaseURL: failingSrv1.URL, Timeout: 5 * time.Second,
	})
	client2 := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "backend-2", BaseURL: failingSrv2.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{
		{Backend: client1, IsDefault: true},
		{Backend: client2},
	})

	code, response := callGetWorkloads(t, h, testutil.ValidUUID2)
	assert.Equal(t, http.StatusOK, code)
	assert.Empty(t, response.Workloads)
	assert.Len(t, response.Warnings, 2, "all backend failures should be surfaced as warnings")
}

func TestGetWorkloads_StackNilServiceImages(t *testing.T) {
	// Simulates cold restart where Items survive but StackManifest (and thus
	// ServiceImages) is nil. The "image" key should be absent from the JSON
	// (omitempty), not present as an empty string.
	srv := newFilteredProvisionServer(t, []backend.ProvisionInfo{
		{
			LeaseUUID: testutil.ValidUUID2,
			Status:    backend.ProvisionStatusReady,
			Quantity:  2,
			Items: []backend.LeaseItem{
				{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
			},
			ServiceImages: nil,
		},
	})
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequest("GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()
	h.GetWorkloads(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&raw))

	workloads, ok := raw["workloads"].(map[string]any)
	require.True(t, ok, "workloads should be a JSON object")
	require.Contains(t, workloads, testutil.ValidUUID2)

	entry, ok := workloads[testutil.ValidUUID2].(map[string]any)
	require.True(t, ok)

	itemsAny, ok := entry["items"].([]any)
	require.True(t, ok, "items should be a JSON array")
	require.Len(t, itemsAny, 1)

	item, ok := itemsAny[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "web", item["service_name"])
	assert.Equal(t, "docker-micro", item["sku"])
	_, hasImage := item["image"]
	assert.False(t, hasImage, "image key should be absent from JSON when ServiceImages is nil")
}

// TestStripImageTag locks the redaction contract. Cases fall into three
// groups: valid references (must be stripped correctly while preserving
// host:port), deliberately-malformed inputs (pinned so a future refactor
// toward a grammar-aware parser is a conscious decision, not a silent
// behavior change), and degenerate edge cases (empty, bare digest).
func TestStripImageTag(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		// --- Valid references ---
		{"empty", "", ""},
		{"no tag", "nginx", "nginx"},
		{"simple tag", "nginx:1.25", "nginx"},
		{"patch tag", "nginx:1.25.3", "nginx"},
		{"latest tag", "nginx:latest", "nginx"},
		{"dash tag", "nginx:1.25-alpine3.18", "nginx"},
		// Mixed case: refs are spec-lowercase but the helper must not
		// silently normalize — that would corrupt the admin UI display.
		{"mixed case registry", "GHCR.IO/Org/Repo:v1", "GHCR.IO/Org/Repo"},
		{"registry with org", "ghcr.io/manifest-network/fred:v1.2.3", "ghcr.io/manifest-network/fred"},
		// host:port is the critical preservation case. Naive "strip after
		// last colon" would collapse these to "registry" or "localhost".
		{"registry with port no tag", "registry.local:5000/org/repo", "registry.local:5000/org/repo"},
		{"registry with port and tag", "registry.local:5000/org/repo:v1", "registry.local:5000/org/repo"},
		{"localhost with port and tag", "localhost:5000/foo:v1", "localhost:5000/foo"},
		// Bare localhost without a port is a legitimate reference and
		// distinguishes "first segment has a dot" from "has a slash".
		{"localhost no port with tag", "localhost/foo:v1", "localhost/foo"},
		// IPv6 literal host — the colons inside the bracketed host are
		// before the last "/" so they pass through untouched. Pinned so a
		// future "parse host properly" rewrite doesn't silently regress.
		{"ipv6 host with port and tag", "[::1]:5000/foo:v1", "[::1]:5000/foo"},
		// --- Digest handling ---
		{"digest only", "nginx@sha256:abc123", "nginx"},
		{"tag and digest", "nginx:1.25@sha256:abc123", "nginx"},
		{"registry port tag digest", "registry.local:5000/org/repo:v1@sha256:abc", "registry.local:5000/org/repo"},
		// Port + digest with *no* tag — exercises the @-strip leaving the
		// host:port intact and the colon-after-slash rule then correctly
		// not-stripping the remaining port colon.
		{"registry port digest no tag", "registry.local:5000/org/repo@sha256:abc", "registry.local:5000/org/repo"},
		// --- Malformed / degenerate inputs (contract pins) ---
		// Empty tag / empty digest: return the name. These are garbage-in,
		// but the behavior is locked so a defensive trim doesn't change it.
		{"empty tag", "nginx:", "nginx"},
		{"empty digest", "nginx@", "nginx"},
		// Double "@" — not grammar-valid. Pinned so the "strip from first
		// @" semantics documented in the helper stays stable.
		{"double at", "nginx@sha256:a@b", "nginx"},
		// Colon before slash: invalid Docker ref. The helper leaves it
		// untouched because the "tag colon" is not after the last "/".
		// Pinned as a non-behavior — garbage-in, garbage-out.
		{"colon before slash", "foo:1.25/bar", "foo:1.25/bar"},
		// Bare digest with no name — degenerate; returns empty. Pinned so
		// a future "require non-empty repo" check is a conscious decision.
		{"bare digest", "@sha256:abc", ""},
		// Trailing slash: no tag to strip, no crash on colon == -1.
		{"trailing slash", "nginx/", "nginx/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stripImageTag(tc.in))
		})
	}
}

func TestGetWorkloads_ContextCancelled(t *testing.T) {
	// Simulate a slow backend that blocks until context is canceled,
	// then verify that GetWorkloads short-circuits with no body when the
	// request context is canceled mid-fan-out.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		// Server side: context canceled, just return.
	}))
	defer srv.Close()

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "slow-backend", BaseURL: srv.URL, Timeout: 5 * time.Second,
	})
	h := newWorkloadsHandler(t, []backend.BackendEntry{{Backend: client, IsDefault: true}})

	ctx, cancel := context.WithCancel(context.Background())
	q := url.Values{"lease_uuid": []string{testutil.ValidUUID2}}
	req := httptest.NewRequestWithContext(ctx, "GET", "/workloads?"+q.Encode(), nil)
	rec := httptest.NewRecorder()

	// Cancel after a brief delay so the goroutine has time to start.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	h.GetWorkloads(rec, req)

	// Handler should bail without writing a body. We don't strictly enforce a
	// status code (the recorder defaults to 200 if no header is written), but
	// the body must NOT be a populated WorkloadLookupResponse.
	body := rec.Body.String()
	assert.NotContains(t, body, `"workloads":`,
		"handler should not write a body when request context is canceled")
}

// fromLeaseUUID is a distinct UUID used as the "original retained lease" in
// RestoreLease tests. It must differ from leaseUUID (the new lease in the request
// path) so the handler can't accidentally confuse the two.
const fromLeaseUUID = "fedcba98-7654-3210-fedc-ba9876543210"

func newRestoreAuthorityForTest(
	t *testing.T,
	sourcePlacements PlacementLookup,
	router *backend.Router,
) *placement.Store {
	t.Helper()
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "restore-placements.db"))
	require.NoError(t, err)
	backendNames := make([]string, 0, len(router.Backends()))
	for _, backendClient := range router.Backends() {
		backendNames = append(backendNames, backendClient.Name())
	}
	require.NoError(t, store.ConfigureBackendTopology(backendNames))
	if sourcePlacements != nil {
		source := sourcePlacements.Lookup(fromLeaseUUID)
		if source.State() == placement.StateConfirmed && source.Attempt == "" {
			require.NoError(t, store.Confirm(fromLeaseUUID, source.Backend))
		}
	}
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	require.NoError(t, err)
	store.EndInventorySession(fence)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func newRestoreServiceForTest(
	t *testing.T,
	providerUUID string,
	targets restoreapp.TargetLeaseReader,
	router *backend.Router,
	sourcePlacements PlacementLookup,
	events restoreapp.EventSink,
) RestoreService {
	t.Helper()
	if sourcePlacements == nil || targets == nil || router == nil {
		return nil
	}
	service, err := restoreapp.NewService(restoreapp.Config{
		ProviderUUID: providerUUID,
		CallbackURL: func(operationID operation.OperationID) (string, error) {
			return provisioner.BuildCallbackURLForOperation("https://fred.example.test", operationID)
		},
		Targets: targets,
		Backends: restoreapp.BackendResolverFunc(func(name string) restoreapp.RestoreBackend {
			return router.GetBackendByName(name)
		}),
		Operations: operation.NewRegistry(),
		Authority:  newRestoreAuthorityForTest(t, sourcePlacements, router),
		Events:     events,
	})
	require.NoError(t, err)
	return service
}

// TestRestoreLease_ForwardsAnd202 verifies the happy path: backend /restore
// returns 202 and the handler responds 202 {"status":"provisioning"}, forwarding
// the from_lease_uuid in the body sent to the backend.
func TestRestoreLease_ForwardsAnd202(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// PENDING lease — restore is called on fresh, not-yet-active leases.
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	var receivedBody []byte
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			var err error
			receivedBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it instead of guessing an arbitrary backend (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())

	var response map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	assert.Equal(t, "provisioning", response["status"])

	// Verify backend received the from_lease_uuid.
	require.NotNil(t, receivedBody, "backend should have received a request body")
	var backendReq map[string]any
	require.NoError(t, json.Unmarshal(receivedBody, &backendReq))
	assert.Equal(t, fromLeaseUUID, backendReq["from_lease_uuid"])
}

// TestRestoreLease_RejectsNonPendingLease verifies that restore refuses a target
// lease that is not PENDING (e.g. ACTIVE or CLOSED) with 409 and never reaches the
// backend — so a tenant cannot deploy onto an already-active or unbilled-closed
// lease, mirroring the provisioning path's LEASE_STATE_PENDING gate.
func TestRestoreLease_RejectsNonPendingLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state billingtypes.LeaseState
	}{
		{"active", billingtypes.LEASE_STATE_ACTIVE},
		{"closed", billingtypes.LEASE_STATE_CLOSED},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
							State:        tc.state, // NOT pending
						}, nil
					}
					return nil, nil
				},
			}

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("backend must NOT be called for a non-pending lease: %s %s", r.Method, r.URL.Path)
			}))
			defer backendServer.Close()

			backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "test-backend",
				BaseURL: backendServer.URL,
				Timeout: 5 * time.Second,
			})
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, err)

			h := &Handlers{
				client:        chainClient,
				backendRouter: router,
				providerUUID:  providerUUID,
				bech32Prefix:  "manifest",
			}

			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestRestoreLease_RereadRejectsTargetThatBecameTerminal(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	var reads atomic.Int32
	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid != leaseUUID {
				return nil, nil
			}
			state := billingtypes.LEASE_STATE_PENDING
			if reads.Add(1) > 1 {
				state = billingtypes.LEASE_STATE_CLOSED
			}
			return &billingtypes.Lease{
				Uuid: uuid, Tenant: kp.Address,
				ProviderUuid: providerUUID, State: state,
			}, nil
		},
	}

	var backendCalls atomic.Int32
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backendServer.Close()
	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "test-backend", BaseURL: backendServer.URL, Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	sourcePlacements := &mockPlacementLookup{getFunc: func(uuid string) string {
		if uuid == fromLeaseUUID {
			return "test-backend"
		}
		return ""
	}}
	handlers := NewHandlers(HandlersConfig{
		Client:          chainClient,
		BackendRouter:   router,
		PlacementLookup: sourcePlacements,
		RestoreService: newRestoreServiceForTest(
			t, providerUUID, chainClient, router, sourcePlacements, nil,
		),
		ProviderUUID: providerUUID,
		Bech32Prefix: "manifest",
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/leases/"+leaseUUID+"/restore",
		strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
	request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	request.SetPathValue("lease_uuid", leaseUUID)
	response := httptest.NewRecorder()

	handlers.RestoreLease(response, request)

	assert.Equal(t, http.StatusConflict, response.Code, "body: %s", response.Body.String())
	assert.Equal(t, int32(2), reads.Load(),
		"restore must re-read after HTTP authentication while lifecycle claims are held")
	assert.Zero(t, backendCalls.Load(), "a target that closed in the delay must never dispatch")
}

// TestRestoreLease_NoRetention404 verifies that a 422 from the backend
// (ErrNotRetained) is surfaced as a 404 to the caller.
func TestRestoreLease_NoRetention404(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			// 422 Unprocessable Entity → ErrNotRetained in the HTTP client.
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and exercises the ErrNotRetained path (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)
}

// TestRestoreLease_InsufficientResources503 verifies that a 503 from the backend
// (ErrInsufficientResources via the HTTP client's 503→sentinel mapping) is
// surfaced as a 503 to the tenant, matching how Provision surfaces capacity —
// NOT a 409.
func TestRestoreLease_InsufficientResources503(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			// 503 → ErrInsufficientResources in the HTTP client.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and exercises the ErrInsufficientResources path (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "insufficient resources to restore", errResp.Error)
}

// TestRestoreLease_PendingLeaseAuthenticates verifies that requireActive=false
// allows a PENDING lease to authenticate successfully (i.e. the handler reaches
// the backend call rather than 404-ing on auth).
func TestRestoreLease_PendingLeaseAuthenticates(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// GetLease returns a PENDING lease; GetActiveLease would return nil (not found).
	backendCalled := false
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
		getActiveLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			// Returning nil simulates "not active" — would cause 404 if called.
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			backendCalled = true
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// Placement maps the source lease to "test-backend" so the handler routes to
	// it and can verify requireActive=false auth (ENG-333).
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	// The handler must NOT 404 on auth — it should reach the backend.
	assert.NotEqual(t, http.StatusNotFound, rec.Code, "handler must not 404 on PENDING lease auth; body: %s", rec.Body.String())
	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())
	assert.True(t, backendCalled, "backend should have been called for a PENDING lease")
}

// TestRestoreLease_MalformedFromLease400 verifies that an invalid source UUID,
// including the target itself, is rejected with 400 before backend dispatch.
func TestRestoreLease_MalformedFromLease400(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend should NOT be called for a malformed from_lease_uuid: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	invalidValues := []string{
		"not-a-uuid", "../etc/passwd", "short", "00000000000000000000000000000000x",
		leaseUUID,
	}
	for _, bad := range invalidValues {
		t.Run(bad, func(t *testing.T) {
			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			reqBody := `{"from_lease_uuid":"` + bad + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code, "from_lease_uuid=%q body: %s", bad, rec.Body.String())

			var errResp ErrorResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Contains(t, errResp.Error, "uuid", "error message should mention uuid")
		})
	}
}

// TestRestoreLease_MissingFromLease400 verifies that an absent or empty
// from_lease_uuid is rejected with 400 before the backend is contacted.
func TestRestoreLease_MissingFromLease400(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend should NOT be called for missing from_lease_uuid: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	cases := []struct {
		name string
		body string
	}{
		{"empty_field", `{"from_lease_uuid":""}`},
		{"absent_field", `{}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code, "case=%q body: %s", tc.name, rec.Body.String())

			var errResp ErrorResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, "from_lease_uuid is required", errResp.Error)
		})
	}
}

// TestRestoreLease_RoutesToSourcePlacementBackend verifies that the handler
// routes the Restore call to the backend recorded in placement for the SOURCE
// lease (from_lease_uuid), not to an arbitrary backend resolved from the new
// lease (which has no placement). This is the core ENG-333 invariant: retained
// volumes live only on the backend that originally provisioned the source lease.
func TestRestoreLease_RoutesToSourcePlacementBackend(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	// Two named backends: only "backend-src" should receive the Restore call.
	srcCalled := false
	srcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			srcCalled = true
			w.WriteHeader(http.StatusAccepted)
			return
		}
		t.Errorf("unexpected request on src backend: %s %s", r.Method, r.URL.Path)
	}))
	defer srcServer.Close()

	otherCalled := false
	otherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherCalled = true
		t.Errorf("backend-other must NOT be called, got: %s %s", r.Method, r.URL.Path)
	}))
	defer otherServer.Close()

	srcBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-src",
		BaseURL: srcServer.URL,
		Timeout: 5 * time.Second,
	})
	otherBackend := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "backend-other",
		BaseURL: otherServer.URL,
		Timeout: 5 * time.Second,
	})
	// backend-other is the SKU-routing default; backend-src is reachable ONLY via
	// the source lease's placement. This makes the test a real regression guard:
	// if RestoreLease reverted to new-lease routing (resolveBackend → Route(sku)),
	// it would land on the default backend-other and the test would FAIL — not
	// silently pass as it would if backend-src were also the default.
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: srcBackend},                    // NOT default; reachable only via placement[fromLeaseUUID]
			{Backend: otherBackend, IsDefault: true}, // default; old new-lease routing would land here
		},
	})
	require.NoError(t, err)

	// Placement maps the SOURCE lease to "backend-src"; the new lease has no placement.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "backend-src"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code, "body: %s", rec.Body.String())
	assert.True(t, srcCalled, "backend-src should have received the Restore call")
	assert.False(t, otherCalled, "backend-other must NOT have been called")
}

// TestRestoreLease_NoSourcePlacement_Returns404 verifies that when the
// placement lookup returns "" for the source lease (no retained data recorded
// on any backend), the handler responds 404 without contacting any backend.
func TestRestoreLease_NoSourcePlacement_Returns404(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend should be called when placement is missing: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// placementLookup always returns "" — no recorded placement for the source lease.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string { return "" },
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)
}

// ENG-635: the sibling of the 404 case above, and the reason the two must not
// be collapsed. A source lease with NO placement record genuinely has no
// retained data anywhere, so 404 is truthful. A source lease WITH a record
// naming a backend the router does not know is a different answer: the data
// exists, on a machine fred currently cannot reach — usually one that was
// paused, renamed or is mid-redeploy.
//
// Answering 404 there tells a tenant their data is gone and invites them to
// destroy and recreate the deployment, which turns a recoverable outage into
// real data loss. 503 is both true and actionable.
func TestRestoreLease_UnresolvableSourcePlacement_Returns503(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend may be called when the source placement does not resolve: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// A record EXISTS, but names a backend absent from the router.
	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string { return "removed-backend" },
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.NotEqual(t, "no retained data found for that lease", errResp.Error,
		"must not tell the tenant their data is gone when it is merely unreachable")
}

// TestRestoreLease_PlacementDisabled_Returns503 verifies that when placement
// routing is disabled (placementLookup is nil), restore returns 503 "service
// not configured" — a service-misconfiguration condition — rather than a
// misleading 404 "no retained data" (Copilot review on PR #120). This matches
// how authenticateLease treats a nil backendRouter.
func TestRestoreLease_PlacementDisabled_Returns503(t *testing.T) {
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
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("no backend should be called when placement routing is disabled: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	// placementLookup is nil => placement routing disabled (service misconfig).
	// backendRouter is non-nil so authenticateLease passes; the nil placement
	// lookup must then surface as 503, not 404.
	h := &Handlers{
		client:        chainClient,
		backendRouter: router,
		providerUUID:  providerUUID,
		bech32Prefix:  "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "body: %s", rec.Body.String())

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, errMsgServiceNotConfigured, errResp.Error)
}

// --- ENG-361: restore-route security gates (pre-mainnet) -------------------
//
// The headline cross-tenant data-theft gate (rec.Tenant != req.Tenant) is
// pinned at the backend layer by TestRestore_TenantMismatch_CollapsesToNotRetained
// (internal/backend/docker/restore_test.go). The three tests below pin the
// restore ROUTE's own enforcement so a refactor of RestoreLease — that skipped
// authenticateLease, or forwarded a tenant other than the ADR-036 signer to the
// backend — would be caught here, not only by inspecting that restore is wired
// with the same withAuthRL wrapper as restart/update.

// TestRestoreLease_RejectsUnauthenticated verifies PROPERTY 1 (ADR-036 auth) for
// the restore route: a missing, expired, or wrong-lease-bound token is rejected
// with 401 before any chain or backend work. Restore is authenticated exactly
// like restart/update — not an unauthenticated outlier.
func TestRestoreLease_RejectsUnauthenticated(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	for _, tc := range []struct {
		name        string
		authHeader  string // "" means no Authorization header
		description string
	}{
		{"missing_auth", "", "no Authorization header at all"},
		{"expired_token", "Bearer " + testutil.CreateExpiredToken(kp, leaseUUID), "token past MaxTokenAge"},
		// A token validly signed for a DIFFERENT lease must not authorize restore
		// onto leaseUUID: token.LeaseUUID is a signed field, so it cannot be
		// retargeted without the tenant's key (authenticateLeaseToken rejects a
		// token whose signed LeaseUUID does not match the path lease).
		{"wrong_target_lease_binding", "Bearer " + testutil.CreateTestToken(kp, testutil.ValidUUID3, time.Now()), "token bound to a different lease UUID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// client is nil and no backend is wired: auth must fail before either is
			// touched. A regression that reached them would nil-deref and fail loudly.
			h := &Handlers{
				client:       nil,
				providerUUID: providerUUID,
				bech32Prefix: "manifest",
			}

			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code, "%s: body: %s", tc.description, rec.Body.String())
		})
	}
}

// TestRestoreLease_RejectsNonOwnedTarget verifies PROPERTY 2a (caller owns the
// TARGET lease) for the restore route: even with a cryptographically valid token,
// a caller who does not own the path (target) lease — or whose lease belongs to a
// different provider — is rejected with 403 and the backend is never contacted.
func TestRestoreLease_RejectsNonOwnedTarget(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	for _, tc := range []struct {
		name  string
		lease *billingtypes.Lease
	}{
		{
			name: "tenant_mismatch",
			// Target lease is owned by a DIFFERENT tenant than the signer.
			lease: &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       "manifest1different",
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			},
		},
		{
			name: "provider_mismatch",
			// Target lease belongs to a different provider.
			lease: &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID3,
				State:        billingtypes.LEASE_STATE_PENDING,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainClient := &mockChainClient{
				getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
					if uuid == leaseUUID {
						return tc.lease, nil
					}
					return nil, nil
				},
			}

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("backend must NOT be called when the caller does not own the target: %s %s", r.Method, r.URL.Path)
			}))
			defer backendServer.Close()

			backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "test-backend",
				BaseURL: backendServer.URL,
				Timeout: 5 * time.Second,
			})
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, err)

			h := &Handlers{
				client:        chainClient,
				backendRouter: router,
				providerUUID:  providerUUID,
				bech32Prefix:  "manifest",
			}

			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestRestoreLease_ForwardsSignerTenantOnCrossTenantSource verifies PROPERTY 2b
// plumbing at the restore route: the handler forwards the ADR-036 SIGNER's tenant
// (auth.Token.Tenant) to the backend — never a body- or path-derived value — and
// surfaces the backend's ErrNotRetained as an indistinguishable 404. This is what
// makes the backend's source-ownership gate (rec.Tenant != req.Tenant) effective:
// the caller owns the fresh TARGET lease but supplies another tenant's retained
// from_lease_uuid; the backend (here simulated with 422) sees the signer's tenant
// and rejects, and the caller cannot tell cross-tenant from not-found.
func TestRestoreLease_ForwardsSignerTenantOnCrossTenantSource(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	// The caller legitimately owns the fresh PENDING target lease.
	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	var receivedBody []byte
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			var err error
			receivedBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			// Simulate the docker backend's cross-tenant rejection
			// (rec.Tenant(other) != req.Tenant(signer) -> ErrNotRetained).
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	// Cross-tenant source is indistinguishable from not-found: 404, same message.
	assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "no retained data found for that lease", errResp.Error)

	// The handler must have forwarded the SIGNER's tenant — the value the backend
	// gate compares against the retained record — not the body's from_lease_uuid
	// or any caller-supplied field. (RestoreRequest carries no tenant field for the
	// caller to set; this pins that RestoreLease sets the backend request's Tenant
	// from the authenticated token, i.e. Tenant: auth.Token.Tenant.)
	require.NotNil(t, receivedBody, "backend should have received a request body")
	var backendReq map[string]any
	require.NoError(t, json.Unmarshal(receivedBody, &backendReq))
	assert.Equal(t, kp.Address, backendReq["tenant"], "handler must forward the ADR-036 signer's tenant to the backend")
	assert.Equal(t, fromLeaseUUID, backendReq["from_lease_uuid"], "handler must forward the requested source lease")
}

// TestRestoreLease_DemoteExceedsTier422 verifies that a backend 422 with
// code="demote_exceeds_tier" (ErrDemoteDataExceedsTier) surfaces as a 422 to
// the fred-api caller — the third layer of the three-layer 422 propagation
// (docker-backend → HTTPClient → fred-api handler).
func TestRestoreLease_DemoteExceedsTier422(t *testing.T) {
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid:         leaseUUID,
					Tenant:       kp.Address,
					ProviderUuid: providerUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/restore" && r.Method == "POST" {
			// 422 with code="demote_exceeds_tier" — retained data is too large for
			// the requested smaller tier (ENG-438).
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"retained data exceeds the requested smaller tier","code":"demote_exceeds_tier"}`))
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "test-backend-demote",
		BaseURL: backendServer.URL,
		Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	placement := &mockPlacementLookup{
		getFunc: func(uuid string) string {
			if uuid == fromLeaseUUID {
				return "test-backend-demote"
			}
			return ""
		},
	}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
	reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+validToken)
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", rec.Body.String())

	// ENG-620: the 422 message must carry the backend's detail EXACTLY ONCE.
	// It used to be doubled — fred re-prefixed a message that already opened
	// with the same sentinel text — so tenants read "retained data exceeds the
	// requested smaller tier: retained data exceeds the requested smaller
	// tier: …".
	assert.Equal(t, 1, strings.Count(rec.Body.String(), "retained data exceeds the requested smaller tier"),
		"the demote 422 detail must appear exactly once, not doubled: %s", rec.Body.String())
}

// TestRestoreLease_MalformedBackendErrorBodyIsNotForwarded is the ENG-620
// regression at the tenant boundary: whatever a backend puts in a 4xx body it
// did not author to contract, none of it reaches the tenant.
//
// The client-layer twin (TestHTTPClient_MalformedErrorBody_IsNeverForwarded)
// proves the error value is clean; this proves the HTTP response is, which is
// what an attacker actually reads.
func TestRestoreLease_MalformedBackendErrorBodyIsNotForwarded(t *testing.T) {
	const hostPathSentinel = `btrfs qgroup show /var/lib/fred/volumes/fred-abc-app-0: exit status 1`

	cases := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{"text/plain 400", http.StatusBadRequest, "text/plain", hostPathSentinel},
		{"html 400", http.StatusBadRequest, "text/html", "<html>400 " + hostPathSentinel + "</html>"},
		{"truncated json 400", http.StatusBadRequest, "application/json", `{"error":"` + hostPathSentinel},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kp := testutil.NewTestKeyPair("test-tenant")
			leaseUUID := testutil.ValidUUID1
			providerUUID := testutil.ValidUUID2

			chainClient := &mockChainClient{
				getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
					if uuid == leaseUUID {
						return &billingtypes.Lease{
							Uuid:         leaseUUID,
							Tenant:       kp.Address,
							ProviderUuid: providerUUID,
							State:        billingtypes.LEASE_STATE_PENDING,
						}, nil
					}
					return nil, nil
				},
			}

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer backendServer.Close()

			backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
				Name:    "test-backend-malformed",
				BaseURL: backendServer.URL,
				Timeout: 5 * time.Second,
			})
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, err)

			placement := &mockPlacementLookup{
				getFunc: func(uuid string) string {
					if uuid == fromLeaseUUID {
						return "test-backend-malformed"
					}
					return ""
				},
			}

			h := &Handlers{
				client:          chainClient,
				backendRouter:   router,
				placementLookup: placement,
				restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
				providerUUID:    providerUUID,
				bech32Prefix:    "manifest",
			}

			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			reqBody := `{"from_lease_uuid":"` + fromLeaseUUID + `"}`
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore", strings.NewReader(reqBody))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			body := rec.Body.String()
			assert.NotContains(t, body, hostPathSentinel,
				"the raw backend body must never reach the tenant response")
			assert.NotContains(t, body, "qgroup", "no fragment of the raw body may reach the tenant")
			assert.NotContains(t, body, "/var/lib/fred", "no host path may reach the tenant")
			assert.Equal(t, http.StatusBadGateway, rec.Code,
				"an off-contract backend body is an upstream fault, not a tenant one: %s", body)
		})
	}
}

// detailErrorFromBackend builds the error a real backend 400 produces, by
// driving the production client against an httptest backend whose response
// body carries the given detail. Deliberately not a constructor exported from
// internal/backend: that would be a test-only hook in production code, and it
// would also let this test pass against a detail shape the wire cannot
// actually produce.
func detailErrorFromBackend(t *testing.T, detail string) error {
	t.Helper()

	body, err := json.Marshal(map[string]string{"error": detail})
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	client := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name:    "detail-src",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})
	err = client.Update(context.Background(), backend.UpdateRequest{LeaseUUID: testutil.ValidUUID1})
	require.ErrorIs(t, err, backend.ErrValidation)
	return err
}

// TestTenantDetail covers the helper that decides what a backend-originated
// 4xx says to a tenant. Its three jobs are all security-relevant: relay only
// the detail authored inside a validated envelope, never the wrapped chain;
// strip control characters, which are the log-forging and terminal-escape
// vector in relayed text; and bound the length.
func TestTenantDetail(t *testing.T) {
	const fallback = "the request was rejected as invalid"

	t.Run("bare sentinel falls back to fred's own message", func(t *testing.T) {
		// A sentinel carries no backend detail, so its own text must NOT be
		// relayed as though a backend had authored it for the tenant.
		got := tenantDetail(backend.ErrValidation, fallback)
		assert.Equal(t, fallback, got)
	})

	t.Run("wrapped non-detail error falls back", func(t *testing.T) {
		// fmt.Errorf-wrapped chains are exactly what used to reach tenants.
		err := fmt.Errorf("adopt retained volumes: %w", backend.ErrValidation)
		got := tenantDetail(err, fallback)
		assert.Equal(t, fallback, got)
		assert.NotContains(t, got, "adopt retained volumes")
	})

	t.Run("strips control characters", func(t *testing.T) {
		// \r and ESC forge extra lines / terminal sequences in anything that
		// renders the body; \n and \t collapse to a space so words stay apart.
		err := detailErrorFromBackend(t, "bad\rmanifest\x1b[31m: field\nx\ty")
		got := tenantDetail(err, fallback)
		assert.NotContains(t, got, "\r")
		assert.NotContains(t, got, "\x1b")
		assert.NotContains(t, got, "\n")
		assert.Equal(t, "badmanifest[31m: field x y", got)
	})

	t.Run("bounds an overlong detail on a rune boundary", func(t *testing.T) {
		// Multi-byte runes so a naive byte slice would split one. 1000 runes =
		// 2000 bytes: comfortably over maxTenantDetailBytes but under the
		// client's 4 KiB body read cap, which is the OTHER bound and kicks in
		// first — a body large enough to hit it is truncated mid-JSON and
		// becomes ErrMalformedErrorBody instead of arriving here.
		err := detailErrorFromBackend(t, strings.Repeat("é", 1000))
		got := tenantDetail(err, fallback)
		assert.LessOrEqual(t, len(got), maxTenantDetailBytes+len("…"))
		assert.True(t, utf8.ValidString(got), "must not split a multi-byte rune: %q", got)
		assert.True(t, strings.HasSuffix(got, "…"))
	})

	t.Run("relays a real detail unchanged", func(t *testing.T) {
		const detail = `service "web": depends_on references unknown service "db"`
		err := detailErrorFromBackend(t, detail)
		assert.Equal(t, detail, tenantDetail(err, fallback))
	})

	t.Run("detail of only control characters falls back", func(t *testing.T) {
		err := detailErrorFromBackend(t, "\x00\x01\x02")
		assert.Equal(t, fallback, tenantDetail(err, fallback))
	})
}

// TestRestoreLease_422KeepsLoadtestContract pins the substrings an EXTERNAL
// consumer greps out of the 422 body. manifest-loadtest scenario
// 17-restore-cross-tier.js hard-checks `body.toLowerCase().includes('tier')`
// (:212) and separates the two refusal arms with /bytes used exceeds disk_mb=/
// and /unable to verify|unmeasur/ (:165, :185). ENG-620 changed how this
// message is composed, so the contract is asserted here rather than discovered
// by a red loadtest run.
//
// The two bodies below are what checkDemoteFit actually produces
// (internal/backend/docker/restore.go), wrapped in the docker-backend envelope.
func TestRestoreLease_422KeepsLoadtestContract(t *testing.T) {
	arms := map[string]string{
		"measured_exceeds": `retained data exceeds the requested smaller tier: service "app": 2097152 bytes used exceeds disk_mb=1 cap (1048576 bytes)`,
		"unmeasurable":     `retained data exceeds the requested smaller tier: service "app": unable to verify retained data fits the requested tier`,
	}

	for arm, backendMsg := range arms {
		t.Run(arm, func(t *testing.T) {
			kp := testutil.NewTestKeyPair("test-tenant")
			leaseUUID := testutil.ValidUUID1
			providerUUID := testutil.ValidUUID2

			chainClient := &mockChainClient{
				getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
					if uuid == leaseUUID {
						return &billingtypes.Lease{
							Uuid: leaseUUID, Tenant: kp.Address,
							ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_PENDING,
						}, nil
					}
					return nil, nil
				},
			}

			body, err := json.Marshal(map[string]string{
				"error": backendMsg,
				"code":  backend.CodeDemoteExceedsTier,
			})
			require.NoError(t, err)

			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write(body)
			}))
			defer backendServer.Close()

			backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
				Name: "loadtest-contract", BaseURL: backendServer.URL, Timeout: 5 * time.Second,
			})
			router, rerr := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, rerr)
			placement := &mockPlacementLookup{getFunc: func(uuid string) string {
				if uuid == fromLeaseUUID {
					return "loadtest-contract"
				}
				return ""
			}}

			h := &Handlers{
				client:          chainClient,
				backendRouter:   router,
				placementLookup: placement,
				restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
				providerUUID:    providerUUID,
				bech32Prefix:    "manifest",
			}

			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore",
				strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
			req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.RestoreLease(rec, req)

			got := rec.Body.String()
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, "body: %s", got)
			assert.Contains(t, strings.ToLower(got), "tier",
				"manifest-loadtest 17-restore-cross-tier.js:212 hard-checks for 'tier'")
			assert.Equal(t, 1, strings.Count(got, "retained data exceeds the requested smaller tier"),
				"the sentinel text must appear exactly once, not doubled: %s", got)

			switch arm {
			case "measured_exceeds":
				assert.Contains(t, got, "bytes used exceeds disk_mb=",
					"loadtest :185 distinguishes the measured arm by this substring")
			case "unmeasurable":
				assert.Contains(t, got, "unable to verify",
					"loadtest :165/:185 distinguishes the unmeasurable arm by this substring")
			}
		})
	}
}

// TestUpdateLease_MalformedBackendErrorBodyIsNotForwarded is the UpdateLease
// twin of TestRestoreLease_MalformedBackendErrorBodyIsNotForwarded. Both
// handlers are named by ENG-620 and they classify independently — restore uses
// a switch, update a chain of ifs — so covering only one lets the other
// regress silently.
//
// It also pins a property restore has no equivalent of: a rejected update must
// not reach the payload store. UpdateLease persists AFTER the backend accepts,
// precisely so a manifest the backend refused is never replayed by the next
// reprovision (ENG-619). A malformed rejection is still a rejection.
func TestUpdateLease_MalformedBackendErrorBodyIsNotForwarded(t *testing.T) {
	const hostPathSentinel = `btrfs qgroup show /var/lib/fred/volumes/fred-abc-app-0: exit status 1`

	for name, body := range map[string]string{
		"text/plain 400":     hostPathSentinel,
		"html 400":           "<html>400 " + hostPathSentinel + "</html>",
		"truncated json 400": `{"error":"` + hostPathSentinel,
		"foreign json 400":   `{"message":"` + hostPathSentinel + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			kp := testutil.NewTestKeyPair("test-tenant")
			leaseUUID := testutil.ValidUUID1
			providerUUID := testutil.ValidUUID2

			chainClient := &mockChainClient{
				getActiveLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
					if uuid == leaseUUID {
						return &billingtypes.Lease{
							Uuid: leaseUUID, Tenant: kp.Address,
							ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_ACTIVE,
						}, nil
					}
					return nil, nil
				},
			}

			router, calls := updateTestBackend(t, http.StatusBadRequest, body)
			persister := &mockPayloadPersister{}

			h := &Handlers{
				client:           chainClient,
				backendRouter:    router,
				providerUUID:     providerUUID,
				bech32Prefix:     "manifest",
				payloadPersister: persister,
			}

			validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())
			req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/update",
				strings.NewReader(`{"payload":"dGVzdA=="}`))
			req.Header.Set("Authorization", "Bearer "+validToken)
			req.SetPathValue("lease_uuid", leaseUUID)

			rec := httptest.NewRecorder()
			h.UpdateLease(rec, req)

			respBody := rec.Body.String()
			assert.Equal(t, 1, *calls, "the backend must have been called")
			assert.NotContains(t, respBody, hostPathSentinel,
				"the raw backend body must never reach the tenant response")
			assert.NotContains(t, respBody, "qgroup", "no fragment of the raw body may reach the tenant")
			assert.NotContains(t, respBody, "/var/lib/fred", "no host path may reach the tenant")
			assert.Equal(t, http.StatusBadGateway, rec.Code,
				"an off-contract backend body is an upstream fault, not a tenant one: %s", respBody)

			var errResp ErrorResponse
			require.NoError(t, json.Unmarshal([]byte(respBody), &errResp))
			assert.Equal(t, errMsgBackendUnusableError, errResp.Error,
				"the tenant must get fred's authored message")

			assert.Empty(t, persister.calls,
				"a payload the backend rejected must never be persisted, however it was rejected (ENG-619)")
		})
	}
}

// TestRestoreLease_UnrecognizedBackendCodeRelays422 is the tenant-visible half
// of the fix: a 422 carrying a code fred does not know must NOT be remapped to
// 404 "no retained data found for that lease".
//
// That remap was the sharpest fabrication left in this path — fred changing the
// status class the backend chose AND asserting a positive fact about the
// tenant's data that the backend's own body contradicted, while discarding the
// message BACKEND_GUIDE obliged the backend to curate.
func TestRestoreLease_UnrecognizedBackendCodeRelays422(t *testing.T) {
	const authored = "retention subsystem is draining; retry in a few minutes"

	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2

	chainClient := &mockChainClient{
		getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			if uuid == leaseUUID {
				return &billingtypes.Lease{
					Uuid: leaseUUID, Tenant: kp.Address,
					ProviderUuid: providerUUID, State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			}
			return nil, nil
		},
	}

	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"` + authored + `","code":"draining"}`))
	}))
	defer backendServer.Close()

	backendClient := backend.NewHTTPClient(backend.HTTPClientConfig{
		Name: "unknown-code", BaseURL: backendServer.URL, Timeout: 5 * time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	placement := &mockPlacementLookup{getFunc: func(uuid string) string {
		if uuid == fromLeaseUUID {
			return "unknown-code"
		}
		return ""
	}}

	h := &Handlers{
		client:          chainClient,
		backendRouter:   router,
		placementLookup: placement,
		restoreService:  newRestoreServiceForTest(t, providerUUID, chainClient, router, placement, nil),
		providerUUID:    providerUUID,
		bech32Prefix:    "manifest",
	}

	req := httptest.NewRequest("POST", "/v1/leases/"+leaseUUID+"/restore",
		strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
	req.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	req.SetPathValue("lease_uuid", leaseUUID)

	rec := httptest.NewRecorder()
	h.RestoreLease(rec, req)

	body := rec.Body.String()
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code,
		"the backend chose 422; fred must not remap it to 404: %s", body)
	assert.Contains(t, body, authored, "the backend's authored message must reach the tenant")
	assert.NotContains(t, body, "no retained data",
		"fred must not assert that no retained data exists when the code says otherwise")
}
