package backend

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

// countingIdentityResolver proves a fenced client never reaches request
// preparation, the only place a request is bound to storage and signed.
type countingIdentityResolver struct {
	id      backendidentity.ID
	lookups atomic.Int64
}

func (resolver *countingIdentityResolver) ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool) {
	resolver.lookups.Add(1)
	return resolver.id, true
}

func newFencedClientForTest(t *testing.T, opts HTTPClientOptions) (*HTTPClient, *countingIdentityResolver) {
	t.Helper()
	policy, err := NewFencedConnectionPolicy("fenced-node")
	require.NoError(t, err)
	resolver := &countingIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA)}
	client, err := NewIdentityBoundHTTPClient(policy, opts, resolver)
	require.NoError(t, err)
	return client, resolver
}

func TestFencedConnectionPolicyCarriesNoConnection(t *testing.T) {
	t.Parallel()

	_, err := NewFencedConnectionPolicy("  ")
	require.Error(t, err)

	fenced, err := NewFencedConnectionPolicy("fenced-node")
	require.NoError(t, err)
	require.True(t, fenced.Fenced())
	require.Nil(t, fenced.state.live, "a fenced policy holds no address, key, or TLS material")

	live, err := NewConnectionPolicy(ConnectionConfig{
		Name: "live-node", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
	})
	require.NoError(t, err)
	require.False(t, live.Fenced())
	require.False(t, ConnectionPolicy{}.Fenced(), "the invalid zero policy is not a fence")
}

func TestFencedClientRefusesEveryOperationWithoutARequest(t *testing.T) {
	t.Parallel()

	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_fenced_requests_total", Help: "test",
	}, []string{"backend", "operation", "status"})
	client, resolver := newFencedClientForTest(t, HTTPClientOptions{RequestsTotal: requests})
	require.True(t, IsFenced(client))
	require.Nil(t, client.wire)

	ctx := t.Context()
	lease := testBackendStorageIDB
	id, err := maintenanceid.New()
	require.NoError(t, err)

	provision := InvokeProvision(ctx, client, ProvisionRequest{LeaseUUID: lease})
	require.True(t, provision.NotDispatched(), "a fenced provision was provably never sent")
	require.ErrorIs(t, provision.Err(), ErrBackendFenced)

	restore := InvokeRestore(ctx, client, RestoreRequest{LeaseUUID: lease})
	require.True(t, restore.NotDispatched())
	require.ErrorIs(t, restore.Err(), ErrBackendFenced)

	for _, outcome := range []MaintenanceCallOutcome{
		InvokeRestart(ctx, client, RestartRequest{LeaseUUID: lease, MaintenanceID: id}),
		InvokeUpdate(ctx, client, UpdateRequest{LeaseUUID: lease, MaintenanceID: id}),
	} {
		require.True(t, outcome.NotDispatched())
		require.ErrorIs(t, outcome.Err(), ErrBackendFenced)
	}

	closeErr := client.Deprovision(ctx, lease)
	require.Equal(t, DeprovisionRefusedFenced, DeprovisionRefusalOf(client, lease, closeErr))
	require.ErrorIs(t, closeErr, ErrBackendFenced)
	require.NotErrorIs(t, closeErr, ErrCircuitOpen, "a fence is not an outage")

	reads := map[string]func() error{
		"get_info":       func() error { _, err := client.GetInfo(ctx, lease); return err },
		"get_provision":  func() error { _, err := client.GetProvision(ctx, lease); return err },
		"lookup":         func() error { _, err := client.LookupProvisions(ctx, []string{lease}); return err },
		"get_logs":       func() error { _, err := client.GetLogs(ctx, lease, 10); return err },
		"get_releases":   func() error { _, err := client.GetReleases(ctx, lease); return err },
		"get_load_stats": func() error { _, err := client.GetLoadStats(ctx); return err },
		"list_provisions": func() error {
			_, err := client.ListProvisions(ctx)
			return err
		},
		"list_provisions_identity": func() error {
			_, _, err := client.ListProvisionsWithIdentity(ctx)
			return err
		},
		"list_retentions": func() error {
			_, err := client.ListRetentions(ctx)
			return err
		},
		"list_retentions_identity": func() error {
			_, _, err := client.ListRetentionsWithIdentity(ctx)
			return err
		},
		"custom_domain": func() error { return client.ReconcileCustomDomain(ctx, lease, nil) },
		"health":        func() error { return client.Health(ctx) },
	}
	for name, read := range reads {
		require.ErrorIs(t, read(), ErrBackendFenced, name)
	}

	require.Zero(t, resolver.lookups.Load(), "no request was prepared or signed")
	require.Equal(t, gobreaker.StateClosed, client.cb.State())
	require.Equal(t, gobreaker.Counts{}, client.cb.Counts(), "a fence never feeds the circuit breaker")
	require.InDelta(t, 1.0, promtestutil.ToFloat64(requests.WithLabelValues("fenced-node", "deprovision", "fenced")), 0)
	require.InDelta(t, 1.0, promtestutil.ToFloat64(requests.WithLabelValues("fenced-node", "provision", "fenced")), 0)
	require.InDelta(t, 0.0, promtestutil.ToFloat64(requests.WithLabelValues("fenced-node", "deprovision", "error")), 0)
}

func TestIsFencedRecognizesOnlyAFencedHTTPClient(t *testing.T) {
	t.Parallel()

	fenced, _ := newFencedClientForTest(t, HTTPClientOptions{})
	require.True(t, IsFenced(fenced))
	live := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "live", BaseURL: "https://backend.example"})
	require.False(t, IsFenced(live))
	require.False(t, IsFenced(nil))
	require.False(t, IsFenced((*HTTPClient)(nil)))
}

func TestFencedPolicyGrantsNoOfflineEvidenceOrProbe(t *testing.T) {
	t.Parallel()

	fenced, err := NewFencedConnectionPolicy("fenced-node")
	require.NoError(t, err)
	_, err = NewAuthenticatedEvidencePolicy(fenced)
	require.ErrorIs(t, err, ErrBackendFenced)
	_, err = ProbeStorageIdentity(context.Background(), fenced)
	require.ErrorIs(t, err, ErrBackendFenced)

	inventory, err := NewBootstrapInventoryClient(fenced, HTTPClientOptions{})
	require.NoError(t, err)
	_, _, err = inventory.ListProvisionsWithIdentity(t.Context())
	require.ErrorIs(t, err, ErrBackendFenced)
	var refusal *fencedError
	require.True(t, errors.As(err, &refusal))
	require.Equal(t, "fenced-node", refusal.backend)
}

func newNamedFencedClientForTest(t *testing.T, name string) *HTTPClient {
	t.Helper()
	policy, err := NewFencedConnectionPolicy(name)
	require.NoError(t, err)
	client, err := NewIdentityBoundHTTPClient(policy, HTTPClientOptions{},
		&testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	return client
}

func TestRouteForProvisionNeverSelectsAFencedBackend(t *testing.T) {
	t.Parallel()

	fenced := newNamedFencedClientForTest(t, "fenced")
	live := NewMockBackend(MockBackendConfig{Name: "live"}) // no stats: round-robin
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: fenced, Match: MatchCriteria{SKUs: []string{"shared", "fenced-only"}}},
		{Backend: live, Match: MatchCriteria{SKUs: []string{"shared"}}, IsDefault: true},
	}})
	require.NoError(t, err)

	for range 20 {
		got := router.RouteForProvision(t.Context(), "shared", nil)
		require.NotNil(t, got)
		require.Equal(t, "live", got.Name(), "round-robin must not rotate onto the fence")
	}
	require.Nil(t, router.RouteForProvision(t.Context(), "fenced-only", nil),
		"a SKU only the fenced backend serves waits; it never lands on the default")
	require.Equal(t, "live", router.RouteForProvision(t.Context(), "unmatched", nil).Name(),
		"an unmatched SKU still uses the default")

	got := router.RouteForProvisionAmong(t.Context(), "shared",
		map[string]struct{}{"fenced": {}, "live": {}}, nil)
	require.NotNil(t, got)
	require.Equal(t, "live", got.Name())
	require.Nil(t, router.RouteForProvisionAmong(t.Context(), "shared",
		map[string]struct{}{"fenced": {}}, nil))
	require.Nil(t, router.RouteForProvisionAmong(t.Context(), "fenced-only",
		map[string]struct{}{"fenced": {}, "live": {}}, nil),
		"an eligible default is no substitute for a fenced SKU match")
	require.Nil(t, router.RouteForProvisionAmong(t.Context(), "fenced-only",
		map[string]struct{}{"live": {}}, nil),
		"a fenced backend never answers, so it is never eligible; the default is still no substitute")

	// Reads still reach the fenced backend, where they fail closed.
	require.Equal(t, "fenced", router.Route("fenced-only").Name())
	require.Len(t, router.RouteAll("shared"), 2)
}

func TestRouteForProvisionSkipsAFencedDefault(t *testing.T) {
	t.Parallel()

	fenced := newNamedFencedClientForTest(t, "fenced-default")
	live := NewMockBackend(MockBackendConfig{Name: "live"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: fenced, IsDefault: true},
		{Backend: live, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)

	require.Equal(t, "live", router.RouteForProvision(t.Context(), "s", nil).Name())
	require.Nil(t, router.RouteForProvision(t.Context(), "unmatched", nil))
	require.Nil(t, router.RouteForProvisionAmong(t.Context(), "unmatched",
		map[string]struct{}{"fenced-default": {}, "live": {}}, nil))
	require.Equal(t, "fenced-default", router.Default().Name(), "reads keep the configured default")
}

func TestFencedGaugeAndHealthReportTheFence(t *testing.T) {
	t.Parallel()

	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_backend_fenced", Help: "test"}, []string{"backend"})
	healthy := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_backend_healthy", Help: "test"}, []string{"backend"})
	fenced, _ := newFencedClientForTest(t, HTTPClientOptions{Fenced: gauge})
	livePolicy, err := NewConnectionPolicy(ConnectionConfig{
		Name: "live", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
	})
	require.NoError(t, err)
	_ = newHTTPClient(livePolicy, HTTPClientOptions{Fenced: gauge})
	require.InDelta(t, 1.0, promtestutil.ToFloat64(gauge.WithLabelValues("fenced-node")), 0)
	require.InDelta(t, 0.0, promtestutil.ToFloat64(gauge.WithLabelValues("live")), 0)

	router, err := NewRouter(RouterConfig{
		Backends:       []BackendEntry{{Backend: fenced, IsDefault: true}, {Backend: NewMockBackend(MockBackendConfig{Name: "mock"})}},
		BackendHealthy: healthy,
	})
	require.NoError(t, err)
	results, allHealthy := router.HealthCheck(t.Context())
	require.False(t, allHealthy)
	require.Equal(t, "fenced-node", results[0].Name)
	require.False(t, results[0].Healthy)
	require.True(t, results[0].Fenced)
	require.False(t, results[1].Fenced)
	require.InDelta(t, 0.0, promtestutil.ToFloat64(healthy.WithLabelValues("fenced-node")), 0)
}

// The fence alone never sends a matched SKU to the default. A SKU whose other
// matching backend merely did not answer keeps the existing degraded-admission
// behavior, which may use the eligible default.
func TestRouteForProvisionAmongKeepsSilenceSemanticsBesideAFence(t *testing.T) {
	t.Parallel()

	fenced := newNamedFencedClientForTest(t, "fenced")
	silent := NewMockBackend(MockBackendConfig{Name: "silent"})
	def := NewMockBackend(MockBackendConfig{Name: "default"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: fenced, Match: MatchCriteria{SKUs: []string{"shared"}}},
		{Backend: silent, Match: MatchCriteria{SKUs: []string{"shared"}}},
		{Backend: def, IsDefault: true},
	}})
	require.NoError(t, err)

	got := router.RouteForProvisionAmong(t.Context(), "shared", map[string]struct{}{"default": {}}, nil)
	require.NotNil(t, got)
	require.Equal(t, "default", got.Name())
}

// Not parallel: it replaces the default logger.
func TestFencedHealthStartsNoProbeObservation(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	client, _ := newFencedClientForTest(t, HTTPClientOptions{})
	require.ErrorIs(t, client.Health(t.Context()), ErrBackendFenced)
	require.NotContains(t, logs.String(), "fenced-node",
		"a fenced backend is not probed, so no probe completion is logged every poll")
}
