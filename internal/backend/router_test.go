package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics"
)

type routerNamedBackend struct {
	Backend
	name string
}

func (backend routerNamedBackend) Name() string { return backend.name }

func TestRouter_Route(t *testing.T) {
	// Create mock backends
	k8sBackend := NewMockBackend(MockBackendConfig{Name: "kubernetes"})
	gpuBackend := NewMockBackend(MockBackendConfig{Name: "gpu"})
	vmBackend := NewMockBackend(MockBackendConfig{Name: "vm"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{
				Backend:   k8sBackend,
				Match:     MatchCriteria{SKUs: []string{"k8s-small", "k8s-large"}},
				IsDefault: true,
			},
			{
				Backend: gpuBackend,
				Match:   MatchCriteria{SKUs: []string{"gpu-a100", "gpu-h100-4x"}},
			},
			{
				Backend: vmBackend,
				Match:   MatchCriteria{SKUs: []string{"vm-ubuntu", "vm-windows-server"}},
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		sku      string
		wantName string
	}{
		{"k8s-small", "kubernetes"},
		{"k8s-large", "kubernetes"},
		{"gpu-a100", "gpu"},
		{"gpu-h100-4x", "gpu"},
		{"vm-ubuntu", "vm"},
		{"vm-windows-server", "vm"},
		{"unknown-sku", "kubernetes"}, // Falls back to default
		{"", "kubernetes"},            // Empty falls back to default
	}

	for _, tt := range tests {
		t.Run(tt.sku, func(t *testing.T) {
			backend := router.Route(tt.sku)
			assert.Equal(t, tt.wantName, backend.Name())
		})
	}
}

func TestRouter_ExactSKUMatch(t *testing.T) {
	specialBackend := NewMockBackend(MockBackendConfig{Name: "special"})
	defaultBackend := NewMockBackend(MockBackendConfig{Name: "default"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{
				Backend: specialBackend,
				Match:   MatchCriteria{SKUs: []string{"exact-sku-1", "exact-sku-2"}},
			},
			{
				Backend:   defaultBackend,
				IsDefault: true,
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		sku      string
		wantName string
	}{
		{"exact-sku-1", "special"},
		{"exact-sku-2", "special"},
		{"exact-sku-3", "default"},
		{"other", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.sku, func(t *testing.T) {
			backend := router.Route(tt.sku)
			assert.Equal(t, tt.wantName, backend.Name())
		})
	}
}

func TestRouter_NoBackends(t *testing.T) {
	_, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{},
	})
	assert.Error(t, err)
}

func TestRouter_MultipleDefaults(t *testing.T) {
	backend1 := NewMockBackend(MockBackendConfig{Name: "b1"})
	backend2 := NewMockBackend(MockBackendConfig{Name: "b2"})

	_, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1, IsDefault: true},
			{Backend: backend2, IsDefault: true},
		},
	})
	assert.Error(t, err)
}

func TestRouter_ImplicitDefault(t *testing.T) {
	backend1 := NewMockBackend(MockBackendConfig{Name: "first"})
	backend2 := NewMockBackend(MockBackendConfig{Name: "second"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1, Match: MatchCriteria{SKUs: []string{"a-1"}}},
			{Backend: backend2, Match: MatchCriteria{SKUs: []string{"b-1"}}},
		},
	})
	require.NoError(t, err)

	// First backend should be implicit default
	assert.Equal(t, "first", router.Default().Name())
}

func TestRouter_Backends(t *testing.T) {
	backend1 := NewMockBackend(MockBackendConfig{Name: "b1"})
	backend2 := NewMockBackend(MockBackendConfig{Name: "b2"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1},
			{Backend: backend2},
		},
	})
	require.NoError(t, err)

	backends := router.Backends()
	assert.Len(t, backends, 2)
}

func TestRouter_GetBackendByName(t *testing.T) {
	backend1 := NewMockBackend(MockBackendConfig{Name: "backend-one"})
	backend2 := NewMockBackend(MockBackendConfig{Name: "backend-two"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1},
			{Backend: backend2},
		},
	})
	require.NoError(t, err)

	// Found
	b := router.GetBackendByName("backend-one")
	require.NotNil(t, b)
	assert.Equal(t, "backend-one", b.Name())

	// Not found
	b = router.GetBackendByName("nonexistent")
	assert.Nil(t, b)
}

func TestRouter_NilBackend(t *testing.T) {
	validBackend := NewMockBackend(MockBackendConfig{Name: "valid"})
	var typedNilBackend *MockBackend

	tests := []struct {
		name     string
		backends []BackendEntry
		wantErr  string
	}{
		{
			name: "nil backend at index 0",
			backends: []BackendEntry{
				{Backend: nil},
			},
			wantErr: "backend at index 0 is nil",
		},
		{
			name: "nil backend at index 1",
			backends: []BackendEntry{
				{Backend: validBackend},
				{Backend: nil},
			},
			wantErr: "backend at index 1 is nil",
		},
		{
			name: "nil default backend",
			backends: []BackendEntry{
				{Backend: nil, IsDefault: true},
			},
			wantErr: "backend at index 0 is nil",
		},
		{
			name: "typed nil backend",
			backends: []BackendEntry{
				{Backend: typedNilBackend},
			},
			wantErr: "backend at index 0 is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRouter(RouterConfig{Backends: tt.backends})
			require.Error(t, err)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestRouter_RejectsInvalidAndDuplicateBackendNames(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "", want: "backend name is required"},
		{name: " backend-a", want: "leading or trailing whitespace"},
		{name: "backend-a\nPASS: forged", want: "non-printable character U+000A"},
		{name: "backend-a\u200B", want: "non-printable character U+200B"},
	} {
		t.Run(fmt.Sprintf("invalid %q", test.name), func(t *testing.T) {
			invalid := routerNamedBackend{
				Backend: NewMockBackend(MockBackendConfig{Name: "delegate"}),
				name:    test.name,
			}
			_, err := NewRouter(RouterConfig{Backends: []BackendEntry{
				{Backend: invalid},
			}})

			require.ErrorContains(t, err, "backend at index 0 has invalid name")
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("duplicate name", func(t *testing.T) {
		_, err := NewRouter(RouterConfig{Backends: []BackendEntry{
			{Backend: NewMockBackend(MockBackendConfig{Name: "same"})},
			{Backend: NewMockBackend(MockBackendConfig{Name: "same"})},
		}})

		require.EqualError(t, err, `duplicate backend name "same"`)
	})
}

func TestRouter_RouteAll(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "backend-a"})
	backendB := NewMockBackend(MockBackendConfig{Name: "backend-b"})
	backendC := NewMockBackend(MockBackendConfig{Name: "backend-c"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUs: []string{"gpu-a100"}}},
			{Backend: backendB, Match: MatchCriteria{SKUs: []string{"gpu-a100"}}},
			{Backend: backendC, Match: MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Two backends match gpu-a100
	matches := router.RouteAll("gpu-a100")
	assert.Len(t, matches, 2)
	names := []string{matches[0].Name(), matches[1].Name()}
	assert.Contains(t, names, "backend-a")
	assert.Contains(t, names, "backend-b")

	// One backend matches k8s-small
	matches = router.RouteAll("k8s-small")
	assert.Len(t, matches, 1)
	assert.Equal(t, "backend-c", matches[0].Name())

	// No match returns nil
	matches = router.RouteAll("unknown-sku")
	assert.Nil(t, matches)
}

func TestRouter_RouteAll_RejectsDuplicateRegistration(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "shared"})

	_, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUs: []string{"gpu-a100"}}, IsDefault: true},
			{Backend: backendA, Match: MatchCriteria{SKUs: []string{"gpu-a100"}}},
		},
	})
	require.EqualError(t, err, `duplicate backend name "shared"`)
}

// unhealthyMockBackend is a mock backend that returns an error on Health check.
type unhealthyMockBackend struct {
	*MockBackend
	healthErr error
}

func (u *unhealthyMockBackend) Health(ctx context.Context) error {
	return u.healthErr
}

func TestRouter_HealthCheck_AllHealthy(t *testing.T) {
	backend1 := NewMockBackend(MockBackendConfig{Name: "backend-1"})
	backend2 := NewMockBackend(MockBackendConfig{Name: "backend-2"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1, IsDefault: true},
			{Backend: backend2},
		},
	})
	require.NoError(t, err)

	results, allHealthy := router.HealthCheck(context.Background())

	assert.True(t, allHealthy)

	require.Len(t, results, 2)

	for _, result := range results {
		assert.True(t, result.Healthy, "Backend %q should be healthy", result.Name)
		assert.Empty(t, result.Error, "Backend %q should have no error", result.Name)
	}
}

func TestRouter_HealthCheck_OneUnhealthy(t *testing.T) {
	healthyBackend := NewMockBackend(MockBackendConfig{Name: "healthy"})
	unhealthyBackend := &unhealthyMockBackend{
		MockBackend: NewMockBackend(MockBackendConfig{Name: "unhealthy"}),
		healthErr:   errors.New("connection refused"),
	}

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: healthyBackend, IsDefault: true},
			{Backend: unhealthyBackend},
		},
	})
	require.NoError(t, err)

	results, allHealthy := router.HealthCheck(context.Background())

	assert.False(t, allHealthy)

	require.Len(t, results, 2)

	// Find the unhealthy result
	var foundUnhealthy bool
	for _, result := range results {
		if result.Name == "unhealthy" {
			foundUnhealthy = true
			assert.False(t, result.Healthy)
			assert.Equal(t, "connection refused", result.Error)
		} else if result.Name == "healthy" {
			assert.True(t, result.Healthy)
		}
	}

	assert.True(t, foundUnhealthy)
}

func TestRouter_HealthCheck_AllUnhealthy(t *testing.T) {
	backend1 := &unhealthyMockBackend{
		MockBackend: NewMockBackend(MockBackendConfig{Name: "backend-1"}),
		healthErr:   errors.New("timeout"),
	}
	backend2 := &unhealthyMockBackend{
		MockBackend: NewMockBackend(MockBackendConfig{Name: "backend-2"}),
		healthErr:   errors.New("service unavailable"),
	}

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend1, IsDefault: true},
			{Backend: backend2},
		},
	})
	require.NoError(t, err)

	results, allHealthy := router.HealthCheck(context.Background())

	assert.False(t, allHealthy)

	require.Len(t, results, 2)

	for _, result := range results {
		assert.False(t, result.Healthy, "Backend %q should be unhealthy", result.Name)
		assert.NotEmpty(t, result.Error, "Backend %q should have an error message", result.Name)
	}
}

func TestRouter_HealthCheck_ContextCancellation(t *testing.T) {
	backend := NewMockBackend(MockBackendConfig{Name: "test"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Use already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// MockBackend.Health ignores context, so this should still work
	// But we're testing the API accepts a context
	results, allHealthy := router.HealthCheck(ctx)

	// MockBackend always returns healthy since it doesn't check context
	assert.True(t, allHealthy)

	require.Len(t, results, 1)
}

func TestRouter_HealthCheck_SetsBackendHealthyGauge(t *testing.T) {
	healthy := NewMockBackend(MockBackendConfig{Name: "healthy-be"})
	unhealthy := &unhealthyBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "sick-be"})}

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: healthy, IsDefault: true},
			{Backend: unhealthy},
		},
		BackendHealthy: metrics.BackendHealthy,
	})
	require.NoError(t, err)

	router.HealthCheck(context.Background())

	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("healthy-be")),
		"healthy backend gauge should be 1")
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("sick-be")),
		"unhealthy backend gauge should be 0")
}

func TestRouter_HealthCheck_SkipsGaugeOnContextCancellation(t *testing.T) {
	be := &contextAwareBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "ctx-be"})}

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: be, IsDefault: true},
		},
		BackendHealthy: metrics.BackendHealthy,
	})
	require.NoError(t, err)

	// Set the gauge to 1 (healthy) first.
	metrics.BackendHealthy.WithLabelValues("ctx-be").Set(1)

	// Call HealthCheck with a canceled context; the backend returns ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, allHealthy := router.HealthCheck(ctx)

	assert.False(t, allHealthy)
	require.Len(t, results, 1)
	assert.False(t, results[0].Healthy)

	// Gauge must remain at 1 — context cancellation should not flip it to 0.
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("ctx-be")),
		"gauge should not change on context cancellation")
}

// TestRouter_HealthCheck_ProbesConcurrently is the regression guard for the
// ENG-522 follow-up Copilot caught on PR #226.
//
// Serial probing made the worst case the SUM of every backend's client timeout
// (30s each by default), so one backend that accepts a connection and never
// answers pushed the /health handler past the API server's own request timeout
// — whose response body is a 503, the exact status code that endpoint exists
// never to send. Concurrency makes the worst case the MAX instead, which is
// what lets healthProbeBudget bound the sweep without starving the backends
// probed last.
//
// Asserted as a hard inequality against the serial floor, not a tight timing
// window, so it cannot flake on a loaded CI box: 5 backends × 200ms is 1s
// serially and ~200ms concurrently.
func TestRouter_HealthCheck_ProbesConcurrently(t *testing.T) {
	const (
		backendCount = 5
		probeDelay   = 200 * time.Millisecond
	)

	var entries []BackendEntry
	for i := range backendCount {
		be := &slowBackend{
			MockBackend: NewMockBackend(MockBackendConfig{Name: fmt.Sprintf("slow-%d", i)}),
			delay:       probeDelay,
		}
		entries = append(entries, BackendEntry{Backend: be, IsDefault: i == 0})
	}

	router, err := NewRouter(RouterConfig{Backends: entries})
	require.NoError(t, err)

	start := time.Now()
	results, allHealthy := router.HealthCheck(context.Background())
	elapsed := time.Since(start)

	assert.True(t, allHealthy)
	require.Len(t, results, backendCount)

	serialFloor := probeDelay * backendCount
	assert.Less(t, elapsed, serialFloor,
		"probes must run concurrently: %d backends × %s took %s, which is at or above the %s serial floor",
		backendCount, probeDelay, elapsed, serialFloor)
}

// TestRouter_HealthCheck_PreservesOrderUnderConcurrency pins that concurrent
// probing did not make the response order depend on completion order. Callers
// diffing successive /health bodies would otherwise see spurious churn.
func TestRouter_HealthCheck_PreservesOrderUnderConcurrency(t *testing.T) {
	// Declared fastest-last so completion order is the reverse of config order.
	delays := []time.Duration{150 * time.Millisecond, 100 * time.Millisecond, 10 * time.Millisecond}

	var entries []BackendEntry
	for i, d := range delays {
		be := &slowBackend{
			MockBackend: NewMockBackend(MockBackendConfig{Name: fmt.Sprintf("ordered-%d", i)}),
			delay:       d,
		}
		entries = append(entries, BackendEntry{Backend: be, IsDefault: i == 0})
	}

	router, err := NewRouter(RouterConfig{Backends: entries})
	require.NoError(t, err)

	results, _ := router.HealthCheck(context.Background())

	require.Len(t, results, len(delays))
	for i := range delays {
		assert.Equal(t, fmt.Sprintf("ordered-%d", i), results[i].Name,
			"results must stay in Backends() order, not completion order")
	}
}

// TestRouter_HealthCheck_RecordsGaugeOnDeadlineExceeded pins the other half of
// the cancellation rule. A probe that blows its DEADLINE is genuine evidence of
// an unhealthy backend — it accepted the connection and never answered — and
// must reach the gauge, because the gauge is now the only signal carrying this
// (the status code no longer does). Only a CANCELED probe is excluded, which
// TestRouter_HealthCheck_SkipsGaugeOnContextCancellation covers.
func TestRouter_HealthCheck_RecordsGaugeOnDeadlineExceeded(t *testing.T) {
	be := &contextAwareBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "deadline-be"})}

	router, err := NewRouter(RouterConfig{
		Backends:       []BackendEntry{{Backend: be, IsDefault: true}},
		BackendHealthy: metrics.BackendHealthy,
	})
	require.NoError(t, err)

	// Start from healthy so a missing write is distinguishable from a write of 0.
	metrics.BackendHealthy.WithLabelValues("deadline-be").Set(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)

	results, allHealthy := router.HealthCheck(ctx)

	assert.False(t, allHealthy)
	require.Len(t, results, 1)
	assert.False(t, results[0].Healthy)

	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("deadline-be")),
		"a probe that exceeded its deadline is genuine unhealth and must reach the gauge")
}

// TestRouter_HealthCheck_RecoversPanickingProbe covers the hazard introduced by
// moving probes onto their own goroutines: net/http recovers a panic raised on
// its own request goroutine, but not one raised on a goroutine the handler
// spawned, so an unrecovered panic here would take the whole daemon down
// instead of failing one probe.
func TestRouter_HealthCheck_RecoversPanickingProbe(t *testing.T) {
	panics := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_health_probe_panics_total", Help: "test"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: &panickingBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "boom-be"})}, IsDefault: true},
			{Backend: NewMockBackend(MockBackendConfig{Name: "fine-be"})},
		},
		HealthProbePanics: panics,
	})
	require.NoError(t, err)

	var results []BackendHealth
	var allHealthy bool
	require.NotPanics(t, func() {
		results, allHealthy = router.HealthCheck(context.Background())
	}, "a panicking backend probe must not escape its goroutine")

	assert.False(t, allHealthy)
	require.Len(t, results, 2)

	assert.Equal(t, "boom-be", results[0].Name)
	assert.False(t, results[0].Healthy, "a panicking probe counts as unhealthy, never as a silent pass")
	assert.Equal(t, "health probe panicked", results[0].Error)

	assert.Equal(t, "fine-be", results[1].Name)
	assert.True(t, results[1].Healthy, "one panicking probe must not contaminate its peers")

	assert.Equal(t, 1.0, promtestutil.ToFloat64(panics))
}

// slowBackend wraps MockBackend and blocks in Health() for a fixed delay,
// standing in for a backend that accepts the connection but is slow to answer.
type slowBackend struct {
	*MockBackend
	delay time.Duration
}

func (s *slowBackend) Health(ctx context.Context) error {
	select {
	case <-time.After(s.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *slowBackend) Name() string {
	return s.name
}

// panickingBackend wraps MockBackend and panics in Health().
type panickingBackend struct {
	*MockBackend
}

func (p *panickingBackend) Health(ctx context.Context) error {
	panic("health probe exploded")
}

func (p *panickingBackend) Name() string {
	return p.name
}

// unhealthyBackend wraps MockBackend but returns an error from Health().
type unhealthyBackend struct {
	*MockBackend
}

func (u *unhealthyBackend) Health(ctx context.Context) error {
	return errors.New("backend down")
}

func (u *unhealthyBackend) Name() string {
	return u.name
}

// contextAwareBackend wraps MockBackend but returns ctx.Err() when the context is done.
type contextAwareBackend struct {
	*MockBackend
}

func (c *contextAwareBackend) Health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *contextAwareBackend) Name() string {
	return c.name
}

func newLeastLoadedRouter(t *testing.T, ratios map[string]float64) (*Router, map[string]*MockBackend) {
	t.Helper()
	backends := make(map[string]*MockBackend, len(ratios))
	var entries []BackendEntry
	first := true
	// Deterministic order for IsDefault assignment.
	for _, name := range []string{"b1", "b2", "b3"} {
		r, ok := ratios[name]
		if !ok {
			continue
		}
		mb := NewMockBackend(MockBackendConfig{Name: name})
		mb.SetLoadStats(&LoadStats{TotalCPUCores: 100, AllocatedCPUCores: r * 100})
		backends[name] = mb
		entries = append(entries, BackendEntry{
			Backend:   mb,
			Match:     MatchCriteria{SKUs: []string{"s"}},
			IsDefault: first,
		})
		first = false
	}
	router, err := NewRouter(RouterConfig{Backends: entries})
	require.NoError(t, err)
	return router, backends
}

func TestRouter_RouteForProvision_LeastLoaded(t *testing.T) {
	router, _ := newLeastLoadedRouter(t, map[string]float64{"b1": 0.8, "b2": 0.2, "b3": 0.5})
	got := router.RouteForProvision(context.Background(), "s", nil)
	assert.Equal(t, "b2", got.Name())
}

func TestRouter_RouteForProvision_TieBrokenByInFlight(t *testing.T) {
	router, _ := newLeastLoadedRouter(t, map[string]float64{"b1": 0.5, "b2": 0.5})
	got := router.RouteForProvision(context.Background(), "s", map[string]int{"b1": 3, "b2": 0})
	assert.Equal(t, "b2", got.Name(), "equal ratio → fewer in-flight wins")
}

func TestRouter_RouteForProvision_SingleAndNoMatch(t *testing.T) {
	b1 := NewMockBackend(MockBackendConfig{Name: "b1"})
	def := NewMockBackend(MockBackendConfig{Name: "def"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: def, Match: MatchCriteria{SKUs: []string{"other"}}, IsDefault: true},
		{Backend: b1, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)
	assert.Equal(t, "b1", router.RouteForProvision(context.Background(), "s", nil).Name())
	assert.Equal(t, "def", router.RouteForProvision(context.Background(), "nope", nil).Name())
}

func TestRouter_RouteForProvision_FallbackToRoundRobinWhenNoStats(t *testing.T) {
	b1 := NewMockBackend(MockBackendConfig{Name: "b1"}) // no SetLoadStats → nil
	b2 := NewMockBackend(MockBackendConfig{Name: "b2"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: b1, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
		{Backend: b2, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)
	seen := map[string]int{}
	for i := 0; i < 100; i++ {
		seen[router.RouteForProvision(context.Background(), "s", nil).Name()]++
	}
	assert.Greater(t, seen["b1"], 0)
	assert.Greater(t, seen["b2"], 0)
}

// statsErrBackend wraps a MockBackend to make GetLoadStats fail or block on the
// context, exercising RouteForProvision's degraded paths (a real backend's
// GetLoadStats can return ErrCircuitOpen / a transport error / a deadline error;
// MockBackend alone never can). Test-only; no production code changes.
type statsErrBackend struct {
	*MockBackend
	err      error // returned from GetLoadStats when non-nil
	blockCtx bool  // when true, GetLoadStats waits for ctx and returns ctx.Err()
}

func (b *statsErrBackend) GetLoadStats(ctx context.Context) (*LoadStats, error) {
	if b.blockCtx {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, b.err
}

func TestRouter_RouteForProvision_PartialFailurePicksHealthy(t *testing.T) {
	// One candidate's stats error out; the only usable candidate must be chosen
	// (NOT round-robin fallback) even though its ratio is high.
	good := NewMockBackend(MockBackendConfig{Name: "good"})
	good.SetLoadStats(&LoadStats{TotalCPUCores: 10, AllocatedCPUCores: 9}) // ratio 0.9, but only usable one
	bad := &statsErrBackend{
		MockBackend: NewMockBackend(MockBackendConfig{Name: "bad"}),
		err:         errors.New("circuit open"),
	}
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: good, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
		{Backend: bad, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)
	// Repeat: a buggy fallback would round-robin and eventually return "bad".
	for i := 0; i < 50; i++ {
		assert.Equal(t, "good", router.RouteForProvision(context.Background(), "s", nil).Name())
	}
}

func TestRouter_RouteForProvision_AllErrorsFallBackAndCount(t *testing.T) {
	fallback := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_routing_fallback_total", Help: "test"})
	b1 := &statsErrBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "b1"}), err: errors.New("boom")}
	b2 := &statsErrBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "b2"}), err: errors.New("boom")}
	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: b1, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
			{Backend: b2, Match: MatchCriteria{SKUs: []string{"s"}}},
		},
		RoutingFallback: fallback,
	})
	require.NoError(t, err)
	seen := map[string]int{}
	for i := 0; i < 50; i++ {
		seen[router.RouteForProvision(context.Background(), "s", nil).Name()]++
	}
	assert.Greater(t, seen["b1"], 0, "all-error → round-robin distributes")
	assert.Greater(t, seen["b2"], 0, "all-error → round-robin distributes")
	assert.Equal(t, float64(50), promtestutil.ToFloat64(fallback), "every all-error decision increments the fallback counter")
}

func TestRouter_RouteForProvision_TimeoutExcludesSlowBackend(t *testing.T) {
	// A backend that never returns stats within the deadline is excluded; the
	// fast healthy backend is chosen. Deterministic via a short caller deadline
	// (which dominates statsFetchTimeout) — no sleeps.
	good := NewMockBackend(MockBackendConfig{Name: "good"})
	good.SetLoadStats(&LoadStats{TotalCPUCores: 10, AllocatedCPUCores: 5})
	slow := &statsErrBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "slow"}), blockCtx: true}
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: good, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
		{Backend: slow, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.Equal(t, "good", router.RouteForProvision(ctx, "s", nil).Name())
}

func TestRouter_RouteForProvision_ConcurrentBurstSpread(t *testing.T) {
	// Identical ratio + zero in-flight everywhere → exact ties. The round-robin
	// counter must spread the burst, not pile it onto one backend. Run with -race
	// to validate the parallel /stats fan-out and counter concurrency.
	router, _ := newLeastLoadedRouter(t, map[string]float64{"b1": 0.5, "b2": 0.5})
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := router.RouteForProvision(context.Background(), "s", map[string]int{})
			mu.Lock()
			seen[b.Name()]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	// Deterministic, NOT statistical: the shared atomic counter hands out 200
	// distinct indices (a permutation of 0..199) regardless of scheduling, so
	// exactly half land on each tied backend. The -race coverage comes from the
	// parallel /stats fan-out goroutines + the shared counter; the count split is
	// scheduling-independent. (Asserting ==100 catches a regression that >50 would
	// silently tolerate, e.g. replacing the shared counter with a per-goroutine source.)
	assert.Equal(t, 100, seen["b1"], "exact-tie burst must split evenly via the RR counter")
	assert.Equal(t, 100, seen["b2"], "exact-tie burst must split evenly via the RR counter")
}

func TestRouter_RouteForProvisionAmong_ExcludesDownIneligibleBackend(t *testing.T) {
	downMock := NewMockBackend(MockBackendConfig{Name: "down"})
	downMock.SetLoadStats(&LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 1})
	down := &unhealthyBackend{MockBackend: downMock}

	eligibleBusy := NewMockBackend(MockBackendConfig{Name: "eligible-busy"})
	eligibleBusy.SetLoadStats(&LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 80})
	eligibleLeastLoaded := NewMockBackend(MockBackendConfig{Name: "eligible-least-loaded"})
	eligibleLeastLoaded.SetLoadStats(&LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 20})

	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: down, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
		{Backend: eligibleBusy, Match: MatchCriteria{SKUs: []string{"s"}}},
		{Backend: eligibleLeastLoaded, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)
	require.Error(t, down.Health(context.Background()), "fixture must represent a down backend")

	got := router.RouteForProvisionAmong(context.Background(), "s", map[string]struct{}{
		"eligible-busy":         {},
		"eligible-least-loaded": {},
	}, nil)
	require.NotNil(t, got)
	assert.Equal(t, "eligible-least-loaded", got.Name(),
		"the lower-load but ineligible backend must not participate")
}

func TestRouter_RouteForProvisionAmong_FallbackNeverEscapesEligibleSet(t *testing.T) {
	excludedDefault := NewMockBackend(MockBackendConfig{Name: "excluded-default"})
	eligibleA := NewMockBackend(MockBackendConfig{Name: "eligible-a"})
	eligibleB := NewMockBackend(MockBackendConfig{Name: "eligible-b"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: excludedDefault, Match: MatchCriteria{SKUs: []string{"s"}}, IsDefault: true},
		{Backend: eligibleA, Match: MatchCriteria{SKUs: []string{"s"}}},
		{Backend: eligibleB, Match: MatchCriteria{SKUs: []string{"s"}}},
	}})
	require.NoError(t, err)

	eligible := map[string]struct{}{"eligible-a": {}, "eligible-b": {}}
	seen := map[string]int{}
	for i := 0; i < 40; i++ {
		got := router.RouteForProvisionAmong(context.Background(), "s", eligible, nil)
		require.NotNil(t, got)
		seen[got.Name()]++
	}

	assert.Zero(t, seen["excluded-default"], "degraded fallback must not widen eligibility")
	assert.Equal(t, 20, seen["eligible-a"])
	assert.Equal(t, 20, seen["eligible-b"])
}

func TestRouter_RouteForProvisionAmong_DefaultMustBeEligible(t *testing.T) {
	matching := NewMockBackend(MockBackendConfig{Name: "matching"})
	fallback := NewMockBackend(MockBackendConfig{Name: "fallback"})
	router, err := NewRouter(RouterConfig{Backends: []BackendEntry{
		{Backend: matching, Match: MatchCriteria{SKUs: []string{"s"}}},
		{Backend: fallback, Match: MatchCriteria{SKUs: []string{"other"}}, IsDefault: true},
	}})
	require.NoError(t, err)

	got := router.RouteForProvisionAmong(context.Background(), "unknown", map[string]struct{}{"fallback": {}}, nil)
	require.NotNil(t, got)
	assert.Equal(t, "fallback", got.Name())

	assert.Nil(t, router.RouteForProvisionAmong(
		context.Background(), "unknown", map[string]struct{}{"matching": {}}, nil,
	), "an ineligible default must not be used")
	assert.Nil(t, router.RouteForProvisionAmong(context.Background(), "s", nil, nil),
		"an empty eligibility set must not route anywhere")
}
