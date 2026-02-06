package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_Route(t *testing.T) {
	// Create mock backends
	k8sBackend := NewMockBackend(MockBackendConfig{Name: "kubernetes"})
	gpuBackend := NewMockBackend(MockBackendConfig{Name: "gpu"})
	vmBackend := NewMockBackend(MockBackendConfig{Name: "vm"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{
				Backend:   k8sBackend,
				Match:     MatchCriteria{SKUPrefix: "k8s-"},
				IsDefault: true,
			},
			{
				Backend: gpuBackend,
				Match:   MatchCriteria{SKUPrefix: "gpu-"},
			},
			{
				Backend: vmBackend,
				Match:   MatchCriteria{SKUPrefix: "vm-"},
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
			{Backend: backend1, Match: MatchCriteria{SKUPrefix: "a-"}},
			{Backend: backend2, Match: MatchCriteria{SKUPrefix: "b-"}},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRouter(RouterConfig{Backends: tt.backends})
			require.Error(t, err)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestRouter_RouteAll(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "backend-a"})
	backendB := NewMockBackend(MockBackendConfig{Name: "backend-b"})
	backendC := NewMockBackend(MockBackendConfig{Name: "backend-c"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: backendB, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: backendC, Match: MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Two backends match gpu-
	matches := router.RouteAll("gpu-a100")
	assert.Len(t, matches, 2)
	names := []string{matches[0].Name(), matches[1].Name()}
	assert.Contains(t, names, "backend-a")
	assert.Contains(t, names, "backend-b")

	// One backend matches k8s-
	matches = router.RouteAll("k8s-small")
	assert.Len(t, matches, 1)
	assert.Equal(t, "backend-c", matches[0].Name())

	// No match returns nil
	matches = router.RouteAll("unknown-sku")
	assert.Nil(t, matches)
}

func TestRouter_RouteAll_Deduplicates(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "shared"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-"}, IsDefault: true},
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-a100"}},
		},
	})
	require.NoError(t, err)

	// Same backend registered twice for matching prefixes — should deduplicate
	matches := router.RouteAll("gpu-a100-4x")
	assert.Len(t, matches, 1)
	assert.Equal(t, "shared", matches[0].Name())
}

func TestRouter_RouteRoundRobin_Distribution(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "backend-a"})
	backendB := NewMockBackend(MockBackendConfig{Name: "backend-b"})
	backendC := NewMockBackend(MockBackendConfig{Name: "backend-c"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: backendB, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: backendC, Match: MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Round-robin across two GPU backends
	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		b := router.RouteRoundRobin("gpu-a100")
		counts[b.Name()]++
	}

	assert.Equal(t, 50, counts["backend-a"])
	assert.Equal(t, 50, counts["backend-b"])
}

func TestRouter_RouteRoundRobin_SingleMatch(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "solo"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-"}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Single match always returns the same backend
	for i := 0; i < 10; i++ {
		b := router.RouteRoundRobin("gpu-a100")
		assert.Equal(t, "solo", b.Name())
	}
}

func TestRouter_RouteRoundRobin_NoMatch_FallsBackToDefault(t *testing.T) {
	backendA := NewMockBackend(MockBackendConfig{Name: "gpu-backend"})
	defaultBackend := NewMockBackend(MockBackendConfig{Name: "default"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backendA, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: defaultBackend, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// Repeated calls with unmatched SKU always return default (no divide-by-zero)
	for i := 0; i < 10; i++ {
		b := router.RouteRoundRobin("unknown-sku")
		assert.Equal(t, "default", b.Name())
	}
}

func TestRouter_RouteRoundRobin_InterleavedSKUs(t *testing.T) {
	gpuA := NewMockBackend(MockBackendConfig{Name: "gpu-a"})
	gpuB := NewMockBackend(MockBackendConfig{Name: "gpu-b"})
	k8s := NewMockBackend(MockBackendConfig{Name: "k8s"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: gpuA, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: gpuB, Match: MatchCriteria{SKUPrefix: "gpu-"}},
			{Backend: k8s, Match: MatchCriteria{SKUPrefix: "k8s-"}, IsDefault: true},
		},
	})
	require.NoError(t, err)

	// The global counter is shared across SKU groups. Interleaving calls
	// for different SKUs advances the counter for all groups, so the
	// per-group distribution is not perfectly even.
	gpuCounts := map[string]int{}
	for i := 0; i < 100; i++ {
		b := router.RouteRoundRobin("gpu-a100")
		gpuCounts[b.Name()]++

		// Interleave a single-backend SKU — advances the shared counter
		k := router.RouteRoundRobin("k8s-small")
		assert.Equal(t, "k8s", k.Name())
	}

	// Both GPU backends must be hit, but the distribution is uneven
	// because the k8s calls consume every other counter tick.
	assert.Greater(t, gpuCounts["gpu-a"], 0)
	assert.Greater(t, gpuCounts["gpu-b"], 0)
	assert.Equal(t, 100, gpuCounts["gpu-a"]+gpuCounts["gpu-b"])
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

	// Use already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// MockBackend.Health ignores context, so this should still work
	// But we're testing the API accepts a context
	results, allHealthy := router.HealthCheck(ctx)

	// MockBackend always returns healthy since it doesn't check context
	assert.True(t, allHealthy)

	require.Len(t, results, 1)
}
