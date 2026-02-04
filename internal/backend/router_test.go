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
