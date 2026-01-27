package backend

import (
	"context"
	"errors"
	"testing"
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

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
			if backend.Name() != tt.wantName {
				t.Errorf("Route(%q) = %q, want %q", tt.sku, backend.Name(), tt.wantName)
			}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

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
			if backend.Name() != tt.wantName {
				t.Errorf("Route(%q) = %q, want %q", tt.sku, backend.Name(), tt.wantName)
			}
		})
	}
}

func TestRouter_NoBackends(t *testing.T) {
	_, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{},
	})
	if err == nil {
		t.Error("NewRouter() with no backends should return error")
	}
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
	if err == nil {
		t.Error("NewRouter() with multiple defaults should return error")
	}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	// First backend should be implicit default
	if router.Default().Name() != "first" {
		t.Errorf("Default() = %q, want %q", router.Default().Name(), "first")
	}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	backends := router.Backends()
	if len(backends) != 2 {
		t.Errorf("Backends() returned %d backends, want 2", len(backends))
	}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	// Found
	b := router.GetBackendByName("backend-one")
	if b == nil || b.Name() != "backend-one" {
		t.Errorf("GetBackendByName(backend-one) failed")
	}

	// Not found
	b = router.GetBackendByName("nonexistent")
	if b != nil {
		t.Errorf("GetBackendByName(nonexistent) = %v, want nil", b)
	}
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
			if err == nil {
				t.Fatal("NewRouter() with nil backend should return error")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("error = %q, want %q", err.Error(), tt.wantErr)
			}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	results, allHealthy := router.HealthCheck(context.Background())

	if !allHealthy {
		t.Error("HealthCheck() allHealthy = false, want true")
	}

	if len(results) != 2 {
		t.Fatalf("HealthCheck() returned %d results, want 2", len(results))
	}

	for _, result := range results {
		if !result.Healthy {
			t.Errorf("Backend %q should be healthy", result.Name)
		}
		if result.Error != "" {
			t.Errorf("Backend %q should have no error, got %q", result.Name, result.Error)
		}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	results, allHealthy := router.HealthCheck(context.Background())

	if allHealthy {
		t.Error("HealthCheck() allHealthy = true, want false (one backend unhealthy)")
	}

	if len(results) != 2 {
		t.Fatalf("HealthCheck() returned %d results, want 2", len(results))
	}

	// Find the unhealthy result
	var foundUnhealthy bool
	for _, result := range results {
		if result.Name == "unhealthy" {
			foundUnhealthy = true
			if result.Healthy {
				t.Error("Unhealthy backend should have Healthy = false")
			}
			if result.Error != "connection refused" {
				t.Errorf("Unhealthy backend Error = %q, want %q", result.Error, "connection refused")
			}
		} else if result.Name == "healthy" {
			if !result.Healthy {
				t.Error("Healthy backend should have Healthy = true")
			}
		}
	}

	if !foundUnhealthy {
		t.Error("Did not find unhealthy backend in results")
	}
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
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	results, allHealthy := router.HealthCheck(context.Background())

	if allHealthy {
		t.Error("HealthCheck() allHealthy = true, want false (all backends unhealthy)")
	}

	if len(results) != 2 {
		t.Fatalf("HealthCheck() returned %d results, want 2", len(results))
	}

	for _, result := range results {
		if result.Healthy {
			t.Errorf("Backend %q should be unhealthy", result.Name)
		}
		if result.Error == "" {
			t.Errorf("Backend %q should have an error message", result.Name)
		}
	}
}

func TestRouter_HealthCheck_ContextCancellation(t *testing.T) {
	backend := NewMockBackend(MockBackendConfig{Name: "test"})

	router, err := NewRouter(RouterConfig{
		Backends: []BackendEntry{
			{Backend: backend, IsDefault: true},
		},
	})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	// Use already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// MockBackend.Health ignores context, so this should still work
	// But we're testing the API accepts a context
	results, allHealthy := router.HealthCheck(ctx)

	// MockBackend always returns healthy since it doesn't check context
	if !allHealthy {
		t.Error("HealthCheck() allHealthy = false, want true (mock ignores context)")
	}

	if len(results) != 1 {
		t.Fatalf("HealthCheck() returned %d results, want 1", len(results))
	}
}
