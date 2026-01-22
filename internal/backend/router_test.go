package backend

import (
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
