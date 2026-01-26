package backend

import (
	"context"
	"fmt"
	"strings"
)

// Router routes requests to backends based on SKU matching.
type Router struct {
	backends       []backendEntry
	backendsByName map[string]Backend // O(1) lookup by name
	defaultBackend Backend
}

type backendEntry struct {
	backend Backend
	match   MatchCriteria
}

// MatchCriteria defines how to match a lease to a backend.
type MatchCriteria struct {
	SKUPrefix string   // Match if SKU starts with this prefix (e.g., "gpu-", "k8s-")
	SKUs      []string // Match if SKU is in this exact list
}

// RouterConfig configures the backend router.
type RouterConfig struct {
	Backends []BackendEntry
}

// BackendEntry pairs a backend with its matching criteria.
type BackendEntry struct {
	Backend   Backend
	Match     MatchCriteria
	IsDefault bool
}

// NewRouter creates a new backend router.
func NewRouter(cfg RouterConfig) (*Router, error) {
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("at least one backend is required")
	}

	r := &Router{
		backendsByName: make(map[string]Backend),
	}

	for i, entry := range cfg.Backends {
		if entry.Backend == nil {
			return nil, fmt.Errorf("backend at index %d is nil", i)
		}

		r.backends = append(r.backends, backendEntry{
			backend: entry.Backend,
			match:   entry.Match,
		})

		// Build name lookup map (first backend with a given name wins)
		name := entry.Backend.Name()
		if _, exists := r.backendsByName[name]; !exists {
			r.backendsByName[name] = entry.Backend
		}

		if entry.IsDefault {
			if r.defaultBackend != nil {
				return nil, fmt.Errorf("multiple default backends specified")
			}
			r.defaultBackend = entry.Backend
		}
	}

	// If no explicit default, use the first backend (already validated non-nil above)
	if r.defaultBackend == nil {
		r.defaultBackend = cfg.Backends[0].Backend
	}

	return r, nil
}

// Route returns the appropriate backend for the given SKU.
func (r *Router) Route(sku string) Backend {
	for _, entry := range r.backends {
		if r.matches(sku, entry.match) {
			return entry.backend
		}
	}
	return r.defaultBackend
}

// matches checks if a SKU matches the given criteria.
func (r *Router) matches(sku string, match MatchCriteria) bool {
	// Check SKU prefix first
	if match.SKUPrefix != "" && strings.HasPrefix(sku, match.SKUPrefix) {
		return true
	}

	// Check exact SKU list
	for _, s := range match.SKUs {
		if sku == s {
			return true
		}
	}

	return false
}

// Default returns the default backend.
func (r *Router) Default() Backend {
	return r.defaultBackend
}

// Backends returns all configured backends (for reconciliation).
func (r *Router) Backends() []Backend {
	seen := make(map[string]bool)
	var backends []Backend

	for _, entry := range r.backends {
		name := entry.backend.Name()
		if !seen[name] {
			seen[name] = true
			backends = append(backends, entry.backend)
		}
	}

	return backends
}

// GetBackendByName returns a backend by its name. Returns nil if not found.
func (r *Router) GetBackendByName(name string) Backend {
	return r.backendsByName[name]
}

// BackendHealth represents the health status of a single backend.
type BackendHealth struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

// HealthCheck checks the health of all configured backends.
// Returns a slice of health statuses and an overall healthy flag.
func (r *Router) HealthCheck(ctx context.Context) ([]BackendHealth, bool) {
	backends := r.Backends()
	results := make([]BackendHealth, 0, len(backends))
	allHealthy := true

	for _, b := range backends {
		health := BackendHealth{Name: b.Name(), Healthy: true}

		if err := b.Health(ctx); err != nil {
			health.Healthy = false
			health.Error = err.Error()
			allHealthy = false
		}

		results = append(results, health)
	}

	return results, allHealthy
}
