package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Router routes requests to backends based on SKU matching.
type Router struct {
	backends       []backendEntry
	backendsByName map[string]Backend // O(1) lookup by name
	defaultBackend Backend
	counter        atomic.Uint64 // round-robin counter for RouteRoundRobin

	// Optional Prometheus gauge for backend health (nil = skip recording)
	backendHealthy *prometheus.GaugeVec

	// Optional Prometheus handles for least-loaded routing (nil = skip recording).
	allocatedCPURatio *prometheus.GaugeVec // labels: backend
	routingFallback   prometheus.Counter

	// Optional Prometheus counter for panics inside a concurrent health probe.
	healthProbePanics prometheus.Counter
}

type backendEntry struct {
	backend Backend
	match   MatchCriteria
}

// MatchCriteria defines how to match a lease to a backend.
type MatchCriteria struct {
	SKUs []string // Match if SKU is in this exact list
}

// RouterConfig configures the backend router.
type RouterConfig struct {
	Backends []BackendEntry

	// Optional Prometheus gauge for backend health status. When nil, HealthCheck
	// skips metric recording. This prevents binaries that don't use these metrics
	// from registering phantom fred-level gauges via transitive imports.
	BackendHealthy *prometheus.GaugeVec // labels: backend

	// AllocatedCPURatio, when non-nil, records the allocated-CPU ratio observed
	// for each backend at provision-routing time. Labels: backend.
	AllocatedCPURatio *prometheus.GaugeVec

	// RoutingFallback, when non-nil, counts provision-routing decisions that
	// fell back to round-robin because no candidate exposed usable load stats.
	RoutingFallback prometheus.Counter

	// HealthProbePanics, when non-nil, counts panics recovered inside a
	// per-backend health probe goroutine. Non-zero is always a bug.
	HealthProbePanics prometheus.Counter
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
		backendsByName:    make(map[string]Backend),
		backendHealthy:    cfg.BackendHealthy,
		allocatedCPURatio: cfg.AllocatedCPURatio,
		routingFallback:   cfg.RoutingFallback,
		healthProbePanics: cfg.HealthProbePanics,
	}

	for i, entry := range cfg.Backends {
		if isNilBackend(entry.Backend) {
			return nil, fmt.Errorf("backend at index %d is nil", i)
		}
		name := entry.Backend.Name()
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("backend at index %d has an empty name", i)
		}
		if _, exists := r.backendsByName[name]; exists {
			return nil, fmt.Errorf("duplicate backend name %q", name)
		}

		r.backends = append(r.backends, backendEntry{
			backend: entry.Backend,
			match:   entry.Match,
		})

		// A backend name is durable placement identity. Ambiguous or empty names
		// are rejected above instead of silently changing which machine owns a
		// placement lookup.
		r.backendsByName[name] = entry.Backend

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

func isNilBackend(candidate Backend) bool {
	if candidate == nil {
		return true
	}
	value := reflect.ValueOf(candidate)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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

// RouteAll returns all backends that match the given SKU, deduplicated by name.
// If no backends match, returns nil.
func (r *Router) RouteAll(sku string) []Backend {
	seen := make(map[string]bool)
	var matches []Backend
	for _, entry := range r.backends {
		if r.matches(sku, entry.match) {
			name := entry.backend.Name()
			if !seen[name] {
				seen[name] = true
				matches = append(matches, entry.backend)
			}
		}
	}
	return matches
}

// RouteRoundRobin distributes requests across all backends matching the SKU
// using round-robin selection. Falls back to the default backend if no match.
func (r *Router) RouteRoundRobin(sku string) Backend {
	return r.routeRoundRobin(r.RouteAll(sku), r.defaultBackend)
}

// routeRoundRobin selects only from candidates. fallback is used solely when
// candidates is empty, which lets eligibility-aware callers retain the normal
// default-backend behavior without widening their candidate set.
func (r *Router) routeRoundRobin(candidates []Backend, fallback Backend) Backend {
	switch len(candidates) {
	case 0:
		return fallback
	case 1:
		return candidates[0]
	default:
		idx := r.counter.Add(1) - 1
		return candidates[idx%uint64(len(candidates))]
	}
}

// statsFetchTimeout bounds how long RouteForProvision waits for backend /stats
// responses. Routing must not block on a slow backend: a candidate that misses
// this deadline is treated as having no usable stats (excluded from the
// least-loaded comparison; eligible only for the round-robin fallback).
const statsFetchTimeout = 2 * time.Second

// cpuRatioEpsilon is the float tolerance under which two CPU-allocation ratios
// are considered equal. In a burst, concurrent provisions read the same /stats
// snapshot and produce byte-identical ratios; the epsilon only guards float
// noise so genuinely-equal snapshots fall through to the in-flight tiebreak.
const cpuRatioEpsilon = 1e-9

// RouteForProvision selects the SKU-matching backend with the lowest observed
// allocated-CPU ratio for a new provision. Ties (equal ratio within
// cpuRatioEpsilon — the burst case, where concurrent provisions read the same
// /stats snapshot) are broken by the fewest in-flight provisions, then by the
// round-robin counter so a burst of identical-state provisions spreads across
// the tied backends. Falls back to round-robin when no SKU-matching backend
// exposes usable load stats. inFlightByBackend may be nil (treated as all-zero).
//
// This is a deliberate live-query design — each candidate's /stats is fetched at
// decision time — NOT a passive-counter load balancer (Envoy/NGINX style). The
// binding CPU-allocation signal lives on the backends and fred holds no local
// per-SKU CPU weights, so the only way to learn load is to ask. A GetLoadStats
// error or timeout demotes that candidate to "no usable signal" (excluded from
// the comparison; the error is recorded by doGet as
// fred_backend_requests_total{operation="get_load_stats",status="error"}).
// A residual herd window remains when concurrent provisions read the same
// pre-update /stats snapshot and the in-flight tiebreak does not separate them;
// it is tolerated because the backend's 503 admission gate hard-caps any
// over-targeted backend and provision QPS is low. The round-robin counter is
// shared with RouteRoundRobin, so tie rotation is intentionally approximate.
func (r *Router) RouteForProvision(ctx context.Context, sku string, inFlightByBackend map[string]int) Backend {
	return r.routeForProvision(ctx, r.RouteAll(sku), r.defaultBackend, inFlightByBackend)
}

// RouteForProvisionAmong selects a provision backend using the same SKU,
// default, and load-balancing rules as RouteForProvision, but treats
// eligibleNames as a hard routing boundary. An empty set, or a set containing
// neither a matching backend nor the default backend, yields nil.
//
// When matching candidates expose no usable load stats, round-robin fallback is
// restricted to those eligible candidates. The default backend is considered
// only when no eligible backend matches the SKU, and only when it is itself
// eligible.
func (r *Router) RouteForProvisionAmong(
	ctx context.Context,
	sku string,
	eligibleNames map[string]struct{},
	inFlightByBackend map[string]int,
) Backend {
	if len(eligibleNames) == 0 {
		return nil
	}

	allCandidates := r.RouteAll(sku)
	candidates := make([]Backend, 0, len(allCandidates))
	for _, candidate := range allCandidates {
		if _, eligible := eligibleNames[candidate.Name()]; eligible {
			candidates = append(candidates, candidate)
		}
	}

	var fallback Backend
	if _, eligible := eligibleNames[r.defaultBackend.Name()]; eligible {
		fallback = r.defaultBackend
	}

	return r.routeForProvision(ctx, candidates, fallback, inFlightByBackend)
}

func (r *Router) routeForProvision(
	ctx context.Context,
	candidates []Backend,
	fallback Backend,
	inFlightByBackend map[string]int,
) Backend {
	switch len(candidates) {
	case 0:
		return fallback
	case 1:
		return candidates[0]
	}

	// Fetch each candidate's load stats concurrently, bounded by statsFetchTimeout
	// so one slow backend cannot stall the provision decision.
	statsCtx, cancel := context.WithTimeout(ctx, statsFetchTimeout)
	defer cancel()

	type candidateLoad struct {
		backend Backend
		ratio   float64
		ok      bool
	}
	// Each goroutine writes only its own loads[i] slot (disjoint indices, no
	// shared-write race); wg.Wait() establishes happens-before for the reads below.
	// Loop vars i, b have per-iteration scope (go 1.22+), so direct capture is safe.
	loads := make([]candidateLoad, len(candidates))
	var wg sync.WaitGroup
	for i, b := range candidates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loads[i].backend = b
			stats, err := b.GetLoadStats(statsCtx)
			if err != nil {
				return // ok stays false → excluded; error counted by doGet's request metric
			}
			ratio, ok := stats.CPUAllocatedRatio()
			if !ok {
				return
			}
			loads[i].ratio = ratio
			loads[i].ok = true
			if r.allocatedCPURatio != nil {
				r.allocatedCPURatio.WithLabelValues(b.Name()).Set(ratio)
			}
		}()
	}
	wg.Wait()

	// Keep only candidates with usable stats.
	usable := make([]candidateLoad, 0, len(loads))
	for _, l := range loads {
		if l.ok {
			usable = append(usable, l)
		}
	}
	if len(usable) == 0 {
		if r.routingFallback != nil {
			r.routingFallback.Inc()
		}
		return r.routeRoundRobin(candidates, fallback)
	}

	// 1) Lowest CPU ratio.
	minRatio := math.Inf(1)
	for _, l := range usable {
		if l.ratio < minRatio {
			minRatio = l.ratio
		}
	}

	// 2) Among the effectively-least-loaded set, fewest in-flight provisions.
	minInFlight := math.MaxInt
	for _, l := range usable {
		if l.ratio <= minRatio+cpuRatioEpsilon {
			if c := inFlightByBackend[l.backend.Name()]; c < minInFlight {
				minInFlight = c
			}
		}
	}

	// 3) Collect exact ties (least-loaded AND fewest in-flight); rotate via the
	// round-robin counter so concurrent identical-state provisions spread.
	var tied []Backend
	for _, l := range usable {
		if l.ratio <= minRatio+cpuRatioEpsilon && inFlightByBackend[l.backend.Name()] == minInFlight {
			tied = append(tied, l.backend)
		}
	}
	if len(tied) == 1 {
		return tied[0]
	}
	idx := r.counter.Add(1) - 1
	return tied[idx%uint64(len(tied))]
}

// matches checks if a SKU matches the given criteria.
// Empty criteria (no SKU list) matches nothing;
// use IsDefault to designate a fallback backend.
func (r *Router) matches(sku string, match MatchCriteria) bool {
	return slices.Contains(match.SKUs, sku)
}

// Default returns the default backend.
func (r *Router) Default() Backend {
	return r.defaultBackend
}

// Backends returns all unique backends for operations like reconciliation and health checks.
// The same backend may be registered multiple times with different SKU lists, but
// this method returns each backend only once (deduplicated by name).
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
//
// Probes run CONCURRENTLY, and the caller is expected to pass a
// deadline-bounded context. This sits on the /health and /readyz request path,
// where probing serially made the worst case the SUM of every backend's client
// timeout (30s each by default) rather than the MAX. One backend that accepts
// a connection and never answers was therefore enough to push the handler past
// the API server's own request timeout — whose response body is a 503, the
// exact status code /health exists never to send (ENG-522). Concurrency is what
// lets a bounded budget hold without starving the backends probed last.
//
// Results stay in Backends() order regardless of completion order: each probe
// writes its own slot, so a caller diffing successive responses sees a stable
// sequence.
func (r *Router) HealthCheck(ctx context.Context) ([]BackendHealth, bool) {
	backends := r.Backends()
	results := make([]BackendHealth, len(backends))

	var wg sync.WaitGroup
	for i, b := range backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = r.probeBackend(ctx, b)
		}()
	}
	wg.Wait()

	allHealthy := true
	for _, health := range results {
		if !health.Healthy {
			allHealthy = false
		}
	}

	return results, allHealthy
}

// probeBackend runs one backend health probe and records its gauge.
//
// It recovers because this now runs on its own goroutine, where net/http's
// per-connection panic recovery no longer applies: an unrecovered panic inside
// a backend client used to fail one request and would now take the whole daemon
// down. A panicking probe counts as unhealthy rather than as a silent pass.
func (r *Router) probeBackend(ctx context.Context, b Backend) (health BackendHealth) {
	health = BackendHealth{Name: b.Name(), Healthy: true}

	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("backend health probe panicked", "backend", b.Name(), "panic", rec)
			if r.healthProbePanics != nil {
				r.healthProbePanics.Inc()
			}
			health = BackendHealth{Name: b.Name(), Healthy: false, Error: "health probe panicked"}
		}
	}()

	err := b.Health(ctx)
	if err == nil {
		if r.backendHealthy != nil {
			r.backendHealthy.WithLabelValues(b.Name()).Set(1)
		}
		return health
	}

	health.Healthy = false
	health.Error = err.Error()

	// Record the gauge for genuine backend failures, and a probe that blew its
	// DEADLINE is one: a backend that accepts the connection and never answers
	// is unhealthy, and that is precisely the case this endpoint most needs to
	// surface now that it no longer reaches the status code. Only a CANCELED
	// probe is excluded — cancellation reflects the caller's lifecycle (client
	// disconnect, server shutdown), not the backend's health.
	if r.backendHealthy != nil && !errors.Is(ctx.Err(), context.Canceled) {
		r.backendHealthy.WithLabelValues(b.Name()).Set(0)
	}

	return health
}
