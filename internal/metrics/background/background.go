// Package background holds the Prometheus collectors for background-goroutine
// health — the whole `fred_background_*` family. The cleanup-panic counter is
// written by every fred binary: providerd for its token-tracker cleanup loop,
// the docker backend for its callback, diagnostics, releases and retention
// loops and its docker_reconciliation, docker_network_reclamation,
// docker_projid_audit, docker_seccomp_census, docker_runtime_inventory,
// docker_runtime_refresh and docker_volume_delete_hold workers, the k3s
// backend for callback, diagnostics and releases (it has no retention store —
// retention is docker-only, ENG-325). The goroutine-panic counter is written by
// providerd for its payload-writer, ack-batcher, withdraw-scheduler,
// timeout-checker and maintenance-recovery goroutines, by both bundled backends
// for the shared callback-replay workers, and by the docker backend for its
// container-death overflow reporter.
//
// It exists as a package of its own because its parent, internal/metrics, is
// providerd's. Those collectors are registered on the default registerer at
// package init, so any binary that links internal/metrics exports all of them —
// and the unlabelled ones export a series at a permanent 0 even on a process
// that can never write them. A docker-backend reporting
// fred_signer_pool_lane_count = 0 is not neutral: a 0 satisfies ordinary gauge
// comparisons, so the natural `fred_signer_pool_lane_count < 3` returned every
// backend and not the one host that has a signer pool (ENG-712). Two collectors
// written on both sides of the fred↔backend boundary were the whole reason the
// backends linked that package.
//
// The rule for anything added here is that it must carry labels. That is the
// enforceable invariant, and the one this package's test asserts: a *Vec
// registers eagerly like everything else but exports no child series until its
// first WithLabelValues, so a binary that never writes it pays nothing. An
// unlabelled Counter, Gauge or Histogram here would export a permanent 0 on all
// three binaries at once — ENG-712 with a wider blast radius than the original.
//
// Being written by more than one binary is the reason to put a collector here,
// not a requirement of membership.
//
// Both vecs are unexported. A component's counter comes from CleanupPanics or
// GoroutinePanics, called in a package-level variable declaration of the
// package that runs the component (internal/testutil's counter series guard
// enforces that). So every binary exports a 0 for exactly the components of
// the packages it links, and the first panic after a restart is an increase()
// from 0 instead of a series that appears already at 1.
//
// What does NOT belong here: a providerd-only collector unrelated to this family
// (internal/metrics), and one only a single backend writes (that backend's
// package-local metrics file). For shared code that must reference no collector
// at all there is a third option — the injection seam used by
// backend.RouterConfig and backend.HTTPClientOptions.
package background

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "fred"

var (
	// cleanupPanicsTotal counts panics recovered inside background
	// cleanup loops (token tracker, callback store, diagnostics store,
	// etc.) driven by util.StartCleanupLoop. Any non-zero value is a
	// latent bug in the cleanup function. Labeled by component.
	cleanupPanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "cleanup_panics_total",
		Help:      "Panics recovered in background cleanup loops, by component",
	}, []string{"component"})

	// goroutinePanicsTotal is the catch-all for long-lived background
	// goroutines that add their own recover() (payload store writer,
	// ack batcher lanes, etc.). Labeled by component for correlation.
	goroutinePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "goroutine_panics_total",
		Help:      "Panics recovered in long-lived background goroutines, by component",
	}, []string{"component"})
)

// CleanupPanics returns the cleanup-panic counter of one component, creating
// its series at 0. Call it only in a package-level variable declaration.
func CleanupPanics(component string) prometheus.Counter {
	return cleanupPanicsTotal.WithLabelValues(component)
}

// GoroutinePanics returns the goroutine-panic counter of one component,
// creating its series at 0. Call it only in a package-level variable
// declaration.
func GoroutinePanics(component string) prometheus.Counter {
	return goroutinePanicsTotal.WithLabelValues(component)
}
