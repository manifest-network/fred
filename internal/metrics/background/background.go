// Package background holds the Prometheus collectors for background-goroutine
// health — the `fred_background_*` family — which every fred binary emits.
// providerd counts panics in its token-tracker cleanup loop and its payload,
// ack-batcher and withdraw goroutines; the docker and k3s backends count panics
// in their callback, diagnostics, releases and retention cleanup loops.
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
// The rule for anything added here: it must be written by every binary that
// links this package, and it must carry labels. A *Vec registers eagerly like
// everything else but exports no child series until its first WithLabelValues,
// so a binary that never writes it pays nothing. An unlabelled Counter, Gauge or
// Histogram in this package would reintroduce ENG-712 across all of them.
//
// Collectors only providerd writes belong in internal/metrics; collectors only
// one backend writes belong in that backend's package-local metrics file. A
// third option, for shared code that must not reference a collector at all, is
// the injection seam used by backend.RouterConfig and backend.HTTPClientConfig.
package background

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "fred"

var (
	// CleanupPanicsTotal counts panics recovered inside background
	// cleanup loops (token tracker, callback store, diagnostics store,
	// etc.) driven by util.StartCleanupLoop. Any non-zero value is a
	// latent bug in the cleanup function. Labeled by component.
	CleanupPanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "cleanup_panics_total",
		Help:      "Panics recovered in background cleanup loops, by component",
	}, []string{"component"})

	// GoroutinePanicsTotal is the catch-all for long-lived background
	// goroutines that add their own recover() (payload store writer,
	// ack batcher lanes, etc.). Labeled by component for correlation.
	GoroutinePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "goroutine_panics_total",
		Help:      "Panics recovered in long-lived background goroutines, by component",
	}, []string{"component"})
)
