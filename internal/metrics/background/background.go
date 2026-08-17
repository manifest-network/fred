// Package background holds the Prometheus collectors for background-goroutine
// health — the whole `fred_background_*` family. CleanupPanicsTotal is written
// by every fred binary: providerd for its token-tracker cleanup loop, the docker
// backend for its callback, diagnostics, releases and retention loops, the k3s
// backend for callback, diagnostics and releases (it has no retention store —
// retention is docker-only, ENG-325). GoroutinePanicsTotal is written only by
// providerd, for its payload-writer, ack-batcher and withdraw goroutines; it
// lives here anyway, see the rule below.
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
// not a requirement of membership. GoroutinePanicsTotal is providerd-only and
// still belongs: it is the other half of the fred_background_* family and of one
// ARCHITECTURE.md table, and being label-bearing it costs the backends nothing.
// Splitting the family would only invite the ENG-712 import edge back the next
// time a backend needs a goroutine-panic counter.
//
// What does NOT belong here: a providerd-only collector unrelated to this family
// (internal/metrics), and one only a single backend writes (that backend's
// package-local metrics file). For shared code that must reference no collector
// at all there is a third option — the injection seam used by
// backend.RouterConfig and backend.HTTPClientConfig.
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
