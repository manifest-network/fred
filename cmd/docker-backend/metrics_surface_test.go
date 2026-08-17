package main

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownPrefix is the namespace+subsystem every collector this binary owns is
// declared under, in internal/backend/docker/metrics.go.
const ownPrefix = "fred_docker_backend_"

// sharedFamilies are the fred_ metric names this binary may legitimately export
// outside its own prefix: the internal/metrics/background panic counters, which
// providerd and every backend write. They are CounterVecs, so they appear only
// after a cleanup loop has actually panicked and normally are absent here — the
// allowance exists so a future pre-initialized series does not fail this test.
var sharedFamilies = map[string]bool{
	"fred_background_cleanup_panics_total":   true,
	"fred_background_goroutine_panics_total": true,
}

// TestMetricSurfaceIsBackendOwned asserts that this binary's /metrics exposes
// only metrics it can actually produce.
//
// This is a link-graph assertion wearing a registry's clothes. promauto
// registers on the default registerer at package init, so a collector
// materialises in a binary the moment any package in its dependency closure is
// linked — no call site required. cmd/docker-backend imported
// internal/backend/docker, which imported internal/metrics for a single panic
// counter, and thereby exported 27 providerd-only metric names at a permanent 0:
// signer pool, withdraw loop, chain gas, payload store, provisioner and backend
// router, on a process that has none of them (ENG-712).
//
// The check is an allowlist rather than a denylist of those 27 on purpose: the
// defect was never about those specific names, it was about anything at all
// leaking across the fred↔backend boundary. main_test.go's TestMain wires no
// metrics, so a failure here is always an import, not a call.
//
// A gauge at 0 is what makes this worth a test. `fred_signer_pool_lane_count < 3`
// matched three docker-backends and missed the one host with a signer pool —
// wrong in both directions at once, and silent.
func TestMetricSurfaceIsBackendOwned(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var own int
	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "fred_") {
			continue // go_* / process_* collectors registered by client_golang itself
		}
		if sharedFamilies[name] {
			continue
		}
		assert.True(t, strings.HasPrefix(name, ownPrefix),
			"docker-backend exports %q, which it does not own. Some package in this binary's "+
				"dependency closure imports a metrics package whose collectors register eagerly "+
				"(promauto -> default registerer at init), so the series exists here at a permanent "+
				"0 on a process that can never write it (ENG-712). Find it with: "+
				"go list -deps ./cmd/docker-backend | grep internal/metrics", name)
		own++
	}

	// Guard against the assertion passing vacuously: internal/backend/docker's
	// init() pre-initializes its closed-label CounterVec series, so a healthy
	// gather is far from empty. Zero own-prefixed families would mean the
	// collectors stopped being linked, not that the surface is clean.
	assert.NotZero(t, own, "expected this binary to export its own %s* metrics", ownPrefix)
}
