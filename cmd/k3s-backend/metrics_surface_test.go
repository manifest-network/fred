package main

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownPrefix is the prefix every collector this binary owns is declared under, in
// internal/backend/k3s/metrics.go.
const ownPrefix = "fred_k3s_backend_"

// sharedFamilies are the fred_ metric names this binary may legitimately export
// outside its own prefix: the internal/metrics/background panic counters, which
// providerd and every backend write. They are CounterVecs, so they appear only
// after a cleanup loop has actually panicked.
var sharedFamilies = map[string]bool{
	"fred_background_cleanup_panics_total":   true,
	"fred_background_goroutine_panics_total": true,
}

// TestMetricSurfaceIsBackendOwned mirrors the docker-backend assertion: this
// binary's /metrics may expose only metrics it can actually produce.
//
// k3s-backend had the same defect and the same single cause — one import of
// internal/metrics for one panic counter, dragging providerd's whole eagerly
// registered set into a process with no signer pool, withdraw loop or chain
// client (ENG-712). It is a non-functional scaffold (ENG-133), but it serves
// /metrics from the default gatherer exactly like the released binary does, so
// it gets the same guard.
//
// Unlike docker's, k3s's own collectors are all CounterVecs with no init-time
// pre-initialization, so a clean gather here is legitimately empty of fred_
// families. There is no non-vacuity counterpart to docker's assertion to make.
func TestMetricSurfaceIsBackendOwned(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "fred_") {
			continue // go_* / process_* collectors registered by client_golang itself
		}
		if sharedFamilies[name] {
			continue
		}
		assert.True(t, strings.HasPrefix(name, ownPrefix),
			"k3s-backend exports %q, which it does not own. Some package in this binary's "+
				"dependency closure imports a metrics package whose collectors register eagerly "+
				"(promauto -> default registerer at init), so the series exists here at a permanent "+
				"0 on a process that can never write it (ENG-712). Find it with: "+
				"go list -deps ./cmd/k3s-backend | grep internal/metrics", name)
	}
}
