package main

import (
	"bufio"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/k3s"
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
// binary's /metrics may serve only metrics it can actually produce.
//
// k3s-backend had the same defect and the same single cause — one import of
// internal/metrics for one panic counter, dragging providerd's whole eagerly
// registered set into a process with no signer pool, withdraw loop or chain
// client (ENG-712). It is a non-functional scaffold (ENG-133), but it serves
// /metrics from the default gatherer exactly like the released binary does, so
// it gets the same guard.
//
// A clean scrape here is thinner than docker's but not empty: k3s declares two
// CounterVecs, which stay silent, and one unlabelled counter
// (callbackStoreErrorsTotal, internal/backend/k3s/metrics.go), which exports at 0
// from init. That one name is what keeps the assertion below from passing
// vacuously — and it is the legitimate form of the same mechanism ENG-712 is
// about, since a binary exporting its own counter at 0 is how absence is told
// apart from healthy.
func TestMetricSurfaceIsBackendOwned(t *testing.T) {
	h := NewServer(nil, testSecret, slog.Default(), k3s.DefaultMaxRequestBodySize).Handler()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var own int
	for _, name := range scrapedMetricNames(t, rec.Body.String()) {
		if !strings.HasPrefix(name, "fred_") {
			continue // go_* / process_* collectors registered by client_golang itself
		}
		if sharedFamilies[name] {
			continue
		}
		assert.True(t, strings.HasPrefix(name, ownPrefix),
			"k3s-backend serves %q on /metrics, which it does not own. Some package in this "+
				"binary's dependency closure imports a metrics package whose collectors register "+
				"eagerly (promauto -> default registerer at init), so the series is exported here at "+
				"a permanent 0 by a process that can never write it (ENG-712). Find it with: "+
				"go list -deps ./cmd/k3s-backend | grep internal/metrics", name)
		own++
	}

	assert.NotZero(t, own, "expected /metrics to serve this binary's own %s* metrics", ownPrefix)
}

// scrapedMetricNames returns the metric name of every sample line in a
// Prometheus text exposition, in wire form: histograms and summaries contribute
// their _bucket/_sum/_count names rather than one family name.
func scrapedMetricNames(t *testing.T, body string) []string {
	t.Helper()

	var names []string
	seen := make(map[string]bool)
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// `name{labels...} value` or `name value`
		name := line
		if i := strings.IndexAny(name, "{ "); i >= 0 {
			name = name[:i]
		}
		if name != "" && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, names, "/metrics served no samples at all")
	return names
}
