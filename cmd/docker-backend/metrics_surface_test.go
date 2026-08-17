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

	"github.com/manifest-network/fred/internal/backend/docker"
)

// ownPrefix is the namespace+subsystem every collector this binary owns is
// declared under, in internal/backend/docker/metrics.go.
const ownPrefix = "fred_docker_backend_"

// sharedFamilies are the fred_ metric names this binary may legitimately export
// outside its own prefix: the internal/metrics/background panic counters, which
// providerd and every backend write. They are CounterVecs, so they appear only
// after a cleanup loop has actually panicked and are normally absent here — the
// allowance exists so a future pre-initialized series does not fail this test.
var sharedFamilies = map[string]bool{
	"fred_background_cleanup_panics_total":   true,
	"fred_background_goroutine_panics_total": true,
}

// TestMetricSurfaceIsBackendOwned scrapes this binary's real /metrics route and
// asserts it exposes only metrics the process can actually produce.
//
// It is a link-graph assertion wearing a scrape's clothes. promauto registers on
// the default registerer at package init, so a collector materialises in a
// binary the moment any package in its dependency closure is linked — no call
// site required. cmd/docker-backend imported internal/backend/docker, which
// imported internal/metrics for a single panic counter, and thereby served 27
// providerd-only metric names at a permanent 0: signer pool, withdraw loop,
// chain gas, payload store, provisioner and backend router, on a process that
// has none of them (ENG-712).
//
// Asserting through Server.Handler() rather than on prometheus.DefaultGatherer
// is deliberate: the endpoint is what an operator's Prometheus consumes, and
// reading the exposition text counts names the way the wire does — each
// histogram as its _bucket/_sum/_count lines, which is why 21 leaked collectors
// were 27 leaked names.
//
// The check is an allowlist rather than a denylist of those 27 on purpose: the
// defect was never about those specific names, it was about anything at all
// crossing the fred<->backend boundary. NewServer(nil, ...) is safe because the
// /metrics route touches no backend.
//
// A gauge at 0 is what makes this worth a test. `fred_signer_pool_lane_count < 3`
// matched three docker-backends and missed the one host with a signer pool —
// wrong in both directions at once, and silent.
func TestMetricSurfaceIsBackendOwned(t *testing.T) {
	h := NewServer(nil, testSecret, slog.Default(), docker.DefaultMaxRequestBodySize).Handler()

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
			"docker-backend serves %q on /metrics, which it does not own. Some package in this "+
				"binary's dependency closure imports a metrics package whose collectors register "+
				"eagerly (promauto -> default registerer at init), so the series is exported here at "+
				"a permanent 0 by a process that can never write it (ENG-712). Find it with: "+
				"go list -deps ./cmd/docker-backend | grep internal/metrics", name)
		own++
	}

	// Guard against the assertion passing vacuously: internal/backend/docker's
	// init() pre-initializes its closed-label CounterVec series, so a healthy
	// scrape is far from empty. Zero own-prefixed names would mean the
	// collectors stopped being linked, not that the surface is clean.
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
