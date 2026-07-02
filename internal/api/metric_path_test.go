package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics"
)

func TestMetricPath(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{"method + templated path", "POST /v1/leases/{lease_uuid}/update", "/v1/leases/{lease_uuid}/update"},
		{"method + static path", "GET /health", "/health"},
		{"path only (no method)", "/metrics", "/metrics"},
		{"unmatched (empty pattern)", "", "unmatched"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, metricPath(&http.Request{Pattern: tt.pattern}))
		})
	}
}

// TestLoggingMiddleware_BoundsMetricPathLabel is the F28 regression: junk 404
// scan paths must collapse to a single "unmatched" series instead of minting a
// new time series per path, while a matched route is labeled with its template.
func TestLoggingMiddleware_BoundsMetricPathLabel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/test-f28/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := loggingMiddleware(mux)

	matched := metrics.APIRequestsTotal.WithLabelValues("GET", "/v1/test-f28/{id}", "200")
	unmatched := metrics.APIRequestsTotal.WithLabelValues("GET", "unmatched", "404")
	m0, u0 := testutil.ToFloat64(matched), testutil.ToFloat64(unmatched)

	// One matched request + two DISTINCT junk 404 scan paths.
	const scanA, scanB = "/scan-f28-should-not-exist", "/scan-f28-another-random"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/test-f28/550e8400-e29b-41d4-a716-446655440000", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanA, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanB, nil))

	require.Equal(t, 1.0, testutil.ToFloat64(matched)-m0, "matched route labeled with its template")
	require.Equal(t, 2.0, testutil.ToFloat64(unmatched)-u0, "both junk paths collapse into the single unmatched series")

	// Assert NO series exists keyed by a raw scan path. Collect the vec's actual
	// series rather than reading a pre-referenced child with ToFloat64 — the
	// latter would itself instantiate the series and mask a regression.
	present := collectPathLabels(t, metrics.APIRequestsTotal)
	require.NotContains(t, present, scanA, "middleware must not mint a per-path series for junk scan paths")
	require.NotContains(t, present, scanB, "middleware must not mint a per-path series for junk scan paths")
}

// collectPathLabels returns the set of "path" label values across every series
// currently registered in the collector.
func collectPathLabels(t *testing.T, c prometheus.Collector) []string {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var paths []string
	for m := range ch {
		var d dto.Metric
		require.NoError(t, m.Write(&d))
		for _, lp := range d.Label {
			if lp.GetName() == "path" {
				paths = append(paths, lp.GetValue())
			}
		}
	}
	return paths
}
