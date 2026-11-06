package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	fredtestutil "github.com/manifest-network/fred/internal/testutil"
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

	requests := func(path, status string) float64 {
		t.Helper()
		value, _ := fredtestutil.CounterValue(t, apiRequestsMetric,
			map[string]string{"method": "GET", "path": path, "status": status})
		return value
	}
	m0, u0 := requests("/v1/test-f28/{id}", "200"), requests("unmatched", "404")

	// One matched request + two DISTINCT junk 404 scan paths.
	const scanA, scanB = "/scan-f28-should-not-exist", "/scan-f28-another-random"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/test-f28/550e8400-e29b-41d4-a716-446655440000", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanA, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanB, nil))

	require.Equal(t, 1.0, requests("/v1/test-f28/{id}", "200")-m0, "matched route labeled with its template")
	require.Equal(t, 2.0, requests("unmatched", "404")-u0, "both junk paths collapse into the single unmatched series")

	// Assert NO series exists keyed by a raw scan path, by gathering the
	// family's actual series: reading a child would create it and mask a
	// regression.
	present := gatheredPathLabels(t)
	require.NotContains(t, present, scanA, "middleware must not mint a per-path series for junk scan paths")
	require.NotContains(t, present, scanB, "middleware must not mint a per-path series for junk scan paths")
}

const apiRequestsMetric = "fred_api_requests_total"

// gatheredPathLabels returns the "path" label value of every series of the
// API request counter in the default registry.
func gatheredPathLabels(t *testing.T) []string {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var paths []string
	for _, family := range families {
		if family.GetName() != apiRequestsMetric {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, pair := range series.GetLabel() {
				if pair.GetName() == "path" {
					paths = append(paths, pair.GetValue())
				}
			}
		}
	}
	return paths
}
