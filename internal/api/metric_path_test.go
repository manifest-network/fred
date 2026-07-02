package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
	perScan := metrics.APIRequestsTotal.WithLabelValues("GET", "/scan-f28-should-not-exist", "404")
	m0, u0, s0 := testutil.ToFloat64(matched), testutil.ToFloat64(unmatched), testutil.ToFloat64(perScan)

	// One matched request + two DISTINCT junk 404 scan paths.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/test-f28/550e8400-e29b-41d4-a716-446655440000", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/scan-f28-should-not-exist", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/scan-f28-another-random", nil))

	require.Equal(t, 1.0, testutil.ToFloat64(matched)-m0, "matched route labeled with its template")
	require.Equal(t, 2.0, testutil.ToFloat64(unmatched)-u0, "both junk paths collapse into the single unmatched series")
	require.Equal(t, 0.0, testutil.ToFloat64(perScan)-s0, "no per-scan-path series is created")
}
