package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	fredtestutil "github.com/manifest-network/fred/internal/testutil"
)

// TestLoggingMiddleware_BoundsMetricPathLabel is the F28 regression: junk 404
// scan paths must collapse to a single "unmatched" series instead of minting a
// new time series per path, while a matched route is labeled with its template.
func TestLoggingMiddleware_BoundsMetricPathLabel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/test-f28/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := loggingMiddleware(mux)

	m0, u0 := apiRequests(t, "GET", "/v1/test-f28/{id}", "200"), apiRequests(t, "GET", "unmatched", "404")

	// One matched request + two DISTINCT junk 404 scan paths.
	const scanA, scanB = "/scan-f28-should-not-exist", "/scan-f28-another-random"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/test-f28/550e8400-e29b-41d4-a716-446655440000", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanA, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", scanB, nil))

	require.Equal(t, 1.0, apiRequests(t, "GET", "/v1/test-f28/{id}", "200")-m0, "matched route labeled with its template")
	require.Equal(t, 2.0, apiRequests(t, "GET", "unmatched", "404")-u0, "both junk paths collapse into the single unmatched series")

	// Assert NO series exists keyed by a raw scan path, by gathering the
	// family's actual series: reading a child would create it and mask a
	// regression.
	present := gatheredLabels(t, "path")
	require.NotContains(t, present, scanA, "middleware must not mint a per-path series for junk scan paths")
	require.NotContains(t, present, scanB, "middleware must not mint a per-path series for junk scan paths")
}

// TestLoggingMiddleware_BoundsMetricMethodLabel is the ENG-2150 regression:
// Go's server hands any method token to the handler and ServeMux answers 405,
// so invented methods must share the single "other" series instead of minting
// one each. A HEAD request, which a GET route serves, keeps its own label.
func TestLoggingMiddleware_BoundsMetricMethodLabel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/test-2150", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := loggingMiddleware(mux)

	const route = "/v1/test-2150"
	head0, other0 := apiRequests(t, "HEAD", route, "200"), apiRequests(t, "other", "unmatched", "405")

	invented := []string{"XQ1-2150", "XQ2-2150", strings.Repeat("Z", 100_000)}
	for _, method := range invented {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, route, nil))
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodHead, route, nil))

	require.Equal(t, float64(len(invented)), apiRequests(t, "other", "unmatched", "405")-other0,
		"every invented method counts in the one other series")
	require.Equal(t, 1.0, apiRequests(t, "HEAD", route, "200")-head0)
	present := gatheredLabels(t, "method")
	for _, method := range invented {
		require.NotContains(t, present, method, "middleware must not mint a per-method series")
	}
}

const apiRequestsMetric = "fred_api_requests_total"

// apiRequests reads one API request counter series.
func apiRequests(t *testing.T, method, path, status string) float64 {
	t.Helper()
	value, _ := fredtestutil.CounterValue(t, apiRequestsMetric,
		map[string]string{"method": method, "path": path, "status": status})
	return value
}

// gatheredLabels returns the value of the named label for every series of the
// API request counter in the default registry.
func gatheredLabels(t *testing.T, label string) []string {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var values []string
	for _, family := range families {
		if family.GetName() != apiRequestsMetric {
			continue
		}
		for _, series := range family.GetMetric() {
			for _, pair := range series.GetLabel() {
				if pair.GetName() == label {
					values = append(values, pair.GetValue())
				}
			}
		}
	}
	return values
}
