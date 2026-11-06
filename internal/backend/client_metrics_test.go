package backend

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientBindsEveryOperationsCountersWhenBuilt pins that a client's request
// series exist before its first request: building the client binds every
// operation it can send, and recording only selects a bound series.
func TestClientBindsEveryOperationsCountersWhenBuilt(t *testing.T) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bind_test_requests"},
		[]string{"backend", "operation", "status"})
	malformed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bind_test_malformed"},
		[]string{"backend", "operation"})
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "bind-test", BaseURL: "http://localhost",
		RequestsTotal: requests, MalformedErrorBodyTotal: malformed,
	})

	require.Len(t, client.requests, len(requestOperations))
	assert.Equal(t, 4*len(requestOperations), promtestutil.CollectAndCount(requests),
		"every operation's four statuses exist before any request")
	assert.Equal(t, len(requestOperations), promtestutil.CollectAndCount(malformed))

	seen := make(map[string]bool, len(requestOperations))
	for _, operation := range requestOperations {
		assert.False(t, seen[operation.name], "operation %s is listed once", operation)
		seen[operation.name] = true
	}

	client.recordMetrics(requestRestore, time.Now(), nil)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(requests.WithLabelValues("bind-test", "restore", "success")))
	assert.Equal(t, 4*len(requestOperations), promtestutil.CollectAndCount(requests),
		"recording creates no series")
}
