package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
)

func TestHealthCheckChainCannotConsumeBackendProbeBudget(t *testing.T) {
	for _, chainTimesOut := range []bool{false, true} {
		name := "chain waits for backend arrival"
		if chainTimesOut {
			name = "chain exceeds shared deadline"
		}
		t.Run(name, func(t *testing.T) {
			arrived := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				close(arrived)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
				Backend: newBackendHTTPClientForTest(t, backendHTTPClientConfig{
					Name: "available", BaseURL: server.URL, Timeout: time.Second,
				}),
				IsDefault: true,
			}}})
			require.NoError(t, err)
			h := &Handlers{backendRouter: router, client: &chaintest.MockClient{PingFunc: func(ctx context.Context) error {
				if chainTimesOut {
					<-ctx.Done()
					return ctx.Err()
				}
				select {
				case <-arrived:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}}
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			chainSamples := healthTimingSamples(t, healthCheckChain, "")
			backendSamples := healthTimingSamples(t, healthCheckBackend, "available")
			response := h.evaluateHealth(ctx)
			assert.Equal(t, chainSamples+1, healthTimingSamples(t, healthCheckChain, ""))
			assert.Equal(t, backendSamples+1, healthTimingSamples(t, healthCheckBackend, "available"))
			assert.Equal(t, checkStatusHealthy, response.Checks["backend:available"].Status,
				"an independent backend must be probed before chain consumes the shared deadline")
			if chainTimesOut {
				assert.Equal(t, healthStatusDegraded, response.Status)
				assert.Equal(t, checkStatusUnhealthy, response.Checks["chain"].Status)
			} else {
				assert.Equal(t, healthStatusHealthy, response.Status)
			}
		})
	}
}

func healthTimingSamples(t *testing.T, check, backendName string) uint64 {
	t.Helper()
	var metric dto.Metric
	observer := metrics.HealthCheckDuration.WithLabelValues(check, backendName)
	require.NoError(t, observer.(prometheus.Metric).Write(&metric))
	return metric.GetHistogram().GetSampleCount()
}

func TestHealthCheckConcurrentProbesJoinOnCallerCancellation(t *testing.T) {
	chainEntered, backendEntered := make(chan struct{}), make(chan struct{})
	chainExited := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(backendEntered)
		<-r.Context().Done()
	}))
	defer server.Close()
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: newBackendHTTPClientForTest(t, backendHTTPClientConfig{
				Name: "canceled-health", BaseURL: server.URL, Timeout: time.Second,
			}), IsDefault: true,
		}}, BackendHealthy: metrics.BackendHealthy,
	})
	require.NoError(t, err)
	h := &Handlers{backendRouter: router, client: &chaintest.MockClient{PingFunc: func(ctx context.Context) error {
		close(chainEntered)
		<-ctx.Done()
		close(chainExited)
		return ctx.Err()
	}}}
	metrics.HealthCheckHealthy.WithLabelValues("chain").Set(1)
	metrics.BackendHealthy.WithLabelValues("canceled-health").Set(1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	completed := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		h.HealthCheck(recorder, httptest.NewRequestWithContext(ctx, http.MethodGet, "/health", nil))
		completed <- recorder
	}()
	for _, entered := range []<-chan struct{}{chainEntered, backendEntered} {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("independent probes did not both start")
		}
	}
	cancel()
	select {
	case recorder := <-completed:
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Contains(t, recorder.Body.String(), `"status":"degraded"`)
	case <-time.After(time.Second):
		t.Fatal("health evaluation did not join its canceled probes")
	}
	select {
	case <-chainExited:
	default:
		t.Fatal("health returned before its chain probe exited")
	}
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.HealthCheckHealthy.WithLabelValues("chain")))
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.BackendHealthy.WithLabelValues("canceled-health")),
		"caller cancellation cannot manufacture evidence of a dependency failure")
}

func TestHealthCheckChainProbePanicIsContained(t *testing.T) {
	before := testutil.ToFloat64(metrics.ChainHealthProbePanicsTotal)
	h := &Handlers{client: &chaintest.MockClient{PingFunc: func(context.Context) error {
		panic("broken chain client")
	}}}
	response := h.evaluateHealth(t.Context())
	assert.Equal(t, healthStatusDegraded, response.Status)
	assert.Equal(t, checkStatusUnhealthy, response.Checks["chain"].Status)
	assert.Equal(t, "chain connectivity failed", response.Checks["chain"].Message)
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.ChainHealthProbePanicsTotal))
}
