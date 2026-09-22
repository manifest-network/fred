package api

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
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

type observedHealthBackend struct {
	*backend.MockBackend
	probe func(context.Context) error
}

func (b *observedHealthBackend) Health(ctx context.Context) error { return b.probe(ctx) }

func TestHealthCheckMeasuresCompletedProbeDurations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chainDelay, backendDelay = 11 * time.Millisecond, 37 * time.Millisecond
		b := &observedHealthBackend{
			MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "timed-health"}),
			probe:       func(context.Context) error { time.Sleep(backendDelay); return nil },
		}
		router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}}})
		require.NoError(t, err)
		h := &Handlers{backendRouter: router, client: &chaintest.MockClient{PingFunc: func(context.Context) error {
			time.Sleep(chainDelay)
			return errors.New("chain unavailable")
		}}}
		beforeChain := healthTimingSum(t, healthCheckChain, "")
		beforeBackend := healthTimingSum(t, healthCheckBackend, "timed-health")
		result := h.evaluateHealth(t.Context())
		assert.Equal(t, healthStatusDegraded, result.Status)
		assert.InDelta(t, chainDelay.Seconds(), healthTimingSum(t, healthCheckChain, "")-beforeChain, 1e-9)
		assert.InDelta(t, backendDelay.Seconds(), healthTimingSum(t, healthCheckBackend, "timed-health")-beforeBackend, 1e-9)
	})
}

func TestHealthCheckPreservesCompletedChainFailureAcrossLaterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &observedHealthBackend{
			MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "late-cancel-health"}),
			probe:       func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		}
		router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}}})
		require.NoError(t, err)
		h := &Handlers{backendRouter: router, client: &chaintest.MockClient{PingFunc: func(context.Context) error {
			return errors.New("chain failed before cancellation")
		}}}
		metrics.HealthCheckHealthy.WithLabelValues(healthCheckChain).Set(1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan HealthResponse, 1)
		go func() { done <- h.evaluateHealth(ctx) }()
		// The chain result is already complete; only the independent backend waits.
		synctest.Wait()
		cancel()
		result := <-done
		assert.Equal(t, healthStatusDegraded, result.Status)
		assert.Equal(t, 0.0, testutil.ToFloat64(metrics.HealthCheckHealthy.WithLabelValues(healthCheckChain)),
			"later caller cancellation cannot erase already observed chain failure")
	})
}

func healthTimingSum(t *testing.T, check, backendName string) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, metrics.HealthCheckDuration.WithLabelValues(check, backendName).(prometheus.Metric).Write(&metric))
	return metric.GetHistogram().GetSampleSum()
}
