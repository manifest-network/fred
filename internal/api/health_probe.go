package api

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
)

// These are the fixed provider health timing labels. Backend names, when
// present, come only from the configured Router.
const (
	healthCheckChain     = "chain"
	healthCheckBackend   = "backend"
	healthCheckToken     = "token_tracker"
	healthCheckPlacement = "placement_store"
	healthCheckInventory = "placement_inventory"
	healthCheckPayload   = "payload_store"
)

type healthProbeResult struct {
	err            error
	duration       time.Duration
	callerCanceled bool
}

func measureHealthProbe(ctx context.Context, probe func() error) healthProbeResult {
	start := time.Now()
	err := probe()
	return healthProbeResult{
		err: err, duration: time.Since(start), callerCanceled: errors.Is(ctx.Err(), context.Canceled),
	}
}

// startChainHealthProbe overlaps the independent chain and backend probes under
// their one request deadline. The caller always receives the result before
// returning; the buffered channel owns no work after evaluation completes.
func (h *Handlers) startChainHealthProbe(ctx context.Context) <-chan healthProbeResult {
	results := make(chan healthProbeResult, 1)
	go func() {
		results <- measureHealthProbe(ctx, func() (err error) {
			// This is the foreign-client goroutine boundary: a client panic must
			// become an unhealthy observation rather than terminate providerd.
			defer func() {
				if recovered := recover(); recovered != nil {
					slog.Error("chain health probe panicked", "panic", recovered, "stack", string(debug.Stack()))
					metrics.ChainHealthProbePanicsTotal.Inc()
					err = errors.New("chain health probe panicked")
				}
			}()
			if err := ctx.Err(); err != nil {
				return err
			}
			return h.client.Ping(ctx)
		})
	}()
	return results
}
