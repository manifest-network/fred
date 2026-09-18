package docker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics/background"
)

func TestStartOwnsNetworkReclamationWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, docker := newInterruptedVolumeStartBackend(t)
		b.cfg.NetworkIsolation = ptrBool(true)
		b.cfg.ReconcileInterval = time.Hour
		var passes atomic.Int32
		docker.ListIdleManagedNetworksFn = func(context.Context) ([]networktypes.Inspect, error) {
			passes.Add(1)
			return nil, nil
		}
		docker.CloseFn = func() error { return nil }
		stop := sync.OnceValue(b.Stop)
		t.Cleanup(func() { require.NoError(t, stop()) })
		require.NoError(t, b.Start(t.Context()))
		synctest.Wait()
		require.Equal(t, int32(1), passes.Load(), "successful startup must launch an immediate independent cleanup pass")
		require.NoError(t, stop())
		time.Sleep(time.Hour)
		synctest.Wait()
		require.Equal(t, int32(1), passes.Load(), "Stop must join the reclamation worker before closing its dependencies")
	})
}

func TestRecoveryLoopsContainPanicsAndRetryNextCadence(t *testing.T) {
	for _, kind := range []string{"docker_reconciliation", "docker_network_reclamation"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBackendForTest(&mockDockerClient{}, nil)
				b.cfg.ReconcileInterval = time.Minute
				defer func() { b.stopCancel(); b.wg.Wait() }()
				var calls atomic.Int32
				panics := background.CleanupPanicsTotal.WithLabelValues(kind)
				before := testutil.ToFloat64(panics)
				work := func(context.Context) error {
					if calls.Add(1) == 1 {
						panic("injected recovery iteration failure")
					}
					return errors.New("next iteration is independently attempted")
				}
				if kind == "docker_reconciliation" {
					b.imageInspectionRecovery = work
					b.wg.Go(b.reconcileLoop)
					synctest.Wait()
					require.Zero(t, calls.Load(), "state recovery starts at its configured cadence")
					time.Sleep(time.Minute)
				} else {
					b.backgroundMaintenance.cleanupOrphanedNetworksFn = func(ctx context.Context) { _ = work(ctx) }
					b.wg.Go(b.networkCleanupLoop)
				}
				synctest.Wait()
				require.Equal(t, int32(1), calls.Load())
				require.Equal(t, before+1, testutil.ToFloat64(panics))
				time.Sleep(time.Minute)
				synctest.Wait()
				require.Equal(t, int32(2), calls.Load(), "a panic cannot kill the periodic convergence worker")
				require.Equal(t, before+1, testutil.ToFloat64(panics))
				b.stopCancel()
				b.wg.Wait()
				time.Sleep(time.Minute)
				require.Equal(t, int32(2), calls.Load())
			})
		})
	}
}
