package docker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestHealthSamplesDurableVolumeLaunchDebt(t *testing.T) {
	for _, replyLost := range []bool{false, true} {
		t.Run(fmt.Sprintf("reply lost=%v", replyLost), func(t *testing.T) {
			h := newVolumeDispatchHarness(t)
			value, ok := operationIntentTestAuthorities.Load(h.callbacks)
			require.True(t, ok)
			authority := value.(*operationIntentTestAuthority)
			h.backend.callbackStore = h.callbacks
			h.backend.releaseStore = authority.releases
			h.backend.retentionStore = authority.retentions
			var err error
			h.backend.diagnosticsStore, err = shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: filepath.Join(t.TempDir(), "diagnostics.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.backend.diagnosticsStore.Close()) })
			cfg := DefaultConfig()
			h.backend.pool = shared.NewResourcePool(1, 1024, 1024, cfg.GetSKUProfile, nil)
			h.backend.docker = &mockDockerClient{PingFn: func(context.Context) error { return nil }}

			require.NoError(t, h.backend.Health(t.Context()))
			requireVolumeLaunchGauge(t, 0)
			h.compose.UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
				// The real journal has committed before the SDK launch is entered.
				// Health must sample that debt even while the request is in flight.
				require.NoError(t, h.backend.Health(t.Context()))
				requireVolumeLaunchGauge(t, 1)
				if replyLost {
					return errors.New("daemon reply lost")
				}
				return nil
			}
			h.execute(t, func(ctx context.Context, q *quiescedVolumes) error {
				return h.backend.volumeLaunches.compose(ctx, q, h.prepared, composeUpOpts{})
			})
			require.NoError(t, h.backend.Health(t.Context()))
			if replyLost {
				requireVolumeLaunchGauge(t, 1)
				require.NoError(t, h.callbacks.Close())
				require.Error(t, h.backend.Health(t.Context()))
				requireVolumeLaunchGauge(t, 1, "an unreadable store must not report a false zero")
			} else {
				requireVolumeLaunchGauge(t, 0, "completed launch settlement must clear the next sample")
			}
		})
	}
}

func requireVolumeLaunchGauge(t *testing.T, count int, explanation ...any) {
	t.Helper()
	const name = "fred_docker_backend_volume_launches_pending"
	want := fmt.Sprintf("# HELP %s Outstanding Docker launch receipts at the last successful backend health inspection\n# TYPE %s gauge\n%s %d\n", name, name, name, count)
	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(want), name), explanation...)
}
