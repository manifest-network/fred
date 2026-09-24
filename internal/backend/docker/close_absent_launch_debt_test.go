package docker

import (
	"context"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestDeprovisionEmptyInventoryPreservesUnknownLaunch(t *testing.T) {
	for _, mode := range []string{"first close", "retry", "journal reopen"} {
		t.Run(mode, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			configureDiagnosticStartupFailure(t, h)
			seedMaintenanceCloseProjection(t, h, shared.MaintenanceIntentRestart)
			h.b.provisions[h.leaseUUID].Status = backend.ProvisionStatusReady
			h.b.cfg.RetainOnClose = true
			bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
			started := make(chan struct{})
			h.b.compose.(*mockComposeExecutor).UpFn = func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
				// The daemon accepted dispatch, but no container is visible yet. Its
				// delayed response cannot be disproved by empty strict inventory.
				h.inventory.mu.Lock()
				h.inventory.containers = nil
				h.inventory.mu.Unlock()
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			command, reply, err := leasesm.NewRestartCommand(ctx, h.target)
			require.NoError(t, err)
			require.NoError(t, h.b.routeToLeaseBlocking(ctx, h.leaseUUID, command))
			require.NoError(t, <-reply.Result())
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("maintenance launch did not start")
			}

			closeErr := h.b.Deprovision(ctx, h.leaseUUID)
			require.True(t, shared.IsLifecyclePending(closeErr), "actual Deprovision must project durable launch debt into the breaker-neutral pending class: %v", closeErr)
			switch mode {
			case "first close":
				require.ErrorIs(t, closeErr, shared.ErrVolumeLaunchUnsettled)
			case "retry":
				require.Error(t, closeErr, "initial uncertainty must retain a retry owner")
				retryErr := h.b.Deprovision(ctx, h.leaseUUID)
				require.ErrorIs(t, retryErr, shared.ErrVolumeLaunchUnsettled,
					"a later independent observation cannot erase durable dispatch uncertainty")
				require.True(t, shared.IsLifecyclePending(retryErr))
			case "journal reopen":
				require.Error(t, closeErr, "initial uncertainty must retain a retry owner")
				h.reopen()
				bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
				require.NoError(t, h.b.recoverState(ctx), "a pending close is a lease-local startup deferral")
				recoveredErr := h.b.Deprovision(ctx, h.leaseUUID)
				require.ErrorIs(t, recoveredErr, shared.ErrVolumeLaunchUnsettled)
				require.True(t, shared.IsLifecyclePending(recoveredErr))
			}
			assertPending := func() {
				t.Helper()
				_, found, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
				require.NoError(t, err)
				require.True(t, found)
				require.ErrorIs(t, h.b.volumeLaunches.checkNamespace(h.leaseUUID), shared.ErrVolumeLaunchUnsettled)
				require.Equal(t, 2, h.b.pool.Stats().AllocationCount)
				releases, err := h.releases.List(h.leaseUUID)
				require.NoError(t, err)
				require.Len(t, releases, 2)
				pending, err := h.callbacks.ListPending()
				require.NoError(t, err)
				require.Len(t, pending, 1, "empty inventory cannot authorize Deprovisioned for an unsettled launch")
				require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
			}
			assertPending()
		})
	}
}
