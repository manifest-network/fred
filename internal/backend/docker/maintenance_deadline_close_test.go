package docker

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// Exercise the whole accepting actor -> timed-out target -> source executor ->
// close handoff. A settled daemon rejection is held after cancellation to prove
// that revoking source work still cannot bypass its completion owner/barrier.
func TestMaintenanceDeadlineThenCloseCancelsCompensationAndDrains(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		for _, duringSource := range []bool{false, true} {
			phase := "before-source"
			if duringSource {
				phase = "during-source"
			}
			t.Run(string(kind)+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h := newMaintenanceRecoveryHarnessForKind(t, kind)
					h.appendTarget(true)
					seedMaintenanceCloseProjection(t, h, kind)
					h.b.provisions[h.leaseUUID].Status = backend.ProvisionStatusReady
					bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
					mock := h.b.docker.(*mockDockerClient)
					imageID := fixtureImageID("source before target deadline")
					sources := h.containersFor(h.source, 2, "running", "")
					for i := range sources {
						sources[i].execution = frozenCompensationFixture(sources[i], imageID)
					}
					h.inventory.containers = slices.Clone(sources)
					mock.InspectImageFn = func(context.Context, string) (*ImageInfo, error) {
						return &ImageInfo{ID: imageID}, nil
					}
					mock.PullImageFn = func(context.Context, string, time.Duration) error { return nil }
					mock.ContainerLogsFn = func(context.Context, string, int) (string, error) { return "target timed out", nil }
					targetExpired, returnTarget := make(chan struct{}), make(chan struct{})
					sourceStarted, sourceCanceled, completeSource := make(chan struct{}), make(chan struct{}), make(chan struct{})
					h.b.compose = &mockComposeExecutor{
						LaunchFn: func(context.Context, *composetypes.Project, composeUpOpts) daemonLaunchOutcome {
							h.inventory.mu.Lock()
							h.inventory.containers = h.containersFor(h.targetRelease, 2, "exited", "")
							h.inventory.mu.Unlock()
							return daemonLaunchOutcome{settled: true}
						},
						PSFn: func(ctx context.Context, _ string) ([]composeContainerSummary, error) {
							<-ctx.Done()
							close(targetExpired)
							<-returnTarget
							return nil, ctx.Err()
						},
					}
					mock.CreateCompensationOutcomeFn = func(ctx context.Context, _ imageexec.Image, _ compensationContainer) (string, daemonLaunchOutcome) {
						close(sourceStarted)
						if !duringSource {
							return "", daemonLaunchOutcome{settled: true, err: errors.New("unexpected source dispatch after close")}
						}
						<-ctx.Done()
						close(sourceCanceled)
						<-completeSource
						return "", daemonLaunchOutcome{settled: true, err: errors.New("daemon completed source rejection")}
					}
					ops, err := storageMutationOperationsForTest(h.b)
					require.NoError(t, err)
					require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
					h.b.cfg.ProvisionTimeout = time.Second
					handoff, cancel := h.b.maintenanceWorkerHandoff()
					defer cancel()
					command, reply, err := leasesm.NewRestartCommand(handoff, h.target)
					if kind == shared.MaintenanceIntentUpdate {
						command, reply, err = leasesm.NewUpdateCommand(handoff, h.target)
					}
					require.NoError(t, err)
					require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, command))
					require.NoError(t, <-reply.Result())
					<-targetExpired
					require.ErrorIs(t, handoff.TargetContext().Err(), context.DeadlineExceeded)
					if duringSource {
						close(returnTarget)
						<-sourceStarted
					}
					closeAt := time.Now()
					require.True(t, leasesm.IsLifecyclePending(h.b.Deprovision(t.Context(), h.leaseUUID)), "close must first retain the exact draining actor")
					_, found, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
					require.NoError(t, err)
					require.False(t, found, "durable teardown ownership cannot precede worker drain")
					if duringSource {
						<-sourceCanceled
						require.Equal(t, closeAt, time.Now(), "close must cancel source immediately, without spending its two-minute deadline")
						require.ErrorIs(t, h.b.volumeLaunches.checkNamespace(h.leaseUUID), shared.ErrVolumeLaunchUnsettled)
						require.True(t, leasesm.IsLifecyclePending(h.b.Deprovision(t.Context(), h.leaseUUID)), "cancellation is not admitted-request completion")
						close(completeSource)
					} else {
						close(returnTarget)
					}
					require.NoError(t, deprovisionAfterWorkerDrain(t, t.Context(), h.b, h.leaseUUID))
					if !duringSource {
						select {
						case <-sourceStarted:
							t.Fatal("close after target deadline dispatched new source work")
						default:
						}
					}
					require.NoError(t, h.b.volumeLaunches.checkNamespace(h.leaseUUID))
					require.Empty(t, h.inventory.containers)
					require.Zero(t, h.b.pool.Stats().AllocationCount)
					pending, err := h.callbacks.ListPending()
					require.NoError(t, err)
					require.Len(t, pending, 2)
					require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
					require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
					require.Less(t, pending[0].Sequence, pending[1].Sequence)
				})
			})
		}
	}
}
