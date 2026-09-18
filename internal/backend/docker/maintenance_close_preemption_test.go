package docker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestDeprovisionPreemptsStartedMaintenance(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate,
	} {
		t.Run(string(kind), func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			h.inventory.containers = append(
				h.containersFor(h.source, 2, "running", HealthStatusNone),
				h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)...,
			)
			seedMaintenanceCloseProjection(t, h, kind)
			bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = h.b.Deprovision(ctx, h.leaseUUID)
			require.NoError(t, err, "close must hand drained Started maintenance to its durable finalizer")
			require.Empty(t, h.inventory.containers, "close must remove both source and partial target")
			require.Len(t, h.inventory.removed, 3)
			require.False(t, h.b.provisionStore.Exists(h.leaseUUID))
			require.Zero(t, h.b.pool.Stats().AllocationCount)
			_, found, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
			require.NoError(t, err)
			require.False(t, found)
			intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Empty(t, intents)
			releases, err := h.releases.List(h.leaseUUID)
			require.NoError(t, err)
			require.Empty(t, releases, "terminal close retires the exact source and its deploying maintenance tail")
			pending, err := h.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 2)
			require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
			require.Contains(t, pending[0].Error, "maintenance preempted by lease close")
			require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
			require.Less(t, pending[0].Sequence, pending[1].Sequence)
		})
	}
}

func TestDeprovisionCancelsInFlightMaintenanceBeforeCloseHandoff(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate,
	} {
		t.Run(string(kind), func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			configureDiagnosticStartupFailure(t, h)
			seedMaintenanceCloseProjection(t, h, kind)
			h.b.provisions[h.leaseUUID].Status = backend.ProvisionStatusReady
			bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
			workerStarted := make(chan struct{})
			workerExited := make(chan struct{})
			h.b.compose.(*mockComposeExecutor).UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
				h.inventory.mu.Lock()
				h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
				h.inventory.mu.Unlock()
				return nil
			}
			h.b.compose.(*mockComposeExecutor).PSFn = func(ctx context.Context, _ string) ([]composeContainerSummary, error) {
				defer close(workerExited)
				close(workerStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			h.b.docker.(*mockDockerClient).RemoveContainerFn = func(ctx context.Context, id string) error {
				select {
				case <-workerExited:
				default:
					t.Error("close cleanup started before the maintenance worker's canceled observation exited")
				}
				return h.inventory.remove(ctx, id)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			command, reply, err := leasesm.NewRestartCommand(ctx, h.target)
			if kind == shared.MaintenanceIntentUpdate {
				command, reply, err = leasesm.NewUpdateCommand(ctx, h.target)
			}
			require.NoError(t, err)
			require.NoError(t, h.b.routeToLeaseBlocking(ctx, h.leaseUUID, command))
			require.NoError(t, <-reply.Result())
			select {
			case <-workerStarted:
			case <-ctx.Done():
				t.Fatal("maintenance worker did not reach its Started execution")
			}
			intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, shared.MaintenanceExecutionStarted, intent.ExecutionPhase())
			require.True(t, h.b.actorOwnsMaintenance(h.leaseUUID, intent.MaintenanceID()))
			require.NoError(t, h.b.volumeLaunches.checkNamespace(h.leaseUUID), "the successful Compose response settled its exact launch debt")

			// Compose has successfully completed the physical replacement. The
			// worker is blocked observing readiness; close must cancel and drain
			// that worker before changing the durable owner and removing targets.
			require.NoError(t, h.b.Deprovision(ctx, h.leaseUUID))
			require.Empty(t, h.inventory.containers)
			require.Len(t, h.inventory.removed, 2)
			pending, err := h.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 2, "canceled worker cannot enqueue a late duplicate completion")
			require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
			require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
			require.Less(t, pending[0].Sequence, pending[1].Sequence)
		})
	}
}

func TestDeprovisionStartedMaintenanceKeepsUnknownCleanupPending(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	h.inventory.containers = h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
	seedMaintenanceCloseProjection(t, h, shared.MaintenanceIntentRestart)
	bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
	h.inventory.inspectErr = errors.New("Docker inspection unavailable")

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.Error(t, h.b.Deprovision(ctx, h.leaseUUID))
	claim, found, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
	require.NoError(t, err)
	require.True(t, found, "unknown physical effects must retain the exact durable close owner")
	require.Equal(t, h.intent.MaintenanceID(), claim.InterruptedMaintenanceID())
	require.Equal(t, h.source.Version, claim.ActiveReleaseVersion())
	require.Empty(t, h.inventory.removed)
	require.Len(t, h.inventory.containers, 1)
	require.Equal(t, 2, h.b.pool.Stats().AllocationCount)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "uncertain cleanup cannot publish Deprovisioned")
	require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	releases, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2, "release history remains fenced until actual cleanup")

	// The exact close head now exclusively owns this Started generation. Its
	// failure callback is history, not another physical cleanup capability.
	receipts, err := h.b.maintenanceSettlement.ListFailedMaintenanceReceipts()
	require.NoError(t, err)
	require.Empty(t, receipts)
	h.inventory.mu.Lock()
	h.inventory.inspectErr = nil
	h.inventory.mu.Unlock()
	require.NoError(t, h.b.recoverState(ctx), "normal recovery must reach the close finalizer before any receipt cleanup")
	require.Empty(t, h.inventory.containers)
	require.Zero(t, h.b.pool.Stats().AllocationCount)
	releases, err = h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)
	pending, err = h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
}

func TestDeprovisionDoesNotBypassUnknownMaintenanceLaunch(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	configureDiagnosticStartupFailure(t, h)
	seedMaintenanceCloseProjection(t, h, shared.MaintenanceIntentRestart)
	h.b.provisions[h.leaseUUID].Status = backend.ProvisionStatusReady
	bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
	started := make(chan struct{})
	h.b.compose.(*mockComposeExecutor).UpFn = func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
		h.inventory.mu.Lock()
		h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
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

	// Cancellation drains the local worker, but does not prove that the Docker
	// daemon finished its request. A close handoff cannot erase that uncertainty.
	require.ErrorIs(t, h.b.Deprovision(ctx, h.leaseUUID), shared.ErrVolumeLaunchUnsettled)
	claim, found, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, h.intent.MaintenanceID(), claim.InterruptedMaintenanceID())
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Empty(t, intents, "the close journal is now the exact cleanup owner")
	require.ErrorIs(t, h.b.volumeLaunches.checkNamespace(h.leaseUUID), shared.ErrVolumeLaunchUnsettled)
	debts, err := h.b.volumeLaunches.pendingCount()
	require.NoError(t, err)
	require.Equal(t, 1, debts, "the original launch remains observable and operator-repairable under the close head")
	require.Equal(t, 2, h.b.pool.Stats().AllocationCount)
	releases, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "unknown Docker completion cannot authorize Deprovisioned")
	require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	_, err = h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.Error(t, err, "the old maintenance capability cannot regain mutation ownership")
}

func TestDeprovisionPreservesCommittedMaintenanceSuccess(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
	require.NoError(t, err)
	seedMaintenanceCloseProjection(t, h, shared.MaintenanceIntentUpdate)
	bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, h.b.Deprovision(ctx, h.leaseUUID))
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Equal(t, backend.CallbackStatusSuccess, pending[0].Status,
		"a committed target cannot be reclassified as close-preempted failure")
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
	require.Less(t, pending[0].Sequence, pending[1].Sequence)
	releases, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)
}

func seedMaintenanceCloseProjection(t *testing.T, h *maintenanceRecoveryHarness, kind shared.MaintenanceIntentKind) {
	t.Helper()
	status := backend.ProvisionStatusRestarting
	if kind == shared.MaintenanceIntentUpdate {
		status = backend.ProvisionStatusUpdating
	}
	identity := mustDockerReleaseRuntimeIdentity(t, h.source)
	h.b.provisions[h.leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: h.leaseUUID, Tenant: identity.Tenant(), ProviderUUID: identity.ProviderUUID(),
		Status: status, CallbackURL: identity.CallbackURL(), LifecycleCallbackURL: identity.LifecycleCallbackURL(),
		ActiveOperationID: h.source.OperationID, ActiveReleaseVersion: h.source.Version,
		Items: slices.Clone(h.source.Items), ResourceProfiles: shared.CloneSKUResourceSnapshot(h.source.ResourceProfiles),
		StackManifest: h.targetReleaseStack(),
	}}
	for _, item := range h.source.Items {
		for index := range item.Quantity {
			require.NoError(t, h.b.pool.TryAllocate(fmt.Sprintf("%s-%s-%d", h.leaseUUID, item.ServiceName, index), item.SKU, identity.Tenant()))
		}
	}
	t.Cleanup(func() {
		h.b.stopCancel()
		h.b.wg.Wait()
	})
}
