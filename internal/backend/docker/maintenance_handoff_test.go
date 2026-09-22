package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestMaintenanceDisconnectAfterEnqueueRetainsExactAdmission(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		for _, interruption := range []string{"caller disconnect", "admission deadline", "backend shutdown"} {
			t.Run(string(kind)+"/"+interruption, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h := newMaintenanceRecoveryHarnessForKind(t, kind)
					h.appendTarget(true)
					require.ErrorContains(t, h.b.failUnacceptedMaintenance(h.intent, h.target, errors.New("fixture refusal")), "fixture refusal")
					acknowledgePendingCallbacksForTest(t, h.callbacks)
					h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
					require.NoError(t, h.b.recoverState(t.Context()))
					store := &pausedProvisionEntryStore{LeaseProvisionStore: h.b.provisionStore,
						entered: make(chan struct{}), resume: make(chan struct{})}
					h.b.provisionStore = store
					var release sync.Once
					t.Cleanup(func() { release.Do(func() { close(store.resume) }); h.b.stopCancel(); h.b.wg.Wait() })
					maintenanceID := newTestMaintenanceID(t)
					invoke := func(ctx context.Context, id shared.MaintenanceID) error {
						if kind == shared.MaintenanceIntentUpdate {
							return h.b.Update(ctx, backend.UpdateRequest{LeaseUUID: h.leaseUUID, MaintenanceID: id,
								CallbackURL: h.intent.LifecycleCallbackURL(), Payload: h.source.Manifest})
						}
						return h.b.Restart(ctx, backend.RestartRequest{LeaseUUID: h.leaseUUID, MaintenanceID: id, CallbackURL: h.intent.LifecycleCallbackURL()})
					}
					ctx, disconnect := context.WithCancel(t.Context())
					defer disconnect()
					returned := make(chan error, 1)
					go func() { returned <- invoke(ctx, maintenanceID) }()
					waitForOperationWorker(t, store.entered)
					if interruption == "caller disconnect" {
						disconnect()
					} else if interruption == "backend shutdown" {
						h.b.stopCancel()
					}
					select {
					case err := <-returned:
						require.ErrorContains(t, err, "maintenance acceptance is unknown")
					case <-time.After(actorAdmissionTimeout + time.Second):
						t.Fatal("maintenance acknowledgment wait exceeded its admission lifetime")
					}
					intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, maintenanceID, intent.MaintenanceID())
					require.Equal(t, shared.MaintenanceExecutionBeforeEffects, intent.ExecutionPhase())
					pending, err := h.callbacks.ListPending()
					require.NoError(t, err)
					require.Empty(t, pending, "a lost response cannot publish a definitive failed attempt")
					if interruption == "backend shutdown" {
						return
					}
					require.NoError(t, invoke(t.Context(), maintenanceID), "exact replay must not wait behind the paused actor")
					require.Error(t, invoke(t.Context(), newTestMaintenanceID(t)), "the durable head must fence a successor")
					_, target, found, err := h.b.maintenanceSettlement.FindMaintenanceRelease(h.leaseUUID, maintenanceID)
					require.NoError(t, err)
					require.True(t, found)
					target, err = h.b.maintenanceSettlement.BindMaintenanceIntentTarget(target)
					require.NoError(t, err)
					cleanup := registerMaintenanceExecutionForTest(t, h.b.maintenanceSettlement, target, maintenanceSeedTargetReady, nil, nil)
					defer cleanup()
					release.Do(func() { close(store.resume) })
					awaitProvisionWorkerQuiescence(t, h.b, h.leaseUUID)
					info, err := h.b.GetProvision(t.Context(), h.leaseUUID)
					require.NoError(t, err)
					require.Equal(t, backend.ProvisionStatusReady, info.Status, "the worker retains its operation lifetime after the admission waiter exits")
				})
			})
		}
	}
}

func TestMaintenanceHandoffDoesNotUseDisconnectedCallerForEnqueue(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			seedMaintenanceCloseProjection(t, h, kind)
			h.b.provisions[h.leaseUUID].Status = backend.ProvisionStatusReady
			cleanup := registerMaintenanceExecutionForTest(t, h.b.maintenanceSettlement, h.target, maintenanceSeedTargetReady, nil, nil)
			defer cleanup()
			t.Cleanup(func() { h.b.stopCancel(); h.b.wg.Wait() })
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			accepted, _ := h.b.handoffMaintenanceAdmission(ctx, h.target)
			require.NotEqual(t, asyncAcceptanceRejected, accepted, "disconnect after durable admission is not a no-dispatch verdict")
			awaitProvisionWorkerQuiescence(t, h.b, h.leaseUUID)
			info, err := h.b.GetProvision(t.Context(), h.leaseUUID)
			require.NoError(t, err)
			require.Equal(t, backend.ProvisionStatusReady, info.Status)
		})
	}
}

func TestDurableActorEnqueueHasIndependentBoundedBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		h.b.cfg.ProvisionTimeout = time.Hour
		start := time.Now()
		_, err := h.b.recoveryCoordinator.WithLease(t.Context(), h.leaseUUID, func(shared.LeaseRecoveryScope) error {
			// This real exclusive scope prevents actor enqueue without inventing
			// an actor status or canceling the durable worker's parent lifetime.
			acceptance, err := h.b.handoffMaintenanceAdmission(t.Context(), h.target)
			require.Equal(t, asyncAcceptanceRejected, acceptance)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, actorAdmissionTimeout, time.Since(start))
		intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, shared.MaintenanceExecutionBeforeEffects, intent.ExecutionPhase())
	})
}
