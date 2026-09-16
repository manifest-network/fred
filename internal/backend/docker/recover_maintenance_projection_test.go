package docker

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestRecoverStateColdPendingMaintenanceStaysTransitional(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart,
		shared.MaintenanceIntentUpdate,
		shared.MaintenanceIntentCustomDomain,
	} {
		for _, inventory := range []string{"target", "source", "partial target", "absent"} {
			t.Run(string(kind)+"/"+inventory, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h := newMaintenanceRecoveryHarnessForKind(t, kind)
					h.appendTarget(true)
					_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
					require.NoError(t, err)
					h.b.cfg.ProvisionTimeout = time.Minute
					h.b.cfg.StartupVerifyDuration = 5 * time.Second
					switch inventory {
					case "target":
						h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
					case "source":
						h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
					case "partial target":
						h.inventory.containers = h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
					}
					for index := range h.inventory.containers {
						h.inventory.containers[index].CreatedAt = time.Now().Add(time.Hour).Round(0)
					}
					// No volatile projection or reservation survives a process restart.
					// Current configuration also cannot supply the frozen resource sizes.
					require.Empty(t, h.b.provisions)
					require.Zero(t, h.b.pool.Stats().AllocationCount)
					h.b.cfg.SKUProfiles = nil
					pendingStatus := backend.ProvisionStatusRestarting
					if kind == shared.MaintenanceIntentUpdate {
						pendingStatus = backend.ProvisionStatusUpdating
					}
					for range 2 {
						require.NoError(t, h.b.recoverState(t.Context()))
						info, infoErr := h.b.GetProvision(t.Context(), h.leaseUUID)
						require.NoError(t, infoErr)
						assert.Equal(t, pendingStatus, info.Status)
						assert.Equal(t, 2, info.Quantity)
						assert.Zero(t, info.FailCount, "transitional absence is not runtime failure evidence")
						assert.Empty(t, info.Reason)
						assert.Equal(t, h.source.ResourceProfiles, h.b.provisions[h.leaseUUID].ResourceProfiles)
						stats := h.b.pool.Stats()
						assert.Equal(t, 2, stats.AllocationCount, "reserve the complete durable cohort, including unseen members")
						assert.Equal(t, float64(1), stats.AllocatedCPU)
						assert.Equal(t, int64(1024), stats.AllocatedMemoryMB)
						assert.Equal(t, int64(2048), stats.AllocatedDiskMB)
						pending, pendingErr := h.b.maintenanceSettlement.ListMaintenanceIntents()
						require.NoError(t, pendingErr)
						require.Len(t, pending, 1)
						assert.Equal(t, h.intent.MaintenanceID(), pending[0].MaintenanceID())
						callbacks, callbackErr := h.callbacks.ListPending()
						require.NoError(t, callbackErr)
						assert.Empty(t, callbacks, "ordinary recovery cannot settle pending maintenance")
						assert.Empty(t, h.inventory.removed)
					}

					// Late exact target visibility and fresh readiness evidence permit
					// the maintenance owner to settle and publish Ready on the next pass.
					time.Sleep(5 * time.Second)
					h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
					require.NoError(t, h.b.recoverState(t.Context()))
					h.assertSettled(backend.CallbackStatusSuccess)
					info, infoErr := h.b.GetProvision(t.Context(), h.leaseUUID)
					require.NoError(t, infoErr)
					assert.Equal(t, backend.ProvisionStatusReady, info.Status)
					assert.Equal(t, 2, h.b.pool.Stats().AllocationCount)
				})
			})
		}
	}
}
