package docker

import (
	"context"
	"fmt"
	"slices"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestFailedMaintenanceReceiptAllowsAnotherLeaseToReuseIdempotencyKey(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	failed, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target, backend.ReasonRestartFailed, "first lease failed", true)
	require.NoError(t, err)
	require.NoError(t, h.b.resolveMaintenanceFailure(failed, "first lease failed"))
	acknowledgePendingCallbacksForTest(t, h.callbacks)
	h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)

	siblingLease := uuid.NewString()
	sibling := h.source
	sibling.Version = 0
	operationID, callbackURL, lifecycleURL := newTestRestoreCallbackAuthority(t)
	sibling.OperationID = operationID
	sibling.RuntimeAuthority = mustTestReleaseRuntimeAuthority(t, operationID, "tenant-a",
		"22222222-2222-4222-8222-222222222222", callbackURL, lifecycleURL)
	seedProvisionReleaseForLeaseTest(t, h.callbacks, h.releases, h.operations, siblingLease, sibling)
	appendSibling := func(releaseContainers []ContainerInfo) {
		for _, c := range releaseContainers {
			c.LeaseUUID = siblingLease
			c.ContainerID = siblingLease + c.ContainerID
			c.Name = fmt.Sprintf("fred-%s-web-%d", siblingLease, c.InstanceIndex)
			h.inventory.containers = append(h.inventory.containers, c)
		}
	}
	appendSibling(h.containersFor(sibling, 2, "running", HealthStatusNone))
	require.NoError(t, h.b.recoverState(t.Context()))
	h.b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			target, _, found, err := h.b.maintenanceSettlement.FindMaintenanceRelease(siblingLease, h.intent.MaintenanceID())
			require.NoError(t, err)
			require.True(t, found)
			h.inventory.mu.Lock()
			defer h.inventory.mu.Unlock()
			h.inventory.containers = slices.DeleteFunc(h.inventory.containers, func(c ContainerInfo) bool { return c.LeaseUUID == siblingLease })
			appendSibling(h.containersFor(target, 2, "running", HealthStatusNone))
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			h.inventory.mu.Lock()
			defer h.inventory.mu.Unlock()
			var result []composeContainerSummary
			for _, c := range h.inventory.containers {
				if c.LeaseUUID == siblingLease {
					result = append(result, composeContainerSummary{ID: c.ContainerID, Service: fmt.Sprintf("web-%d", c.InstanceIndex), State: "running"})
				}
			}
			return result, nil
		},
	}
	t.Cleanup(func() { h.b.stopCancel(); h.b.wg.Wait() })
	require.NoError(t, h.b.Restart(t.Context(), backend.RestartRequest{
		LeaseUUID: siblingLease, MaintenanceID: h.intent.MaintenanceID(), CallbackURL: lifecycleURL,
	}))
	awaitProvisionWorkerQuiescence(t, h.b, siblingLease)
	info, err := h.b.GetProvision(t.Context(), siblingLease)
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusReady, info.Status)
	require.NoError(t, h.b.recoverState(t.Context()), "a lease-scoped receipt must not poison fleet recovery on key reuse")
	require.Empty(t, h.inventory.removed, "the other lease's healthy maintenance cohort is outside the receipt's authority")
	receipts, err := h.b.maintenanceSettlement.ListFailedMaintenanceReceipts()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.Equal(t, h.leaseUUID, receipts[0].LeaseUUID())
}
