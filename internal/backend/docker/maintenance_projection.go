package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
)

// inspectMaintenanceFailureReadiness observes only an already failed exact
// runtime. Docker list rows do not contain healthcheck state. A fresh bounded
// inspection can permit ordinary recovery to clear the failure; unavailable or
// incomplete evidence preserves it. The publication boundary separately checks
// the captured projection and durable generation, so this grants no authority
// over a successor admitted while inspection was in flight.
func (b *Backend) inspectMaintenanceFailureReadiness(
	ctx context.Context,
	baseline map[string]provisionRecoveryBaseline,
	building map[string]*recoveredProvision,
	containers []ContainerInfo,
) map[string]bool {
	ready := make(map[string]bool)
	for leaseUUID, snapshot := range baseline {
		previous := snapshot.value
		if previous.Status != backend.ProvisionStatusFailed ||
			(previous.Reason != backend.ReasonRestartFailed && previous.Reason != backend.ReasonUpdateFailed) {
			continue
		}
		current := building[leaseUUID]
		if current == nil || current.StackManifest == nil ||
			current.ActiveReleaseVersion != previous.ActiveReleaseVersion || current.ActiveOperationID != previous.ActiveOperationID {
			continue
		}
		ready[leaseUUID] = true
	}
	for _, listed := range containers {
		if !ready[listed.LeaseUUID] {
			continue
		}
		service := building[listed.LeaseUUID].StackManifest.Services[listed.ServiceName]
		if service == nil || listed.Status != "running" {
			ready[listed.LeaseUUID] = false
			continue
		}
		inspected, err := b.inspectContainerForRecovery(ctx, listed.ContainerID)
		if err != nil || inspected == nil {
			ready[listed.LeaseUUID] = false
			continue
		}
		// A running member a startup watch saw pass its check stays healthy
		// (the sticky health rule, gatedHealth).
		health := b.gatedHealth(listed.ContainerID, inspected.Health)
		ready[listed.LeaseUUID] = inspected.ContainerID == listed.ContainerID && inspected.Status == "running" &&
			health != HealthStatusUnhealthy &&
			(!service.HasActiveHealthCheck() || health == HealthStatusHealthy)
	}
	return ready
}
