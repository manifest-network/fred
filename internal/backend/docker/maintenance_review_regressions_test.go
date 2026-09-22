package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestMaintenanceTargetContainersRequiresLeaseAndMaintenanceIdentity(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	matchingLater := ContainerInfo{ContainerID: "target-z", LeaseUUID: h.leaseUUID, MaintenanceID: h.intent.MaintenanceID()}
	matchingEarlier := ContainerInfo{ContainerID: "target-a", LeaseUUID: h.leaseUUID, MaintenanceID: h.intent.MaintenanceID()}
	prior := ContainerInfo{ContainerID: "prior", LeaseUUID: h.leaseUUID}
	foreign := ContainerInfo{ContainerID: "foreign", LeaseUUID: "another-lease", MaintenanceID: h.intent.MaintenanceID()}
	unnamed := ContainerInfo{ContainerID: "unattributed", MaintenanceID: h.intent.MaintenanceID()}
	listed := []ContainerInfo{foreign, matchingLater, prior, unnamed, matchingEarlier}

	target, lease := maintenanceTargetContainers(h.intent, listed)

	require.Equal(t, []ContainerInfo{matchingEarlier, matchingLater}, target,
		"the maintenance ID is scoped to the exact lease and target output is sorted")
	require.Equal(t, []ContainerInfo{matchingLater, prior, matchingEarlier}, lease,
		"the lease cohort must include its prior runtime but never a foreign lease reusing the ID")
	require.Equal(t, []ContainerInfo{foreign, matchingLater, prior, unnamed, matchingEarlier}, listed,
		"selection must not reorder the caller's full inventory")
}

func TestInspectMaintenanceFailureReadinessRequiresExactHealthyRuntime(t *testing.T) {
	for _, reason := range []backend.Reason{backend.ReasonRestartFailed, backend.ReasonUpdateFailed} {
		for _, scenario := range []struct {
			name          string
			ready         bool
			inspections   int
			noHealth      bool
			listedStatus  string
			inspectState  string
			inspectHealth HealthStatus
		}{
			{name: "healthy", ready: true, inspections: 1},
			{name: "inspect error", inspections: 1},
			{name: "nil inspection", inspections: 1},
			{name: "wrong container ID", inspections: 1},
			{name: "listed container stopped", listedStatus: "exited"},
			{name: "inspected container stopped", inspections: 1, inspectState: "exited"},
			{name: "health starting", inspections: 1, inspectHealth: HealthStatusStarting},
			{name: "health unhealthy", inspections: 1, inspectHealth: HealthStatusUnhealthy},
			{name: "health missing", inspections: 1, inspectHealth: HealthStatusNone},
			{name: "service missing"},
			{name: "manifest missing"},
			{name: "release generation changed"},
			{name: "operation generation changed"},
			{name: "not failed"},
			{name: "unrelated failure reason"},
			{name: "service without healthcheck", ready: true, inspections: 1, noHealth: true, inspectHealth: HealthStatusNone},
			{name: "service without healthcheck unhealthy", inspections: 1, noHealth: true, inspectHealth: HealthStatusUnhealthy},
		} {
			t.Run(string(reason)+"/"+scenario.name, func(t *testing.T) {
				const leaseUUID = "00000000-0000-4000-8000-000000000123"
				operationID, _, _ := newTestRestoreCallbackAuthority(t)
				service := &manifest.Manifest{Image: "nginx:1.27", HealthCheck: &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}}
				if scenario.noHealth {
					service.HealthCheck = nil
				}
				current := &recoveredProvision{ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, ActiveReleaseVersion: 2, ActiveOperationID: operationID,
					StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": service}},
				}}
				previous := recoveredProvision{ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed, Reason: reason,
					ActiveReleaseVersion: 2, ActiveOperationID: operationID,
				}}
				listed := ContainerInfo{ContainerID: "exact-runtime", LeaseUUID: leaseUUID, ServiceName: "web", Status: "running"}
				if scenario.listedStatus != "" {
					listed.Status = scenario.listedStatus
				}
				inspected := listed
				inspected.Health = HealthStatusHealthy
				if scenario.inspectState != "" {
					inspected.Status = scenario.inspectState
				}
				if scenario.inspectHealth != HealthStatusNone || scenario.noHealth || scenario.name == "health missing" {
					inspected.Health = scenario.inspectHealth
				}
				switch scenario.name {
				case "wrong container ID":
					inspected.ContainerID = "another-runtime"
				case "service missing":
					delete(current.StackManifest.Services, "web")
				case "manifest missing":
					current.StackManifest = nil
				case "release generation changed":
					current.ActiveReleaseVersion++
				case "operation generation changed":
					current.ActiveOperationID, _, _ = newTestRestoreCallbackAuthority(t)
				case "not failed":
					previous.Status = backend.ProvisionStatusReady
				case "unrelated failure reason":
					previous.Reason = backend.ReasonUnknown
				}
				calls := 0
				b := newBackendForTest(&mockDockerClient{InspectContainerFn: func(ctx context.Context, id string) (*ContainerInfo, error) {
					calls++
					require.Equal(t, listed.ContainerID, id)
					_, bounded := ctx.Deadline()
					require.True(t, bounded, "recovery inspection must retain its bounded context")
					if scenario.name == "inspect error" {
						return nil, errors.New("inspection unavailable")
					}
					if scenario.name == "nil inspection" {
						return nil, nil
					}
					return &inspected, nil
				}}, nil)
				t.Cleanup(b.stopCancel)
				ready := b.inspectMaintenanceFailureReadiness(t.Context(),
					map[string]provisionRecoveryBaseline{leaseUUID: {value: previous}},
					map[string]*recoveredProvision{leaseUUID: current}, []ContainerInfo{listed})

				require.Equal(t, scenario.ready, ready[leaseUUID])
				require.Equal(t, scenario.inspections, calls)
			})
		}
	}
}
