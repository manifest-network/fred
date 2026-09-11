package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func frozenCompensationFixture(info ContainerInfo, imageID string) *compensationContainerRecord {
	return &compensationContainerRecord{
		Name: info.Name, ImageID: imageID, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Config: &container.Config{Image: imageID, User: "1234:1234", Env: []string{"SOURCE_POLICY=frozen"}, Labels: map[string]string{
			LabelLeaseUUID: info.LeaseUUID, LabelTenant: info.Tenant, LabelProviderUUID: info.ProviderUUID,
			LabelBackendName: info.BackendName, LabelSKU: info.SKU, LabelServiceName: info.ServiceName,
			LabelInstanceIndex: strconv.Itoa(info.InstanceIndex), LabelCallbackURL: info.CallbackURL,
			LabelLifecycleCallbackURL: info.LifecycleCallbackURL, LabelMaintenanceID: info.MaintenanceID.String(),
			LabelImageReference: info.Image, LabelImageID: imageID, LabelCreatedAt: info.CreatedAt.Format(time.RFC3339),
		}},
		Host:     &container.HostConfig{ReadonlyRootfs: true, CapDrop: []string{"ALL"}, Resources: container.Resources{Memory: 123456789}},
		Networks: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{"source-network": {Aliases: []string{"source-alias"}}}},
	}
}

func TestMaintenanceCompensationDockerRestoresFrozenImageAndPolicyAfterTargetFailure(t *testing.T) {
	t.Run("current authority", func(t *testing.T) {
		testFrozenDockerCompensation(t, newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate))
	})
	t.Run("v0.13 moved callback base", func(t *testing.T) {
		testFrozenDockerCompensation(t, newLegacyMaintenanceRecoveryHarnessForKindAtCallback(t, shared.MaintenanceIntentUpdate, "https://new-provider.example/callbacks/provision"))
	})
}

func testFrozenDockerCompensation(t *testing.T, h *maintenanceRecoveryHarness) {
	h.appendTarget(true)
	mock := h.b.docker.(*mockDockerClient)
	sourceID, targetID := fixtureImageID("source immutable"), fixtureImageID("moved mutable tag")
	currentTag := sourceID
	sources := h.containersFor(h.source, 2, "running", "")
	for index := range sources {
		sources[index].execution = frozenCompensationFixture(sources[index], sourceID)
	}
	h.inventory.containers = slices.Clone(sources)
	mock.InspectImageFn = func(_ context.Context, reference string) (*ImageInfo, error) {
		id := currentTag
		if strings.HasPrefix(reference, "sha256:") {
			id = reference
		}
		return &ImageInfo{ID: id}, nil
	}
	pulls, creates, starts := 0, 0, 0
	mock.PullImageFn = func(context.Context, string, time.Duration) error {
		pulls++
		currentTag = targetID
		// A changed deployment default must not change source replay policy.
		h.b.cfg.ContainerReadonlyRootfs = ptrBool(false)
		return nil
	}
	var events []string
	mock.ContainerLogsFn = func(_ context.Context, id string, _ int) (string, error) {
		events = append(events, "logs:"+id)
		return "target startup error", nil
	}
	mock.RemoveContainerFn = func(ctx context.Context, id string) error {
		events = append(events, "remove:"+id)
		return h.inventory.remove(ctx, id)
	}
	failedTargets := h.containersFor(h.targetRelease, 2, "exited", "")
	composeCalls := 0
	h.b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			composeCalls++
			for _, service := range project.Services {
				require.Equal(t, targetID, service.Image)
			}
			h.inventory.containers = slices.Clone(failedTargets)
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: failedTargets[0].ContainerID, Service: "web-0", State: "exited"}, {ID: failedTargets[1].ContainerID, Service: "web-1", State: "exited"}}, nil
		},
	}
	mock.CreateCompensationContainerFn = func(_ context.Context, image imageexec.Image, snapshot compensationContainer) (string, error) {
		creates++
		require.Equal(t, sourceID, image.ID(), "source image must precede the target pull, even for the same mutable tag")
		require.True(t, snapshot.Host.ReadonlyRootfs)
		require.Equal(t, int64(123456789), snapshot.Host.Memory)
		require.Equal(t, []string{"ALL"}, []string(snapshot.Host.CapDrop))
		require.Equal(t, []string{"SOURCE_POLICY=frozen"}, snapshot.Config.Env)
		require.Equal(t, "1234:1234", snapshot.Config.User)
		require.Equal(t, []string{"source-alias"}, snapshot.Networks.EndpointsConfig["source-network"].Aliases)
		index, err := strconv.Atoi(snapshot.Config.Labels[LabelInstanceIndex])
		require.NoError(t, err)
		info := sources[index]
		info.ContainerID = fmt.Sprintf("restored-source-%d", index)
		info.Status = "created"
		info.CreatedAt = time.Now()
		info.execution = frozenCompensationFixture(info, image.ID())
		h.inventory.containers = append(h.inventory.containers, info)
		return info.ContainerID, nil
	}
	mock.StartContainerFn = func(_ context.Context, id string, _ time.Duration) error {
		require.Equal(t, 2, creates, "the complete source cohort must exist before any source Start")
		starts++
		for index := range h.inventory.containers {
			if h.inventory.containers[index].ContainerID == id {
				h.inventory.containers[index].Status = "running"
				return nil
			}
		}
		return errors.New("start of unknown source")
	}
	ops, err := storageMutationOperationsForTest(h.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
	execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	outcome := h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(shared.MaintenanceExecutionFailure)
	require.True(t, ok, "outcome = %T (%+v)", outcome, outcome)
	require.True(t, failed.SourceRecovered(), "source restoration must yield the original active release")
	require.Equal(t, 1, pulls)
	require.Equal(t, 1, composeCalls, "source must use immutable direct creation, never rerun the mutable target Compose plan")
	require.Equal(t, 2, starts)
	for _, target := range failedTargets {
		logged, removed := slices.Index(events, "logs:"+target.ContainerID), slices.Index(events, "remove:"+target.ContainerID)
		require.GreaterOrEqual(t, logged, 0)
		require.Greater(t, removed, logged, "failure diagnostics must precede destructive cleanup")
	}
	proof, err := h.b.maintenanceSettlement.FailMaintenance(failed, backend.ReasonInternal, "target failed; exact source restored")
	require.NoError(t, err)
	require.True(t, proof.Valid())
	// Converge through the production dropped-worker projection path, then
	// publish the attempt-owned failure while the exact source remains Ready.
	h.b.provisions[h.leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: h.leaseUUID}}
	require.NoError(t, h.b.convergeMaintenanceFailure(t.Context(), proof.Intent(), h.source, h.inventory.containers, true, recoveredMaintenanceFailureInfo(proof.Intent(), &h.targetRelease)))
	require.NoError(t, h.b.resolveMaintenanceFailure(proof, "target startup failed"))
	mock.ContainerLogsFn = func(_ context.Context, id string, _ int) (string, error) {
		if strings.HasPrefix(id, "restored-source-") {
			return "source live log", nil
		}
		return "", errors.New("failed target removed")
	}
	logs, err := h.b.GetLogs(t.Context(), h.leaseUUID, 100)
	require.NoError(t, err)
	require.Equal(t, "source live log", logs["web/0"])
	require.Equal(t, "target startup error", logs["failed/web/0"], "restored source keeps the failed attempt's published diagnostics")
	require.Equal(t, "target startup error", logs["failed/web/1"])
	sourceAuthority, ok := h.source.RuntimeIdentity()
	require.True(t, ok)
	require.Equal(t, sourceAuthority.LifecycleCallbackURL(), h.b.provisions[h.leaseUUID].LifecycleCallbackURL, "restoration uses actual source route, including moved target callback bases")
	mock.ContainerLogsFn = func(_ context.Context, id string, _ int) (string, error) {
		if id == "restored-source-0" {
			return strings.Repeat("x", maxTotalLogBytes-5), nil
		}
		return "", nil
	}
	bounded, err := h.b.GetLogs(t.Context(), h.leaseUUID, 100)
	require.NoError(t, err)
	require.Len(t, bounded["failed/web/0"], 5, "failed-attempt logs share the remaining aggregate budget")
	total := 0
	for _, value := range bounded {
		total += len(value)
	}
	require.LessOrEqual(t, total, maxTotalLogBytes)
}

func TestCompensationStartupOrdersDependenciesAndWaitsForEveryHealthyReplica(t *testing.T) {
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"app": {Image: "nginx", DependsOn: map[string]manifest.DependsOnCondition{"db": {Condition: "service_healthy"}}},
		"db":  {Image: "postgres", HealthCheck: &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}},
	}}
	encoded, err := json.Marshal(stack)
	require.NoError(t, err)
	snapshot := func(name, service string) compensationContainer {
		return compensationContainer{Name: name, Config: &container.Config{Labels: map[string]string{LabelServiceName: service}}}
	}
	schedule, err := orderCompensationContainers(compensationLaunchPlan{Source: shared.Release{Manifest: encoded}, Containers: []compensationContainer{
		snapshot("app-0", "app"), snapshot("db-1", "db"), snapshot("db-0", "db"),
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"db-0", "db-1", "app-0"}, []string{schedule.containers[0].Name, schedule.containers[1].Name, schedule.containers[2].Name})
	synctest.Test(t, func(t *testing.T) {
		observations := make(map[string]int)
		mock := &mockDockerClient{InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			observations[id]++
			health := HealthStatusHealthy
			if id == "db-second" && observations[id] == 1 {
				health = HealthStatusStarting
			}
			return &ContainerInfo{ContainerID: id, Status: "running", Health: health}, nil
		}}
		b := newBackendForTest(mock, nil)
		defer b.stopCancel()
		require.NoError(t, b.waitForCompensationDependencies(t.Context(), schedule, "app", map[string][]string{"db": {"db-first", "db-second"}}, "lease"))
		require.Equal(t, 1, observations["db-first"])
		require.Equal(t, 2, observations["db-second"], "all dependency replicas must be healthy before the dependent starts")
	})
}

func TestMaintenanceCompensationPrelaunchImageFailureKeepsExactHealthySource(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	mock := h.b.docker.(*mockDockerClient)
	sourceID := fixtureImageID("intact source")
	sources := h.containersFor(h.source, 2, "running", "")
	for index := range sources {
		sources[index].execution = frozenCompensationFixture(sources[index], sourceID)
	}
	h.inventory.containers = sources
	mock.InspectImageFn = func(_ context.Context, ref string) (*ImageInfo, error) { return &ImageInfo{ID: sourceID}, nil }
	mock.PullImageFn = func(context.Context, string, time.Duration) error { return errors.New("registry unavailable") }
	creates := 0
	mock.CreateCompensationContainerFn = func(context.Context, imageexec.Image, compensationContainer) (string, error) {
		creates++
		return "", errors.New("intact source must not be replaced")
	}
	ops, err := storageMutationOperationsForTest(h.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
	execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	outcome := h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(shared.MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.True(t, failed.SourceRecovered(), "a failed replacement must retain a positively healthy original source")
	require.Zero(t, creates)
	require.Empty(t, h.inventory.removed)
	var physical *physicalOperationError
	require.ErrorAs(t, failed.Cause(), &physical)
	require.Equal(t, backend.ReasonImagePullFailed, physical.reason)
	proof, err := h.b.maintenanceSettlement.FailMaintenance(failed, physical.reason, physical.callback)
	require.NoError(t, err)
	require.True(t, proof.Valid())
}

func TestMaintenanceCompensationRepairsExactPartialSource(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		for _, targetStatus := range []string{"running", "exited"} {
			t.Run(string(kind)+"/"+targetStatus, func(t *testing.T) {
				h := newMaintenanceRecoveryHarnessForKind(t, kind)
				h.appendTarget(true)
				h.inventory.containers = h.containersFor(h.source, 1, "running", HealthStatusNone)
				mock := h.b.docker.(*mockDockerClient)
				pulls, launches := 0, 0
				mock.PullImageFn = func(context.Context, string, time.Duration) error { pulls++; return nil }
				mock.ContainerLogsFn = func(context.Context, string, int) (string, error) { return "repair target output", nil }
				mock.ReadmitCompensationImageFn = func(context.Context, compensationContainerRecord) (imageexec.Image, error) {
					t.Fatal("incomplete source cannot authorize source replay")
					return imageexec.Image{}, errors.New("unexpected source replay")
				}
				targets := h.containersFor(h.targetRelease, 2, targetStatus, HealthStatusNone)
				h.b.compose = &mockComposeExecutor{
					UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
						launches++
						h.inventory.containers = slices.Clone(targets)
						return nil
					},
					PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
						return []composeContainerSummary{{ID: targets[0].ContainerID, Service: "web-0", State: targetStatus}, {ID: targets[1].ContainerID, Service: "web-1", State: targetStatus}}, nil
					},
				}
				ops, err := storageMutationOperationsForTest(h.b)
				require.NoError(t, err)
				require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
				execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
				require.NoError(t, err)
				outcome := h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
				require.Equal(t, 1, launches, "exact missing replicas must not prevent the target repair attempt")
				if kind == shared.MaintenanceIntentUpdate {
					require.Equal(t, 1, pulls)
				}
				current, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
				require.NoError(t, err)
				require.True(t, found)
				pending, err := h.b.maintenanceSettlement.CompensationPending(current)
				require.NoError(t, err)
				require.False(t, pending, "an exact subset must never become a partial replay plan")
				if targetStatus == "running" {
					success, ok := outcome.(shared.MaintenanceExecutionSuccess)
					require.True(t, ok, "%T: %v", outcome, outcome)
					active, err := h.b.maintenanceSettlement.ActivateMaintenance(success)
					require.NoError(t, err)
					ready, ok := active.TargetReady()
					require.True(t, ok)
					_, ids, services := ready.Projection()
					require.Len(t, ids, 2)
					require.Len(t, services["web"], 2)
				} else {
					require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, outcome,
						"failed repair needs exact target cleanup; it cannot claim a recovered partial source")
				}
			})
		}
	}
}

func TestMaintenanceSourceCaptureRefusesUncertainSubset(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*maintenanceRecoveryHarness)
	}{
		{"foreign tenant", func(h *maintenanceRecoveryHarness) { h.inventory.containers[0].Tenant = "other-tenant" }},
		{"foreign generation", func(h *maintenanceRecoveryHarness) {
			h.inventory.containers[0].MaintenanceID = newTestMaintenanceID(h.t)
		}},
		{"unknown instance", func(h *maintenanceRecoveryHarness) { h.inventory.containers[0].InstanceIndex = 2 }},
		{"duplicate instance", func(h *maintenanceRecoveryHarness) {
			h.inventory.containers = append(h.inventory.containers, h.inventory.containers[0])
		}},
		{"unreadable survivor", func(h *maintenanceRecoveryHarness) { h.inventory.inspectErr = errors.New("daemon read unavailable") }},
		{"changed inspected identity", func(h *maintenanceRecoveryHarness) {
			h.b.docker.(*mockDockerClient).InspectContainerFn = func(context.Context, string) (*ContainerInfo, error) {
				changed := h.inventory.containers[0]
				changed.Tenant = "changed-tenant"
				return &changed, nil
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
			h.appendTarget(true)
			h.inventory.containers = h.containersFor(h.source, 1, "running", HealthStatusNone)
			test.mutate(h)
			mock := h.b.docker.(*mockDockerClient)
			mock.PullImageFn = func(context.Context, string, time.Duration) error {
				t.Fatal("uncertain source crossed target image dispatch")
				return errors.New("unexpected target dispatch")
			}
			ops, err := storageMutationOperationsForTest(h.b)
			require.NoError(t, err)
			require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
			execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			outcome := h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
			failure, ok := outcome.(shared.MaintenanceExecutionFailure)
			require.True(t, ok, "%T: %v", outcome, outcome)
			require.False(t, failure.SourceRecovered())
			require.ErrorContains(t, failure.Cause(), "capture maintenance source")
			require.Empty(t, h.inventory.removed)
		})
	}
}
