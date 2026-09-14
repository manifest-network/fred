package docker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestImageSetupAggregateBudgetPrecedesEveryHelper(t *testing.T) {
	helpers := 0
	client := &mockDockerClient{
		InspectImageFn: func(_ context.Context, image string) (*ImageInfo, error) {
			volumes := map[string]struct{}{"/data": {}}
			if image == "large:latest" {
				volumes = make(map[string]struct{})
				for index := range 16 {
					volumes[fmt.Sprintf("/data%d", index)] = struct{}{}
				}
			}
			return &ImageInfo{ID: fixtureImageID(image), User: "nonroot", Volumes: volumes}, nil
		},
		ResolveImageUserFn:    func(context.Context, string, string) (int, int, error) { helpers++; return 1000, 1000, nil },
		DetectWritablePathsFn: func(context.Context, string, int, []string) ([]string, error) { helpers++; return nil, nil },
	}
	b := newBackendForTest(client, nil)
	defer b.stopCancel()
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"first": {Image: "ordinary:latest"}, "second": {Image: "large:latest"},
	}}
	items := []backend.LeaseItem{{ServiceName: "first", SKU: "sku", Quantity: 1}, {ServiceName: "second", SKU: "sku", Quantity: 1023}}
	err := runSubjectStorageObservationForTest(t, b, durableCallbackTestLeaseUUID, func(mutations *storageMutations) error {
		_, err := b.inspectImagesForSetup(mutations, t.Context(), stack, items)
		return err
	})
	require.ErrorContains(t, err, "budget exceeded")
	require.Zero(t, helpers, "even the first service's helpers must wait for the aggregate decision")
}

func TestCompensationMountBudgetCoversFrozenRepresentations(t *testing.T) {
	plan := compensationSourcePlan{Containers: []compensationContainerRecord{{Host: &container.HostConfig{
		Binds: []string{"/source:/data"}, Mounts: []mount.Mount{{Type: mount.TypeTmpfs, Target: "/cache"}},
		Tmpfs: map[string]string{"/tmp": "size=64m"},
	}}}}
	require.NoError(t, admitCompensationMountBudget(plan))
	for _, representation := range []string{"bind", "mount", "tmpfs"} {
		t.Run(representation, func(t *testing.T) {
			host := &container.HostConfig{}
			switch representation {
			case "bind":
				host.Binds = make([]string, maxLaunchMounts+1)
			case "mount":
				host.Mounts = make([]mount.Mount, maxLaunchMounts+1)
			case "tmpfs":
				host.Tmpfs = make(map[string]string, maxLaunchMounts+1)
				for index := range maxLaunchMounts + 1 {
					host.Tmpfs[fmt.Sprintf("/cache%d", index)] = "size=64m"
				}
			}
			require.ErrorContains(t, admitCompensationMountBudget(compensationSourcePlan{Containers: []compensationContainerRecord{{Host: host}}}), "count budget exceeded")
		})
	}
}

func TestMaintenanceSourceBudgetRefusesBeforeTargetEffects(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	mock := h.b.docker.(*mockDockerClient)
	sourceID := fixtureImageID("source resource budget")
	sources := h.containersFor(h.source, 2, "running", "")
	for index := range sources {
		sources[index].execution = frozenCompensationFixture(sources[index], sourceID)
		sources[index].execution.Host.Tmpfs = make(map[string]string, maxLaunchMounts/2+1)
		for target := range maxLaunchMounts/2 + 1 {
			sources[index].execution.Host.Tmpfs[fmt.Sprintf("/cache%d", target)] = "size=64m"
		}
	}
	h.inventory.containers = sources
	mock.InspectImageFn = func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: sourceID}, nil }
	mutations := 0
	mock.RemoveContainerFn = func(context.Context, string) error { mutations++; return nil }
	mock.PullImageFn = func(context.Context, string, time.Duration) error { mutations++; return nil }
	ops, err := storageMutationOperationsForTest(h.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))
	execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	outcome := h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(shared.MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.True(t, failed.SourceRecovered())
	require.ErrorContains(t, failed.Cause(), "count budget exceeded")
	require.Zero(t, mutations)
	require.Len(t, h.inventory.containers, 2)
}

func TestImageSetupRejectsImageTmpfsOverlapBeforeHelpers(t *testing.T) {
	helpers := 0
	b := newBackendForTest(&mockDockerClient{
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: fixtureImageID("overlap"), User: "nonroot", Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		ResolveImageUserFn: func(context.Context, string, string) (int, int, error) { helpers++; return 1000, 1000, nil },
	}, nil)
	defer b.stopCancel()
	err := runSubjectStorageObservationForTest(t, b, durableCallbackTestLeaseUUID, func(mutations *storageMutations) error {
		_, err := b.inspectImagesForSetup(mutations, t.Context(), &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"app": {Image: "fixture:latest", Tmpfs: []string{"/data/cache"}},
		}}, []backend.LeaseItem{{ServiceName: "app", SKU: "sku", Quantity: 1}})
		return err
	})
	require.ErrorContains(t, err, "overlapping")
	require.Zero(t, helpers)
}

func TestLaunchMountBudgetCountsExpandedQuantityAndBytes(t *testing.T) {
	targets := make([]string, 16)
	for index := range targets {
		targets[index] = fmt.Sprintf("/data%d", index)
	}
	var counts launchMountBudget
	require.NoError(t, counts.reserve(targets, 0, 0, 1024))
	require.Error(t, counts.reserve([]string{"/extra"}, 0, 0, 1))
	var bytes launchMountBudget
	require.NoError(t, bytes.reserve([]string{"/data"}, 0, (32<<10)-len("/data"), 1024))
	require.Error(t, bytes.reserve([]string{"/extra"}, 0, 0, 1))
	var oversized launchMountBudget
	require.Error(t, oversized.reserve([]string{"/" + strings.Repeat("a", 4096)}, 0, 0, 1))
	require.Error(t, oversized.reserve(nil, 0, 0, int(^uint(0)>>1)))
}
