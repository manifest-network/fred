package docker

import (
	"context"
	"errors"
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
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
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
	outcome := h.b.maintenanceSettlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution)
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

func TestImageSetupRequiresCompleteLeaseBeforeHelpers(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		stack *manifest.StackManifest
		items []backend.LeaseItem
	}{
		{name: "zero quantity", stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": {Image: "app:latest"}}}, items: []backend.LeaseItem{{ServiceName: "app", Quantity: 0}}},
		{name: "missing stack", items: []backend.LeaseItem{{ServiceName: "app", Quantity: 1}}},
		{name: "unrequested service", stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": {Image: "app:latest"}, "extra": {Image: "extra:latest"}}}, items: []backend.LeaseItem{{ServiceName: "app", Quantity: 1}}},
		{name: "absent requested service", stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"other": {Image: "other:latest"}}}, items: []backend.LeaseItem{{ServiceName: "app", Quantity: 1}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			b := newBackendForTest(&mockDockerClient{
				InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
					t.Fatal("incomplete lease reached image admission")
					return nil, nil
				},
				ResolveImageUserFn: func(context.Context, string, string) (int, int, error) {
					t.Fatal("incomplete lease reached an image helper")
					return 0, 0, nil
				},
			}, nil)
			defer b.stopCancel()
			err := runSubjectStorageObservationForTest(t, b, durableCallbackTestLeaseUUID, func(mutations *storageMutations) error {
				setups, err := b.inspectImagesForSetup(mutations, t.Context(), scenario.stack, scenario.items)
				require.Nil(t, setups, "a partial service set cannot authorize launch preparation")
				return err
			})
			require.Error(t, err)
		})
	}
}

func TestImageSetupAdmitsAllServicesBeforeResolvingUsers(t *testing.T) {
	for _, failHelper := range []bool{false, true} {
		t.Run(fmt.Sprintf("helper failure=%v", failHelper), func(t *testing.T) {
			failure := errors.New("image user lookup unavailable")
			inspected := make(map[string]bool)
			helpers := 0
			b := newBackendForTest(&mockDockerClient{
				InspectImageFn: func(_ context.Context, image string) (*ImageInfo, error) {
					inspected[image] = true
					return &ImageInfo{ID: fixtureImageID(image), User: "nonroot", Volumes: map[string]struct{}{"/data": {}}}, nil
				},
				ResolveImageUserFn: func(_ context.Context, image, user string) (int, int, error) {
					require.Len(t, inspected, 2, "the whole lease must be admitted before the first helper")
					require.Contains(t, []string{fixtureImageID("first:latest"), fixtureImageID("second:latest")}, image)
					require.Equal(t, "application", user)
					helpers++
					if failHelper {
						return 0, 0, failure
					}
					return 1000, 1001, nil
				},
				DetectWritablePathsFn: func(context.Context, string, int, []string) ([]string, error) { return nil, nil },
			}, nil)
			defer b.stopCancel()
			stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"first":  {Image: "first:latest", User: "application", Tmpfs: []string{"/cache"}},
				"second": {Image: "second:latest", User: "application", Tmpfs: []string{"/scratch"}},
			}}
			items := []backend.LeaseItem{{ServiceName: "first", Quantity: 2}, {ServiceName: "second", Quantity: 3}}
			err, physical := executeStorageWorkflowForTest(t, b, durableCallbackTestLeaseUUID, "tenant-a", func(mutations *storageMutations) error {
				setups, err := b.inspectImagesForSetup(mutations, t.Context(), stack, items)
				if failHelper {
					require.Nil(t, setups, "failed setup must not return launchable partial results")
				} else {
					require.NoError(t, err)
					require.Len(t, setups, 2)
					for _, setup := range setups {
						require.Equal(t, "1000:1001", setup.ContainerUser)
						require.Equal(t, []string{"/data"}, setup.Volumes)
					}
				}
				return err
			})
			if failHelper {
				require.ErrorIs(t, err, failure)
				require.Equal(t, 1, helpers)
				require.Equal(t, substratemutation.Ambiguous, physical.Kind())
				require.ErrorIs(t, physical.Err(), failure)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, helpers)
				require.Equal(t, substratemutation.Attested, physical.Kind())
				require.NoError(t, physical.Err())
			}
		})
	}
}

func TestCompensationMountBudgetRejectsUnusableFrozenLayouts(t *testing.T) {
	for _, scenario := range []struct {
		name string
		plan compensationSourcePlan
		want string
	}{
		{name: "empty source", want: "instance budget"},
		{name: "excess instances", plan: compensationSourcePlan{Containers: make([]compensationContainerRecord, backend.MaxOperationQuantity+1)}, want: "instance budget"},
		{name: "missing frozen host", plan: compensationSourcePlan{Containers: []compensationContainerRecord{{}}}, want: "no frozen mount configuration"},
		{name: "malformed bind", plan: compensationSourcePlan{Containers: []compensationContainerRecord{{Host: &container.HostConfig{Binds: []string{"/source:/data:ro:extra"}}}}}, want: "source launch bind"},
		{name: "oversized target", plan: compensationSourcePlan{Containers: []compensationContainerRecord{{Host: &container.HostConfig{Mounts: []mount.Mount{{Type: mount.TypeTmpfs, Target: "/" + strings.Repeat("x", 4096)}}}}}}, want: "path byte budget"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			require.ErrorContains(t, admitCompensationMountBudget(scenario.plan), scenario.want)
		})
	}
}
