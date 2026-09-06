package docker

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func containersForOperationClaim(claim shared.OperationIntentClaim) []ContainerInfo {
	spec := shared.OperationIntentSpec{
		Kind:                 claim.Kind(),
		LeaseUUID:            claim.LeaseUUID(),
		CallbackURL:          claim.CallbackURL(),
		LifecycleCallbackURL: claim.LifecycleCallbackURL(),
		Tenant:               claim.Tenant(),
		ProviderUUID:         claim.ProviderUUID(),
		Items:                claim.Items(),
		ResourceProfiles:     claim.ResourceProfiles(),
		EffectiveItems:       claim.EffectiveItems(),
		HealthCheckServices:  claim.HealthCheckServices(),
		Manifest:             claim.Manifest(),
		SourceLeaseUUID:      claim.SourceLeaseUUID(),
		SourceGeneration:     claim.SourceGeneration(),
	}
	containers := make([]ContainerInfo, 0)
	for _, item := range claim.EffectiveItems() {
		for index := range item.Quantity {
			containers = append(containers, dockerIntentContainer(
				spec, "container-"+item.ServiceName, item.SKU, index,
			))
		}
	}
	return containers
}

func blockingOperationCompose(
	t *testing.T,
	allowWorker func(),
) (*mockComposeExecutor, <-chan []string) {
	t.Helper()
	projects := make(map[string][]composeContainerSummary)
	var projectsMu sync.Mutex
	serviceNames := make(chan []string, 1)
	compose := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			names := slices.Sorted(maps.Keys(project.Services))
			containers := make([]composeContainerSummary, 0, len(names))
			for _, name := range names {
				containers = append(containers, composeContainerSummary{
					ID:      "container-" + name,
					Service: name,
					State:   "running",
				})
			}
			projectsMu.Lock()
			projects[project.Name] = containers
			projectsMu.Unlock()
			serviceNames <- names
			return nil
		},
		PSFn: func(_ context.Context, projectName string) ([]composeContainerSummary, error) {
			projectsMu.Lock()
			defer projectsMu.Unlock()
			return slices.Clone(projects[projectName]), nil
		},
	}
	t.Cleanup(allowWorker)
	return compose, serviceNames
}

func waitForOperationWorker(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("asynchronous operation worker did not reach the test barrier")
	}
}

func waitForOperationProject(t *testing.T, projects <-chan []string) []string {
	t.Helper()
	select {
	case services := <-projects:
		return services
	case <-time.After(2 * time.Second):
		t.Fatal("asynchronous operation worker did not build a Compose project")
		return nil
	}
}

func TestProvision_AsyncWorkerOwnsRequestAfterReturn(t *testing.T) {
	workerEntered := make(chan struct{})
	allowWorker := make(chan struct{})
	var enterOnce sync.Once
	var allowOnce sync.Once
	releaseWorker := func() { allowOnce.Do(func() { close(allowWorker) }) }

	var b *Backend
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error {
			enterOnce.Do(func() { close(workerEntered) })
			<-allowWorker
			return nil
		},
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			claims, err := b.operationSettlement.ListOperationIntents()
			if err != nil || len(claims) == 0 {
				return nil, err
			}
			for _, container := range containersForOperationClaim(claims[0]) {
				if container.ContainerID == id {
					copy := container
					return &copy, nil
				}
			}
			return nil, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			claims, err := b.operationSettlement.ListOperationIntents()
			if err != nil || len(claims) == 0 {
				return nil, err
			}
			return containersForOperationClaim(claims[0]), nil
		},
	}
	b = newBackendForProvisionTest(t, mock, nil)
	b.cfg.StartupVerifyDuration = time.Millisecond
	compose, projects := blockingOperationCompose(t, releaseWorker)
	b.compose = compose

	originalPayload := validManifestJSON("nginx:latest")
	req := newProvisionRequest(
		"11111111-1111-4111-8111-111111111111",
		"tenant-a",
		"docker-small",
		1,
		originalPayload,
	)
	req.CallbackURL = testOperationCallbackURL(req.CallbackURL)

	require.NoError(t, b.Provision(context.Background(), req))
	waitForOperationWorker(t, workerEntered)

	// Normalize the backend-owned command, never the caller's backing array.
	assert.Empty(t, req.Items[0].ServiceName)

	// This is deliberately after Provision returned and while the async worker
	// is parked. Channel synchronization makes the regression race-friendly:
	// the mutation completes-before the worker resumes and reads its plan.
	req.Items[0].Quantity = 2
	req.Items[0].ServiceName = "attacker"
	for index := range req.Payload {
		req.Payload[index] = 'x'
	}
	releaseWorker()

	assert.Equal(t, []string{manifest.DefaultServiceName}, waitForOperationProject(t, projects),
		"the Compose cohort must come from the sealed operation intent, not caller memory")
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		provision := b.provisions["11111111-1111-4111-8111-111111111111"]
		return provision != nil && provision.Status == backend.ProvisionStatusReady &&
			len(provision.ContainerIDs) == 1 && len(provision.Items) == 1 &&
			provision.Items[0].Quantity == 1 &&
			provision.Items[0].ServiceName == manifest.DefaultServiceName &&
			provision.StackManifest.Services[manifest.DefaultServiceName].Image == "nginx:latest"
	}, 3*time.Second, 10*time.Millisecond)

	b.stopCancel()
	b.wg.Wait()
}

func TestRestore_AsyncWorkerOwnsRequestAfterReturn(t *testing.T) {
	workerEntered := make(chan struct{})
	allowWorker := make(chan struct{})
	var enterOnce sync.Once
	var allowOnce sync.Once
	releaseWorker := func() { allowOnce.Do(func() { close(allowWorker) }) }

	var b *Backend
	mock := &mockDockerClient{
		InspectImageFn: func(_ context.Context, _ string) (*ImageInfo, error) {
			enterOnce.Do(func() { close(workerEntered) })
			<-allowWorker
			return &ImageInfo{ID: "image-1", Volumes: map[string]struct{}{}}, nil
		},
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			claims, err := b.operationSettlement.ListOperationIntents()
			if err != nil || len(claims) == 0 {
				return nil, err
			}
			for _, container := range containersForOperationClaim(claims[0]) {
				if container.ContainerID == id {
					copy := container
					return &copy, nil
				}
			}
			return nil, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			claims, err := b.operationSettlement.ListOperationIntents()
			if err != nil || len(claims) == 0 {
				return nil, err
			}
			return containersForOperationClaim(claims[0]), nil
		},
	}
	b = newBackendForProvisionTest(t, mock, nil)
	b.cfg.StartupVerifyDuration = time.Millisecond
	retentions := attachRetentionStore(t, b)
	seedActiveRetained(t, retentions, "22222222-2222-4222-8222-222222222222")
	b.volumes = &mockVolumeManager{RenameVolumeFn: func(_, _ string) error { return nil }}
	compose, projects := blockingOperationCompose(t, releaseWorker)
	b.compose = compose

	req := restoreRequest(
		"33333333-3333-4333-8333-333333333333",
		"22222222-2222-4222-8222-222222222222",
		"http://localhost/callbacks/provision",
	)
	req.Items[0].ServiceName = "" // exercise boundary normalization on owned storage

	require.NoError(t, b.Restore(context.Background(), req))
	waitForOperationWorker(t, workerEntered)

	assert.Empty(t, req.Items[0].ServiceName,
		"restore normalization must not mutate the caller's backing array")
	req.Items[0].Quantity = 2
	req.Items[0].ServiceName = "attacker"
	releaseWorker()

	assert.Equal(t, []string{manifest.DefaultServiceName}, waitForOperationProject(t, projects),
		"the restored cohort must come from the sealed Restoring proof, not caller memory")
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		provision := b.provisions["33333333-3333-4333-8333-333333333333"]
		return provision != nil && provision.Status == backend.ProvisionStatusReady &&
			len(provision.ContainerIDs) == 1 && len(provision.Items) == 1 &&
			provision.Items[0].Quantity == 1 &&
			provision.Items[0].ServiceName == manifest.DefaultServiceName
	}, 3*time.Second, 10*time.Millisecond)

	b.stopCancel()
	b.wg.Wait()
}
