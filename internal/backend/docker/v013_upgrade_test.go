package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func v013StackRelease() *shared.Release {
	return &shared.Release{
		Version: 2,
		Manifest: []byte(`{"services":{"web":{"image":"nginx:1.27"},` +
			`"worker":{"image":"busybox:1.37"}}}`),
		Image: "stack", Status: "active",
	}
}

func v013StackCohort() []ContainerInfo {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	return []ContainerInfo{
		{
			ContainerID: "web-1", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: "22222222-2222-4222-8222-222222222222",
			SKU:          "sku-web", ServiceName: "web", InstanceIndex: 1,
			Image: "nginx:1.27", CustomDomain: "www.example.test",
		},
		{
			ContainerID: "worker-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: "22222222-2222-4222-8222-222222222222",
			SKU:          "sku-worker", ServiceName: "worker", InstanceIndex: 0,
			Image: "busybox:1.37",
		},
		{
			ContainerID: "web-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: "22222222-2222-4222-8222-222222222222",
			SKU:          "sku-web", ServiceName: "web", InstanceIndex: 0,
			Image: "nginx:1.27", CustomDomain: "www.example.test",
		},
	}
}

func TestDeriveV013ActiveReleaseItems_ExactStackCohort(t *testing.T) {
	items, err := deriveV013ActiveReleaseItems(v013StackRelease(), v013StackCohort())
	require.NoError(t, err)
	require.Equal(t, []backend.LeaseItem{
		{SKU: "sku-web", Quantity: 2, ServiceName: "web", CustomDomain: "www.example.test"},
		{SKU: "sku-worker", Quantity: 1, ServiceName: "worker"},
	}, items, "the durable order is canonical service order, never Docker list order")
}

func TestDeriveV013ActiveReleaseItems_RejectsLocallyProvableDivergence(t *testing.T) {
	tests := map[string]func([]ContainerInfo) []ContainerInfo{
		"missing manifest service":  func(in []ContainerInfo) []ContainerInfo { return []ContainerInfo{in[0], in[2]} },
		"duplicate index":           func(in []ContainerInfo) []ContainerInfo { in[0].InstanceIndex = 0; return in },
		"sparse indexes":            func(in []ContainerInfo) []ContainerInfo { in[0].InstanceIndex = 2; return in },
		"wrong service":             func(in []ContainerInfo) []ContainerInfo { in[1].ServiceName = "ghost"; return in },
		"wrong image":               func(in []ContainerInfo) []ContainerInfo { in[0].Image = "nginx:latest"; return in },
		"service SKU divergence":    func(in []ContainerInfo) []ContainerInfo { in[0].SKU = "sku-other"; return in },
		"service domain divergence": func(in []ContainerInfo) []ContainerInfo { in[0].CustomDomain = "other.example.test"; return in },
		"tenant divergence":         func(in []ContainerInfo) []ContainerInfo { in[0].Tenant = "tenant-b"; return in },
		"provider divergence": func(in []ContainerInfo) []ContainerInfo {
			in[0].ProviderUUID = "33333333-3333-4333-8333-333333333333"
			return in
		},
		"pre-stack member": func(in []ContainerInfo) []ContainerInfo { in[0].ServiceName = ""; return in },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cohort := append([]ContainerInfo(nil), v013StackCohort()...)
			_, err := deriveV013ActiveReleaseItems(v013StackRelease(), mutate(cohort))
			require.Error(t, err)
		})
	}
}

func TestDeriveV013ActiveReleaseItems_HighestMissingIndexNeedsChainProof(t *testing.T) {
	release := &shared.Release{
		Version: 1, Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image: "stack", Status: "active",
	}
	cohort := v013StackCohort()[2:]
	cohort[0].ServiceName = "app"
	cohort[0].CustomDomain = ""
	items, err := deriveV013ActiveReleaseItems(release, cohort)
	require.NoError(t, err)
	require.Equal(t, 1, items[0].Quantity)
}

func TestDeriveV013ActiveReleaseItems_RejectsPreStackFlatLineage(t *testing.T) {
	release := &shared.Release{
		Version: 1, Manifest: []byte(`{"image":"nginx:1.27"}`),
		Image: "nginx:1.27", Status: "active",
	}
	cohort := v013StackCohort()[2:]
	cohort[0].ServiceName = ""
	cohort[0].CustomDomain = ""
	_, err := deriveV013ActiveReleaseItems(release, cohort)
	require.Error(t, err)
}

func TestRecoverState_BackfillsAndFreezesV013MultiSKUStackAuthority(t *testing.T) {
	b, fakeDocker, _, releases := newMigrationTestBackend(t)
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
	)
	createdAt := time.Now().Add(-time.Hour).UTC()
	fakeDocker.containers = []ContainerInfo{
		{ContainerID: "web-1", LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID, BackendName: "docker", SKU: "docker-small", ServiceName: "web", InstanceIndex: 1, Image: "nginx:1.27", CustomDomain: "www.example.test", CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt},
		{ContainerID: "worker-0", LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID, BackendName: "docker", SKU: "docker-large", ServiceName: "worker", InstanceIndex: 0, Image: "busybox:1.37", CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt},
		{ContainerID: "web-0", LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID, BackendName: "docker", SKU: "docker-small", ServiceName: "web", InstanceIndex: 0, Image: "nginx:1.27", CustomDomain: "www.example.test", CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt},
	}
	releases.SeedRelease(t, leaseUUID, shared.Release{
		Manifest: v013StackRelease().Manifest, Image: "stack", Status: "active", CreatedAt: createdAt,
	})
	require.NoError(t, b.recoverState(t.Context()))
	active, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 2, ServiceName: "web", CustomDomain: "www.example.test"},
		{SKU: "docker-large", Quantity: 1, ServiceName: "worker"},
	}, active.Items)
	require.Equal(t, []shared.SKUResourceSnapshot{
		{SKU: "docker-large", CPUCores: 2, MemoryMB: 2048, DiskMB: 4096},
		{SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
	}, active.ResourceProfiles)

	b.cfg.SKUProfiles = map[string]SKUProfile{}
	require.NoError(t, b.recoverState(t.Context()))
	replayed, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, active.Items, replayed.Items)
	require.Equal(t, active.ResourceProfiles, replayed.ResourceProfiles)
}

func TestRecoveredV013ContainerDeathPublishesTokenlessLifecycleObservation(t *testing.T) {
	b, dockerState, _, releases := newMigrationTestBackend(t)
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
	)
	createdAt := time.Now().Add(-time.Hour).UTC()
	dockerState.containers = []ContainerInfo{{
		ContainerID: "app-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
		ProviderUUID: providerUUID, BackendName: "docker", SKU: "docker-small",
		ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.27",
		CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt,
	}}
	releases.SeedRelease(t, leaseUUID, shared.Release{
		Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:    "stack", Status: "active", CreatedAt: createdAt,
	})
	require.NoError(t, b.recoverState(t.Context()))

	runtime, err := releases.Store.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, shared.ReleaseAuthorityLegacy, runtime.AuthorityClass())
	require.True(t, runtime.OperationID().IsZero(), "v0.13 runtime authority is intentionally tokenless")

	dockerState.containers[0].Status = "exited"
	dockerState.containers[0].ExitCode = 137
	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	mock.ContainerLogsFn = func(context.Context, string, int) (string, error) {
		return "legacy container exited", nil
	}
	observation, err := leasesm.NewContainerDiedObservation("app-0", runtime)
	require.NoError(t, err)
	require.True(t, b.routeActorObservation(observation))

	require.Eventually(t, func() bool {
		pending, listErr := b.callbackStore.ListPending()
		return listErr == nil && len(pending) == 1
	}, 2*time.Second, 10*time.Millisecond, "legacy lifecycle observation was suppressed")
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, callbackURL, pending[0].CallbackURL)
	require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	require.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestStrictManagedInventoryRejectsPreStackContainers(t *testing.T) {
	err := validateStrictManagedContainerLabels("container-a", "docker", map[string]string{
		LabelManaged: "true", LabelBackendName: "docker",
		LabelLeaseUUID: "550e8400-e29b-41d4-a716-446655440000",
		LabelTenant:    "tenant-a", LabelProviderUUID: "22222222-2222-4222-8222-222222222222",
		LabelSKU: "docker-small", LabelInstanceIndex: "0",
		LabelCallbackURL: "https://fred.example/callbacks/provision",
	})
	require.ErrorIs(t, err, ErrPreStackWorkloadUnsupported)
}
