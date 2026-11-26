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
	seedAdoptedAppRelease(t, releases, leaseUUID, providerUUID, callbackURL,
		[]byte(`{"services":{"app":{"image":"nginx:1.27"}}}`), createdAt)
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
	requireAccepted(t, func() bool { return b.routeActorObservation(observation) })

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

func TestRecoverV013PolicyDriftPreservesRestartAndUpdateSource(t *testing.T) {
	b, dockerState, _, releases := newMigrationTestBackend(t)
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	createdAt := time.Now().Add(-time.Hour).UTC()
	dockerState.containers = []ContainerInfo{{
		ContainerID: "app-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
		ProviderUUID: providerUUID, BackendName: "docker", SKU: "docker-small",
		ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.27",
		CallbackURL: "https://fred.example/callbacks/provision", Status: "running", CreatedAt: createdAt,
	}}
	seedAdoptedAppRelease(t, releases, leaseUUID, providerUUID, "https://fred.example/callbacks/provision",
		[]byte(`{"services":{"app":{"image":"nginx:1.27","labels":{"com.docker.compose.project":"legacy","traefiK.enable":"true"},"user":"1:2:3"}}}`), createdAt)
	require.NoError(t, b.recoverState(t.Context()))
	active, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NotEmpty(t, active.Items)
	require.NotEmpty(t, active.ResourceProfiles)
	require.NoError(t, validateReplaceSourceRelease(*active))
	require.NoError(t, releases.Store.Healthy())
}

// seedAdoptedAppRelease writes the active release of a one-instance lease
// adopted from v0.13, with its items, resource profiles and tokenless runtime
// authority recorded, as the first 0.14 start left it.
func seedAdoptedAppRelease(
	t *testing.T,
	releases *fakeReleaseStore,
	leaseUUID, providerUUID, callbackURL string,
	stackManifest []byte,
	createdAt time.Time,
) {
	t.Helper()
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	authority, err := shared.NewLegacyRuntimeAuthority("tenant-a", providerUUID, callbackURL, "")
	require.NoError(t, err)
	releases.seedReleaseRows(t, map[string]shared.Release{leaseUUID: {
		Manifest: stackManifest, Image: "stack", Status: "active", CreatedAt: createdAt,
		Items: items, ResourceProfiles: testResourceProfiles(t, items),
		LegacyRuntimeAuthority: &authority,
	}})
}
