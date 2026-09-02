package docker

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func newReleaseCohortBackend(t *testing.T, existing map[string]*provision) *Backend {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 2, ServiceName: "app",
	}}
	container := ContainerInfo{
		ContainerID: "c0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
		SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.25",
		Status: "running", CreatedAt: time.Now(),
	}
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{container}, nil
		},
	}, existing)
	t.Cleanup(b.stopCancel)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	require.NoError(t, releases.Append("lease-1", shared.Release{
		Manifest:         []byte(`{"image":"nginx:1.25"}`),
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Image:            "stack", Status: "active", CreatedAt: time.Now(),
	}))
	b.releaseStore = releases
	return b
}

func TestValidateRecoveredReleaseCohort_ExactInstanceSet(t *testing.T) {
	release := &shared.Release{
		Manifest: []byte(`{"image":"nginx:1.25"}`),
		Items: []backend.LeaseItem{{
			SKU: "docker-small", Quantity: 2, ServiceName: "app", CustomDomain: "tenant.example",
		}},
	}
	cohort := []ContainerInfo{
		{
			ContainerID: "c0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.25",
			CustomDomain: "tenant.example",
		},
		{
			ContainerID: "c1", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 1, Image: "nginx:1.25",
			CustomDomain: "tenant.example",
		},
	}

	require.NoError(t, validateRecoveredReleaseCohort(release, cohort))

	tests := map[string]func([]ContainerInfo) []ContainerInfo{
		"missing sibling": func(in []ContainerInfo) []ContainerInfo {
			return in[:1]
		},
		"duplicate index hides missing sibling": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[1].InstanceIndex = 0
			return out
		},
		"wrong tenant": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[1].Tenant = "tenant-2"
			return out
		},
		"wrong provider": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[1].ProviderUUID = "provider-2"
			return out
		},
		"missing tenant identity": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[0].Tenant = ""
			out[1].Tenant = ""
			return out
		},
		"missing provider identity": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[0].ProviderUUID = ""
			out[1].ProviderUUID = ""
			return out
		},
		"wrong image": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[1].Image = "nginx:latest"
			return out
		},
		"wrong domain": func(in []ContainerInfo) []ContainerInfo {
			out := append([]ContainerInfo(nil), in...)
			out[1].CustomDomain = "other.example"
			return out
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateRecoveredReleaseCohort(release, mutate(cohort)))
		})
	}
}

func TestValidateRecoveredReleaseCohort_LegacyReleaseWithoutItems(t *testing.T) {
	require.NoError(t, validateRecoveredReleaseCohort(
		&shared.Release{Manifest: []byte(`{"image":"nginx:1.25"}`)},
		[]ContainerInfo{{ContainerID: "only-survivor"}},
	))
}

func TestValidateRecoveredReleaseCohort_RejectsUnboundedDurableQuantities(t *testing.T) {
	for name, items := range map[string][]backend.LeaseItem{
		"single overflow quantity": {{
			SKU: "docker-small", Quantity: math.MaxInt, ServiceName: "app",
		}},
		"aggregate exceeds bound": {
			{SKU: "docker-small", Quantity: backend.MaxOperationQuantity, ServiceName: "app"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "worker"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			release := &shared.Release{
				Manifest: []byte(`{"image":"nginx:1.25"}`),
				Items:    items,
			}
			require.ErrorContains(t,
				validateRecoveredReleaseCohort(release, nil),
				"validate durable release quantities",
			)

			b := newBackendForTest(&mockDockerClient{}, nil)
			_, err := b.recoveredReleaseAllocations("lease-1", "tenant-1", items, nil)
			require.ErrorContains(t, err, "validate durable release quantities")
		})
	}
}

func TestRecoverState_ExactCohortRestoresDurableItemOrder(t *testing.T) {
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	// Docker deliberately returns the services in the opposite order from the
	// accepted operation. The set is exact, but list order is not authority.
	containers := []ContainerInfo{
		{
			ContainerID: "db-0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "db", InstanceIndex: 0, Image: "postgres:17",
			Status: "running", CreatedAt: time.Now(),
		},
		{
			ContainerID: "web-0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "web", InstanceIndex: 0, Image: "nginx:1.25",
			Status: "running", CreatedAt: time.Now(),
		},
	}
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return containers, nil
		},
	}, nil)
	t.Cleanup(b.stopCancel)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	require.NoError(t, releases.Append("lease-1", shared.Release{
		Manifest:         []byte(`{"services":{"web":{"image":"nginx:1.25"},"db":{"image":"postgres:17"}}}`),
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Image:            "stack", Status: "active", CreatedAt: time.Now(),
	}))
	b.releaseStore = releases

	require.NoError(t, b.recoverState(context.Background()))
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, items, info.Items,
		"recovery must preserve durable operation order, not Docker list order")
}

func TestRecoverState_PartialDurableReleaseCohortFailsClosedOnColdStart(t *testing.T) {
	b := newReleaseCohortBackend(t, nil)

	require.NoError(t, b.recoverState(context.Background()))
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	require.Equal(t, backend.ReasonInternal, info.Reason)
	require.Equal(t, 2, info.Quantity, "desired quantity comes from the durable release, not survivors")
	require.Len(t, info.Items, 1)
	require.Equal(t, 2, info.Items[0].Quantity)
	stats := b.pool.Stats()
	require.Equal(t, 2, stats.AllocationCount,
		"every durable desired instance remains reserved when a sibling disappears")
	require.Equal(t, 1.0, stats.AllocatedCPU)
	require.Equal(t, int64(1024), stats.AllocatedMemoryMB)
	require.Equal(t, int64(2048), stats.AllocatedDiskMB,
		"a missing stateful sibling must not make its disk reservation appear free")
}

func TestRecoverState_ExactReleaseWithNoSurvivorsFailsStartupClosed(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}, nil)
	t.Cleanup(b.stopCancel)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: "app",
	}}
	require.NoError(t, releases.Append("lease-with-no-survivors", shared.Release{
		Manifest:         []byte(`{"image":"nginx:1.25"}`),
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Image:            "stack", Status: "active", CreatedAt: time.Now(),
	}))
	b.releaseStore = releases

	err = b.recoverState(context.Background())
	require.ErrorContains(t, err, `durable release cohort for lease "lease-with-no-survivors" cannot be materialized`)
	require.ErrorContains(t, err, "found 0 containers, expected 1")
}

func TestRecoverState_DuplicateSurvivorDoesNotDoubleCountAllocation(t *testing.T) {
	containers := []ContainerInfo{
		{
			ContainerID: "c0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.25",
			Status: "running", CreatedAt: time.Now(),
		},
		{
			ContainerID: "duplicate-c0", LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Image: "nginx:1.25",
			Status: "running", CreatedAt: time.Now(),
		},
	}
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return containers, nil
		},
	}, nil)
	t.Cleanup(b.stopCancel)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: "app",
	}}
	require.NoError(t, releases.Append("lease-1", shared.Release{
		Manifest:         []byte(`{"image":"nginx:1.25"}`),
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Image:            "stack", Status: "active", CreatedAt: time.Now(),
	}))
	b.releaseStore = releases

	require.NoError(t, b.recoverState(context.Background()))
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	stats := b.pool.Stats()
	require.Equal(t, 1, stats.AllocationCount)
	require.Equal(t, 0.5, stats.AllocatedCPU)
	require.Equal(t, int64(512), stats.AllocatedMemoryMB)
	require.Equal(t, int64(1024), stats.AllocatedDiskMB,
		"duplicate Docker rows must not inflate aggregate accounting")
}

func TestRecoverState_PartialDurableReleaseCohortTransitionsExistingActor(t *testing.T) {
	existing := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-1", Tenant: "tenant-1", ProviderUUID: "provider-1",
			Status: backend.ProvisionStatusReady, Quantity: 2, CreatedAt: time.Now(),
		}},
	}
	b := newReleaseCohortBackend(t, existing)

	require.NoError(t, b.recoverState(context.Background()))
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	require.Equal(t, backend.ReasonInternal, info.Reason)
}
