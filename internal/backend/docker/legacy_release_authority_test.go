package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func legacyStackRelease() *shared.Release {
	return &shared.Release{
		Version: 2,
		Manifest: []byte(`{"services":{"web":{"image":"nginx:1.27"},` +
			`"worker":{"image":"busybox:1.37"}}}`),
		Image:  "stack",
		Status: "active",
	}
}

func legacyStackCohort() []ContainerInfo {
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

func TestDeriveLegacyActiveReleaseItems_ExactObservedCohort(t *testing.T) {
	items, err := deriveLegacyActiveReleaseItems(legacyStackRelease(), legacyStackCohort())
	require.NoError(t, err)
	require.Equal(t, []backend.LeaseItem{
		{SKU: "sku-web", Quantity: 2, ServiceName: "web", CustomDomain: "www.example.test"},
		{SKU: "sku-worker", Quantity: 1, ServiceName: "worker"},
	}, items, "the durable order is canonical service order, never Docker list order")
}

func TestDeriveLegacyActiveReleaseItems_RejectsLocallyProvableDivergence(t *testing.T) {
	tests := map[string]func([]ContainerInfo) []ContainerInfo{
		"missing manifest service": func(in []ContainerInfo) []ContainerInfo {
			return []ContainerInfo{in[0], in[2]}
		},
		"duplicate index": func(in []ContainerInfo) []ContainerInfo {
			in[0].InstanceIndex = 0
			return in
		},
		"sparse indexes": func(in []ContainerInfo) []ContainerInfo {
			in[0].InstanceIndex = 2
			return in
		},
		"wrong service": func(in []ContainerInfo) []ContainerInfo {
			in[1].ServiceName = "ghost"
			return in
		},
		"wrong image": func(in []ContainerInfo) []ContainerInfo {
			in[0].Image = "nginx:latest"
			return in
		},
		"service SKU divergence": func(in []ContainerInfo) []ContainerInfo {
			in[0].SKU = "sku-other"
			return in
		},
		"service domain divergence": func(in []ContainerInfo) []ContainerInfo {
			in[0].CustomDomain = "other.example.test"
			return in
		},
		"tenant divergence": func(in []ContainerInfo) []ContainerInfo {
			in[0].Tenant = "tenant-b"
			return in
		},
		"provider divergence": func(in []ContainerInfo) []ContainerInfo {
			in[0].ProviderUUID = "33333333-3333-4333-8333-333333333333"
			return in
		},
		"mixed legacy and stack labels": func(in []ContainerInfo) []ContainerInfo {
			in[0].ServiceName = ""
			return in
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cohort := append([]ContainerInfo(nil), legacyStackCohort()...)
			_, err := deriveLegacyActiveReleaseItems(legacyStackRelease(), mutate(cohort))
			require.Error(t, err)
		})
	}
}

func TestDeriveLegacyActiveReleaseItems_HighestMissingIndexNeedsChainProof(t *testing.T) {
	// v0.13 did not persist Items. A surviving prefix {0} is locally coherent,
	// so Docker+Release evidence cannot distinguish original quantity=1 from a
	// quantity=2 cohort whose highest sibling disappeared. The mandatory stopped
	// placement preflight compares this derived inventory with immutable chain
	// items and rejects the latter case.
	release := &shared.Release{
		Version: 1, Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image: "stack", Status: "active",
	}
	cohort := legacyStackCohort()[2:]
	cohort[0].ServiceName = "app"
	cohort[0].CustomDomain = ""
	items, err := deriveLegacyActiveReleaseItems(release, cohort)
	require.NoError(t, err)
	require.Equal(t, 1, items[0].Quantity)
}

func TestDeriveLegacyActiveReleaseItems_AcceptsPreStackFlatLineage(t *testing.T) {
	release := &shared.Release{
		Version: 1, Manifest: []byte(`{"image":"nginx:1.27"}`),
		Image: "nginx:1.27", Status: "active",
	}
	cohort := legacyStackCohort()[2:]
	cohort[0].ServiceName = ""
	cohort[0].CustomDomain = ""
	items, err := deriveLegacyActiveReleaseItems(release, cohort)
	require.NoError(t, err)
	require.Equal(t, []backend.LeaseItem{{
		SKU: "sku-web", Quantity: 1, ServiceName: "app",
	}}, items)
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
		{
			ContainerID: "web-1", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: providerUUID, SKU: "docker-small", ServiceName: "web",
			InstanceIndex: 1, Image: "nginx:1.27", CustomDomain: "www.example.test",
			CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt,
		},
		{
			ContainerID: "worker-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: providerUUID, SKU: "docker-large", ServiceName: "worker",
			InstanceIndex: 0, Image: "busybox:1.37", CallbackURL: callbackURL,
			Status: "running", CreatedAt: createdAt,
		},
		{
			ContainerID: "web-0", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: providerUUID, SKU: "docker-small", ServiceName: "web",
			InstanceIndex: 0, Image: "nginx:1.27", CustomDomain: "www.example.test",
			CallbackURL: callbackURL, Status: "running", CreatedAt: createdAt,
		},
	}
	require.NoError(t, releases.Store.Append(leaseUUID, shared.Release{
		Manifest: legacyStackRelease().Manifest,
		Image:    "stack", Status: "active", CreatedAt: createdAt,
	}))

	require.NoError(t, b.recoverState(t.Context()))
	active, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, []backend.LeaseItem{
		{
			SKU: "docker-small", Quantity: 2, ServiceName: "web",
			CustomDomain: "www.example.test",
		},
		{SKU: "docker-large", Quantity: 1, ServiceName: "worker"},
	}, active.Items)
	require.Equal(t, []shared.SKUResourceSnapshot{
		{SKU: "docker-large", CPUCores: 2, MemoryMB: 2048, DiskMB: 4096},
		{SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
	}, active.ResourceProfiles)

	// A second cold projection must consume the frozen snapshot. Removing both
	// SKUs from current policy would make the old per-container recovery path
	// fail or reprice; it is irrelevant once the v0.13 row has been backfilled.
	b.cfg.SKUProfiles = map[string]SKUProfile{}
	require.NoError(t, b.recoverState(t.Context()))
	replayed, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, active.Items, replayed.Items)
	require.Equal(t, active.ResourceProfiles, replayed.ResourceProfiles)
}

func TestRecoverState_V013CommittedMigrationMarkerMakesImmediateCloseConsumeRollbackIDs(t *testing.T) {
	b, fakeDocker, _, releases := newMigrationTestBackend(t)
	t.Cleanup(b.stopCancel)
	b.cfg.MigrationGracePeriod = time.Hour
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
	)
	createdAt := time.Now().Add(-time.Hour).UTC()
	fakeDocker.containers = []ContainerInfo{
		{
			ContainerID: "stack-id", Name: "fred-" + leaseUUID + "-app-0",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			BackendName: "docker",
			SKU:         "docker-small", ServiceName: "app", InstanceIndex: 0,
			Image: "nginx:1.27", CallbackURL: callbackURL,
			Status: "running", CreatedAt: createdAt,
		},
		{
			ContainerID: "prev-id", Name: "fred-" + leaseUUID + "-app-0-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			BackendName: "docker",
			SKU:         "docker-small", InstanceIndex: 0, Image: "nginx:1.27",
			CallbackURL: callbackURL, Status: "exited", CreatedAt: createdAt,
		},
	}
	require.NoError(t, releases.Store.Append(leaseUUID, shared.Release{
		Manifest:  []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:     "stack",
		Status:    "active",
		CreatedAt: createdAt,
	}))

	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	bindTestStorageIdentity(t, b, mock)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: b.cfg.CallbackDBPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks
	b.operationIntents = callbacks

	require.NoError(t, b.recoverState(t.Context()))
	active, err := releases.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.True(t, active.LegacyMigration,
		"the exact post-RecordMigration cohort must persist cleanup authority with Items")
	require.Equal(t, []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: "app",
	}}, active.Items)

	var removed []string
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		claim, found, claimErr := callbacks.GetCloseIntent(leaseUUID)
		require.NoError(t, claimErr)
		require.True(t, found, "destructive rollback cleanup must be owned by the close journal")
		require.Equal(t, []shared.CloseLegacyRollbackTarget{{
			ContainerID: "prev-id",
			Name:        "fred-" + leaseUUID + "-app-0-prev",
		}}, claim.LegacyRollbackTargets())
		history, historyErr := releases.Store.List(leaseUUID)
		require.NoError(t, historyErr)
		require.NotEmpty(t, history,
			"release authority must remain until immutable rollback cleanup succeeds")
		removed = append(removed, containerID)
		return nil
	}

	require.NoError(t, b.doDeprovision(t.Context(), leaseUUID))
	require.Equal(t, []string{"prev-id"}, removed,
		"close must delete the immutable ID, never name-only authority")
	history, err := releases.Store.List(leaseUUID)
	require.NoError(t, err)
	require.Empty(t, history, "release history retires only after rollback cleanup")
	_, found, err := callbacks.GetCloseIntent(leaseUUID)
	require.NoError(t, err)
	require.False(t, found)
}

func TestInspectV013MigrationCrashCohort_ClassifiesBothCommitBoundaries(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	stack := []ContainerInfo{
		{
			ContainerID: "stack-0", Name: "fred-" + leaseUUID + "-app-0",
			LeaseUUID: leaseUUID, Tenant: "tenant-a",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0,
			Image: "nginx:1.27",
		},
		{
			ContainerID: "stack-1", Name: "fred-" + leaseUUID + "-app-1",
			LeaseUUID: leaseUUID, Tenant: "tenant-a",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 1,
			Image: "nginx:1.27",
		},
	}
	prev := []ContainerInfo{
		{
			ContainerID: "prev-0", Name: "fred-" + leaseUUID + "-app-0-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			SKU: "docker-small", InstanceIndex: 0, Image: "nginx:1.27",
			CustomDomain: "legacy.example", Status: "exited",
		},
		{
			ContainerID: "prev-1", Name: "fred-" + leaseUUID + "-app-1-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			SKU: "docker-small", InstanceIndex: 1, Image: "nginx:1.27",
			CustomDomain: "legacy.example", Status: "exited",
		},
	}

	t.Run("before RecordMigration keeps complete rollback authority", func(t *testing.T) {
		release := &shared.Release{
			Version: 1, Manifest: []byte(`{"image":"nginx:1.27"}`),
			Image: "nginx:1.27", Status: "active",
		}
		class, items, err := inspectV013MigrationCrashCohort(
			release,
			append(append([]ContainerInfo(nil), stack...), prev...),
		)
		require.NoError(t, err)
		require.Equal(t, v013MigrationCrashBeforeRelease, class)
		require.Equal(t, []backend.LeaseItem{{
			SKU: "docker-small", Quantity: 2, ServiceName: "app",
			CustomDomain: "legacy.example",
		}}, items, "the untouched rollback labels preserve v0.13's omitted domain")
	})

	t.Run("after RecordMigration never infers quantity from partial rollback", func(t *testing.T) {
		release := &shared.Release{
			Version:  2,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
			Image:    "stack", Status: "active",
		}
		class, items, err := inspectV013MigrationCrashCohort(
			release,
			append(append([]ContainerInfo(nil), stack...), prev[1]),
		)
		require.NoError(t, err)
		require.Equal(t, v013MigrationCrashAfterRelease, class)
		require.Equal(t, 2, items[0].Quantity,
			"the complete stack, not the one surviving prev, owns desired quantity")
	})

	for _, status := range []string{"running", "paused"} {
		t.Run("rejects non-stopped rollback state "+status, func(t *testing.T) {
			release := &shared.Release{
				Version: 1, Manifest: []byte(`{"image":"nginx:1.27"}`),
				Image: "nginx:1.27", Status: "active",
			}
			unsafePrev := append([]ContainerInfo(nil), prev...)
			unsafePrev[0].Status = status
			_, _, err := inspectV013MigrationCrashCohort(
				release,
				append(append([]ContainerInfo(nil), stack...), unsafePrev...),
			)
			require.ErrorContains(t, err, "is not stopped")
		})
	}
}

func TestPlanLegacyMigrations_TreatsPostRecordPrevAsCleanupOnly(t *testing.T) {
	b, _, _, releases := newMigrationTestBackend(t)
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	require.NoError(t, releases.Store.Append(leaseUUID, shared.Release{
		Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:    "stack", Status: "active", CreatedAt: time.Now(),
	}))
	containers := []ContainerInfo{
		{
			ContainerID: "stack-0", Name: "fred-" + leaseUUID + "-app-0",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: "provider-a",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0,
			Image: "nginx:1.27",
		},
		{
			ContainerID: "prev-0", Name: "fred-" + leaseUUID + "-app-0-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: "provider-a",
			SKU: "docker-small", InstanceIndex: 0, Image: "nginx:1.27",
			Status: "exited",
		},
	}
	plans, cleanup, err := b.planLegacyMigrations(t.Context(), containers)
	require.NoError(t, err)
	require.Empty(t, plans, "a partial cleanup cohort must never be reprovisioned")
	require.Equal(t, []ContainerInfo{containers[1]}, cleanup[leaseUUID].remnants)
	require.Equal(t, shared.LegacyActiveAuthorityMigration,
		cleanup[leaseUUID].legacyAuthorityClass,
		"only the exact post-RecordMigration classifier can authorize a missing marker backfill")
}

func TestPlanLegacyMigrations_PrevNameAloneCannotMintMigrationMarker(t *testing.T) {
	b, _, _, releases := newMigrationTestBackend(t)
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: "app",
	}}
	profiles := testResourceProfiles(t, items)
	require.NoError(t, releases.Store.Append(leaseUUID, shared.Release{
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:            "stack",
		Items:            items,
		ResourceProfiles: profiles,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	containers := []ContainerInfo{
		{
			ContainerID: "stack-0", Name: "fred-" + leaseUUID + "-app-0",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: "provider-a",
			SKU: "docker-small", ServiceName: "app", InstanceIndex: 0,
			Image: "nginx:1.27",
		},
		{
			ContainerID: "prev-0", Name: "fred-" + leaseUUID + "-app-0-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: "provider-a",
			SKU: "docker-small", InstanceIndex: 0, Image: "nginx:1.27",
		},
	}

	plans, cleanup, err := b.planLegacyMigrations(t.Context(), containers)
	require.NoError(t, err)
	require.Empty(t, plans)
	require.Equal(t, []ContainerInfo{containers[1]}, cleanup[leaseUUID].remnants)
	require.Zero(t, cleanup[leaseUUID].legacyAuthorityClass,
		"an exact-looking name is cleanup evidence, not authority to rewrite release provenance")
}

func TestStorageIdentityVolumeEvidenceAcceptsExactRenamedPrevSource(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	root := t.TempDir()
	newVolume := canonicalVolumeName(leaseUUID, "app", 0)
	require.NoError(t, os.MkdirAll(filepath.Join(root, newVolume, "data"), 0o700))
	require.NoError(t, os.MkdirAll(
		filepath.Join(root, newVolume, writablePathSubdir, "var", "cache", "app"),
		0o700,
	))
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	cfg.VolumeDataPath = root
	container := ContainerInfo{
		ContainerID: "prev-0", Name: "fred-" + leaseUUID + "-app-0-prev",
		LeaseUUID: leaseUUID, SKU: "docker-small", InstanceIndex: 0,
		Mounts: []ContainerMount{{
			Type: "bind", Target: "/data",
			Source: filepath.Join(root, "fred-"+leaseUUID+"-0", "data"),
		}, {
			Type: "bind", Target: "/var/cache/app",
			Source: filepath.Join(
				root,
				"fred-"+leaseUUID+"-0",
				writablePathSubdir,
				"var",
				"cache",
				"app",
			),
		}},
	}

	evidence, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{container})
	require.NoError(t, err)
	require.Contains(t, evidence, newVolume)
	container.Mounts[0].Target = "/other"
	_, err = storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{container})
	require.Error(t, err,
		"the alternate path is admitted only when its tail is the exact sanitized mount target")
	container.Mounts[0].Target = "/data"
	require.NoError(t, os.MkdirAll(
		filepath.Join(root, "fred-"+leaseUUID+"-0"),
		0o700,
	))
	_, err = storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{container})
	require.Error(t, err,
		"an existing old parent is not the exact whole-parent v0.13 rename boundary")
}
