package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// TestResolveMigratedBindSource pins the ENG-539 hardening on the legacy→stack
// migration path: a stateful volume target must never resolve through a symlink
// that escapes the volume root, or the recreated container would bind-mount an
// arbitrary host path. Legacy containers are stopped before this runs, so there
// is no concurrent writer racing the check.
func TestResolveMigratedBindSource(t *testing.T) {
	t.Run("existing real directory resolves within the volume root", func(t *testing.T) {
		hostRoot := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(hostRoot, "data"), 0o700))
		src, err := resolveMigratedBindSource(hostRoot, "data")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(hostRoot, "data"), src)
	})

	t.Run("missing subdir is allowed (Docker creates it safely within the root)", func(t *testing.T) {
		hostRoot := t.TempDir()
		src, err := resolveMigratedBindSource(hostRoot, "data")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(hostRoot, "data"), src)
	})

	t.Run("absent volume root falls back to plain join (behavior-preserving)", func(t *testing.T) {
		// In the normal flow the root exists after RenameVolume; when it does not
		// (e.g. nothing was ever written), there is no tree to hide a symlink, so
		// the pre-ENG-539 behavior is preserved rather than failing the migration.
		hostRoot := filepath.Join(t.TempDir(), "does-not-exist")
		src, err := resolveMigratedBindSource(hostRoot, "data")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(hostRoot, "data"), src)
	})

	t.Run("leaf symlink escaping the volume root is rejected", func(t *testing.T) {
		hostRoot := t.TempDir()
		outside := t.TempDir()
		require.NoError(t, os.Symlink(outside, filepath.Join(hostRoot, "data")))
		_, err := resolveMigratedBindSource(hostRoot, "data")
		require.Error(t, err, "a migrated target that is a symlink out of the volume root must be refused")
	})

	t.Run("intermediate symlink escaping the volume root is rejected", func(t *testing.T) {
		hostRoot := t.TempDir()
		outside := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(outside, "sub"), 0o700))
		require.NoError(t, os.Symlink(outside, filepath.Join(hostRoot, "esc")))
		_, err := resolveMigratedBindSource(hostRoot, "esc/sub")
		require.Error(t, err, "a migrated target traversing an escaping symlink must be refused")
	})
}

// TestRecoverState_MigratesLegacyContainer: a managed container with
// fred.lease_uuid but no fred.service_name is recreated as a stack-form
// container named fred-{uuid}-app-0; the volume directory is renamed; the
// release store gets a wrapped manifest entry.
//
// RED for Task 1: production recoverState does not yet trigger migration,
// so the post-recover asserts (volume renamed, compose project up, wrapped
// release stored) all fail. Tasks 8-9 wire the migration and turn this
// green.
func TestRecoverState_MigratesLegacyContainer(t *testing.T) {
	b, fakeDocker, fakeVolumeBackend, fakeRelStore := newMigrationTestBackend(t)
	profile := b.cfg.SKUProfiles["docker-micro"]
	profile.DiskMB = 0
	b.cfg.SKUProfiles["docker-micro"] = profile
	b.cfg.ContainerTmpfsSizeMB = 73
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "provider.example",
		Entrypoint:     "websecure",
	}

	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		CustomDomain:  "legacy.example",
		FailCount:     3,
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision?trace=keep",
		InstanceIndex: 0,
		// ServiceName empty: legacy
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeRelStore.releases["lease-1"] = []byte(
		`{"image":"nginx:1.25","ports":{"80/tcp":{}}}`,
	)
	fakeRelStore.Seed(t) // flush the test-side releases map into the backing store

	if err := b.recoverState(context.Background()); err != nil {
		t.Fatalf("recoverState failed: %v", err)
	}

	if !fakeVolumeBackend.renamed("fred-lease-1-0", "fred-lease-1-app-0") {
		t.Fatalf("volume not renamed: %v", fakeVolumeBackend.renames)
	}
	if !strings.Contains(fakeDocker.lastComposeProjectName, "fred-lease-1") {
		t.Fatalf("compose up not invoked for project: %v", fakeDocker.lastComposeProjectName)
	}
	require.NotNil(t, fakeDocker.lastComposeProject)
	require.Contains(t, fakeDocker.lastComposeProject.Services, "app")
	labels := fakeDocker.lastComposeProject.Services["app"].Labels
	assert.Equal(t,
		"https://fred.example/callbacks/provision?trace=keep",
		labels[LabelCallbackURL],
	)
	assert.Equal(t,
		"https://fred.example/callbacks/provision?trace=keep",
		labels[LabelLifecycleCallbackURL],
		"v0.13 migration must preserve its tokenless observation route")
	assert.Equal(t, nominalDockerProviderUUID, labels[LabelProviderUUID])
	assert.Equal(t, b.cfg.Name, labels[LabelBackendName])
	assert.Equal(t, "3", labels[LabelFailCount])
	assert.Equal(t, "legacy.example", labels[LabelCustomDomain])
	if !fakeRelStore.hasWrappedRelease("lease-1") {
		t.Fatalf("release store missing wrapped entry")
	}
	latest, err := fakeRelStore.Store.LatestActive("lease-1")
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 1, ServiceName: "app", CustomDomain: "legacy.example",
	}}, latest.Items, "migration must durably record the exact desired cohort")
	require.Equal(t, []shared.SKUResourceSnapshot{{
		SKU: "docker-micro", CPUCores: 0.25, MemoryMB: 256, ScratchDiskMB: 73,
	}}, latest.ResourceProfiles,
		"the one-time v0.13 migration must freeze diskless scratch authority")
	require.NotNil(t, latest.LegacyRuntimeAuthority)
	assert.Equal(t, nominalDockerProviderUUID, latest.LegacyRuntimeAuthority.ProviderUUID())
	assert.Equal(t, "https://fred.example/callbacks/provision?trace=keep",
		latest.LegacyRuntimeAuthority.CallbackURL())
}

func TestExecuteLegacyMigrationAtomicallyPersistsRuntimeAuthority(t *testing.T) {
	b, fakeDocker, _, _ := newMigrationTestBackend(t)
	dbPath := filepath.Join(t.TempDir(), "atomic-migration-releases.db")
	store, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	b.releaseStore = store

	const (
		leaseUUID   = "33333333-3333-4333-8333-333333333333"
		callbackURL = "https://fred.example/callbacks/provision?trace=legacy"
	)
	legacy := ContainerInfo{
		ContainerID:   "legacy-atomic-cid",
		Name:          "fred-33333333-3333-4333-8333-333333333333-0",
		LeaseUUID:     leaseUUID,
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   callbackURL,
		InstanceIndex: 0,
	}
	fakeDocker.containers = []ContainerInfo{legacy}
	migration := &legacyMigration{
		LeaseUUID:    leaseUUID,
		Tenant:       legacy.Tenant,
		ProviderUUID: legacy.ProviderUUID,
		SKU:          legacy.SKU,
		Stack: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			manifest.DefaultServiceName: {Image: legacy.Image},
		}},
		Instances: []legacyMigrationInstance{{
			LegacyContainer:  legacy,
			NewContainerName: "fred-33333333-3333-4333-8333-333333333333-app-0",
			PrevName:         "fred-33333333-3333-4333-8333-333333333333-app-0-prev",
		}},
	}

	require.NoError(t, b.executeLegacyMigration(context.Background(), migration, slog.Default()))
	active, err := store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NotNil(t, active.LegacyRuntimeAuthority,
		"the migration commit itself must carry zero-survivor callback authority")
	assert.Equal(t, callbackURL, active.LegacyRuntimeAuthority.CallbackURL())
	assert.Equal(t, callbackURL, active.LegacyRuntimeAuthority.LifecycleCallbackURL())

	require.NoError(t, store.Close())
	reopened, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer reopened.Close()
	active, err = reopened.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NotNil(t, active.LegacyRuntimeAuthority)
	assert.Equal(t, callbackURL, active.LegacyRuntimeAuthority.CallbackURL())
}

func TestResolveLegacyMigrationCallbackURLs(t *testing.T) {
	const (
		operationID  = "550e8400-e29b-41d4-a716-446655440000"
		operationURL = "https://fred.example/callbacks/provision?trace=one%20two&operation_id=" + operationID + "&path=%2Fdata"
		lifecycleURL = "https://fred.example/callbacks/provision?trace=one%20two&lifecycle_id=" + operationID + "&path=%2Fdata"
	)

	t.Run("derives missing legacy lifecycle route without rewriting operation URL", func(t *testing.T) {
		gotOperation, gotLifecycle, err := resolveLegacyMigrationCallbackURLs([]legacyMigrationInstance{{
			LegacyContainer: ContainerInfo{InstanceIndex: 0, CallbackURL: operationURL},
		}})
		require.NoError(t, err)
		assert.Equal(t, operationURL, gotOperation)
		assert.Equal(t, lifecycleURL, gotLifecycle)
	})

	t.Run("keeps a v0.13 operationless route tokenless", func(t *testing.T) {
		const legacyURL = "https://fred.example/callbacks/provision?trace=keep"
		gotOperation, gotLifecycle, err := resolveLegacyMigrationCallbackURLs([]legacyMigrationInstance{{
			LegacyContainer: ContainerInfo{InstanceIndex: 0, CallbackURL: legacyURL},
		}})
		require.NoError(t, err)
		assert.Equal(t, legacyURL, gotOperation)
		assert.Equal(t, legacyURL, gotLifecycle)
	})

	t.Run("preserves a valid explicit lifecycle route and accepts mixed old labels", func(t *testing.T) {
		gotOperation, gotLifecycle, err := resolveLegacyMigrationCallbackURLs([]legacyMigrationInstance{
			{LegacyContainer: ContainerInfo{
				InstanceIndex:        0,
				CallbackURL:          operationURL,
				LifecycleCallbackURL: lifecycleURL,
			}},
			{LegacyContainer: ContainerInfo{
				InstanceIndex: 1,
				CallbackURL:   operationURL,
			}},
		})
		require.NoError(t, err)
		assert.Equal(t, operationURL, gotOperation)
		assert.Equal(t, lifecycleURL, gotLifecycle)
	})

	t.Run("rejects mismatched sibling completion authority", func(t *testing.T) {
		_, _, err := resolveLegacyMigrationCallbackURLs([]legacyMigrationInstance{
			{LegacyContainer: ContainerInfo{InstanceIndex: 0, CallbackURL: operationURL}},
			{LegacyContainer: ContainerInfo{
				InstanceIndex: 1,
				CallbackURL:   "https://other.example/callbacks/provision?operation_id=" + operationID,
			}},
		})
		require.ErrorContains(t, err, "callback_url differs")
	})

	t.Run("rejects an unrelated explicit lifecycle route", func(t *testing.T) {
		_, _, err := resolveLegacyMigrationCallbackURLs([]legacyMigrationInstance{{
			LegacyContainer: ContainerInfo{
				InstanceIndex:        0,
				CallbackURL:          operationURL,
				LifecycleCallbackURL: "https://attacker.example/callbacks/provision?lifecycle_id=" + operationID,
			},
		}})
		require.ErrorContains(t, err, "lifecycle URL must exactly match")
	})
}

func TestRecoverState_MigrationRejectsInvalidCallbackPairBeforeMutation(t *testing.T) {
	b, fakeDocker, fakeVolumeBackend, fakeRelStore := newMigrationTestBackend(t)
	const operationID = "550e8400-e29b-41d4-a716-446655440000"
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:          "legacy-cid",
		LeaseUUID:            "lease-1",
		Tenant:               "tenant-a",
		SKU:                  "docker-micro",
		CallbackURL:          "https://fred.example/callbacks/provision?operation_id=" + operationID,
		LifecycleCallbackURL: "https://attacker.example/callbacks/provision?lifecycle_id=" + operationID,
		InstanceIndex:        0,
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.ErrorContains(t, err, "resolve legacy callback routes")
	assert.Empty(t, fakeVolumeBackend.renames,
		"callback authority must be validated before any durable volume rename")
	assert.Nil(t, fakeDocker.lastComposeProject,
		"invalid callback authority must be rejected before Compose mutation")
}

func TestRecoverState_MigrationRefusesStopErrorWhileContainerStillRunning(t *testing.T) {
	b, fakeDocker, fakeVolumes, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		Name:          "fred-lease-1-0",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
		Status:        "running",
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeDocker.stopContainer = func(_ context.Context, containerID string, _ time.Duration) error {
		assert.Equal(t, "legacy-cid", containerID)
		return errors.New("transient Docker stop failure")
	}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.ErrorContains(t, err, "remains in non-quiescent state")
	assert.Empty(t, fakeVolumes.renames,
		"a failed Stop followed by a still-running exact-ID inspect must block volume movement")
	assert.Nil(t, fakeDocker.lastComposeProject,
		"a still-running legacy container must block its replacement generation")
}

func TestRecoverState_MigrationRefusesRunningPrevRetry(t *testing.T) {
	b, fakeDocker, fakeVolumes, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-prev-cid",
		Name:          "fred-lease-1-app-0-prev",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
		Status:        "running",
	}}
	fakeDocker.mounts["legacy-prev-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	stopCalls := 0
	fakeDocker.stopContainer = func(_ context.Context, containerID string, _ time.Duration) error {
		stopCalls++
		assert.Equal(t, "legacy-prev-cid", containerID)
		// A nil Docker response is still not proof. Keep the inspect state
		// running to model a stale/incorrect daemon acknowledgement.
		return nil
	}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.ErrorContains(t, err, "remains in non-quiescent state")
	assert.Equal(t, 1, stopCalls, "an already-named -prev retry must still issue Stop")
	assert.Empty(t, fakeVolumes.renames,
		"a running -prev retry must not advance to volume movement")
	assert.Nil(t, fakeDocker.lastComposeProject,
		"a running -prev retry must not create the replacement cohort")
}

// TestRecoverState_MigrationFailure_AbortsStartup: fred must refuse to start
// if any legacy container fails to migrate, with the lease UUID surfaced in
// the error so operators can locate it.
func TestRecoverState_MigrationFailure_AbortsStartup(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
	}}
	fakeDocker.composeUpErr = errors.New("compose up failed")
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	if err == nil {
		t.Fatalf("expected recoverState to fail when migration fails")
	}
	require.ErrorContains(t, err, "compose up failed")
	if !strings.Contains(err.Error(), "lease-1") {
		t.Fatalf("expected error to identify lease, got: %v", err)
	}
}

func TestRecoverState_BoundsWholeLegacyMigration(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	b.cfg.ProvisionTimeout = 20 * time.Millisecond
	b.cfg.MigrationReadyTimeout = 10 * time.Millisecond
	b.compose = &mockComposeExecutor{UpFn: func(
		ctx context.Context,
		_ *composetypes.Project,
		_ composeUpOpts,
	) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		Name:          "fred-lease-1-0",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
	}}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	started := time.Now()
	err := b.recoverState(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second,
		"a stalled migration mutation must not wedge startup for process lifetime")
}

// TestPlanLegacyMigration_RejectsMultiServiceStack: a legacy-labeled
// container whose release-store entry is stack-shaped with multiple
// services is unproducible state (legacy writers only emitted flat
// manifests). The planner must fail loudly rather than letting the
// executor silently pick "app" and ignore the rest.
func TestPlanLegacyMigration_RejectsMultiServiceStack(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID: "legacy-cid",
		LeaseUUID:   "lease-1",
		SKU:         "docker-micro",
	}}
	fakeRelStore.releases["lease-1"] = []byte(
		`{"services":{"app":{"image":"nginx:1.25"},"sidecar":{"image":"redis:7"}}}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has 2 services")
	assert.Contains(t, err.Error(), "lease-1")
}

// TestPlanLegacyMigration_RejectsWrongServiceName: a stack-shaped
// release with a single service whose name is not the synthetic
// "app" wrap-target is also unproducible state — the executor and
// buildComposeProject downstream assume manifest.DefaultServiceName.
func TestPlanLegacyMigration_RejectsWrongServiceName(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID: "legacy-cid",
		LeaseUUID:   "lease-1",
		SKU:         "docker-micro",
	}}
	fakeRelStore.releases["lease-1"] = []byte(
		`{"services":{"web":{"image":"nginx:1.25"}}}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `with service "web"`)
	assert.Contains(t, err.Error(), `expected "app"`)
	assert.Contains(t, err.Error(), "lease-1")
}

func TestPlanLegacyMigration_RejectsSparseInstanceIndexes(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	for _, index := range []int{1, 2} {
		fakeDocker.containers = append(fakeDocker.containers, ContainerInfo{
			ContainerID:   fmt.Sprintf("legacy-cid-%d", index),
			Name:          fmt.Sprintf("fred-lease-1-%d", index),
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "provider-a",
			SKU:           "docker-micro",
			Image:         "nginx:1.25",
			InstanceIndex: index,
		})
	}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.ErrorContains(t, err, "non-contiguous instance indexes")
	assert.Contains(t, err.Error(), "missing 0")
	assert.Nil(t, fakeDocker.lastComposeProject,
		"sparse topology must fail before any migration mutation")
}

func TestPlanLegacyMigration_RejectsDivergentCustomDomains(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	for index, customDomain := range []string{"legacy.example", "other.example"} {
		fakeDocker.containers = append(fakeDocker.containers, ContainerInfo{
			ContainerID:   fmt.Sprintf("legacy-cid-%d", index),
			Name:          fmt.Sprintf("fred-lease-1-%d", index),
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "provider-a",
			SKU:           "docker-micro",
			CustomDomain:  customDomain,
			Image:         "nginx:1.25",
			InstanceIndex: index,
		})
	}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	require.ErrorContains(t, err, "divergent lease, tenant, provider, SKU, or custom-domain identity")
	assert.Nil(t, fakeDocker.lastComposeProject,
		"ambiguous domain authority must fail before any migration mutation")
}

// TestRecoverState_SkipsPrevRemnants proves both halves of resumability: the
// migration pre-pass consumes a "-prev" rollback artifact and idempotently
// reconverges/persists the stack, while the main recovery projection still
// excludes that artifact so it cannot inflate Quantity or append an unnamed
// LeaseItem during the inspection grace window.
func TestRecoverState_SkipsPrevRemnants(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	releaseItems := []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 1, ServiceName: "app",
	}}
	require.NoError(t, fakeRelStore.Store.Append("lease-1", shared.Release{
		Manifest:         []byte(`{"image":"nginx:1.25"}`),
		Items:            releaseItems,
		ResourceProfiles: testResourceProfiles(t, releaseItems),
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	fakeDocker.containers = []ContainerInfo{
		{
			// A healthy migrated container — the normal stack-form shape.
			ContainerID:   "app-cid",
			Name:          "fred-lease-1-app-0",
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			SKU:           "docker-micro",
			ProviderUUID:  nominalDockerProviderUUID,
			ServiceName:   "app",
			InstanceIndex: 0,
			Image:         "nginx:1.25",
			CallbackURL:   "https://fred.example/callbacks/provision",
			Status:        "running",
		},
		{
			// A -prev remnant for the same lease: same lease_uuid label,
			// no service_name, name ends in "-prev". The main loop must
			// skip it. Without the skip, prov.Quantity would tick to 2 and
			// a LeaseItem{ServiceName:""} would land in prov.Items.
			ContainerID:   "prev-cid",
			Name:          "fred-lease-1-app-0-prev",
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  nominalDockerProviderUUID,
			SKU:           "docker-micro",
			InstanceIndex: 0,
			Image:         "nginx:1.25",
			CallbackURL:   "https://fred.example/callbacks/provision",
			Status:        "exited",
		},
	}

	require.NoError(t, b.recoverState(context.Background()))

	prov, ok := b.provisions["lease-1"]
	require.True(t, ok, "lease-1 should be recovered")
	assert.Equal(t, 1, prov.Quantity, "prov.Quantity must not count the -prev remnant")
	require.Len(t, prov.Items, 1, "exactly one LeaseItem for the stack-form service")
	assert.Equal(t, "app", prov.Items[0].ServiceName)
	assert.NotContains(t, prov.ContainerIDs, "prev-cid", "ContainerIDs must not include the -prev remnant")
}

// A migration release is committed before rollback-window cleanup begins. If
// cleanup removes only one of several `-prev` containers and the process then
// restarts, the surviving subset must never be mistaken for desired topology:
// doing so would run Compose at the smaller quantity and downscale a healthy
// tenant workload. Exact release Items make the remnants cleanup-only.
func TestRecoverState_DurableMigrationPartialCleanupDoesNotReinferTopology(t *testing.T) {
	b, fakeDocker, fakeVolumes, fakeRelStore := newMigrationTestBackend(t)
	b.cfg.MigrationGracePeriod = 20 * time.Millisecond
	releaseItems := []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 2, ServiceName: "app",
	}}
	require.NoError(t, fakeRelStore.Store.Append("lease-1", shared.Release{
		Manifest:         []byte(`{"image":"nginx:1.25"}`),
		Items:            releaseItems,
		ResourceProfiles: testResourceProfiles(t, releaseItems),
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	for index := range 2 {
		fakeDocker.containers = append(fakeDocker.containers, ContainerInfo{
			ContainerID:   fmt.Sprintf("app-cid-%d", index),
			Name:          fmt.Sprintf("fred-lease-1-app-%d", index),
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "provider-a",
			SKU:           "docker-micro",
			ServiceName:   "app",
			InstanceIndex: index,
			Image:         "nginx:1.25",
			Status:        "running",
		})
	}
	// Index 0's rollback container was already removed before the crash; only
	// index 1 remains.
	fakeDocker.containers = append(fakeDocker.containers, ContainerInfo{
		ContainerID:   "prev-cid-1",
		Name:          "fred-lease-1-app-1-prev",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  "provider-a",
		SKU:           "docker-micro",
		InstanceIndex: 1,
		Image:         "nginx:1.25",
		Status:        "exited",
	})

	var mu sync.Mutex
	var removed []string
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		mu.Lock()
		defer mu.Unlock()
		removed = append(removed, containerID)
		return nil
	}

	require.NoError(t, b.recoverState(context.Background()))
	assert.Nil(t, fakeDocker.lastComposeProject,
		"durably committed rollback remnants must not trigger Compose Up")
	assert.Empty(t, fakeVolumes.renames,
		"durably committed rollback remnants must not trigger volume migration")
	require.NotNil(t, b.provisions["lease-1"])
	assert.Equal(t, 2, b.provisions["lease-1"].Quantity)
	assert.Equal(t, releaseItems, b.provisions["lease-1"].Items)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Equal(removed, []string{"prev-cid-1"})
	}, time.Second, 10*time.Millisecond)
}

// A crash after RecordLegacyMigration commits but after every target container
// disappears can leave only the stopped `-prev` rollback cohort. Those remnants
// are cleanup evidence, never live-runtime or sizing authority: recovery must
// reconstruct the complete conservative projection from the reopened Release
// and retain the rollback cohort while the target generation is divergent.
func TestRecoverState_CommittedLegacyMigrationWithZeroTargetSurvivorsKeepsRollbackRemnants(t *testing.T) {
	b, fakeDocker, fakeVolumes, _ := newMigrationTestBackend(t)
	b.cfg.MigrationGracePeriod = 20 * time.Millisecond

	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		tenant       = "tenant-a"
		providerUUID = nominalDockerProviderUUID
		callbackURL  = "https://fred.example/callbacks/provision?route=v013"
	)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	authority, err := shared.NewLegacyRuntimeAuthority(
		tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	items := []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 2, ServiceName: "app",
	}}
	profiles := testResourceProfiles(t, items)
	manifestBytes := []byte(`{"services":{"app":{"image":"nginx:1.25"}}}`)
	createdAt := time.Now().Add(-time.Hour).UTC()

	// Cross a real bbolt close/reopen boundary so no process-local migration
	// state can contribute to the recovered projection or allocation.
	releasePath := filepath.Join(t.TempDir(), "committed-migration-releases.db")
	writer, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	require.NoError(t, writer.RecordLegacyMigrationAt(
		leaseUUID, manifestBytes, items, profiles, authority, createdAt,
	))
	require.NoError(t, writer.Close())
	reopened, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	b.releaseStore = reopened
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})

	// Both exact rollback containers survived, but the committed target cohort
	// has zero survivors. A migration replay or grace cleanup here would erase the
	// only rollback evidence while the current generation is known divergent.
	for index := range 2 {
		fakeDocker.containers = append(fakeDocker.containers, ContainerInfo{
			ContainerID:          fmt.Sprintf("prev-cid-%d", index),
			Name:                 fmt.Sprintf("fred-%s-app-%d-prev", leaseUUID, index),
			LeaseUUID:            leaseUUID,
			Tenant:               tenant,
			ProviderUUID:         providerUUID,
			BackendName:          b.Name(),
			SKU:                  "docker-micro",
			InstanceIndex:        index,
			Image:                "nginx:1.25",
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
			Status:               "exited",
			CreatedAt:            createdAt,
		})
	}

	var mu sync.Mutex
	var removed []string
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		mu.Lock()
		defer mu.Unlock()
		removed = append(removed, containerID)
		return nil
	}

	require.NoError(t, b.recoverState(t.Context()))
	assert.Nil(t, fakeDocker.lastComposeProject,
		"a committed migration must not be replayed from rollback remnants")
	assert.Empty(t, fakeVolumes.renames,
		"rollback remnants must not trigger volume migration")

	b.provisionsMu.RLock()
	recovered := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
	assert.Equal(t, tenant, recovered.Tenant)
	assert.Equal(t, providerUUID, recovered.ProviderUUID)
	assert.Equal(t, callbackURL, recovered.CallbackURL)
	assert.Equal(t, lifecycleCallbackURL, recovered.LifecycleCallbackURL)
	assert.Equal(t, items, recovered.Items)
	assert.Equal(t, 2, recovered.Quantity)
	assert.Empty(t, recovered.ContainerIDs,
		"rollback remnants are not members of the recovered live generation")
	assert.Equal(t, backend.ReasonInternal, recovered.Reason)
	assert.Contains(t, recovered.LastError, "found 0 containers, expected 2")

	for index := range 2 {
		allocation := b.pool.GetAllocation(fmt.Sprintf("%s-app-%d", leaseUUID, index))
		require.NotNil(t, allocation, "release-owned instance %d must remain fully reserved", index)
		assert.Equal(t, tenant, allocation.Tenant)
		assert.Equal(t, profiles[0].CPUCores, allocation.CPUCores)
		assert.Equal(t, profiles[0].MemoryMB, allocation.MemoryMB)
		assert.Equal(t, profiles[0].DiskMB, allocation.DiskMB)
	}

	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(removed) != 0
	}, 5*b.cfg.MigrationGracePeriod, 5*time.Millisecond,
		"divergent target cohort must retain exact stopped rollback remnants")

	active, err := reopened.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.True(t, active.LegacyMigration)
	assert.Equal(t, items, active.Items)
	assert.Equal(t, profiles, active.ResourceProfiles)
	require.NotNil(t, active.LegacyRuntimeAuthority)
	assert.Equal(t, authority, *active.LegacyRuntimeAuthority)
}

// TestExecuteLegacyMigration_EnsuresTenantNetwork: when network
// isolation is enabled (the production default), the migration
// executor must call EnsureTenantNetwork before bringing the new
// Compose project up — otherwise the migrated containers come up
// off-network and Traefik / inter-container DNS break immediately.
// Mirrors the doProvision flow.
func TestExecuteLegacyMigration_EnsuresTenantNetwork(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	// Opt the test backend into network isolation; newBackendForTest
	// defaults to off so unrelated tests don't need network stubs.
	b.cfg.NetworkIsolation = ptrBool(true)

	var calls []string
	fakeDocker.ensureTenantNetwork = func(_ context.Context, tenant string) (string, error) {
		calls = append(calls, tenant)
		return "net-id-" + tenant, nil
	}

	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		Name:          "fred-lease-1-0",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	require.NoError(t, b.recoverState(context.Background()))

	require.Len(t, calls, 1, "EnsureTenantNetwork must be called exactly once during migration")
	assert.Equal(t, "tenant-a", calls[0])
}

// TestExecuteLegacyMigration_PrevCleanupSurvivesCallerContextCancel pins
// ENG-592: the docker-backend runs Start under a 30s context that main cancels
// the moment Start returns. Migration's background `-prev` grace-removal
// goroutine must therefore be scoped to the backend lifecycle context
// (b.stopCtx), NOT the caller's short startup context — otherwise every startup
// migration permanently leaks its `-prev` containers because the grace goroutine
// sees ctx.Done() fire at ~0s. This test cancels the caller ctx right after the
// migration returns and asserts the `-prev` container is still removed once the
// grace window elapses.
func TestExecuteLegacyMigration_PrevCleanupSurvivesCallerContextCancel(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	// Grace long enough that we cancel the caller ctx well before it fires, so
	// the test distinguishes "keyed on caller ctx" (leak) from "keyed on
	// b.stopCtx" (cleaned up).
	b.cfg.MigrationGracePeriod = 750 * time.Millisecond

	var mu sync.Mutex
	var removed []string
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		mu.Lock()
		removed = append(removed, containerID)
		mu.Unlock()
		return nil
	}

	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		Name:          "fred-lease-1-0",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  nominalDockerProviderUUID,
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		CallbackURL:   "https://fred.example/callbacks/provision",
		InstanceIndex: 0,
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	// Recover (and migrate) under a caller context that we cancel immediately
	// afterwards — exactly what cmd/docker-backend/main.go does to the 30s Start
	// context once Start returns.
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	require.NoError(t, b.recoverState(callerCtx))
	cancelCaller()

	// The grace goroutine (spawned during migration) must still remove the
	// `-prev` container after the grace window, despite the caller ctx being
	// canceled.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, containerID := range removed {
			if containerID == "legacy-cid" {
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond,
		"migration -prev cleanup must survive caller-context cancellation (got removals: leaked -prev container)")
}

func TestScheduleLegacyPrevCleanup_RemovesImmutableContainerID(t *testing.T) {
	b, fakeDocker, _, _ := newMigrationTestBackend(t)
	b.cfg.MigrationGracePeriod = 10 * time.Millisecond

	removed := make(chan string, 1)
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		removed <- containerID
		return nil
	}
	b.scheduleLegacyPrevCleanup([]legacyRollbackCleanupTarget{{
		ContainerID: "immutable-old-id",
		Name:        "fred-lease-1-app-0-prev",
	}}, b.logger)

	select {
	case got := <-removed:
		assert.Equal(t, "immutable-old-id", got,
			"a replacement that reuses the delayed name must never be targeted")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rollback cleanup")
	}
}

func TestRemoveCommittedLegacyRollbackRemnants_DoesNotDeleteReusedName(t *testing.T) {
	b, fakeDocker, _, releases := newMigrationTestBackend(t)
	const leaseUUID = "lease-1"
	items := []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 1, ServiceName: "app",
	}}
	releaseProfiles := testResourceProfiles(t, items)
	require.NoError(t, releases.Store.Append(leaseUUID, shared.Release{
		Manifest:         []byte(`{"services":{"app":{"image":"busybox"}}}`),
		Image:            "stack",
		Items:            items,
		ResourceProfiles: releaseProfiles,
		Status:           "active",
		CreatedAt:        time.Now(),
		LegacyMigration:  true,
	}))

	// The original rollback remnant has already been removed. A later, unrelated
	// managed container acquired the reusable Docker name but carries a different
	// lease identity. Cleanup must inventory and re-attest the labels, then leave
	// this replacement untouched rather than deleting by name.
	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "replacement-id",
		Name:          "fred-" + leaseUUID + "-app-0-prev",
		LeaseUUID:     "different-lease",
		BackendName:   b.Name(),
		ServiceName:   "",
		InstanceIndex: 0,
		Status:        "exited",
	}}
	var removed []string
	fakeDocker.removeContainer = func(_ context.Context, containerID string) error {
		removed = append(removed, containerID)
		return nil
	}

	require.NoError(t, b.removeCommittedLegacyRollbackRemnants(
		context.Background(), leaseUUID, b.logger,
	))
	assert.Empty(t, removed, "a replacement that only reuses the old name is not migration authority")
}

// TestFilterManagedMounts_SeparatorBoundary verifies that the prefix
// check uses a separator-terminated root so sibling directories whose
// names happen to begin with the configured volume_data_path are NOT
// misclassified as managed.
//
// Example failure (without the fix): root=/var/lib/fred would match
// /var/lib/fred-other/.../ — flagged as managed and renamed under
// migration. This test exists to pin the fix Task 16 lands.
func TestFilterManagedMounts_SeparatorBoundary(t *testing.T) {
	root := "/var/lib/fred"
	b := &Backend{cfg: Config{VolumeDataPath: root}}

	tests := []struct {
		name   string
		source string
		typ    string
		want   bool
	}{
		{"managed bind directly under root", filepath.Join(root, "vol-1"), "bind", true},
		{"managed bind deep under root", filepath.Join(root, "sub", "vol-1"), "bind", true},
		{"sibling prefix not managed", "/var/lib/fred-other/vol-1", "bind", false},
		{"sibling-prefix exact-name not managed", "/var/lib/fred-extra", "bind", false},
		{"tmpfs filtered regardless of source", filepath.Join(root, "vol-1"), "tmpfs", false},
		{"unrelated bind filtered", "/etc/localtime", "bind", false},
		{"exact root path managed", root, "bind", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mounts := []ContainerMount{{Type: tc.typ, Source: tc.source}}
			got := filterManagedMounts(b, mounts)
			if tc.want {
				require.Len(t, got, 1, "mount should be classified as managed")
				assert.Equal(t, tc.source, got[0].Source)
			} else {
				assert.Empty(t, got, "mount should NOT be classified as managed")
			}
		})
	}
}

// TestFilterManagedMounts_EmptyRoot covers the early-return path:
// when VolumeDataPath is unconfigured, every bind mount is treated
// as unmanaged (no false-positive renames possible).
func TestFilterManagedMounts_EmptyRoot(t *testing.T) {
	b := &Backend{cfg: Config{VolumeDataPath: ""}}
	mounts := []ContainerMount{
		{Type: "bind", Source: "/var/lib/fred/vol-1"},
		{Type: "bind", Source: "/some/other/path"},
	}
	assert.Nil(t, filterManagedMounts(b, mounts),
		"empty VolumeDataPath must return nil — no mount is managed")
}
