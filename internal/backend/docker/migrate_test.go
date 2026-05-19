package docker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	fakeDocker.containers = []ContainerInfo{{
		ContainerID:   "legacy-cid",
		LeaseUUID:     "lease-1",
		Tenant:        "tenant-a",
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
		InstanceIndex: 0,
		// ServiceName empty: legacy
	}}
	fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
		Source: "/var/lib/fred/volumes/fred-lease-1-0/data",
		Target: "/data",
		Type:   "bind",
	}}
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
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
	if !fakeRelStore.hasWrappedRelease("lease-1") {
		t.Fatalf("release store missing wrapped entry")
	}
}

// TestRecoverState_MigrationFailure_AbortsStartup: fred must refuse to start
// if any legacy container fails to migrate, with the lease UUID surfaced in
// the error so operators can locate it.
func TestRecoverState_MigrationFailure_AbortsStartup(t *testing.T) {
	b, fakeDocker, _, fakeRelStore := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{{
		ContainerID: "legacy-cid",
		LeaseUUID:   "lease-1",
		SKU:         "docker-micro",
	}}
	fakeDocker.composeUpErr = errors.New("compose up failed")
	fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)
	fakeRelStore.Seed(t)

	err := b.recoverState(context.Background())
	if err == nil {
		t.Fatalf("expected recoverState to fail when migration fails")
	}
	if !strings.Contains(err.Error(), "lease-1") {
		t.Fatalf("expected error to identify lease, got: %v", err)
	}
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

// TestRecoverState_SkipsPrevRemnants: a managed container with a "-prev"
// suffix on its name is a transient migration rollback artifact — left
// behind when fred is interrupted between the rename step and the
// post-grace cleanup, or when recoverState runs while the grace timer
// is still pending. The main recovery loop must skip it; otherwise the
// legacy-single-item branch fires (the container has no service_name)
// and inflates prov.Quantity / appends a spurious LeaseItem.
func TestRecoverState_SkipsPrevRemnants(t *testing.T) {
	b, fakeDocker, _, _ := newMigrationTestBackend(t)
	fakeDocker.containers = []ContainerInfo{
		{
			// A healthy migrated container — the normal stack-form shape.
			ContainerID:   "app-cid",
			Name:          "fred-lease-1-app-0",
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			SKU:           "docker-micro",
			ServiceName:   "app",
			InstanceIndex: 0,
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
			SKU:           "docker-micro",
			InstanceIndex: 0,
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
		SKU:           "docker-micro",
		Image:         "nginx:1.25",
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
