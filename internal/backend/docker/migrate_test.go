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
