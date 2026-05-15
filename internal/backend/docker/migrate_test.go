package docker

import (
	"context"
	"errors"
	"strings"
	"testing"
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
		Source: "/var/lib/fred/volumes/fred-lease-1-0",
		Target: "/data",
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
