//go:build integration

package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// TestIntegration_Docker_TenantLinkedSourceIsNotAnotherLeasesVolumeWriter runs
// ENG-2181 against a real daemon. The unit tests assume two daemon behaviors;
// this test observes both:
//
//   - the runtime mounts a bind source whose intermediate components are
//     in-volume links, so a tenant can obtain such a container; and
//   - the daemon keeps reporting that source as the string it was created
//     with, so the links the running workload re-points are what a later
//     launch on the same daemon would resolve.
//
// It then classifies the daemon's own observation the way another lease's
// launch does.
func TestIntegration_Docker_TenantLinkedSourceIsNotAnotherLeasesVolumeWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)
	sdk := newImageSecurityFixtureClient(t)
	require.NoError(t, pullImageForTest(t, docker, ctx, "busybox:latest", 60*time.Second))

	// An earlier deploy of the lease left data/a -> b/l and data/b/l -> . in
	// its volume. The next deploy declares VOLUME /data/b and VOLUME /data/a/c,
	// and bind construction creates the second leaf through the links.
	storage := t.TempDir()
	lease := uuid.NewString()
	source := tenantLinkedVolumeSource(t, storage, lease, ".", "c")
	volume := filepath.Join(storage, canonicalVolumeName(lease, "app", 0))
	require.NoError(t, os.Mkdir(source, 0o700))
	leaf, err := os.Lstat(filepath.Join(volume, "data", "b", "c"))
	require.NoError(t, err, "the leaf exists only through the tenant's links")
	require.True(t, leaf.IsDir())

	created, err := sdk.ContainerCreate(ctx,
		&container.Config{
			Image:  "busybox:latest",
			Cmd:    []string{"sleep", "3600"},
			Labels: map[string]string{LabelManaged: "true", LabelLeaseUUID: lease},
		},
		&container.HostConfig{
			// The workload needs no network, so it does not depend on the
			// daemon's default bridge (attaching to it failed on both CI legs).
			NetworkMode: "none",
			Binds: []string{
				filepath.Join(volume, "data", "b") + ":/data/b",
				source + ":/data/a/c",
			},
		}, nil, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = sdk.ContainerRemove(cctx, created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
	})
	require.NoError(t, sdk.ContainerStart(ctx, created.ID, container.StartOptions{}),
		"the runtime accepts a bind source that passes through in-volume links")

	// The running workload re-points its own link into a loop. Its mounts do
	// not move; only the path the daemon reports stops resolving.
	execInContainer(t, created.ID, []string{"ln", "-sfn", "l", "/data/b/l"})
	execInContainer(t, created.ID, []string{"touch", "/data/a/c/still-mounted"})
	require.FileExists(t, filepath.Join(volume, "data", "b", "c", "still-mounted"))
	_, err = filepath.EvalSymlinks(source)
	require.Error(t, err, "the reported source no longer resolves")

	writers, err := docker.ListVolumeWriters(ctx)
	require.NoError(t, err)
	var workload *ContainerInfo
	for index := range writers {
		if writers[index].ContainerID == created.ID {
			workload = &writers[index]
		}
	}
	require.NotNil(t, workload, "the inventory lists the workload")
	require.Equal(t, lease, workload.LeaseUUID)
	var reported []string
	for _, mount := range workload.Mounts {
		reported = append(reported, mount.Source)
	}
	require.Contains(t, reported, source, "the daemon reports the bind source unresolved")

	// Another lease's launch protects its own volume on the same daemon.
	other, err := fsidentity.OpenDirectory(t.TempDir())
	require.NoError(t, err)
	defer other.Close()
	volumes := map[string]protectedVolume{"other-lease": {root: other}}

	// Without the labels that say fred launched it, this observation is
	// exactly what failed every other launch before the fix.
	unattested := *workload
	unattested.LeaseUUID = ""
	_, err = protectedVolumesAffectWriter(volumes, unattested)
	require.Error(t, err)

	affected, err := protectedVolumesAffectWriter(volumes, *workload)
	require.NoError(t, err, "a link in one lease's volume must not fail another lease's launch")
	require.False(t, affected)
}
