//go:build integration

package e2e

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

// requireImage pulls an image the daemon does not have yet. ContainerCreate
// never pulls, and a test that creates containers itself must not depend on
// an earlier test or a CI step having fetched the image.
func requireImage(t *testing.T, docker *client.Client, ref string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := docker.ImageInspect(ctx, ref)
	if err == nil {
		return
	}
	require.True(t, errdefs.IsNotFound(err), "inspect %s: %v", ref, err)
	progress, err := docker.ImagePull(ctx, ref, image.PullOptions{})
	require.NoError(t, err)
	defer func() { _ = progress.Close() }()
	// The pull runs as its progress stream is read, and reports a failure in
	// that stream, so the image is looked up again afterwards.
	_, err = io.Copy(io.Discard, progress)
	require.NoError(t, err)
	_, err = docker.ImageInspect(ctx, ref)
	require.NoError(t, err, "pull %s", ref)
}

// TestSweepDockerResources checks the cleanup every fixture and TestMain rely
// on. It must remove both kinds of container a backend creates, and nothing
// of a backend the caller did not name. A helper it missed would outlive the
// scratch root holding the only record that could recover it.
func TestSweepDockerResources(t *testing.T) {
	docker := dockerClient(t, DockerSockets()[0])
	ctx := t.Context()
	requireImage(t, docker, BusyboxImage)
	swept := backendNamePrefix + randomHex(t, 4)
	kept := backendNamePrefix + randomHex(t, 4)
	create := func(labels map[string]string) string {
		created, err := docker.ContainerCreate(ctx,
			&container.Config{Image: BusyboxImage, Labels: labels}, nil, nil, nil, "")
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true})
		})
		return created.ID
	}
	managed := create(map[string]string{labelManaged: "true", labelBackendName: swept})
	helper := create(map[string]string{labelInspectionSchema: "1", labelInspectionBackend: swept})
	keptManaged := create(map[string]string{labelManaged: "true", labelBackendName: kept})
	keptHelper := create(map[string]string{labelInspectionSchema: "1", labelInspectionBackend: kept})

	require.NoError(t, sweepDockerResources(ctx, docker, func(name string) bool { return name == swept }))

	for _, id := range []string{managed, helper} {
		_, err := docker.ContainerInspect(ctx, id)
		require.True(t, errdefs.IsNotFound(err), "container %s must be removed, inspect returned: %v", id, err)
	}
	for _, id := range []string{keptManaged, keptHelper} {
		_, err := docker.ContainerInspect(ctx, id)
		require.NoError(t, err, "container %s belongs to another backend and must be kept", id)
	}
}
