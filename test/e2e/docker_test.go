//go:build integration

package e2e

import (
	"context"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
)

// TestSweepDockerResources checks the cleanup every fixture and TestMain rely
// on. It must remove both kinds of container a backend creates, and nothing
// of a backend the caller did not name. A helper it missed would outlive the
// scratch root holding the only record that could recover it.
func TestSweepDockerResources(t *testing.T) {
	docker := dockerClient(t)
	ctx := t.Context()
	swept := backendNamePrefix + randomHex(t, 4)
	kept := backendNamePrefix + randomHex(t, 4)
	create := func(labels map[string]string) string {
		created, err := docker.ContainerCreate(ctx,
			&container.Config{Image: "busybox:latest", Labels: labels}, nil, nil, nil, "")
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
