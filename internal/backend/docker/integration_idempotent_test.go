//go:build integration

package docker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDockerClient returns a DockerClient wired to the real daemon, with a
// cleanup that removes any containers it was asked to leave behind.
func newTestDockerClient(t *testing.T) *DockerClient {
	t.Helper()
	d, err := NewDockerClient("", fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano()))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// baseCreateParams returns a minimal CreateContainerParams suitable for the
// real Docker daemon. Caller typically overrides LeaseUUID/Manifest.Image.
func baseCreateParams(leaseUUID, image string) CreateContainerParams {
	return CreateContainerParams{
		LeaseUUID:     leaseUUID,
		Tenant:        "test-tenant",
		ProviderUUID:  "test-provider",
		SKU:           "docker-micro",
		Manifest:      &DockerManifest{Image: image, Command: []string{"sleep", "3600"}},
		Profile:       SKUProfile{CPUCores: 0.1, MemoryMB: 32, DiskMB: 0},
		InstanceIndex: 0,
		FailCount:     0,
		BackendName:   "test",
		Quantity:      1,
	}
}

// forceRemove best-effort cleans up a raw container ID.
func forceRemove(t *testing.T, d *DockerClient, containerID string) {
	t.Helper()
	if containerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
		if !dockerclient.IsErrNotFound(err) {
			t.Logf("cleanup: failed to remove container %s: %v", containerID, err)
		}
	}
}

// TestIntegration_Docker_RemoveContainer_Concurrent verifies that concurrent
// RemoveContainer calls against the same container all return nil and leave
// the container physically gone. The new waitForContainerGone path in the
// "removal in progress" branch ensures no caller sees nil while a bind mount
// is still active.
func TestIntegration_Docker_RemoveContainer_Concurrent(t *testing.T) {
	d := newTestDockerClient(t)
	ctx := context.Background()

	// Pull a small image we can create and destroy quickly.
	require.NoError(t, d.PullImage(ctx, "busybox:latest", 60*time.Second))

	params := baseCreateParams(fmt.Sprintf("concurrent-%d", time.Now().UnixNano()), "busybox:latest")
	containerID, err := d.CreateContainer(ctx, params, 30*time.Second)
	require.NoError(t, err)
	defer forceRemove(t, d, containerID)

	require.NoError(t, d.StartContainer(ctx, containerID, 30*time.Second))

	const goroutines = 5
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	start := make(chan struct{})
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = d.RemoveContainer(ctx, containerID)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, e := range errs {
		assert.NoError(t, e, "goroutine %d: RemoveContainer should return nil (idempotent)", i)
	}

	// Container must actually be gone — not merely "removal acknowledged".
	_, inspectErr := d.client.ContainerInspect(ctx, containerID)
	require.Error(t, inspectErr)
	assert.True(t, dockerclient.IsErrNotFound(inspectErr), "container should be fully gone, got: %v", inspectErr)
}

// TestIntegration_Docker_CreateContainer_AdoptOnReplay verifies the
// validateAdoption-based recovery path: if a prior crashed attempt left a
// container matching the caller's params, a replayed CreateContainer returns
// the existing ID instead of failing with "name already in use".
func TestIntegration_Docker_CreateContainer_AdoptOnReplay(t *testing.T) {
	d := newTestDockerClient(t)
	ctx := context.Background()

	require.NoError(t, d.PullImage(ctx, "busybox:latest", 60*time.Second))

	params := baseCreateParams(fmt.Sprintf("adopt-%d", time.Now().UnixNano()), "busybox:latest")

	before := testutil.ToFloat64(idempotentOpsTotal.WithLabelValues("create", "already_exists"))

	firstID, err := d.CreateContainer(ctx, params, 30*time.Second)
	require.NoError(t, err)
	defer forceRemove(t, d, firstID)

	secondID, err := d.CreateContainer(ctx, params, 30*time.Second)
	require.NoError(t, err, "replay with identical params should adopt, not fail")
	assert.Equal(t, firstID, secondID, "replay should return the existing container ID")

	after := testutil.ToFloat64(idempotentOpsTotal.WithLabelValues("create", "already_exists"))
	assert.InDelta(t, 1.0, after-before, 0.001, "idempotentOpsTotal{create,already_exists} should increment by 1")
}

// TestIntegration_Docker_CreateContainer_RejectsImageMismatch verifies the
// adopt predicate refuses to take over a container whose image differs from
// the caller's params — even if the lease matches. This is the guard against
// silently running stale config after a mutated-payload re-provision.
func TestIntegration_Docker_CreateContainer_RejectsImageMismatch(t *testing.T) {
	d := newTestDockerClient(t)
	ctx := context.Background()

	require.NoError(t, d.PullImage(ctx, "busybox:latest", 60*time.Second))
	require.NoError(t, d.PullImage(ctx, "alpine:latest", 60*time.Second))

	leaseUUID := fmt.Sprintf("mismatch-%d", time.Now().UnixNano())
	firstParams := baseCreateParams(leaseUUID, "busybox:latest")
	firstID, err := d.CreateContainer(ctx, firstParams, 30*time.Second)
	require.NoError(t, err)
	defer forceRemove(t, d, firstID)

	// Replay with same lease/instance but a different image — must NOT adopt.
	mismatchParams := baseCreateParams(leaseUUID, "alpine:latest")
	_, err = d.CreateContainer(ctx, mismatchParams, 30*time.Second)
	require.Error(t, err, "create with mismatched image must not silently adopt")
	assert.Contains(t, err.Error(), "alpine:latest")
	assert.Contains(t, err.Error(), "busybox:latest")
}
