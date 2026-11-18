//go:build integration

package e2e

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/stretchr/testify/require"
)

// The labels docker-backend puts on what it creates. They are part of its
// observable surface, so the suite names them itself.
const (
	labelManaged     = "fred.managed"
	labelLeaseUUID   = "fred.lease_uuid"
	labelBackendName = "fred.backend_name"
)

// newDockerClient connects to the daemon the backends use.
func newDockerClient() (*client.Client, error) {
	return client.NewClientWithOpts(client.WithHost("unix://"+dockerSocket), client.WithAPIVersionNegotiation())
}

// dockerClient returns a client closed with the test, and skips the test when
// the daemon does not answer.
func dockerClient(t *testing.T) *client.Client {
	t.Helper()
	docker, err := newDockerClient()
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}
	return docker
}

// LeaseContainers inspects every container of a lease, running or not.
func LeaseContainers(t *testing.T, leaseUUID string) []container.InspectResponse {
	t.Helper()
	docker := dockerClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listed, err := docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", labelLeaseUUID+"="+leaseUUID)),
	})
	require.NoError(t, err)
	inspected := make([]container.InspectResponse, 0, len(listed))
	for _, summary := range listed {
		one, err := docker.ContainerInspect(ctx, summary.ID)
		require.NoError(t, err)
		inspected = append(inspected, one)
	}
	return inspected
}

// Exec runs a command in a container and returns its output. A non-zero exit
// fails the test.
func Exec(t *testing.T, containerID string, cmd ...string) string {
	t.Helper()
	docker := dockerClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	created, err := docker.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd: cmd, AttachStdout: true, AttachStderr: true,
	})
	require.NoError(t, err)
	attached, err := docker.ContainerExecAttach(ctx, created.ID, container.ExecAttachOptions{})
	require.NoError(t, err)
	defer attached.Close()
	var output bytes.Buffer
	_, err = stdcopy.StdCopy(&output, &output, attached.Reader)
	require.NoError(t, err)
	result, err := docker.ContainerExecInspect(ctx, created.ID)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode, "exec %v: %s", cmd, output.String())
	return output.String()
}

// removeDockerResources removes the containers and networks of every backend
// whose name match accepts, with the containers' anonymous volumes.
func removeDockerResources(t *testing.T, docker *client.Client, match func(backendName string) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := sweepDockerResources(ctx, docker, match); err != nil {
		t.Errorf("remove Docker resources: %v", err)
	}
}

func sweepDockerResources(ctx context.Context, docker *client.Client, match func(backendName string) bool) error {
	managed := filters.NewArgs(filters.Arg("label", labelManaged+"=true"))
	var failures []error
	containers, err := docker.ContainerList(ctx, container.ListOptions{All: true, Filters: managed})
	if err != nil {
		return err
	}
	for _, one := range containers {
		if !match(one.Labels[labelBackendName]) {
			continue
		}
		err := docker.ContainerRemove(ctx, one.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		if err != nil {
			failures = append(failures, err)
		}
	}
	networks, err := docker.NetworkList(ctx, network.ListOptions{Filters: managed})
	if err != nil {
		return errors.Join(append(failures, err)...)
	}
	for _, one := range networks {
		if !match(one.Labels[labelBackendName]) {
			continue
		}
		if err := docker.NetworkRemove(ctx, one.ID); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// ownedByThisSuite reports whether a backend name is one a fixture generated.
func ownedByThisSuite(backendName string) bool {
	return strings.HasPrefix(backendName, backendNamePrefix)
}
