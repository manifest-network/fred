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
	// An image-inspection helper carries none of the labels above. The backend
	// removes it when admission ends, so one is left behind only when the
	// process dies during admission.
	labelInspectionSchema  = "fred.inspection.schema"
	labelInspectionBackend = "fred.inspection.backend"
)

// The workload images, pinned by digest so that a run never depends on what a
// floating tag points at that day. The tag is for the reader: Docker pulls by
// the digest, which names the multi-platform index. integration.yml pre-pulls
// the same two references.
const (
	BusyboxImage = "busybox:1.37.0@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec"
	// RedisImage declares VOLUME /data.
	RedisImage = "redis:7.4.12@sha256:2e3eb11462c4e3759e6972678dd051d150ff54b1e60714b3c33b701260172a90"
)

// containerKinds lists the kinds of container docker-backend creates: the
// label filter that selects a kind, and the label that names its backend.
var containerKinds = []struct{ selector, backendLabel string }{
	{labelManaged + "=true", labelBackendName},
	{labelInspectionSchema, labelInspectionBackend},
}

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

// Containers inspects the containers this backend runs for a lease. Two
// backends can each hold containers of one lease, which LeaseContainers
// cannot tell apart.
func (b *Backend) Containers(t *testing.T, leaseUUID string) []container.InspectResponse {
	t.Helper()
	var own []container.InspectResponse
	for _, one := range LeaseContainers(t, leaseUUID) {
		if one.Config.Labels[labelBackendName] == b.Name {
			own = append(own, one)
		}
	}
	return own
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

// removeDockerResources removes the containers, image-inspection helpers and
// networks of every backend whose name match accepts, with the containers'
// anonymous volumes.
func removeDockerResources(t *testing.T, docker *client.Client, match func(backendName string) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := sweepDockerResources(ctx, docker, match); err != nil {
		t.Errorf("remove Docker resources: %v", err)
	}
}

func sweepDockerResources(ctx context.Context, docker *client.Client, match func(backendName string) bool) error {
	var failures []error
	for _, kind := range containerKinds {
		containers, err := docker.ContainerList(ctx, container.ListOptions{
			All:     true,
			Filters: filters.NewArgs(filters.Arg("label", kind.selector)),
		})
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		for _, one := range containers {
			if !match(one.Labels[kind.backendLabel]) {
				continue
			}
			err := docker.ContainerRemove(ctx, one.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
			if err != nil {
				failures = append(failures, err)
			}
		}
	}
	managed := filters.NewArgs(filters.Arg("label", labelManaged+"=true"))
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
