//go:build integration

package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
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
	labelServiceName = "fred.service_name"
	// An image-inspection helper carries none of the labels above. The backend
	// removes it when admission ends, so one is left behind only when the
	// process dies during admission.
	labelInspectionSchema  = "fred.inspection.schema"
	labelInspectionBackend = "fred.inspection.backend"
)

// The workload images, pinned by digest so that a run never depends on what a
// floating tag points at that day. The tag is for the reader: Docker pulls by
// the digest. integration.yml pre-pulls the same references.
//
// All of them come from the suite's own repository, the Docker Official
// Images among them, which test/e2e/images republishes unchanged. Every test
// starts a backend, and a backend asks the registry for the manifest of each
// image it has not seen. It can send no credentials, and Docker Hub and ECR
// Public both refuse clients without credentials once they have asked as
// often as this suite does.
const (
	// testImages is the repository the suite's images are published to.
	testImages = "ghcr.io/manifest-network/fred-e2e:"

	// BusyboxImage is busybox 1.37.0: test/e2e/images/busybox.
	BusyboxImage = testImages + "busybox-6fb94d8d9227@sha256:b66abc847a308ea12831a6c12cb6f3920cfb99400d92b951c52bc4d914cf48b2"
	// MissingImage names an image that does not exist, in a registry the
	// backends allow.
	MissingImage = testImages + "no-such-image"
	// RedisImage is redis 7.4.12, which declares VOLUME /data:
	// test/e2e/images/redis.
	RedisImage = testImages + "redis-5612f20db1e9@sha256:abb96089ca435b3d656fa453f155ec3aa54dd757cbe3f3d42fc2eabd8cbbbf03"
	// RedisAlpineImage is redis 7.4.12 on Alpine, which declares the same
	// volume as RedisImage, so a lease can be updated from one to the other
	// and keep its data: test/e2e/images/redis-alpine.
	RedisAlpineImage = testImages + "redis-alpine-801a158eb48c@sha256:621bf77af3920abd1956aa833809b12297ed53350ac7a698ba4c437f944b38a6"
	// NestedVolumesImage declares VOLUME /data/b and VOLUME /data/a/c, sets no
	// user, and owns those directories as root, so its workload runs as
	// root. It is built from test/e2e/images/nested-volumes.
	NestedVolumesImage = testImages + "nested-volumes-ac191cead6d9@sha256:73e8285a3e87689fdf7ca9a8b6a3617239ff3da75d90e9122e48075c7e7dff00"
	// ProjectProbeImage declares VOLUME /data, runs as root, and carries
	// /probe, which tries to move a file to another XFS project. It is built
	// from test/e2e/images/projid-probe.
	ProjectProbeImage = testImages + "projid-probe-8bd9cd6c79a8@sha256:00877d22f67cf71f211aed949c3a8f238755690f0a35a7bc712685ad876eb396"
)

// containerKinds lists the kinds of container docker-backend creates: the
// label filter that selects a kind, and the label that names its backend.
var containerKinds = []struct{ selector, backendLabel string }{
	{labelManaged + "=true", labelBackendName},
	{labelInspectionSchema, labelInspectionBackend},
}

// DockerSocketsEnv lists the Docker daemons the suite may use, as socket
// paths separated by commas. A docker-backend needs a daemon to itself: it
// refuses to work beside another backend's containers. A provider with n
// backends therefore needs n daemons. Unset, the suite has the system daemon
// alone. scripts/e2e-second-dockerd.sh starts a second one.
const DockerSocketsEnv = "FRED_E2E_DOCKER_SOCKETS"

const systemDockerSocket = "/var/run/docker.sock"

// DockerSockets returns the sockets of the daemons the suite may use.
func DockerSockets() []string {
	var sockets []string
	for _, socket := range strings.Split(os.Getenv(DockerSocketsEnv), ",") {
		if socket = strings.TrimSpace(socket); socket != "" {
			sockets = append(sockets, socket)
		}
	}
	if len(sockets) == 0 {
		return []string{systemDockerSocket}
	}
	return sockets
}

// newDockerClient connects to the daemon behind a socket.
func newDockerClient(socket string) (*client.Client, error) {
	return client.NewClientWithOpts(client.WithHost("unix://"+socket), client.WithAPIVersionNegotiation())
}

// dockerClient returns a client closed with the test, and skips the test when
// the daemon does not answer.
func dockerClient(t *testing.T, socket string) *client.Client {
	t.Helper()
	docker, err := newDockerClient(socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := docker.Ping(ctx); err != nil {
		t.Skipf("Docker not available at %s: %v", socket, err)
	}
	return docker
}

// Containers inspects every container of a lease on this backend's daemon,
// running or not.
func (b *Backend) Containers(t *testing.T, leaseUUID string) []container.InspectResponse {
	t.Helper()
	inspected, err := b.TryContainers(leaseUUID)
	require.NoError(t, err)
	return inspected
}

// TryContainers is Containers for use off the test's goroutine, where a
// failure must not end the test: it returns the error.
func (b *Backend) TryContainers(leaseUUID string) ([]container.InspectResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listed, err := b.docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", labelLeaseUUID+"="+leaseUUID)),
	})
	if err != nil {
		return nil, err
	}
	inspected := make([]container.InspectResponse, 0, len(listed))
	for _, summary := range listed {
		one, err := b.docker.ContainerInspect(ctx, summary.ID)
		if errdefs.IsNotFound(err) {
			// Removed since it was listed, as when a backend replaces a
			// lease's container: it is not one of the lease's containers.
			continue
		}
		if err != nil {
			return nil, err
		}
		inspected = append(inspected, one)
	}
	return inspected, nil
}

// LoseContainers removes the lease's containers behind the backend's back, the
// way a host that lost them would have none.
func (b *Backend) LoseContainers(t *testing.T, leaseUUID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, one := range b.Containers(t, leaseUUID) {
		require.NoError(t, b.docker.ContainerRemove(ctx, one.ID, container.RemoveOptions{Force: true, RemoveVolumes: true}))
	}
}

// OccupyContainerName creates a container on this backend's daemon under the
// given name, from an image the daemon already has, and never starts it. The
// container carries none of the backend's labels, so the backend does not see
// it; Docker refuses to create another container of that name. It is removed
// when the test ends.
func (b *Backend) OccupyContainerName(t *testing.T, name, image string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	created, err := b.docker.ContainerCreate(ctx, &container.Config{Image: image}, nil, nil, nil, name)
	require.NoError(t, err, "create a container named %s", name)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// With its volumes: Docker gives a container the anonymous volumes
		// its image declares when it creates it, started or not.
		_ = b.docker.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
	})
}

// Exec runs a command in a container on this backend's daemon and returns its
// output. A non-zero exit fails the test.
func (b *Backend) Exec(t *testing.T, containerID string, cmd ...string) string {
	t.Helper()
	exitCode, output := b.TryExec(t, containerID, cmd...)
	require.Zero(t, exitCode, "exec %v: %s", cmd, output)
	return output
}

// TryExec runs a command in a container on this backend's daemon and returns
// its exit code and output, for a command a test expects to fail.
func (b *Backend) TryExec(t *testing.T, containerID string, cmd ...string) (exitCode int, output string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	created, err := b.docker.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd: cmd, AttachStdout: true, AttachStderr: true,
	})
	require.NoError(t, err)
	attached, err := b.docker.ContainerExecAttach(ctx, created.ID, container.ExecAttachOptions{})
	require.NoError(t, err)
	defer attached.Close()
	var collected bytes.Buffer
	_, err = stdcopy.StdCopy(&collected, &collected, attached.Reader)
	require.NoError(t, err)
	result, err := b.docker.ContainerExecInspect(ctx, created.ID)
	require.NoError(t, err)
	return result.ExitCode, collected.String()
}

// KillContainers kills the lease's containers, as a crashed workload would
// leave them: present, and no longer running.
func (b *Backend) KillContainers(t *testing.T, leaseUUID string) {
	t.Helper()
	for _, one := range b.Containers(t, leaseUUID) {
		b.KillContainer(t, one.ID)
	}
}

// KillContainer kills one container, as KillContainers kills a lease's.
func (b *Backend) KillContainer(t *testing.T, containerID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, b.docker.ContainerKill(ctx, containerID, "SIGKILL"))
}

// networkNames lists the networks this backend's daemon holds for a lease. A
// failed listing reads as one unnamed network, so a caller waiting for none
// keeps waiting.
func (b *Backend) networkNames(leaseUUID string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listed, err := b.docker.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labelLeaseUUID+"="+leaseUUID)),
	})
	if err != nil {
		return []string{""}
	}
	names := make([]string, len(listed))
	for i, one := range listed {
		names[i] = one.Name
	}
	return names
}

// Started lists the containers of a lease that this backend's daemon has
// started since the given time, by ID and in order, from the daemon's event
// stream. A container that ran only for a moment is listed too.
func (b *Backend) Started(t *testing.T, leaseUUID string, since time.Time) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	messages, errs := b.docker.Events(ctx, events.ListOptions{
		Since: strconv.FormatInt(since.Unix(), 10),
		Until: strconv.FormatInt(time.Now().Unix()+1, 10),
		Filters: filters.NewArgs(
			filters.Arg("type", string(events.ContainerEventType)),
			filters.Arg("event", string(events.ActionStart)),
			filters.Arg("label", labelLeaseUUID+"="+leaseUUID),
		),
	})
	var started []string
	for {
		select {
		case message := <-messages:
			started = append(started, message.Actor.ID)
		case err := <-errs:
			if !errors.Is(err, io.EOF) {
				require.NoError(t, err, "read the daemon's events")
			}
			return started
		}
	}
}

// Leftovers names what the host still holds of a lease: its containers and
// networks on this backend's daemon, and its volume directories under either
// name. A failed listing reads as a leftover, so a caller waiting for none
// keeps waiting.
func (b *Backend) Leftovers(leaseUUID string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var left []string
	containers, err := b.docker.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", labelLeaseUUID+"="+leaseUUID)),
	})
	if err != nil {
		left = append(left, "containers: "+err.Error())
	}
	for _, one := range containers {
		left = append(left, "container "+strings.Join(one.Names, ","))
	}
	for _, name := range b.networkNames(leaseUUID) {
		left = append(left, "network "+name)
	}
	volumes, err := filepath.Glob(filepath.Join(b.VolumePath, "*"+leaseUUID+"*"))
	if err != nil {
		left = append(left, "volumes: "+err.Error())
	}
	for _, path := range volumes {
		left = append(left, "volume "+path)
	}
	return left
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
