//go:build integration

package docker

import (
	"context"
	"regexp"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// anonVolNameRE matches Docker's anonymous-volume naming (64 lowercase hex).
// The leak this suite guards against is specifically these volumes, so the
// before/after diff filters to them — staying immune to unrelated named volumes
// that may already exist (or be created by something else) on a shared daemon.
var anonVolNameRE = regexp.MustCompile("^[0-9a-f]{64}$")

// newIntegrationDockerClient returns a real DockerClient for integration tests,
// registering Close cleanup and skipping the test if the daemon is unreachable.
func newIntegrationDockerClient(t *testing.T, ctx context.Context) *DockerClient {
	t.Helper()
	docker, err := NewDockerClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	if err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}
	return docker
}

// dockerVolumeSet returns the set of all Docker volume names currently present.
func dockerVolumeSet(t *testing.T, ctx context.Context, docker *DockerClient) map[string]bool {
	t.Helper()
	resp, err := docker.client.VolumeList(ctx, volume.ListOptions{})
	require.NoError(t, err)
	set := make(map[string]bool, len(resp.Volumes))
	for _, v := range resp.Volumes {
		set[v.Name] = true
	}
	return set
}

// TestComposeDown_RemovesAnonymousVolumes pins the leak-prevention contract for
// ENG-372: tearing a lease's compose project down must also remove the
// anonymous Docker volumes attached to its containers.
//
// Anonymous volumes arise from image VOLUME directives that fred's tmpfs
// override does not cover (e.g. a stateful service whose image declares an
// extra VOLUME — the override is skipped whenever any stateful bind is
// present). A Down that does not reap them leaks one anonymous volume per such
// container on every close, which is the source of the thousands of orphaned
// 64-hex volumes observed accumulating on dev backends.
func TestComposeDown_RemovesAnonymousVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	// fred provisions with PullPolicy=never, so the image must be present before
	// Up — otherwise Up errors on a clean daemon instead of pulling.
	require.NoError(t, docker.PullImage(ctx, "busybox:latest", 60*time.Second))

	composeSvc, err := newComposeService("")
	require.NoError(t, err)

	// Build a valid project via the real builder, then force an anonymous
	// volume onto the service (a volume mount with no Source => Docker
	// allocates a 64-hex anonymous volume).
	params := baseProjectParams()
	params.LeaseUUID = "eng372-anonvol"
	params.NetworkName = "" // use compose's default network; no pre-created tenant net
	params.Stack.Services["web"] = &manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	project := buildComposeProject(params)
	svc := project.Services["web"]
	svc.Volumes = append(svc.Volumes, composetypes.ServiceVolumeConfig{
		Type:   "volume",
		Target: "/anon-data",
	})
	project.Services["web"] = svc

	projectName := composeProjectName(params.LeaseUUID)
	containerName := "fred-" + params.LeaseUUID + "-web-0"

	t.Cleanup(func() {
		_ = composeSvc.Down(context.Background(), projectName, 5*time.Second)
	})

	require.NoError(t, composeSvc.Up(ctx, project, composeUpOpts{}))

	// Discover the anonymous volume Docker attached to the container.
	inspected, err := docker.client.ContainerInspect(ctx, containerName)
	require.NoError(t, err)
	var anonVol string
	for _, m := range inspected.Mounts {
		if string(m.Type) == "volume" && m.Destination == "/anon-data" {
			anonVol = m.Name
		}
	}
	require.NotEmpty(t, anonVol, "expected an anonymous volume mounted at /anon-data")

	// Sanity: the volume exists right after Up.
	_, err = docker.client.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after Up")

	// Best-effort reap if the assertion below fails (the pre-fix RED run leaks it).
	t.Cleanup(func() {
		_ = docker.client.VolumeRemove(context.Background(), anonVol, true)
	})

	// Tear down the project. This MUST also remove the anonymous volume.
	require.NoError(t, composeSvc.Down(ctx, projectName, 5*time.Second))

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be removed by Down; got err=%v", anonVol, err)
}

// TestRemoveContainer_RemovesAnonymousVolumes pins the same leak-prevention
// contract on the individual-container fallback path (ENG-372). fred falls back
// to RemoveContainer when compose Down fails (deprovision.go) and uses it for
// create-rollback, so it too must reap the container's anonymous volumes —
// otherwise the leak survives whenever the compose path is bypassed.
func TestRemoveContainer_RemovesAnonymousVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	require.NoError(t, docker.PullImage(ctx, "busybox:latest", 60*time.Second))

	name := "fred-eng372-rmvol"
	// Clean any stale container from a prior run.
	_ = docker.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true, RemoveVolumes: true})

	created, err := docker.client.ContainerCreate(ctx,
		&container.Config{
			Image:   "busybox:latest",
			Cmd:     []string{"sleep", "3600"},
			Volumes: map[string]struct{}{"/anon-data": {}}, // anonymous volume
		},
		&container.HostConfig{}, nil, nil, name)
	require.NoError(t, err)

	inspected, err := docker.client.ContainerInspect(ctx, created.ID)
	require.NoError(t, err)
	var anonVol string
	for _, m := range inspected.Mounts {
		if string(m.Type) == "volume" && m.Destination == "/anon-data" {
			anonVol = m.Name
		}
	}
	require.NotEmpty(t, anonVol, "expected an anonymous volume on the created container")

	t.Cleanup(func() {
		_ = docker.client.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		_ = docker.client.VolumeRemove(context.Background(), anonVol, true)
	})

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after create")

	require.NoError(t, docker.RemoveContainer(ctx, created.ID))

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be removed by RemoveContainer; got err=%v", anonVol, err)
}

// TestImageIntrospection_DoesNotLeakAnonymousVolumes pins that the image-
// introspection temp containers (ENG-372 (a)) do not leak anonymous volumes.
// Each of ResolveImageUser/DetectVolumeOwner/DetectWritablePaths spins up a
// throwaway container FROM the tenant image to read its filesystem; Docker
// materializes the image's VOLUME directives as anonymous volumes at create
// time (even though these containers are never started), so the teardown must
// remove them. These run on the provision path (cache-missed image setup), so
// a leak here accumulates per distinct image and across backend restarts.
func TestImageIntrospection_DoesNotLeakAnonymousVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	const img = "redis:7-alpine" // declares VOLUME /data
	// Pull rather than skip-if-absent: a skip here would silently turn the leak
	// assertion into a false green on a clean CI daemon.
	require.NoError(t, docker.PullImage(ctx, img, 120*time.Second))

	before := dockerVolumeSet(t, ctx, docker)

	// Drive the three introspection entrypoints; each creates one temp
	// container from the image (sites readFileFromImage / DetectVolumeOwner /
	// DetectWritablePaths). Return values are irrelevant — the leak, if any,
	// happens on the temp container's removal regardless of outcome.
	_, _, _ = docker.ResolveImageUser(ctx, img, "redis") // → readFileFromImage(/etc/passwd)
	_, _, _ = docker.DetectVolumeOwner(ctx, img, []string{"/data"})
	_, _ = docker.DetectWritablePaths(ctx, img, 0, []string{"/"})

	after := dockerVolumeSet(t, ctx, docker)

	// Count only newly-appeared anonymous (64-hex) volumes — the leak shape —
	// so an unrelated named volume on a shared daemon can't cause a false fail.
	var leaked []string
	for v := range after {
		if !before[v] && anonVolNameRE.MatchString(v) {
			leaked = append(leaked, v)
		}
	}
	// Best-effort cleanup so a RED run doesn't pollute the daemon.
	t.Cleanup(func() {
		for _, v := range leaked {
			_ = docker.client.VolumeRemove(context.Background(), v, true)
		}
	})

	assert.Empty(t, leaked, "image introspection leaked anonymous volume(s): %v", leaked)
}
