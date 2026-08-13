//go:build integration

package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// cleanupTimeout bounds each test-cleanup Docker call. Cleanups use a fresh
// context (the test ctx is already canceled by the time t.Cleanup runs) but
// must also be time-bounded so a hung daemon can't stall the suite — mirroring
// cleanupTestContainers/cleanupTestNetworks.
const cleanupTimeout = 30 * time.Second

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

// TestIntegration_Docker_ComposeDown_RemovesAnonymousVolumes pins the leak-prevention contract for
// ENG-372: tearing a lease's compose project down must also remove the
// anonymous Docker volumes attached to its containers.
//
// Anonymous volumes arise from image VOLUME directives that fred's tmpfs
// override does not cover (e.g. a stateful service whose image declares an
// extra VOLUME — the override is skipped whenever any stateful bind is
// present). A Down that does not reap them leaks one anonymous volume per such
// container on every close, which is the source of the thousands of orphaned
// 64-hex volumes observed accumulating on dev backends.
func TestIntegration_Docker_ComposeDown_RemovesAnonymousVolumes(t *testing.T) {
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
	// allocates a 64-hex anonymous volume). The lease UUID is unique per run so
	// a crashed prior run can't collide on the project/container names.
	params := baseProjectParams()
	params.LeaseUUID = fmt.Sprintf("eng372-anonvol-%d", time.Now().UnixNano())
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
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = composeSvc.Down(cctx, projectName, 5*time.Second)
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
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = docker.client.VolumeRemove(cctx, anonVol, true)
	})

	// Tear down the project. This MUST also remove the anonymous volume.
	require.NoError(t, composeSvc.Down(ctx, projectName, 5*time.Second))

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be removed by Down; got err=%v", anonVol, err)
}

// TestIntegration_Docker_RemoveContainer_RemovesAnonymousVolumes pins the same leak-prevention
// contract on the individual-container fallback path (ENG-372). fred falls back
// to RemoveContainer when compose Down fails (deprovision.go) and uses it for
// create-rollback, so it too must reap the container's anonymous volumes —
// otherwise the leak survives whenever the compose path is bypassed.
func TestIntegration_Docker_RemoveContainer_RemovesAnonymousVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	require.NoError(t, docker.PullImage(ctx, "busybox:latest", 60*time.Second))

	// Unique per run so concurrent runs / a crashed prior run can't collide.
	name := fmt.Sprintf("fred-eng372-rmvol-%d", time.Now().UnixNano())

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
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = docker.client.ContainerRemove(cctx, created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
		_ = docker.client.VolumeRemove(cctx, anonVol, true)
	})

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after create")

	require.NoError(t, docker.RemoveContainer(ctx, created.ID))

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be removed by RemoveContainer; got err=%v", anonVol, err)
}

// TestIntegration_Docker_ImageIntrospection_DoesNotLeakAnonymousVolumes pins that the image-
// introspection temp containers (ENG-372 (a)) do not leak anonymous volumes.
// Each of ResolveImageUser/DetectVolumeOwner/DetectWritablePaths spins up a
// throwaway container FROM the tenant image to read its filesystem; Docker
// materializes the image's VOLUME directives as anonymous volumes at create
// time (even though these containers are never started), so the teardown must
// remove them. These run on the provision path (cache-missed image setup), so
// a leak here accumulates per distinct image and across backend restarts.
func TestIntegration_Docker_ImageIntrospection_DoesNotLeakAnonymousVolumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	const img = "redis:7-alpine" // declares VOLUME /data
	// Pull rather than skip-if-absent: a skip here would silently turn the leak
	// assertion into a false green on a clean CI daemon.
	require.NoError(t, docker.PullImage(ctx, img, 120*time.Second))

	before := dockerVolumeSet(t, ctx, docker)

	// Drive the three introspection entrypoints; each creates one temp container
	// from the image (sites readFileFromImage / DetectVolumeOwner /
	// DetectWritablePaths). Require success: if a helper failed before creating
	// its container, the diff would be empty and the leak assertion would
	// false-green without ever exercising the reap path.
	_, _, err := docker.ResolveImageUser(ctx, img, "redis") // → readFileFromImage(/etc/passwd)
	require.NoError(t, err)
	_, _, err = docker.DetectVolumeOwner(ctx, img, []string{"/data"})
	require.NoError(t, err)
	_, err = docker.DetectWritablePaths(ctx, img, 0, []string{"/data"}) // small dir; only the temp-container create/remove path matters here
	require.NoError(t, err)

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
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		for _, v := range leaked {
			_ = docker.client.VolumeRemove(cctx, v, true)
		}
	})

	assert.Empty(t, leaked, "image introspection leaked anonymous volume(s): %v", leaked)
}

// TestIntegration_Docker_TeardownFallback_RemovesAnonymousVolumesWhenDownFails closes
// the loop the other two tests in this file each cover half of: it drives fred's real
// compensation path — teardownLeaseContainers — against a real daemon, with compose
// Down forced to fail so the per-container fallback is the ONLY thing that can reap.
//
// The two halves it joins: ComposeDown_RemovesAnonymousVolumes pins the happy path,
// RemoveContainer_RemovesAnonymousVolumes pins the primitive. Neither proves fred
// FINDS the containers when Down fails — which is the ENG-647 defect, since the
// provision record names none of them on the restore-rollback arms. Discovery here is
// by fred label against a live daemon, so a labelling or filtering regression fails
// this test rather than silently leaking.
//
// It also pins the boundary that makes the fallback a safe substitute for Down: the
// bind-mounted tenant directory must survive. RemoveContainer passes RemoveVolumes:true
// (`docker rm -v`), which reaps ANONYMOUS volumes only — never binds, and never named
// volumes, which is why fred's projects must declare none
// (TestBuildComposeProject_DeclaresNoNamedVolumes).
func TestIntegration_Docker_TeardownFallback_RemovesAnonymousVolumesWhenDownFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)

	require.NoError(t, docker.PullImage(ctx, "busybox:latest", 60*time.Second))

	composeSvc, err := newComposeService("")
	require.NoError(t, err)

	// Tenant data lives in a bind mount, exactly as applyVolumeBinds produces.
	tenantDir := t.TempDir()
	dataFile := filepath.Join(tenantDir, "tenant.dat")
	require.NoError(t, os.WriteFile(dataFile, []byte("tenant data"), 0o600))

	params := baseProjectParams()
	params.LeaseUUID = fmt.Sprintf("eng647-teardown-%d", time.Now().UnixNano())
	params.NetworkName = "" // compose's default network; no pre-created tenant net
	params.Stack.Services["web"] = &manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	params.VolBinds = map[string]map[int]serviceVolBinds{
		"web": {0: {StatefulBinds: map[string]string{tenantDir: "/data"}}},
	}
	project := buildComposeProject(params)
	svc := project.Services["web"]
	// Force the anonymous volume an uncovered image VOLUME would produce.
	svc.Volumes = append(svc.Volumes, composetypes.ServiceVolumeConfig{
		Type:   "volume",
		Target: "/anon-data",
	})
	project.Services["web"] = svc

	projectName := composeProjectName(params.LeaseUUID)
	containerName := "fred-" + params.LeaseUUID + "-web-0"

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = composeSvc.Down(cctx, projectName, 5*time.Second)
	})

	require.NoError(t, composeSvc.Up(ctx, project, composeUpOpts{}))

	inspected, err := docker.client.ContainerInspect(ctx, containerName)
	require.NoError(t, err)
	var anonVol string
	for _, m := range inspected.Mounts {
		if string(m.Type) == "volume" && m.Destination == "/anon-data" {
			anonVol = m.Name
		}
	}
	require.NotEmpty(t, anonVol, "expected an anonymous volume mounted at /anon-data")
	_, err = docker.client.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after Up")

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = docker.client.VolumeRemove(cctx, anonVol, true)
	})

	// A real daemon behind RemoveContainer/ListManagedContainers, with compose Down
	// failing the way v5 does when its errgroup cancels teardown part-way.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.docker = docker
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose down canceled after first removal failed")
		},
	}

	// No recorded container IDs — the failed-restore shape, where discovery is the
	// only way to find anything at all.
	remaining, err := b.teardownLeaseContainers(ctx, params.LeaseUUID, nil, 5*time.Second,
		teardownOpDeprovision, slog.Default())
	require.NoError(t, err, "the fallback must finish the teardown compose Down abandoned")
	assert.Empty(t, remaining)

	_, err = docker.client.ContainerInspect(ctx, containerName)
	assert.True(t, client.IsErrNotFound(err),
		"container %s must be removed by the fallback; got err=%v", containerName, err)

	_, err = docker.client.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be reaped by the fallback; got err=%v", anonVol, err)

	// The tenant's bind-mounted data is NOT a Docker volume and must be untouched.
	got, err := os.ReadFile(dataFile)
	require.NoError(t, err, "teardown must never remove bind-mounted tenant data")
	assert.Equal(t, "tenant data", string(got))
}
