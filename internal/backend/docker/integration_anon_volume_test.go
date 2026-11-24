//go:build integration

package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// cleanupTimeout bounds each test-cleanup Docker call. Cleanups use a fresh
// context (the test ctx is already canceled by the time t.Cleanup runs) but
// must also be time-bounded so a hung daemon can't stall the suite — mirroring
// cleanupTestContainers/cleanupTestNetworks.
const cleanupTimeout = 30 * time.Second

// newIntegrationDockerClient returns a real DockerClient for integration tests,
// registering Close cleanup and skipping the test if the daemon is unreachable.
func newIntegrationDockerClient(t *testing.T, ctx context.Context) *DockerClient {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	if err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}
	return docker
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
	sdk := newImageSecurityFixtureClient(t)

	// fred provisions with PullPolicy=never, so the image must be present before
	// Up — otherwise Up errors on a clean daemon instead of pulling.
	require.NoError(t, pullImageForTest(t, docker, ctx, "busybox:latest", 60*time.Second))
	admitted, err := docker.AdmitImage(ctx, "busybox:latest")
	require.NoError(t, err)

	composeSvc, err := newComposeService("", docker.images, docker.tenantSeccomp)
	require.NoError(t, err)

	// Build a valid project via the real builder, then force an anonymous
	// volume onto the service (a volume mount with no Source => Docker
	// allocates a 64-hex anonymous volume). The lease UUID is unique per run so
	// a crashed prior run can't collide on the project/container names.
	params := baseProjectParams()
	params.LeaseUUID = fmt.Sprintf("eng372-anonvol-%d", time.Now().UnixNano())
	params.Network = leaseNetworkAuthority{}
	params.Cfg.NetworkIsolation = ptrBool(false)
	params.Stack.Services["web"] = &manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	project := buildTestComposeProject(t, params)
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

	prepared, err := composeSvc.PrepareProject(project, map[string]imageexec.Image{"web": admitted})
	require.NoError(t, err)
	require.NoError(t, composeSvc.launch(ctx, prepared, composeUpOpts{}).err)

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
	_, err = sdk.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after Up")

	// Best-effort reap if the assertion below fails (the pre-fix RED run leaks it).
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = sdk.VolumeRemove(cctx, anonVol, true)
	})

	// Tear down the project. This MUST also remove the anonymous volume.
	require.NoError(t, composeSvc.Down(ctx, projectName, 5*time.Second))

	_, err = sdk.VolumeInspect(ctx, anonVol)
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
	sdk := newImageSecurityFixtureClient(t)

	require.NoError(t, pullImageForTest(t, docker, ctx, "busybox:latest", 60*time.Second))

	// Unique per run so concurrent runs / a crashed prior run can't collide.
	name := fmt.Sprintf("fred-eng372-rmvol-%d", time.Now().UnixNano())

	created, err := sdk.ContainerCreate(ctx,
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
		_ = sdk.VolumeRemove(cctx, anonVol, true)
	})

	_, err = sdk.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after create")

	require.NoError(t, docker.RemoveContainer(ctx, created.ID))

	_, err = sdk.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be removed by RemoveContainer; got err=%v", anonVol, err)
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
	sdk := newImageSecurityFixtureClient(t)

	require.NoError(t, pullImageForTest(t, docker, ctx, "busybox:latest", 60*time.Second))
	admitted, err := docker.AdmitImage(ctx, "busybox:latest")
	require.NoError(t, err)

	composeSvc, err := newComposeService("", docker.images, docker.tenantSeccomp)
	require.NoError(t, err)

	// Tenant data lives in a bind mount, exactly as applyVolumeBinds produces.
	tenantDir := t.TempDir()
	dataFile := filepath.Join(tenantDir, "tenant.dat")
	require.NoError(t, os.WriteFile(dataFile, []byte("tenant data"), 0o600))

	params := baseProjectParams()
	params.LeaseUUID = fmt.Sprintf("eng647-teardown-%d", time.Now().UnixNano())
	params.Network = leaseNetworkAuthority{}
	params.Cfg.NetworkIsolation = ptrBool(false)
	params.Stack.Services["web"] = &manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	params.VolBinds = map[string]map[int]serviceVolBinds{
		"web": {0: {StatefulBinds: map[string]string{tenantDir: "/data"}}},
	}
	project := buildTestComposeProject(t, params)
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

	prepared, err := composeSvc.PrepareProject(project, map[string]imageexec.Image{"web": admitted})
	require.NoError(t, err)
	require.NoError(t, composeSvc.launch(ctx, prepared, composeUpOpts{}).err)

	inspected, err := docker.client.ContainerInspect(ctx, containerName)
	require.NoError(t, err)
	var anonVol string
	for _, m := range inspected.Mounts {
		if string(m.Type) == "volume" && m.Destination == "/anon-data" {
			anonVol = m.Name
		}
	}
	require.NotEmpty(t, anonVol, "expected an anonymous volume mounted at /anon-data")
	_, err = sdk.VolumeInspect(ctx, anonVol)
	require.NoError(t, err, "anonymous volume should exist after Up")

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer ccancel()
		_ = sdk.VolumeRemove(cctx, anonVol, true)
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

	_, err = sdk.VolumeInspect(ctx, anonVol)
	assert.True(t, client.IsErrNotFound(err),
		"anonymous volume %s must be reaped by the fallback; got err=%v", anonVol, err)

	// The tenant's bind-mounted data is NOT a Docker volume and must be untouched.
	got, err := os.ReadFile(dataFile)
	require.NoError(t, err, "teardown must never remove bind-mounted tenant data")
	assert.Equal(t, "tenant data", string(got))
}

// newIntegrationInspectionHarness retains actual daemon and journal attestation
// while driving only preparation through a real Started-subject executor.
func newIntegrationInspectionHarness(t *testing.T, docker *DockerClient, reference string) *inspectionHarness {
	t.Helper()
	expected, err := docker.DaemonInfo(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, expected.SystemID)
	h := newInspectionHarnessWithClient(t, func(*inspectionDaemon) *DockerClient { return docker }, reference)
	h.backend.storageVerifier = testDockerRuntimeStorageVerifier{id: h.authority.storage.ID(), verify: func(ctx context.Context) error {
		if err := h.callbacks.VerifyStorageIdentity(h.authority.storage); err != nil {
			return err
		}
		actual, err := docker.DaemonInfo(ctx)
		if err != nil {
			return err
		}
		if actual.SystemID != expected.SystemID {
			return backendidentity.ErrIdentityDrift
		}
		return nil
	}}
	return h
}

// newImageSecurityFixtureClient owns the raw SDK only in test fixtures. The
// production lifecycle wrapper deliberately cannot create arbitrary containers.
func newImageSecurityFixtureClient(t *testing.T) *client.Client {
	t.Helper()
	sdk, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sdk.Close()) })
	return sdk
}
