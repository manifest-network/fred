//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
	mobyseccomp "github.com/moby/profiles/seccomp"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type integrationXFSVolume struct {
	name   managedVolumeName
	path   string
	projID uint32
}

func createIntegrationXFSVolume(t *testing.T, mgr volumeManager) integrationXFSVolume {
	t.Helper()
	name, err := parseManagedVolumeName(canonicalVolumeName(uuid.NewString(), "app", 0))
	require.NoError(t, err)
	path, created, err := mgr.Create(t.Context(), name.value(), 16)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		require.NoError(t, volDestroyer(t, mgr).Destroy(ctx, name.value()))
	})
	projID, err := readProjectIDFile(path)
	require.NoError(t, err)
	require.NotZero(t, projID)
	return integrationXFSVolume{name: name, path: path, projID: projID}
}

func integrationXFSAttributes(t *testing.T, path string) linuxFSXAttr {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	attr, err := readXFSProjectAttributes(file)
	require.NoError(t, err)
	return attr
}

// importXFSProjectIDProbe imports a scratch image containing only a static Go
// probe. No registry, Docker build service, shell or in-container packages are
// needed; root CI already supplies Go and the XFS tools.
func importXFSProjectIDProbe(t *testing.T, ctx context.Context, sdk *client.Client) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "probe")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/xfs-projid-probe")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "build project-ID probe: %s", out)
	data, err := os.ReadFile(binary)
	require.NoError(t, err)
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "probe", Mode: 0o755, Size: int64(len(data))}))
	_, err = tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	tag := "fred-xfs-projid-probe:" + uuid.NewString()
	stream, err := sdk.ImageImport(ctx, image.ImportSource{Source: &archive, SourceName: "-"}, tag, image.ImportOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_, err := sdk.ImageRemove(ctx, tag, image.RemoveOptions{})
		require.NoError(t, err)
	})
	defer func() { _ = stream.Close() }()
	decoder := json.NewDecoder(stream)
	for {
		var message struct{ Error string }
		err := decoder.Decode(&message)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Empty(t, message.Error)
	}
	return tag
}

func waitXFSProjectIDProbe(t *testing.T, ctx context.Context, sdk *client.Client, id string) map[string]int {
	t.Helper()
	done, failed := sdk.ContainerWait(ctx, id, container.WaitConditionNotRunning)
	var exitCode int64
	select {
	case result := <-done:
		require.Nil(t, result.Error)
		exitCode = result.StatusCode
	case err := <-failed:
		t.Fatalf("wait for project-ID probe: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for project-ID probe: %v", ctx.Err())
	}
	logs, err := sdk.ContainerLogs(ctx, id, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	require.NoError(t, err)
	defer func() { _ = logs.Close() }()
	var stdout, stderr bytes.Buffer
	_, err = stdcopy.StdCopy(&stdout, &stderr, logs)
	require.NoError(t, err)
	require.Zero(t, exitCode, "probe failed: stdout=%s stderr=%s", stdout.String(), stderr.String())
	var results map[string]int
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &results), "probe output: %s", stdout.String())
	return results
}

// ENG-1118: exercise both production creation paths on real XFS. The default
// profile control must successfully change attributes as the very same
// non-root, capability-free owner; EPERM in the tenant cases must therefore
// come from the new policy, not from an unwritable mount or invalid ioctl.
func TestIntegration_Docker_XFS_TenantCannotChangeProjectID(t *testing.T) {
	mountPath := setupXFSLoopback(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)
	sdk := newImageSecurityFixtureClient(t)
	mgr, err := newVolumeManager(mountPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	// Destroy the source first: the control charges one of its files to the
	// other project, so that file must be gone before the other dquot retires.
	other := createIntegrationXFSVolume(t, mgr)
	volume := createIntegrationXFSVolume(t, mgr)
	require.NotEqual(t, other.projID, volume.projID)
	tag := importXFSProjectIDProbe(t, ctx, sdk)
	admitted, err := docker.AdmitImage(ctx, tag)
	require.NoError(t, err)
	args := []string{fmt.Sprint(volume.projID), fmt.Sprint(other.projID)}

	// The probe needs no network. Attach the direct creates to an internal
	// user-defined network, as fred's own creates do, instead of the default
	// bridge, which CI runners do not provide.
	netName := "fred-xfs-probe-" + uuid.NewString()
	createdNet, err := sdk.NetworkCreate(ctx, netName, networktypes.CreateOptions{Internal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.NetworkRemove(context.Background(), createdNet.ID) })
	probeNetwork := func() *networktypes.NetworkingConfig {
		return &networktypes.NetworkingConfig{EndpointsConfig: map[string]*networktypes.EndpointSettings{netName: {}}}
	}

	for _, launch := range []string{"default-profile-control", "sdk", "compose"} {
		t.Run(launch, func(t *testing.T) {
			dataPath := filepath.Join(volume.path, launch)
			require.NoError(t, os.Mkdir(dataPath, 0o700))
			require.NoError(t, os.Chown(dataPath, 10000, 10000))
			var id string
			switch launch {
			case "default-profile-control":
				// The raw SDK is used only for the vulnerable control. Pin the
				// default explicitly so a daemon-wide profile cannot hide it.
				profile, err := json.Marshal(mobyseccomp.DefaultProfile())
				require.NoError(t, err)
				created, err := sdk.ContainerCreate(ctx, &container.Config{
					Image: admitted.ID(), User: "10000:10000", Entrypoint: []string{"/probe"}, Cmd: args,
				}, &container.HostConfig{
					CapDrop: []string{"ALL"}, ReadonlyRootfs: true,
					SecurityOpt: []string{"no-new-privileges:true", "seccomp=" + string(profile)},
					Mounts:      []mount.Mount{{Type: mount.TypeBind, Source: dataPath, Target: "/data"}},
				}, probeNetwork(), nil, "fred-xfs-control-"+uuid.NewString())
				require.NoError(t, err)
				id = created.ID
			case "sdk":
				id, err = docker.CreateContainer(ctx, CreateContainerParams{
					Image: admitted, LeaseUUID: uuid.NewString(), ServiceName: "app", User: "10000:10000",
					Manifest:       &manifest.Manifest{Image: tag, Command: []string{"/probe"}, Args: args},
					ReadonlyRootfs: true, TmpfsSizeMB: 1, VolumeBinds: map[string]string{dataPath: "/data"},
					NetworkConfig: probeNetwork(),
				}, 30*time.Second)
				require.NoError(t, err)
			case "compose":
				compose, err := newComposeService(sdk.DaemonHost(), docker.images, docker.tenantSeccomp)
				require.NoError(t, err)
				params := baseProjectParams()
				params.LeaseUUID = uuid.NewString()
				params.NetworkName = ""
				params.Stack.Services["web"] = &manifest.Manifest{Image: tag, Command: []string{"/probe"}, Args: args}
				params.ImageSetups["web"] = &imageSetup{Image: admitted, ContainerUser: "10000:10000"}
				params.VolBinds = map[string]map[int]serviceVolBinds{
					"web": {0: {StatefulBinds: map[string]string{dataPath: "/data"}}},
				}
				project, err := compose.PrepareProject(buildTestComposeProject(t, params), map[string]imageexec.Image{"web": admitted})
				require.NoError(t, err)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
					defer cancel()
					require.NoError(t, compose.Down(ctx, project.Name(), time.Second))
				})
				require.NoError(t, compose.launch(ctx, project, composeUpOpts{}).err)
				containers, err := compose.PS(ctx, project.Name())
				require.NoError(t, err)
				require.Len(t, containers, 1)
				id = containers[0].ID
			}
			if launch != "compose" {
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
					defer cancel()
					require.NoError(t, docker.RemoveContainer(ctx, id))
				})
				require.NoError(t, docker.StartContainer(ctx, id, 30*time.Second))
			}
			results := waitXFSProjectIDProbe(t, ctx, sdk, id)
			wantErrno := int(unix.EPERM)
			if launch == "default-profile-control" {
				wantErrno = 0
			}
			require.Equal(t, map[string]int{
				"file-zero": wantErrno, "file-foreign": wantErrno,
				"dir-fsxattr": wantErrno, "dir-setflags": wantErrno,
			}, results)
			for _, name := range []string{"file-zero", "file-foreign", "dir-fsxattr", "dir-setflags"} {
				attr := integrationXFSAttributes(t, filepath.Join(dataPath, name))
				wantID := volume.projID
				if launch == "default-profile-control" && name == "file-zero" {
					wantID = 0
				} else if launch == "default-profile-control" && name == "file-foreign" {
					wantID = other.projID
				}
				require.Equal(t, wantID, attr.ProjectID, name)
				if name == "dir-fsxattr" || name == "dir-setflags" {
					require.Equal(t, launch != "default-profile-control", attr.XFlags&linuxFSXFlagProjInherit != 0, name)
					if launch != "default-profile-control" {
						child := integrationXFSAttributes(t, filepath.Join(dataPath, name, "child"))
						require.Equal(t, volume.projID, child.ProjectID, "new writes must remain charged to the tenant")
					}
				}
			}
		})
	}
}
