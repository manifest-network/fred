//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// Validate Docker's archive semantics as well as the absence of volume copies.
// A cleanup-only assertion would miss peak allocation, and an empty-directory
// fixture would miss an override that hid the image content we need to inspect.
func TestIntegration_ImageInspectionPreservesContentWithoutVolumeCopies(t *testing.T) {
	for _, store := range []string{"host", "classic"} {
		t.Run(store, func(t *testing.T) {
			sdk := newImageSecurityFixtureClient(t)
			if store == "classic" {
				sdk = newClassicImageTestDaemon(t)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			if _, err := sdk.Ping(ctx); err != nil {
				t.Skipf("Docker daemon unavailable: %v", err)
			}
			ref := importInspectionContentFixture(t, ctx, sdk)
			docker, err := NewDockerClient(ctx, sdk.DaemonHost(), "")
			require.NoError(t, err)
			t.Cleanup(func() { _ = docker.Close() })
			h := newIntegrationInspectionHarness(t, docker, ref)
			h.execute(t, func(work context.Context, origin shared.ImageInspectionOrigin) error {
				session, err := docker.openImageInspection(work, h.image, origin)
				require.NoError(t, err)
				defer func() { require.NoError(t, session.close()) }()
				actual, err := sdk.ContainerInspect(work, session.containerID)
				require.NoError(t, err)
				require.Equal(t, "created", actual.State.Status)
				require.Equal(t, "/", actual.Config.WorkingDir)
				require.Equal(t, container.RestartPolicyDisabled, actual.HostConfig.RestartPolicy.Name)
				for _, mounted := range actual.Mounts {
					require.NotEqual(t, "volume", string(mounted.Type), "assert the live allocation, before cleanup")
				}
				require.Len(t, actual.HostConfig.Tmpfs, 3)
				passwd, err := session.readFile(work, "/etc/passwd")
				require.NoError(t, err)
				require.Contains(t, string(passwd), "app:x:1234:2345:")
				proof, err := session.readFile(work, "/data/nested/proof")
				require.NoError(t, err)
				require.Equal(t, "original image content\n", string(proof))
				workdir, _, err := session.copy(work, "/image-workdir-must-stay-absent")
				if workdir != nil {
					_ = workdir.Close()
				}
				require.True(t, errdefs.IsNotFound(err), "Create must not materialize image WORKDIR: %v", err)
				uid, gid, err := docker.ResolveImageUser(work, h.image, "", origin)
				require.NoError(t, err)
				require.Equal(t, 1234, uid)
				require.Equal(t, 2345, gid)
				uid, gid, err = docker.DetectVolumeOwner(work, h.image, []string{"/data", "/other"}, origin)
				require.NoError(t, err)
				require.Equal(t, 1234, uid)
				require.Equal(t, 2345, gid)
				paths, err := docker.DetectWritablePaths(work, h.image, 1234, []string{"/data"}, origin)
				require.NoError(t, err)
				require.Contains(t, paths, "/data/nested")
				destination := t.TempDir()
				require.Empty(t, docker.ExtractImageContent(work, h.image, []string{"/data"}, destination, 1<<20, 100, origin))
				extracted, err := os.ReadFile(filepath.Join(destination, "data/nested/proof"))
				require.NoError(t, err)
				require.Equal(t, proof, extracted)
				return nil
			})
		})
	}
}

func importInspectionContentFixture(t *testing.T, ctx context.Context, sdk *client.Client) string {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, name := range []string{"etc", "data", "data/nested", "other"} {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755, Uid: 1234, Gid: 2345}))
	}
	for name, value := range map[string]string{
		"etc/passwd":        "app:x:1234:2345::/data:/bin/false\n",
		"etc/group":         "appgroup:x:2345:\n",
		"data/nested/proof": "original image content\n",
		"other/proof":       "second image volume\n",
	} {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(value)), Uid: 1234, Gid: 2345}))
		_, err := io.WriteString(writer, value)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	ref := "fred-inspection-contract:" + uuid.NewString()
	stream, err := sdk.ImageImport(ctx, image.ImportSource{Source: &archive, SourceName: "-"}, ref, image.ImportOptions{Changes: []string{
		`VOLUME ["/etc", "/data", "/other"]`, `USER app:appgroup`,
		`WORKDIR /image-workdir-must-stay-absent`, `CMD ["/never-start"]`,
	}})
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	decoder := json.NewDecoder(stream)
	for {
		var result struct{ Error string }
		if err := decoder.Decode(&result); err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
		require.Empty(t, result.Error)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = sdk.ImageRemove(cleanup, ref, image.RemoveOptions{Force: false})
	})
	return ref
}
