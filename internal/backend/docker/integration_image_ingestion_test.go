//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

// This checks both original OCI identity (containerd) and original config
// identity (classic stores) against the daemon actually running the suite.
func TestIntegration_BoundedImageImportPreservesIdentity(t *testing.T) {
	docker, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	if _, err := docker.Ping(t.Context()); err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	verifyBoundedImageImport(t, docker)
}

// Production deployments use classic overlay2. The main integration daemon
// deliberately uses containerd, so exercise the same verified archive against
// a second private classic daemon without changing the host's Docker service.
func TestIntegration_BoundedImageImportClassicOverlay2(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("a private classic Docker daemon requires root")
	}
	binary, err := exec.LookPath("dockerd")
	if err != nil {
		t.Skipf("dockerd unavailable: %v", err)
	}
	// Keep managed containerd's Unix socket below the kernel path limit.
	root, err := os.MkdirTemp("/tmp", "fred-import-docker-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	config := filepath.Join(root, "daemon.json")
	require.NoError(t, os.WriteFile(config, []byte(`{"features":{"containerd-snapshotter":false}}`), 0o600))
	log, err := os.Create(filepath.Join(root, "daemon.log"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	host := "unix://" + filepath.Join(root, "docker.sock")
	command := exec.Command(binary,
		"--config-file", config, "--data-root", filepath.Join(root, "data"),
		"--exec-root", filepath.Join(root, "exec"), "--pidfile", filepath.Join(root, "pid"),
		"--host", host, "--storage-driver", "overlay2", "--bridge=none",
		"--iptables=false", "--ip6tables=false", "--ip-forward=false", "--ip-masq=false",
	)
	command.Stdout, command.Stderr = log, log
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			<-done
		}
	})
	docker, err := client.NewClientWithOpts(client.WithHost(host), client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	ready := false
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, pingErr := docker.Ping(ctx)
		cancel()
		if pingErr == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		output, _ := os.ReadFile(filepath.Join(root, "daemon.log"))
		t.Fatalf("private classic Docker daemon did not start: %s", output)
	}
	info, err := docker.Info(t.Context())
	require.NoError(t, err)
	require.Equal(t, "overlay2", info.Driver)
	require.False(t, daemonUsesContainerd(info))
	verifyBoundedImageImport(t, docker)
}

func verifyBoundedImageImport(t *testing.T, docker *client.Client) {
	t.Helper()
	for _, digestOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("digest_only=%t", digestOnly), func(t *testing.T) {
			verifyBoundedImageImportReference(t, docker, digestOnly)
		})
	}
}

func verifyBoundedImageImportReference(t *testing.T, docker *client.Client, digestOnly bool) {
	t.Helper()
	info, err := docker.Info(t.Context())
	require.NoError(t, err)
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)
	ref, err := name.ParseReference(strings.TrimPrefix(server.URL, "http://") + "/fred-import:latest")
	require.NoError(t, err)
	var data bytes.Buffer
	writer := tar.NewWriter(&data)
	contents := []byte(newIntegrationLeaseUUID())
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "proof.txt", Mode: 0o644, Size: int64(len(contents))}))
	_, err = writer.Write(contents)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data.Bytes())), nil
	})
	require.NoError(t, err)
	fixture, err := mutate.AppendLayers(empty.Image, layer)
	require.NoError(t, err)
	config, err := fixture.ConfigFile()
	require.NoError(t, err)
	platform := daemonImagePlatform(info)
	config.OS, config.Architecture, config.Variant = platform.OS, platform.Architecture, platform.Variant
	fixture, err = mutate.ConfigFile(fixture, config)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, fixture, remote.WithContext(t.Context())))
	manifestID, err := fixture.Digest()
	require.NoError(t, err)
	configID, err := fixture.ConfigName()
	require.NoError(t, err)
	loader, err := imagefetch.NewLoader(docker, t.TempDir(), 8*imageMiB)
	require.NoError(t, err)
	source := ref.Name()
	if digestOnly {
		source = ref.Context().Digest(manifestID.String()).Name()
	}
	prepared, err := loader.Prepare(t.Context(), source, platform)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prepared.Close() })
	// No request after preparation can fetch another representation from the
	// registry. Docker must receive only the exact private, validated bytes.
	server.Close()
	imported, err := loader.Import(t.Context(), prepared)
	require.NoError(t, err)
	require.Equal(t, manifestID.String(), imported.ManifestID())
	require.Equal(t, configID.String(), imported.ConfigID())
	require.Equal(t, fmt.Sprintf("%s@%s", ref.Context().Name(), manifestID), imported.SourceReference())
	executionID := imported.ConfigID()
	if daemonUsesContainerd(info) {
		executionID = imported.ManifestID()
	}
	t.Cleanup(func() {
		_, _ = docker.ImageRemove(context.Background(), ref.Name(), image.RemoveOptions{PruneChildren: false})
		_, _ = docker.ImageRemove(context.Background(), executionID, image.RemoveOptions{PruneChildren: false})
	})
	inspection, err := docker.ImageInspect(t.Context(), executionID)
	require.NoError(t, err)
	require.Equal(t, executionID, inspection.ID)
	if daemonUsesContainerd(info) {
		require.NotNil(t, inspection.Descriptor)
		require.Equal(t, manifestID.String(), inspection.Descriptor.Digest.String())
	}
}
