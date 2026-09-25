//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/image"
	"github.com/google/go-containerregistry/pkg/compression"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func TestIntegration_ImageBudgetCoversClassicTarSplitAndExactRecovery(t *testing.T) {
	docker := newClassicImageTestDaemon(t)
	info, err := docker.Info(t.Context())
	require.NoError(t, err)
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref, err := name.ParseReference(strings.TrimPrefix(server.URL, "https://") + "/fred-budget:latest")
	require.NoError(t, err)
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "proof.txt", Mode: 0o644, Size: 1}))
	_, err = writer.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	// Repeated high-entropy blocks compress well with zstd's window but are
	// retained almost verbatim by classic Docker's gzip/base64 tar-split.
	block := make([]byte, 64<<10)
	for offset := 0; offset < len(block); offset += sha256.Size {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(offset))
		sum := sha256.Sum256(seed[:])
		copy(block[offset:], sum[:])
	}
	raw.Write(bytes.Repeat(block, 32))
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw.Bytes())), nil
	}, tarball.WithCompression(compression.ZStd), tarball.WithMediaType(types.OCILayerZStd))
	require.NoError(t, err)
	fixture, err := mutate.AppendLayers(empty.Image, layer, layer)
	require.NoError(t, err)
	config, err := fixture.ConfigFile()
	require.NoError(t, err)
	platform := daemonImagePlatform(info)
	config.OS, config.Architecture, config.Variant = platform.OS, platform.Architecture, platform.Variant
	fixture, err = mutate.ConfigFile(fixture, config)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, fixture, remote.WithContext(t.Context()), remote.WithTransport(server.Client().Transport)))
	loader, err := imagefetch.NewLoader(docker, t.TempDir(), 1024*imageMiB, imagefetch.WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), ref.Name(), platform)
	require.NoError(t, err)
	defer prepared.Close()
	imported, err := loader.Import(t.Context(), prepared)
	require.NoError(t, err, "classic Docker must accept the completely verified padded layer")
	var retained int64
	var metadataFiles int
	require.NoError(t, filepath.WalkDir(filepath.Join(info.DockerRootDir, "image", "overlay2", "layerdb"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == "tar-split.json.gz" {
			stat, err := entry.Info()
			if err != nil {
				return err
			}
			retained += stat.Size()
			metadataFiles++
		}
		return nil
	}))
	require.Equal(t, 2, metadataFiles, "repeated layer occurrences retain separate layer metadata")
	require.Greater(t, retained, int64(4*imageMiB), "the compressed registry blob is not a bound on retained tar-split")
	require.Less(t, retained, prepared.ImportBytes())
	t.Logf("classic tar-split retained %d bytes; complete import allowance %d; saved verification %d", retained, prepared.ImportBytes(), prepared.Budget().Verification().Bytes())
	_, err = docker.ImageRemove(t.Context(), imported.ConfigID(), image.RemoveOptions{Force: true, PruneChildren: true})
	require.NoError(t, err)
	_, err = docker.ImageInspect(t.Context(), imported.ConfigID())
	require.Error(t, err)
	saved, err := imagebudget.Decode(imagebudget.Stored{VerificationBytes: prepared.Budget().Verification().Bytes(), ImportBytes: prepared.ImportBytes()})
	require.NoError(t, err)
	recovery, err := loader.WithBudget(saved.Verification())
	require.NoError(t, err)
	recovered, err := recovery.Prepare(t.Context(), imported.SourceReference(), platform)
	require.NoError(t, err)
	defer recovered.Close()
	restored, err := recovery.Import(t.Context(), recovered)
	require.NoError(t, err)
	require.Equal(t, imported.ConfigID(), restored.ConfigID())
	_, err = docker.ImageInspect(t.Context(), imported.ConfigID())
	require.NoError(t, err)
}
