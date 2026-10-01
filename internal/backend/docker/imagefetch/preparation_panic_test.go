package imagefetch

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestPreparationPanicClosesEveryStagedFileAndExchange(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("first verified layer")))
	second := digest.FromString("second blob")
	f.updateImage(t, func(cfg *ocispec.Image, manifest *ocispec.Manifest) {
		cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, digest.FromString("second decoded layer"))
		manifest.Layers = append(manifest.Layers, ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: second, Size: 123})
	})
	stage := t.TempDir()
	var exchange context.Context
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, second.String()) {
			exchange = req.Context()
			panic("registry transport panic")
		}
		return f.server.Client().Transport.RoundTrip(req)
	})
	loader, err := NewLoader(&recordingImporter{}, stage, 1<<20, withRegistryTransportForTest(transport))
	require.NoError(t, err)
	require.PanicsWithValue(t, "registry transport panic", func() {
		_, _ = loader.Prepare(t.Context(), f.ref(), testPlatform)
	})
	require.NotNil(t, exchange)
	require.ErrorIs(t, exchange.Err(), context.Canceled)
	entries, err := os.ReadDir(stage)
	require.NoError(t, err)
	require.Empty(t, entries, "panic unwinding retains directory cleanup ownership")
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil {
			require.NotContains(t, target, stage, "both the first staged layer and the current unlinked file must close synchronously")
		}
	}
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending)
}
