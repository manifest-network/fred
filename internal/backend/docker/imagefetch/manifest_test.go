package imagefetch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestResolutionRequiresRunnableManifestBeforeIssuingIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ocispec.Manifest)
	}{
		{"artifact", func(m *ocispec.Manifest) { m.ArtifactType = "application/vnd.example.artifact" }},
		{"subject", func(m *ocispec.Manifest) { subject := m.Config; m.Subject = &subject }},
		{"schema", func(m *ocispec.Manifest) { m.SchemaVersion = 1 }},
		{"config media", func(m *ocispec.Manifest) { m.Config.MediaType = "application/octet-stream" }},
		{"config size", func(m *ocispec.Manifest) { m.Config.Size = 0 }},
		{"config digest", func(m *ocispec.Manifest) { m.Config.Digest = "invalid" }},
		{"config URL", func(m *ocispec.Manifest) { m.Config.URLs = []string{"https://elsewhere.example/config"} }},
		{"layer media", func(m *ocispec.Manifest) { m.Layers[0].MediaType = "application/octet-stream" }},
		{"layer digest", func(m *ocispec.Manifest) { m.Layers[0].Digest = "invalid" }},
		{"layer size", func(m *ocispec.Manifest) { m.Layers[0].Size = -1 }},
		{"layer URL", func(m *ocispec.Manifest) { m.Layers[0].URLs = []string{"https://elsewhere.example/layer"} }},
		{"embedded layer", func(m *ocispec.Manifest) { m.Layers[0].Data = []byte("embedded") }},
		{"conflicting repeat", func(m *ocispec.Manifest) { m.Layers = append(m.Layers, m.Layers[0]); m.Layers[1].Size++ }},
		{"layer count", func(m *ocispec.Manifest) {
			layer := m.Layers[0]
			m.Layers = make([]ocispec.Descriptor, maxLayers+1)
			for i := range m.Layers {
				m.Layers[i] = layer
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			var manifest ocispec.Manifest
			require.NoError(t, json.Unmarshal(f.manifest, &manifest))
			tc.change(&manifest)
			f.manifest = mustJSON(t, manifest)
			f.manifestID = digest.FromBytes(f.manifest)
			daemon := &recordingImporter{}
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
			require.Empty(t, resolved.ConfigID())
			require.Empty(t, resolved.SourceReference())
			require.Zero(t, daemon.loads)
		})
	}
}

func TestResolutionDoesNotSpendNewDownloadBudgetOnExistingLayers(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	f.updateImage(t, func(_ *ocispec.Image, m *ocispec.Manifest) { m.Layers[0].Size = 2 << 20 })
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err, "structural selection does not download or re-admit an already cached image")
	require.Equal(t, f.configID.String(), resolved.ConfigID())
	_, err = loader.PrepareResolved(t.Context(), resolved)
	require.ErrorContains(t, err, "staging budget", "new ingestion still requires its own byte allowance")
}

func TestResolutionRequiresConfigProofBeforeIssuingIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ocispec.Image)
	}{
		{"root type", func(c *ocispec.Image) { c.RootFS.Type = "unknown" }},
		{"layer count", func(c *ocispec.Image) { c.RootFS.DiffIDs = nil }},
		{"platform", func(c *ocispec.Image) { c.OS = "windows" }},
		{"diffID", func(c *ocispec.Image) { c.RootFS.DiffIDs[0] = "invalid" }},
		{"metadata", func(c *ocispec.Image) { c.Config.Labels = map[string]string{"fred.lease_id": "forged"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			f.updateImage(t, func(c *ocispec.Image, _ *ocispec.Manifest) { tc.change(c) })
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			resolution, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
			require.Empty(t, resolution.SourceReference())
			require.Empty(t, resolution.ConfigID())
		})
	}
}

func TestResolutionOwnsConfigProofWithoutRefetching(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	resolution, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	f.mu.Lock()
	f.config = []byte("registry configuration changed after selection")
	f.mu.Unlock()
	copyOfResolution := resolution
	prepared, err := loader.PrepareResolved(t.Context(), copyOfResolution)
	require.NoError(t, err, "preparation consumes immutable config evidence already owned by the selection")
	require.Equal(t, resolution.ConfigID(), prepared.ConfigID())
	require.NoError(t, prepared.Close())
}

func TestResolutionBindsOneDecodedIdentityToEachLayer(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprint(repeated), func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			f.updateImage(t, func(c *ocispec.Image, m *ocispec.Manifest) {
				if repeated {
					m.Layers = append(m.Layers, m.Layers[0])
					c.RootFS.DiffIDs = append(c.RootFS.DiffIDs, digest.FromString("a different decoded identity"))
				} else {
					m.Layers[0].MediaType = ocispec.MediaTypeImageLayer
				}
			})
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			resolution, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.Error(t, err, "metadata-provable layer contradictions cannot issue a recovery selection")
			require.Empty(t, resolution.SourceReference())
		})
	}
}

func TestResolutionUsesMetadataAllowanceForHistoricalConfig(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	f.updateImage(t, func(c *ocispec.Image, _ *ocispec.Manifest) {
		c.Config.Env = []string{"PADDING=" + strings.Repeat("x", 1<<20)}
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	resolution, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err, "bounded metadata selection does not apply new layer-ingestion policy to historical config")
	_, err = loader.PrepareResolved(t.Context(), resolution)
	require.ErrorContains(t, err, "staging budget")
}
