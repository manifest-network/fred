package imagefetch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestConfigCacheSharesVerifiedBytesAcrossCopiesAndRecovery(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	var gets atomic.Int64
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, f.configID.String()) {
			gets.Add(1)
		}
		return f.server.Client().Transport.RoundTrip(r)
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, withRegistryTransportForTest(transport))
	require.NoError(t, err)
	resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.EqualValues(t, 1, gets.Load())
	f.mu.Lock()
	f.config = []byte("config CDN is now unavailable")
	f.mu.Unlock()
	copyOfLoader := *loader
	recovery, err := loader.WithBudget(loader.VerificationBudget())
	require.NoError(t, err)
	var workers sync.WaitGroup
	for i := range 12 {
		workers.Go(func() {
			owner := &copyOfLoader
			if i%2 == 0 {
				owner = recovery
			}
			again, err := owner.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			require.Equal(t, resolved.ConfigID(), again.ConfigID())
		})
	}
	workers.Wait()
	require.EqualValues(t, 1, gets.Load(), "cached digest evidence must not revisit the config CDN")
	// The cache is content evidence, not a manifest or platform admission.
	_, err = loader.Resolve(t.Context(), f.ref(), ocispec.Platform{OS: "linux", Architecture: "arm64"})
	require.ErrorContains(t, err, "selected platform")
}

func TestConfigCacheCannotAdmitDescriptorOrLayerContradictions(t *testing.T) {
	for _, change := range []string{"size", "layers"} {
		t.Run(change, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			_, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			var manifest ocispec.Manifest
			require.NoError(t, json.Unmarshal(f.manifest, &manifest))
			if change == "size" {
				manifest.Config.Size++
			} else {
				manifest.Layers = nil
			}
			f.mu.Lock()
			f.manifest = mustJSON(t, manifest)
			f.manifestID = digest.FromBytes(f.manifest)
			f.mu.Unlock()
			resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
			require.Empty(t, resolved.SourceReference())
		})
	}
}

func TestConfigCacheNeverRetainsFailedContent(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	original := f.config
	f.config = []byte(strings.Repeat("!", len(original)))
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	_, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.Error(t, err)
	require.Zero(t, loader.configs.bytes)
	f.mu.Lock()
	f.config = original
	f.mu.Unlock()
	_, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, len(original), loader.configs.bytes)
}

func TestManifestConfigEvidenceCannotBeJoinedToAnotherDescriptor(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	shape, err := parseManifestShape(f.manifest, int64(len(f.manifest)))
	require.NoError(t, err)
	foreign := verifiedConfig{digest: digest.FromString("another config"), raw: f.config}
	_, err = admitRunnableManifest(shape, foreign, testPlatform)
	require.ErrorContains(t, err, "does not belong")
	foreign.digest = shape.config.Digest
	shape.config.Size++
	_, err = admitRunnableManifest(shape, foreign, testPlatform)
	require.ErrorContains(t, err, "does not belong")
}

func TestConfigCacheEvictsWithinBothBoundsAndKeepsActiveEvidence(t *testing.T) {
	for _, size := range []int{1, int(maxMetadataBytes)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cache := &configCache{}
			first := verifiedConfig{digest: digest.FromString("first"), raw: []byte(strings.Repeat("x", size))}
			cache.retain(first)
			retained, ok := cache.get(first.digest)
			require.True(t, ok)
			for i := range maxCachedConfigs + 1 {
				cache.retain(verifiedConfig{digest: digest.FromString(fmt.Sprint(i)), raw: make([]byte, size)})
				require.LessOrEqual(t, cache.bytes, maxCachedConfigBytes)
				require.LessOrEqual(t, len(cache.entries), maxCachedConfigs)
			}
			_, ok = cache.get(first.digest)
			require.False(t, ok)
			require.Equal(t, strings.Repeat("x", size), string(retained.raw), "eviction does not mutate an active resolution's evidence")
		})
	}
}
