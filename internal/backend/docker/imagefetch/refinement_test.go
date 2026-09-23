package imagefetch

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func (f *registryFixture) updateImage(t *testing.T, update func(*ocispec.Image, *ocispec.Manifest)) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var cfg ocispec.Image
	var manifest ocispec.Manifest
	require.NoError(t, json.Unmarshal(f.config, &cfg))
	require.NoError(t, json.Unmarshal(f.manifest, &manifest))
	update(&cfg, &manifest)
	f.config = mustJSON(t, cfg)
	f.configID = digest.FromBytes(f.config)
	manifest.Config.Digest = f.configID
	manifest.Config.Size = int64(len(f.config))
	f.manifest = mustJSON(t, manifest)
	f.manifestID = digest.FromBytes(f.manifest)
}

func TestRepeatedLayerDescriptorsStageOnceAndPreserveReferences(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	f.updateImage(t, func(cfg *ocispec.Image, m *ocispec.Manifest) {
		cfg.RootFS.DiffIDs = []digest.Digest{cfg.RootFS.DiffIDs[0], cfg.RootFS.DiffIDs[0], cfg.RootFS.DiffIDs[0]}
		m.Layers = []ocispec.Descriptor{m.Layers[0], m.Layers[0], m.Layers[0]}
	})
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer p.Close()
	count := 0
	for _, blob := range p.state.blobs {
		if blob.name == blobPath(f.layerID) {
			count++
		}
	}
	require.Equal(t, 1, count)
	_, err = loader.Import(t.Context(), p)
	require.NoError(t, err)
	var legacy []struct{ Layers []string }
	require.NoError(t, json.Unmarshal(daemon.archive["manifest.json"], &legacy))
	require.Equal(t, []string{blobPath(f.layerID), blobPath(f.layerID), blobPath(f.layerID)}, legacy[0].Layers)
	require.Equal(t, f.manifest, daemon.archive[blobPath(f.manifestID)])
	require.GreaterOrEqual(t, p.ImportBytes(), int64(4*128<<10), "every layer occurrence retains daemon metadata allowance")
}

func TestRepeatedLayerMustMatchItsOriginalDescriptorAndDiffID(t *testing.T) {
	for _, change := range []string{"size", "media type", "diffID"} {
		t.Run(change, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			f.updateImage(t, func(cfg *ocispec.Image, m *ocispec.Manifest) {
				cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, cfg.RootFS.DiffIDs[0])
				m.Layers = append(m.Layers, m.Layers[0])
				switch change {
				case "size":
					m.Layers[1].Size++
				case "media type":
					m.Layers[1].MediaType = ocispec.MediaTypeImageLayer
				case "diffID":
					cfg.RootFS.DiffIDs[1] = digest.FromString("other")
				}
			})
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
		})
	}
}

func TestResolutionBindsSelectionAcrossMutableTagChanges(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	original := f.manifestID.String()
	f.mu.Lock()
	f.manifest = []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	f.mu.Unlock()
	prepared, err := loader.PrepareResolved(t.Context(), resolved)
	require.NoError(t, err)
	defer prepared.Close()
	require.Equal(t, original, resolved.ManifestID())
	require.Equal(t, original, prepared.ManifestID())
	require.Equal(t, resolved.SourceReference(), prepared.SourceReference())
	require.Equal(t, resolved.Platform(), prepared.Platform())
	require.True(t, prepared.Metadata().Valid())
	other, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	_, err = other.PrepareResolved(t.Context(), resolved)
	require.ErrorContains(t, err, "foreign")
	_, err = loader.PrepareResolved(t.Context(), Resolution{})
	require.Error(t, err)
}

func TestMetadataAdmissionPrecedesLayerDownloadAndDaemonMutation(t *testing.T) {
	for _, scenario := range []string{"reserved label", "volume path", "volume count"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("content")))
			f.updateImage(t, func(cfg *ocispec.Image, _ *ocispec.Manifest) {
				switch scenario {
				case "reserved label":
					cfg.Config.Labels = map[string]string{"fred.lease_id": "forged"}
				case "volume path":
					cfg.Config.Volumes = map[string]struct{}{"/proc": {}}
				case "volume count":
					cfg.Config.Volumes = make(map[string]struct{})
					for i := range 17 {
						cfg.Config.Volumes[fmt.Sprintf("/data%d", i)] = struct{}{}
					}
				}
			})
			f.replaceBlob = []byte("never admissible")
			daemon := &recordingImporter{}
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.ErrorContains(t, err, "admit image configuration")
			require.Zero(t, daemon.loads)
			pending, err := loader.PendingBytes()
			require.NoError(t, err)
			require.Zero(t, pending)
		})
	}
}

func TestImportAllowanceCapsFilesystemMetadataExpansion(t *testing.T) {
	f := newRegistry(t, encodedTar(t, tar.Header{Name: strings.Repeat("d/", 200) + "file", Typeflag: tar.TypeReg}))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "import allocation exceeds")
	require.Zero(t, daemon.loads)
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending)
}

func TestRegistryRefusesPrivateIPHTTPFallbackAndDowngrade(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(fmt.Sprintf("redirect=%t", redirect), func(t *testing.T) {
			var httpRequests atomic.Int64
			plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { httpRequests.Add(1); w.WriteHeader(http.StatusOK) }))
			defer plain.Close()
			registryURL := plain.URL
			var opts []Option
			if redirect {
				secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, plain.URL+r.URL.Path, http.StatusTemporaryRedirect)
				}))
				defer secure.Close()
				registryURL = secure.URL
				opts = append(opts, WithRegistryTransport(secure.Client().Transport))
			}
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, opts...)
			require.NoError(t, err)
			_, err = loader.Resolve(t.Context(), strings.TrimPrefix(strings.TrimPrefix(registryURL, "https://"), "http://")+"/tenant/image:latest", testPlatform)
			require.Error(t, err)
			require.Zero(t, httpRequests.Load(), "HTTP fallback/redirect must never reach the network transport")
		})
	}
}

func TestGlobalPAXMetadataMatchesDaemonIgnoreSemantics(t *testing.T) {
	raw := encodedTar(t, tar.Header{Name: "GlobalHead.0", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "archive metadata", "path": "ignored", "SCHILY.xattr.trusted.overlay.metacopy": ""}}, tar.Header{Name: "app", Typeflag: tar.TypeReg, Size: 12})
	budget := &layerBudget{remaining: 1 << 20}
	require.NoError(t, checkLayer(t, budget, raw))
	require.NotContains(t, budget.root.children, "ignored")
	require.NotContains(t, budget.root.children, "GlobalHead.0")
	require.Contains(t, budget.root.children, "app")
	oversized := encodedTar(t, tar.Header{Name: "GlobalHead.1", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": strings.Repeat("x", maxHeaderBytes)}})
	require.ErrorContains(t, checkLayer(t, &layerBudget{remaining: 1 << 20}, oversized), "header exceeds metadata budget")
}

func TestImportAdmissionCopiesShareSingleUseAndUndispatchedRelease(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer p.Close()
	admitted, err := loader.ReserveImport(t.Context(), p)
	require.NoError(t, err)
	copy := *admitted
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, p.ImportBytes(), pending)
	unknown, err := loader.UnknownBytes()
	require.NoError(t, err)
	require.Zero(t, unknown)
	require.NoError(t, copy.Close())
	_, err = loader.ImportAdmitted(t.Context(), admitted)
	require.ErrorContains(t, err, "closed")
	require.NoError(t, admitted.Close())
	pending, err = loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending)
	require.Zero(t, daemon.loads)
	require.NoError(t, loader.Shutdown(t.Context()))
}

func TestLoaderShutdownOwnsImportGraceAndUnknownDebit(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	transport := f.server.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	stage := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		daemon := &coordinatedImporter{arrivals: make(chan context.Context, 1), results: make(chan error)}
		loader, err := NewLoader(daemon, stage, 1<<20, WithRegistryTransport(transport))
		require.NoError(t, err)
		p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
		require.NoError(t, err)
		defer p.Close()
		imported := make(chan error, 1)
		go func() { _, err := loader.Import(t.Context(), p); imported <- err }()
		work := <-daemon.arrivals
		synctest.Wait()
		require.NoError(t, work.Err())
		drained := make(chan error, 1)
		go func() { drained <- loader.Shutdown(t.Context()) }()
		synctest.Wait()
		require.NoError(t, work.Err())
		time.Sleep(29 * time.Second)
		require.NoError(t, work.Err())
		time.Sleep(time.Second)
		synctest.Wait()
		require.Error(t, <-imported)
		require.NoError(t, <-drained)
		require.ErrorIs(t, work.Err(), context.Canceled)
		pending, err := loader.PendingBytes()
		require.NoError(t, err)
		require.Equal(t, p.ImportBytes(), pending)
		unknown, err := loader.UnknownBytes()
		require.NoError(t, err)
		require.Equal(t, pending, unknown)
	})
}

func TestUnknownDebitIsNotHiddenBySettlingAnotherAdmission(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	require.NoError(t, loader.changeDebit(123))
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer p.Close()
	admitted, err := loader.ReserveImport(t.Context(), p)
	require.NoError(t, err)
	unknown, err := loader.UnknownBytes()
	require.NoError(t, err)
	require.Equal(t, int64(123), unknown)
	_, err = loader.ImportAdmitted(t.Context(), admitted)
	require.NoError(t, err)
	unknown, err = loader.UnknownBytes()
	require.NoError(t, err)
	require.Equal(t, int64(123), unknown)
}

func TestPreparedCloseAndReservationShareOneLifetime(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	for range 16 {
		prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
		require.NoError(t, err)
		start := make(chan struct{})
		closed := make(chan error, 1)
		reserved := make(chan *ImportAdmission, 1)
		go func() { <-start; closed <- prepared.Close() }()
		go func() { <-start; admission, _ := loader.ReserveImport(t.Context(), prepared); reserved <- admission }()
		close(start)
		require.NoError(t, <-closed)
		admission := <-reserved
		pending, err := loader.PendingBytes()
		require.NoError(t, err)
		require.Zero(t, pending, "closing preparation must settle any concurrently created undispatched reservation")
		require.NoError(t, admission.Close())
	}
	require.Zero(t, daemon.loads)
}

func TestCopiedImportAdmissionCannotDispatchTwiceOrThroughForeignLoader(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	admitted, err := loader.ReserveImport(t.Context(), prepared)
	require.NoError(t, err)
	copied := *admitted
	other, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.NoError(t, err)
	_, err = other.ImportAdmitted(t.Context(), &copied)
	require.ErrorContains(t, err, "foreign")
	_, err = loader.ImportAdmitted(t.Context(), &copied)
	require.NoError(t, err)
	_, err = loader.ImportAdmitted(t.Context(), admitted)
	require.ErrorContains(t, err, "consumed")
	require.NoError(t, admitted.Close())
	require.Equal(t, 1, daemon.loads)
}

func TestRecoveryBudgetSharesDebitAndShutdownWithoutMutatingAdmissionLimit(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	original, err := NewLoader(&recordingImporter{}, t.TempDir(), 64<<10, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	recovery, err := original.WithBudget(1 << 20)
	require.NoError(t, err)
	_, err = original.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "import allocation exceeds")
	prepared, err := recovery.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	_, err = original.ReserveImport(t.Context(), prepared)
	require.ErrorContains(t, err, "foreign")
	admitted, err := recovery.ReserveImport(t.Context(), prepared)
	require.NoError(t, err)
	pending, err := original.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, prepared.ImportBytes(), pending)
	unknown, err := original.UnknownBytes()
	require.NoError(t, err)
	require.Zero(t, unknown)
	require.NoError(t, admitted.Close())
	require.NoError(t, original.Shutdown(t.Context()))
	another, err := recovery.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer another.Close()
	_, err = recovery.ReserveImport(t.Context(), another)
	require.ErrorContains(t, err, "shut down")
	_, err = original.WithBudget(0)
	require.Error(t, err)
}
