package imagefetch

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

var testPlatform = ocispec.Platform{OS: "linux", Architecture: "amd64"}

type recordingImporter struct {
	archive map[string][]byte
	loads   int
	result  string
}

func (d *recordingImporter) ImageLoad(_ context.Context, input io.Reader, _ ...client.ImageLoadOption) (image.LoadResponse, error) {
	d.loads++
	d.archive = make(map[string][]byte)
	r := tar.NewReader(input)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return image.LoadResponse{}, err
		}
		body, err := io.ReadAll(r)
		if err != nil {
			return image.LoadResponse{}, err
		}
		d.archive[h.Name] = body
	}
	result := d.result
	if result == "" {
		result = `{"stream":"Loaded image ID: sha256:test"}`
	}
	return image.LoadResponse{Body: io.NopCloser(strings.NewReader(result)), JSON: true}, nil
}

type registryFixture struct {
	mu                                  sync.Mutex
	server                              *httptest.Server
	manifest, config, compressed, layer []byte
	manifestID, configID, layerID       digest.Digest
	requests                            int
	replaceBlob                         []byte
	manifestResponse                    []byte
	manifestType                        string
	manifests                           map[string][]byte
}

func newRegistry(t *testing.T, layer []byte) *registryFixture {
	t.Helper()
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	_, err := w.Write(layer)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f := &registryFixture{layer: layer, compressed: compressed.Bytes()}
	f.layerID = digest.FromBytes(f.compressed)
	f.config = mustJSON(t, ocispec.Image{Platform: testPlatform, RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromBytes(layer)}}})
	f.configID = digest.FromBytes(f.config)
	f.manifest = mustJSON(t, ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: f.configID, Size: int64(len(f.config))}, Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: f.layerID, Size: int64(len(f.compressed))}}})
	f.manifestID = digest.FromBytes(f.manifest)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests++
		if r.URL.Path == "/v2/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if strings.Contains(r.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			if body, ok := f.manifests[filepath.Base(r.URL.Path)]; ok {
				var document struct{ MediaType string }
				_ = json.Unmarshal(body, &document)
				w.Header().Set("Content-Type", document.MediaType)
				_, _ = w.Write(body)
				return
			}
			if f.manifestResponse != nil && strings.HasSuffix(r.URL.Path, "/latest") {
				w.Header().Set("Content-Type", f.manifestType)
				_, _ = w.Write(f.manifestResponse)
				return
			}
			_, _ = w.Write(f.manifest)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		switch {
		case strings.HasSuffix(r.URL.Path, f.configID.String()):
			_, _ = w.Write(f.config)
		case strings.HasSuffix(r.URL.Path, f.layerID.String()):
			if f.replaceBlob != nil {
				_, _ = w.Write(f.replaceBlob)
			} else {
				_, _ = w.Write(f.compressed)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *registryFixture) ref() string {
	return strings.TrimPrefix(f.server.URL, "http://") + "/tenant/image:latest"
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func layerTar(t *testing.T, contents []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	require.NoError(t, w.WriteHeader(&tar.Header{Name: "app", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(contents))}))
	_, err := w.Write(contents)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestPrepareImportUsesOriginalVerifiedBytesAndIdentities(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("tenant content")))
	daemon := &recordingImporter{}
	stage := t.TempDir()
	loader, err := NewLoader(daemon, stage, 1<<20)
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	require.Equal(t, strings.Split(f.ref(), ":latest")[0]+"@"+f.manifestID.String(), p.SourceReference())
	require.Greater(t, p.ImportBytes(), int64(len(f.compressed)+len(f.layer)))
	// There is no visible staged content that another process can swap, and
	// changing registry responses after preparation cannot alter the import.
	entries, err := os.ReadDir(p.state.dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	f.mu.Lock()
	f.replaceBlob = bytes.Repeat([]byte("bomb"), 1<<20)
	requests := f.requests
	f.mu.Unlock()
	imported, err := loader.Import(t.Context(), p)
	require.NoError(t, err)
	require.Equal(t, f.manifestID.String(), imported.ManifestID())
	require.Equal(t, f.configID.String(), imported.ConfigID())
	require.Equal(t, testPlatform, imported.Platform())
	require.Equal(t, p.SourceReference(), imported.SourceReference())
	require.Equal(t, f.compressed, daemon.archive[blobPath(f.layerID)])
	require.Equal(t, f.manifest, daemon.archive[blobPath(f.manifestID)])
	require.Equal(t, f.config, daemon.archive[blobPath(f.configID)])
	var index ocispec.Index
	require.NoError(t, json.Unmarshal(daemon.archive["index.json"], &index))
	require.Equal(t, f.manifestID, index.Manifests[0].Digest)
	require.Equal(t, f.ref(), index.Manifests[0].Annotations[ocispec.AnnotationRefName])
	var legacy []struct {
		Config   string
		RepoTags []string
		Layers   []string
	}
	require.NoError(t, json.Unmarshal(daemon.archive["manifest.json"], &legacy))
	require.Equal(t, blobPath(f.configID), legacy[0].Config)
	require.Equal(t, []string{blobPath(f.layerID)}, legacy[0].Layers)
	require.Equal(t, []string{f.ref()}, legacy[0].RepoTags)
	f.mu.Lock()
	require.Equal(t, requests, f.requests)
	f.mu.Unlock()
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "consumed")
	require.NoError(t, p.Close())
	entries, err = os.ReadDir(stage)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, debitFileName, entries[0].Name())
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending)
}

func TestPreparedCapabilityRejectsZeroForeignAndClosed(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.NoError(t, err)
	other, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.NoError(t, err)
	for _, invalid := range []*Prepared{nil, {}} {
		_, err = loader.Import(t.Context(), invalid)
		require.Error(t, err)
	}
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	_, err = other.Import(t.Context(), p)
	require.ErrorContains(t, err, "foreign")
	require.NoError(t, p.Close())
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "closed")
	require.Zero(t, daemon.loads)
}

func TestPreparationRejectsExpansionTrailingDataAndLogicalBombs(t *testing.T) {
	var header bytes.Buffer
	w := tar.NewWriter(&header)
	require.NoError(t, w.WriteHeader(&tar.Header{Name: "huge", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1 << 40}))
	for _, tt := range []struct {
		name  string
		layer []byte
	}{
		{"compressed expansion", layerTar(t, bytes.Repeat([]byte{0}, 2<<20))},
		{"data after tar EOF", append(layerTar(t, []byte("small")), make([]byte, 2<<20)...)},
		{"logical header", header.Bytes()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRegistry(t, tt.layer)
			daemon := &recordingImporter{}
			stage := t.TempDir()
			loader, err := NewLoader(daemon, stage, 64<<10)
			require.NoError(t, err)
			_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
			require.Zero(t, daemon.loads)
			entries, err := os.ReadDir(stage)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestPreparationRejectsChangedOrOversizedRegistryBlob(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "hash mismatch", true: "oversized response"}[oversized], func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("content")))
			f.replaceBlob = bytes.Repeat([]byte("x"), len(f.compressed))
			if oversized {
				f.replaceBlob = bytes.Repeat([]byte("x"), 2<<20)
			}
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 64<<10)
			require.NoError(t, err)
			_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
		})
	}
}

func TestPreparationBoundsManifestHTTPBodyBeforeDecode(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	f.manifestResponse = bytes.Repeat([]byte(" "), int(maxMetadataBytes)+1)
	f.manifestType = ocispec.MediaTypeImageManifest
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 8<<20)
	require.NoError(t, err)
	_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "byte limit")
}

func TestImportPropagatesDaemonStreamFailure(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	loader, err := NewLoader(&recordingImporter{result: `{"errorDetail":{"message":"disk full"}}`}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "disk full")
}

func TestPrepareCancellationCreatesNoStage(t *testing.T) {
	stage := t.TempDir()
	loader, err := NewLoader(&recordingImporter{}, stage, 1<<20)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = loader.Prepare(ctx, "registry.invalid/image", testPlatform)
	require.ErrorIs(t, err, context.Canceled)
	entries, err := os.ReadDir(stage)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestCleanupAbandonedOnlyRemovesOwnedEmptyStages(t *testing.T) {
	stage := t.TempDir()
	loader, err := NewLoader(&recordingImporter{}, stage, 1<<20)
	require.NoError(t, err)
	empty, err := os.MkdirTemp(stage, ".fred-image-")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(empty, "blob-123"), nil, 0o600))
	unrelated := filepath.Join(stage, "leave-me")
	require.NoError(t, os.Mkdir(unrelated, 0o700))
	require.NoError(t, loader.CleanupAbandoned())
	_, err = os.Stat(empty)
	require.True(t, errors.Is(err, os.ErrNotExist))
	_, err = os.Stat(unrelated)
	require.NoError(t, err)
	unsafe, err := os.MkdirTemp(stage, ".fred-image-")
	require.NoError(t, err)
	file := filepath.Join(unsafe, "blob-123")
	require.NoError(t, os.WriteFile(file, []byte("preserve"), 0o600))
	require.ErrorContains(t, loader.CleanupAbandoned(), "unrecognized")
	contents, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(contents))
}

func TestPreparationSelectsImmutablePlatformLeaf(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("native content")))
	wrong := ocispec.Platform{OS: "linux", Architecture: "arm64"}
	f.manifestResponse = mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{
		{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromString("foreign"), Size: 1, Platform: &wrong},
		{MediaType: ocispec.MediaTypeImageManifest, Digest: f.manifestID, Size: int64(len(f.manifest)), Platform: &testPlatform},
	}})
	f.manifestType = ocispec.MediaTypeImageIndex
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()
	require.Contains(t, p.SourceReference(), f.manifestID.String())
	_, err = loader.Prepare(t.Context(), f.ref(), ocispec.Platform{OS: "linux", Architecture: "riscv64"})
	require.ErrorContains(t, err, "no matching platform")
}

func TestPreparationBoundsNestedImageIndexes(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("native content")))
	f.manifests = make(map[string][]byte)
	child := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: f.manifestID, Size: int64(len(f.manifest)), Platform: &testPlatform}
	for range maxIndexDepth + 1 {
		raw := mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{child}})
		id := digest.FromBytes(raw)
		f.manifests[id.String()] = raw
		f.manifestResponse = raw
		child = ocispec.Descriptor{MediaType: ocispec.MediaTypeImageIndex, Digest: id, Size: int64(len(raw)), Platform: &testPlatform}
	}
	f.manifestType = ocispec.MediaTypeImageIndex
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "nesting limit")
}

func TestNewLoaderRejectsTypedNilImporter(t *testing.T) {
	var daemon *recordingImporter
	_, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.Error(t, err)
}

func TestCopiedPreparedSharesImportAndCloseLifetime(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	copied := *p
	require.Equal(t, p.ImportBytes(), copied.ImportBytes())
	require.Equal(t, p.SourceReference(), copied.SourceReference())
	_, err = loader.Import(t.Context(), &copied)
	require.NoError(t, err)
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "consumed")
	require.Equal(t, 1, daemon.loads)
	require.NoError(t, copied.Close())
	require.NoError(t, p.Close())
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "closed")
}

type pausedImporter struct {
	started, release chan struct{}
	underlying       recordingImporter
}

func (d *pausedImporter) ImageLoad(ctx context.Context, input io.Reader, opts ...client.ImageLoadOption) (image.LoadResponse, error) {
	close(d.started)
	select {
	case <-d.release:
	case <-ctx.Done():
		return image.LoadResponse{}, ctx.Err()
	}
	return d.underlying.ImageLoad(ctx, input, opts...)
}

func TestCopiedPreparedCloseWaitsForActiveImport(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &pausedImporter{started: make(chan struct{}), release: make(chan struct{})}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20)
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	copied := *p
	imported := make(chan error, 1)
	go func() { _, err := loader.Import(t.Context(), p); imported <- err }()
	<-daemon.started
	// The copied value must expose the same busy state mutex; otherwise its
	// Close could release read handles still owned by the active import.
	if copied.state.mu.TryLock() {
		copied.state.mu.Unlock()
		t.Fatal("copied capability duplicated its lifetime mutex")
	}
	closed := make(chan error, 1)
	go func() { closed <- copied.Close() }()
	close(daemon.release)
	require.NoError(t, <-imported)
	require.NoError(t, <-closed)
	require.Equal(t, f.compressed, daemon.underlying.archive[blobPath(f.layerID)])
	require.NoError(t, p.Close())
}
