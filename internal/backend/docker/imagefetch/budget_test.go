package imagefetch

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func imageBudgetPadding() []byte {
	block := make([]byte, 64<<10)
	for off := 0; off < len(block); off += sha256.Size {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(off))
		sum := sha256.Sum256(seed[:])
		copy(block[off:], sum[:])
	}
	return bytes.Repeat(block, 32)
}

func zstdBudgetRegistry(t *testing.T, raw []byte, repeats int) *registryFixture {
	t.Helper()
	f := newRegistry(t, raw)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	require.NoError(t, err)
	f.compressed = encoder.EncodeAll(raw, nil)
	encoder.Close()
	f.layerID = digest.FromBytes(f.compressed)
	f.updateImage(t, func(cfg *ocispec.Image, manifest *ocispec.Manifest) {
		cfg.RootFS.DiffIDs = nil
		manifest.Layers = nil
		for range repeats {
			cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, digest.FromBytes(raw))
			manifest.Layers = append(manifest.Layers, ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayerZstd, Digest: f.layerID, Size: int64(len(f.compressed))})
		}
	})
	return f
}

func TestImageBudgetExactRecoveryCoversDecodedPadding(t *testing.T) {
	for _, padding := range [][]byte{bytes.Repeat([]byte{0}, 2<<20), imageBudgetPadding()} {
		for _, repeats := range []int{1, 2} {
			raw := append(layerTar(t, []byte("content")), padding...)
			f := zstdBudgetRegistry(t, raw, repeats)
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<30, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			defer p.Close()
			budget := p.Budget()
			require.GreaterOrEqual(t, budget.Verification().Bytes(), int64(len(raw)*repeats))
			// Serialized fields preserve their dimensions through reopening.
			saved, err := imagebudget.Decode(imagebudget.Stored{VerificationBytes: budget.Verification().Bytes(), ImportBytes: budget.Allocation().Bytes()})
			require.NoError(t, err)
			recovery, err := loader.WithBudget(saved.Verification())
			require.NoError(t, err)
			recovered, err := recovery.Prepare(t.Context(), p.SourceReference(), p.Platform())
			require.NoError(t, err, "the same immutable content must fit its saved verification budget")
			require.NoError(t, recovered.Close())
		}
	}
}

func TestImageBudgetExactRecoveryCoversCompressibleFilePayload(t *testing.T) {
	f := newRegistry(t, layerTar(t, bytes.Repeat([]byte{0}, 4<<20)))
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 8<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	again, err := recovery.Prepare(t.Context(), prepared.SourceReference(), prepared.Platform())
	require.NoError(t, err)
	require.NoError(t, again.Close())
}

func TestImageBudgetExactRecoveryCoversCompressedSkippableFrames(t *testing.T) {
	f := zstdBudgetRegistry(t, layerTar(t, []byte("content")), 1)
	// Zstd skippable frames consume staging bytes while decoding to no bytes.
	// They make the compressed/staged dimension independently load-bearing.
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], 0x184D2A50)
	binary.LittleEndian.PutUint32(header[4:], 2<<20)
	f.compressed = append(f.compressed, header[:]...)
	f.compressed = append(f.compressed, bytes.Repeat([]byte{'x'}, 2<<20)...)
	f.layerID = digest.FromBytes(f.compressed)
	f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
		manifest.Layers[0].Digest = f.layerID
		manifest.Layers[0].Size = int64(len(f.compressed))
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 4<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	again, err := recovery.Prepare(t.Context(), prepared.SourceReference(), prepared.Platform())
	require.NoError(t, err, "the saved staging dimension must include skippable compressed frames")
	require.NoError(t, again.Close())
}

func TestLayerAllocationCoversRetainedGlobalHeaderJSON(t *testing.T) {
	// Global PAX entries create no filesystem nodes, but tar-split retains
	// their entry names. JSON escaping can expand one name byte to six.
	for _, size := range []int{2000, 32000} {
		name := strings.Repeat("\x01", size)
		raw := encodedTar(t, tar.Header{Name: name, Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "metadata"}})
		budget := &layerBudget{remaining: 1 << 20}
		require.NoError(t, checkLayer(t, budget, raw))
		entry, err := json.Marshal(struct {
			Type     int    `json:"type"`
			Name     string `json:"name"`
			Position int    `json:"position"`
		}{Type: 1, Name: name})
		require.NoError(t, err)
		require.Greater(t, budget.allocated, int64(len(entry)), "the allocation must cover retained entry JSON even with no filesystem entry")
	}
}

func TestLayerAllocationCoversIncompressibleTarSplitSegments(t *testing.T) {
	padding := bytes.Repeat(imageBudgetPadding(), 4)
	raw := append(encodedTar(t), padding...)
	budget := &layerBudget{remaining: 16 << 20}
	require.NoError(t, checkLayer(t, budget, raw))
	// Use tar-split's stored segment shape and gzip encoding to measure the
	// retained bytes independently of the allocation formula.
	var retained bytes.Buffer
	compressor := gzip.NewWriter(&retained)
	encoder := json.NewEncoder(compressor)
	for start := 0; start < len(raw); start += 1 << 20 {
		end := min(start+(1<<20), len(raw))
		require.NoError(t, encoder.Encode(struct {
			Type     int    `json:"type"`
			Payload  []byte `json:"payload"`
			Position int    `json:"position"`
		}{Type: 2, Payload: raw[start:end], Position: start / (1 << 20)}))
	}
	require.NoError(t, compressor.Close())
	require.Greater(t, retained.Len(), len(raw), "fixture must expose base64/gzip expansion")
	require.Greater(t, budget.allocated, int64(retained.Len()))
}

func TestImageAllocationCoversTarSplitPaddingAcrossUnknownReopen(t *testing.T) {
	padding := imageBudgetPadding()
	// Moby's tar-split writes raw post-EOF segments as JSON/base64 into gzip.
	// Measure the retained padding alone, independent of Fred's formula.
	var retained bytes.Buffer
	compressed := gzip.NewWriter(&retained)
	encoder := json.NewEncoder(compressed)
	for pos := range 2 {
		require.NoError(t, encoder.Encode(struct {
			Type     int    `json:"type"`
			Payload  []byte `json:"payload"`
			Position int    `json:"position"`
		}{Type: 2, Payload: padding[pos*(1<<20) : (pos+1)*(1<<20)], Position: pos}))
	}
	require.NoError(t, compressed.Close())
	f := zstdBudgetRegistry(t, append(layerTar(t, []byte("content")), padding...), 2)
	stage := t.TempDir()
	loader, err := NewLoader(&recordingImporter{result: `{"stream":`}, stage, 1<<30, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Greater(t, prepared.ImportBytes(), 2*int64(retained.Len()), "each layer occurrence retains metadata even when its compressed blob is shared")
	_, err = loader.Import(t.Context(), prepared)
	require.Error(t, err)
	require.NoError(t, prepared.Close())
	reopened, err := NewLoader(&recordingImporter{}, stage, 1<<30)
	require.NoError(t, err)
	unknown, err := reopened.UnknownBytes()
	require.NoError(t, err)
	require.Equal(t, prepared.ImportBytes(), unknown)
	require.Greater(t, unknown, 2*int64(retained.Len()), "unknown completion cannot drop the tar-split allowance")
}

func TestImageBudgetRejectsConfusedAndInvalidSavedDimensions(t *testing.T) {
	for _, saved := range [][2]int64{{-1, 1}, {1, -1}, {1, 0}} {
		_, err := imagebudget.Decode(imagebudget.Stored{VerificationBytes: saved[0], ImportBytes: saved[1]})
		require.Error(t, err)
	}
	legacy, err := imagebudget.Decode(imagebudget.Stored{ImportBytes: 1 << 20})
	require.NoError(t, err)
	require.False(t, legacy.Verification().Valid(), "legacy allocation cannot mint verification authority")
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	_, err = loader.WithBudget(legacy.Verification())
	require.Error(t, err)
}

func TestImportDebitLegacyPositiveRequiresOfflineRecovery(t *testing.T) {
	for _, amount := range []int64{0, 12345} {
		stage := t.TempDir()
		root, err := os.OpenRoot(stage)
		require.NoError(t, err)
		require.NoError(t, writeDebit(root, amount))
		require.NoError(t, root.Close())
		path := filepath.Join(stage, debitFileName)
		record, err := os.ReadFile(path)
		require.NoError(t, err)
		copy(record[:8], legacyDebitMagic)
		checksum := sha256.Sum256(record[:16])
		copy(record[16:], checksum[:])
		require.NoError(t, os.WriteFile(path, record, 0o600))
		loader, err := NewLoader(&recordingImporter{}, stage, 1<<20)
		require.NoError(t, err)
		pending, err := loader.PendingBytes()
		if amount > 0 {
			require.ErrorContains(t, err, "external runtime drain and offline debit recovery")
			require.Error(t, loader.changeDebit(1), "new imports cannot erase or mix incomplete old accounting")
			unchanged, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, record, unchanged)
			continue
		}
		require.NoError(t, err)
		require.Zero(t, pending)
		require.NoError(t, loader.changeDebit(7))
		record, err = os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, debitMagic, string(record[:8]))
		reopened, err := NewLoader(&recordingImporter{}, stage, 1<<20)
		require.NoError(t, err)
		pending, err = reopened.PendingBytes()
		require.NoError(t, err)
		require.Equal(t, int64(7), pending)
	}
}

func TestImageBudgetRejectsMalformedCompressionAfterTarEOF(t *testing.T) {
	f := newRegistry(t, append(layerTar(t, []byte("content")), bytes.Repeat([]byte{0}, 2<<20)...))
	f.compressed[len(f.compressed)-8] ^= 0x80 // invalid gzip CRC after the valid tar EOF
	f.layerID = digest.FromBytes(f.compressed)
	f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
		manifest.Layers[0].Digest = f.layerID
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 4<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "checksum")
}

func TestImageBudgetPreservesIndexAndArchiveMetadataBound(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	f.manifestResponse = mustJSON(t, ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
		Manifests:   []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: f.manifestID, Size: int64(len(f.manifest)), Platform: &testPlatform}},
		Annotations: map[string]string{"example.invalid/metadata": strings.Repeat("x", 1<<20)},
	})
	f.manifestType = ocispec.MediaTypeImageIndex
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 4<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	require.Greater(t, prepared.Budget().Verification().Bytes(), int64(len(f.manifestResponse)))
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	exact, err := recovery.Prepare(t.Context(), prepared.SourceReference(), testPlatform)
	require.NoError(t, err)
	require.NoError(t, exact.Close())
}
