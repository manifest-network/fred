package imagefetch

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"math"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	"github.com/vbatts/tar-split/tar/asm"
	"github.com/vbatts/tar-split/tar/storage"
)

func gzipMembers(t *testing.T, prefix []byte, count int) ([]byte, []byte) {
	t.Helper()
	compress := func(raw []byte) []byte {
		var encoded bytes.Buffer
		writer := gzip.NewWriter(&encoded)
		_, err := writer.Write(raw)
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		return encoded.Bytes()
	}
	var members [256][]byte
	for i := range members {
		members[i] = compress([]byte{byte(i)})
	}
	encoded := compress(prefix)
	raw := bytes.Clone(prefix)
	for i := range count {
		value := byte(i * 73)
		encoded = append(encoded, members[value]...)
		raw = append(raw, value)
	}
	return encoded, raw
}

func TestTarSplitTailAllowanceCoversPrimaryCodecReadFragmentation(t *testing.T) {
	compressed, raw := gzipMembers(t, encodedTar(t), 200000)
	decoder, err := gzip.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer decoder.Close()
	var retained bytes.Buffer
	writer := gzip.NewWriter(&retained)
	stream, done, err := asm.NewInputTarStreamWithDone(decoder, storage.NewJSONPacker(writer), nil)
	require.NoError(t, err)
	defer stream.Close()
	_, err = io.Copy(io.Discard, stream)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.NoError(t, writer.Close())
	require.Greater(t, retained.Len(), 2*len(raw)+2*(128<<10), "read fragmentation exceeds the old raw-byte multiplier and fixed metadata margin")
	f := newRegistry(t, raw)
	f.compressed, f.layerID = compressed, digest.FromBytes(compressed)
	f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
		manifest.Layers[0].Digest = f.layerID
		manifest.Layers[0].Size = int64(len(compressed))
	})
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 64<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	var archive bytes.Buffer
	require.NoError(t, writeArchive(t.Context(), &archive, prepared.state.blobs))
	require.GreaterOrEqual(t, prepared.ImportBytes(), int64(archive.Len()+retained.Len()),
		"import authority must cover the simultaneously retained archive and tar-split metadata")
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	again, err := recovery.Prepare(t.Context(), prepared.SourceReference(), prepared.Platform())
	require.NoError(t, err)
	require.NoError(t, again.Close())
	require.Zero(t, daemon.loads)
}

func TestTarSplitTailByteChargeCoversEveryPrimaryOneByteRecord(t *testing.T) {
	// Segment records contain only Type, Payload and Position. Their maximum
	// position has 19 decimal digits; every possible one-byte payload is four
	// base64 bytes. Thus this exhausts the maximum per-byte JSON record shape,
	// including the newline. Larger payloads amortize the same fixed framing.
	for value := range 256 {
		var encoded bytes.Buffer
		require.NoError(t, json.NewEncoder(&encoded).Encode(storage.Entry{Type: storage.SegmentType, Payload: []byte{byte(value)}, Position: math.MaxInt}))
		require.LessOrEqual(t, encoded.Len(), 64)
	}
	// The per-layer 128KiB allowance separately covers gzip's stream header and
	// footer; the 128-byte rate retains at least double the maximum JSON bytes
	// per one-byte segment for DEFLATE block framing and incompressible data.
	require.GreaterOrEqual(t, tarSplitTailByteAllocation, 128)
}

func TestTarSplitAllowanceCannotOverflowOrBorrowFromNegativeUsage(t *testing.T) {
	for _, metadata := range []retainedTarMetadata{
		{parserBytes: math.MaxUint64},
		{tailBytes: math.MaxUint64},
		{parserBytes: math.MaxInt64 / 8, tailBytes: 1},
	} {
		_, err := metadata.allowance(0)
		require.Error(t, err)
	}
	_, err := (retainedTarMetadata{}).allowance(math.MaxUint64)
	require.Error(t, err)
	_, err = (retainedTarMetadata{tailBytes: 1}).allowance(math.MaxInt64/4 - 1)
	require.Error(t, err)
	allowance, err := (retainedTarMetadata{parserBytes: 512, tailBytes: 1}).allowance(0)
	require.NoError(t, err)
	require.Greater(t, allowance, int64(2*512))
}
