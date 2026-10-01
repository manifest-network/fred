package imagefetch

import (
	"archive/tar"
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	splittar "github.com/vbatts/tar-split/archive/tar"
)

func chainedTarExtensions(t *testing.T, header tar.Header, count int) []byte {
	t.Helper()
	var seed bytes.Buffer
	writer := tar.NewWriter(&seed)
	require.NoError(t, writer.WriteHeader(&header))
	require.NoError(t, writer.Close())
	// Zero-size entries end with the ordinary header and two zero blocks.
	prefix := seed.Bytes()[:seed.Len()-1536]
	require.NotEmpty(t, prefix)
	raw := bytes.Repeat(prefix, count)
	return append(raw, seed.Bytes()[seed.Len()-1536:]...)
}

func TestRawTarHeaderSpanBoundsHiddenExtensions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header tar.Header
		count  int
	}{
		{"PAX", tar.Header{Name: "file", Typeflag: tar.TypeReg, Format: tar.FormatPAX, PAXRecords: map[string]string{"comment": strings.Repeat("x", 60<<10)}}, 128},
		{"GNU name", tar.Header{Name: strings.Repeat("n", 300), Typeflag: tar.TypeReg, Format: tar.FormatGNU}, 200},
		{"GNU link", tar.Header{Name: "link", Linkname: strings.Repeat("n", 300), Typeflag: tar.TypeSymlink, Format: tar.FormatGNU}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := chainedTarExtensions(t, tc.header, tc.count)
			split := splittar.NewReader(bytes.NewReader(raw))
			split.RawAccounting = true
			header, err := split.Next()
			require.NoError(t, err)
			require.Equal(t, tc.header.Name, header.Name)
			require.Greater(t, len(split.RawBytes()), maxRawHeaderBytes)
			input := bytes.NewReader(raw)
			archive := newLayerArchive(input)
			copyOfArchive := archive
			_, err = archive.next()
			require.ErrorContains(t, err, "raw header")
			require.LessOrEqual(t, len(raw)-input.Len(), maxRawHeaderBytes)
			_, copiedErr := copyOfArchive.next()
			require.Equal(t, err, copiedErr, "copies cannot reset a failed parser's allowance")

			f := newRegistry(t, raw)
			daemon := &recordingImporter{}
			loader, err := NewLoader(daemon, t.TempDir(), 32<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.Nil(t, prepared)
			require.ErrorContains(t, err, "raw header")
			require.Zero(t, daemon.loads)
		})
	}
}

func TestLayerArchiveSeparatesPayloadFromEachRawHeaderSpan(t *testing.T) {
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	const size = 2<<20 + 7
	for _, name := range []string{"first", "second"} {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: size,
			Format: tar.FormatPAX, PAXRecords: map[string]string{"comment": strings.Repeat("x", 60<<10)}}))
		_, err := io.Copy(writer, bytes.NewReader(make([]byte, size)))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	archive := newLayerArchive(bytes.NewReader(raw.Bytes()))
	for _, name := range []string{"first", "second"} {
		header, err := archive.next()
		require.NoError(t, err)
		require.Equal(t, name, header.Name)
	}
	_, err := archive.next()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(2*size), archive.payloadBytes())
	// The full path also verifies payload, padding, compressor checksum and
	// diffID while retaining the original import/namespace accounting.
	f := newRegistry(t, raw.Bytes())
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 32<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.NoError(t, prepared.Close())
}

func TestLayerArchiveBoundsExtensionsBeforeGlobalPAXHeader(t *testing.T) {
	raw := chainedTarExtensions(t, tar.Header{Name: "file", Typeflag: tar.TypeReg, Format: tar.FormatPAX,
		PAXRecords: map[string]string{"comment": strings.Repeat("x", 60<<10)}}, 3)
	global := encodedTar(t, tar.Header{Name: "global", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": "global metadata"}})
	raw = append(raw[:len(raw)-1536], global...)
	archive := newLayerArchive(bytes.NewReader(raw))
	_, err := archive.next()
	require.ErrorContains(t, err, "raw header")
	// Separate global headers release each parser-owned span and are valid.
	archive = newLayerArchive(bytes.NewReader(encodedTar(t,
		tar.Header{Name: "g1", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": strings.Repeat("x", 60<<10)}},
		tar.Header{Name: "g2", Typeflag: tar.TypeXGlobalHeader, PAXRecords: map[string]string{"comment": strings.Repeat("y", 60<<10)}})))
	for range 2 {
		header, err := archive.next()
		require.NoError(t, err)
		require.Equal(t, byte(tar.TypeXGlobalHeader), header.Typeflag)
	}
	_, err = archive.next()
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, archive.payloadBytes())
}
