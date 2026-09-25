package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestExtractImageContentPreservesSharedBudgetAfterFailure(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "disallowed entry after complete file"
		if partial {
			name = "truncated file body"
		}
		t.Run(name, func(t *testing.T) {
			h := newInspectionHarness(t)
			h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) {
				if path == "/one" {
					if partial {
						return io.NopCloser(truncatedExtractionArchive(t, "one/data", 8, "1234")), nil
					}
					return io.NopCloser(createTestTar(t, []testTarEntry{
						{Name: "one/data", Typeflag: tar.TypeReg, Mode: 0o600, Content: "12345678"},
						{Name: "one/pipe", Typeflag: tar.TypeFifo, Mode: 0o600},
					})), nil
				}
				return io.NopCloser(createTestTar(t, []testTarEntry{
					{Name: "two/data", Typeflag: tar.TypeReg, Mode: 0o600, Content: "12345678"},
				})), nil
			}
			destination := t.TempDir()
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				failures := h.client.ExtractImageContent(ctx, h.image, []string{"/one", "/two"}, destination, 8, 10, origin)
				require.Error(t, failures["/one"])
				require.ErrorContains(t, failures["/two"], "8-byte limit")
				require.NoFileExists(t, filepath.Join(destination, "two", "data"))
				expected := int64(8)
				if partial {
					expected = 4
				}
				require.Equal(t, expected, sumRegularFileBytes(t, destination))
				return nil
			})
		})
	}
}

func TestExtractImageContentSharesBytesAndKeepsPerPathEntryLimits(t *testing.T) {
	h := newInspectionHarness(t)
	h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) {
		return io.NopCloser(createTestTar(t, []testTarEntry{
			{Name: strings.TrimPrefix(path, "/") + "/data", Typeflag: tar.TypeReg, Mode: 0o600, Content: "1234"},
		})), nil
	}
	destination := t.TempDir()
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		failures := h.client.ExtractImageContent(ctx, h.image, []string{"/one", "/two"}, destination, 8, 1, origin)
		require.Empty(t, failures)
		require.Equal(t, int64(8), sumRegularFileBytes(t, destination))
		for _, path := range []string{"one", "two"} {
			content, err := os.ReadFile(filepath.Join(destination, path, "data"))
			require.NoError(t, err)
			require.Equal(t, "1234", string(content))
		}
		return nil
	})
}

func TestTarExtractorCopiesRetainReservationsAfterWriteFailure(t *testing.T) {
	for _, failure := range []string{"partial copy", "file creation", "canceled copy"} {
		t.Run(failure, func(t *testing.T) {
			destination := t.TempDir()
			extractor := newTarExtractor(8, 10)
			otherPath := extractor
			ctx := t.Context()
			source := truncatedExtractionArchive(t, "data", 8, "1234")
			expectedWritten := int64(4)
			switch failure {
			case "file creation":
				require.NoError(t, os.Mkdir(filepath.Join(destination, "data"), 0o700))
				source = createTestTar(t, []testTarEntry{{Name: "data", Typeflag: tar.TypeReg, Content: "12345678"}})
				expectedWritten = 0
			case "canceled copy":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				source = &cancelExtractionBodyReader{source: source, cancel: cancel}
			}
			written, _, err := extractor.extract(ctx, source, destination)
			require.Error(t, err)
			require.Equal(t, expectedWritten, written)
			require.Equal(t, expectedWritten, sumRegularFileBytes(t, destination))

			// The copied extractor shares the reservation, including bytes that
			// never reached disk. A different archive cannot spend them again.
			written, _, err = otherPath.extract(t.Context(), createTestTar(t, []testTarEntry{
				{Name: "other", Typeflag: tar.TypeReg, Content: "x"},
			}), destination)
			require.ErrorContains(t, err, "8-byte limit")
			require.Zero(t, written)
			require.NoFileExists(t, filepath.Join(destination, "other"))
		})
	}
}

func TestTarExtractorRequiresConstructedBudget(t *testing.T) {
	for _, extractor := range []tarExtractor{{}, newTarExtractor(-1, 10)} {
		destination := t.TempDir()
		written, _, err := extractor.extract(t.Context(), createTestTar(t, []testTarEntry{
			{Name: "empty", Typeflag: tar.TypeReg},
		}), destination)
		require.Error(t, err)
		require.Zero(t, written)
		require.NoFileExists(t, filepath.Join(destination, "empty"))
	}

	// A deliberately configured zero-byte allowance still permits empty files.
	destination := t.TempDir()
	written, _, err := newTarExtractor(0, 1).extract(t.Context(), createTestTar(t, []testTarEntry{
		{Name: "empty", Typeflag: tar.TypeReg},
	}), destination)
	require.NoError(t, err)
	require.Zero(t, written)
	require.FileExists(t, filepath.Join(destination, "empty"))
}

func TestSanitizeAndExtractTarReportsPartialCopy(t *testing.T) {
	destination := t.TempDir()
	written, _, err := sanitizeAndExtractTarContext(t.Context(), truncatedExtractionArchive(t, "partial", 8, "1234"), destination, 8, 10)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, int64(4), written)
	require.Equal(t, written, sumRegularFileBytes(t, destination))
}

func truncatedExtractionArchive(t *testing.T, name string, declaredSize int64, body string) io.Reader {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: declaredSize, Typeflag: tar.TypeReg}))
	_, err := io.WriteString(writer, body)
	require.NoError(t, err)
	// Leave the body truncated, as with an interrupted Docker archive response.
	return bytes.NewReader(buffer.Bytes())
}

type cancelExtractionBodyReader struct {
	source io.Reader
	cancel context.CancelFunc
	read   int
}

func (r *cancelExtractionBodyReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	r.read += n
	if r.read > 512 {
		r.cancel()
	}
	return n, err
}
