package imagefetch

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func layerTestBudget(t *testing.T, bytes int64) *layerBudget {
	t.Helper()
	verification, err := imagebudget.NewVerificationBudget(bytes)
	require.NoError(t, err)
	budget := newLayerBudget(verification)
	return &budget
}

func encodedTar(t *testing.T, headers ...tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	for _, h := range headers {
		require.NoError(t, w.WriteHeader(&h))
		if h.Typeflag == tar.TypeReg {
			_, err := w.Write(bytes.Repeat([]byte("x"), int(h.Size)))
			require.NoError(t, err)
		}
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func checkLayer(t *testing.T, budget *layerBudget, raw []byte) error {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "layer-")
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(raw)
	require.NoError(t, err)
	return inspectLayer(t.Context(), f, ocispec.MediaTypeImageLayer, digest.FromBytes(raw), budget)
}

func TestLayerNamespaceAcceptsInheritedUsrmergeAndRelativeLinks(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	require.NoError(t, checkLayer(t, budget, encodedTar(t,
		tar.Header{Name: "usr", Typeflag: tar.TypeDir},
		tar.Header{Name: "usr/bin", Typeflag: tar.TypeDir},
		tar.Header{Name: "bin", Typeflag: tar.TypeSymlink, Linkname: "usr/bin"},
		tar.Header{Name: "usr/local", Typeflag: tar.TypeDir},
		tar.Header{Name: "usr/local/bin", Typeflag: tar.TypeSymlink, Linkname: "../bin"},
	)))
	require.NoError(t, checkLayer(t, budget, encodedTar(t,
		tar.Header{Name: "bin/app", Typeflag: tar.TypeReg, Size: 4096},
		tar.Header{Name: "usr/local/bin/other", Typeflag: tar.TypeLink, Linkname: "bin/app"},
	)))
	bin := budget.root.children["usr"].children["bin"]
	require.Equal(t, int64(4096), bin.children["app"].size)
	require.Equal(t, int64(4096), bin.children["other"].size)
	require.GreaterOrEqual(t, budget.allocated, int64(8192))
}

func TestLayerRejectsStaleHardlinkAuthority(t *testing.T) {
	for _, tt := range []struct {
		name              string
		previous, current []tar.Header
		want              string
	}{
		{"ancestor replacement", nil, []tar.Header{{Name: "d/file", Typeflag: tar.TypeReg}, {Name: "d", Typeflag: tar.TypeSymlink, Linkname: "/lower"}, {Name: "out", Typeflag: tar.TypeLink, Linkname: "d/file"}}, "used directory"},
		{"inherited alias replacement", []tar.Header{{Name: "a", Typeflag: tar.TypeSymlink, Linkname: "b"}, {Name: "c/file", Typeflag: tar.TypeReg, Size: 8192}}, []tar.Header{{Name: "a/file", Typeflag: tar.TypeReg}, {Name: "b", Typeflag: tar.TypeSymlink, Linkname: "c"}, {Name: "out", Typeflag: tar.TypeLink, Linkname: "a/file"}}, "used directory"},
		{"lower-layer link", []tar.Header{{Name: "lower", Typeflag: tar.TypeReg, Size: 8192}}, []tar.Header{{Name: "out", Typeflag: tar.TypeLink, Linkname: "lower"}}, "earlier regular file"},
		{"whiteout target", nil, []tar.Header{{Name: "d/file", Typeflag: tar.TypeReg}, {Name: ".wh.d", Typeflag: tar.TypeReg}, {Name: "out", Typeflag: tar.TypeLink, Linkname: "d/file"}}, "invalidates"},
		{"opaque target", nil, []tar.Header{{Name: "d/file", Typeflag: tar.TypeReg}, {Name: "d/.wh..wh..opq", Typeflag: tar.TypeReg}}, "invalidates"},
		{"absolute aliases", nil, []tar.Header{{Name: "/file", Typeflag: tar.TypeReg}, {Name: "//file", Typeflag: tar.TypeReg}}, "duplicate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			budget := layerTestBudget(t, 1<<20)
			if len(tt.previous) > 0 {
				require.NoError(t, checkLayer(t, budget, encodedTar(t, tt.previous...)))
			}
			require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tt.current...)), tt.want)
		})
	}
}

func TestLayerCountsInheritedDanglingSymlinkDirectories(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	target := strings.Repeat("nested/", 40) + "target"
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: target})))
	before := budget.allocated
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "a/file", Typeflag: tar.TypeReg})))
	require.GreaterOrEqual(t, budget.allocated-before, 41*metadataAllocation)
	require.GreaterOrEqual(t, budget.nodes, 43)
}

func TestLayerBoundsImplicitPathsBeforeAllocation(t *testing.T) {
	budget := layerTestBudget(t, 16<<20)
	budget.namespace.remaining = 1 << 20
	var headers []tar.Header
	for i := range 20 {
		headers = append(headers, tar.Header{Name: fmt.Sprintf("%d/", i) + strings.Repeat("d/", 2000) + "file", Typeflag: tar.TypeReg})
	}
	err := checkLayer(t, budget, encodedTar(t, headers...))
	require.ErrorContains(t, err, "namespace exceeds memory budget")
	require.LessOrEqual(t, budget.namespace.pathBytes, int64(minRetainedPathBytes))
	require.LessOrEqual(t, budget.nodes, int((1<<20)/namespaceNodeMemory))
}

func TestLayerBoundsSymlinkLoops(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: "b"}, tar.Header{Name: "b", Typeflag: tar.TypeSymlink, Linkname: "a"})))
	require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "a/file", Typeflag: tar.TypeReg})), "traversal limit")
}

func TestLayerChargesDirectoryCopiesAcrossLayers(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "d", Typeflag: tar.TypeDir})))
	before := budget.allocated
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "d/file", Typeflag: tar.TypeReg})))
	require.GreaterOrEqual(t, budget.allocated-before, metadataAllocation*2)
}

func TestLayerRejectsOverlayNamespaceXattrs(t *testing.T) {
	for _, key := range []string{"trusted.overlay.redirect", "trusted.overlay.metacopy", "user.overlay.opaque"} {
		t.Run(key, func(t *testing.T) {
			raw := encodedTar(t, tar.Header{Name: "d", Typeflag: tar.TypeDir, PAXRecords: map[string]string{"SCHILY.xattr." + key: "target"}})
			require.ErrorContains(t, checkLayer(t, layerTestBudget(t, 1<<20), raw), "reserved overlay")
		})
	}
}

func TestLayerValidatesZstdAndGzipChecksums(t *testing.T) {
	raw := layerTar(t, []byte("content"))
	var gz bytes.Buffer
	g := gzip.NewWriter(&gz)
	_, err := g.Write(raw)
	require.NoError(t, err)
	require.NoError(t, g.Close())
	z, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	zbytes := z.EncodeAll(raw, nil)
	z.Close()
	for _, tt := range []struct {
		name, media string
		body        []byte
	}{{"gzip", ocispec.MediaTypeImageLayerGzip, gz.Bytes()}, {"zstd", ocispec.MediaTypeImageLayerZstd, zbytes}} {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "layer-")
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Write(tt.body)
			require.NoError(t, err)
			require.NoError(t, inspectLayer(t.Context(), f, tt.media, digest.FromBytes(raw), layerTestBudget(t, 1<<20)))
			require.ErrorContains(t, inspectLayer(t.Context(), f, tt.media, digest.FromString("wrong"), layerTestBudget(t, 1<<20)), "uncompressed digest")
		})
	}
}

func TestLayerRejectsEmptyOverlayControlPAXRecords(t *testing.T) {
	for _, key := range []string{"trusted.overlay.metacopy", "user.overlay.metacopy"} {
		raw := encodedTar(t, tar.Header{Name: "file", Typeflag: tar.TypeReg, PAXRecords: map[string]string{"SCHILY.xattr." + key: ""}})
		require.ErrorContains(t, checkLayer(t, layerTestBudget(t, 1<<20), raw), "reserved overlay")
	}
}

func TestLayerChargesRetainedSymlinkTargets(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	budget.namespace.pathBytes = minRetainedPathBytes - 256
	raw := encodedTar(t, tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: strings.Repeat("d/", 512)})
	require.ErrorContains(t, checkLayer(t, budget, raw), "retained path byte budget")
	require.LessOrEqual(t, budget.namespace.pathBytes, int64(minRetainedPathBytes))
}

func TestLayerChargesSlashHeavySymlinkResolution(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: strings.Repeat("/", maxPathBytes)})))
	budget.namespace.resolvedBytes = minResolvedPathBytes - 128
	require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "link/file", Typeflag: tar.TypeReg})), "path work budget")
	require.LessOrEqual(t, budget.namespace.resolvedBytes, int64(minResolvedPathBytes))
}

func TestLayerRejectsGNUSparseEntries(t *testing.T) {
	raw := encodedTar(t, tar.Header{Name: "sparse", Typeflag: tar.TypeGNUSparse, Format: tar.FormatGNU})
	require.Error(t, checkLayer(t, layerTestBudget(t, 1<<20), raw))
}

func TestLayerRejectsAmbiguousSymlinkParentTraversal(t *testing.T) {
	for _, target := range []string{"alias/../file", "../alias/../file", "/alias/../file"} {
		raw := encodedTar(t, tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: target})
		require.ErrorContains(t, checkLayer(t, layerTestBudget(t, 1<<20), raw), "ambiguous internal parent traversal")
	}
	for _, target := range []string{"../bin", "../../lib", "./../bin", "/../../bin"} {
		require.NoError(t, stableSymlinkTarget(target))
	}
}
