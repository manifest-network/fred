package imagefetch

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func TestNamespaceLargeDependencyImagePreparesAndRecoversAtDefaultByteLimit(t *testing.T) {
	// The deployed dependency-heavy image has 167561 entries. Model that shape
	// with 168000 distinct regular paths, exceeding the former 131072 cutoff.
	// The budget-derived memory envelope admits it without changing byte policy.
	const entries = 168000
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	for i := range entries {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: fmt.Sprintf("usr/lib/packages/p-%06d", i), Typeflag: tar.TypeReg, Mode: 0o644}))
	}
	require.NoError(t, writer.Close())
	f := newRegistry(t, raw.Bytes())
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 10<<30, withRegistryTransportForTest(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	again, err := recovery.Prepare(t.Context(), prepared.SourceReference(), prepared.Platform())
	require.NoError(t, err, "saved recovery retains the same finite namespace envelope")
	require.NoError(t, again.Close())
	require.Zero(t, daemon.loads, "registry verification needs no daemon mutation")
}

func TestNamespaceBudgetStopsRetentionBeforeAllocating(t *testing.T) {
	for _, kind := range []byte{tar.TypeReg, tar.TypeXGlobalHeader} {
		t.Run(fmt.Sprintf("header=%c", kind), func(t *testing.T) {
			budget := layerTestBudget(t, 1<<20)
			require.NoError(t, checkLayer(t, budget, encodedTar(t)))
			budget.namespace.remaining = namespaceHeaderMemory - 1
			header := tar.Header{Name: "new-file", Typeflag: kind}
			if kind == tar.TypeXGlobalHeader {
				header.PAXRecords = map[string]string{"comment": "global metadata"}
			}
			before := budget.allocated
			require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, header)), "namespace exceeds memory budget")
			require.Zero(t, budget.entries)
			require.Zero(t, budget.nodes)
			require.Equal(t, before, budget.allocated)
			require.Empty(t, budget.root.children)
		})
	}
	t.Run("implicit node", func(t *testing.T) {
		budget := layerTestBudget(t, 1<<20)
		require.NoError(t, checkLayer(t, budget, encodedTar(t)))
		budget.namespace.remaining = namespaceHeaderMemory + namespaceStringMemory(len("implicit/file")) + namespaceNodeMemory - 1
		require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "implicit/file", Typeflag: tar.TypeReg})), "namespace exceeds memory budget")
		require.Zero(t, budget.nodes)
		require.Empty(t, budget.root.children)
	})
	t.Run("retained name", func(t *testing.T) {
		budget := layerTestBudget(t, 1<<20)
		require.NoError(t, checkLayer(t, budget, encodedTar(t)))
		budget.namespace.remaining = namespaceHeaderMemory + namespaceNodeMemory + 2*namespaceStringMemory(len("file")) - 1
		require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "file", Typeflag: tar.TypeReg})), "namespace exceeds memory budget")
		require.Zero(t, budget.nodes)
		require.Empty(t, budget.root.children)
	})
}

func TestNamespaceChargesOnlyRetainedNodeComponents(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	name := "usr/local/lib/node_modules/dependency/index.js"
	require.NoError(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: name, Typeflag: tar.TypeReg})))
	components := []string{"usr", "local", "lib", "node_modules", "dependency", "index.js"}
	pathBytes := len(name)
	memory := namespaceHeaderMemory + namespaceStringMemory(len(name))
	for _, component := range components {
		pathBytes += len(component)
		memory += namespaceNodeMemory + namespaceStringMemory(len(component))
	}
	require.EqualValues(t, pathBytes, budget.namespace.pathBytes, "the seen map retains the path once, nodes retain only their own component")
	require.Equal(t, memory, namespaceCharge(minNamespaceMemory)-budget.namespace.remaining)
	require.Equal(t, len(components), budget.nodes)
}

func TestNamespaceReplacementsCannotRecycleMemoryAuthority(t *testing.T) {
	budget := layerTestBudget(t, 1<<20)
	raw := encodedTar(t, tar.Header{Name: "same-file", Typeflag: tar.TypeReg})
	require.NoError(t, checkLayer(t, budget, raw))
	first := budget.namespace.remaining
	for range 20 {
		require.NoError(t, checkLayer(t, budget, raw))
	}
	require.Less(t, budget.namespace.remaining, first-20*(namespaceHeaderMemory+namespaceNodeMemory))
	require.Len(t, budget.root.children, 1)
	require.Equal(t, 21, budget.nodes, "discarded nodes retain their work debit")
}

func TestNamespaceEnvelopePreservesPreviouslyAdmittedShapes(t *testing.T) {
	// Every former entry/node maximum plus worst-case string-class rounding
	// fits the new envelope. The change cannot reduce old namespace admission.
	const priorEntries = 131072
	require.LessOrEqual(t, priorEntries*(namespaceHeaderMemory+namespaceNodeMemory)+2*minRetainedPathBytes, namespaceCharge(minNamespaceMemory))
	for length := 1; length <= maxPathBytes; length++ {
		charge := namespaceStringMemory(length)
		require.GreaterOrEqual(t, charge, namespaceCharge(length))
		require.LessOrEqual(t, charge, namespaceCharge(2*length))
	}
	var unissued namespaceMemory
	require.Error(t, unissued.claim(namespaceNodeMemory))
	issued := layerTestBudget(t, 1<<20).namespace
	require.Error(t, issued.claim(^namespaceCharge(0)), "overflowed unsigned charges cannot mint capacity")
	require.Equal(t, namespaceCharge(minNamespaceMemory), issued.remaining)
}

func TestNamespaceEnvelopeDerivesEveryDimensionFromVerificationBudget(t *testing.T) {
	for _, bytes := range []int64{1, 10 << 30, 20 << 30, 32 << 30, 2 << 40, math.MaxInt64 / 8} {
		t.Run(fmt.Sprint(bytes), func(t *testing.T) {
			verification, err := imagebudget.NewVerificationBudget(bytes)
			require.NoError(t, err)
			issued := newNamespaceMemory(verification)
			require.EqualValues(t, max(minNamespaceMemory, min(bytes, maxNamespaceVerification)/namespaceMemoryRatio), issued.remaining)
			require.EqualValues(t, max(minRetainedPathBytes, min(bytes, maxNamespaceVerification)/retainedPathRatio), issued.retainedPathLimit())
			require.EqualValues(t, max(minResolvedPathBytes, min(bytes, maxNamespaceVerification)/resolvedPathRatio), issued.resolvedPathLimit())
			require.NoError(t, issued.claim(issued.remaining))
			issued.pathBytes = issued.retainedPathLimit()
			issued.resolvedBytes = issued.resolvedPathLimit()
			projected := issued.recoveryBytes()
			require.LessOrEqual(t, projected, bytes, "inverse projection cannot enlarge or overflow its issuer")
		})
	}
	var unissued imagebudget.VerificationBudget
	zero := newNamespaceMemory(unissued)
	require.Error(t, zero.claim(namespaceHeaderMemory), "an unissued byte budget cannot grant namespace authority")
	require.Error(t, zero.claimName("file"))
	require.Error(t, zero.claimResolution("component"))
}

func TestNamespaceSavedProjectionPreservesEachIndependentDimension(t *testing.T) {
	for _, kind := range []string{"memory", "retained paths", "resolution work"} {
		t.Run(kind, func(t *testing.T) {
			issued := layerTestBudget(t, 10<<30).namespace
			var memory namespaceCharge
			var retained, resolved, want int64
			switch kind {
			case "memory":
				memory = minNamespaceMemory + 1
				want = int64(memory) * namespaceMemoryRatio
			case "retained paths":
				retained = minRetainedPathBytes + 1
				want = retained * retainedPathRatio
			case "resolution work":
				resolved = minResolvedPathBytes + 1
				want = resolved * resolvedPathRatio
			}
			require.NoError(t, issued.claim(memory))
			for left := retained; left > 0; {
				n := min(left, maxPathBytes)
				require.NoError(t, issued.claimName(strings.Repeat("p", int(n))))
				left -= n
			}
			for left := resolved; left > 0; {
				n := min(left, maxPathBytes)
				require.NoError(t, issued.claimResolution(strings.Repeat("d", int(n)-1)))
				left -= n
			}
			projected := issued.recoveryBytes()
			require.Equal(t, want, projected)
			stored, err := imagebudget.Decode(imagebudget.Stored{VerificationBytes: projected, ImportBytes: 1})
			require.NoError(t, err)
			recovered := newNamespaceMemory(stored.Verification())
			require.NoError(t, recovered.claim(memory))
			require.GreaterOrEqual(t, recovered.retainedPathLimit(), retained)
			require.GreaterOrEqual(t, recovered.resolvedPathLimit(), resolved)
		})
	}
	issued := layerTestBudget(t, 10<<30).namespace
	require.NoError(t, issued.claim(minNamespaceMemory))
	issued.pathBytes = minRetainedPathBytes
	issued.resolvedBytes = minResolvedPathBytes
	require.Zero(t, issued.recoveryBytes(), "fixed floors require no extra saved byte authority")
}

func TestNamespaceImplicitNodesRecoverAboveCompatibilityFloor(t *testing.T) {
	// Many implicit parent nodes exercise namespace independently of compressed,
	// decoded and physical bytes. Saving only the latter ceilings would shrink
	// recovery below this image's namespace use despite immutable content.
	const roots, depth = 11000, 20
	component := strings.Repeat("d", 129)
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	for i := range roots {
		name := fmt.Sprintf("p%05d/", i) + strings.Repeat(component+"/", depth) + "file"
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644}))
	}
	require.NoError(t, writer.Close())
	f := newRegistry(t, raw.Bytes())
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 10<<30, withRegistryTransportForTest(f.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	// Distinct implicit directories and their retained basename allocations
	// alone exceed the old fixed namespace envelope.
	minimumMemory := namespaceCharge(roots*depth) * (namespaceNodeMemory + namespaceStringMemory(len(component)))
	require.Greater(t, minimumMemory, namespaceCharge(minNamespaceMemory))
	require.GreaterOrEqual(t, prepared.Budget().Verification().Bytes(), int64(minimumMemory)*namespaceMemoryRatio)
	require.Greater(t, prepared.Budget().Verification().Bytes(), (prepared.ImportBytes()+1)/2,
		"fixture must make namespace projection independently load-bearing")
	recovery, err := loader.WithBudget(prepared.Budget().Verification())
	require.NoError(t, err)
	again, err := recovery.Prepare(t.Context(), prepared.SourceReference(), prepared.Platform())
	require.NoError(t, err, "saved verification must restore every consumed namespace dimension")
	require.NoError(t, again.Close())
}

func TestNamespaceMorpheusPathHistogramHasThreefoldHeadroom(t *testing.T) {
	// Captured by a complete read-only Prepare of the immutable linux/amd64
	// image. The fixture retains length/count histograms, not tenant filenames.
	// This characterizes namespace capacity independently of physical import
	// bytes: three copies of the full image are not promised to fit that cap.
	var fixture struct {
		Manifest    string
		Measurement struct {
			Entries           int
			Nodes             int
			NamespaceBytes    uint64 `json:"namespace_bytes"`
			RetainedPathBytes int64  `json:"retained_path_bytes"`
			ResolvedPathBytes int64  `json:"resolved_path_bytes"`
		}
		Layers []struct {
			HeaderPaths       [][2]int `json:"header_paths"`
			NodeBases         [][2]int `json:"node_bases"`
			SymlinkTargets    [][2]int `json:"symlink_targets"`
			ResolvedPathBytes int64    `json:"resolved_path_bytes"`
		}
	}
	data, err := os.ReadFile("testdata/morpheus-namespace.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, "ghcr.io/everclaw/everclaw@sha256:84e45cf568299ad6dc7e38236907a0c6da8480d057fbc1d2d7482590e3928d9a", fixture.Manifest)
	require.Len(t, fixture.Layers, 31)
	require.Equal(t, 167561, fixture.Measurement.Entries)
	issued := layerTestBudget(t, 10<<30).namespace
	var entries, nodes int
	for repeat := range 3 {
		for _, layer := range fixture.Layers {
			for _, bucket := range layer.HeaderPaths {
				name := strings.Repeat("p", bucket[0])
				for range bucket[1] {
					require.NoError(t, issued.claim(namespaceHeaderMemory))
					require.NoError(t, issued.claimName(name))
					entries++
				}
			}
			for _, bucket := range layer.NodeBases {
				base := strings.Repeat("b", bucket[0])
				for range bucket[1] {
					require.NoError(t, issued.claim(namespaceNodeMemory))
					require.NoError(t, issued.claimName(base))
					nodes++
				}
			}
			for _, bucket := range layer.SymlinkTargets {
				target := strings.Repeat("l", bucket[0])
				for range bucket[1] {
					require.NoError(t, issued.claimName(target))
				}
			}
			for work := layer.ResolvedPathBytes; work > 0; {
				n := min(work, maxPathBytes)
				require.NoError(t, issued.claimResolution(strings.Repeat("d", int(n)-1)))
				work -= n
			}
		}
		factor := int64(repeat + 1)
		require.Equal(t, fixture.Measurement.Entries*(repeat+1), entries)
		require.Equal(t, fixture.Measurement.Nodes*(repeat+1), nodes)
		require.EqualValues(t, fixture.Measurement.NamespaceBytes*uint64(factor), issued.limit()-issued.remaining)
		require.Equal(t, fixture.Measurement.RetainedPathBytes*factor, issued.pathBytes)
		require.Equal(t, fixture.Measurement.ResolvedPathBytes*factor, issued.resolvedBytes)
	}
}

func TestNamespaceLegacyDiskHeadroomCannotBecomeParserMemory(t *testing.T) {
	legacy, err := imagebudget.NewVerificationBudget(2 << 40)
	require.NoError(t, err)
	memory := newNamespaceMemory(legacy)
	require.EqualValues(t, 1<<30, memory.limit())
	require.EqualValues(t, 256<<20, memory.retainedPathLimit())
	require.EqualValues(t, 512<<20, memory.resolvedPathLimit())
	require.NoError(t, memory.claim(memory.limit()))
	require.Error(t, memory.claim(namespaceNodeMemory))
	memory.pathBytes = memory.retainedPathLimit()
	memory.resolvedBytes = memory.resolvedPathLimit()
	require.Error(t, memory.claimName("another"))
	require.Error(t, memory.claimResolution("another"))
	saved, err := imagebudget.NewVerificationBudget(memory.recoveryBytes())
	require.NoError(t, err)
	again := newNamespaceMemory(saved)
	require.Equal(t, memory.envelope, again.envelope, "saved projection preserves all admitted dimensions without regranting disk-derived memory")
}
