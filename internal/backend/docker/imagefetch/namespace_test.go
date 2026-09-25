package imagefetch

import (
	"archive/tar"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceLargeDependencyImagePreparesAndRecoversAtDefaultByteLimit(t *testing.T) {
	// The deployed dependency-heavy image has 167561 entries. Model that shape
	// with 168000 distinct regular paths, exceeding the former 131072 cutoff.
	// The fixed model-memory envelope admits it without changing byte policy.
	const entries = 168000
	var raw bytes.Buffer
	writer := tar.NewWriter(&raw)
	for i := range entries {
		require.NoError(t, writer.WriteHeader(&tar.Header{Name: fmt.Sprintf("usr/lib/packages/p-%06d", i), Typeflag: tar.TypeReg, Mode: 0o644}))
	}
	require.NoError(t, writer.Close())
	f := newRegistry(t, raw.Bytes())
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 10<<30, WithRegistryTransport(f.server.Client().Transport))
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
			budget := &layerBudget{remaining: 1 << 20}
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
		budget := &layerBudget{remaining: 1 << 20}
		require.NoError(t, checkLayer(t, budget, encodedTar(t)))
		budget.namespace.remaining = namespaceHeaderMemory + namespaceStringMemory(len("implicit/file")) + namespaceNodeMemory - 1
		require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "implicit/file", Typeflag: tar.TypeReg})), "namespace exceeds memory budget")
		require.Zero(t, budget.nodes)
		require.Empty(t, budget.root.children)
	})
	t.Run("retained name", func(t *testing.T) {
		budget := &layerBudget{remaining: 1 << 20}
		require.NoError(t, checkLayer(t, budget, encodedTar(t)))
		budget.namespace.remaining = namespaceHeaderMemory + namespaceNodeMemory + 2*namespaceStringMemory(len("file")) - 1
		require.ErrorContains(t, checkLayer(t, budget, encodedTar(t, tar.Header{Name: "file", Typeflag: tar.TypeReg})), "namespace exceeds memory budget")
		require.Zero(t, budget.nodes)
		require.Empty(t, budget.root.children)
	})
}

func TestNamespaceReplacementsCannotRecycleMemoryAuthority(t *testing.T) {
	budget := &layerBudget{remaining: 1 << 20}
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
	require.LessOrEqual(t, priorEntries*(namespaceHeaderMemory+namespaceNodeMemory)+2*maxRetainedPathBytes, namespaceCharge(maxNamespaceMemory))
	for length := 1; length <= maxPathBytes; length++ {
		charge := namespaceStringMemory(length)
		require.GreaterOrEqual(t, charge, namespaceCharge(length))
		require.LessOrEqual(t, charge, namespaceCharge(2*length))
	}
	var unissued namespaceMemory
	require.Error(t, unissued.claim(namespaceNodeMemory))
	issued := newNamespaceMemory()
	require.Error(t, issued.claim(^namespaceCharge(0)), "overflowed unsigned charges cannot mint capacity")
	require.Equal(t, namespaceCharge(maxNamespaceMemory), issued.remaining)
}
