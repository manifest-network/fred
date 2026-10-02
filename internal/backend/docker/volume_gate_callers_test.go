package docker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The two volume-mutation gates keep their documented callers (ENG-1117).
// Start takes the gate that accepts held deletions; only the new, adopt and
// preflight identity proof takes the strict gate, which refuses them. ZFS
// holds no deletion, so its Start gate is the strict one by delegation. Any
// other production reference, call or method value, fails here.
func TestVolumeMutationGatesKeepTheirDocumentedCallers(t *testing.T) {
	got := volumeGateReferences(t, ".")
	require.Equal(t, map[string][]string{
		"RequireNoUnheldVolumeMutations": {
			"(*Backend).Start",
			"projectVolumeRead",
		},
		"RequireNoInterruptedVolumeMutations": {
			"(*zfsVolumeManager).RequireNoUnheldVolumeMutations",
			"acquireDockerStorageIdentityProof",
			"projectVolumeRead",
		},
	}, got)
}

// volumeGateReferences maps each gate to the production functions that
// reference it by selector, sorted and de-duplicated.
func volumeGateReferences(t *testing.T, dir string) map[string][]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	gates := map[string]bool{
		"RequireNoUnheldVolumeMutations":      true,
		"RequireNoInterruptedVolumeMutations": true,
	}
	references := make(map[string][]string)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			owner := funcDeclName(fn)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok && gates[selector.Sel.Name] {
					references[selector.Sel.Name] = append(references[selector.Sel.Name], owner)
				}
				return true
			})
		}
	}
	for gate, owners := range references {
		slices.Sort(owners)
		references[gate] = slices.Compact(owners)
	}
	return references
}

// funcDeclName renders a function as "name" or "(*Recv).name".
func funcDeclName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	receiver := fn.Recv.List[0].Type
	pointer := ""
	if star, ok := receiver.(*ast.StarExpr); ok {
		pointer, receiver = "*", star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return "(" + pointer + ident.Name + ")." + fn.Name.Name
	}
	return fn.Name.Name
}
