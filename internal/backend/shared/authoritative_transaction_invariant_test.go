package shared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAuthoritativeStoresUseGuardedTransactions prevents a future callback,
// intent, release, or retention mutation/read from bypassing boltStore's
// identity, path, inode, and sticky-failure checks. The base implementation is
// the only authority file allowed to touch its embedded db directly;
// diagnostics is explicitly non-authoritative and intentionally exempt.
func TestAuthoritativeStoresUseGuardedTransactions(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") || name == "bolt_store.go" ||
			name == "diagnostics.go" {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (method.Sel.Name != "Update" && method.Sel.Name != "Batch" && method.Sel.Name != "View") {
				return true
			}
			database, ok := method.X.(*ast.SelectorExpr)
			if !ok || database.Sel.Name != "db" {
				return true
			}
			position := fset.Position(call.Pos())
			t.Errorf("%s bypasses guarded boltStore transaction wrapper", position)
			return true
		})
	}
}
