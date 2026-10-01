package placementsnapshot

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// nonEmptyLiteralsOutside reports every non-empty composite literal of a
// guarded type in dir's production files that sits outside the functions
// allowed to mint it. Empty literals are zero values and mint nothing.
func nonEmptyLiteralsOutside(t *testing.T, dir string, allowed map[string]map[string]bool) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	fset := token.NewFileSet()
	var violations []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			owner := "<package scope>"
			if function, ok := declaration.(*ast.FuncDecl); ok {
				owner = function.Name.Name
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok || len(literal.Elts) == 0 {
					return true
				}
				typeName, ok := literal.Type.(*ast.Ident)
				if !ok {
					return true
				}
				if minters, guarded := allowed[typeName.Name]; guarded && !minters[owner] {
					violations = append(violations, fset.Position(literal.Pos()).String()+
						": "+typeName.Name+" minted in "+owner)
				}
				return true
			})
		}
	}
	return violations
}
