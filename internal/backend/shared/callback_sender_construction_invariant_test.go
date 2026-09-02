package shared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEphemeralCallbackSenderConstruction_IsTestOnly pins the repository-level
// safety boundary: production callback delivery is durable and re-attests its
// storage lineage. The exported ephemeral seam exists only because tests in
// sibling internal packages cannot use an unexported constructor.
func TestEphemeralCallbackSenderConstruction_IsTestOnly(t *testing.T) {
	t.Parallel()

	repositoryRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	constructorFile := "internal/backend/shared/callback_sender.go"
	fset := token.NewFileSet()
	for _, topLevel := range []string{"cmd", "internal", "scripts"} {
		root := filepath.Join(repositoryRoot, topLevel)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repositoryRoot, path)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				var name string
				switch called := call.Fun.(type) {
				case *ast.Ident:
					name = called.Name
				case *ast.SelectorExpr:
					name = called.Sel.Name
				}
				switch name {
				case "MustNewEphemeralCallbackSender":
					position := fset.Position(call.Pos())
					t.Errorf("production source constructs an ephemeral callback sender at %s", position)
				case "NewEphemeralCallbackSender":
					allowedDelegation := filepath.ToSlash(relative) == constructorFile &&
						insideFunction(file, call, "MustNewEphemeralCallbackSender")
					if !allowedDelegation {
						position := fset.Position(call.Pos())
						t.Errorf("production source constructs an ephemeral callback sender at %s", position)
					}
				}
				return true
			})
			return nil
		})
		require.NoError(t, err)
	}
}

func insideFunction(file *ast.File, node ast.Node, name string) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name && function.Pos() <= node.Pos() && node.End() <= function.End() {
			return true
		}
	}
	return false
}
