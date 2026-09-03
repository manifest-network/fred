package backend

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapInventoryClient_DoesNotExposeMutationAuthority(t *testing.T) {
	t.Parallel()

	client := NewBootstrapInventoryClient(HTTPClientConfig{
		Name:    "backend-a",
		BaseURL: "http://backend.invalid",
	})
	require.NotNil(t, client)
	_, isConcreteClient := client.(*HTTPClient)
	assert.False(t, isConcreteClient,
		"bootstrap construction must not leak the full concrete client through type assertion")
	_, hasMutationAuthority := client.(Backend)
	assert.False(t, hasMutationAuthority,
		"bootstrap inventory capability must not implement the side-effecting Backend port")
}

// TestRawHTTPClientConstructionStaysAtTheChokePoint is a repository-level
// architecture invariant. Bootstrap code gets a read-only wrapper and normal
// runtime code gets an identity-bound client; no other production source may
// construct the full unbound transport.
func TestRawHTTPClientConstructionStaysAtTheChokePoint(t *testing.T) {
	t.Parallel()

	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
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
				case "NewHTTPClient":
					position := fset.Position(call.Pos())
					t.Errorf("production source constructs a full unbound backend client at %s", position)
				case "newHTTPClient":
					if filepath.ToSlash(relative) != "internal/backend/client.go" {
						position := fset.Position(call.Pos())
						t.Errorf("raw HTTP client constructor escaped its choke point at %s", position)
					}
				}
				return true
			})
			return nil
		})
		require.NoError(t, err)
	}
}
