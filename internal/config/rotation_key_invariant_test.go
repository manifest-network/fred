package config

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

// rotationKeyReaders lists the only production functions allowed to read a
// verify-only rotation key: config validation and the one constructor that
// turns it into hmacauth.VerifyKeys. Any other read could hand the key to a
// signer, which would let a rotation silently start signing with the old or the
// next key.
var rotationKeyReaders = map[string]map[string]bool{
	"HMACSecretPrevious": {
		"internal/config/config.go:Validate":            true,
		"internal/config/config.go:BackendCallbackKeys": true,
	},
	"CallbackSecretNext": {
		"internal/backend/docker/config.go:Validate":    true,
		"internal/backend/docker/config.go:RequestKeys": true,
		"cmd/docker-backend/main.go:applyEnvOverrides":  true,
	},
}

func TestRotationKeysAreReadOnlyByValidationAndTheirKeysConstructor(t *testing.T) {
	t.Parallel()

	repositoryRoot := filepath.Clean(filepath.Join("..", ".."))
	fset := token.NewFileSet()
	for _, topLevel := range []string{"cmd", "internal"} {
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
			relative = filepath.ToSlash(relative)
			for _, declaration := range file.Decls {
				owner := "<package scope>"
				if function, ok := declaration.(*ast.FuncDecl); ok {
					owner = function.Name.Name
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					allowed, guarded := rotationKeyReaders[selector.Sel.Name]
					if guarded && !allowed[relative+":"+owner] {
						t.Errorf("%s reads %s outside its allowed readers (in %s)",
							fset.Position(selector.Pos()), selector.Sel.Name, owner)
					}
					return true
				})
			}
			return nil
		})
		require.NoError(t, err)
	}
}
