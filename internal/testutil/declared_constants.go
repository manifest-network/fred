package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// DeclaredConstantNames returns the name of every constant of typeName
// declared in the Go file at path, including the implicitly typed lines of an
// iota block. A test pairs it with a decision table to require an explicit
// mapping for every member of a closed enum: a new member fails the test until
// someone decides how it is handled, instead of reaching a default branch.
func DeclaredConstantNames(t testing.TB, path, typeName string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	var names []string
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		inEnum := false
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			switch {
			case value.Type != nil:
				ident, isIdent := value.Type.(*ast.Ident)
				inEnum = isIdent && ident.Name == typeName
			case len(value.Values) != 0:
				// An explicit untyped value starts a different constant; only a
				// bare line repeats the previous type and expression.
				inEnum = false
			}
			if !inEnum {
				continue
			}
			for _, name := range value.Names {
				if name.Name != "_" {
					names = append(names, name.Name)
				}
			}
		}
	}
	require.NotEmpty(t, names, "no %s constants in %s", typeName, path)
	return names
}
