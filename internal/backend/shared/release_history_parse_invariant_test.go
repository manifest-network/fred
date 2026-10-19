package shared

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Go cannot stop a package's own code from building a struct literal or writing
// an unexported field, so this guard enforces what the release-history parse
// types document. Only the v0.13 array decoder mints a v013ReleaseHistory. Only
// newStrictReleaseHistory and adoptV013ReleaseHistory mint a populated
// strictReleaseHistory. No code assigns their fields after construction. Only
// the parsers and recognizers read raw v0.13 rows, so no writer can bind them
// without passing strict validation.
var releaseHistoryMinters = map[string]map[string]struct{}{
	"v013ReleaseHistory": {"decodeLegacyReleaseHistory": {}},
	"strictReleaseHistory": {
		"newStrictReleaseHistory": {},
		"adoptV013ReleaseHistory": {},
	},
}

var rawV013RowReaders = map[string]struct{}{
	"adoptV013ReleaseHistory":            {},
	"parseStrictStoredReleaseHistory":    {},
	"validateV013StoppedDeployingTails":  {},
	"validateV013DuplicateActiveHistory": {},
}

var releaseHistoryParseFields = map[string]struct{}{
	"v013Rows":           {},
	"strictRows":         {},
	"normalizedFromV013": {},
}

func TestStoppedReleaseHistoryTypesStayAtTheirChokePoints(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	minted := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry.Name(), nil, 0)
		require.NoError(t, err)
		for _, violation := range releaseHistoryChokePointViolations(fset, file, minted) {
			t.Error(violation)
		}
	}
	for typeName := range releaseHistoryMinters {
		assert.True(t, minted[typeName],
			"no constructor mints %s; the guard is matching nothing", typeName)
	}
}

func TestReleaseHistoryChokePointGuardFires(t *testing.T) {
	t.Parallel()

	const source = `package shared
func bypass(history v013ReleaseHistory) strictReleaseHistory {
	_ = v013ReleaseHistory{v013Rows: nil}
	_ = []strictReleaseHistory{{strictRows: history.v013Rows}}
	result := strictReleaseHistory{}
	result.normalizedFromV013 = true
	return result
}`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "bypass.go", source, 0)
	require.NoError(t, err)
	violations := releaseHistoryChokePointViolations(fset, file, make(map[string]bool))
	joined := strings.Join(violations, "\n")
	assert.Contains(t, joined, "bypass.go:3:")
	assert.Contains(t, joined, "v013ReleaseHistory literal outside")
	assert.Contains(t, joined, "strictReleaseHistory literal outside")
	assert.Contains(t, joined, "reads raw v0.13 rows")
	assert.Contains(t, joined, "assigns normalizedFromV013")
	assert.NotContains(t, joined, "bypass.go:5:",
		"the zero value carries no rows and is the error-path return")
}

func releaseHistoryChokePointViolations(
	fset *token.FileSet,
	file *ast.File,
	minted map[string]bool,
) []string {
	var violations []string
	report := func(node ast.Node, format string, args ...any) {
		violations = append(violations,
			fset.Position(node.Pos()).String()+": "+fmt.Sprintf(format, args...))
	}
	checkLiteral := func(enclosing, typeName string, literal *ast.CompositeLit) {
		allowed, guarded := releaseHistoryMinters[typeName]
		if !guarded || len(literal.Elts) == 0 {
			return
		}
		if _, ok := allowed[enclosing]; !ok {
			report(literal, "%s literal outside its constructor in %q", typeName, enclosing)
			return
		}
		minted[typeName] = true
	}
	for _, declaration := range file.Decls {
		enclosing := ""
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = function.Name.Name
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.CompositeLit:
				switch typed := node.Type.(type) {
				case *ast.Ident:
					checkLiteral(enclosing, typed.Name, node)
				case *ast.ArrayType:
					// Elided element types: []strictReleaseHistory{{...}}.
					if element, ok := typed.Elt.(*ast.Ident); ok {
						for _, value := range node.Elts {
							if inner, ok := value.(*ast.CompositeLit); ok && inner.Type == nil {
								checkLiteral(enclosing, element.Name, inner)
							}
						}
					}
				}
			case *ast.AssignStmt:
				for _, target := range node.Lhs {
					selector, ok := target.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if _, guarded := releaseHistoryParseFields[selector.Sel.Name]; guarded {
						report(selector, "assigns %s after construction", selector.Sel.Name)
					}
				}
			case *ast.SelectorExpr:
				if node.Sel.Name != "v013Rows" {
					break
				}
				if _, ok := rawV013RowReaders[enclosing]; !ok {
					report(node, "reads raw v0.13 rows outside the parsers in %q", enclosing)
				}
			}
			return true
		})
	}
	return violations
}
