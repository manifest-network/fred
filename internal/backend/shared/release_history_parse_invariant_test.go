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
// an unexported field, so this guard enforces what strictReleaseHistory
// documents. Only newStrictReleaseHistory mints a populated one, so every
// history has passed validation. Its row field may appear only in its accessor
// method, as the direct argument of cloneReleaseHistory, so no code can write,
// index into, alias, or copy a history's rows except through that accessor.
var releaseHistoryMinters = map[string]map[string]struct{}{
	"strictReleaseHistory": {"newStrictReleaseHistory": {}},
}

// releaseHistoryRowFields maps the row field to the one accessor that may read
// it.
var releaseHistoryRowFields = map[string]string{
	"strictRows": "strictReleaseHistory.strictReleases",
}

func TestStrictReleaseHistoryStaysAtItsChokePoints(t *testing.T) {
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
func bypass(strict strictReleaseHistory, rows []Release) strictReleaseHistory {
	_ = strictReleaseHistory{strictRows: rows}
	_ = []strictReleaseHistory{{strictRows: rows}}
	result := strictReleaseHistory{}
	strict.strictRows[0].Status = "active"
	alias := strict.strictRows
	alias[0] = Release{}
	_ = cloneReleaseHistory(strict.strictRows)
	for range strict.strictRows {
	}
	return result
}
func (h strictReleaseHistory) strictReleases() []Release { return cloneReleaseHistory(h.strictRows) }
func (h strictReleaseHistory) otherReleases() []Release { return cloneReleaseHistory(h.strictRows) }`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "bypass.go", source, 0)
	require.NoError(t, err)
	violations := releaseHistoryChokePointViolations(fset, file, make(map[string]bool))
	byLine := make(map[int][]string)
	for _, violation := range violations {
		var line, column int
		_, scanErr := fmt.Sscanf(violation, "bypass.go:%d:%d:", &line, &column)
		require.NoError(t, scanErr, violation)
		byLine[line] = append(byLine[line], violation)
	}
	for line, want := range map[int][]string{
		3: {"strictReleaseHistory literal outside"},
		4: {"strictReleaseHistory literal outside"},
		6: {"strictRows read other than by strictReleaseHistory.strictReleases"},
		7: {"strictRows read other than by strictReleaseHistory.strictReleases"},
		// A deep copy outside the accessor is still a read outside it.
		9:  {"strictRows read other than by strictReleaseHistory.strictReleases"},
		10: {"strictRows read other than by strictReleaseHistory.strictReleases"},
		// A different method on the same receiver gets no allowance.
		15: {"strictRows read other than by strictReleaseHistory.strictReleases"},
	} {
		joined := strings.Join(byLine[line], "\n")
		for _, fragment := range want {
			assert.Contains(t, joined, fragment, "line %d", line)
		}
	}
	// Line 5 is the zero value and line 14 is the accessor. Line 8 writes
	// through an alias the guard already refused where it was taken (line 7).
	for _, line := range []int{5, 8, 14} {
		assert.Empty(t, byLine[line], "line %d", line)
	}
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
	// A row field read is permitted only when it is cloned on the spot.
	cloned := make(map[*ast.SelectorExpr]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if function, ok := call.Fun.(*ast.Ident); ok && function.Name == "cloneReleaseHistory" {
			for _, argument := range call.Args {
				if selector, ok := argument.(*ast.SelectorExpr); ok {
					cloned[selector] = true
				}
			}
		}
		return true
	})
	for _, declaration := range file.Decls {
		enclosing := ""
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = function.Name.Name
			if function.Recv != nil && len(function.Recv.List) == 1 {
				if receiver, ok := function.Recv.List[0].Type.(*ast.Ident); ok {
					enclosing = receiver.Name + "." + enclosing
				}
			}
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
			case *ast.SelectorExpr:
				if accessor, guarded := releaseHistoryRowFields[node.Sel.Name]; guarded &&
					(enclosing != accessor || !cloned[node]) {
					report(node, "%s read other than by %s cloning it, in %q",
						node.Sel.Name, accessor, enclosing)
				}
			}
			return true
		})
	}
	return violations
}
