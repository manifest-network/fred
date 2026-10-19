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
// types document. Only the v0.13 array decoder mints a v013ReleaseHistory, and
// only newStrictReleaseHistory and adoptV013ReleaseHistory mint a populated
// strictReleaseHistory. A row field may appear only in its accessor method, as
// the direct argument of cloneReleaseHistory, so no code can write, index into,
// alias, or copy a history's rows except through that accessor. Only the
// parsers and recognizers call the raw v0.13 accessor, so no writer can bind
// v0.13 rows without passing strict validation.
var releaseHistoryMinters = map[string]map[string]struct{}{
	"v013ReleaseHistory": {"decodeLegacyReleaseHistory": {}},
	"strictReleaseHistory": {
		"newStrictReleaseHistory": {},
		"adoptV013ReleaseHistory": {},
	},
}

// releaseHistoryRowFields maps each row field to the one accessor that may
// read it. Cloning alone is not enough: a clone of raw v0.13 rows elsewhere
// would bypass the rawV013RowReaders allowlist on the accessor.
var releaseHistoryRowFields = map[string]string{
	"v013Rows":   "v013ReleaseHistory.v013Releases",
	"strictRows": "strictReleaseHistory.strictReleases",
}

var rawV013RowReaders = map[string]struct{}{
	"adoptV013ReleaseHistory":            {},
	"parseStrictStoredReleaseHistory":    {},
	"validateV013StoppedDeployingTails":  {},
	"validateV013DuplicateActiveHistory": {},
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
func bypass(history v013ReleaseHistory, strict strictReleaseHistory) strictReleaseHistory {
	_ = v013ReleaseHistory{v013Rows: nil}
	_ = []strictReleaseHistory{{strictRows: history.v013Releases()}}
	result := strictReleaseHistory{}
	result.normalizedFromV013 = true
	strict.strictRows[0].Status = "active"
	rows := history.v013Rows
	rows[0] = Release{}
	_ = cloneReleaseHistory(history.v013Rows)
	for range strict.strictRows {
	}
	return result
}
func (h v013ReleaseHistory) v013Releases() []Release { return cloneReleaseHistory(h.v013Rows) }
func (h strictReleaseHistory) strictReleases() []Release { return cloneReleaseHistory(h.strictRows) }
func (h strictReleaseHistory) v013Releases() []Release { return cloneReleaseHistory(h.strictRows) }`
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
		3: {"v013ReleaseHistory literal outside"},
		4: {"strictReleaseHistory literal outside", "reads raw v0.13 rows"},
		6: {"assigns normalizedFromV013"},
		7: {"strictRows read other than by strictReleaseHistory.strictReleases"},
		8: {"v013Rows read other than by v013ReleaseHistory.v013Releases"},
		// A deep copy outside the accessor still bypasses the reader allowlist.
		10: {"v013Rows read other than by v013ReleaseHistory.v013Releases"},
		11: {"strictRows read other than by strictReleaseHistory.strictReleases"},
		// A same-named method on the wrong receiver gets no allowance.
		17: {"strictRows read other than by strictReleaseHistory.strictReleases"},
	} {
		joined := strings.Join(byLine[line], "\n")
		for _, fragment := range want {
			assert.Contains(t, joined, fragment, "line %d", line)
		}
	}
	// Line 5 is the zero value, and lines 15 and 16 are the two accessors. Line 9
	// writes through an alias the guard already refused where it was taken
	// (line 8).
	for _, line := range []int{5, 9, 15, 16} {
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
			case *ast.AssignStmt:
				for _, target := range node.Lhs {
					if selector, ok := target.(*ast.SelectorExpr); ok &&
						selector.Sel.Name == "normalizedFromV013" {
						report(selector, "assigns normalizedFromV013 after construction")
					}
				}
			case *ast.SelectorExpr:
				if accessor, guarded := releaseHistoryRowFields[node.Sel.Name]; guarded &&
					(enclosing != accessor || !cloned[node]) {
					report(node, "%s read other than by %s cloning it, in %q",
						node.Sel.Name, accessor, enclosing)
				}
				if node.Sel.Name == "v013Releases" {
					if _, ok := rawV013RowReaders[enclosing]; !ok {
						report(node, "reads raw v0.13 rows outside the parsers in %q", enclosing)
					}
				}
			}
			return true
		})
	}
	return violations
}
