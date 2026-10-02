package terminalverdict

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The seal on Verdict and Exhaustion holds across packages by construction
// (unexported fields). Inside this package nothing would stop a helper from
// returning Exhaustion{leaseUUID: id, consecutiveFailures: 1}, the proof an
// irreversible on-chain close accepts (ENG-799). These rules pin, across every
// production file and declaration, who may build or mutate the values and name
// the exhausted kind.
var (
	verdictLiteralMinters = map[string][]string{
		"Verdict":    {"FromProvision", "Labels"},
		"Exhaustion": {"Verdict.Exhausted"},
	}
	verdictFieldWriters    = []string{"FromProvision"}
	exhaustedKindReferents = []string{"FromProvision", "Verdict.Label", "Verdict.Exhausted", "TenantView"}
	verdictFields          = []string{"kind", "leaseUUID", "consecutiveFailures"}
)

func TestVerdictsMintedOnlyByTheirMinters(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var findings []string
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings = append(findings, verdictMintingViolations(fset, parsed)...)
		checked++
	}
	require.NotZero(t, checked)
	assert.Empty(t, findings)
}

func TestVerdictMintingGuardFires(t *testing.T) {
	const src = `package terminalverdict
var forged = Exhaustion{"lease", 1}
func proof(id string) Exhaustion { return Exhaustion{leaseUUID: id, consecutiveFailures: 1} }
func upgrade(v Verdict) Verdict { v.kind = kindExhausted; return v }
func fresh() Verdict { return Verdict{kind: kindRetry} }
func FromProvision() Verdict { v := Verdict{leaseUUID: "x"}; v.kind = kindExhausted; return v }
func (v Verdict) Exhausted() Exhaustion { return Exhaustion{leaseUUID: v.leaseUUID} }
func zero() Exhaustion { return Exhaustion{} }
`
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	findings := verdictMintingViolations(fset, parsed)
	for _, want := range []string{
		"package-level var builds an Exhaustion",
		"proof builds an Exhaustion",
		"upgrade writes kind",
		"upgrade names kindExhausted",
		"fresh builds a Verdict",
	} {
		assert.True(t, slices.ContainsFunc(findings, func(f string) bool { return strings.Contains(f, want) }),
			"missing %q in %q", want, findings)
	}
	assert.Len(t, findings, 5, "the sanctioned shapes stay silent: %q", findings)
}

func verdictMintingViolations(fset *token.FileSet, file *ast.File) []string {
	var findings []string
	inspect := func(key string, root ast.Node) {
		where := key
		if where == "" {
			where = "package-level var"
		}
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CompositeLit:
				typeName, ok := typed.Type.(*ast.Ident)
				if !ok || len(typed.Elts) == 0 {
					return true
				}
				if minters, sealed := verdictLiteralMinters[typeName.Name]; sealed && !slices.Contains(minters, key) {
					article := "a"
					if strings.ContainsRune("AEIOU", rune(typeName.Name[0])) {
						article = "an"
					}
					findings = append(findings, fmt.Sprintf("%s: %s builds %s %s",
						fset.Position(typed.Pos()), where, article, typeName.Name))
				}
			case *ast.AssignStmt:
				for _, lhs := range typed.Lhs {
					selector, ok := lhs.(*ast.SelectorExpr)
					if ok && slices.Contains(verdictFields, selector.Sel.Name) && !slices.Contains(verdictFieldWriters, key) {
						findings = append(findings, fmt.Sprintf("%s: %s writes %s",
							fset.Position(selector.Pos()), where, selector.Sel.Name))
					}
				}
			case *ast.Ident:
				if typed.Name == "kindExhausted" && !slices.Contains(exhaustedKindReferents, key) {
					findings = append(findings, fmt.Sprintf("%s: %s names kindExhausted",
						fset.Position(typed.Pos()), where))
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok {
			if general, isGeneral := decl.(*ast.GenDecl); isGeneral && general.Tok == token.CONST {
				continue // the const block declaring the kinds
			}
			inspect("", decl)
			continue
		}
		key := function.Name.Name
		if function.Recv != nil && len(function.Recv.List) == 1 {
			receiver := function.Recv.List[0].Type
			if star, isStar := receiver.(*ast.StarExpr); isStar {
				receiver = star.X
			}
			if ident, isIdent := receiver.(*ast.Ident); isIdent {
				key = ident.Name + "." + key
			}
		}
		inspect(key, function)
	}
	return findings
}
