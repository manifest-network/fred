package testutil

// This repository-level guard makes the terminal budget's Ready boundary
// (ENG-799) structural. leasesm.ProvisionState.Status is written only by
// (*ProvisionState).SetStatus, and a budget moves between projections only
// through InheritTerminalBudget; both apply the one Ready-boundary function, so
// no present or future transition into or out of Ready can skip the
// sustained-Ready anchor or reset. A type cannot express "this exported field
// is written in one place", so these rules do:
//
//   - In every package that holds projections (leasesm and each package
//     importing it), an assignment to a .Status field happens only inside
//     SetStatus, unless it assigns a constant of another status vocabulary
//     (a shared.DiagnosticCapture value). Taking a .Status address is refused.
//   - A ProvisionState construction literal is keyed, and names Status only
//     as a backend.ProvisionStatus constant other than Ready: a new projection
//     enters Ready through SetStatus.
//   - The same literal names TerminalBudget only as the zero value, and no
//     code outside leasesm/terminal_budget.go assigns or addresses a
//     .TerminalBudget field.
//   - The budget's own fields are named only in leasesm/terminal_budget.go,
//     and its Ready anchor is written only by crossStatus.
//
// Every rule is proven to fire by TestProjectionStatusGuardsFire.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const (
	leasesmImportPath = "github.com/manifest-network/fred/internal/backend/shared/leasesm"
	backendImportPath = "github.com/manifest-network/fred/internal/backend"
	leasesmDir        = "internal/backend/shared/leasesm"
	budgetChokeFile   = "internal/backend/shared/leasesm/terminal_budget.go"
)

// foreignStatusConstantPrefixes are the other status vocabularies that share
// the field name Status in projection-holding packages. An assignment of one
// of their constants cannot write a projection's status. Any other right-hand
// side must be classified here or go through SetStatus.
var foreignStatusConstantPrefixes = []string{"DiagnosticCapture"}

// budgetFieldNames are TerminalBudget's unexported fields. Only leasesm can
// name them at all; the rule pins them to the one file that owns the rules.
var budgetFieldNames = []string{"consecutive", "readySince", "lastFailureCounted"}

type parsedGoFile struct {
	rel  string
	file *ast.File
	fset *token.FileSet
}

func TestProjectionStatusWritesUseTheChokePoint(t *testing.T) {
	root := repoRoot(t)
	var files []parsedGoFile
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			files = append(files, parsedGoFile{rel: rel, file: file, fset: fset})
		})
	}
	scoped := map[string]bool{leasesmDir: true}
	for _, parsed := range files {
		if _, imports := importNames(parsed.file)[leasesmImportPath]; imports {
			scoped[path.Dir(parsed.rel)] = true
		}
	}
	if len(scoped) < 2 {
		t.Fatalf("the guard must see the packages that import leasesm, saw %v", scoped)
	}
	var findings []string
	checked := 0
	for _, parsed := range files {
		if !scoped[path.Dir(parsed.rel)] {
			continue
		}
		checked++
		findings = append(findings, projectionStatusFindings(parsed.rel, parsed.file, parsed.fset)...)
	}
	if checked == 0 {
		t.Fatal("the guard checked no file")
	}
	if len(findings) > 0 {
		t.Errorf("a projection's status or terminal budget is written outside its choke point (ENG-799):\n  %s",
			strings.Join(findings, "\n  "))
	}
}

func TestProjectionStatusGuardsFire(t *testing.T) {
	const docker = "internal/backend/docker/recover.go"
	const leasesm = "internal/backend/shared/leasesm/lease_sm.go"
	const header = `package p
import (
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)
`
	tests := []struct {
		name string
		rel  string
		src  string
		want string
	}{
		{"Ready assigned directly", docker, header +
			`func f(p *leasesm.ProvisionState) { p.Status = backend.ProvisionStatusReady }`, "writes Status outside SetStatus"},
		{"a computed status assigned", docker, header +
			`func f(p *leasesm.ProvisionState, s backend.ProvisionStatus) { p.Status = s }`, "writes Status outside SetStatus"},
		{"op-assign", docker, header +
			`func f(p *leasesm.ProvisionState) { p.Status += "x" }`, "writes Status outside SetStatus"},
		{"address taken", docker, header +
			`func f(p *leasesm.ProvisionState) *backend.ProvisionStatus { return &p.Status }`, "takes the address of Status"},
		{"leasesm writes outside the choke point", leasesm, header +
			`func f(p *ProvisionState) { p.Status = backend.ProvisionStatusFailed }`, "writes Status outside SetStatus"},
		{"literal enters Ready", docker, header +
			`var v = leasesm.ProvisionState{Status: backend.ProvisionStatusReady}`, "constructs Status"},
		{"literal computes its status", docker, header +
			`func f(s backend.ProvisionStatus) any { return leasesm.ProvisionState{Status: s} }`, "constructs Status"},
		{"literal through an aliased import", docker, `package p
import (
	"github.com/manifest-network/fred/internal/backend"
	lsm "github.com/manifest-network/fred/internal/backend/shared/leasesm"
)
var v = &lsm.ProvisionState{Status: backend.ProvisionStatusReady}`, "constructs Status"},
		{"positional literal", docker, header +
			`var v = leasesm.ProvisionState{"lease"}`, "unkeyed ProvisionState literal"},
		{"literal carries a budget", docker, header +
			`func f(e leasesm.ProvisionState) any { return leasesm.ProvisionState{TerminalBudget: e.TerminalBudget} }`,
			"constructs TerminalBudget"},
		{"budget assigned", docker, header +
			`func f(r, e *leasesm.ProvisionState) { r.TerminalBudget = e.TerminalBudget }`, "writes TerminalBudget"},
		{"budget address taken", docker, header +
			`func f(r *leasesm.ProvisionState) any { return &r.TerminalBudget }`, "takes the address of TerminalBudget"},
		{"budget field named outside its file", leasesm, `package leasesm
func f(p *ProvisionState) { p.TerminalBudget.consecutive++ }`, "names TerminalBudget.consecutive"},
		{"anchor written outside crossStatus", budgetChokeFile, `package leasesm
import "time"
func (b *TerminalBudget) other(now time.Time) { b.readySince = now }`, "writes the Ready anchor outside crossStatus"},
		{"dot import", docker, `package p
import . "github.com/manifest-network/fred/internal/backend/shared/leasesm"
var v = ProvisionState{}`, "dot-imports leasesm"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := parseAndCheckProjectionStatus(t, test.rel, test.src)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	allowed := []struct{ rel, src string }{
		{budgetChokeFile, `package leasesm
import "time"
func (p *ProvisionState) SetStatus(status backend.ProvisionStatus, now time.Time) { p.Status = status }
func (b *TerminalBudget) crossStatus(now time.Time) { b.readySince = now; b.consecutive = 0 }
func (p *ProvisionState) InheritTerminalBudget() { p.TerminalBudget = TerminalBudget{} }`},
		{"internal/backend/docker/failure_diagnostics.go", header +
			`func f(o *shared.DiagnosticObservation) { o.Status = shared.DiagnosticCaptureUnavailable }`},
		{docker, header + `var v = leasesm.ProvisionState{
	Status: backend.ProvisionStatusProvisioning, TerminalBudget: leasesm.TerminalBudget{},
}`},
		{docker, header + `func f(r, e *leasesm.ProvisionState) { r.InheritTerminalBudget(e, now); r.SetStatus(backend.ProvisionStatusReady, now) }`},
	}
	for _, control := range allowed {
		if findings := parseAndCheckProjectionStatus(t, control.rel, control.src); len(findings) != 0 {
			t.Errorf("sanctioned shape in %s reported: %q", control.rel, findings)
		}
	}
}

func parseAndCheckProjectionStatus(t *testing.T, rel, src string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse synthetic source: %v", err)
	}
	return projectionStatusFindings(rel, file, fset)
}

// importNames maps each imported path to the name it is referenced by in this
// file ("." for a dot import).
func importNames(file *ast.File) map[string]string {
	names := make(map[string]string)
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		names[importPath] = name
	}
	return names
}

func projectionStatusFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	imports := importNames(file)
	inLeasesm := path.Dir(rel) == leasesmDir
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
	if !inLeasesm && imports[leasesmImportPath] == "." {
		report(file, "dot-imports leasesm, which hides who writes a projection")
	}
	// isPackageSelector reports whether expr is pkg.name for the given import.
	isPackageSelector := func(expr ast.Expr, importPath, name string) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != name {
			return false
		}
		qualifier, ok := selector.X.(*ast.Ident)
		return ok && imports[importPath] != "" && qualifier.Name == imports[importPath]
	}
	isLeasesmType := func(expr ast.Expr, name string) bool {
		if ident, ok := expr.(*ast.Ident); ok {
			return inLeasesm && ident.Name == name
		}
		return isPackageSelector(expr, leasesmImportPath, name)
	}
	isProvisionStatusConstant := func(expr ast.Expr) (string, bool) {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok || !strings.HasPrefix(selector.Sel.Name, "ProvisionStatus") {
			return "", false
		}
		if !isPackageSelector(selector, backendImportPath, selector.Sel.Name) {
			return "", false
		}
		return strings.TrimPrefix(selector.Sel.Name, "ProvisionStatus"), true
	}
	isForeignStatusConstant := func(expr ast.Expr) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		return slices.ContainsFunc(foreignStatusConstantPrefixes, func(prefix string) bool {
			return strings.HasPrefix(selector.Sel.Name, prefix)
		})
	}

	inspect := func(function string, root ast.Node) {
		inSetStatus := rel == budgetChokeFile && function == "ProvisionState.SetStatus"
		inCrossStatus := rel == budgetChokeFile && function == "TerminalBudget.crossStatus"
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for index, lhs := range typed.Lhs {
					selector, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					switch selector.Sel.Name {
					case "Status":
						if inSetStatus {
							continue
						}
						if typed.Tok == token.ASSIGN && len(typed.Rhs) == len(typed.Lhs) &&
							isForeignStatusConstant(typed.Rhs[index]) {
							continue
						}
						report(selector, "writes Status outside SetStatus")
					case "TerminalBudget":
						if rel != budgetChokeFile {
							report(selector, "writes TerminalBudget outside InheritTerminalBudget")
						}
					case "readySince":
						if !inCrossStatus {
							report(selector, "writes the Ready anchor outside crossStatus")
						}
					}
				}
			case *ast.IncDecStmt:
				if selector, ok := typed.X.(*ast.SelectorExpr); ok && selector.Sel.Name == "Status" && !inSetStatus {
					report(selector, "writes Status outside SetStatus")
				}
			case *ast.UnaryExpr:
				selector, ok := typed.X.(*ast.SelectorExpr)
				if typed.Op != token.AND || !ok {
					return true
				}
				switch {
				case selector.Sel.Name == "Status" && !inSetStatus:
					report(selector, "takes the address of Status")
				case selector.Sel.Name == "TerminalBudget" && rel != budgetChokeFile:
					report(selector, "takes the address of TerminalBudget")
				}
			case *ast.SelectorExpr:
				if inLeasesm && rel != budgetChokeFile && slices.Contains(budgetFieldNames, typed.Sel.Name) {
					report(typed, "names TerminalBudget.%s outside terminal_budget.go", typed.Sel.Name)
				}
			case *ast.CompositeLit:
				if !isLeasesmType(typed.Type, "ProvisionState") {
					return true
				}
				for _, element := range typed.Elts {
					field, ok := element.(*ast.KeyValueExpr)
					if !ok {
						report(element, "unkeyed ProvisionState literal: its status cannot be checked")
						continue
					}
					key, ok := field.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch key.Name {
					case "Status":
						if name, ok := isProvisionStatusConstant(field.Value); !ok || name == "Ready" {
							report(field, "constructs Status other than as a non-Ready backend.ProvisionStatus constant; enter it through SetStatus")
						}
					case "TerminalBudget":
						budget, ok := field.Value.(*ast.CompositeLit)
						if !ok || len(budget.Elts) != 0 || !isLeasesmType(budget.Type, "TerminalBudget") {
							report(field, "constructs TerminalBudget other than as the zero value; carry one with InheritTerminalBudget")
						}
					}
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok {
			inspect("", decl)
			continue
		}
		inspect(functionKey(function), function)
	}
	return findings
}

// functionKey is Name for a function and Receiver.Name for a method.
func functionKey(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + function.Name.Name
	}
	return function.Name.Name
}
