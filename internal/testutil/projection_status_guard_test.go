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
//   - Inside that file the streak's count and start are written only by
//     countFailure and resetStreak, so the minimum-span floor is always
//     measured from the start of the streak being counted, and the exhausting
//     standing is named only by streakVerdict (which applies both floors) and
//     the predicate that reads it.
//   - A whole projection, which carries both Status and TerminalBudget, is
//     never overwritten or copied into another: no assignment to a
//     .ProvisionState field, no `*p = ...` through a pointer declared as a
//     projection (leasesm.ProvisionState, or a package type that embeds it),
//     and no `ProvisionState: <expr>` in a literal unless <expr> is itself a
//     checked ProvisionState literal. The two sanctioned value copies
//     (projectionCopySites) are the docker substrate's materialize and
//     recoveredFromProvision, which move a projection unchanged.
//   - A ProvisionState literal is checked however it is spelled: with its
//     type elided inside a slice, array or map literal, and no package may
//     declare another name (alias or defined type) for ProvisionState.
//
// What the guard cannot see without type information: a pointer obtained by
// `:=` (q := &x.ProvisionState; *q = ...), and a projection value held in a
// variable of an inferred type. Those stay review's job. The rules also say
// nothing about how fresh a predecessor passed to InheritTerminalBudget is:
// "only toward a reset" holds relative to that predecessor, so its freshness
// is the caller's responsibility, held in the docker substrate by passing the
// live entry under provisionsMu and by the recovery compare-and-swap, which
// compares TerminalBudget exactly.
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
var budgetFieldNames = []string{"consecutive", "streakStartedAt", "readySince", "standing"}

// streakFieldNames are the streak's count and start. They move together: only
// streakWriters write them, so a streak's start cannot drift from its count
// and the minimum-span floor cannot be measured from a stale start.
var (
	streakFieldNames = []string{"consecutive", "streakStartedAt"}
	streakWriters    = []string{"TerminalBudget.countFailure", "TerminalBudget.resetStreak"}
)

// exhaustingStanding is the one standing that exhausts the budget. Only the
// streak decision (which applies the count threshold and the minimum span) and
// the predicate that reads it may name it, so no other code can record or
// test for an exhausting failure without both floors.
const exhaustingStanding = "countedExhausting"

var exhaustingStandingNamers = []string{"streakVerdict", "standingFailure.exhausts"}

// projectionCopySites are the only functions that may copy a whole projection
// into a literal (`ProvisionState: <expr>`), keyed by file then function. Both
// move a projection unchanged: materialize publishes a recovered snapshot, and
// recoveredFromProvision snapshots a live entry for recover's compare-and-swap.
var projectionCopySites = map[string][]string{
	"internal/backend/docker/recovered_provision.go": {"recoveredProvision.materialize", "recoveredFromProvision"},
}

// projectionTypeNames returns the names of the types declared in file whose
// struct embeds leasesm.ProvisionState (in leasesm itself, ProvisionState is
// one). A pointer to one of them reaches a whole projection.
func projectionTypeNames(rel string, file *ast.File) []string {
	inLeasesm := path.Dir(rel) == leasesmDir
	leasesmName := importNames(file)[leasesmImportPath]
	var names []string
	if inLeasesm {
		names = append(names, "ProvisionState")
	}
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok {
			return true
		}
		structType, ok := spec.Type.(*ast.StructType)
		if !ok {
			return true
		}
		for _, field := range structType.Fields.List {
			if len(field.Names) != 0 {
				continue
			}
			embedded := field.Type
			if star, ok := embedded.(*ast.StarExpr); ok {
				embedded = star.X
			}
			switch typed := embedded.(type) {
			case *ast.Ident:
				if inLeasesm && typed.Name == "ProvisionState" {
					names = append(names, spec.Name.Name)
				}
			case *ast.SelectorExpr:
				qualifier, ok := typed.X.(*ast.Ident)
				if ok && leasesmName != "" && qualifier.Name == leasesmName && typed.Sel.Name == "ProvisionState" {
					names = append(names, spec.Name.Name)
				}
			}
		}
		return true
	})
	return names
}

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
	projectionTypes := make(map[string]map[string]bool)
	for _, parsed := range files {
		dir := path.Dir(parsed.rel)
		if !scoped[dir] {
			continue
		}
		for _, name := range projectionTypeNames(parsed.rel, parsed.file) {
			if projectionTypes[dir] == nil {
				projectionTypes[dir] = make(map[string]bool)
			}
			projectionTypes[dir][name] = true
		}
	}
	// The docker substrate's projection types must be recognized, or the
	// whole-projection rules would see no pointer to check.
	for _, name := range []string{"provision", "recoveredProvision"} {
		if !projectionTypes["internal/backend/docker"][name] {
			t.Fatalf("the guard did not recognize docker's %s as a projection type: %v", name, projectionTypes)
		}
	}
	var findings []string
	checked := 0
	for _, parsed := range files {
		dir := path.Dir(parsed.rel)
		if !scoped[dir] {
			continue
		}
		checked++
		findings = append(findings, projectionStatusFindings(parsed.rel, parsed.file, parsed.fset, projectionTypes[dir])...)
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
		{"count raised outside countFailure", budgetChokeFile, `package leasesm
func (b *TerminalBudget) other() { b.consecutive++ }`, "writes the streak outside countFailure and resetStreak"},
		{"count cleared outside resetStreak", budgetChokeFile, `package leasesm
func (b *TerminalBudget) crossStatus() { b.consecutive = 0 }`, "writes the streak outside countFailure and resetStreak"},
		{"streak start written outside countFailure", budgetChokeFile, `package leasesm
import "time"
func (b *TerminalBudget) other(now time.Time) { b.streakStartedAt = now }`, "writes the streak outside countFailure and resetStreak"},
		{"streak address taken", budgetChokeFile, `package leasesm
func (b *TerminalBudget) other() *int { return &b.consecutive }`, "takes the address of the streak's consecutive"},
		{"exhausting standing recorded without the floors", budgetChokeFile, `package leasesm
func (b *TerminalBudget) other() { b.standing = countedExhausting }`, "names countedExhausting"},
		{"exhausting standing named in another leasesm file", leasesm, `package leasesm
var v = countedExhausting`, "names countedExhausting"},
		{"dot import", docker, `package p
import . "github.com/manifest-network/fred/internal/backend/shared/leasesm"
var v = ProvisionState{}`, "dot-imports leasesm"},
		{"whole projection overwritten in an UpdateFn closure", docker, header +
			`func f(s interface{ UpdateFn(string, func(*leasesm.ProvisionState)) bool }, q *leasesm.ProvisionState) {
	s.UpdateFn("l", func(p *leasesm.ProvisionState) { *p = *q })
}`, "overwrites a whole projection through *p"},
		{"whole projection overwritten through a declared var", docker, header +
			`func f(q *leasesm.ProvisionState) { var p *leasesm.ProvisionState = q; *p = leasesm.ProvisionState{} }`,
			"overwrites a whole projection through *p"},
		{"embedding projection overwritten through its pointer", docker, header +
			`type provision struct{ leasesm.ProvisionState }
func f(p, q *provision) { *p = *q }`, "overwrites a whole projection through *p"},
		{"embedded projection field overwritten", docker, header +
			`type provision struct{ leasesm.ProvisionState }
func f(p *provision, q leasesm.ProvisionState) { p.ProvisionState = q }`, "overwrites a whole projection (.ProvisionState)"},
		{"projection copied into a literal", docker, header +
			`type provision struct{ leasesm.ProvisionState }
func f(q *provision) any { return &provision{ProvisionState: q.ProvisionState} }`, "copies a whole projection into a literal"},
		{"unkeyed embedding literal", docker, header +
			`type provision struct{ leasesm.ProvisionState }
func f(s leasesm.ProvisionState) any { return provision{s} }`, "unkeyed projection literal"},
		{"type-elided slice element enters Ready", docker, header +
			`var v = []leasesm.ProvisionState{{Status: backend.ProvisionStatusReady}}`, "constructs Status"},
		{"type-elided pointer map element enters Ready", docker, header +
			`var v = map[string]*leasesm.ProvisionState{"a": {Status: backend.ProvisionStatusReady}}`, "constructs Status"},
		{"nested type-elided element enters Ready", docker, header +
			`var v = [][]leasesm.ProvisionState{{{Status: backend.ProvisionStatusReady}}}`, "constructs Status"},
		{"alias of ProvisionState", docker, header +
			`type ps = leasesm.ProvisionState
var v = ps{Status: backend.ProvisionStatusReady}`, "declares ps as another name for ProvisionState"},
		{"defined type over ProvisionState", docker, header +
			`type ps leasesm.ProvisionState`, "declares ps as another name for ProvisionState"},
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
func (b *TerminalBudget) crossStatus(now time.Time) { b.readySince = now; b.resetStreak() }
func (b *TerminalBudget) countFailure(now time.Time) { b.streakStartedAt = now; b.consecutive++ }
func (b *TerminalBudget) resetStreak() { b.consecutive = 0; b.streakStartedAt = time.Time{} }
const (
	countedWithinBudget standingFailure = iota
	countedExhausting
)
func streakVerdict() standingFailure { return countedExhausting }
func (s standingFailure) exhausts() bool { return s == countedExhausting }
func (p *ProvisionState) InheritTerminalBudget() { p.TerminalBudget = TerminalBudget{} }`},
		{"internal/backend/docker/failure_diagnostics.go", header +
			`func f(o *shared.DiagnosticObservation) { o.Status = shared.DiagnosticCaptureUnavailable }`},
		{docker, header + `var v = leasesm.ProvisionState{
	Status: backend.ProvisionStatusProvisioning, TerminalBudget: leasesm.TerminalBudget{},
}`},
		{docker, header + `func f(r, e *leasesm.ProvisionState) { r.InheritTerminalBudget(e, now); r.SetStatus(backend.ProvisionStatusReady, now) }`},
		{"internal/backend/docker/recovered_provision.go", header + `type provision struct{ leasesm.ProvisionState }
type recoveredProvision struct{ leasesm.ProvisionState }
func (rec recoveredProvision) materialize() *provision { state := rec.ProvisionState; return &provision{ProvisionState: state} }
func recoveredFromProvision(p *provision) recoveredProvision { return recoveredProvision{ProvisionState: p.ProvisionState} }`},
		{docker, header + `type provision struct{ leasesm.ProvisionState }
var v = []provision{{ProvisionState: leasesm.ProvisionState{Status: backend.ProvisionStatusProvisioning}}}
func f(n *int, b *bool, p *provision) { *n = 1; *b = true; p.SetStatus(backend.ProvisionStatusReady, now) }
var w = []struct{ Status string }{{Status: "x"}}`},
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
	projectionTypes := make(map[string]bool)
	for _, name := range projectionTypeNames(rel, file) {
		projectionTypes[name] = true
	}
	return projectionStatusFindings(rel, file, fset, projectionTypes)
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

func projectionStatusFindings(rel string, file *ast.File, fset *token.FileSet, projectionTypes map[string]bool) []string {
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
	// isProjectionType reports whether expr names leasesm.ProvisionState or a
	// type of this package that embeds it.
	isProjectionType := func(expr ast.Expr) bool {
		if ident, ok := expr.(*ast.Ident); ok && projectionTypes[ident.Name] {
			return true
		}
		return isLeasesmType(expr, "ProvisionState")
	}
	stripStar := func(expr ast.Expr) ast.Expr {
		if star, ok := expr.(*ast.StarExpr); ok {
			return star.X
		}
		return expr
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
		inStreakWriter := rel == budgetChokeFile && slices.Contains(streakWriters, function)
		inExhaustingNamer := rel == budgetChokeFile && slices.Contains(exhaustingStandingNamers, function)
		inProjectionCopySite := slices.Contains(projectionCopySites[rel], function)
		// projectionPointers are the names this declaration declares as a
		// pointer to a whole projection: parameters (of the function and of
		// any closure in it, such as an UpdateFn callback) and var specs.
		projectionPointers := make(map[string]bool)
		declarePointers := func(names []*ast.Ident, typeExpr ast.Expr) {
			star, ok := typeExpr.(*ast.StarExpr)
			if !ok || !isProjectionType(star.X) {
				return
			}
			for _, name := range names {
				projectionPointers[name.Name] = true
			}
		}
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncType:
				for _, list := range []*ast.FieldList{typed.Params, typed.Results} {
					if list == nil {
						continue
					}
					for _, field := range list.List {
						declarePointers(field.Names, field.Type)
					}
				}
			case *ast.ValueSpec:
				if typed.Type != nil {
					declarePointers(typed.Names, typed.Type)
				}
			}
			return true
		})
		if function != "" {
			if decl, ok := root.(*ast.FuncDecl); ok && decl.Recv != nil {
				for _, field := range decl.Recv.List {
					declarePointers(field.Names, field.Type)
				}
			}
		}
		// elided maps a type-elided composite literal (an element of a slice,
		// array or map literal) to the element type its parent implies.
		elided := make(map[*ast.CompositeLit]ast.Expr)
		// The constant's own declaration names it; that is not a use.
		declaring := make(map[*ast.Ident]bool)
		if gen, ok := root.(*ast.GenDecl); ok && gen.Tok == token.CONST {
			for _, spec := range gen.Specs {
				if value, ok := spec.(*ast.ValueSpec); ok {
					for _, name := range value.Names {
						declaring[name] = true
					}
				}
			}
		}
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.Ident:
				if inLeasesm && typed.Name == exhaustingStanding && !inExhaustingNamer && !declaring[typed] {
					report(typed, "names %s outside streakVerdict and standingFailure.exhausts", exhaustingStanding)
				}
			case *ast.AssignStmt:
				for index, lhs := range typed.Lhs {
					if star, ok := lhs.(*ast.StarExpr); ok {
						if ident, ok := ast.Unparen(star.X).(*ast.Ident); ok && projectionPointers[ident.Name] {
							report(star, "overwrites a whole projection through *%s, moving its Status and TerminalBudget around their choke points", ident.Name)
						}
						continue
					}
					selector, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					switch selector.Sel.Name {
					case "ProvisionState":
						report(selector, "overwrites a whole projection (.ProvisionState), moving its Status and TerminalBudget around their choke points")
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
					case "consecutive", "streakStartedAt":
						if rel == budgetChokeFile && !inStreakWriter {
							report(selector, "writes the streak outside countFailure and resetStreak")
						}
					}
				}
			case *ast.IncDecStmt:
				selector, ok := typed.X.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch {
				case selector.Sel.Name == "Status" && !inSetStatus:
					report(selector, "writes Status outside SetStatus")
				case rel == budgetChokeFile && slices.Contains(streakFieldNames, selector.Sel.Name) && !inStreakWriter:
					report(selector, "writes the streak outside countFailure and resetStreak")
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
				case rel == budgetChokeFile && slices.Contains(streakFieldNames, selector.Sel.Name):
					report(selector, "takes the address of the streak's %s", selector.Sel.Name)
				}
			case *ast.SelectorExpr:
				if inLeasesm && rel != budgetChokeFile && slices.Contains(budgetFieldNames, typed.Sel.Name) {
					report(typed, "names TerminalBudget.%s outside terminal_budget.go", typed.Sel.Name)
				}
			case *ast.TypeSpec:
				if isLeasesmType(stripStar(typed.Type), "ProvisionState") {
					report(typed, "declares %s as another name for ProvisionState, which hides its literals from this guard", typed.Name.Name)
				}
			case *ast.CompositeLit:
				litType := typed.Type
				if litType == nil {
					litType = elided[typed]
				}
				// Carry the element type into type-elided children, so
				// []leasesm.ProvisionState{{...}} is checked like a named
				// literal.
				var elementType ast.Expr
				switch container := stripStar(litType).(type) {
				case *ast.ArrayType:
					elementType = container.Elt
				case *ast.MapType:
					elementType = container.Value
				}
				for _, element := range typed.Elts {
					if field, ok := element.(*ast.KeyValueExpr); ok {
						element = field.Value
						if key, ok := field.Key.(*ast.Ident); ok && key.Name == "ProvisionState" && elementType == nil {
							value, ok := field.Value.(*ast.CompositeLit)
							if (!ok || !isLeasesmType(value.Type, "ProvisionState")) && !inProjectionCopySite {
								report(field, "copies a whole projection into a literal (ProvisionState: <expr>); build it from a checked ProvisionState literal")
							}
						}
					}
					if child, ok := element.(*ast.CompositeLit); ok && child.Type == nil && elementType != nil {
						elided[child] = elementType
					}
				}
				if litType == nil || !isLeasesmType(stripStar(litType), "ProvisionState") {
					if litType != nil && isProjectionType(stripStar(litType)) {
						for _, element := range typed.Elts {
							if _, ok := element.(*ast.KeyValueExpr); !ok {
								report(element, "unkeyed projection literal: its embedded ProvisionState cannot be checked")
							}
						}
					}
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
