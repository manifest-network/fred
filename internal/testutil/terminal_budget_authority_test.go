package testutil

// This repository-level guard pins who may mint, decode and act on the
// consecutive-failure terminal budget (ENG-799), whose exhausted verdict makes
// providerd close a paying lease on-chain, irreversibly. The types already seal
// most of it across package boundaries (failurecause.Cause, terminalverdict.
// Verdict and leasesm.TerminalBudget have unexported fields, and the event
// session type is unexported); these rules cover what a type cannot: the
// wire's exported string constant and type, and the attribution entry points.
// Package references are resolved through each file's imports by path, so an
// aliased import, a method value or a parenthesized conversion is caught too.
//
//   - The wire constant TerminalVerdictExhausted is named only where it is
//     declared, minted and decoded.
//   - The literal "exhausted" appears in production only at that declaration,
//     or as a metric label argument.
//   - The TerminalVerdict type is named only in those same three files.
//   - A non-empty TerminalBudgetObservation literal is built only by the one
//     backend minter, leasesm's ObserveTerminalBudget.
//   - In internal/provisioner, FailCount is read only as a log argument: it
//     never decides a close.
//   - failurecause.ClassifyDeath, the only constructor of a counting cause, is
//     named only in the state machine's death entry action.
//   - failurecause.NewEventSession and the session's Observe methods, the only
//     minters of live provenance, are named only in the Docker event loop's
//     reader, and leasesm.NewLiveContainerDiedObservation only in its
//     dispatcher; recover.go's sweep can mint no live provenance.
//   - Neither package may be dot-imported, which would hide these names.
//
// Every rule is proven to fire by TestTerminalBudgetAuthorityGuardsFire.

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
	terminalBudgetWireFile   = "internal/backend/client.go"
	terminalBudgetMintFile   = "internal/backend/shared/leasesm/terminal_budget.go"
	terminalBudgetDecodeFile = "internal/provisioner/terminalverdict/verdict.go"
	terminalBudgetDeathFile  = "internal/backend/shared/leasesm/lease_sm.go"
	terminalBudgetEventsFile = "internal/backend/docker/container_event_loop.go"
	failurecauseImportPath   = "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	failurecauseDir          = "internal/backend/shared/leasesm/failurecause"
	backendDir               = "internal/backend"
)

var terminalVerdictAuthorityFiles = []string{
	terminalBudgetWireFile, terminalBudgetMintFile, terminalBudgetDecodeFile,
}

// attributionSite is the one file and function allowed to name an attribution
// entry point.
type attributionSite struct{ file, function string }

var (
	classifyDeathSite = attributionSite{terminalBudgetDeathFile, "leaseSM.onEnterFailing"}
	eventReaderSite   = attributionSite{terminalBudgetEventsFile, "Backend.consumeContainerEventStream"}
	liveDeathSite     = attributionSite{terminalBudgetEventsFile, "Backend.dispatchLiveContainerDeath"}
)

// sessionMethods are the event session's methods. Only the reader holding the
// session may name them; the names are distinctive in this repository.
var sessionMethods = []string{"ObserveStart", "ObserveSignal", "ObserveExit"}

// logCallNames are the slog and *slog.Logger methods through which a
// FailCount may legitimately flow: as a structured log attribute.
var logCallNames = []string{
	"Debug", "Info", "Warn", "Error", "DebugContext", "InfoContext", "WarnContext", "ErrorContext", "Log",
}

func TestTerminalBudgetAuthorityIsSealed(t *testing.T) {
	root := repoRoot(t)
	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			findings = append(findings, terminalBudgetAuthorityFindings(rel, file, fset)...)
		})
	}
	if len(findings) > 0 {
		t.Errorf("terminal budget authority escaped its sealed sites (ENG-799):\n  %s",
			strings.Join(findings, "\n  "))
	}
}

func TestTerminalBudgetAuthorityGuardsFire(t *testing.T) {
	const failurecauseImport = `import "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
`
	const leasesmImport = `import "github.com/manifest-network/fred/internal/backend/shared/leasesm"
`
	const backendImport = `import "github.com/manifest-network/fred/internal/backend"
`
	tests := []struct {
		name string
		rel  string
		src  string
		want string
	}{
		{"wire constant outside its sites", "internal/provisioner/reconciler.go",
			"package p\n" + backendImport + `var v = backend.TerminalVerdictExhausted`, "names TerminalVerdictExhausted"},
		{"exhausted literal compared directly", "internal/provisioner/reconciler.go",
			`package p
func f(v string) bool { return v == "exhausted" }`, `literal "exhausted"`},
		{"verdict conversion outside its sites", "internal/api/handlers.go",
			"package p\n" + backendImport + `var v = backend.TerminalVerdict("retry")`, "names the TerminalVerdict type"},
		{"parenthesized verdict conversion", "internal/api/handlers.go",
			"package p\n" + backendImport + `func f(s string) any { return (backend.TerminalVerdict)(s) }`,
			"names the TerminalVerdict type"},
		{"verdict type alias", "internal/api/handlers.go",
			"package p\n" + backendImport + `type tv = backend.TerminalVerdict`, "names the TerminalVerdict type"},
		{"verdict type in its own package", "internal/backend/router.go",
			`package backend
var v = TerminalVerdict("retry")`, "names the TerminalVerdict type"},
		{"budget observation literal outside the minter", "internal/backend/docker/info.go",
			"package p\n" + backendImport + `var v = &backend.TerminalBudgetObservation{ConsecutiveFailures: 3}`,
			"builds a TerminalBudgetObservation"},
		{"fail count deciding in the provisioner", "internal/provisioner/reconcile_plan.go",
			`package p
func f(p struct{ FailCount int }) bool { return p.FailCount >= 3 }`, "reads FailCount"},
		{"fail count stored in provisioner facts", "internal/provisioner/reconciler.go",
			`package p
type facts struct{ n int }
func f(p struct{ FailCount int }) facts { return facts{n: p.FailCount} }`, "reads FailCount"},
		{"death classified outside the state machine", "internal/backend/docker/recover.go",
			"package p\n" + failurecauseImport +
				`var c = failurecause.ClassifyDeath("c", failurecause.Provenance{}, failurecause.Exited())`,
			"names failurecause.ClassifyDeath"},
		{"death classified elsewhere in the state machine", terminalBudgetDeathFile,
			"package leasesm\n" + failurecauseImport +
				`func (lsm *leaseSM) other() { _ = failurecause.ClassifyDeath("c", failurecause.Provenance{}, failurecause.Exited()) }`,
			"names failurecause.ClassifyDeath"},
		{"event session minted outside the event loop", "internal/backend/docker/info.go",
			"package p\n" + failurecauseImport + `var s = failurecause.NewEventSession()`,
			"names failurecause.NewEventSession"},
		{"event session minted in the sweep", "internal/backend/docker/recover.go",
			"package docker\n" + failurecauseImport +
				`func (b *Backend) recoverState() { s := failurecause.NewEventSession(); _ = s }`,
			"names failurecause.NewEventSession"},
		{"event session minted through an alias", "internal/backend/docker/recover.go",
			`package docker
import fc "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
func (b *Backend) recoverState() { _ = fc.NewEventSession() }`, "names failurecause.NewEventSession"},
		{"event session constructor as a value", "internal/backend/docker/recover.go",
			"package docker\n" + failurecauseImport + `var mint = failurecause.NewEventSession`,
			"names failurecause.NewEventSession"},
		{"event session minted by the dispatcher", terminalBudgetEventsFile,
			"package docker\n" + failurecauseImport +
				`func (b *Backend) dispatchLiveContainerDeath() { _ = failurecause.NewEventSession() }`,
			"names failurecause.NewEventSession"},
		{"session observed outside the reader", "internal/backend/docker/recover.go",
			"package docker\n" + failurecauseImport +
				`func f(s interface{ ObserveExit(string) failurecause.Provenance }) { _ = s.ObserveExit("c") }`,
			"names ObserveExit"},
		{"live death built in the sweep", "internal/backend/docker/recover.go",
			"package docker\n" + leasesmImport +
				`func (b *Backend) recoverState() { _, _ = leasesm.NewLiveContainerDiedObservation(r, p) }`,
			"names leasesm.NewLiveContainerDiedObservation"},
		{"live death built inside leasesm", "internal/backend/shared/leasesm/lease_actor.go",
			`package leasesm
func NewContainerDiedObservation() { _, _ = NewLiveContainerDiedObservation(r, p) }`,
			"names leasesm.NewLiveContainerDiedObservation"},
		{"failurecause dot-imported", "internal/backend/docker/recover.go",
			`package docker
import . "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
var c = Platform()`, "dot-imports failurecause"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", test.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse synthetic source: %v", err)
			}
			findings := terminalBudgetAuthorityFindings(test.rel, file, fset)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	// Negative controls: the sanctioned shapes must stay silent.
	allowed := []struct {
		rel string
		src string
	}{
		{terminalBudgetDecodeFile, "package p\n" + backendImport + `var v = backend.TerminalVerdictExhausted`},
		{terminalBudgetWireFile, `package backend
type TerminalVerdict string
const TerminalVerdictExhausted TerminalVerdict = "exhausted"`},
		{"internal/chain/client.go", `package p
func f(m interface{ WithLabelValues(...string) any }) { m.WithLabelValues("exhausted") }`},
		{"internal/provisioner/reconciler.go", `package p
import "log/slog"
func f(p struct{ FailCount int }) { slog.Warn("x", "fail_count", p.FailCount) }`},
		{"internal/backend/docker/info.go", "package p\n" + backendImport + `var v = backend.TerminalBudgetObservation{}`},
		{terminalBudgetDeathFile, "package leasesm\n" + failurecauseImport +
			`func (lsm *leaseSM) onEnterFailing() { _ = failurecause.ClassifyDeath("c", failurecause.Provenance{}, failurecause.Exited()) }`},
		{terminalBudgetEventsFile, "package docker\n" + failurecauseImport +
			`func (b *Backend) consumeContainerEventStream() { s := failurecause.NewEventSession(); s.ObserveStart("c"); s.ObserveSignal("c"); _ = s.ObserveExit("c") }`},
		{terminalBudgetEventsFile, "package docker\n" + leasesmImport +
			`func (b *Backend) dispatchLiveContainerDeath() { _, _ = leasesm.NewLiveContainerDiedObservation(r, p) }`},
		{"internal/backend/shared/leasesm/lease_actor.go", `package leasesm
func NewLiveContainerDiedObservation() {}`},
		{failurecauseDir + "/provenance.go", `package failurecause
func NewEventSession() *eventSession { return nil }
func (s *eventSession) ObserveExit(string) Provenance { return Provenance{} }`},
	}
	for _, control := range allowed {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "synthetic.go", control.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse synthetic source: %v", err)
		}
		if findings := terminalBudgetAuthorityFindings(control.rel, file, fset); len(findings) != 0 {
			t.Errorf("sanctioned shape in %s reported: %q", control.rel, findings)
		}
	}
}

func terminalBudgetAuthorityFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	authorized := slices.Contains(terminalVerdictAuthorityFiles, rel)
	exempt := sanctionedArguments(file)
	var findings []string
	if !authorized {
		findings = append(findings, wireConstantFindings(rel, file, fset)...)
	}
	findings = append(findings, literalFindings(rel, file, fset, exempt)...)
	findings = append(findings, attributionFindings(rel, file, fset, authorized)...)
	findings = append(findings, compositeFindings(rel, file, fset)...)
	if strings.HasPrefix(rel, "internal/provisioner/") {
		findings = append(findings, failCountFindings(rel, file, fset, exempt)...)
	}
	return findings
}

// sanctionedArguments are the expressions passed directly to a metric label or
// a log call. A literal or a FailCount there labels or logs; it decides nothing.
// An expression nested inside such an argument is not exempt.
func sanctionedArguments(file *ast.File) map[ast.Node]bool {
	exempt := make(map[ast.Node]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name == "WithLabelValues" || slices.Contains(logCallNames, selector.Sel.Name) {
			for _, arg := range call.Args {
				exempt[arg] = true
			}
		}
		return true
	})
	return exempt
}

func wireConstantFindings(rel string, root ast.Node, fset *token.FileSet) []string {
	var findings []string
	ast.Inspect(root, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "TerminalVerdictExhausted" {
			findings = append(findings, fmt.Sprintf("%s (%s): names TerminalVerdictExhausted", fset.Position(ident.Pos()), rel))
		}
		return true
	})
	return findings
}

func literalFindings(rel string, root ast.Node, fset *token.FileSet, exempt map[ast.Node]bool) []string {
	if rel == terminalBudgetWireFile {
		return nil
	}
	var findings []string
	ast.Inspect(root, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || exempt[literal] {
			return true
		}
		if text, err := strconv.Unquote(literal.Value); err == nil && text == "exhausted" {
			findings = append(findings, fmt.Sprintf("%s (%s): uses the literal \"exhausted\"", fset.Position(literal.Pos()), rel))
		}
		return true
	})
	return findings
}

// attributionFindings resolves every package-qualified reference through the
// file's imports and reports each attribution entry point named outside its
// one site, and the TerminalVerdict type named outside its authorized files.
// A reference counts whether it is called or not, so a function value is
// caught as well as a call.
func attributionFindings(rel string, file *ast.File, fset *token.FileSet, authorized bool) []string {
	imports := importNames(file)
	packageOf := make(map[string]string, len(imports))
	for importPath, name := range imports {
		packageOf[name] = importPath
	}
	dir := path.Dir(rel)
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
	if dir != failurecauseDir && imports[failurecauseImportPath] == "." {
		report(file, "dot-imports failurecause, which hides who mints a cause or a provenance")
	}
	allowedAt := func(site attributionSite, function string) bool {
		return rel == site.file && function == site.function
	}
	inspect := func(function string, root ast.Node, skip *ast.Ident) {
		selected := make(map[*ast.Ident]bool)
		ast.Inspect(root, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				selected[selector.Sel] = true
			}
			return true
		})
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				qualifier := ""
				if ident, ok := typed.X.(*ast.Ident); ok {
					qualifier = packageOf[ident.Name]
				}
				name := typed.Sel.Name
				switch {
				case qualifier == failurecauseImportPath && name == "ClassifyDeath" && !allowedAt(classifyDeathSite, function):
					report(typed, "names failurecause.ClassifyDeath outside the death entry action")
				case qualifier == failurecauseImportPath && name == "NewEventSession" && !allowedAt(eventReaderSite, function):
					report(typed, "names failurecause.NewEventSession outside the event loop's reader")
				case qualifier == leasesmImportPath && name == "NewLiveContainerDiedObservation" &&
					!allowedAt(liveDeathSite, function):
					report(typed, "names leasesm.NewLiveContainerDiedObservation outside the event loop's dispatcher")
				case qualifier == backendImportPath && name == "TerminalVerdict" && !authorized:
					report(typed, "names the TerminalVerdict type outside its sealed sites")
				case qualifier == "" && slices.Contains(sessionMethods, name) && dir != failurecauseDir &&
					!allowedAt(eventReaderSite, function):
					report(typed, "names %s, an event session method, outside the event loop's reader", name)
				}
			case *ast.Ident:
				if typed == skip || selected[typed] {
					return true
				}
				switch {
				case dir == leasesmDir && typed.Name == "NewLiveContainerDiedObservation":
					report(typed, "names leasesm.NewLiveContainerDiedObservation inside leasesm; only the event loop's dispatcher may")
				case dir == backendDir && typed.Name == "TerminalVerdict" && !authorized:
					report(typed, "names the TerminalVerdict type outside its sealed sites")
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok {
			inspect(functionKey(function), function, function.Name)
			continue
		}
		inspect("", decl, nil)
	}
	return findings
}

func compositeFindings(rel string, root ast.Node, fset *token.FileSet) []string {
	if rel == terminalBudgetMintFile {
		return nil
	}
	var findings []string
	ast.Inspect(root, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || len(literal.Elts) == 0 {
			return true
		}
		if name, _ := calleeName(literal.Type); name == "TerminalBudgetObservation" {
			findings = append(findings, fmt.Sprintf("%s (%s): builds a TerminalBudgetObservation", fset.Position(literal.Pos()), rel))
		}
		return true
	})
	return findings
}

func failCountFindings(rel string, root ast.Node, fset *token.FileSet, exempt map[ast.Node]bool) []string {
	var findings []string
	ast.Inspect(root, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "FailCount" || exempt[selector] {
			return true
		}
		findings = append(findings, fmt.Sprintf("%s (%s): reads FailCount outside a log attribute", fset.Position(selector.Pos()), rel))
		return true
	})
	return findings
}

// calleeName returns the final name of an identifier or selector expression
// and, for a selector on a package identifier, that qualifier.
func calleeName(expr ast.Expr) (name, qualifier string) {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name, ""
	case *ast.SelectorExpr:
		if ident, ok := typed.X.(*ast.Ident); ok {
			return typed.Sel.Name, ident.Name
		}
		return typed.Sel.Name, ""
	case *ast.IndexExpr:
		return calleeName(typed.X)
	case *ast.ParenExpr:
		return calleeName(typed.X)
	default:
		return "", ""
	}
}
