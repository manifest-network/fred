package testutil

// This repository-level guard pins who may mint, decode and act on the
// consecutive-failure terminal budget (ENG-799), whose exhausted verdict makes
// providerd close a paying lease on-chain, irreversibly. The types already seal
// most of it across package boundaries (failurecause.Cause, terminalverdict.
// Verdict and leasesm.TerminalBudget have unexported fields); these rules cover
// what a type cannot: the wire's exported string constant and type, and the
// two attribution entry points.
//
//   - The wire constant TerminalVerdictExhausted is named only where it is
//     declared, minted and decoded.
//   - The literal "exhausted" appears in production only at that declaration,
//     or as a metric label argument.
//   - A conversion to TerminalVerdict happens only in those same three files.
//   - A non-empty TerminalBudgetObservation literal is built only by the one
//     backend minter, leasesm's ObserveTerminalBudget.
//   - In internal/provisioner, FailCount is read only as a log argument: it
//     never decides a close.
//   - failurecause.ClassifyDeath, the only constructor of a counting cause, is
//     called only from the state machine's death entry action, and
//     failurecause.NewEventSession, the only minter of live provenance, only
//     from the Docker event loop.
//
// Every rule is proven to fire by TestTerminalBudgetAuthorityGuardsFire.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const (
	terminalBudgetWireFile    = "internal/backend/client.go"
	terminalBudgetMintFile    = "internal/backend/shared/leasesm/terminal_budget.go"
	terminalBudgetDecodeFile  = "internal/provisioner/terminalverdict/verdict.go"
	terminalBudgetDeathFile   = "internal/backend/shared/leasesm/lease_sm.go"
	terminalBudgetSessionFile = "internal/backend/docker/container_event_loop.go"
)

var terminalVerdictAuthorityFiles = []string{
	terminalBudgetWireFile, terminalBudgetMintFile, terminalBudgetDecodeFile,
}

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
	tests := []struct {
		name string
		rel  string
		src  string
		want string
	}{
		{
			name: "wire constant outside its sites",
			rel:  "internal/provisioner/reconciler.go",
			src: `package p
import "github.com/manifest-network/fred/internal/backend"
var v = backend.TerminalVerdictExhausted`,
			want: "names TerminalVerdictExhausted",
		},
		{
			name: "exhausted literal compared directly",
			rel:  "internal/provisioner/reconciler.go",
			src: `package p
func f(v string) bool { return v == "exhausted" }`,
			want: `literal "exhausted"`,
		},
		{
			name: "verdict conversion outside its sites",
			rel:  "internal/api/handlers.go",
			src: `package p
import "github.com/manifest-network/fred/internal/backend"
var v = backend.TerminalVerdict("retry")`,
			want: "converts to TerminalVerdict",
		},
		{
			name: "budget observation literal outside the minter",
			rel:  "internal/backend/docker/info.go",
			src: `package p
import "github.com/manifest-network/fred/internal/backend"
var v = &backend.TerminalBudgetObservation{ConsecutiveFailures: 3}`,
			want: "builds a TerminalBudgetObservation",
		},
		{
			name: "fail count deciding in the provisioner",
			rel:  "internal/provisioner/reconcile_plan.go",
			src: `package p
func f(p struct{ FailCount int }) bool { return p.FailCount >= 3 }`,
			want: "reads FailCount",
		},
		{
			name: "fail count stored in provisioner facts",
			rel:  "internal/provisioner/reconciler.go",
			src: `package p
type facts struct{ n int }
func f(p struct{ FailCount int }) facts { return facts{n: p.FailCount} }`,
			want: "reads FailCount",
		},
		{
			name: "death classified outside the state machine",
			rel:  "internal/backend/docker/recover.go",
			src: `package p
import "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
var c = failurecause.ClassifyDeath(failurecause.Provenance{}, failurecause.Exited())`,
			want: "calls failurecause.ClassifyDeath",
		},
		{
			name: "event session minted outside the event loop",
			rel:  "internal/backend/docker/info.go",
			src: `package p
import "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
var s = failurecause.NewEventSession()`,
			want: "calls failurecause.NewEventSession",
		},
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
		{terminalBudgetDecodeFile, `package p
import "github.com/manifest-network/fred/internal/backend"
var v = backend.TerminalVerdictExhausted`},
		{"internal/chain/client.go", `package p
func f(m interface{ WithLabelValues(...string) any }) { m.WithLabelValues("exhausted") }`},
		{"internal/provisioner/reconciler.go", `package p
import "log/slog"
func f(p struct{ FailCount int }) { slog.Warn("x", "fail_count", p.FailCount) }`},
		{"internal/backend/docker/info.go", `package p
import "github.com/manifest-network/fred/internal/backend"
var v = backend.TerminalBudgetObservation{}`},
		{terminalBudgetDeathFile, `package p
import "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
var c = failurecause.ClassifyDeath(failurecause.Provenance{}, failurecause.Exited())`},
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
	findings = append(findings, callFindings(rel, file, fset, authorized)...)
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

func callFindings(rel string, root ast.Node, fset *token.FileSet, authorized bool) []string {
	var findings []string
	ast.Inspect(root, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, qualifier := calleeName(call.Fun)
		switch {
		case name == "TerminalVerdict" && len(call.Args) == 1 && !authorized:
			findings = append(findings, fmt.Sprintf("%s (%s): converts to TerminalVerdict", fset.Position(call.Pos()), rel))
		case qualifier == "failurecause" && name == "ClassifyDeath" && rel != terminalBudgetDeathFile:
			findings = append(findings, fmt.Sprintf("%s (%s): calls failurecause.ClassifyDeath", fset.Position(call.Pos()), rel))
		case qualifier == "failurecause" && name == "NewEventSession" && rel != terminalBudgetSessionFile:
			findings = append(findings, fmt.Sprintf("%s (%s): calls failurecause.NewEventSession", fset.Position(call.Pos()), rel))
		}
		return true
	})
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
	default:
		return "", ""
	}
}
