package testutil

// This repository-level guard pins who may mint the facts that turn a startup
// crash into a definite provision failure (ENG-1125). The definite path ends
// a durable attempt, and its attribution can count toward closing a paying
// lease, so every link of the chain has exactly one producer:
//
//   - the settled-launch receipt (newSettledLaunch, and any non-empty
//     settledLaunch/settledLaunchState literal) only in the launch dispatch,
//     after the launch's exchange and journal row settled;
//   - the startup finding (newStartupFailure, and any non-empty
//     startupFailure/startupFailureState literal) only in observeStartup, from
//     a failed whole-cohort watch;
//   - the sealed shared account (shared.NewOperationStartupFailure) only in
//     newStartupFailure, and the classifier evidence
//     (shared.NewOperationStartupFailed) only in confirmStartupFailure, after
//     the classifier's own positive reads;
//   - the live-death ledger: written only by the event loop, deaths
//     (recordLiveDeath) by its recorder and the stream mark
//     (markLiveDeathStream) by the loop around its reader, never by the reader
//     itself, which may wait on nothing but its stream (ENG-799); read only
//     by newStartupFailure (awaitLiveDeath) and the Ready-entry re-dispatch
//     (takeLiveDeaths); and its fields named only in its own file.
//
// The zero values of these types are invalid by construction (a nil state),
// so only the constructors and literals above need confining. Every rule is
// proven to fire by TestStartupFailureAuthorityGuardsFire.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	sharedImportPath           = "github.com/manifest-network/fred/internal/backend/shared"
	sharedDir                  = "internal/backend/shared"
	dockerDir                  = "internal/backend/docker"
	startupFailureFile         = "internal/backend/docker/startup_failure.go"
	startupObservationFile     = "internal/backend/docker/startup_observation.go"
	settledLaunchFile          = "internal/backend/docker/settled_launch.go"
	launchDispatchFile         = "internal/backend/docker/storage_mutation_guard.go"
	liveDeathLedgerFile        = "internal/backend/docker/live_death_ledger.go"
	sharedStartupFailureFile   = "internal/backend/shared/operation_startup_failure.go"
	startupFailureConfirmSite  = "Backend.confirmStartupFailure"
	startupFailureMintSite     = "Backend.newStartupFailure"
	startupObservationSite     = "Backend.observeStartup"
	settledLaunchMintSite      = "newSettledLaunch"
	launchDispatchSite         = "newVolumeLaunchCoordinator"
	startupDeathRedispatchSite = "Backend.redispatchStartupDeaths"
	liveDeathRecorderSite      = "Backend.recordLiveContainerDeaths"
	liveDeathStreamMarkSite    = "Backend.runContainerEventLoop"
)

// startupAuthorityRule confines one name to its sites. A name declared by a
// function is also allowed at its own declaration.
type startupAuthorityRule struct {
	name  string
	sites []attributionSite
	what  string
}

// Unqualified names inside internal/backend/docker.
var dockerStartupRules = []startupAuthorityRule{
	{"newSettledLaunch", []attributionSite{{launchDispatchFile, launchDispatchSite}}, "the settled-launch receipt"},
	{"newStartupFailure", []attributionSite{{startupObservationFile, startupObservationSite}}, "the startup finding"},
	{"recordLiveDeath", []attributionSite{{terminalBudgetEventsFile, liveDeathRecorderSite}}, "a live-death ledger write"},
	{"markLiveDeathStream", []attributionSite{{terminalBudgetEventsFile, liveDeathStreamMarkSite}}, "a live-death ledger write"},
	{"awaitLiveDeath", []attributionSite{{startupFailureFile, startupFailureMintSite}}, "a live-death ledger read"},
	{"takeLiveDeaths", []attributionSite{{terminalBudgetEventsFile, startupDeathRedispatchSite}}, "a live-death ledger read"},
}

// Package-qualified names of package shared, and their one docker site.
var sharedStartupRules = []startupAuthorityRule{
	{"NewOperationStartupFailure", []attributionSite{{startupFailureFile, startupFailureMintSite}}, "the sealed startup failure"},
	{"NewOperationStartupFailed", []attributionSite{{startupFailureFile, startupFailureConfirmSite}}, "startup failure evidence"},
}

// Non-empty literals of these docker types are minted only by their
// constructors.
var startupLiteralSites = map[string]attributionSite{
	"settledLaunch":       {settledLaunchFile, settledLaunchMintSite},
	"settledLaunchState":  {settledLaunchFile, settledLaunchMintSite},
	"startupFailure":      {startupFailureFile, startupFailureMintSite},
	"startupFailureState": {startupFailureFile, startupFailureMintSite},
}

// liveDeathLedgerFields are named only in the ledger's own file.
var liveDeathLedgerFields = []string{"deathsByID", "deathOrder", "deathRecorded", "streamConnected"}

func TestStartupFailureAuthorityIsSealed(t *testing.T) {
	root := repoRoot(t)
	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			findings = append(findings, startupFailureAuthorityFindings(rel, file, fset)...)
		})
	}
	if len(findings) > 0 {
		t.Errorf("startup failure authority escaped its sealed sites (ENG-1125):\n  %s",
			strings.Join(findings, "\n  "))
	}
}

func TestStartupFailureAuthorityGuardsFire(t *testing.T) {
	const sharedImport = `import "github.com/manifest-network/fred/internal/backend/shared"
`
	tests := []struct {
		name string
		rel  string
		src  string
		want string
	}{
		{"receipt minted outside the launch dispatch", "internal/backend/docker/provision.go",
			`package docker
func (b *Backend) doProvisionPhysical() { _ = newSettledLaunch(nil) }`, "names newSettledLaunch"},
		{"receipt minted elsewhere in the dispatch file", launchDispatchFile,
			`package docker
func (m *storageMutations) launch() { _ = newSettledLaunch(nil) }`, "names newSettledLaunch"},
		{"receipt constructor as a value", "internal/backend/docker/volume_launch.go",
			`package docker
var mint = newSettledLaunch`, "names newSettledLaunch"},
		{"receipt forged by a literal", "internal/backend/docker/provision.go",
			`package docker
var l = settledLaunch{state: nil}`, "builds a settledLaunch"},
		{"receipt state built outside its constructor", launchDispatchFile,
			`package docker
func newVolumeLaunchCoordinator() { _ = &settledLaunchState{} }`, "builds a settledLaunchState"},
		{"finding minted outside the observation", "internal/backend/docker/provision.go",
			`package docker
func (b *Backend) doProvisionPhysical() { _, _ = b.newStartupFailure(nil, nil, settledLaunch{}, nil, startupWatch{}) }`,
			"names newStartupFailure"},
		{"finding forged by a literal", startupObservationFile,
			`package docker
func (b *Backend) observeStartup() { _ = startupFailure{state: nil} }`, "builds a startupFailure"},
		{"finding state built outside its constructor", startupFailureFile,
			`package docker
func (b *Backend) rollbackStartupFailure() { _ = &startupFailureState{} }`, "builds a startupFailureState"},
		{"sealed account minted outside the finding constructor", "internal/backend/docker/recover.go",
			"package docker\n" + sharedImport + `var f, _ = shared.NewOperationStartupFailure(shared.OperationStartupFailureTerms{})`,
			"names shared.NewOperationStartupFailure"},
		{"evidence minted outside the classifier's confirmation", "internal/backend/docker/physical_execution.go",
			"package docker\n" + sharedImport +
				`func (b *Backend) classifyOperationPhysical() { _, _ = shared.NewOperationStartupFailed(s, f) }`,
			"names shared.NewOperationStartupFailed"},
		{"evidence minted through an alias", startupFailureFile,
			`package docker
import sh "github.com/manifest-network/fred/internal/backend/shared"
func (b *Backend) rollbackStartupFailure() { _, _ = sh.NewOperationStartupFailed(s, f) }`,
			"names shared.NewOperationStartupFailed"},
		{"evidence minted inside package shared", "internal/backend/shared/operation_handoff.go",
			`package shared
func f() { _, _ = NewOperationStartupFailed(s, x) }`, "names shared.NewOperationStartupFailed"},
		{"ledger written by the reader", terminalBudgetReaderFile,
			`package docker
func (r containerEventReader) consume() { r.ledger.recordLiveDeath(p) }`, "names recordLiveDeath"},
		{"ledger stream marked by the reader", terminalBudgetReaderFile,
			`package docker
func (r containerEventReader) consume() { r.ledger.markLiveDeathStream(true) }`, "names markLiveDeathStream"},
		{"ledger written outside the recorder", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.liveDeaths.recordLiveDeath(p) }`, "names recordLiveDeath"},
		{"ledger stream marked by the dispatcher", terminalBudgetEventsFile,
			`package docker
func (b *Backend) dispatchLiveContainerDeath() { b.liveDeaths.markLiveDeathStream(true) }`,
			"names markLiveDeathStream"},
		{"ledger read by the sweep", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { _ = b.liveDeaths.takeLiveDeaths(nil) }`, "names takeLiveDeaths"},
		{"ledger awaited outside the finding constructor", startupObservationFile,
			`package docker
func (b *Backend) observeStartup() { _, _ = b.liveDeaths.awaitLiveDeath(ctx, "c", 0) }`, "names awaitLiveDeath"},
		{"ledger fields read outside the ledger", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { _ = b.liveDeaths.deathsByID }`, "names deathsByID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", test.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse synthetic source: %v", err)
			}
			findings := startupFailureAuthorityFindings(test.rel, file, fset)
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
		{launchDispatchFile, `package docker
func newVolumeLaunchCoordinator() { _ = func() settledLaunch { return newSettledLaunch(q) } }`},
		{settledLaunchFile, `package docker
type settledLaunch struct{ state *settledLaunchState }
func newSettledLaunch(q *quiescedVolumes) settledLaunch { return settledLaunch{state: &settledLaunchState{}} }`},
		{"internal/backend/docker/provision.go", `package docker
func (b *Backend) doProvisionPhysical() (startupFailure, error) { return startupFailure{}, nil }`},
		{startupObservationFile, `package docker
func (b *Backend) observeStartup() { _, _ = b.newStartupFailure(ctx, m, l, c, w) }`},
		{startupFailureFile, "package docker\n" + sharedImport + `func (b *Backend) newStartupFailure() {
	_, _ = shared.NewOperationStartupFailure(shared.OperationStartupFailureTerms{})
	_, _ = b.liveDeaths.awaitLiveDeath(ctx, "c", 0)
	_ = startupFailure{state: &startupFailureState{}}
}`},
		{startupFailureFile, "package docker\n" + sharedImport +
			`func (b *Backend) confirmStartupFailure() { _, _ = shared.NewOperationStartupFailed(s, f) }`},
		{terminalBudgetEventsFile, `package docker
func (b *Backend) runContainerEventLoop() { b.liveDeaths.markLiveDeathStream(true) }
func (b *Backend) recordLiveContainerDeaths() { b.liveDeaths.recordLiveDeath(p) }
func (b *Backend) redispatchStartupDeaths() { _ = b.liveDeaths.takeLiveDeaths(nil) }`},
		{liveDeathLedgerFile, `package docker
type liveDeathLedger struct{ deathsByID map[string]int }
func (l *liveDeathLedger) recordLiveDeath(p int) { l.deathsByID["c"] = p }
func (l *liveDeathLedger) takeLiveDeaths(ids []string) []int { return nil }`},
		{sharedStartupFailureFile, `package shared
func NewOperationStartupFailed(s, f int) (int, error) { return 0, nil }
func NewOperationStartupFailure(t int) (int, error) { return 0, nil }`},
	}
	for _, control := range allowed {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "synthetic.go", control.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse synthetic source: %v", err)
		}
		if findings := startupFailureAuthorityFindings(control.rel, file, fset); len(findings) != 0 {
			t.Errorf("sanctioned shape in %s reported: %q", control.rel, findings)
		}
	}
}

func startupFailureAuthorityFindings(rel string, file *ast.File, fset *token.FileSet) []string {
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
	allowedAtAny := func(sites []attributionSite, function string) bool {
		return slices.ContainsFunc(sites, func(site attributionSite) bool {
			return rel == site.file && function == site.function
		})
	}
	inspect := func(function string, root ast.Node, declared *ast.Ident) {
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				qualifier := ""
				if ident, ok := typed.X.(*ast.Ident); ok {
					qualifier = packageOf[ident.Name]
				}
				name := typed.Sel.Name
				if qualifier == sharedImportPath {
					for _, rule := range sharedStartupRules {
						if name == rule.name && !allowedAtAny(rule.sites, function) {
							report(typed, "names shared.%s, %s, outside its one site", name, rule.what)
						}
					}
					return true
				}
				if dir == dockerDir {
					for _, rule := range dockerStartupRules {
						if name == rule.name && !allowedAtAny(rule.sites, function) {
							report(typed, "names %s, %s, outside its one site", name, rule.what)
						}
					}
					if slices.Contains(liveDeathLedgerFields, name) && rel != liveDeathLedgerFile {
						report(typed, "names %s, a live-death ledger field, outside the ledger", name)
					}
				}
				return true
			case *ast.Ident:
				if typed == declared {
					return true
				}
				switch dir {
				case sharedDir:
					for _, rule := range sharedStartupRules {
						if typed.Name == rule.name {
							report(typed, "names shared.%s, %s, inside package shared", typed.Name, rule.what)
						}
					}
				case dockerDir:
					for _, rule := range dockerStartupRules {
						if typed.Name == rule.name && !allowedAtAny(rule.sites, function) && !isSelectorName(root, typed) {
							report(typed, "names %s, %s, outside its one site", typed.Name, rule.what)
						}
					}
				}
			case *ast.CompositeLit:
				if dir != dockerDir || len(typed.Elts) == 0 {
					return true
				}
				name, _ := calleeName(typed.Type)
				if site, sealed := startupLiteralSites[name]; sealed && !allowedAtAny([]attributionSite{site}, function) {
					report(typed, "builds a %s outside its constructor", name)
				}
			case *ast.UnaryExpr:
				// &settledLaunchState{} and friends are composite literals too,
				// including empty ones: a state pointer is never the zero value.
				literal, ok := typed.X.(*ast.CompositeLit)
				if !ok || dir != dockerDir || len(literal.Elts) != 0 {
					return true
				}
				name, _ := calleeName(literal.Type)
				if site, sealed := startupLiteralSites[name]; sealed && strings.HasSuffix(name, "State") &&
					!allowedAtAny([]attributionSite{site}, function) {
					report(typed, "builds a %s outside its constructor", name)
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

// isSelectorName reports whether ident is the selected name of a selector in
// root, which the selector case already judged.
func isSelectorName(root ast.Node, ident *ast.Ident) bool {
	selected := false
	ast.Inspect(root, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel == ident {
			selected = true
		}
		return !selected
	})
	return selected
}
