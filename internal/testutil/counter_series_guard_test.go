package testutil

// This repository-level guard keeps every labeled counter series in existence
// before its first event. A counter child created by its first increment
// appears already at 1, and increase() and rate() see no change, so an alert
// on the first panic, refusal or failed sweep after a restart never fires
// (ENG-1116). Prometheus's guidance is to export every series that may exist
// at 0 from startup and to keep references to the children of labeled
// metrics; these rules make that the only way to write one:
//
//   - every counter vec is an unexported package variable, so no other
//     package can create a child;
//   - every package that declares a counter vec has a vocabulary file
//     (counterVocabularyFiles), and a counter vec's child is created only in
//     a vocabulary file. Everything else increments a handle a vocabulary
//     bound;
//   - a binder is a function named bind… or Bind… that a vocabulary file
//     declares. Outside the vocabulary files it runs only at package
//     initialization or in a constructor (a function or method named New… or
//     new…), never in a function literal that runs later. Recording paths
//     only look series up: metrics.ForBackend cannot create one;
//   - a background component's panic counter (background.CleanupPanics or
//     GoroutinePanics) is declared by a package-level variable of the package
//     that runs the component, so each binary exports exactly its own.
//
// A vocabulary file may create a child anywhere, and is reviewed as the
// vocabulary it declares: what its functions bind and when they run.
//
// A constructor is recognized by its name, which approximates "runs while the
// component is built"; a constructor that binds and then records in the same
// call would still pass. The unexported vecs and the bound handles carry the
// guarantee for recording paths; this rule keeps the binders where review
// expects them.
//
// Counter vecs are recognized by name: package variables built by
// NewCounterVec and fields or parameters of type *prometheus.CounterVec. The
// guard cannot see a vec passed under another name; the vecs are unexported,
// which closes that path outside their package.
//
// TestCounterSeriesGuardsFire proves every rule fires.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const (
	modulePath           = "github.com/manifest-network/fred"
	backgroundImportPath = modulePath + "/internal/metrics/background"
)

var (
	// counterVocabularyFiles declare vocabularies: the only files that create a
	// counter child, one in every package that declares a counter vec.
	counterVocabularyFiles = []string{
		"internal/metrics/series.go",
		"internal/metrics/background/background.go",
		"internal/backend/docker/series.go",
		"internal/backend/docker/imagefetch/series.go",
		"internal/backend/k3s/series.go",
	}
	// counterChildMethods create (or look up and create) a child series.
	counterChildMethods = []string{
		"WithLabelValues", "With", "GetMetricWithLabelValues", "GetMetricWith",
		"CurryWith", "MustCurryWith",
	}
	backgroundComponentConstructors = []string{"CleanupPanics", "GoroutinePanics"}
)

func TestCounterSeriesExistBeforeTheirFirstEvent(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			files[filepath.ToSlash(rel)] = file
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	for _, vocabulary := range counterVocabularyFiles {
		if _, ok := files[vocabulary]; !ok {
			t.Errorf("%s is gone; point counterVocabularyFiles at the vocabulary", vocabulary)
		}
	}
	for _, finding := range counterSeriesFindings(fset, files) {
		t.Error(finding)
	}
}

// counterSeriesFindings applies every rule to production files keyed by
// repository-relative path.
func counterSeriesFindings(fset *token.FileSet, files map[string]*ast.File) []string {
	vecs := counterVecNames(files)
	binders := vocabularyBinders(files)
	vocabularyDirs := make(map[string]bool)
	for _, rel := range counterVocabularyFiles {
		vocabularyDirs[path.Dir(rel)] = true
	}
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		args = append([]any{fset.Position(node.Pos())}, args...)
		findings = append(findings, fmt.Sprintf("%s: "+format, args...))
	}
	for _, rel := range slices.Sorted(maps.Keys(files)) {
		file := files[rel]
		dir := path.Dir(rel)
		for _, name := range packageCounterVecs(file) {
			if ast.IsExported(name.Name) {
				report(name, "counter vec %s is exported; export bound handles instead", name.Name)
			}
			if !vocabularyDirs[dir] {
				report(name, "counter vec %s is declared in a package with no vocabulary file; add one to counterVocabularyFiles", name.Name)
			}
		}
		if slices.Contains(counterVocabularyFiles, rel) {
			continue
		}
		backgroundNames := importNamesOf(file, backgroundImportPath)
		walkCounterScopes(file, func(node ast.Node, scope counterScope, function string) {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return
			}
			qualifier, name := qualifiedCall(call)
			if slices.Contains(backgroundNames, qualifier) && slices.Contains(backgroundComponentConstructors, name) &&
				scope != scopePackageInitializer {
				report(call, "background.%s outside a package-level variable declaration", name)
			}
			if binder, ok := binderCalled(file, dir, call, binders); ok && scope == scopeLater && !isConstructorName(function) {
				report(call, "%s outside a constructor or package initialization; bind where the component is built and keep the handles",
					binder)
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !slices.Contains(counterChildMethods, selector.Sel.Name) {
				return
			}
			if receiver := finalName(selector.X); receiver != "" && vecs[receiver] {
				report(call, "%s.%s creates a counter series outside a vocabulary file; bind it there and increment the handle",
					receiver, selector.Sel.Name)
			}
		})
	}
	return findings
}

// vocabularyBinders is, per package directory, the binder functions its
// vocabulary file declares.
func vocabularyBinders(files map[string]*ast.File) map[string][]string {
	binders := make(map[string][]string)
	for _, rel := range counterVocabularyFiles {
		file, ok := files[rel]
		if !ok {
			continue
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if ok && function.Recv == nil && isBinderName(function.Name.Name) {
				binders[path.Dir(rel)] = append(binders[path.Dir(rel)], function.Name.Name)
			}
		}
	}
	return binders
}

// binderCalled names the vocabulary binder call calls, if it calls one: an
// unqualified call to its own package's binder, or a qualified call to an
// imported package's.
func binderCalled(file *ast.File, dir string, call *ast.CallExpr, binders map[string][]string) (string, bool) {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name, slices.Contains(binders[dir], fun.Name)
	case *ast.SelectorExpr:
		qualifier, ok := fun.X.(*ast.Ident)
		if !ok {
			return "", false
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil || !strings.HasPrefix(importPath, modulePath+"/") {
				continue
			}
			name := path.Base(importPath)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			importedDir := strings.TrimPrefix(importPath, modulePath+"/")
			if name == qualifier.Name && slices.Contains(binders[importedDir], fun.Sel.Name) {
				return path.Base(importPath) + "." + fun.Sel.Name, true
			}
		}
	}
	return "", false
}

func isBinderName(function string) bool {
	return strings.HasPrefix(function, "Bind") || strings.HasPrefix(function, "bind")
}

type counterScope uint8

const (
	// scopePackageInitializer runs once, at package initialization, directly
	// in a package-level variable declaration.
	scopePackageInitializer counterScope = iota + 1
	// scopeInit runs at package initialization, inside init or a function
	// literal a package-level declaration calls immediately.
	scopeInit
	// scopeLater runs whenever its function is called.
	scopeLater
)

// walkCounterScopes visits every node of file with the scope it runs in and
// the declared function it runs as part of. A function literal that does not
// run at initialization runs as part of no declared function.
func walkCounterScopes(file *ast.File, visit func(ast.Node, counterScope, string)) {
	var walk func(node ast.Node, scope counterScope, function string)
	walk = func(node ast.Node, scope counterScope, function string) {
		ast.Inspect(node, func(n ast.Node) bool {
			if n == nil {
				return true
			}
			if n == node {
				visit(n, scope, function)
				return true
			}
			switch typed := n.(type) {
			case *ast.CallExpr:
				visit(typed, scope, function)
				if literal, ok := typed.Fun.(*ast.FuncLit); ok && scope != scopeLater {
					walk(literal.Body, scopeInit, function)
					for _, argument := range typed.Args {
						walk(argument, scope, function)
					}
					return false
				}
				return true
			case *ast.FuncLit:
				walk(typed.Body, scopeLater, "")
				return false
			default:
				visit(n, scope, function)
				return true
			}
		})
	}
	for _, decl := range file.Decls {
		switch typed := decl.(type) {
		case *ast.GenDecl:
			if typed.Tok == token.VAR {
				walk(typed, scopePackageInitializer, "")
			}
		case *ast.FuncDecl:
			if typed.Body == nil {
				continue
			}
			scope := scopeLater
			if typed.Recv == nil && typed.Name.Name == "init" {
				scope = scopeInit
			}
			walk(typed.Body, scope, typed.Name.Name)
		}
	}
}

func isConstructorName(function string) bool {
	return strings.HasPrefix(function, "New") || strings.HasPrefix(function, "new")
}

// counterVecNames is every name a counter vec goes by in the repository.
func counterVecNames(files map[string]*ast.File) map[string]bool {
	names := make(map[string]bool)
	for _, file := range files {
		for _, name := range packageCounterVecs(file) {
			names[name.Name] = true
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if field, ok := node.(*ast.Field); ok && isCounterVecType(field.Type) {
				for _, name := range field.Names {
					names[name.Name] = true
				}
			}
			return true
		})
	}
	return names
}

// packageCounterVecs are the package-level variables a file builds with
// NewCounterVec.
func packageCounterVecs(file *ast.File) []*ast.Ident {
	var names []*ast.Ident
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, expr := range value.Values {
				call, ok := expr.(*ast.CallExpr)
				if !ok || i >= len(value.Names) {
					continue
				}
				if _, name := qualifiedCall(call); name == "NewCounterVec" {
					names = append(names, value.Names[i])
				}
			}
		}
	}
	return names
}

func isCounterVecType(expr ast.Expr) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "CounterVec"
}

// qualifiedCall splits pkg.Name(...) into its qualifier and name.
func qualifiedCall(call *ast.CallExpr) (string, string) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", ""
	}
	qualifier, ok := selector.X.(*ast.Ident)
	if !ok {
		return "", selector.Sel.Name
	}
	return qualifier.Name, selector.Sel.Name
}

// finalName is the last identifier of x, y.x or a.b.x.
func finalName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}

// importNamesOf are the names a file refers to an import path by.
func importNamesOf(file *ast.File, importPath string) []string {
	var names []string
	for _, spec := range file.Imports {
		if unquoted, err := strconv.Unquote(spec.Path.Value); err != nil || unquoted != importPath {
			continue
		}
		if spec.Name != nil {
			names = append(names, spec.Name.Name)
			continue
		}
		names = append(names, path.Base(importPath))
	}
	return names
}

func TestCounterSeriesGuardsFire(t *testing.T) {
	const metricsFile = `package metrics
import "github.com/prometheus/client_golang/prometheus/promauto"
var ExportedTotal = promauto.NewCounterVec(opts, labels)
var hiddenTotal = promauto.NewCounterVec(opts, labels)
`
	const vocabulary = `package metrics
var bound = hiddenTotal.WithLabelValues("ok")
func BindAckBatchLane(lane string) int { return hiddenTotal.WithLabelValues(lane) }
func bindLater() { _ = BindAckBatchLane("in the vocabulary") }
func record() { hiddenTotal.WithLabelValues("vocabulary may") }
`
	const writer = `package writer
import (
	"github.com/manifest-network/fred/internal/metrics"
	bg "github.com/manifest-network/fred/internal/metrics/background"
)
type client struct{ requests *prometheus.CounterVec }
var bound = hiddenTotal.WithLabelValues("ok")
var later = func() { hiddenTotal.WithLabelValues("late").Inc() }
var atInit = func() int { hiddenTotal.WithLabelValues("init"); return 0 }()
var component = bg.CleanupPanics("component")
func init() { hiddenTotal.WithLabelValues("init") }
func (c *client) record() {
	c.requests.WithLabelValues("op").Inc()
	hiddenTotal.With(nil).Inc()
	bg.GoroutinePanics("late").Inc()
	metrics.BindAckBatchLane("0").Restarts.Inc()
	lane := metrics.BindAckBatchLane("1")
	lane.Restarts.Inc()
}
func init() { _ = bg.CleanupPanics("in init") }
func newClient() client {
	_ = metrics.BindAckBatchLane("2")
	go func() { _ = metrics.BindAckBatchLane("3") }()
	return client{}
}
var lane = metrics.BindAckBatchLane("at init")
func helper() { _ = metrics.NotABinder("x") }
`
	const sameDirectory = `package metrics
func apply() { _ = BindAckBatchLane("late") }
func NewApplier() { _ = BindAckBatchLane("built") }
`
	const unregistered = `package other
var otherTotal = promauto.NewCounterVec(opts, labels)
`
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for rel, source := range map[string]string{
		"internal/metrics/metrics.go": metricsFile,
		"internal/metrics/series.go":  vocabulary,
		"internal/metrics/apply.go":   sameDirectory,
		"internal/writer/writer.go":   writer,
		"internal/other/other.go":     unregistered,
	} {
		file, err := parser.ParseFile(fset, rel, source, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		files[rel] = file
	}
	findings := strings.Join(counterSeriesFindings(fset, files), "\n")
	for _, want := range []string{
		"internal/metrics/metrics.go:3:5: counter vec ExportedTotal is exported",
		"internal/other/other.go:2:5: counter vec otherTotal is declared in a package with no vocabulary file",
		"internal/writer/writer.go:7:13: hiddenTotal.WithLabelValues creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:8:22: hiddenTotal.WithLabelValues creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:9:27: hiddenTotal.WithLabelValues creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:11:15: hiddenTotal.WithLabelValues creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:13:2: requests.WithLabelValues creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:14:2: hiddenTotal.With creates a counter series outside a vocabulary file",
		"internal/writer/writer.go:15:2: background.GoroutinePanics outside a package-level variable declaration",
		"internal/writer/writer.go:16:2: metrics.BindAckBatchLane outside a constructor or package initialization",
		"internal/writer/writer.go:17:10: metrics.BindAckBatchLane outside a constructor or package initialization",
		"internal/writer/writer.go:20:19: background.CleanupPanics outside a package-level variable declaration",
		"internal/writer/writer.go:23:18: metrics.BindAckBatchLane outside a constructor or package initialization",
		"internal/metrics/apply.go:2:20: BindAckBatchLane outside a constructor or package initialization",
	} {
		if !strings.Contains(findings, want) {
			t.Errorf("guard did not report %q; findings:\n%s", want, findings)
		}
	}
	for _, allowed := range []string{
		"metrics.go:4:", "series.go:", "writer.go:6:", "writer.go:10:", "writer.go:22:", "writer.go:26:",
		"writer.go:27:", "apply.go:3:",
	} {
		if strings.Contains(findings, allowed) {
			t.Errorf("guard reported an allowed site %s; findings:\n%s", allowed, findings)
		}
	}
}
