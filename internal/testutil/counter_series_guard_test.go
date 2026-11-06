package testutil

// This repository-level guard keeps every labeled counter series in existence
// before its first event. A counter child created by its first increment
// appears already at 1, and increase() and rate() see no change, so an alert
// on the first panic, refusal or failed sweep after a restart never fires
// (ENG-1116). Prometheus's guidance is to export every series that may exist
// at 0 from startup and to keep references to the children of labeled
// metrics; these rules make that the only way to write one:
//
//   - providerd's counter vecs (internal/metrics and its background
//     subpackage) are unexported, so no other package can create a child;
//   - a counter vec's child is created only at package level (a variable
//     initializer or init), or in a file that declares a vocabulary
//     (counterVocabularyFiles), never inside a function that runs later;
//   - a background component's panic counter (background.CleanupPanics or
//     GoroutinePanics) is declared by a package-level variable of the package
//     that runs the component, so each binary exports exactly its own;
//   - a binder's result (metrics.Bind*) is kept, never used inline, since an
//     inline bind creates its series at the first event.
//
// Counter vecs are recognized by name: package variables built by
// NewCounterVec and fields or parameters of type *prometheus.CounterVec. The
// guard cannot see a vec passed under another name; the providerd vecs are
// unexported, which closes that path for them.
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
	metricsImportPath    = "github.com/manifest-network/fred/internal/metrics"
	backgroundImportPath = "github.com/manifest-network/fred/internal/metrics/background"
)

var (
	// counterVecPackages must keep their counter vecs unexported.
	counterVecPackages = []string{"internal/metrics", "internal/metrics/background"}
	// counterVocabularyFiles declare vocabularies; they may create a child
	// series anywhere, and are reviewed as the vocabulary they declare.
	counterVocabularyFiles = []string{
		"internal/metrics/series.go",
		"internal/metrics/background/background.go",
	}
	// counterSeriesPendingDirs still create some of their own package's
	// counter children at increment sites. The docker-backend follow-up to
	// ENG-1517's zero-initialization item binds them and removes this list.
	counterSeriesPendingDirs = []string{
		"internal/backend/docker",
		"internal/backend/docker/imagefetch",
		"internal/backend/k3s",
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
	if _, ok := files[counterVocabularyFiles[0]]; !ok {
		t.Fatalf("%s is gone; point counterVocabularyFiles at the vocabulary", counterVocabularyFiles[0])
	}
	for _, finding := range counterSeriesFindings(fset, files) {
		t.Error(finding)
	}
}

// counterSeriesFindings applies every rule to production files keyed by
// repository-relative path.
func counterSeriesFindings(fset *token.FileSet, files map[string]*ast.File) []string {
	vecs := counterVecNames(files)
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		args = append([]any{fset.Position(node.Pos())}, args...)
		findings = append(findings, fmt.Sprintf("%s: "+format, args...))
	}
	for _, rel := range slices.Sorted(maps.Keys(files)) {
		file := files[rel]
		dir := path.Dir(rel)
		if slices.Contains(counterVecPackages, dir) {
			for _, name := range packageCounterVecs(file) {
				if ast.IsExported(name.Name) {
					report(name, "counter vec %s is exported; export bound handles instead", name.Name)
				}
			}
		}
		metricsNames := importNamesOf(file, metricsImportPath)
		backgroundNames := importNamesOf(file, backgroundImportPath)
		vocabulary := slices.Contains(counterVocabularyFiles, rel)
		pending := slices.Contains(counterSeriesPendingDirs, dir)
		walkCounterScopes(file, func(node ast.Node, scope counterScope) {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if inner, ok := selector.X.(*ast.CallExpr); ok {
						if qualifier, name := qualifiedCall(inner); slices.Contains(metricsNames, qualifier) &&
							strings.HasPrefix(name, "Bind") {
							report(inner, "%s.%s's result is used inline; keep it so its series exist before the first event",
								qualifier, name)
						}
					}
				}
				return
			}
			qualifier, name := qualifiedCall(call)
			if slices.Contains(backgroundNames, qualifier) && slices.Contains(backgroundComponentConstructors, name) &&
				scope != scopePackageInitializer {
				report(call, "background.%s outside a package-level variable declaration", name)
			}
			if vocabulary || pending || scope != scopeLater {
				return
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !slices.Contains(counterChildMethods, selector.Sel.Name) {
				return
			}
			if receiver := finalName(selector.X); receiver != "" && vecs[receiver] {
				report(call, "%s.%s creates a counter series when it first runs; bind it at package level, in init, or in a vocabulary file",
					receiver, selector.Sel.Name)
			}
		})
	}
	return findings
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

// walkCounterScopes visits every node of file with the scope it runs in.
func walkCounterScopes(file *ast.File, visit func(ast.Node, counterScope)) {
	var walk func(node ast.Node, scope counterScope)
	walk = func(node ast.Node, scope counterScope) {
		ast.Inspect(node, func(n ast.Node) bool {
			if n == nil || n == node {
				if n != nil {
					visit(n, scope)
				}
				return true
			}
			switch typed := n.(type) {
			case *ast.CallExpr:
				visit(typed, scope)
				if literal, ok := typed.Fun.(*ast.FuncLit); ok && scope != scopeLater {
					walk(literal.Body, scopeInit)
					for _, argument := range typed.Args {
						walk(argument, scope)
					}
					return false
				}
				return true
			case *ast.FuncLit:
				walk(typed.Body, scopeLater)
				return false
			default:
				visit(n, scope)
				return true
			}
		})
	}
	for _, decl := range file.Decls {
		switch typed := decl.(type) {
		case *ast.GenDecl:
			if typed.Tok == token.VAR {
				walk(typed, scopePackageInitializer)
			}
		case *ast.FuncDecl:
			if typed.Body == nil {
				continue
			}
			scope := scopeLater
			if typed.Recv == nil && typed.Name.Name == "init" {
				scope = scopeInit
			}
			walk(typed.Body, scope)
		}
	}
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
`
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for rel, source := range map[string]string{
		"internal/metrics/metrics.go":        metricsFile,
		"internal/writer/writer.go":          writer,
		"internal/backend/docker/pending.go": strings.ReplaceAll(writer, "package writer", "package docker"),
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
		"internal/writer/writer.go:8:22: hiddenTotal.WithLabelValues creates a counter series when it first runs",
		"internal/writer/writer.go:13:2: requests.WithLabelValues creates a counter series when it first runs",
		"internal/writer/writer.go:14:2: hiddenTotal.With creates a counter series when it first runs",
		"internal/writer/writer.go:15:2: background.GoroutinePanics outside a package-level variable declaration",
		"internal/writer/writer.go:16:2: metrics.BindAckBatchLane's result is used inline",
		"internal/writer/writer.go:20:19: background.CleanupPanics outside a package-level variable declaration",
		// A pending directory is exempt from the creation rule only.
		"internal/backend/docker/pending.go:15:2: background.GoroutinePanics outside a package-level variable declaration",
		"internal/backend/docker/pending.go:16:2: metrics.BindAckBatchLane's result is used inline",
	} {
		if !strings.Contains(findings, want) {
			t.Errorf("guard did not report %q; findings:\n%s", want, findings)
		}
	}
	for _, allowed := range []string{
		"metrics.go:4:", "writer.go:7:", "writer.go:9:", "writer.go:10:", "writer.go:11:", "writer.go:18:",
		"pending.go:8:", "pending.go:13:",
	} {
		if strings.Contains(findings, allowed) {
			t.Errorf("guard reported an allowed site %s; findings:\n%s", allowed, findings)
		}
	}
}
