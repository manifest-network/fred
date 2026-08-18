package testutil

// This file is a repo-wide guard, not a fixture. It walks fred's
// production Go sources and fails if a declaration whose only purpose is
// to serve tests is compiled into a production package (ENG-354).
//
// Why a test and not a linter: golangci-lint's forbidigo — the obvious
// candidate — matches *usages*, never *declarations*, and every usage of
// a test hook is by definition inside a _test.go file, which .golangci.yml
// excludes from forbidigo. The invariant that matters here ("no test-only
// declaration in a non-test file") is out of reach for it. A test runs in
// `go test -short ./...`, which is exactly what CI executes.
//
// The guard is a tripwire, not an oracle. It catches the two shapes the
// class has actually taken in this repo — a ForTest/TestOnly name, and a
// doc comment that says the declaration is for tests — and it is blind to
// a test-only helper that is named and documented like production code.
// Adding to this file is cheaper than the audit that found the originals.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// testOnlyNamePattern matches identifiers that advertise themselves as
// test scaffolding. Such a declaration belongs in a _test.go file.
var testOnlyNamePattern = regexp.MustCompile(`ForTest|ForTesting|TestOnly|TestHook`)

// testOnlyDocPhrases are doc-comment phrases that state outright that a
// declaration exists to serve tests. Matched case-insensitively against
// the declaration's doc comment.
var testOnlyDocPhrases = []string{
	"only be used in tests",
	"only used in tests",
	"only be used by tests",
	"for testing purposes",
	"is a testing helper",
	"test-only",
	"testing helper that",
	"(for testing)",
	"only for cross-package tests",
	"exposed for testing",
}

// testSupportPackages are directories whose entire purpose is to support
// tests. They are imported only from _test.go files and appear in no
// binary's dependency graph (asserted by TestNoTestSupportPackageInBinary
// -- see the assertion at the bottom of this file), so a test-only
// declaration there is by design, not a leak.
var testSupportPackages = []string{
	"internal/testutil",
	"internal/chain/chaintest",
}

// exemptDecls are individual production declarations the scan skips,
// keyed "<repo-relative file>:<declaration name>", with the reason. Every
// entry is a hole in the guard, so keep the list short and justified.
//
// Scoped to named declarations rather than whole files on purpose: a new
// test-shaped accessor added next to an exempt one is still caught.
var exemptDecls = map[string]string{
	// MockBackend is the implementation behind the cmd/mock-backend binary
	// (Makefile builds and installs it), and these three accessors
	// are driven from tests in OTHER packages -- internal/provisioner's
	// fleet harness calls SetProvisionStatus and Clear, cmd/mock-backend's
	// own tests call SetGetLoadStatsErr. Go test-package visibility does not
	// cross package boundaries, so none of them can live in an
	// internal/backend/*_test.go file.
	"internal/backend/mock.go:SetProvisionStatus": "driven by internal/provisioner's fleet harness",
	"internal/backend/mock.go:Clear":              "driven by internal/provisioner's fleet harness",
	"internal/backend/mock.go:SetGetLoadStatsErr": "driven by cmd/mock-backend's tests",
}

func TestNoTestOnlyCodeInProductionFiles(t *testing.T) {
	root := repoRoot(t)

	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			if inTestSupportPackage(rel) {
				return
			}
			for _, decl := range file.Decls {
				for _, d := range declsOf(decl) {
					if _, exempt := exemptDecls[rel+":"+d.name]; exempt {
						continue
					}
					if reason := testOnlyReason(d); reason != "" {
						findings = append(findings, fset.Position(d.pos).String()+": "+d.name+" -- "+reason)
					}
				}
			}
		})
	}

	if len(findings) > 0 {
		t.Errorf("test-only declarations found in production (non-_test.go) files.\n"+
			"Move each into an in-package export_test.go, or -- if a different package's tests\n"+
			"need it -- restructure those tests to drive the production seam instead (ENG-354).\n  %s",
			strings.Join(findings, "\n  "))
	}
}

// TestNoTestSupportPackageInBinary backs the testSupportPackages
// allowance above: those packages are exempt from the scan only because
// nothing production-facing links them.
func TestNoTestSupportPackageInBinary(t *testing.T) {
	root := repoRoot(t)
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, _ *token.FileSet) {
			if inTestSupportPackage(rel) {
				return
			}
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				for _, pkg := range testSupportPackages {
					if strings.HasSuffix(path, "/"+pkg) {
						t.Errorf("%s imports test-support package %s from a production file", rel, path)
					}
				}
			}
		})
	}
}

type declInfo struct {
	name string
	doc  string
	pos  token.Pos
}

// declsOf flattens a top-level declaration into the named entities it
// introduces, carrying the nearest doc comment for each.
func declsOf(decl ast.Decl) []declInfo {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return []declInfo{{name: d.Name.Name, doc: d.Doc.Text(), pos: d.Name.Pos()}}
	case *ast.GenDecl:
		var out []declInfo
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				doc := s.Doc.Text()
				if doc == "" {
					doc = d.Doc.Text()
				}
				out = append(out, declInfo{name: s.Name.Name, doc: doc, pos: s.Name.Pos()})
			case *ast.ValueSpec:
				doc := s.Doc.Text()
				if doc == "" {
					doc = d.Doc.Text()
				}
				for _, n := range s.Names {
					out = append(out, declInfo{name: n.Name, doc: doc, pos: n.Pos()})
				}
			}
		}
		return out
	}
	return nil
}

func testOnlyReason(d declInfo) string {
	if testOnlyNamePattern.MatchString(d.name) {
		return "name declares it as test scaffolding"
	}
	doc := strings.ToLower(d.doc)
	for _, phrase := range testOnlyDocPhrases {
		if strings.Contains(doc, phrase) {
			return "doc comment says it exists for tests: " + phrase
		}
	}
	return ""
}

func inTestSupportPackage(rel string) bool {
	dir := filepath.ToSlash(filepath.Dir(rel))
	for _, pkg := range testSupportPackages {
		if dir == pkg {
			return true
		}
	}
	return false
}

func walkGoFiles(t *testing.T, dir, root string, fn func(rel string, file *ast.File, fset *token.FileSet)) {
	t.Helper()
	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			if e.Name() == "testdata" || e.Name() == "vendor" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, perr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		fn(filepath.ToSlash(rel), parsed, fset)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
}

// repoRoot walks up from the package directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}
