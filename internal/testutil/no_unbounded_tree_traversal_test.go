package testutil

// This file is a repo-wide guard, not a fixture. It fails if production code
// uses a recursive remover or walker that does not bound its resources
// (ENG-1117).
//
// A tenant controls the shape of the trees fred removes and audits, and a
// tree can be arbitrarily deep or wide. os.RemoveAll and (*os.Root).RemoveAll
// keep one open descriptor per level, and filepath.Walk, filepath.WalkDir,
// fs.WalkDir and os.CopyFS recurse with no bound on depth or work, so a deep
// enough tree exhausts the process's descriptors or stack. internal/fstree's
// RemoveBeneath and WalkBeneath bound descriptors, depth and work; every
// recursive removal or traversal goes through them.
//
// forbidigo carries the same rules (.golangci.yml) but matches source text,
// so an aliased import (fp.Walk) escapes it. This guard resolves each file's
// imports, flags every RemoveAll selector whatever its receiver (a method
// value included), and runs in `go test -short ./...` with the rest of this
// package.
//
// TestUnboundedTreeTraversalRules pins the matching rules, with positive
// controls for each shape. Extend it alongside any change to the rules.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// unboundedTreeFuncs are the recursive standard-library functions, by import
// path, that production code must not use.
var unboundedTreeFuncs = map[string][]string{
	"path/filepath": {"Walk", "WalkDir"},
	"io/fs":         {"WalkDir"},
	"os":            {"CopyFS"},
}

// unboundedRemoveMethod is flagged on any receiver: os.RemoveAll,
// (*os.Root).RemoveAll, and any method value taken from either.
const unboundedRemoveMethod = "RemoveAll"

func TestNoUnboundedTreeTraversalInProductionFiles(t *testing.T) {
	root := repoRoot(t)
	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			if inTestSupportPackage(rel) {
				return
			}
			for _, finding := range unboundedTreeTraversals(file) {
				findings = append(findings, rel+":"+strconv.Itoa(fset.Position(finding.pos).Line)+": "+finding.what)
			}
		})
	}
	for _, finding := range findings {
		t.Errorf("%s: use internal/fstree (RemoveBeneath or WalkBeneath), which bounds descriptors, depth and work", finding)
	}
}

type treeTraversalFinding struct {
	pos  token.Pos
	what string
}

// unboundedTreeTraversals reports every use, call or not, of a recursive
// remover or walker in file. Package names are resolved from the file's own
// imports, so an alias is caught; a dot import of a package that exports one
// is itself a finding, since its uses can no longer be told apart.
func unboundedTreeTraversals(file *ast.File) []treeTraversalFinding {
	var findings []treeTraversalFinding
	forbidden := make(map[string][]string) // local package name -> funcs
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		funcs, ok := unboundedTreeFuncs[path]
		if !ok {
			continue
		}
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch name {
		case "_":
			continue
		case ".":
			findings = append(findings, treeTraversalFinding{
				pos: spec.Pos(), what: "dot import of " + path + ", which exports a recursive walker",
			})
			continue
		}
		forbidden[name] = funcs
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name == unboundedRemoveMethod {
			findings = append(findings, treeTraversalFinding{pos: selector.Pos(), what: "RemoveAll"})
			return true
		}
		if pkg, ok := selector.X.(*ast.Ident); ok && slices.Contains(forbidden[pkg.Name], selector.Sel.Name) {
			findings = append(findings, treeTraversalFinding{
				pos: selector.Pos(), what: pkg.Name + "." + selector.Sel.Name,
			})
		}
		return true
	})
	return findings
}

func TestUnboundedTreeTraversalRules(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "os.RemoveAll call",
			src:  `package p; import "os"; func f() { _ = os.RemoveAll("x") }`,
			want: []string{"RemoveAll"},
		},
		{
			name: "os.Root RemoveAll method",
			src:  `package p; import "os"; func f(r *os.Root) { _ = r.RemoveAll("x") }`,
			want: []string{"RemoveAll"},
		},
		{
			name: "RemoveAll method value",
			src:  `package p; import "os"; var remove = os.RemoveAll`,
			want: []string{"RemoveAll"},
		},
		{
			name: "filepath.Walk and WalkDir",
			src:  `package p; import "path/filepath"; func f() { _ = filepath.Walk("x", nil); _ = filepath.WalkDir("x", nil) }`,
			want: []string{"filepath.Walk", "filepath.WalkDir"},
		},
		{
			name: "aliased filepath",
			src:  `package p; import fp "path/filepath"; func f() { _ = fp.WalkDir("x", nil) }`,
			want: []string{"fp.WalkDir"},
		},
		{
			name: "fs.WalkDir, aliased or not",
			src:  `package p; import ("io/fs"; iofs "io/fs"); func f(s fs.FS) { _ = fs.WalkDir(s, ".", nil); _ = iofs.WalkDir(s, ".", nil) }`,
			want: []string{"fs.WalkDir", "iofs.WalkDir"},
		},
		{
			name: "os.CopyFS",
			src:  `package p; import ("io/fs"; "os"); func f(s fs.FS) { _ = os.CopyFS("x", s) }`,
			want: []string{"os.CopyFS"},
		},
		{
			name: "dot import",
			src:  `package p; import . "path/filepath"; func f() { _ = Join("a") }`,
			want: []string{"dot import of path/filepath, which exports a recursive walker"},
		},
		{
			name: "bounded and unrelated functions are not flagged",
			src: `package p
import ("io/fs"; "os"; "path/filepath")
// os.RemoveAll and filepath.Walk in a comment are not uses.
func f(s fs.FS) {
	_ = os.Remove("x")
	_ = filepath.Join("a", "b")
	_, _ = fs.ReadDir(s, ".")
	_, _ = os.ReadDir("x")
}`,
			want: nil,
		},
		{
			name: "same names from other packages are not flagged",
			src:  `package p; import "example.com/walk/filepath"; func f() { filepath.Walk() }`,
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "p.go", test.src, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			var got []string
			for _, finding := range unboundedTreeTraversals(file) {
				got = append(got, finding.what)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("findings = %q, want %q", got, test.want)
			}
		})
	}
}
