package fstree

// This file is a guard over fstree's production sources, not a fixture.
// The types in internal/at make fstree's confinement a compile-time property
// only as long as nothing goes around them, so it pins the four ways around:
//
//  1. Raw descriptor access: a call into golang.org/x/sys/unix or syscall,
//     os.NewFile, or a Fd or SyscallConn method call, anywhere but
//     internal/at/sys_linux.go, where every flag is fixed.
//  2. The parent directory: a string literal with a ".." path component
//     anywhere but (*Dir).OpenParent, whose callers must verify what it
//     reached.
//  3. The walk: walk_linux.go names nothing of package at outside its
//     read-only side, calls no method named like a mutation, and reaches
//     nothing of the remover, so fstree's walk path cannot change the tree.
//  4. Names: ParseName is called only in name_linux.go, so a name the
//     traversal read from one directory cannot be turned back into a Name
//     and resolved in another.
//
// It is a tripwire, not an oracle. Its known blind spot: a raw function
// taken as a value (f := unix.Unlinkat) rather than called. Each rule has a
// violating fixture below proving it fires, and each scan asserts it saw the
// one place its rule allows, so a broken scanner cannot pass vacuously.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	unixImport = "golang.org/x/sys/unix"
	atImport   = "github.com/manifest-network/fred/internal/fstree/internal/at"
	// rawAccessFile is the only file allowed raw descriptor access.
	rawAccessFile = "internal/at/sys_linux.go"
	// walkFile holds the whole walk path.
	walkFile = "walk_linux.go"
	// nameFile is the only fstree file that makes a Name.
	nameFile = "name_linux.go"
)

// readOnlyAt is everything walk_linux.go may name from package at.
var readOnlyAt = map[string]bool{
	"View": true, "ListedView": true, "BorrowView": true, "Reader": true, "NewReader": true,
	"Identity": true, "Lender": true, "Borrowed": true, "Name": true,
}

// mutatingMethods are method names walk_linux.go may not call on anything.
var mutatingMethods = map[string]bool{"Unlink": true, "Rmdir": true, "RenameNoReplaceInto": true}

// removerNames are the remover's entry points, which walk_linux.go may not
// reach.
var removerNames = map[string]bool{"RemoveBeneath": true, "removeBeneath": true, "newRemover": true, "remover": true}

// guardFile is one parsed source file and its slash-separated path relative
// to internal/fstree.
type guardFile struct {
	path string
	fset *token.FileSet
	file *ast.File
}

func (f guardFile) where(node ast.Node) string {
	return f.path + ":" + strconv.Itoa(f.fset.Position(node.Pos()).Line)
}

// imports maps each local package name of f to its import path.
func (f guardFile) imports() map[string]string {
	names := map[string]string{}
	for _, spec := range f.file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		names[name] = path
	}
	return names
}

// packageSelector returns the import path and name of a selector X.Sel
// whose X names an import of f.
func (f guardFile) packageSelector(expr ast.Expr, imports map[string]string) (path, name string, ok bool) {
	selector, isSelector := expr.(*ast.SelectorExpr)
	if !isSelector {
		return "", "", false
	}
	pkg, isIdent := selector.X.(*ast.Ident)
	if !isIdent {
		return "", "", false
	}
	path, ok = imports[pkg.Name]
	return path, selector.Sel.Name, ok
}

// productionFiles parses every non-test Go file under internal/fstree.
func productionFiles(t *testing.T) []guardFile {
	t.Helper()
	var files []guardFile
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		files = append(files, guardFile{path: filepath.ToSlash(path), fset: fset, file: file})
		return nil
	})
	require.NoError(t, err)
	return files
}

// fixture parses src as the file at path, for proving a rule fires.
func fixture(t *testing.T, path, src string) guardFile {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	require.NoError(t, err)
	return guardFile{path: path, fset: fset, file: file}
}

// rawAccess reports every raw descriptor access in files: a call into
// golang.org/x/sys/unix or syscall, os.NewFile, or a Fd or SyscallConn
// method call. allowed counts those in rawAccessFile, which may make them.
func rawAccess(files []guardFile) (violations []string, allowed int) {
	for _, f := range files {
		imports := f.imports()
		ast.Inspect(f.file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			raw := false
			if path, name, ok := f.packageSelector(call.Fun, imports); ok {
				raw = path == unixImport || path == "syscall" || (path == "os" && name == "NewFile")
			} else if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				raw = selector.Sel.Name == "Fd" || selector.Sel.Name == "SyscallConn"
			}
			switch {
			case !raw:
			case f.path == rawAccessFile:
				allowed++
			default:
				violations = append(violations, f.where(call)+": raw descriptor access outside "+rawAccessFile)
			}
			return true
		})
	}
	return violations, allowed
}

// funcOwner names a function declaration as Recv.Name, or Name.
func funcOwner(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}
	recv := decl.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + decl.Name.Name
	}
	return decl.Name.Name
}

// parentLiterals reports every string literal with a ".." path component in
// files. allowed counts those in (*Dir).OpenParent of internal/at, the one
// place that may reach a parent directory.
func parentLiterals(files []guardFile) (violations []string, allowed int) {
	for _, f := range files {
		for _, decl := range f.file.Decls {
			owner := ""
			if function, ok := decl.(*ast.FuncDecl); ok {
				owner = funcOwner(function)
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil || !hasParentComponent(value) {
					return true
				}
				if strings.HasPrefix(f.path, "internal/at/") && owner == "Dir.OpenParent" {
					allowed++
				} else {
					violations = append(violations, f.where(literal)+`: ".." outside (*Dir).OpenParent`)
				}
				return true
			})
		}
	}
	return violations, allowed
}

func hasParentComponent(value string) bool {
	for _, component := range strings.Split(value, "/") {
		if component == ".." {
			return true
		}
	}
	return false
}

// walkMutations reports everything in walkFile that could reach a mutation:
// a name from package at outside readOnlyAt, a method call named like a
// mutation, or the remover. readOnly counts the read-only at names it uses.
func walkMutations(files []guardFile) (violations []string, readOnly int) {
	for _, f := range files {
		if f.path != walkFile {
			continue
		}
		imports := f.imports()
		ast.Inspect(f.file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.SelectorExpr:
				if path, name, ok := f.packageSelector(node, imports); ok && path == atImport {
					if readOnlyAt[name] {
						readOnly++
					} else {
						violations = append(violations, f.where(node)+": walk names at."+name+", outside the read-only side")
					}
				} else if mutatingMethods[node.Sel.Name] {
					violations = append(violations, f.where(node)+": walk calls "+node.Sel.Name)
				}
			case *ast.Ident:
				if removerNames[node.Name] {
					violations = append(violations, f.where(node)+": walk reaches the remover's "+node.Name)
				}
			}
			return true
		})
	}
	return violations, readOnly
}

// nameMinting reports every call of ParseName, fstree's or at's, outside
// nameFile. allowed counts those in nameFile.
func nameMinting(files []guardFile) (violations []string, allowed int) {
	for _, f := range files {
		imports := f.imports()
		ast.Inspect(f.file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			minted := false
			if ident, ok := call.Fun.(*ast.Ident); ok {
				minted = ident.Name == "ParseName"
			} else if path, name, ok := f.packageSelector(call.Fun, imports); ok {
				minted = path == atImport && name == "ParseName"
			}
			switch {
			case !minted:
			case f.path == nameFile:
				allowed++
			default:
				violations = append(violations, f.where(call)+": ParseName outside "+nameFile)
			}
			return true
		})
	}
	return violations, allowed
}

func TestConfinementGuard(t *testing.T) {
	files := productionFiles(t)
	paths := map[string]bool{}
	for _, f := range files {
		paths[f.path] = true
	}
	for _, want := range []string{rawAccessFile, walkFile, nameFile, "remove_linux.go", "internal/at/dir_linux.go"} {
		require.True(t, paths[want], "the guard must scan %s", want)
	}

	violations, allowed := rawAccess(files)
	require.Empty(t, violations)
	require.Positive(t, allowed, "the scan must see the raw calls in %s", rawAccessFile)

	violations, allowed = parentLiterals(files)
	require.Empty(t, violations)
	require.Equal(t, 1, allowed, `exactly one ".." literal, in (*Dir).OpenParent`)

	violations, readOnly := walkMutations(files)
	require.Empty(t, violations)
	require.Positive(t, readOnly, "the scan must see the walk's use of at.View")

	violations, allowed = nameMinting(files)
	require.Empty(t, violations)
	require.Positive(t, allowed, "the scan must see ParseName in %s", nameFile)
}

// Every rule fires on a violating fixture, and stays quiet on its allowed
// counterpart.
func TestConfinementGuardFiresOnViolations(t *testing.T) {
	type check func([]guardFile) ([]string, int)
	cases := []struct {
		name, path, src string
		check           check
		violations      int
	}{
		{
			name: "unix call", path: "remove_linux.go", check: rawAccess, violations: 1,
			src: "package fstree\nimport \"golang.org/x/sys/unix\"\nfunc f() { _ = unix.Unlinkat(3, \"x\", 0) }",
		},
		{
			name: "aliased unix call", path: "walk_linux.go", check: rawAccess, violations: 1,
			src: "package fstree\nimport sys \"golang.org/x/sys/unix\"\nfunc f() { _ = sys.Close(3) }",
		},
		{
			name: "syscall call", path: "fstree_linux.go", check: rawAccess, violations: 1,
			src: "package fstree\nimport \"syscall\"\nfunc f() { _ = syscall.Close(3) }",
		},
		{
			name: "unix call in another at file", path: "internal/at/dir_linux.go", check: rawAccess, violations: 1,
			src: "package at\nimport \"golang.org/x/sys/unix\"\nfunc f() { _ = unix.Close(3) }",
		},
		{
			name: "os.NewFile and Fd", path: "remove_linux.go", check: rawAccess, violations: 3,
			src: "package fstree\nimport \"os\"\nfunc f(p *os.File) { _ = os.NewFile(p.Fd(), \"x\"); _, _ = p.SyscallConn() }",
		},
		{
			name: "unix constants and the raw file are allowed", path: "remove_linux.go", check: rawAccess,
			src: "package fstree\nimport \"golang.org/x/sys/unix\"\nvar _ = unix.ENOENT\nvar _ = unix.Stat_t{}",
		},
		{
			name: "parent literal", path: "remove_linux.go", check: parentLiterals, violations: 3,
			src: "package fstree\nfunc f(g func(string)) { g(\"..\"); g(\"../x\"); g(\"a/..\"); g(\"...\") }",
		},
		{
			name: "parent literal in another at method", path: "internal/at/dir_linux.go", check: parentLiterals,
			violations: 1,
			src:        "package at\ntype Dir struct{}\nfunc (d *Dir) OpenChild(g func(string)) { g(\"..\") }",
		},
		{
			name: "parent literal in OpenParent is allowed", path: "internal/at/dir_linux.go", check: parentLiterals,
			src: "package at\ntype Dir struct{}\nfunc (d *Dir) OpenParent(g func(string)) { g(\"..\") }",
		},
		{
			name: "walk names a mutable at type", path: walkFile, check: walkMutations, violations: 3,
			src: "package fstree\nimport \"" + atImport + "\"\nfunc f(d *at.Dir, l at.Listed) { _ = at.Borrow }",
		},
		{
			name: "walk calls a mutation", path: walkFile, check: walkMutations, violations: 3,
			src: "package fstree\nfunc f(v interface{ Unlink() error; Rmdir() error; RenameNoReplaceInto() error }) " +
				"{ _ = v.Unlink(); _ = v.Rmdir(); _ = v.RenameNoReplaceInto() }",
		},
		{
			name: "walk reaches the remover", path: walkFile, check: walkMutations, violations: 2,
			src: "package fstree\nfunc f() { _ = removeBeneath; _ = newRemover }",
		},
		{
			name: "read-only walk is allowed", path: walkFile, check: walkMutations,
			src: "package fstree\nimport \"" + atImport + "\"\nfunc f(v at.View, l at.ListedView, r at.Reader) {}",
		},
		{
			name: "remover side may mutate", path: "remove_linux.go", check: walkMutations,
			src: "package fstree\nimport \"" + atImport + "\"\nfunc f(d *at.Dir) { _ = d.Unlink }",
		},
		{
			name: "listed name laundered into a Name", path: "remove_linux.go", check: nameMinting, violations: 1,
			src: "package fstree\nfunc f(e interface{ String() string }) { _, _ = ParseName(e.String()) }",
		},
		{
			name: "at.ParseName in the walk", path: walkFile, check: nameMinting, violations: 1,
			src: "package fstree\nimport \"" + atImport + "\"\nfunc f() { _, _ = at.ParseName(\"x\") }",
		},
		{
			name: "ParseName in name_linux.go is allowed", path: nameFile, check: nameMinting,
			src: "package fstree\nfunc f() { _, _ = ParseName(\"x\") }",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			violations, _ := tc.check([]guardFile{fixture(t, tc.path, tc.src)})
			require.Len(t, violations, tc.violations, "%v", violations)
		})
	}
}
