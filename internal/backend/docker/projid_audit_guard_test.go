package docker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The project-ID audit only reads. Its whole path lives in projid_audit.go:
// the opener, the walker and the attribute getter. These guards pin what
// review alone would otherwise have to: the file names no mutating API, its
// one ioctl is FS_IOC_FSGETXATTR, and no other production file opens a volume
// for the audit, so the raw directory the opener returns stays inside it.

const projidAuditFile = "projid_audit.go"

// projidAuditPackageAllow is, by import path, every package-level name the
// audit file may use from the packages that reach the filesystem. Anything
// else from them, such as an unlink, a rename or an ioctl setter, is a
// finding. The syscall package is allowed only for the stat type.
var projidAuditPackageAllow = map[string]map[string]bool{
	"golang.org/x/sys/unix": {
		"Openat": true, "Fstat": true, "Close": true, "Syscall": true, "SYS_IOCTL": true, "Stat_t": true,
		"O_RDONLY": true, "O_NOFOLLOW": true, "O_NONBLOCK": true, "O_NOCTTY": true, "O_CLOEXEC": true, "O_NOATIME": true,
		"S_IFMT": true, "S_IFREG": true, "DT_REG": true,
		"ENOENT": true, "ELOOP": true, "ENXIO": true, "ENOTDIR": true, "EAGAIN": true, "EWOULDBLOCK": true, "EPERM": true,
	},
	"os":      {"File": true, "Root": true, "OpenRoot": true},
	"syscall": {"Stat_t": true},
	"github.com/manifest-network/fred/internal/fstree": {
		"WalkBeneath": true, "ParseName": true, "BorrowedDir": true, "ErrTooDeep": true, "ErrTreeChanged": true,
	},
}

// projidAuditForbiddenNames are this package's mutating entry points and the
// FS_IOC_FSSETXATTR request: the audit file must not name them at all.
var projidAuditForbiddenNames = map[string]bool{
	"linuxFSIOCFSSetXAttr": true, "SetProjectID": true, "RemoveBeneath": true, "DetachCondemnedAnchor": true,
	"Destroy": true, "RetryHeldVolumeDelete": true, "RenameVolume": true, "EnsureQuota": true,
}

// projidAuditForbiddenMethods are the mutating methods of *os.File and
// *os.Root, which the audit file holds; it must call none of them.
var projidAuditForbiddenMethods = map[string]bool{
	"Remove": true, "RemoveAll": true, "Rename": true, "Mkdir": true, "MkdirAll": true, "Symlink": true,
	"Link": true, "Chmod": true, "Chown": true, "Lchown": true, "Chtimes": true, "Truncate": true,
	"Write": true, "WriteAt": true, "WriteString": true, "WriteFile": true, "Create": true, "OpenFile": true,
}

// projidAuditMutations returns a finding for every use in file of a name the
// read-only audit must not use, and for every ioctl other than
// FS_IOC_FSGETXATTR.
func projidAuditMutations(fset *token.FileSet, file *ast.File) []string {
	imports := make(map[string]string) // local name -> import path
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	var findings []string
	report := func(node ast.Node, what string) {
		findings = append(findings, fset.Position(node.Pos()).String()+": "+what)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.Ident:
			if projidAuditForbiddenNames[node.Name] {
				report(node, node.Name)
			}
		case *ast.SelectorExpr:
			if pkg, ok := node.X.(*ast.Ident); ok {
				if path, imported := imports[pkg.Name]; imported {
					if allow, guarded := projidAuditPackageAllow[path]; guarded && !allow[node.Sel.Name] {
						report(node, pkg.Name+"."+node.Sel.Name)
					}
				}
			}
		case *ast.CallExpr:
			selector, ok := node.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, isPkg := selector.X.(*ast.Ident); isPkg && imports[pkg.Name] != "" {
				if imports[pkg.Name] == "golang.org/x/sys/unix" && selector.Sel.Name == "Syscall" && !fsGetXAttrIoctl(node) {
					report(node, "an ioctl other than FS_IOC_FSGETXATTR")
				}
				return true
			}
			if projidAuditForbiddenMethods[selector.Sel.Name] {
				report(node, "method "+selector.Sel.Name)
			}
		}
		return true
	})
	return findings
}

// fsGetXAttrIoctl reports whether call is unix.Syscall(unix.SYS_IOCTL, fd,
// linuxFSIOCFSGetXAttr, buffer).
func fsGetXAttrIoctl(call *ast.CallExpr) bool {
	if len(call.Args) != 4 {
		return false
	}
	number, ok := call.Args[0].(*ast.SelectorExpr)
	if !ok || number.Sel.Name != "SYS_IOCTL" {
		return false
	}
	request, ok := call.Args[2].(*ast.Ident)
	return ok && request.Name == "linuxFSIOCFSGetXAttr"
}

func TestProjidAuditFileOnlyReads(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, projidAuditFile, nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	// The scan must have reached the code it guards.
	var walks, ioctls int
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				switch selector.Sel.Name {
				case "WalkBeneath":
					walks++
				case "Syscall":
					if fsGetXAttrIoctl(call) {
						ioctls++
					}
				}
			}
		}
		return true
	})
	require.Equal(t, 1, walks, "the audit walks through fstree.WalkBeneath")
	require.Equal(t, 1, ioctls, "the getter's FS_IOC_FSGETXATTR")
	require.Empty(t, projidAuditMutations(fset, file), "%s must stay read-only", projidAuditFile)
}

// Every rule fires on a known positive.
func TestProjidAuditGuardRulesFire(t *testing.T) {
	const source = `package docker

import (
	"os"
	sys "golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree"
)

func mutate(root *os.Root, file *os.File, fd int) {
	_ = sys.Unlinkat(fd, "entry", 0)
	_ = sys.IoctlSetInt(fd, 1, 2)
	sys.Syscall(sys.SYS_IOCTL, uintptr(fd), linuxFSIOCFSSetXAttr, 0)
	sys.Syscall(sys.SYS_IOCTL, uintptr(fd), 0x401c5820, 0)
	_ = root.RemoveAll("entry")
	_ = file.Chmod(0)
	_, _ = fstree.RemoveBeneath(nil, file, fstree.Name{}, fstree.RemoveOptions{})
	_ = linuxXFSProjectAttributes{}.SetProjectID(root, 1)
	_ = os.Remove("entry")
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "positive.go", source, parser.SkipObjectResolution)
	require.NoError(t, err)
	findings := strings.Join(projidAuditMutations(fset, file), "\n")
	for _, want := range []string{
		"sys.Unlinkat", "sys.IoctlSetInt", "linuxFSIOCFSSetXAttr", "an ioctl other than FS_IOC_FSGETXATTR",
		"method RemoveAll", "method Chmod", "fstree.RemoveBeneath", "fstree.Name", "fstree.RemoveOptions",
		"SetProjectID", "os.Remove",
	} {
		require.Contains(t, findings, want)
	}
	// Both raw ioctls are flagged, the setter by name as well.
	require.Equal(t, 2, strings.Count(findings, "an ioctl other than FS_IOC_FSGETXATTR"))
}

// openProjectIDAuditUses returns the uses of OpenProjectIDAudit in file: a
// call or a method value. Declarations are not uses.
func openProjectIDAuditUses(fset *token.FileSet, file *ast.File) []string {
	var uses []string
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "OpenProjectIDAudit" {
			uses = append(uses, fset.Position(selector.Pos()).String())
		}
		return true
	})
	return uses
}

// The opener hands out a raw directory of a tenant volume. Only the audit
// may call it; the read view only forwards it.
func TestOnlyTheAuditOpensVolumesForTheAudit(t *testing.T) {
	allowed := map[string]bool{projidAuditFile: true, "substrate_read_views.go": true}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	scanned := 0
	var findings []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		require.NoError(t, err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, source, parser.SkipObjectResolution)
		require.NoError(t, err)
		scanned++
		if uses := openProjectIDAuditUses(fset, file); len(uses) > 0 && !allowed[path] {
			findings = append(findings, uses...)
		}
	}
	require.Greater(t, scanned, 50, "the scan must cover the package's production files")
	require.Empty(t, findings, "only %s opens a volume for the project-ID audit", projidAuditFile)

	// Positive control: a call and a method value are both uses.
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "positive.go", `package docker
func use(v volumeReader) {
	_, _ = v.OpenProjectIDAudit(nil, managedVolumeName{})
	open := v.OpenProjectIDAudit
	_ = open
}
`, parser.SkipObjectResolution)
	require.NoError(t, err)
	require.Len(t, openProjectIDAuditUses(fset, file), 2)
}
