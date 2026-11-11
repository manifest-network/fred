package testutil

// This repository-level guard pins where a close command is minted (ENG-2154).
// A close's answer tells a close the lease actor handled from one a retiring
// actor never handled, and only docker-backend's routeClose acts on the second
// case correctly: it admits the close again under a fresh command fence, which
// re-reads what the first admission checked, before routing it again. A second
// minting site would be a second loop to get right. The historical defect was
// exactly that: a single-attempt helper whose second caller reported a close
// nobody ran as a failed close.
//
//   - leasesm.NewDeprovisionCommand is named only in Backend.routeClose, in
//     internal/backend/docker/deprovision.go, however leasesm is imported.
//   - Inside leasesm's production code it is named only where it is declared.
//
// Dot-importing leasesm is already refused by the projection status guard.
// Test files are outside the rule. Every rule is proven to fire by
// TestCloseRoutingGuardFires.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

var closeRoutingSite = attributionSite{"internal/backend/docker/deprovision.go", "Backend.routeClose"}

func TestCloseRoutingIsSealed(t *testing.T) {
	root := repoRoot(t)
	var findings []string
	sanctioned := 0
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			fileFindings, fileSanctioned := closeRoutingFindings(rel, file, fset)
			findings = append(findings, fileFindings...)
			sanctioned += fileSanctioned
		})
	}
	if len(findings) > 0 {
		t.Errorf("a close command is minted outside routeClose (ENG-2154):\n  %s", strings.Join(findings, "\n  "))
	}
	// Positive control: a moved or renamed site would otherwise leave this
	// guard checking nothing.
	if sanctioned != 1 {
		t.Errorf("%s in %s names leasesm.NewDeprovisionCommand %d times, want once",
			closeRoutingSite.function, closeRoutingSite.file, sanctioned)
	}
}

func TestCloseRoutingGuardFires(t *testing.T) {
	const leasesmImport = `import "github.com/manifest-network/fred/internal/backend/shared/leasesm"
`
	tests := []struct {
		name string
		rel  string
		src  string
		want bool
	}{
		{"minted by another docker file", "internal/backend/docker/recover.go",
			"package docker\n" + leasesmImport + `func (b *Backend) resumeRecoveredClose() { _, _, _ = leasesm.NewDeprovisionCommand(f) }`, true},
		{"minted by another function of the routing file", closeRoutingSite.file,
			"package docker\n" + leasesmImport + `func (b *Backend) closeThroughActor() { _, _, _ = leasesm.NewDeprovisionCommand(f) }`, true},
		{"minted through an alias", "internal/backend/docker/volume_delete_hold.go",
			`package docker
import sm "github.com/manifest-network/fred/internal/backend/shared/leasesm"
func (b *Backend) resume() { _, _, _ = sm.NewDeprovisionCommand(f) }`, true},
		{"taken as a value", "internal/backend/docker/backend.go",
			"package docker\n" + leasesmImport + `var mint = leasesm.NewDeprovisionCommand`, true},
		{"minted inside leasesm", "internal/backend/shared/leasesm/lease_actor.go",
			`package leasesm
func other() { _, _, _ = NewDeprovisionCommand(f) }`, true},
		{"minted by routeClose", closeRoutingSite.file,
			"package docker\n" + leasesmImport + `func (b *Backend) routeClose() { _, _, _ = leasesm.NewDeprovisionCommand(f) }`, false},
		{"declared in leasesm", "internal/backend/shared/leasesm/lease_actor.go",
			`package leasesm
func NewDeprovisionCommand(f int) (int, int, error) { return 0, 0, nil }`, false},
		{"a same-named function of another package", "internal/backend/docker/recover.go",
			`package docker
import other "github.com/manifest-network/fred/internal/backend/shared"
func (b *Backend) f() { _ = other.NewDeprovisionCommand }`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, test.rel, test.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			findings, _ := closeRoutingFindings(test.rel, file, fset)
			if got := len(findings) > 0; got != test.want {
				t.Fatalf("findings = %q, want a finding: %t", findings, test.want)
			}
		})
	}
}

// closeRoutingFindings reports every place in a production file that names
// leasesm.NewDeprovisionCommand outside its one site, and counts the names at
// that site.
func closeRoutingFindings(rel string, file *ast.File, fset *token.FileSet) (findings []string, sanctioned int) {
	leasesmName, imported := importNames(file)[leasesmImportPath]
	inLeasesm := path.Dir(rel) == leasesmDir
	if !imported && !inLeasesm {
		return nil, 0
	}
	inspect := func(function string, root ast.Node, declared *ast.Ident) {
		atSite := rel == closeRoutingSite.file && function == closeRoutingSite.function
		var visit func(ast.Node) bool
		visit = func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				if qualifier, ok := typed.X.(*ast.Ident); ok && imported && qualifier.Name == leasesmName &&
					typed.Sel.Name == "NewDeprovisionCommand" {
					if atSite {
						sanctioned++
						return false
					}
					findings = append(findings, fmt.Sprintf("%s (%s): names leasesm.NewDeprovisionCommand outside %s",
						fset.Position(typed.Pos()), rel, closeRoutingSite.function))
				}
				// A selected name is a field or method, not an unqualified
				// reference: visit only the operand.
				ast.Inspect(typed.X, visit)
				return false
			case *ast.Ident:
				if inLeasesm && typed != declared && typed.Name == "NewDeprovisionCommand" {
					findings = append(findings, fmt.Sprintf("%s (%s): names NewDeprovisionCommand inside leasesm",
						fset.Position(typed.Pos()), rel))
				}
			}
			return true
		}
		ast.Inspect(root, visit)
	}
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok {
			inspect(functionKey(function), function, function.Name)
			continue
		}
		inspect("", decl, nil)
	}
	return findings, sanctioned
}
