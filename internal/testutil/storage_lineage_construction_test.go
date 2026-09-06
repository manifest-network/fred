package testutil

// This repository-level tripwire prevents the removed unbound shared-store
// constructors from being reintroduced at a production call site. Tests that
// need a corrupt or stopped pre-binding journal use package-local fixtures;
// every external consumer must present verified storage lineage.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

const (
	sharedPackagePath = "github.com/manifest-network/fred/internal/backend/shared"
)

var forbiddenProductionCalls = map[string]map[string]struct{}{
	sharedPackagePath: {
		"NewCallbackStore":  {},
		"NewReleaseStore":   {},
		"NewRetentionStore": {},
	},
}

func TestProductionUsesIdentityBoundStoreConstruction(t *testing.T) {
	root := repoRoot(t)

	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			if rel == "internal/backendidentity/marker.go" {
				return
			}

			localPackagePath := ""
			if filepath.ToSlash(filepath.Dir(rel)) == "internal/backend/shared" {
				localPackagePath = sharedPackagePath
			}
			for _, finding := range forbiddenCallsInFile(file, fset, localPackagePath) {
				findings = append(findings, strings.TrimPrefix(finding, root+string(filepath.Separator)))
			}
		})
	}

	if len(findings) > 0 {
		t.Errorf("production code bypasses authoritative-store lineage construction.\n"+
			"Use the identity-bound store constructors and the store-aware, crash-resumable marker initializer instead:\n  %s",
			strings.Join(findings, "\n  "))
	}
}

func forbiddenCallsInFile(file *ast.File, fset *token.FileSet, localPackagePath string) []string {
	aliases := make(map[string]string)
	dotImports := make(map[string]struct{})
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if _, tracked := forbiddenProductionCalls[path]; !tracked {
			continue
		}

		if imp.Name != nil {
			switch imp.Name.Name {
			case "_":
				continue
			case ".":
				dotImports[path] = struct{}{}
				continue
			default:
				aliases[imp.Name.Name] = path
			}
			continue
		}
		aliases[filepath.Base(path)] = path
	}

	var findings []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		var packagePath, function string
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			ident, ok := fun.X.(*ast.Ident)
			if !ok {
				return true
			}
			packagePath = aliases[ident.Name]
			function = fun.Sel.Name
		case *ast.Ident:
			function = fun.Name
			if _, forbidden := forbiddenProductionCalls[localPackagePath][function]; forbidden {
				packagePath = localPackagePath
				break
			}
			for path := range dotImports {
				if _, forbidden := forbiddenProductionCalls[path][function]; forbidden {
					packagePath = path
					break
				}
			}
		}

		if _, forbidden := forbiddenProductionCalls[packagePath][function]; forbidden {
			position := fset.Position(call.Pos())
			findings = append(findings, position.String()+": "+packagePath+"."+function)
		}
		return true
	})

	return findings
}

func TestForbiddenProductionCallDetection(t *testing.T) {
	tests := []struct {
		name             string
		src              string
		localPackagePath string
		want             int
	}{
		{
			name: "default import",
			src: `package p
import "github.com/manifest-network/fred/internal/backend/shared"
func f() { shared.NewCallbackStore(shared.CallbackStoreConfig{}) }
`,
			want: 1,
		},
		{
			name: "dot import",
			src: `package p
import . "github.com/manifest-network/fred/internal/backend/shared"
func f() { NewReleaseStore(ReleaseStoreConfig{}) }
`,
			want: 1,
		},
		{
			name: "strict construction allowed",
			src: `package p
import "github.com/manifest-network/fred/internal/backend/shared"
func f() { shared.OpenIdentityBoundCallbackStore(shared.CallbackStoreConfig{}, nil, nil) }
`,
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", test.src, 0)
			if err != nil {
				t.Fatalf("parse synthetic source: %v", err)
			}
			if got := len(forbiddenCallsInFile(file, fset, test.localPackagePath)); got != test.want {
				t.Fatalf("got %d findings, want %d", got, test.want)
			}
		})
	}
}
