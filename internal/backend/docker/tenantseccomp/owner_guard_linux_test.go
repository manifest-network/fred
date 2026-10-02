//go:build linux

package tenantseccomp

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

// ownerFile is the only production file that may spell the seccomp
// security-option key.
const ownerFile = "internal/backend/docker/tenantseccomp/securityopt_linux.go"

// seccompOptionLiteral reports whether a Go string literal could write or
// select a seccomp security option: the bare key, or the key followed by
// either separator dockerd accepts.
func seccompOptionLiteral(value string) bool {
	return value == "seccomp" || strings.HasPrefix(value, "seccomp=") || strings.HasPrefix(value, "seccomp:")
}

func TestSeccompOptionKeyOnlyInOwnerFile(t *testing.T) {
	root := moduleRoot(t)
	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		require.NoError(t, filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if name := entry.Name(); name == "testdata" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if filepath.ToSlash(rel) == ownerFile {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && seccompOptionLiteral(value) {
					findings = append(findings, fset.Position(literal.Pos()).String())
				}
				return true
			})
			return nil
		}))
	}
	require.Empty(t, findings, "only %s may spell the seccomp security-option key; route the option through it", ownerFile)
}

func TestSeccompOptionLiteralRule(t *testing.T) {
	for value, want := range map[string]bool{
		"seccomp":                 true,
		"seccomp=":                true,
		"seccomp:unconfined":      true,
		"name=seccomp":            false,
		"seccomp_census_total":    false,
		"no-new-privileges:true":  false,
		"tenant seccomp profile":  false,
		"seccomp profile refused": false,
	} {
		require.Equal(t, want, seccompOptionLiteral(value), value)
	}
}
