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
// either separator dockerd accepts anywhere in the literal, so a template or
// a CLI-style flag such as "--security-opt=seccomp=unconfined" counts too.
func seccompOptionLiteral(value string) bool {
	return value == "seccomp" || strings.Contains(value, "seccomp=") || strings.Contains(value, "seccomp:")
}

// seccompOptionLiterals returns the position of every string literal in the
// Go source that could write or select a seccomp security option. src is
// passed to parser.ParseFile: nil reads path.
func seccompOptionLiterals(t *testing.T, path string, src any) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	require.NoError(t, err)
	var findings []string
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
	return findings
}

func TestSeccompOptionKeyOnlyInOwnerFile(t *testing.T) {
	root := moduleRoot(t)
	var findings []string
	scanned, ownerSeen := 0, false
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
				ownerSeen = true
				return nil
			}
			scanned++
			findings = append(findings, seccompOptionLiterals(t, path, nil)...)
			return nil
		}))
	}
	require.True(t, ownerSeen, "the walk must reach %s", ownerFile)
	require.Greater(t, scanned, 100, "the walk must cover the production sources")
	require.Empty(t, findings, "only %s may spell the seccomp security-option key; route the option through it", ownerFile)
}

// The guard fires on the file that legitimately spells the key, and on the
// shapes a caller could use to smuggle it in elsewhere.
func TestSeccompOptionGuardFires(t *testing.T) {
	owner := seccompOptionLiterals(t, filepath.Join(moduleRoot(t), ownerFile), nil)
	require.NotEmpty(t, owner, "the inspector must find the key in %s", ownerFile)

	findings := seccompOptionLiterals(t, "positive.go", `package positive

var options = []string{
	"seccomp=unconfined",
	"seccomp:builtin",
	"--security-opt=seccomp=unconfined",
	`+"`security_opt: [\"seccomp:unconfined\"]`"+`,
	"no-new-privileges:true",
	"name=seccomp,profile=builtin",
}

const key = "seccomp"
`)
	require.Len(t, findings, 5)
}

func TestSeccompOptionLiteralRule(t *testing.T) {
	for value, want := range map[string]bool{
		"seccomp":                            true,
		"seccomp=":                           true,
		"seccomp:unconfined":                 true,
		"--security-opt=seccomp=unconfined":  true,
		"security_opt: [seccomp:unconfined]": true,
		"name=seccomp":                       false,
		"name=seccomp,profile=builtin":       false,
		"seccomp_census_total":               false,
		"no-new-privileges:true":             false,
		"tenant seccomp profile":             false,
		"seccomp profile refused":            false,
		"github.com/moby/profiles/seccomp":   false,
	} {
		require.Equal(t, want, seccompOptionLiteral(value), value)
	}
}
