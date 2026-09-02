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
// doc comment that says the declaration is for tests (including the
// nil-in-prod injection point CLAUDE.md forbids, whose doc says so
// outright) — and applies them to
// top-level funcs, types, consts and vars, and to struct fields (reported
// as Type.Field). A field's doc comment and its trailing comment both feed
// the match, because Go documents fields either way.
//
// The remaining blind spots, stated so they are documented limits rather
// than unknown ones:
//
//   - A test-only helper named and documented like production code. No
//     pattern reaches that; the audit that found the originals does.
//   - Methods declared inside an interface type. structFields walks
//     *ast.StructType only, so `interface { ResetForTest() }` is invisible.
//     Left out because it has never occurred: a scan of the 187 interface
//     methods in internal/ and cmd/ found no match, even against a phrase
//     list widened well past the one below. If one appears, extend
//     structFields to *ast.InterfaceType rather than widening phrases.
//   - A hook whose comment hedges ("For testing; defaults to …") instead of
//     using one of the phrases below. A widening keyed on "for testing" was
//     measured and rejected: the smallest one that catches those also flags
//     internal/chain/client.go's TLSSkipVerify, which is shipped production
//     config (wired at cmd/providerd/main.go from cfg.GRPCTLSSkipVerify,
//     documented as grpc_tls_skip_verify in config.example.yaml) whose
//     "(for testing only)" comment is an operator security warning. A guard
//     that flags production config is a guard the next person disables.
//     ENG-765 added "set in tests", which reaches the nil-in-prod shape
//     without paying that cost — see the note on the phrase itself for why
//     it is keyed on purpose rather than on state.
//   - A field written in production but read only by tests, whose name and
//     doc are both ordinary production prose. No phrase can reach that:
//     there is nothing in the text to match. ENG-765 found three
//     (docker.Backend.httpClient, k3s.Backend.httpClient,
//     provisioner.Manager.handlers) by enumerating every unexported
//     production struct field and comparing prod reads against test reads.
//     Catching it needs write-vs-read selector analysis, not a phrase.
//
// TestTestOnlyRules pins the matching rules themselves, including the
// deliberate misses above. Extend it alongside any change to the patterns.
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
	// Deliberately the shorter stem: it subsumes "exposed for testing" and
	// also catches the wordings that slipped past it, which is how
	// docker.Backend.httpClient survived ENG-354 documented "exposed for
	// test replacement" (ENG-765). Note the reason string reports the
	// MATCHED phrase, and matching is first-match-wins over this slice, so
	// shortening an entry changes what a hit prints.
	"exposed for test",
	// ENG-765. Catches the nil-in-prod injection point CLAUDE.md forbids,
	// whose doc habitually reads "Nil in production; set in tests ...".
	// Deliberately keyed on the PURPOSE half of that sentence, not the
	// state half: "nil in production" would be a state phrase, and this
	// repo documents legitimate optional dependencies exactly that way
	// ("nil = disabled" in provisioner/manager.go, "Store may be nil to
	// disable callback persistence" in shared/callback_sender.go, "Left
	// nil when no TLS fields are set" in cmd/providerd/main.go). Reword
	// any of those slightly and a state phrase flags shipped config —
	// the failure mode this list's third blind spot below exists to
	// avoid. chain/client.go's clock seam already normalizes to
	// "time.now in production". Measured across all production files
	// under internal/ and cmd/: this phrase matched only the two k3s
	// hooks ENG-765 removed.
	"set in tests",
}

// testSupportPackages are directories whose entire purpose is to support
// tests. They are imported only from _test.go files and appear in no
// binary's dependency graph (asserted by TestNoTestSupportPackageInBinary
// -- see the assertion at the bottom of this file), so a test-only
// declaration there is by design, not a leak.
var testSupportPackages = []string{
	"internal/testutil",
	"internal/chain/chaintest",
	"internal/testsupport/backendclient",
}

// exemptDecls are individual production declarations the scan skips,
// keyed "<repo-relative file>:<declaration name>", with the reason. Every
// entry is a hole in the guard, so keep the list short and justified.
// Struct fields are named "<Type>.<Field>", so a field exemption reads
// "internal/foo/bar.go:Backend.someField".
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

// TestTestOnlyRules pins the matching rules against synthetic sources, so
// the guard's own coverage is asserted rather than assumed. The repo-wide
// scan above passes when the rules match nothing at all, which is also
// what a broken rule looks like.
//
// The want lists are exact: a case that expects nothing is a deliberate
// miss, and reading one here is the cheapest way to see what this guard
// does not do.
func TestTestOnlyRules(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "production declarations are not flagged",
			src: `package p
// Server serves requests.
type Server struct {
	addr string
	// client dials the backend.
	client *http.Client
}
// Serve starts the server.
func (s *Server) Serve() error { return nil }
const DefaultAddr = ":8080"
`,
			want: nil,
		},
		{
			name: "func named for tests",
			src: `package p
func ResetForTest() {}
`,
			want: []string{"ResetForTest -- name declares it as test scaffolding"},
		},
		{
			name: "var documented for tests",
			src: `package p
// Clock is a testing helper that freezes time.
var Clock = 0
`,
			want: []string{"Clock -- doc comment says it exists for tests: is a testing helper"},
		},
		{
			name: "struct field named for tests",
			src: `package p
type Backend struct {
	hookForTest func()
}
`,
			want: []string{"Backend.hookForTest -- name declares it as test scaffolding"},
		},
		{
			name: "struct field with doc comment above",
			src: `package p
type Backend struct {
	// hook is test-only.
	hook func()
}
`,
			want: []string{"Backend.hook -- doc comment says it exists for tests: test-only"},
		},
		{
			name: "struct field with trailing comment",
			src: `package p
type Backend struct {
	hook func() // test-only
}
`,
			want: []string{"Backend.hook -- doc comment says it exists for tests: test-only"},
		},
		{
			// The phrase itself straddles the line break, which is what
			// gofmt does to a long doc comment. ast.Doc.Text() joins the
			// lines with "\n", so an unnormalized Contains misses this.
			name: "phrase wrapped across comment lines, field",
			src: `package p
type Backend struct {
	// hook should only be used
	// in tests.
	hook func()
}
`,
			want: []string{"Backend.hook -- doc comment says it exists for tests: only be used in tests"},
		},
		{
			name: "phrase wrapped across comment lines, top level",
			src: `package p
// Clock is exposed for
// testing.
var Clock = 0
`,
			want: []string{"Clock -- doc comment says it exists for tests: exposed for test"},
		},
		{
			// The wording that slipped past the longer "exposed for
			// testing" and left docker.Backend.httpClient uncaught
			// through all of ENG-354 (ENG-765).
			name: "exposed-for-test variant wording, field",
			src: `package p
type Backend struct {
	// httpClient is used by callbackSender; exposed for test replacement.
	httpClient *http.Client
}
`,
			want: []string{"Backend.httpClient -- doc comment says it exists for tests: exposed for test"},
		},
		{
			name: "nested anonymous struct field",
			src: `package p
type Backend struct {
	inner struct {
		hookForTest func()
	}
}
`,
			want: []string{"Backend.inner.hookForTest -- name declares it as test scaffolding"},
		},
		{
			name: "embedded field is skipped (flagged at its own declaration)",
			src: `package p
type Backend struct {
	SomethingForTest
}
`,
			want: nil,
		},
		{
			name: "type named for tests is reported once, not per field",
			src: `package p
type StubForTest struct {
	addr string
}
`,
			want: []string{"StubForTest -- name declares it as test scaffolding"},
		},
		{
			name: "documented limit: interface methods are not walked",
			src: `package p
type Store interface {
	ResetForTest()
}
`,
			want: nil,
		},
		{
			// ENG-765's shape. The trailing-comment position, and the
			// phrase on one line.
			name: "nil-in-prod hook field, trailing comment",
			src: `package p
type Backend struct {
	beforeSend func() // nil in production; set in tests to force an interleaving
}
`,
			want: []string{"Backend.beforeSend -- doc comment says it exists for tests: set in tests"},
		},
		{
			// The same shape in the doc position, with the phrase
			// straddling a line break — which is how gofmt left the real
			// k3s hooks, and the reason an unnormalized Contains would
			// have missed one of the two.
			name: "nil-in-prod hook field, phrase wrapped across doc lines",
			src: `package p
type Backend struct {
	// beforeSend fires at checkpoint 2. Nil in production; set in
	// tests to interleave a teardown race.
	beforeSend func()
}
`,
			want: []string{"Backend.beforeSend -- doc comment says it exists for tests: set in tests"},
		},
		{
			// The optional-dependency idiom this repo uses everywhere.
			// It is the reason the phrase above is keyed on purpose
			// ("set in tests") rather than state ("nil in production").
			name: "documented limit: an optional production dependency is not flagged",
			src: `package p
type Manager struct {
	// placementStore is optional and is nil in production when the
	// operator has not configured a placement database.
	placementStore *Store
}
`,
			want: nil,
		},
		{
			name: "documented limit: a hedged clock seam is not flagged",
			src: `package p
type Auth struct {
	nowFunc func() time.Time // For testing; defaults to time.Now
}
`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", tt.src, parser.ParseComments|parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parsing synthetic source: %v", err)
			}
			var got []string
			for _, decl := range file.Decls {
				for _, d := range declsOf(decl) {
					if reason := testOnlyReason(d); reason != "" {
						got = append(got, d.name+" -- "+reason)
					}
				}
			}
			if strings.Join(got, "\n") != strings.Join(tt.want, "\n") {
				t.Errorf("findings mismatch\n got: %v\nwant: %v", got, tt.want)
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
				out = append(out, structFields(s.Name.Name, s.Type)...)
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

// structFields flattens a struct type's named fields, so the scan sees the
// `hookForTest func()` inside a type and not just the type. A nil-in-prod
// injection point is a field, which is exactly the shape ENG-354 forbids,
// and a top-level-only walk cannot see one.
//
// Both comment positions are read: Go documents a field either above it
// (ast.Field.Doc) or on the trailing line (ast.Field.Comment), and the
// trailing form is the common one for a one-line hook. Embedded fields
// carry no Names, and are skipped rather than synthesized: the field name
// an embedded type contributes is its own type name, so a test-only
// embedded type is already reported at its TypeSpec, and one embedded from
// another package is covered by TestNoTestSupportPackageInBinary. Naming it
// here would double-report. Anonymous nested structs recurse, keeping the
// dotted path.
func structFields(prefix string, expr ast.Expr) []declInfo {
	st, ok := expr.(*ast.StructType)
	if !ok || st.Fields == nil {
		return nil
	}
	var out []declInfo
	for _, f := range st.Fields.List {
		doc := f.Doc.Text() + "\n" + f.Comment.Text()
		for _, n := range f.Names {
			name := prefix + "." + n.Name
			out = append(out, declInfo{name: name, doc: doc, pos: n.Pos()})
			out = append(out, structFields(name, f.Type)...)
		}
	}
	return out
}

func testOnlyReason(d declInfo) string {
	// Match the identifier itself, not the dotted path: a field of a type
	// already flagged by name would otherwise be reported a second time
	// for its owner's name.
	ident := d.name[strings.LastIndex(d.name, ".")+1:]
	if testOnlyNamePattern.MatchString(ident) {
		return "name declares it as test scaffolding"
	}
	// Collapse newlines and runs of spaces: gofmt wraps a long doc comment
	// mid-phrase, and a raw Contains against the wrapped text silently
	// misses it ("should only be used\nin tests").
	doc := strings.Join(strings.Fields(strings.ToLower(d.doc)), " ")
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
