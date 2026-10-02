package failurecause

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCauseVocabularyIsClosed(t *testing.T) {
	seen := make(map[string]causeKind)
	for raw := range 256 {
		kind := causeKind(raw)
		cause := Cause{kind: kind}
		assert.Equal(t, kind == causeTenantWorkload, cause.Counts(),
			"only the tenant workload cause may count (kind %d)", raw)
		label := cause.Label()
		if kind >= causeSentinel || kind == causeUnknown {
			assert.Equal(t, "unknown", label, "kind %d outside the closed set must read as unknown", raw)
			continue
		}
		prior, duplicate := seen[label]
		assert.False(t, duplicate, "label %q shared by kinds %d and %d", label, prior, kind)
		seen[label] = kind
	}
	assert.Equal(t, []string{"unknown", "tenant_workload", "disruption", "platform", "maintenance"}, Labels())
	assert.False(t, Cause{}.Counts(), "the zero cause is unknown and never counts")
	assert.False(t, Platform().Counts())
	assert.False(t, Maintenance().Counts())
	assert.Equal(t, "platform", Platform().Label())
	assert.Equal(t, "maintenance", Maintenance().Label())
}

func TestClassifyDeath(t *testing.T) {
	provenances := map[string]Provenance{
		"unobserved":     {kind: provenanceUnobserved},
		"partial_run":    {kind: provenancePartialRun, instanceID: "c1"},
		"signaled":       {kind: provenanceSignaled, instanceID: "c1"},
		"observed_run":   {kind: provenanceObservedRun, instanceID: "c1"},
		"sentinel":       {kind: provenanceSentinel, instanceID: "c1"},
		"kind255":        {kind: 255, instanceID: "c1"},
		"other instance": {kind: provenanceObservedRun, instanceID: "c2"},
		"other signaled": {kind: provenanceSignaled, instanceID: "c2"},
		"unbound":        {kind: provenanceObservedRun},
	}
	terminations := map[string]Termination{
		"unknown": {kind: terminationUnknown},
		"exited":  Exited(),
		"gone":    Gone(),
		"bogus":   {kind: 255},
	}
	for _, instance := range []string{"c1", ""} {
		for provenanceName, provenance := range provenances {
			for terminationName, termination := range terminations {
				t.Run(fmt.Sprintf("%q/%s/%s", instance, provenanceName, terminationName), func(t *testing.T) {
					got := ClassifyDeath(instance, provenance, termination)
					bound := instance != "" && provenance.instanceID == instance
					switch {
					case (bound && provenanceName == "signaled") || terminationName == "gone":
						assert.Equal(t, "disruption", got.Label())
					case bound && provenanceName == "observed_run" && terminationName == "exited":
						assert.Equal(t, "tenant_workload", got.Label())
						assert.True(t, got.Counts())
					default:
						assert.Equal(t, "unknown", got.Label())
					}
					if got.Counts() {
						assert.True(t, bound, "only a provenance minted for this instance may count")
						assert.Equal(t, "observed_run", provenanceName)
						assert.Equal(t, "exited", terminationName)
					}
				})
			}
		}
	}
}

func TestEventSessionAttributesEachRun(t *testing.T) {
	tests := []struct {
		name   string
		events []string
		want   []string
	}{
		{name: "observed run", events: []string{"start", "die"}, want: []string{"observed_run"}},
		{name: "signaled run", events: []string{"start", "kill", "die"}, want: []string{"signaled"}},
		{name: "unobserved start", events: []string{"die"}, want: []string{"partial_run"}},
		{name: "signal without start", events: []string{"kill", "die"}, want: []string{"partial_run"}},
		{name: "die consumes the run", events: []string{"start", "die", "die"}, want: []string{"observed_run", "partial_run"}},
		{
			name:   "a signal does not outlive its run",
			events: []string{"start", "kill", "die", "start", "die"},
			want:   []string{"signaled", "observed_run"},
		},
		{
			name:   "restart after a signal is a new run",
			events: []string{"start", "kill", "start", "die"},
			want:   []string{"observed_run"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := NewEventSession()
			var got []string
			for _, event := range test.events {
				switch event {
				case "start":
					session.ObserveStart("c1")
				case "kill":
					session.ObserveSignal("c1")
				case "die":
					provenance := session.ObserveExit("c1")
					assert.Equal(t, "c1", provenance.InstanceID(), "minted for the instance that died")
					got = append(got, provenance.Label())
				}
			}
			assert.Equal(t, test.want, got)
		})
	}
}

func TestEventSessionIsOneContinuousStream(t *testing.T) {
	before := NewEventSession()
	before.ObserveStart("c1")
	before.ObserveSignal("c1")

	// A reconnect starts a new session. The signal seen on the old stream is
	// gone with it, so the death must not attribute as an observed run.
	after := NewEventSession()
	assert.Equal(t, "partial_run", after.ObserveExit("c1").Label())

	var missing *eventSession
	missing.ObserveStart("c1")
	missing.ObserveSignal("c1")
	assert.Equal(t, "partial_run", missing.ObserveExit("c1").Label(),
		"a nil session observes nothing")

	empty := NewEventSession()
	empty.ObserveStart("")
	assert.Equal(t, Provenance{}, empty.ObserveExit(""), "no instance, no provenance")
	assert.Equal(t, "unobserved", Provenance{}.Label())
	assert.Empty(t, Provenance{}.InstanceID())
}

// A session that did not come from NewEventSession tracks nothing, so it can
// never vouch for a whole run.
func TestZeroEventSessionIsInert(t *testing.T) {
	assert.NotPanics(t, func() {
		session := &eventSession{}
		session.ObserveStart("c1")
		session.ObserveSignal("c1")
		assert.Equal(t, "partial_run", session.ObserveExit("c1").Label())
	})
}

func TestEventSessionBoundsTrackedRuns(t *testing.T) {
	session := NewEventSession()
	for index := range maxObservedRuns {
		session.ObserveStart(fmt.Sprintf("c%d", index))
	}
	session.ObserveStart("overflow")
	assert.Len(t, session.runs, maxObservedRuns)
	assert.Equal(t, "partial_run", session.ObserveExit("overflow").Label(),
		"an untracked run fails toward never counting")
	assert.Equal(t, "observed_run", session.ObserveExit("c0").Label())
	session.ObserveStart("c0")
	assert.Equal(t, "observed_run", session.ObserveExit("c0").Label(),
		"a slot freed by a death is reusable")
}

func TestSealedTypesExportNoFields(t *testing.T) {
	for _, value := range []any{Cause{}, Provenance{}, Termination{}, eventSession{}} {
		typeOf := reflect.TypeOf(value)
		for index := range typeOf.NumField() {
			assert.Falsef(t, typeOf.Field(index).IsExported(),
				"%s.%s must not be settable outside this package", typeOf, typeOf.Field(index).Name)
		}
	}
}

// The counting kinds may be named only by the functions allowed to mint or
// read them, and a non-empty Cause, Provenance or Termination literal may be
// built only by its minters. A package-external caller cannot name any of
// them at all; this pins the in-package surface, across every production file
// and declaration, so a new helper cannot quietly mint a counting value, from
// a constant, an integer or a package-level var. Methods are keyed by
// receiver type.
var countingKindReferences = map[string][]string{
	"causeTenantWorkload":   {"ClassifyDeath", "Cause.Counts", "Cause.Label"},
	"provenanceObservedRun": {"ClassifyDeath", "eventSession.ObserveExit", "Provenance.Label"},
	"terminationExited":     {"Exited", "ClassifyDeath"},
}

var literalMinters = map[string][]string{
	"Cause":       {"Platform", "Maintenance", "Labels", "ClassifyDeath"},
	"Provenance":  {"eventSession.ObserveExit"},
	"Termination": {"Exited", "Gone"},
}

func TestCountingValuesMintedOnlyByTheirMinters(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var findings []string
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings = append(findings, mintingViolations(fset, parsed)...)
		checked++
	}
	require.GreaterOrEqual(t, checked, 3, "the guard must scan every production file")
	assert.Empty(t, findings)
}

func TestMintingGuardFires(t *testing.T) {
	const src = `package failurecause
var forged = Cause{kind: 1}
func helper() Cause { return Cause{kind: causeTenantWorkload} }
func (s *eventSession) ObserveExit() Provenance { return Provenance{kind: provenanceObservedRun} }
func (c Cause) Label() string { _ = provenanceObservedRun; return "" }
func ClassifyDeath() Cause { return Cause{kind: causeTenantWorkload} }
func shift(c Cause) Cause { c.kind = causeUnknown + 1; return c }
func rebind(p Provenance) Provenance { p.instanceID = "other"; return p }
func exitedAgain() Termination { return Termination{terminationExited} }
func zeroIsFine() Provenance { return Provenance{} }
`
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	findings := mintingViolations(fset, parsed)
	for _, want := range []string{
		"synthetic.go:2:14: package-level var builds a Cause",
		"synthetic.go:3:30: helper builds a Cause",
		"helper names causeTenantWorkload",
		"Cause.Label names provenanceObservedRun",
		"shift writes kind",
		"rebind writes instanceID",
		"exitedAgain builds a Termination",
		"exitedAgain names terminationExited",
	} {
		assert.True(t, slices.ContainsFunc(findings, func(f string) bool { return strings.Contains(f, want) }),
			"missing %q in %q", want, findings)
	}
	assert.Len(t, findings, 8, "the sanctioned shapes stay silent: %q", findings)
}

func mintingViolations(fset *token.FileSet, file *ast.File) []string {
	var findings []string
	inspect := func(key string, root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.Ident:
				allowed, tracked := countingKindReferences[typed.Name]
				if tracked && !slices.Contains(allowed, key) {
					findings = append(findings, fmt.Sprintf("%s: %s names %s",
						fset.Position(typed.Pos()), key, typed.Name))
				}
			case *ast.CompositeLit:
				typeName, ok := typed.Type.(*ast.Ident)
				if !ok || len(typed.Elts) == 0 {
					return true
				}
				if minters, sealed := literalMinters[typeName.Name]; sealed && !slices.Contains(minters, key) {
					where := key
					if where == "" {
						where = "package-level var"
					}
					findings = append(findings, fmt.Sprintf("%s: %s builds a %s",
						fset.Position(typed.Pos()), where, typeName.Name))
				}
			case *ast.AssignStmt:
				for _, lhs := range typed.Lhs {
					if selector, ok := lhs.(*ast.SelectorExpr); ok &&
						(selector.Sel.Name == "kind" || selector.Sel.Name == "instanceID") {
						findings = append(findings, fmt.Sprintf("%s: %s writes %s",
							fset.Position(selector.Pos()), key, selector.Sel.Name))
					}
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok {
			general, isGeneral := decl.(*ast.GenDecl)
			if isGeneral && general.Tok == token.CONST {
				continue // the const blocks declaring the kinds
			}
			inspect("", decl)
			continue
		}
		inspect(functionKey(function), function)
	}
	return findings
}

// functionKey is Name for a function and Receiver.Name for a method.
func functionKey(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + function.Name.Name
	}
	return function.Name.Name
}
