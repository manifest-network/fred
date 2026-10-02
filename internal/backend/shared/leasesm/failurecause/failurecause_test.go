package failurecause

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"slices"
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
	provenances := map[provenanceKind]Provenance{
		provenanceUnobserved:  {},
		provenancePartialRun:  {kind: provenancePartialRun},
		provenanceSignaled:    {kind: provenanceSignaled},
		provenanceObservedRun: {kind: provenanceObservedRun},
		provenanceSentinel:    {kind: provenanceSentinel},
		255:                   {kind: 255},
	}
	terminations := map[string]Termination{
		"unknown": {kind: terminationUnknown},
		"exited":  Exited(),
		"gone":    Gone(),
		"bogus":   {kind: 255},
	}
	for provenanceName, provenance := range provenances {
		for terminationName, termination := range terminations {
			name := fmt.Sprintf("%s/%s", provenance.Label(), terminationName)
			if provenanceName >= provenanceSentinel {
				name = fmt.Sprintf("kind%d/%s", provenanceName, terminationName)
			}
			t.Run(name, func(t *testing.T) {
				got := ClassifyDeath(provenance, termination)
				switch {
				case provenanceName == provenanceSignaled || terminationName == "gone":
					assert.Equal(t, "disruption", got.Label())
				case provenanceName == provenanceObservedRun && terminationName == "exited":
					assert.Equal(t, "tenant_workload", got.Label())
					assert.True(t, got.Counts())
				default:
					assert.Equal(t, "unknown", got.Label())
				}
				if got.Counts() {
					assert.Equal(t, provenanceObservedRun, provenanceName,
						"only a fully observed, unsignaled run may count")
					assert.Equal(t, "exited", terminationName,
						"only an observed exit may count")
				}
			})
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
					got = append(got, session.ObserveExit("c1").Label())
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

	var missing *EventSession
	missing.ObserveStart("c1")
	missing.ObserveSignal("c1")
	assert.Equal(t, "partial_run", missing.ObserveExit("c1").Label(),
		"a nil session observes nothing")

	empty := NewEventSession()
	empty.ObserveStart("")
	assert.Equal(t, "partial_run", empty.ObserveExit("").Label())
	assert.Equal(t, "unobserved", Provenance{}.Label())
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
	for _, value := range []any{Cause{}, Provenance{}, Termination{}, EventSession{}} {
		typeOf := reflect.TypeOf(value)
		for index := range typeOf.NumField() {
			assert.Falsef(t, typeOf.Field(index).IsExported(),
				"%s.%s must not be settable outside this package", typeOf, typeOf.Field(index).Name)
		}
	}
}

// The counting kinds may be named only by the functions allowed to mint or
// read them. A package-external caller cannot name them at all; this pins the
// in-package surface so a new helper cannot quietly mint a counting cause.
// Methods are keyed by receiver type.
var countingKindReferences = map[string][]string{
	"causeTenantWorkload":   {"ClassifyDeath", "Cause.Counts", "Cause.Label"},
	"provenanceObservedRun": {"ClassifyDeath", "EventSession.ObserveExit", "Provenance.Label"},
}

func TestCountingKindsNamedOnlyByTheirMinters(t *testing.T) {
	fset := token.NewFileSet()
	var findings []string
	for _, file := range []string{"failurecause.go", "provenance.go"} {
		parsed, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings = append(findings, countingKindViolations(fset, parsed)...)
	}
	assert.Empty(t, findings)
}

func TestCountingKindGuardFires(t *testing.T) {
	const src = `package failurecause
func helper() Cause { return Cause{kind: causeTenantWorkload} }
func (s *EventSession) ObserveExit() Provenance { return Provenance{kind: provenanceObservedRun} }
func (c Cause) Label() string { _ = provenanceObservedRun; return "" }
func ClassifyDeath() Cause { return Cause{kind: causeTenantWorkload} }
`
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	findings := countingKindViolations(fset, parsed)
	require.Len(t, findings, 2, "both forged references must be reported: %v", findings)
	assert.Contains(t, findings[0], "helper names causeTenantWorkload")
	assert.Contains(t, findings[1], "Cause.Label names provenanceObservedRun",
		"a method with an allowed name on the wrong receiver must still be reported")
}

func countingKindViolations(fset *token.FileSet, file *ast.File) []string {
	var findings []string
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue // the const block declaring the kinds
		}
		key := functionKey(function)
		ast.Inspect(function, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			allowed, tracked := countingKindReferences[ident.Name]
			if tracked && !slices.Contains(allowed, key) {
				findings = append(findings, fmt.Sprintf("%s: %s names %s",
					fset.Position(ident.Pos()), key, ident.Name))
			}
			return true
		})
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
