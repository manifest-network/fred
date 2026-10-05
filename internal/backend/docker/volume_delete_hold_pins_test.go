package docker

// AST pins over the held-deletion types (ENG-1117). Two invariants would be
// P1-class if broken, and Go cannot keep the fields of a same-package type
// private, so these rules keep them structural instead of conventional:
//
//  1. A residual settle is backed by a footprint parsed from a quota row: a
//     residual phase is built only by residualHoldPhase, from a
//     residualFootprintMB that only residualFootprintFromRow sizes, from an
//     xfsProjectQuotaRow that only a successful parseXfsReportRow marks
//     parsed. No other code writes the parsed bits or the residual phase
//     constant.
//     The row also records its resource, so an inode row cannot size a
//     footprint in blocks; only parseXfsReportRow writes it.
//  2. The anchor detach (project 0, PROJINHERIT cleared) is reachable only
//     from the condemned-volume type: condemnedXFSVolume is built only in
//     emptyAndRemoveCondemnedXFSVolume, after the delete stage, the project
//     authority and the volume root were attested; DetachCondemnedAnchor is
//     called only from its detachAnchor, which only its removeOptions lends,
//     which only its removeEntry passes to fstree.
//  3. Only a starting Backend defers first-time deletions to its hold
//     executor: Backend.Start is the only caller of the coordinator's
//     deferVolumeDeletesUntilExecutorRuns, the coordinator's constructor is
//     the only caller of a manager's DeferDeletesUntilExecutorRuns, and only
//     the XFS manager's DeferDeletesUntilExecutorRuns builds a deferral that
//     defers anything. A manager no Backend is starting therefore deletes
//     inline and never waits for an executor that does not exist.
//
// The older single-constructor rules (cause, outcome, hold record), the only
// callers of the cleanup attempt, the allowlisted holdable sites, and the
// only caller of the live-volume subtree removal (removeManagedVolumeSubtree,
// which wraps fstree.RemoveBeneath inside a tenant's live volume) are pinned
// here too. A literal's elided type cannot be resolved without type
// checking, so the parsed bits are pinned by their unique field names
// instead, which also catches an elided literal. Each rule has a violating
// fixture below, and each production scan must see the rule's allowed use.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deleteHoldPin struct {
	name    string
	match   func(ast.Node) bool
	allowed []string // function keys ("Recv.Method" or "Func"); nil allows none
}

func typedLiteral(typeName string) func(ast.Node) bool {
	return func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || len(literal.Elts) == 0 {
			return false // a zero value carries no reason, phase, bit or authority
		}
		ident, ok := literal.Type.(*ast.Ident)
		return ok && ident.Name == typeName
	}
}

func keyedField(field string) func(ast.Node) bool {
	return func(node ast.Node) bool {
		kv, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return false
		}
		key, ok := kv.Key.(*ast.Ident)
		return ok && key.Name == field
	}
}

func assignedField(field string) func(ast.Node) bool {
	return func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if selector, ok := lhs.(*ast.SelectorExpr); ok && selector.Sel.Name == field {
					return true
				}
			}
		case *ast.IncDecStmt:
			selector, ok := n.X.(*ast.SelectorExpr)
			return ok && selector.Sel.Name == field
		}
		return false
	}
}

// writtenIdent matches name used as a value written somewhere: a composite
// literal field, an assignment, or a declaration. Comparisons and case
// labels only read it.
func writtenIdent(name string) func(ast.Node) bool {
	is := func(expr ast.Expr) bool {
		ident, ok := expr.(*ast.Ident)
		return ok && ident.Name == name
	}
	return func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.KeyValueExpr:
			return is(n.Value)
		case *ast.AssignStmt:
			return slices.ContainsFunc(n.Rhs, is)
		case *ast.ValueSpec:
			return slices.ContainsFunc(n.Values, is)
		}
		return false
	}
}

func selectorNamed(name string) func(ast.Node) bool {
	return func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == name
	}
}

func callOf(name string) func(ast.Node) bool {
	return func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return false
		}
		ident, ok := call.Fun.(*ast.Ident)
		return ok && ident.Name == name
	}
}

var deleteHoldPins = []deleteHoldPin{
	{"hold cause literal", typedLiteral("xfsDeleteHoldCause"), []string{"holdable"}},
	{"cleanup outcome literal", typedLiteral("deleteStageOutcome"), []string{"classifyXFSDeleteStageCleanup"}},
	{"hold record literal", typedLiteral("xfsDeleteHold"), []string{"newXFSDeleteHold"}},
	{"residual footprint literal", typedLiteral("residualFootprintMB"), []string{"residualFootprintFromRow"}},
	{"footprint parsed-row bit set", keyedField("fromParsedRow"), []string{"residualFootprintFromRow"}},
	{"footprint parsed-row bit assigned", assignedField("fromParsedRow"), nil},
	{"quota row literal", typedLiteral("xfsProjectQuotaRow"), []string{"parseXfsReportRow"}},
	{"quota row parsed bit set", keyedField("parsedFromReport"), []string{"parseXfsReportRow"}},
	{"quota row parsed bit assigned", assignedField("parsedFromReport"), nil},
	{"quota row resource set", keyedField("quotaResource"), []string{"parseXfsReportRow"}},
	{"quota row resource assigned", assignedField("quotaResource"), nil},
	{"live subtree removal", callOf("removeManagedVolumeSubtree"), []string{"launchVolume.removeWritablePaths"}},
	{"hold phase literal", typedLiteral("xfsDeleteHoldPhase"),
		[]string{"removalHoldPhase", "unsizedHoldPhase", "residualHoldPhase"}},
	{"residual phase written", writtenIdent("holdPhaseResidual"), []string{"residualHoldPhase"}},
	{"condemned volume literal", typedLiteral("condemnedXFSVolume"),
		[]string{"xfsVolumeManager.emptyAndRemoveCondemnedXFSVolume"}},
	{"anchor detach call", selectorNamed("DetachCondemnedAnchor"), []string{"condemnedXFSVolume.detachAnchor"}},
	{"anchor detach lent", selectorNamed("detachAnchor"), []string{"condemnedXFSVolume.removeOptions"}},
	{"cut options built", selectorNamed("removeOptions"), []string{"condemnedXFSVolume.removeEntry"}},
	{"condemned removal", selectorNamed("removeEntry"), []string{"removeCondemnedXFSEntry"}},
	{"cleanup attempt", selectorNamed("cleanupXFSDeleteStageWith"),
		[]string{"xfsVolumeManager.RetryHeldVolumeDelete", "xfsVolumeManager.firstDeleteAttempt"}},
	{"holdable site", callOf("holdable"), []string{
		"stopHold", "recoveredXFSDeleteHold",
		"xfsVolumeManager.runXFSDeleteStageCleanup", "xfsVolumeManager.emptyAndRemoveCondemnedXFSVolume",
		"xfsVolumeManager.firstDeleteAttempt", "xfsVolumeManager.registeredHoldOutcome",
	}},
	{"start's delete deferral", selectorNamed("deferVolumeDeletesUntilExecutorRuns"), []string{"Backend.Start"}},
	{"manager delete deferral", selectorNamed("DeferDeletesUntilExecutorRuns"),
		[]string{"newBackgroundMaintenanceCoordinator"}},
	{"delete deferral literal", typedLiteral("volumeDeleteDeferral"),
		[]string{"xfsVolumeManager.DeferDeletesUntilExecutorRuns"}},
	// A residual hold settles its caller only after an admission publication
	// counted it (make before break): every phase write goes through the one
	// setter that starts a residual entry unacknowledged, and only the
	// publication may acknowledge one.
	{"hold phase assigned", assignedField("phase"), []string{"xfsVolumeManager.setHoldPhaseLocked"}},
	{"residual sequence assigned", assignedField("residualSeq"), []string{"xfsVolumeManager.setHoldPhaseLocked"}},
	{"residual acknowledgment assigned", assignedField("residualAcknowledged"),
		[]string{"xfsVolumeManager.setHoldPhaseLocked", "xfsVolumeManager.AcknowledgeResidualAccounting"}},
	{"residual acknowledgment set", keyedField("residualAcknowledged"), nil},
	{"residual accounting acknowledged", selectorNamed("AcknowledgeResidualAccounting"),
		[]string{"Backend.publishRetainedDiskLocked", "projectVolumeRead"}},
	// The derived settlement bit every settlement reader consumes is built
	// only from the hold, by the classifier and the hold's view.
	{"settlement bit set", keyedField("callerSettled"),
		[]string{"classifyXFSDeleteStageCleanup", "xfsDeleteHold.view"}},
	{"settlement bit assigned", assignedField("callerSettled"), nil},
}

// funcKey names a function "Func", or a method "Recv.Method".
func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// deleteHoldPinFindings applies every pin to files: violations by pin name,
// and how many uses each pin saw anywhere.
func deleteHoldPinFindings(fset *token.FileSet, files []*ast.File) (map[string][]string, map[string]int) {
	violations := map[string][]string{}
	seen := map[string]int{}
	visit := func(key string, root ast.Node) {
		ast.Inspect(root, func(node ast.Node) bool {
			if node == nil {
				return false
			}
			for _, pin := range deleteHoldPins {
				if !pin.match(node) {
					continue
				}
				seen[pin.name]++
				if !slices.Contains(pin.allowed, key) {
					where := key
					if where == "" {
						where = "package scope"
					}
					violations[pin.name] = append(violations[pin.name],
						fset.Position(node.Pos()).String()+" (in "+where+")")
				}
			}
			return true
		})
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					visit(funcKey(d), d.Body)
				}
			case *ast.GenDecl:
				visit("", d)
			}
		}
	}
	return violations, seen
}

func TestDeleteHoldTypesHaveSingleConstructors(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		files = append(files, file)
	}
	violations, seen := deleteHoldPinFindings(fset, files)
	for _, pin := range deleteHoldPins {
		for _, violation := range violations[pin.name] {
			t.Errorf("%s: %s outside %v", violation, pin.name, pin.allowed)
		}
		if pin.allowed != nil {
			assert.Positive(t, seen[pin.name], "%s: the guard matched nothing; its allowed use moved", pin.name)
		}
	}
}

// deleteHoldPinFixture violates every pin once, at package scope or inside a
// function no pin allows.
const deleteHoldPinFixture = `package docker
var packageLevel = residualFootprintMB{mb: 1, fromParsedRow: true}
func misuse(v condemnedXFSVolume, x *xfsVolumeManager, row xfsProjectQuotaRow, f residualFootprintMB) {
	_ = xfsDeleteHoldCause{reason: holdReasonDeadline}
	_ = deleteStageOutcome{kind: deleteStageHeld}
	_ = xfsDeleteHold{reason: holdReasonDeadline}
	_ = []residualFootprintMB{{mb: 1, fromParsedRow: true}}
	f.fromParsedRow = true
	_ = xfsProjectQuotaRow{parsedFromReport: true, quotaResource: xfsQuotaBlocks}
	row.parsedFromReport = true
	row.quotaResource = xfsQuotaBlocks
	_ = removeManagedVolumeSubtree(nil, "", "", "")
	_ = xfsDeleteHoldPhase{kind: holdPhaseUnsized}
	view := volumeDeleteHoldView{phase: holdPhaseResidual}
	_ = condemnedXFSVolume{device: 1}
	_ = v.attributes.DetachCondemnedAnchor(0, 0)
	_ = v.detachAnchor(0)
	_ = v.removeOptions()
	_, _ = v.removeEntry(nil, fstree.Name{})
	_ = x.cleanupXFSDeleteStageWith(nil, xfsDeleteStageName{}, nil, nil, nil)
	_ = holdable(holdReasonDeadline, nil)
	_ = x.deferVolumeDeletesUntilExecutorRuns()
	_ = x.DeferDeletesUntilExecutorRuns()
	_ = volumeDeleteDeferral{release: func() {}}
	_ = view
	hold := &xfsDeleteHold{}
	hold.phase = xfsDeleteHoldPhase{}
	hold.residualSeq++
	hold.residualAcknowledged = true
	_ = struct{ residualAcknowledged bool }{residualAcknowledged: true}
	_ = x.AcknowledgeResidualAccounting(nil)
	settled := volumeDeleteHoldView{callerSettled: true}
	settled.callerSettled = true
	_ = settled
}
`

func TestDeleteHoldPinsFireOnViolations(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", deleteHoldPinFixture, parser.SkipObjectResolution)
	require.NoError(t, err)
	violations, _ := deleteHoldPinFindings(fset, []*ast.File{file})
	for _, pin := range deleteHoldPins {
		assert.NotEmpty(t, violations[pin.name], "%s: the violating fixture was not flagged", pin.name)
	}
	assert.Contains(t, strings.Join(violations["residual footprint literal"], ";"), "package scope",
		"a package-level initializer is scanned too")
}
