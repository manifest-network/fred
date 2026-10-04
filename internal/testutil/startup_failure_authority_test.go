package testutil

// This repository-level guard pins who may mint the facts that turn a startup
// failure into a definite provision failure (ENG-1125). The definite path ends
// a durable attempt, and its attribution can count toward closing a paying
// lease, so every link of the chain has exactly one producer:
//
//   - the settled-launch receipt (newSettledLaunch) only in the launch
//     dispatch, after the launch's exchange and journal row settled;
//   - the startup finding (newStartupFailure) only where an observation seals
//     its watch (startupObservationOf), and the two observations only in the
//     provision workflow;
//   - the conclusion (concludeStartupFailure) only in the provision workflow,
//     and it alone accepts a finding (acceptStartupFailure), admits a rollback
//     (admitStartupRollback) and rolls back (rollbackStartupFailure), in that
//     order;
//   - the sealed shared account (shared.NewOperationStartupFailure) only in
//     newStartupFailure, and the classifier evidence
//     (shared.NewOperationStartupFailed) only in confirmStartupFailure, after
//     the classifier's own positive reads;
//   - the live-death ledger: written only by the event loop's recorder, both
//     the deaths (recordLiveDeath) and the stream marks
//     (markLiveDeathStream), never by the reader itself, which may wait on
//     nothing but its stream (ENG-799); read only by newStartupFailure
//     (awaitLiveDeath) and the Ready-entry re-dispatch (takeLiveDeaths);
//     reached through b.liveDeaths only by those four entry points; and its
//     fields and unexported helpers named only in its own file;
//   - the paths a live provenance travels: the recorder
//     (recordLiveContainerDeaths) and the dispatcher
//     (dispatchLiveContainerDeaths) are started only by the event loop, which
//     alone hands them their queues, and one death is dispatched
//     (dispatchLiveContainerDeath) only by the dispatcher and the Ready-entry
//     re-dispatch, so a provenance that left the loop, through a sealed
//     startup failure or a ledger read, cannot be fed back in;
//   - the startup health ledger behind the sticky health rule: written
//     (recordPassedHealth) only by a startup watch's memory
//     (startupMemory.remember), from a pass it inspected; its fields are named
//     only in its own file;
//   - a launch's degradation sets (quiescedVolumes.degradations,
//     imageSetup.Degradations) only grow: no assignment to either field, no
//     literal keying one outside the receipt's constructor, and the set's bits
//     named only in its own file, so a marked degradation cannot be reset
//     before the receipt reads it.
//
// A zero receipt, finding or plan is invalid (a nil state), but a valid one
// could still be forged in its package from a state value. So the state types
// themselves (settledLaunchState, startupFailureState, startupRollbackState,
// and package shared's operationStartupFailureState and
// operationStartupFailedState) are named only at their declaration and in
// their one constructor, which rules out a literal, new(), a type alias or an
// elided literal anywhere else. Package shared's startup evidence and failure
// kinds are named only where they are declared, minted and matched. Every rule
// is proven to fire by TestStartupFailureAuthorityGuardsFire.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	sharedImportPath           = "github.com/manifest-network/fred/internal/backend/shared"
	sharedDir                  = "internal/backend/shared"
	dockerDir                  = "internal/backend/docker"
	startupFailureFile         = "internal/backend/docker/startup_failure.go"
	startupObservationFile     = "internal/backend/docker/startup_observation.go"
	settledLaunchFile          = "internal/backend/docker/settled_launch.go"
	provisionFile              = "internal/backend/docker/provision.go"
	launchDispatchFile         = "internal/backend/docker/storage_mutation_guard.go"
	liveDeathLedgerFile        = "internal/backend/docker/live_death_ledger.go"
	startupHealthLedgerFile    = "internal/backend/docker/startup_health_ledger.go"
	sharedStartupFailureFile   = "internal/backend/shared/operation_startup_failure.go"
	sharedHandoffFile          = "internal/backend/shared/operation_handoff.go"
	sharedPhysicalOutcomeFile  = "internal/backend/shared/physical_outcome.go"
	startupFailureConfirmSite  = "Backend.confirmStartupFailure"
	startupFailureMintSite     = "Backend.newStartupFailure"
	startupObservationSealSite = "Backend.startupObservationOf"
	startupConclusionSite      = "Backend.concludeStartupFailure"
	startupRollbackAdmitSite   = "Backend.admitStartupRollback"
	provisionWorkflowSite      = "Backend.doProvisionPhysical"
	settledLaunchMintSite      = "newSettledLaunch"
	launchDispatchSite         = "newVolumeLaunchCoordinator"
	startupDeathRedispatchSite = "Backend.redispatchStartupDeaths"
	liveDeathRecorderSite      = "Backend.recordLiveContainerDeaths"
	liveDeathDispatcherSite    = "Backend.dispatchLiveContainerDeaths"
	containerEventLoopSite     = "Backend.runContainerEventLoop"
	startupMemoryRememberSite  = "startupMemory.remember"
	startupPassInspectionSite  = "Backend.inspectStartupCohort"
)

// typeSite and valueSite name one top-level declaration as a site: a type
// declaration by its type, and a const or var declaration by its first name.
func typeSite(file, name string) attributionSite  { return attributionSite{file, "type " + name} }
func valueSite(file, name string) attributionSite { return attributionSite{file, "value " + name} }

// startupAuthorityRule confines one name to its sites. A name declared by a
// function is also allowed at its own declaration.
type startupAuthorityRule struct {
	name  string
	sites []attributionSite
	what  string
}

// Unqualified names inside internal/backend/docker.
var dockerStartupRules = []startupAuthorityRule{
	{"newSettledLaunch", []attributionSite{{launchDispatchFile, launchDispatchSite}}, "the settled-launch receipt"},
	{"newStartupFailure", []attributionSite{{startupObservationFile, startupObservationSealSite}}, "the startup finding"},
	{"observeStartup", []attributionSite{{provisionFile, provisionWorkflowSite}}, "a provision's startup observation"},
	{"observeRejectedLaunch", []attributionSite{{provisionFile, provisionWorkflowSite}}, "a provision's startup observation"},
	{"concludeStartupFailure", []attributionSite{{provisionFile, provisionWorkflowSite}}, "the startup conclusion"},
	{"acceptStartupFailure", []attributionSite{{startupFailureFile, startupConclusionSite}}, "a startup finding's acceptance"},
	{"admitStartupRollback", []attributionSite{{startupFailureFile, startupConclusionSite}}, "a startup rollback's admission"},
	{"rollbackStartupFailure", []attributionSite{{startupFailureFile, startupConclusionSite}}, "a startup rollback"},
	{"recordLiveDeath", []attributionSite{{terminalBudgetEventsFile, liveDeathRecorderSite}}, "a live-death ledger write"},
	{"markLiveDeathStream", []attributionSite{{terminalBudgetEventsFile, liveDeathRecorderSite}}, "a live-death ledger write"},
	{"awaitLiveDeath", []attributionSite{{startupFailureFile, startupFailureMintSite}}, "a live-death ledger read"},
	{"takeLiveDeaths", []attributionSite{{terminalBudgetEventsFile, startupDeathRedispatchSite}}, "a live-death ledger read"},
	{"recordLiveContainerDeaths", []attributionSite{{terminalBudgetEventsFile, containerEventLoopSite}}, "the live-death recorder"},
	{"dispatchLiveContainerDeaths", []attributionSite{{terminalBudgetEventsFile, containerEventLoopSite}}, "the live-death dispatcher"},
	{"dispatchLiveContainerDeath", []attributionSite{
		{terminalBudgetEventsFile, liveDeathDispatcherSite}, {terminalBudgetEventsFile, startupDeathRedispatchSite},
	}, "a live death's dispatch"},
	{"recordPassedHealth", []attributionSite{{startupObservationFile, startupMemoryRememberSite}}, "a startup health ledger write"},
	{"remember", []attributionSite{{startupObservationFile, startupPassInspectionSite}}, "a startup pass record"},
}

// Package-qualified names of package shared, and their one docker site.
var sharedStartupRules = []startupAuthorityRule{
	{"NewOperationStartupFailure", []attributionSite{{startupFailureFile, startupFailureMintSite}}, "the sealed startup failure"},
	{"NewOperationStartupFailed", []attributionSite{{startupFailureFile, startupFailureConfirmSite}}, "startup failure evidence"},
}

// Non-empty literals of these docker types are minted only by their
// constructors.
var startupLiteralSites = map[string]attributionSite{
	"settledLaunch":   {settledLaunchFile, settledLaunchMintSite},
	"startupFailure":  {startupFailureFile, startupFailureMintSite},
	"startupRollback": {startupFailureFile, startupRollbackAdmitSite},
}

// Sealed state types are named only in their own type declaration, the
// declaration of the value type that wraps them, and their one constructor:
// in package docker and in package shared respectively. Package shared's
// startup kinds are named only where they are declared, minted and matched.
var dockerSealedStateIdents = []startupAuthorityRule{
	{"settledLaunchState", []attributionSite{
		typeSite(settledLaunchFile, "settledLaunch"), typeSite(settledLaunchFile, "settledLaunchState"),
		{settledLaunchFile, settledLaunchMintSite},
	}, "the settled-launch receipt's state"},
	{"startupFailureState", []attributionSite{
		typeSite(startupFailureFile, "startupFailure"), typeSite(startupFailureFile, "startupFailureState"),
		{startupFailureFile, startupFailureMintSite},
	}, "the startup finding's state"},
	{"startupRollbackState", []attributionSite{
		typeSite(startupFailureFile, "startupRollback"), typeSite(startupFailureFile, "startupRollbackState"),
		{startupFailureFile, startupRollbackAdmitSite},
	}, "the startup rollback plan's state"},
}

var sharedSealedIdents = []startupAuthorityRule{
	{"operationStartupFailureState", []attributionSite{
		typeSite(sharedStartupFailureFile, "OperationStartupFailure"),
		typeSite(sharedStartupFailureFile, "operationStartupFailureState"),
		{sharedStartupFailureFile, "NewOperationStartupFailure"},
	}, "the sealed startup failure's state"},
	{"operationStartupFailedState", []attributionSite{
		typeSite(sharedStartupFailureFile, "OperationStartupFailed"),
		typeSite(sharedStartupFailureFile, "operationStartupFailedState"),
		{sharedStartupFailureFile, "NewOperationStartupFailed"},
	}, "startup failure evidence's state"},
	{"operationPhysicalEvidenceStartupFailed", []attributionSite{
		valueSite(sharedPhysicalOutcomeFile, "operationPhysicalEvidenceStartupFailed"),
		{sharedStartupFailureFile, "NewOperationStartupFailed"},
		{sharedPhysicalOutcomeFile, "validateOperationPhysicalEvidence"},
		{sharedHandoffFile, "OperationSettlement.ExecuteOperation"},
		{sharedHandoffFile, "OperationSettlement.RecoverOperationExecution"},
		{sharedHandoffFile, "OperationSettlement.CleanupRecoveredOperation"},
	}, "the startup failure evidence kind"},
	{"operationExecutionAttestedStartupFailure", []attributionSite{
		valueSite(sharedHandoffFile, "operationExecutionAttestedStartupFailure"),
		{sharedHandoffFile, "OperationSettlement.ExecuteOperation"},
		{sharedHandoffFile, "OperationExecutionFailure.Valid"},
		{sharedHandoffFile, "OperationExecutionFailure.StartupFailure"},
	}, "the startup failure outcome kind"},
}

// liveDeathLedgerEntryPoints are the only names that may follow b.liveDeaths.
var liveDeathLedgerEntryPoints = []string{"recordLiveDeath", "markLiveDeathStream", "awaitLiveDeath", "takeLiveDeaths"}

// liveDeathLedgerInternals are named only in the ledger's own file.
var liveDeathLedgerInternals = []string{
	"deathsByID", "deathOrder", "deathRecorded", "streamConnected", "takeLocked", "notifyLocked",
}

// startupHealthLedgerInternals are named only in the startup health ledger's
// own file.
var startupHealthLedgerInternals = []string{"healthyByID", "healthyRing", "healthyNext"}

// launchDegradationFields hold a launch's degradation set (quiescedVolumes and
// imageSetup). The set only grows: a field of it is never assigned, only
// added to through add or addAll, and only the receipt's constructor keys one
// in a literal (to copy it into the receipt). launchDegradationInternals are
// the set's bits, named only in its own file.
var (
	launchDegradationFields    = []string{"degradations", "Degradations"}
	launchDegradationInternals = []string{"degradationBits"}
)

// sealedInternalOwner reports the one file that may name an internal of a
// sealed docker type, and what that type is.
func sealedInternalOwner(name string) (file, what string, sealed bool) {
	switch {
	case slices.Contains(liveDeathLedgerInternals, name):
		return liveDeathLedgerFile, "live-death ledger", true
	case slices.Contains(startupHealthLedgerInternals, name):
		return startupHealthLedgerFile, "startup health ledger", true
	case slices.Contains(launchDegradationInternals, name):
		return settledLaunchFile, "launch degradation set", true
	default:
		return "", "", false
	}
}

func TestStartupFailureAuthorityIsSealed(t *testing.T) {
	root := repoRoot(t)
	var findings []string
	for _, dir := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, dir), root, func(rel string, file *ast.File, fset *token.FileSet) {
			findings = append(findings, startupFailureAuthorityFindings(rel, file, fset)...)
		})
	}
	if len(findings) > 0 {
		t.Errorf("startup failure authority escaped its sealed sites (ENG-1125):\n  %s",
			strings.Join(findings, "\n  "))
	}
}

func TestStartupFailureAuthorityGuardsFire(t *testing.T) {
	const sharedImport = `import "github.com/manifest-network/fred/internal/backend/shared"
`
	tests := []struct {
		name string
		rel  string
		src  string
		want string
	}{
		{"receipt minted outside the launch dispatch", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { _ = newSettledLaunch(nil, o) }`, "names newSettledLaunch"},
		{"receipt minted elsewhere in the dispatch file", launchDispatchFile,
			`package docker
func (m *storageMutations) launch() { _ = newSettledLaunch(nil, o) }`, "names newSettledLaunch"},
		{"receipt constructor as a value", "internal/backend/docker/volume_launch.go",
			`package docker
var mint = newSettledLaunch`, "names newSettledLaunch"},
		{"receipt forged by a literal", provisionFile,
			`package docker
var l = settledLaunch{state: nil}`, "builds a settledLaunch"},
		{"receipt state built outside its constructor", launchDispatchFile,
			`package docker
func newVolumeLaunchCoordinator() { _ = &settledLaunchState{} }`, "names settledLaunchState"},
		{"receipt state allocated by new", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { var l settledLaunch; l.state = new(settledLaunchState) }`,
			"names settledLaunchState"},
		{"receipt state through a type alias", settledLaunchFile,
			`package docker
type forged = settledLaunchState`, "names settledLaunchState"},
		{"finding minted outside the observation seal", startupObservationFile,
			`package docker
func (b *Backend) observeStartup() { _, _ = b.newStartupFailure(nil, nil, settledLaunch{}, nil, startupWatch{}) }`,
			"names newStartupFailure"},
		{"finding forged by a literal", startupObservationFile,
			`package docker
func (b *Backend) startupObservationOf() { _ = startupFailure{state: nil} }`, "builds a startupFailure"},
		{"finding state built outside its constructor", startupFailureFile,
			`package docker
func (b *Backend) rollbackStartupFailure() { _ = &startupFailureState{} }`, "names startupFailureState"},
		{"finding state in an elided literal", startupFailureFile,
			`package docker
func (b *Backend) concludeStartupFailure() { _ = []*startupFailureState{{}} }`, "names startupFailureState"},
		{"observation outside the provision workflow", "internal/backend/docker/restore.go",
			`package docker
func (b *Backend) doRestorePhysical() { _ = b.observeStartup(ctx, m, l, c, nil) }`, "names observeStartup"},
		{"rejected launch observed outside the provision workflow", startupObservationFile,
			`package docker
func (b *Backend) watchStartup() { _ = b.observeRejectedLaunch(ctx, m, l, c, e, nil) }`, "names observeRejectedLaunch"},
		{"conclusion outside the provision workflow", "internal/backend/docker/physical_execution.go",
			`package docker
func (b *Backend) executeProvisionWork() { _, _ = b.concludeStartupFailure(ctx, m, f, nil) }`,
			"names concludeStartupFailure"},
		{"finding accepted outside the conclusion", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { _, _ = mutations.acceptStartupFailure(f) }`, "names acceptStartupFailure"},
		{"rollback admitted outside the conclusion", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { _, _ = b.admitStartupRollback(ctx, m, a) }`, "names admitStartupRollback"},
		{"rollback run outside the conclusion", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { _ = b.rollbackStartupFailure(ctx, m, r, nil) }`, "names rollbackStartupFailure"},
		{"rollback plan forged by a literal", startupFailureFile,
			`package docker
func (b *Backend) concludeStartupFailure() { _ = startupRollback{state: nil} }`, "builds a startupRollback"},
		{"rollback plan state built outside its admission", startupFailureFile,
			`package docker
func (b *Backend) rollbackStartupFailure() { _ = &startupRollbackState{} }`, "names startupRollbackState"},
		{"sealed account minted outside the finding constructor", "internal/backend/docker/recover.go",
			"package docker\n" + sharedImport + `var f, _ = shared.NewOperationStartupFailure(shared.OperationStartupFailureTerms{})`,
			"names shared.NewOperationStartupFailure"},
		{"evidence minted outside the classifier's confirmation", "internal/backend/docker/physical_execution.go",
			"package docker\n" + sharedImport +
				`func (b *Backend) classifyOperationPhysical() { _, _ = shared.NewOperationStartupFailed(s, f) }`,
			"names shared.NewOperationStartupFailed"},
		{"evidence minted through an alias", startupFailureFile,
			`package docker
import sh "github.com/manifest-network/fred/internal/backend/shared"
func (b *Backend) rollbackStartupFailure() { _, _ = sh.NewOperationStartupFailed(s, f) }`,
			"names shared.NewOperationStartupFailed"},
		{"evidence minted inside package shared", sharedHandoffFile,
			`package shared
func f() { _, _ = NewOperationStartupFailed(s, x) }`, "names shared.NewOperationStartupFailed"},
		{"shared account state forged in package shared", sharedHandoffFile,
			`package shared
func f() { _ = OperationStartupFailure{state: &operationStartupFailureState{}} }`,
			"names operationStartupFailureState"},
		{"shared evidence state forged in its own file", sharedStartupFailureFile,
			`package shared
func (f OperationStartupFailure) Kind() { _ = new(operationStartupFailedState) }`,
			"names operationStartupFailedState"},
		{"startup evidence kind minted outside its constructor", sharedPhysicalOutcomeFile,
			`package shared
func NewOperationExactAbsent() { _ = OperationPhysicalEvidence{kind: operationPhysicalEvidenceStartupFailed} }`,
			"names operationPhysicalEvidenceStartupFailed"},
		{"startup outcome kind minted outside ExecuteOperation", sharedHandoffFile,
			`package shared
func (s *OperationSettlement) RefuseOperationExecution() { _ = OperationExecutionFailure{kind: operationExecutionAttestedStartupFailure} }`,
			"names operationExecutionAttestedStartupFailure"},
		{"ledger written by the reader", terminalBudgetReaderFile,
			`package docker
func (r containerEventReader) consume() { r.ledger.recordLiveDeath(p) }`, "names recordLiveDeath"},
		{"ledger stream marked by the reader", terminalBudgetReaderFile,
			`package docker
func (r containerEventReader) consume() { r.ledger.markLiveDeathStream(true) }`, "names markLiveDeathStream"},
		{"ledger written outside the recorder", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.liveDeaths.recordLiveDeath(p) }`, "names recordLiveDeath"},
		{"ledger stream marked by the dispatcher", terminalBudgetEventsFile,
			`package docker
func (b *Backend) dispatchLiveContainerDeath() { b.liveDeaths.markLiveDeathStream(true) }`,
			"names markLiveDeathStream"},
		{"ledger stream marked by the loop, out of order with the deaths", terminalBudgetEventsFile,
			`package docker
func (b *Backend) runContainerEventLoop() { b.liveDeaths.markLiveDeathStream(false) }`,
			"names markLiveDeathStream"},
		{"recorder fed outside the loop", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.recordLiveContainerDeaths(replayed, dispatch, overflow) }`,
			"names recordLiveContainerDeaths"},
		{"recorder fed elsewhere in the loop's file", terminalBudgetEventsFile,
			`package docker
func (b *Backend) redispatchStartupDeaths() { go b.recordLiveContainerDeaths(replayed, dispatch, overflow) }`,
			"names recordLiveContainerDeaths"},
		{"dispatcher fed outside the loop", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.dispatchLiveContainerDeaths(replayed) }`, "names dispatchLiveContainerDeaths"},
		{"a sealed startup failure's provenance dispatched again", startupFailureFile,
			`package docker
func (b *Backend) concludeStartupFailure() { b.dispatchLiveContainerDeath(sealed.Provenance()) }`,
			"names dispatchLiveContainerDeath"},
		{"a death dispatched by the recorder, past the dispatcher's queue", terminalBudgetEventsFile,
			`package docker
func (b *Backend) recordLiveContainerDeaths() { b.dispatchLiveContainerDeath(death) }`,
			"names dispatchLiveContainerDeath"},
		{"a death's dispatch as a method value", "internal/backend/docker/recover.go",
			`package docker
var dispatch = (*Backend).dispatchLiveContainerDeath`, "names dispatchLiveContainerDeath"},
		{"ledger read by the sweep", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { _ = b.liveDeaths.takeLiveDeaths(nil) }`, "names takeLiveDeaths"},
		{"ledger awaited outside the finding constructor", startupObservationFile,
			`package docker
func (b *Backend) observeStartup() { _, _ = b.liveDeaths.awaitLiveDeath(ctx, "c", 0) }`, "names awaitLiveDeath"},
		{"ledger fields read outside the ledger", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { _ = len(deathsByID) }`, "names deathsByID"},
		{"ledger entry taken through its unexported helper", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { _, _ = ledger.takeLocked("c") }`, "names takeLocked"},
		{"ledger waiters woken outside the ledger", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { notifyLocked() }`, "names notifyLocked"},
		{"ledger lock taken from outside", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.liveDeaths.mu.Lock() }`, "reaches the live-death ledger"},
		{"ledger aliased from outside", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { ledger := &b.liveDeaths; _ = ledger }`, "reaches the live-death ledger"},
		{"health ledger written outside the watch's memory", "internal/backend/docker/operation_intent.go",
			`package docker
func (b *Backend) classifyOperationIntentSubstrate() { b.startupHealth.recordPassedHealth(pass) }`,
			"names recordPassedHealth"},
		{"health ledger written elsewhere in the observation file", startupObservationFile,
			`package docker
func (b *Backend) observeRejectedLaunch() { m.health.recordPassedHealth(pass) }`, "names recordPassedHealth"},
		{"startup pass remembered outside its inspection", startupObservationFile,
			`package docker
func (b *Backend) watchStartup() { memory.remember(pass) }`, "names remember"},
		{"health ledger facts set outside the ledger", "internal/backend/docker/recover.go",
			`package docker
func (b *Backend) recoverState() { b.startupHealth.healthyByID["c"] = struct{}{} }`, "names healthyByID"},
		{"launch degradations reset", "internal/backend/docker/volume_launch.go",
			`package docker
func (m *storageMutations) launch() { q.degradations = launchDegradations{} }`, "assigns degradations"},
		{"image setup degradations reset", provisionFile,
			`package docker
func (b *Backend) inspectImagesForSetup() { result.Degradations = launchDegradations{} }`, "assigns Degradations"},
		{"launch degradations reset in a multiple assignment", provisionFile,
			`package docker
func (b *Backend) doProvisionPhysical() { x, mutations.degradations = 1, launchDegradations{} }`, "assigns degradations"},
		{"image setup keyed with degradations", provisionFile,
			`package docker
func (b *Backend) inspectImagesForSetup() { _ = imageSetup{Degradations: launchDegradations{}} }`, "keys Degradations"},
		{"quiesced volumes keyed with degradations", "internal/backend/docker/volume_launch.go",
			`package docker
func (m *storageMutations) quiesce() { _ = &quiescedVolumes{degradations: d} }`, "keys degradations"},
		{"degradation bits cleared outside the set", "internal/backend/docker/volume_launch.go",
			`package docker
func (m *storageMutations) quiesce() { q.degradations.degradationBits = 0 }`, "names degradationBits"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", test.src, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse synthetic source: %v", err)
			}
			findings := startupFailureAuthorityFindings(test.rel, file, fset)
			if len(findings) == 0 || !slices.ContainsFunc(findings, func(finding string) bool {
				return strings.Contains(finding, test.want)
			}) {
				t.Fatalf("want a finding containing %q, got %q", test.want, findings)
			}
		})
	}

	// Negative controls: the sanctioned shapes must stay silent.
	allowed := []struct {
		rel string
		src string
	}{
		{launchDispatchFile, `package docker
func newVolumeLaunchCoordinator() { _ = func() settledLaunch { return newSettledLaunch(q, o) } }`},
		{settledLaunchFile, `package docker
type settledLaunch struct{ state *settledLaunchState }
type settledLaunchState struct{ created []string }
func newSettledLaunch(q *quiescedVolumes, o daemonLaunchOutcome) settledLaunch {
	return settledLaunch{state: &settledLaunchState{}}
}`},
		{provisionFile, `package docker
func (b *Backend) doProvisionPhysical() (acceptedStartupFailure, error) {
	_ = b.observeStartup(ctx, m, l, c, nil)
	_ = b.observeRejectedLaunch(ctx, m, l, c, e, nil)
	return b.concludeStartupFailure(ctx, m, f, nil)
}`},
		{startupObservationFile, `package docker
func (b *Backend) startupObservationOf() { _, _ = b.newStartupFailure(ctx, m, l, c, w) }`},
		{startupFailureFile, "package docker\n" + sharedImport + `func (b *Backend) newStartupFailure() {
	_, _ = shared.NewOperationStartupFailure(shared.OperationStartupFailureTerms{})
	_, _ = b.liveDeaths.awaitLiveDeath(ctx, "c", 0)
	_ = startupFailure{state: &startupFailureState{}}
}`},
		{startupFailureFile, `package docker
func (b *Backend) concludeStartupFailure() {
	_, _ = mutations.acceptStartupFailure(f)
	_, _ = b.admitStartupRollback(ctx, m, a)
	_ = b.rollbackStartupFailure(ctx, m, r, nil)
}
func (b *Backend) admitStartupRollback() { _ = startupRollback{state: &startupRollbackState{}} }`},
		{startupFailureFile, "package docker\n" + sharedImport +
			`func (b *Backend) confirmStartupFailure() { _, _ = shared.NewOperationStartupFailed(s, f) }`},
		{startupObservationFile, `package docker
func (m *startupMemory) remember(pass startupPass) { m.health.recordPassedHealth(pass) }`},
		{provisionFile, `package docker
func (b *Backend) inspectImagesForSetup() { result.Degradations.add(launchVolumeOwnerUndetected); var skipped launchDegradations; skipped.add(d); _ = skipped }`},
		{settledLaunchFile, `package docker
func newSettledLaunch(q *quiescedVolumes, o daemonLaunchOutcome) settledLaunch {
	return settledLaunch{state: &settledLaunchState{degradations: q.degradations}}
}
func (d *launchDegradations) add(degradation launchDegradation) { d.degradationBits |= uint8(degradation) }`},
		{startupHealthLedgerFile, `package docker
func (l *startupHealthLedger) recordPassedHealth(pass startupPass) { l.healthyByID["c"] = struct{}{}; l.healthyNext++ }`},
		{terminalBudgetEventsFile, `package docker
func (b *Backend) runContainerEventLoop() {
	pipeline.Go(func() { b.recordLiveContainerDeaths(subscriptions, dispatch, overflow) })
	pipeline.Go(func() { b.dispatchLiveContainerDeaths(dispatch) })
}
func (b *Backend) recordLiveContainerDeaths() {
	b.liveDeaths.markLiveDeathStream(true)
	b.liveDeaths.recordLiveDeath(p)
	b.liveDeaths.markLiveDeathStream(false)
}
func (b *Backend) dispatchLiveContainerDeaths() { b.dispatchLiveContainerDeath(p) }
func (b *Backend) dispatchLiveContainerDeath(p int) {}
func (b *Backend) redispatchStartupDeaths() { _ = b.liveDeaths.takeLiveDeaths(nil); b.dispatchLiveContainerDeath(p) }`},
		{liveDeathLedgerFile, `package docker
type liveDeathLedger struct{ deathsByID map[string]int }
func (l *liveDeathLedger) recordLiveDeath(p int) { l.deathsByID["c"] = p; l.notifyLocked() }
func (l *liveDeathLedger) takeLiveDeaths(ids []string) []int { _, _ = l.takeLocked("c"); return nil }`},
		{sharedStartupFailureFile, `package shared
type OperationStartupFailure struct{ state *operationStartupFailureState }
type operationStartupFailureState struct{ kind int }
func NewOperationStartupFailed(s, f int) (OperationPhysicalEvidence, error) {
	_ = &operationStartupFailedState{}
	return OperationPhysicalEvidence{kind: operationPhysicalEvidenceStartupFailed}, nil
}
func NewOperationStartupFailure(t int) (int, error) { _ = &operationStartupFailureState{}; return 0, nil }`},
		{sharedHandoffFile, `package shared
const (
	operationExecutionRefusedBeforeStart operationExecutionFailureKind = iota
	operationExecutionAttestedStartupFailure
)
func (s *OperationSettlement) ExecuteOperation() {
	switch evidence.kind {
	case operationPhysicalEvidenceStartupFailed:
		_ = OperationExecutionFailure{kind: operationExecutionAttestedStartupFailure}
	}
}
func (outcome OperationExecutionFailure) Valid() bool { return outcome.kind == operationExecutionAttestedStartupFailure }`},
	}
	for _, control := range allowed {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "synthetic.go", control.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse synthetic source: %v", err)
		}
		if findings := startupFailureAuthorityFindings(control.rel, file, fset); len(findings) != 0 {
			t.Errorf("sanctioned shape in %s reported: %q", control.rel, findings)
		}
	}
}

func startupFailureAuthorityFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	imports := importNames(file)
	packageOf := make(map[string]string, len(imports))
	for importPath, name := range imports {
		packageOf[name] = importPath
	}
	dir := path.Dir(rel)
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
	allowedAtAny := func(sites []attributionSite, function string) bool {
		return slices.ContainsFunc(sites, func(site attributionSite) bool {
			return rel == site.file && function == site.function
		})
	}
	// sealedIdent reports ident if it names a sealed state type or kind outside
	// its sites.
	sealedIdent := func(rules []startupAuthorityRule, ident *ast.Ident, function string) {
		for _, rule := range rules {
			if ident.Name == rule.name && !allowedAtAny(rule.sites, function) {
				report(ident, "names %s, %s, outside its declaration and constructor", ident.Name, rule.what)
			}
		}
	}
	inspect := func(function string, root ast.Node, declared *ast.Ident) {
		// One pre-pass: the selectors that reach the ledger through one of its
		// entry points, and every selected name, which the selector case
		// already judges.
		sanctionedLedger := make(map[*ast.SelectorExpr]bool)
		selectedName := make(map[*ast.Ident]bool)
		ast.Inspect(root, func(node ast.Node) bool {
			if outer, ok := node.(*ast.SelectorExpr); ok {
				selectedName[outer.Sel] = true
				if inner, ok := outer.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "liveDeaths" &&
					slices.Contains(liveDeathLedgerEntryPoints, outer.Sel.Name) {
					sanctionedLedger[inner] = true
				}
			}
			return true
		})
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				qualifier := ""
				if ident, ok := typed.X.(*ast.Ident); ok {
					qualifier = packageOf[ident.Name]
				}
				name := typed.Sel.Name
				if qualifier == sharedImportPath {
					for _, rule := range sharedStartupRules {
						if name == rule.name && !allowedAtAny(rule.sites, function) {
							report(typed, "names shared.%s, %s, outside its one site", name, rule.what)
						}
					}
					return true
				}
				if dir == dockerDir {
					for _, rule := range dockerStartupRules {
						if name == rule.name && !allowedAtAny(rule.sites, function) {
							report(typed, "names %s, %s, outside its one site", name, rule.what)
						}
					}
					if owner, what, sealed := sealedInternalOwner(name); sealed && rel != owner {
						report(typed, "names %s, a %s internal, outside its own file", name, what)
					}
					if name == "liveDeaths" && !sanctionedLedger[typed] && rel != liveDeathLedgerFile {
						report(typed, "reaches the live-death ledger other than through its four entry points")
					}
				}
				return true
			case *ast.Ident:
				if typed == declared {
					return true
				}
				switch dir {
				case sharedDir:
					for _, rule := range sharedStartupRules {
						if typed.Name == rule.name {
							report(typed, "names shared.%s, %s, inside package shared", typed.Name, rule.what)
						}
					}
					sealedIdent(sharedSealedIdents, typed, function)
				case dockerDir:
					selected := selectedName[typed]
					for _, rule := range dockerStartupRules {
						if typed.Name == rule.name && !allowedAtAny(rule.sites, function) && !selected {
							report(typed, "names %s, %s, outside its one site", typed.Name, rule.what)
						}
					}
					if owner, what, sealed := sealedInternalOwner(typed.Name); sealed && rel != owner && !selected {
						report(typed, "names %s, a %s internal, outside its own file", typed.Name, what)
					}
					sealedIdent(dockerSealedStateIdents, typed, function)
				}
			case *ast.AssignStmt:
				if dir != dockerDir {
					return true
				}
				for _, lhs := range typed.Lhs {
					if selector, ok := lhs.(*ast.SelectorExpr); ok && slices.Contains(launchDegradationFields, selector.Sel.Name) {
						report(selector, "assigns %s, a launch degradation set, other than through add or addAll", selector.Sel.Name)
					}
				}
			case *ast.CompositeLit:
				if dir != dockerDir || len(typed.Elts) == 0 {
					return true
				}
				name, _ := calleeName(typed.Type)
				if site, sealed := startupLiteralSites[name]; sealed && !allowedAtAny([]attributionSite{site}, function) {
					report(typed, "builds a %s outside its constructor", name)
				}
				for _, element := range typed.Elts {
					keyed, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := keyed.Key.(*ast.Ident); ok && slices.Contains(launchDegradationFields, key.Name) &&
						!allowedAtAny([]attributionSite{{settledLaunchFile, settledLaunchMintSite}}, function) {
						report(keyed, "keys %s, a launch degradation set, in a literal outside the receipt's constructor", key.Name)
					}
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			inspect(functionKey(typed), typed, typed.Name)
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				inspect(declarationKey(spec), spec, nil)
			}
		default:
			inspect("", decl, nil)
		}
	}
	return findings
}

// declarationKey names one top-level declaration the way typeSite and
// valueSite do.
func declarationKey(spec ast.Spec) string {
	switch typed := spec.(type) {
	case *ast.TypeSpec:
		return "type " + typed.Name.Name
	case *ast.ValueSpec:
		if len(typed.Names) > 0 {
			return "value " + typed.Names[0].Name
		}
	}
	return ""
}
