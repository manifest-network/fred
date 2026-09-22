package shared

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Authority-bearing values deliberately expose behavior through methods, not
// writable fields. This keeps sibling packages from assembling a capability,
// relabeling a replay as first-dispatch authority, or changing its lineage.
func TestAuthorityBearingTypesExposeNoWritableFields(t *testing.T) {
	t.Parallel()

	values := []any{
		ActiveRetentionCandidate{},
		ActiveRetentionProof{},
		RuntimeGenerationProof{},
		RuntimeObservationPermit{},
		CallbackPublisher{},
		CallbackStorageAttestor{},
		CleanupCloseRequest{},
		ClosedLeaseReceipt{},
		ClosePhysicalEvidence{},
		CloseRequest{},
		CloseSettlement{},
		CloseIntentAdmission{},
		closeIntentCandidate{},
		CloseIntentClaim{},
		MaintenanceAppendClaim{},
		MaintenanceExecutionAmbiguous{},
		MaintenanceExecutionClaim{},
		MaintenanceExecutionFailure{},
		MaintenanceExecutionSuccess{},
		MaintenancePhysicalEvidence{},
		MaintenanceIntentAdmission{},
		MaintenanceIntentCandidate{},
		MaintenanceIntentClaim{},
		MaintenanceIntentDispatch{},
		MaintenanceReleaseClaim{},
		MaintenanceReleaseActive{},
		MaintenanceReleaseFailure{},
		MaintenanceSourceClaim{},
		MaintenanceSourceSnapshot{},
		MaintenanceRequestAuthority{},
		MaintenanceSettlement{},
		LeaseRecoveryScope{},
		OperationIntentAdmission{},
		OperationIntentCandidate{},
		OperationIntentClaim{},
		OperationIntentProbe{},
		OperationExecutionAmbiguous{},
		OperationExecutionClaim{},
		OperationExecutionFailure{},
		OperationExecutionSuccess{},
		OperationPhysicalEvidence{},
		OperationReleaseCandidate{},
		OperationReleaseCommitted{},
		OperationReleaseUncommitted{},
		OperationSettlement{},
		FailedOperationReceipt{},
		ReapingRetentionProof{},
		ReleaseBackfiller{},
		ReleaseClaim{},
		ReleaseRuntimeAuthority{},
		ReleaseRuntimeIdentity{},
		RecoveryLineage{},
		LegacyRuntimeAuthority{},
		RestoreClaimCandidate{},
		RestoringRetentionProof{},
		RestoreSettlement{},
	}

	for _, value := range values {
		typeOf := reflect.TypeOf(value)
		t.Run(typeOf.Name(), func(t *testing.T) {
			t.Parallel()
			for index := range typeOf.NumField() {
				field := typeOf.Field(index)
				if field.IsExported() {
					t.Errorf("authority-bearing type %s exposes writable field %s", typeOf, field.Name)
				}
			}
		})
	}
}

func TestRawOperationMutationMethodsAreNotProductionAPI(t *testing.T) {
	t.Parallel()

	forbidden := map[string]map[string]struct{}{
		"ReleaseStore": {
			"Append":                                {},
			"AppendActive":                          {},
			"CheckAppendActiveCapacity":             {},
			"UpdateLatestStatus":                    {},
			"ActivateLatest":                        {},
			"Delete":                                {},
			"DeleteCloseHistory":                    {},
			"AppendMaintenance":                     {},
			"ActivateMaintenance":                   {},
			"FailMaintenance":                       {},
			"FindMaintenanceRelease":                {},
			"PrepareOperationRelease":               {},
			"AppendOperationRelease":                {},
			"ProveCommittedOperation":               {},
			"ProveUncommittedOperation":             {},
			"BackfillActiveResourceProfilesContext": {},
			"BackfillLegacyActiveAuthorityContext":  {},
			"BackfillLegacyRuntimeAuthorityContext": {},
		},
		"CallbackStore": {
			"GetMaintenanceIntent":                          {},
			"ListMaintenanceIntents":                        {},
			"Store":                                         {},
			"StoreEntry":                                    {},
			"RemoveEntry":                                   {},
			"NewOperationIntentProbe":                       {},
			"ProbeOperationIntent":                          {},
			"NewOperationIntentCandidate":                   {},
			"BeginOperationIntent":                          {},
			"ListOperationIntents":                          {},
			"ListOperationRecoveryStates":                   {},
			"ListFailedOperationReceipts":                   {},
			"LookupOperationRecovery":                       {},
			"BeginCloseIntent":                              {},
			"IncrementCloseCleanupAttempts":                 {},
			"NewCloseIntentCandidate":                       {},
			"ResolveCloseIntent":                            {},
			"NewMaintenanceRequestAuthority":                {},
			"NewMaintenanceIntentCandidate":                 {},
			"ProbeMaintenanceIntent":                        {},
			"BeginMaintenanceIntent":                        {},
			"StartMaintenanceAppend":                        {},
			"BindMaintenanceIntentTarget":                   {},
			"TryBindMaintenanceIntentTarget":                {},
			"CancelMaintenanceIntent":                       {},
			"ResolveMaintenanceIntent":                      {},
			"TryResolveMaintenanceIntent":                   {},
			"TryResolveMaintenanceIntentWithRuntimeFailure": {},
			"ResolveOperationIntent":                        {},
			"ResolveOperationSuccess":                       {},
			"ResolveOperationFailure":                       {},
			"FailOperationIntentIfPresent":                  {},
		},
		"RetentionStore": {
			"PrepareRestoreClaim": {},
			"ClaimForRestore":     {},
		},
		"CallbackSender": {
			"DeliverCallback":        {},
			"ReplayPendingCallbacks": {},
			"SendOperationCallback":  {},
			"SendLifecycleCallback":  {},
		},
		"OperationSettlement": {
			"ResolveOperationSuccess":          {},
			"ResolveOperationFailure":          {},
			"ProveUncommittedOperation":        {},
			"ProveCurrentUncommittedOperation": {},
		},
		"MaintenanceSettlement": {
			"ResolveSuccess":                     {},
			"TryResolveSuccess":                  {},
			"ResolveFailure":                     {},
			"TryResolveFailure":                  {},
			"TryResolveActiveWithRuntimeFailure": {},
		},
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil || len(function.Recv.List) != 1 {
				continue
			}
			receiver := function.Recv.List[0].Type
			if pointer, ok := receiver.(*ast.StarExpr); ok {
				receiver = pointer.X
			}
			identifier, ok := receiver.(*ast.Ident)
			if !ok {
				continue
			}
			if methods := forbidden[identifier.Name]; methods != nil {
				if _, blocked := methods[function.Name.Name]; blocked {
					t.Errorf("raw %s.%s remains production-callable at %s",
						identifier.Name, function.Name.Name, fset.Position(function.Pos()))
				}
			}
		}
	}
}
