package placement

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// TestInventorySweepRefusalNamesOnlyTheOpenSweepsBarrier pins the one
// predicate that decides whether a barrier refusal is retryable: every barrier
// on the lease belongs to the sweep still collecting, and no recovery is
// pending. Anything else waits on more than that sweep's projection.
func TestInventorySweepRefusalNamesOnlyTheOpenSweepsBarrier(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000002161"
	const (
		refusedNone = iota
		refusedRetryable
		refusedPlain
	)
	for _, test := range []struct {
		name     string
		recovery bool
		pending  uint64
		barriers []uint64
		want     int
	}{
		{name: "no barrier", pending: 7, want: refusedNone},
		{name: "barrier from the open sweep", pending: 7, barriers: []uint64{7}, want: refusedRetryable},
		{name: "barrier an earlier projection left unresolved", barriers: []uint64{6}, want: refusedPlain},
		{name: "unresolved barrier while a new sweep collects", pending: 7, barriers: []uint64{6, 7}, want: refusedPlain},
		{name: "open-sweep barrier during recovery", recovery: true, pending: 7, barriers: []uint64{7}, want: refusedPlain},
		{name: "recovery without a lease barrier", recovery: true, pending: 7, want: refusedPlain},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			store.mu.Lock()
			store.inventoryRecoveryRequired = test.recovery
			store.pendingInventorySweepID = test.pending
			if len(test.barriers) != 0 {
				boundaries := make(map[uint64]map[inventoryPositiveObservation]struct{})
				for _, sweepID := range test.barriers {
					boundaries[sweepID] = map[inventoryPositiveObservation]struct{}{
						{backendName: "backend-a", class: inventoryPositiveRetention}: {},
					}
				}
				store.unprojectedPositives[leaseUUID] = boundaries
			}
			err := store.unprojectedPositiveErrorLocked(leaseUUID)
			store.mu.Unlock()

			switch test.want {
			case refusedNone:
				require.NoError(t, err)
			case refusedRetryable:
				require.True(t, inventorySweepPending(err), err)
				require.ErrorIs(t, err, ErrUnprojectedInventoryPositive,
					"existing readers of the barrier sentinel must still match")
			case refusedPlain:
				require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
				require.False(t, inventorySweepPending(err),
					"a barrier that outlives the open sweep must not read as retryable: %v", err)
			}
		})
	}
}

// TestOnlyTheExactSweepRefusalIsRetryable proves the classifiers cannot
// promote evidence that passed through anything else. A joined error may
// carry a failed authority postcheck, and a wrapped one names a path the
// source did not vouch for; both stay the generic unavailable answer.
func TestOnlyTheExactSweepRefusalIsRetryable(t *testing.T) {
	exact := inventorySweepRefusal{leaseUUID: "lease", sweepID: 3}
	for _, test := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "exact source refusal", err: exact, retryable: true},
		{name: "wrapped refusal", err: fmt.Errorf("admit: %w", exact)},
		{name: "joined with another failure", err: errors.Join(exact, ErrRuntimeAuthorityUnavailable)},
		{name: "bare barrier sentinel", err: fmt.Errorf("%w: lease %q", ErrUnprojectedInventoryPositive, "lease")},
	} {
		t.Run(test.name, func(t *testing.T) {
			restore, maintenance := RestoreApplicationServiceUnavailable, MaintenanceApplicationServiceUnavailable
			if test.retryable {
				restore, maintenance = RestoreApplicationInventoryReconciling, MaintenanceApplicationInventoryReconciling
			}
			assert.Equal(t, restore, restoreAdmissionApplicationFailure(test.err).Disposition())
			assert.Equal(t, maintenance, resultForPreparation(MaintenancePreparation{err: test.err}).Outcome())
			assert.Equal(t, maintenance, resultForMaintenanceBeginError(test.err).Outcome())
		})
	}
}

// TestRestoreAdmittedDuringSweepCollectionIsInventoryReconciling reproduces
// ENG-2161. A sweep installs a barrier on every retained lease as each backend
// answers, and clears it only at projection. A restore admitted in between was
// refused with a silent generic 503. It is now a typed, retryable refusal, and
// the same request is accepted once the sweep projects.
func TestRestoreAdmittedDuringSweepCollectionIsInventoryReconciling(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
	})
	var restored int
	owner := &executionTestBackend{name: "backend-a", restore: func(
		context.Context, backend.RestoreRequest,
	) error {
		restored++
		return nil
	}}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(
		owner, &executionTestBackend{name: "backend-b"},
	))
	reader := executionLeaseReaderFunc(func(
		_ context.Context, leaseUUID string,
	) (*billingtypes.Lease, error) {
		state := billingtypes.LEASE_STATE_PENDING
		if leaseUUID == fencedAvailabilityOwner {
			state = billingtypes.LEASE_STATE_CLOSED
		}
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: state, Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		}, nil
	})
	reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)
	restore, err := restoreAuthorityForTest(execution, nil)
	require.NoError(t, err)
	request, err := NewRestoreApplicationRequest(
		fencedAvailabilitySibling, "tenant-test", fencedAvailabilityOwner,
	)
	require.NoError(t, err)

	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	// The source's own backend answers first and reports the retention it is
	// already known to hold.
	require.NoError(t, sweep.RecordRetention(
		"backend-a", testBackendStorageID("backend-a"), []string{fencedAvailabilityOwner},
	))

	result := restore.ExecuteApplication(t.Context(), request)
	require.Equal(t, RestoreApplicationInventoryReconciling, result.Disposition(), result.Err())
	require.ErrorIs(t, result.Err(), ErrUnprojectedInventoryPositive)
	require.False(t, result.DefinitivelyRefused(), "a deferral publishes no failed lifecycle event")
	assert.Zero(t, restored, "no backend may see the target")
	assert.Equal(t, StateAbsent, store.Lookup(fencedAvailabilitySibling).State(),
		"the refusal writes nothing")

	require.NoError(t, sweep.RecordProvision("backend-a", testBackendStorageID("backend-a"), nil))
	require.NoError(t, sweep.RecordProvision("backend-b", testBackendStorageID("backend-b"), nil))
	require.NoError(t, sweep.RecordRetention("backend-b", testBackendStorageID("backend-b"), nil))
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{
		Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
	})
	require.NoError(t, err)

	result = restore.ExecuteApplication(t.Context(), request)
	require.Equal(t, RestoreApplicationAccepted, result.Disposition(), result.Err())
	assert.Equal(t, 1, restored)
}

// TestMaintenanceAdmittedDuringSweepCollectionIsInventoryReconciling is the
// maintenance half of ENG-2161: a new restart refused by the same barrier is
// the typed retryable refusal, records no command, and is admitted after the
// sweep projects.
func TestMaintenanceAdmittedDuringSweepCollectionIsInventoryReconciling(t *testing.T) {
	var restarts int
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(),
		&executionTestBackend{name: "backend-a", restart: func(context.Context, backend.RestartRequest) error {
			restarts++
			return nil
		}})
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	reconciliation, err := authority.coordinator.execution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	id := mustMaintenanceID(t, maintenanceIDA)
	request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandRestart, nil)
	require.NoError(t, err)

	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	storageID := testBackendStorageID("backend-a")
	require.NoError(t, sweep.RecordProvision("backend-a", storageID, []backend.ProvisionInfo{{
		LeaseUUID: maintenanceLease, BackendName: "backend-a",
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
	}}))

	result := application.Execute(t.Context(), request)
	require.Equal(t, MaintenanceApplicationInventoryReconciling, result.Outcome(), result.Err())
	require.ErrorIs(t, result.Err(), ErrUnprojectedInventoryPositive)
	assert.Zero(t, restarts)
	_, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
	require.NoError(t, err)
	require.False(t, found, "the refusal must precede the durable command")

	require.NoError(t, sweep.RecordRetention("backend-a", storageID, nil))
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{
		Placements: map[string]string{maintenanceLease: "backend-a"},
	})
	require.NoError(t, err)

	result = application.Execute(t.Context(), request)
	require.Equal(t, MaintenanceApplicationAccepted, result.Outcome(), result.Err())
	assert.Equal(t, 1, restarts)
}

// TestInventorySweepRefusalHasOneMinterAndOneReader keeps the retryable
// verdict authored at the barrier check and recognized in one place. A second
// minter could call a barrier retryable without checking its sweep, and a
// second reader could unwrap evidence the exact check refuses.
func TestInventorySweepRefusalHasOneMinterAndOneReader(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var findings []string
	scanned := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		findings = append(findings, inventorySweepRefusalViolations(fset, file)...)
		scanned++
	}
	require.Greater(t, scanned, 20, "the guard must scan the package's production files")
	assert.Empty(t, findings)
}

func TestInventorySweepRefusalGuardFires(t *testing.T) {
	const src = `package placement
var forged = inventorySweepRefusal{sweepID: 1}
func unprojectedPositiveErrorLocked() error { return inventorySweepRefusal{} }
func elsewhere() error { return inventorySweepRefusal{leaseUUID: "x"} }
func inventorySweepPending(err error) bool { _, ok := err.(inventorySweepRefusal); return ok }
func classify(err error) bool { _, ok := err.(inventorySweepRefusal); return ok }
func classifyAs(err error) bool { var r inventorySweepRefusal; return errors.As(err, &r) }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	findings := inventorySweepRefusalViolations(fset, file)
	for _, want := range []string{
		"synthetic.go:2:14: package-level var builds inventorySweepRefusal",
		"synthetic.go:4:33: elsewhere builds inventorySweepRefusal",
		"classify reads inventorySweepRefusal",
		"classifyAs names inventorySweepRefusal",
	} {
		assert.True(t, slices.ContainsFunc(findings, func(f string) bool { return strings.Contains(f, want) }),
			"missing %q in %q", want, findings)
	}
	assert.Len(t, findings, 4, "the sanctioned minter and reader stay silent: %q", findings)
}

// inventorySweepRefusalViolations reports every use of the refusal type
// outside its type declaration, methods, minter, and reader.
func inventorySweepRefusalViolations(fset *token.FileSet, file *ast.File) []string {
	const (
		typeName = "inventorySweepRefusal"
		minter   = "unprojectedPositiveErrorLocked"
		reader   = "inventorySweepPending"
	)
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s: %s", fset.Position(node.Pos()), fmt.Sprintf(format, args...)))
	}
	for _, decl := range file.Decls {
		function, isFunc := decl.(*ast.FuncDecl)
		if !isFunc {
			ast.Inspect(decl, func(node ast.Node) bool {
				if literal, ok := node.(*ast.CompositeLit); ok && isIdent(literal.Type, typeName) {
					report(literal, "package-level var builds %s", typeName)
				}
				return true
			})
			continue
		}
		if function.Recv != nil && len(function.Recv.List) == 1 && isIdent(function.Recv.List[0].Type, typeName) {
			continue
		}
		where := function.Name.Name
		ast.Inspect(function, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CompositeLit:
				if isIdent(typed.Type, typeName) {
					if where != minter {
						report(typed, "%s builds %s", where, typeName)
					}
					return false
				}
			case *ast.TypeAssertExpr:
				if isIdent(typed.Type, typeName) {
					if where != reader {
						report(typed, "%s reads %s", where, typeName)
					}
					return false
				}
			case *ast.Ident:
				if typed.Name == typeName {
					report(typed, "%s names %s", where, typeName)
				}
			}
			return true
		})
	}
	return findings
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}
