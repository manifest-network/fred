package placement

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestWorkerStagesResolveDistinctCountersAtInit(t *testing.T) {
	names := make(map[string]workerStage, workerStageCount)
	for stage := range workerStageCount {
		entry := stage.entry()
		require.NotEmpty(t, entry.name, "stage %d", stage)
		require.NotNil(t, entry.panics, "stage %d has no counter series", stage)
		previous, duplicate := names[entry.name]
		require.False(t, duplicate, "stages %d and %d share a name", previous, stage)
		names[entry.name] = stage
	}
	unknown := workerStageCount.entry()
	require.Nil(t, unknown.panics, "a value outside the closed set must not count under a real stage")
	require.Contains(t, unknown.name, "unknown worker stage")
}

func TestRunBoundedReturnsAPanicAsAnUnsettledError(t *testing.T) {
	counter := workerChainInventory.entry().panics
	before := testutil.ToFloat64(counter)
	value, err := runBounded(workerChainInventory, func() ([]string, error) {
		panic("chain client bug")
	})
	require.ErrorIs(t, err, errWorkerPanicked)
	require.ErrorContains(t, err, "chain inventory read")
	require.Nil(t, value, "a panic never yields a partial result")
	require.Equal(t, before+1, testutil.ToFloat64(counter))

	value, err = runBounded(workerChainInventory, func() ([]string, error) {
		return []string{"lease"}, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"lease"}, value)
	failure := errors.New("transport failure")
	_, err = runBounded(workerChainInventory, func() ([]string, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	require.NotErrorIs(t, err, errWorkerPanicked)
	require.Equal(t, before+1, testutil.ToFloat64(counter), "only a panic counts")
}

func TestStartWorkerDeliversExactlyOneResultHoweverTheWorkerEnds(t *testing.T) {
	receive := func(results <-chan workerResult[int]) workerResult[int] {
		t.Helper()
		select {
		case result := <-results:
			select {
			case extra := <-results:
				t.Fatalf("worker delivered a second result: %+v", extra)
			default:
			}
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("worker delivered no result")
			return workerResult[int]{}
		}
	}

	returned := receive(startWorker(workerChainInventory, func() (int, error) { return 7, nil }))
	require.NoError(t, returned.err)
	require.Equal(t, 7, returned.value)

	panicked := receive(startWorker(workerChainInventory, func() (int, error) { panic("worker bug") }))
	require.ErrorIs(t, panicked.err, errWorkerPanicked)
	require.Zero(t, panicked.value)

	exited := receive(startWorker(workerChainInventory, func() (int, error) {
		runtime.Goexit()
		return 7, nil
	}))
	require.ErrorContains(t, exited.err, "exited without a result",
		"a worker that never returned must not be read as a success")
	require.Zero(t, exited.value)
}

// TestPlacementGoroutinesStartThroughStartWorker keeps every worker goroutine
// in this package behind the panic boundary. A goroutine started any other way
// can crash providerd: a recover in its caller never sees its panic.
// copyOwnedRead is the one reviewed exception: its two goroutines run a pipe
// protocol whose cleanup order (close the pipe, then end the read
// transaction) must hold on every exit, and the writer recovers its own
// panics.
func TestPlacementGoroutinesStartThroughStartWorker(t *testing.T) {
	allowed := map[string]int{"startWorker": 1, "copyOwnedRead": 2}
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	started := make(map[string]int)
	var violations []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			owner := "<package scope>"
			if function, ok := declaration.(*ast.FuncDecl); ok {
				owner = function.Name.Name
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.GoStmt:
					started[owner]++
					if _, ok := allowed[owner]; !ok {
						violations = append(violations, fmt.Sprintf("%s: go statement in %s", fset.Position(node.Pos()), owner))
					}
				case *ast.CallExpr:
					if selector, ok := node.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Go" {
						violations = append(violations, fmt.Sprintf("%s: %s.Go in %s", fset.Position(node.Pos()), receiverName(selector.X), owner))
					}
				}
				return true
			})
		}
	}
	require.Empty(t, violations, "start worker goroutines through startWorker")
	for owner, count := range allowed {
		require.Equal(t, count, started[owner], "reviewed goroutines in %s changed; review the new one", owner)
	}
}

func receiverName(expression ast.Expr) string {
	if identifier, ok := expression.(*ast.Ident); ok {
		return identifier.Name
	}
	return fmt.Sprintf("%T", expression)
}

type panickingPendingInventory struct{ *reconciliationSweepReader }

func (panickingPendingInventory) GetPendingLeases(context.Context, string) ([]billingtypes.Lease, error) {
	panic("chain client bug")
}

func TestCollectChainInventoryPanicFailsTheWholeInventory(t *testing.T) {
	active := billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE,
	}
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(&executionTestBackend{name: "backend-a"}))
	reader := panickingPendingInventory{&reconciliationSweepReader{active: []billingtypes.Lease{active}}}
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)

	counter := workerChainInventory.entry().panics
	before := testutil.ToFloat64(counter)
	pending, activeLeases, err := coordinator.CollectChainInventory(t.Context(), time.Second)
	require.ErrorIs(t, err, errWorkerPanicked)
	require.ErrorContains(t, err, "failed to get pending leases")
	require.Nil(t, pending)
	require.Nil(t, activeLeases, "the ACTIVE half that answered is not returned as partial authority")
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	reader.mu.Lock()
	defer reader.mu.Unlock()
	require.Equal(t, 1, reader.activeReads, "the other read still ran to completion")
}

func TestMaintenanceRecoveryPanicFailsOnlyItsOwnCommand(t *testing.T) {
	var restartHealthy atomic.Bool
	var panicLease atomic.Value
	panicLease.Store("")
	var restartsMu sync.Mutex
	restarts := map[string]int{}
	tenants := map[string]string{}
	reader := maintenanceLeaseReaderFunc(func(_ context.Context, lease string) (*billingtypes.Lease, error) {
		if panicLease.Load().(string) == lease {
			panic("chain client bug")
		}
		tenant := tenants[lease]
		if tenant == "" {
			tenant = "tenant-test"
		}
		return &billingtypes.Lease{Uuid: lease, Tenant: tenant, ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
	})
	client := &executionTestBackend{name: "backend-a", restart: func(_ context.Context, request backend.RestartRequest) error {
		restartsMu.Lock()
		restarts[request.LeaseUUID]++
		restartsMu.Unlock()
		if !restartHealthy.Load() {
			return errors.New("ambiguous delivery")
		}
		return nil
	}}
	restartCount := func(lease string) int {
		restartsMu.Lock()
		defer restartsMu.Unlock()
		return restarts[lease]
	}
	authority, store := newMaintenanceCoordinatorForTest(t, reader, client)
	scope := requireAdmissionScope(t, store, store.CurrentAdmissionBaseline(), "backend-a")
	other := prepareTenantMaintenance(t, store, scope, 950, "tenant-a", []byte("manifest")).Command().LeaseUUID()
	tenants[other] = "tenant-a"
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	poisonedID := mustMaintenanceID(t, maintenanceIDA)
	for _, command := range []struct {
		id     string
		lease  string
		tenant string
	}{{maintenanceIDA, maintenanceLease, "tenant-test"}, {maintenanceIDB, other, "tenant-a"}} {
		request, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, command.id), command.lease, command.tenant, MaintenanceCommandRestart, nil)
		require.NoError(t, err)
		require.Equal(t, MaintenanceApplicationServiceUnavailable, application.Execute(t.Context(), request).Outcome())
	}
	require.Equal(t, 1, restartCount(maintenanceLease))
	require.Equal(t, 1, restartCount(other))

	restartHealthy.Store(true)
	panicLease.Store(maintenanceLease)
	counter := workerMaintenanceRecovery.entry().panics
	before := testutil.ToFloat64(counter)
	err = application.RecoverPending(t.Context())
	require.ErrorIs(t, err, errWorkerPanicked)
	require.ErrorContains(t, err, maintenanceLease)
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	require.Equal(t, 2, restartCount(other), "the same backend lane recovered the other lease's command")
	require.Equal(t, 1, restartCount(maintenanceLease))

	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, poisonedID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, MaintenanceOutcomePending, record.Outcome(), "a panic settles nothing")
	application.heldMu.Lock()
	held := application.held[maintenanceLease]
	application.heldMu.Unlock()
	require.NotNil(t, held, "the lifecycle claim stays held for the pending command")
	require.True(t, held.dispatchMu.TryLock(), "the panic released the dispatch lane")
	held.dispatchMu.Unlock()

	panicLease.Store("")
	require.NoError(t, application.RecoverPending(t.Context()))
	require.Equal(t, 2, restartCount(maintenanceLease), "the next pass recovers the command")
}
