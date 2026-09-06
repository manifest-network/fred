package docker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// operationSeedExecutors lets tests create an already-running substrate and
// then commit its release through the same Started -> guarded evidence ->
// CommitOperationSuccess protocol as production. It replaces the former raw
// AppendOperationRelease test shortcut without exposing that shortcut in the
// production API.
var operationSeedExecutors sync.Map   // *shared.OperationSettlement -> *operationSeedExecutor
var maintenanceSeedExecutors sync.Map // *shared.MaintenanceSettlement -> *maintenanceSeedExecutor

type operationSeedExecutor struct {
	mu    sync.RWMutex
	ready map[shared.OperationID]operationSeedProjection
}

type operationSeedProjection struct {
	containerIDs      []string
	serviceContainers map[string][]string
}

type maintenanceSeedKind uint8

const (
	maintenanceSeedTargetReady maintenanceSeedKind = iota + 1
	maintenanceSeedSourceReady
	maintenanceSeedAbsent
	maintenanceSeedAmbiguous
)

type maintenanceSeedPlan struct {
	kind    maintenanceSeedKind
	started chan<- struct{}
	proceed <-chan struct{}
}

type maintenanceSeedExecutor struct {
	mu      sync.RWMutex
	results map[shared.MaintenanceID]maintenanceSeedPlan
}

func newMaintenanceSeedExecutor() *maintenanceSeedExecutor {
	return &maintenanceSeedExecutor{results: make(map[shared.MaintenanceID]maintenanceSeedPlan)}
}

func (s *maintenanceSeedExecutor) register(
	id shared.MaintenanceID,
	kind maintenanceSeedKind,
) (func(), error) {
	return s.registerPlan(id, maintenanceSeedPlan{kind: kind})
}

func (s *maintenanceSeedExecutor) registerPlan(
	id shared.MaintenanceID,
	plan maintenanceSeedPlan,
) (func(), error) {
	if s == nil || !id.Valid() || plan.kind < maintenanceSeedTargetReady || plan.kind > maintenanceSeedAmbiguous {
		return nil, errors.New("test maintenance seed is invalid")
	}
	s.mu.Lock()
	if _, exists := s.results[id]; exists {
		s.mu.Unlock()
		return nil, errors.New("test maintenance seed is already registered")
	}
	s.results[id] = plan
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.results, id)
		s.mu.Unlock()
	}, nil
}

func (s *maintenanceSeedExecutor) result(id shared.MaintenanceID) (maintenanceSeedPlan, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	plan, ok := s.results[id]
	return plan, ok
}

func newOperationSeedExecutor() *operationSeedExecutor {
	return &operationSeedExecutor{ready: make(map[shared.OperationID]operationSeedProjection)}
}

func (s *operationSeedExecutor) register(
	id shared.OperationID,
	release shared.Release,
) (func(), error) {
	if s == nil || !id.Valid() {
		return nil, errors.New("test operation seed requires an exact operation ID")
	}
	ids := make([]string, 0)
	services := make(map[string][]string, len(release.Items))
	for _, item := range release.Items {
		for index := range item.Quantity {
			containerID := fmt.Sprintf("seed-%s-%d-%s", item.ServiceName, index, id.String())
			ids = append(ids, containerID)
			services[item.ServiceName] = append(services[item.ServiceName], containerID)
		}
	}
	projection := operationSeedProjection{containerIDs: ids, serviceContainers: services}
	s.mu.Lock()
	if _, exists := s.ready[id]; exists {
		s.mu.Unlock()
		return nil, errors.New("test operation seed is already registered")
	}
	s.ready[id] = projection
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.ready, id)
		s.mu.Unlock()
	}, nil
}

func (s *operationSeedExecutor) projection(
	id shared.OperationID,
) (operationSeedProjection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	projection, ok := s.ready[id]
	return projection, ok
}

type seedOperationSubstrate func(context.Context) error

func bindSeedOnlyOperationExecutor(
	settlement *shared.OperationSettlement,
	state *operationSeedExecutor,
) error {
	return shared.BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) seedOperationSubstrate {
			return func(ctx context.Context) error {
				if _, ok := state.projection(subject.OperationID()); !ok {
					return errors.New("test operation has no registered seed substrate")
				}
				return runner.Step(ctx, "seed exact running substrate", func(context.Context) error { return nil })
			}
		},
		func(ctx context.Context, capability seedOperationSubstrate, _ shared.OperationPhysicalSubject) error {
			if capability == nil {
				return errors.New("test operation substrate is unavailable")
			}
			return capability(ctx)
		},
		func(_ context.Context, subject shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			projection, ok := state.projection(subject.OperationID())
			if !ok {
				return shared.OperationPhysicalEvidence{}, errors.New("test operation has no registered seed projection")
			}
			return shared.NewOperationTargetReady(subject, projection.containerIDs, projection.serviceContainers)
		},
	)
}

func bindBackendTestPhysicalExecutors(
	b *Backend,
	operations *shared.OperationSettlement,
	maintenance *shared.MaintenanceSettlement,
) error {
	ops, err := storageMutationOperationsForTest(b)
	if err != nil {
		return err
	}
	state := newOperationSeedExecutor()
	operationSeedExecutors.Store(operations, state)
	buildOperation := buildOperationSubstrate(b, ops)
	err = shared.BindOperationSubstrateExecutor(
		operations,
		b.authorizeStorageMutation,
		b.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) operationSubstrate {
			if _, seeded := state.projection(subject.OperationID()); seeded {
				return func(ctx context.Context) error {
					return runner.Step(
						ctx, "seed exact running substrate", func(context.Context) error { return nil },
					)
				}
			}
			return buildOperation(runner, subject)
		},
		runOperationSubstrate,
		func(ctx context.Context, subject shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			if projection, seeded := state.projection(subject.OperationID()); seeded {
				return shared.NewOperationTargetReady(
					subject, projection.containerIDs, projection.serviceContainers,
				)
			}
			return b.classifyOperationPhysical(ctx, subject)
		},
	)
	if err != nil {
		operationSeedExecutors.Delete(operations)
		return err
	}
	maintenanceState := newMaintenanceSeedExecutor()
	maintenanceSeedExecutors.Store(maintenance, maintenanceState)
	buildMaintenance := buildMaintenanceSubstrate(b, ops)
	err = shared.BindMaintenanceSubstrateExecutor(
		maintenance,
		b.authorizeStorageMutation,
		b.completeStorageMutation,
		func(runner substratemutation.Runner, subject shared.MaintenancePhysicalSubject) maintenanceSubstrate {
			if plan, seeded := maintenanceState.result(subject.MaintenanceID()); seeded && !subject.RecoveryCleanup() {
				return func(ctx context.Context) error {
					if plan.started != nil {
						select {
						case plan.started <- struct{}{}:
						default:
						}
					}
					if plan.proceed != nil {
						select {
						case <-plan.proceed:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return runner.Step(
						ctx, "seed exact maintenance substrate", func(context.Context) error { return nil },
					)
				}
			}
			return buildMaintenance(runner, subject)
		},
		runMaintenanceSubstrate,
		func(ctx context.Context, subject shared.MaintenancePhysicalSubject) (shared.MaintenancePhysicalEvidence, error) {
			if plan, seeded := maintenanceState.result(subject.MaintenanceID()); seeded && !subject.RecoveryCleanup() {
				return maintenanceSeedEvidence(subject, plan.kind)
			}
			return b.classifyMaintenancePhysical(ctx, subject)
		},
	)
	if err != nil {
		maintenanceSeedExecutors.Delete(maintenance)
	}
	return err
}

type seedMaintenanceSubstrate func(context.Context) error

func bindSeedOnlyMaintenanceExecutor(
	settlement *shared.MaintenanceSettlement,
	state *maintenanceSeedExecutor,
) error {
	return shared.BindMaintenanceSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject shared.MaintenancePhysicalSubject) seedMaintenanceSubstrate {
			return func(ctx context.Context) error {
				plan, ok := state.result(subject.MaintenanceID())
				if !ok {
					return errors.New("test maintenance has no registered substrate")
				}
				if plan.started != nil {
					select {
					case plan.started <- struct{}{}:
					default:
					}
				}
				if plan.proceed != nil {
					select {
					case <-plan.proceed:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return runner.Step(ctx, "seed exact maintenance substrate", func(context.Context) error { return nil })
			}
		},
		func(ctx context.Context, capability seedMaintenanceSubstrate, _ shared.MaintenancePhysicalSubject) error {
			if capability == nil {
				return errors.New("test maintenance substrate is unavailable")
			}
			return capability(ctx)
		},
		func(_ context.Context, subject shared.MaintenancePhysicalSubject) (shared.MaintenancePhysicalEvidence, error) {
			plan, ok := state.result(subject.MaintenanceID())
			if !ok {
				return shared.MaintenancePhysicalEvidence{}, errors.New("test maintenance has no registered result")
			}
			return maintenanceSeedEvidence(subject, plan.kind)
		},
	)
}

func maintenanceSeedEvidence(
	subject shared.MaintenancePhysicalSubject,
	kind maintenanceSeedKind,
) (shared.MaintenancePhysicalEvidence, error) {
	if kind == maintenanceSeedAmbiguous {
		return shared.MaintenancePhysicalEvidence{}, errors.New("test maintenance substrate classification is ambiguous")
	}
	release, ok := subject.TargetRelease()
	if kind == maintenanceSeedSourceReady {
		release, ok = subject.SourceRelease()
	}
	if kind == maintenanceSeedAbsent {
		return shared.NewMaintenanceTargetAbsent(subject)
	}
	if !ok {
		return shared.MaintenancePhysicalEvidence{}, errors.New("test maintenance subject has no selected release")
	}
	projection := operationSeedProjection{}
	projection.serviceContainers = make(map[string][]string, len(release.Items))
	for _, item := range release.Items {
		for index := range item.Quantity {
			id := fmt.Sprintf("seed-%s-%d-%s", item.ServiceName, index, subject.MaintenanceID())
			projection.containerIDs = append(projection.containerIDs, id)
			projection.serviceContainers[item.ServiceName] = append(
				projection.serviceContainers[item.ServiceName], id,
			)
		}
	}
	if kind == maintenanceSeedSourceReady {
		return shared.NewMaintenanceSourceReady(
			subject, projection.containerIDs, projection.serviceContainers,
		)
	}
	return shared.NewMaintenanceTargetReady(
		subject, projection.containerIDs, projection.serviceContainers,
	)
}

func seedExecutorForMaintenanceSettlement(
	settlement *shared.MaintenanceSettlement,
) (*maintenanceSeedExecutor, error) {
	if value, ok := maintenanceSeedExecutors.Load(settlement); ok {
		return value.(*maintenanceSeedExecutor), nil
	}
	state := newMaintenanceSeedExecutor()
	if err := bindSeedOnlyMaintenanceExecutor(settlement, state); err != nil {
		return nil, err
	}
	actual, loaded := maintenanceSeedExecutors.LoadOrStore(settlement, state)
	if loaded {
		return actual.(*maintenanceSeedExecutor), nil
	}
	return state, nil
}

func seedExecutorForOperationSettlement(
	settlement *shared.OperationSettlement,
) (*operationSeedExecutor, error) {
	if value, ok := operationSeedExecutors.Load(settlement); ok {
		return value.(*operationSeedExecutor), nil
	}
	state := newOperationSeedExecutor()
	if err := bindSeedOnlyOperationExecutor(settlement, state); err != nil {
		return nil, err
	}
	actual, loaded := operationSeedExecutors.LoadOrStore(settlement, state)
	if loaded {
		return actual.(*operationSeedExecutor), nil
	}
	return state, nil
}

func commitOperationSuccessForTest(
	t require.TestingT,
	service operationSettlementService,
	claim shared.OperationIntentClaim,
) shared.OperationReleaseCommitted {
	committed, err := commitOperationSuccessFixture(service, claim)
	require.NoError(t, err)
	return committed
}

func commitOperationSuccessFixture(
	service operationSettlementService,
	claim shared.OperationIntentClaim,
) (shared.OperationReleaseCommitted, error) {
	concrete, ok := concreteOperationSettlementForTest(service)
	if !ok {
		return shared.OperationReleaseCommitted{}, errors.New(
			"operation success fixture requires the concrete settlement",
		)
	}
	state, err := seedExecutorForOperationSettlement(concrete)
	if err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	cleanup, err := state.register(claim.OperationID(), shared.Release{Items: claim.EffectiveItems()})
	if err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	defer cleanup()

	candidate, err := service.PrepareOperationRelease(claim)
	if err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	execution, err := service.StartOperationExecution(candidate)
	if err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	outcome := service.ExecuteOperation(context.Background(), execution)
	success, ok := outcome.(shared.OperationExecutionSuccess)
	if !ok {
		return shared.OperationReleaseCommitted{}, fmt.Errorf(
			"seed execution outcome = %T, want success", outcome,
		)
	}
	committed, err := service.CommitOperationSuccess(success)
	if err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	return committed, nil
}

func activateMaintenanceForTest(
	t require.TestingT,
	settlement *shared.MaintenanceSettlement,
	target shared.MaintenanceReleaseClaim,
) (shared.MaintenanceReleaseActive, error) {
	state, err := seedExecutorForMaintenanceSettlement(settlement)
	require.NoError(t, err)
	cleanup, err := state.register(target.MaintenanceID(), maintenanceSeedTargetReady)
	require.NoError(t, err)
	defer cleanup()
	execution, err := settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := settlement.ExecuteMaintenance(context.Background(), execution)
	success, ok := outcome.(shared.MaintenanceExecutionSuccess)
	require.True(t, ok, "seed maintenance outcome = %T, want success", outcome)
	active, err := settlement.ActivateMaintenance(success)
	require.NoError(t, err)
	return active, nil
}

func registerMaintenanceExecutionForTest(
	t require.TestingT,
	settlement *shared.MaintenanceSettlement,
	target shared.MaintenanceReleaseClaim,
	kind maintenanceSeedKind,
	started chan<- struct{},
	proceed <-chan struct{},
) func() {
	state, err := seedExecutorForMaintenanceSettlement(settlement)
	require.NoError(t, err)
	cleanup, err := state.registerPlan(target.MaintenanceID(), maintenanceSeedPlan{
		kind: kind, started: started, proceed: proceed,
	})
	require.NoError(t, err)
	return cleanup
}

func failMaintenanceForTest(
	t require.TestingT,
	settlement *shared.MaintenanceSettlement,
	target shared.MaintenanceReleaseClaim,
	reason backend.Reason,
	message string,
	sourceRecovered bool,
) (shared.MaintenanceReleaseFailure, error) {
	state, err := seedExecutorForMaintenanceSettlement(settlement)
	require.NoError(t, err)
	kind := maintenanceSeedAbsent
	if sourceRecovered {
		kind = maintenanceSeedSourceReady
	}
	cleanup, err := state.register(target.MaintenanceID(), kind)
	require.NoError(t, err)
	defer cleanup()
	execution, err := settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := settlement.ExecuteMaintenance(context.Background(), execution)
	failure, ok := outcome.(shared.MaintenanceExecutionFailure)
	require.True(t, ok, "seed maintenance outcome = %T, want failure", outcome)
	failed, err := settlement.FailMaintenance(failure, reason, message)
	require.NoError(t, err)
	return failed, nil
}

// runSubjectStorageMutationForTest executes a low-level helper with a real
// one-shot Runner while keeping its facade scoped to one canonical lease. It
// is used only by focused helper tests which intentionally stop below the
// operation/maintenance settlement layer.
func runSubjectStorageMutationForTest[T any](
	t require.TestingT,
	b *Backend,
	leaseUUID string,
	workflow func(*storageMutations) T,
) T {
	return runStorageMutationForTest(t, b, leaseUUID, "tenant-a", workflow)
}

func runTenantStorageMutationForTest[T any](
	t require.TestingT,
	b *Backend,
	leaseUUID, tenant string,
	workflow func(*storageMutations) T,
) T {
	return runStorageMutationForTest(t, b, leaseUUID, tenant, workflow)
}

func runStorageMutationForTest[T any](
	t require.TestingT,
	b *Backend,
	leaseUUID, tenant string,
	workflow func(*storageMutations) T,
) T {
	var result T
	ops, err := storageMutationOperationsForTest(b)
	require.NoError(t, err)
	protocol := substratemutation.NewProtocol[string]()
	binding, err := protocol.NewGuardBinding()
	require.NoError(t, err)
	guard, _, err := substratemutation.NewExecutor(
		binding,
		b.authorizeStorageMutation,
		b.completeStorageMutation,
		func(runner substratemutation.Runner, subject string) *storageMutations {
			return &storageMutations{
				runner: runner, ops: ops,
				leaseUUID: subject, tenant: tenant,
				providerUUID: nominalDockerProviderUUID,
				allowedLease: map[string]struct{}{subject: {}},
			}
		},
		func(_ context.Context, mutations *storageMutations, _ string) error {
			result = workflow(mutations)
			return nil
		},
		func(context.Context, string) (struct{}, error) { return struct{}{}, nil },
	)
	require.NoError(t, err)
	execution, err := protocol.BeginAfter(func() (string, error) { return leaseUUID, nil })
	require.NoError(t, err)
	physical := guard.Execute(execution, context.Background())
	if physical.Kind() == substratemutation.Invalid || physical.Kind() == substratemutation.Refused {
		require.NoError(t, physical.Err())
	}
	return result
}

func inspectImageForSetupForTest(
	t require.TestingT,
	b *Backend,
	ctx context.Context,
	imageName, userOverride string,
) (*imageSetup, error) {
	type inspection struct {
		setup *imageSetup
		err   error
	}
	result := runSubjectStorageMutationForTest(t, b, durableCallbackTestLeaseUUID,
		func(mutations *storageMutations) inspection {
			setup, err := b.inspectImageForSetup(
				mutations, ctx, imageName, userOverride,
			)
			return inspection{setup: setup, err: err}
		},
	)
	return result.setup, result.err
}

func destroyVolumesForTest(
	b *Backend,
	owner string,
	ctx context.Context,
	site string,
	names ...string,
) destroyReport {
	installTestStorageMutationAdapters(b)
	_, _, destroyVolumes, _ := backgroundCapabilitiesForTest(b)
	return b.volumeOp(owner, b.logger).destroy(
		destroyVolumes, ctx, site, names...,
	)
}

func createManagedVolumeForTest(
	t require.TestingT,
	b *Backend,
	ctx context.Context,
	leaseUUID, volumeID string,
	sizeMB int64,
) (string, bool, error) {
	type createResult struct {
		path    string
		created bool
		err     error
	}
	result := runSubjectStorageMutationForTest(t, b, leaseUUID,
		func(mutations *storageMutations) createResult {
			path, created, err := b.createManagedVolume(
				mutations, ctx, volumeID, sizeMB,
			)
			return createResult{path: path, created: created, err: err}
		},
	)
	return result.path, result.created, result.err
}
