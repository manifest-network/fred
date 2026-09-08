package leasesm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backend/shared/workbarrier"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/operationid"
)

func createdMaintenanceDispatch(
	t *testing.T,
	admission shared.MaintenanceIntentAdmission,
) shared.MaintenanceIntentDispatch {
	t.Helper()
	dispatch, ok := admission.CreatedDispatch()
	require.True(t, ok, "maintenance admission must carry first-dispatch authority")
	return dispatch
}

func mustLeaseSMOperationID(text string) shared.OperationID {
	id, err := operationid.Parse(text)
	if err != nil {
		panic(err)
	}
	return id
}

func newBoundLeaseSMCallbackStore(
	t testing.TB,
	dbPath, backendName string,
) *shared.CallbackStore {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, boundPath.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		backendName,
		"leasesm-test-substrate",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				return shared.PrepareBoundCallbackStoreStorage(boundPath, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				return shared.CheckBoundCallbackStoreStorage(boundPath, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				return shared.VerifyBoundCallbackStoreStorage(boundPath, verified)
			},
		},
	)
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	store, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: dbPath}, storage, gate,
	)
	require.NoError(t, err)
	return store
}

func newBoundLeaseSMReleaseStore(
	t testing.TB,
	dbPath, backendName string,
) *shared.ReleaseStore {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, boundPath.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		backendName,
		"leasesm-test-substrate",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(pending backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
				return shared.PrepareBoundReleaseStoreStorage(boundPath, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				return shared.CheckBoundReleaseStoreStorage(boundPath, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				return shared.VerifyBoundReleaseStoreStorage(boundPath, verified)
			},
		},
	)
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	store, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: dbPath}, storage, gate,
	)
	require.NoError(t, err)
	return store
}

func newBoundLeaseSMMaintenanceStores(
	t testing.TB,
	dir, backendName string,
) (*shared.CallbackStore, *shared.ReleaseStore, backendidentity.VerifiedStorage, *backendidentity.StorageAuthorityGate) {
	t.Helper()
	callbackPath := filepath.Join(dir, "callbacks.db")
	releasePath := filepath.Join(dir, "releases.db")
	callbackBound, err := shared.BindAuthoritativeStorePath(callbackPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, callbackBound.Close()) }()
	releaseBound, err := shared.BindAuthoritativeStorePath(releasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, releaseBound.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(dir, "storage-identity.json"),
		filepath.Join(dir, "storage-identity-anchor.json"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		backendName,
		"leasesm-test-substrate",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				if err := shared.PrepareBoundCallbackStoreStorage(callbackBound, pending, profile); err != nil {
					return err
				}
				return shared.PrepareBoundReleaseStoreStorage(releaseBound, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				if err := shared.CheckBoundCallbackStoreStorage(callbackBound, pending); err != nil {
					return err
				}
				return shared.CheckBoundReleaseStoreStorage(releaseBound, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				if err := shared.VerifyBoundCallbackStoreStorage(callbackBound, verified); err != nil {
					return err
				}
				return shared.VerifyBoundReleaseStoreStorage(releaseBound, verified)
			},
		},
	)
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: callbackPath}, storage, gate,
	)
	require.NoError(t, err)
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: releasePath}, storage, gate,
	)
	require.NoError(t, err)
	return callbacks, releases, storage, gate
}

// mockProvisionStore is a real concurrent in-memory implementation of
// LeaseProvisionStore. The closure passed to UpdateFn runs UNDER the
// mutex Lock — the same atomicity contract the Docker backend's
// backendProvisionStore adapter exposes.
//
// IMPORTANT — architect caveat #4: this mock MUST keep the closure
// under the lock. A stub that ignores locking would silently mask any
// regression where the SM holds the closure result across the supposed
// critical section, defeating the purpose of testing closure atomicity.
// TestMockProvisionStore_ConcurrentUpdate (in this file) exercises the
// contract under -race -short; any race detection there halts E
// completion.
type mockProvisionStore struct {
	mu     sync.Mutex
	states map[string]*ProvisionState
}

// leaseSMTestMutation is the narrow no-op physical capability used by actor
// fixtures. It still crosses Runner.Step, so the real construction-bound
// settlement protocol—not a raw release-store escape hatch—mints success.
type leaseSMTestMutation struct{ runner substratemutation.Runner }

func testProjectionForRelease(release shared.Release) ([]string, map[string][]string) {
	var ids []string
	services := make(map[string][]string, len(release.Items))
	for _, item := range release.Items {
		for i := range item.Quantity {
			id := item.ServiceName + "-container-" + string(rune('a'+i))
			ids = append(ids, id)
			services[item.ServiceName] = append(services[item.ServiceName], id)
		}
	}
	return ids, services
}

func bindLeaseSMMaintenanceExecutor(
	t testing.TB,
	settlement *shared.MaintenanceSettlement,
) {
	t.Helper()
	err := shared.BindMaintenanceSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, _ shared.MaintenancePhysicalSubject) leaseSMTestMutation {
			return leaseSMTestMutation{runner: runner}
		},
		func(ctx context.Context, mutation leaseSMTestMutation, _ shared.MaintenancePhysicalSubject) error {
			return mutation.runner.Step(ctx, "leasesm maintenance fixture", func(context.Context) error { return nil })
		},
		func(_ context.Context, subject shared.MaintenancePhysicalSubject) (shared.MaintenancePhysicalEvidence, error) {
			release, ok := subject.TargetRelease()
			if !ok {
				return shared.MaintenancePhysicalEvidence{}, errors.New("maintenance fixture has no target release")
			}
			ids, services := testProjectionForRelease(release)
			return shared.NewMaintenanceTargetReady(subject, ids, services)
		},
	)
	require.NoError(t, err)
}

func bindLeaseSMOperationExecutor(
	t testing.TB,
	settlement *shared.OperationSettlement,
	projection ...ReplaceSuccessProjection,
) {
	t.Helper()
	err := shared.BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, _ shared.OperationPhysicalSubject) leaseSMTestMutation {
			return leaseSMTestMutation{runner: runner}
		},
		func(ctx context.Context, mutation leaseSMTestMutation, _ shared.OperationPhysicalSubject) error {
			return mutation.runner.Step(ctx, "leasesm operation fixture", func(context.Context) error { return nil })
		},
		func(_ context.Context, subject shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
			if len(projection) != 0 {
				return shared.NewOperationTargetReady(
					subject, projection[0].ContainerIDs, projection[0].ServiceContainers,
				)
			}
			release, ok := subject.ExpectedRelease()
			if !ok {
				return shared.OperationPhysicalEvidence{}, errors.New("operation fixture has no expected release")
			}
			ids, services := testProjectionForRelease(release)
			return shared.NewOperationTargetReady(subject, ids, services)
		},
	)
	require.NoError(t, err)
}

const testActorLeaseUUID = "11111111-1111-4111-8111-111111111111"

func newMockProvisionStore() *mockProvisionStore {
	return &mockProvisionStore{states: make(map[string]*ProvisionState)}
}

// put seeds a state. Tests call this in setup before the actor runs.
// Takes a shallow copy so a mutation through put doesn't alias outer
// state in surprising ways.
func (m *mockProvisionStore) put(uuid string, state *ProvisionState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := *state
	m.states[uuid] = &snap
}

// remove deletes a state. Used by tests simulating handleDeprovision's
// post-removal state without driving the full SM transition.
func (m *mockProvisionStore) remove(uuid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, uuid)
}

// Get returns a shallow value-copy snapshot. Matches the
// LeaseProvisionStore contract (callers do NOT hold the lock; slices
// and maps share the underlying record).
func (m *mockProvisionStore) Get(uuid string) (*ProvisionState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.states[uuid]
	if !ok {
		return nil, false
	}
	snap := *p
	return &snap, true
}

func (m *mockProvisionStore) LookupStatus(uuid string) (backend.ProvisionStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.states[uuid]
	if !ok {
		return backend.ProvisionStatusUnknown, false
	}
	return p.Status, true
}

func (m *mockProvisionStore) Exists(uuid string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.states[uuid]
	return ok
}

func classifyMockReadyProjection(
	state *ProvisionState,
	proof shared.RuntimeGenerationProof,
	instanceID string,
) ObservationGenerationState {
	if state == nil {
		return ObservationGenerationAbsent
	}
	if !proof.Valid() || state.LeaseUUID != proof.LeaseUUID() ||
		state.ActiveReleaseVersion <= 0 || state.ActiveReleaseVersion != proof.Version() {
		return ObservationGenerationSuperseded
	}
	switch proof.AuthorityClass() {
	case shared.ReleaseAuthorityTyped:
		if !state.ActiveOperationID.Valid() || state.ActiveOperationID != proof.OperationID() {
			return ObservationGenerationSuperseded
		}
	case shared.ReleaseAuthorityLegacy:
		if !state.ActiveOperationID.IsZero() || !proof.OperationID().IsZero() {
			return ObservationGenerationSuperseded
		}
	default:
		return ObservationGenerationSuperseded
	}
	if state.Status != backend.ProvisionStatusReady {
		return ObservationGenerationAdvanced
	}
	if instanceID != "" && !slices.Contains(state.ContainerIDs, instanceID) {
		return ObservationGenerationAdvanced
	}
	return ObservationGenerationCurrent
}

func (m *mockProvisionStore) ClassifyReadyRuntime(
	proof shared.RuntimeGenerationProof,
) ObservationGenerationState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return classifyMockReadyProjection(m.states[proof.LeaseUUID()], proof, "")
}

func (m *mockProvisionStore) ClassifyReadyInstance(
	proof shared.RuntimeGenerationProof,
	instanceID string,
) ObservationGenerationState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return classifyMockReadyProjection(m.states[proof.LeaseUUID()], proof, instanceID)
}

// UpdateFn runs fn UNDER the lock, mirroring backendProvisionStore. The
// closure idempotence requirement is the caller's responsibility (see
// LeaseProvisionStore docstring in leasesm.go).
func (m *mockProvisionStore) UpdateFn(uuid string, fn func(*ProvisionState)) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.states[uuid]
	if !ok {
		return false
	}
	fn(p)
	return true
}

// Delete implements LeaseProvisionStore. Removes the seeded state under the
// same mutex as Get/UpdateFn. Returns true if present.
func (m *mockProvisionStore) Delete(uuid string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.states[uuid]
	delete(m.states, uuid)
	return ok
}

// mockSMMetrics is a no-op SMMetrics implementation for tests that
// don't assert on metric emission. Tests that need to assert on
// specific metric calls can wrap their own counters via the function-
// field stub pattern shown below (see mockInstanceInspector).
type mockSMMetrics struct{}

func (mockSMMetrics) SMTransition(_, _, _ string)   {}
func (mockSMMetrics) ActorCreated()                 {}
func (mockSMMetrics) WorkerPanic(_ string)          {}
func (mockSMMetrics) ActorPanic()                   {}
func (mockSMMetrics) TerminalEventDropped(_ string) {}

// mockInstanceInspector implements InstanceInspector with a function-
// field stub. Tests set InspectInstanceFn to control the inspect
// result for a specific scenario.
type mockInstanceInspector struct {
	InspectInstanceFn func(ctx context.Context, instanceID string) (*InstanceState, error)
}

func (m *mockInstanceInspector) InspectInstance(ctx context.Context, instanceID string) (*InstanceState, error) {
	if m.InspectInstanceFn != nil {
		return m.InspectInstanceFn(ctx, instanceID)
	}
	return nil, nil
}

// mockDiagnosticsGatherer implements DiagnosticsGatherer with a
// function-field stub. Tests set GatherDiagnosticsFn to control the
// gathered diagnostic string for a specific scenario.
type mockDiagnosticsGatherer struct {
	GatherDiagnosticsFn func(ctx context.Context, instanceID string, state *InstanceState) string
}

func (m *mockDiagnosticsGatherer) GatherDiagnostics(ctx context.Context, instanceID string, state *InstanceState) string {
	if m.GatherDiagnosticsFn != nil {
		return m.GatherDiagnosticsFn(ctx, instanceID, state)
	}
	return ""
}

// testActorOpts groups the optional dependencies newTestActor accepts.
// All fields are zero-value safe: omitted fields get a no-op default
// from newTestActor so tests only specify what they care about. Tests
// that need specific behavior set the relevant field; everything else
// inherits sensible defaults.
type testActorOpts struct {
	Logger                       *slog.Logger
	StopCtx                      context.Context
	WG                           *sync.WaitGroup
	WorkerDrainTimeout           time.Duration
	Inspector                    InstanceInspector
	Diag                         DiagnosticsGatherer
	ProvisionStore               LeaseProvisionStore
	Metrics                      SMMetrics
	ProvisionWorkFn              func(context.Context, shared.OperationIntentClaim) ProvisionWorkOutcome
	RestoreWorkFn                func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome
	MaintenanceWorkFn            func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome
	OnTerminated                 func(uuid string)
	PersistDiagnosticsFn         func(entry shared.DiagnosticEntry, ids []string, keys map[string]string)
	PersistDiagnosticsWithLogsFn func(entry shared.DiagnosticEntry, logs map[string]string)
	SendOperationCallbackFn      func(uuid, url string, status backend.CallbackStatus, errMsg string)
	SendLifecycleFailureFn       func(runtime shared.RuntimeGenerationProof, errMsg string)
	SendMaintenanceCallbackFn    func(claim shared.MaintenanceIntentClaim, status backend.CallbackStatus, errMsg string)
	DoDeprovisionFn              func(ctx context.Context, scope ActorCloseScope) error
}

func testRecoveryLineage(t *testing.T) shared.RecoveryLineage {
	t.Helper()
	return mustTestRecoveryLineage()
}

func mustTestRecoveryLineage() shared.RecoveryLineage {
	coordinator, err := shared.NewRecoveryCoordinator(shared.RecoveryCoordinatorConfig{
		ExcludeLease: func(_ context.Context, _ string, run func() error) (bool, error) {
			return true, run()
		},
	})
	if err != nil {
		panic(err)
	}
	return coordinator.Lineage()
}

// newTestActor constructs a LeaseActor wired to the supplied test
// fixtures. Defaults: discarded logger, context.Background StopCtx, a
// fresh WaitGroup, a fresh mockProvisionStore seeded with a Provisioning
// entry for leaseUUID, mockSMMetrics, no-op InstanceInspector and
// DiagnosticsGatherer, no-op callback / persist / deprovision closures.
//
// Tests that need to read or pre-seed the provision state must construct
// the store themselves and pass it in via opts.ProvisionStore — opts is
// passed by value, so the defaulted store created here is NOT observable
// to the caller after newTestActor returns. Typical pattern:
//
//	store := newMockProvisionStore()
//	store.put(leaseUUID, &ProvisionState{Status: backend.ProvisionStatusReady})
//	actor := newTestActor(t, leaseUUID, testActorOpts{ProvisionStore: store, ...})
//	// later: store.Get(leaseUUID) is observable here
//
// The actor's run-loop goroutine starts inside NewLeaseActor. Tests
// that want to drive the actor synchronously must take care to either:
//   - cancel opts.StopCtx to terminate the loop before assertions
//   - block on <-actor.Done() to wait for full quiescence
//   - or test against the SM/actor state without ever sending a message
func newTestActor(t *testing.T, leaseUUID string, opts testActorOpts) *LeaseActor {
	t.Helper()

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.StopCtx == nil {
		opts.StopCtx = context.Background()
	}
	if opts.WG == nil {
		opts.WG = &sync.WaitGroup{}
	}
	if opts.Inspector == nil {
		opts.Inspector = &mockInstanceInspector{}
	}
	if opts.Diag == nil {
		opts.Diag = &mockDiagnosticsGatherer{}
	}
	if opts.ProvisionStore == nil {
		store := newMockProvisionStore()
		store.put(leaseUUID, &ProvisionState{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusProvisioning,
		})
		opts.ProvisionStore = store
	}
	if opts.Metrics == nil {
		opts.Metrics = mockSMMetrics{}
	}
	if opts.ProvisionWorkFn == nil {
		opts.ProvisionWorkFn = func(_ context.Context, claim shared.OperationIntentClaim) ProvisionWorkOutcome {
			outcome, _ := NewProvisionWorkAmbiguous(errors.New("test provision work not configured"), claim)
			return outcome
		}
	}
	if opts.RestoreWorkFn == nil {
		opts.RestoreWorkFn = func(_ context.Context, claim shared.OperationIntentClaim) ReplaceWorkOutcome {
			outcome, _ := NewAmbiguousRestoreWork(errors.New("test restore work not configured"), claim)
			return outcome
		}
	}
	if opts.MaintenanceWorkFn == nil {
		opts.MaintenanceWorkFn = func(_ context.Context, target shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			outcome, _ := NewAmbiguousMaintenanceWork(
				errors.New("test maintenance work not configured"), target.Intent(),
			)
			return outcome
		}
	}
	if opts.OnTerminated == nil {
		opts.OnTerminated = func(string) {}
	}
	if opts.PersistDiagnosticsFn == nil {
		opts.PersistDiagnosticsFn = func(shared.DiagnosticEntry, []string, map[string]string) {}
	}
	if opts.PersistDiagnosticsWithLogsFn == nil {
		opts.PersistDiagnosticsWithLogsFn = func(shared.DiagnosticEntry, map[string]string) {}
	}
	if opts.SendOperationCallbackFn == nil {
		opts.SendOperationCallbackFn = func(string, string, backend.CallbackStatus, string) {}
	}
	if opts.SendLifecycleFailureFn == nil {
		opts.SendLifecycleFailureFn = func(shared.RuntimeGenerationProof, string) {}
	}
	if opts.SendMaintenanceCallbackFn == nil {
		opts.SendMaintenanceCallbackFn = func(shared.MaintenanceIntentClaim, backend.CallbackStatus, string) {}
	}
	if opts.DoDeprovisionFn == nil {
		opts.DoDeprovisionFn = func(context.Context, ActorCloseScope) error { return nil }
	}

	var actor *LeaseActor
	actor, err := NewLeaseActor(LeaseActorConfig{
		LeaseUUID:                    leaseUUID,
		Logger:                       opts.Logger,
		StopCtx:                      opts.StopCtx,
		WG:                           opts.WG,
		WorkerDrainTimeout:           opts.WorkerDrainTimeout,
		Inspector:                    opts.Inspector,
		Diag:                         opts.Diag,
		ProvisionStore:               opts.ProvisionStore,
		Metrics:                      opts.Metrics,
		ProvisionWorkFn:              opts.ProvisionWorkFn,
		RestoreWorkFn:                opts.RestoreWorkFn,
		MaintenanceWorkFn:            opts.MaintenanceWorkFn,
		OnTerminated:                 func(uuid string, _ *LeaseActor) { opts.OnTerminated(uuid) },
		PersistDiagnosticsFn:         opts.PersistDiagnosticsFn,
		PersistDiagnosticsWithLogsFn: opts.PersistDiagnosticsWithLogsFn,
		SendOperationSuccessFn: func(shared.OperationReleaseCommitted) {
			url := ""
			opts.ProvisionStore.UpdateFn(leaseUUID, func(state *ProvisionState) { url = state.CallbackURL })
			opts.SendOperationCallbackFn(leaseUUID, url, backend.CallbackStatusSuccess, "")
		},
		SendOperationFailureFn: func(_ shared.OperationReleaseUncommitted, errMsg string) {
			url := ""
			opts.ProvisionStore.UpdateFn(leaseUUID, func(state *ProvisionState) { url = state.CallbackURL })
			opts.SendOperationCallbackFn(leaseUUID, url, backend.CallbackStatusFailed, errMsg)
		},
		SendLifecycleFailureFn: opts.SendLifecycleFailureFn,
		SendMaintenanceSuccessFn: func(active shared.MaintenanceReleaseActive) {
			claim := active.Intent()
			if !claim.Valid() {
				// Older state-transition tests deliberately use a zero worker proof;
				// retain their callback-routing assertion without weakening the
				// production proof-only function type.
				claim = actor.pendingMaintenance
			}
			opts.SendMaintenanceCallbackFn(claim, backend.CallbackStatusSuccess, "")
		},
		SendMaintenanceFailureFn: func(failed shared.MaintenanceReleaseFailure, errMsg string) {
			claim := failed.Intent()
			if !claim.Valid() {
				claim = actor.pendingMaintenance
			}
			opts.SendMaintenanceCallbackFn(claim, backend.CallbackStatusFailed, errMsg)
		},
		RecoveryLineage: testRecoveryLineage(t),
		DoDeprovisionFn: opts.DoDeprovisionFn,
	}, func(*LeaseActor) {})
	require.NoError(t, err)
	return actor
}

// newTestActorNoSpawn constructs a LeaseActor without spawning its
// run-loop goroutine. Test-only seam for tests that need to observe
// inbox state or invoke internal methods (gatherDiagAsync, handle*Msg)
// synchronously from the test goroutine without the run loop racing to
// drain. This is a leasesm-internal helper — it lives in a _test.go
// file so it's only available to leasesm-internal tests; production
// code MUST use NewLeaseActor (which always spawns) because
// registry-spawn atomicity is load-bearing under production routing.
//
// Mirrors newTestActor's defaults precisely; the only difference is
// the missing WG.Go(a.run) call.
func newTestActorNoSpawn(t *testing.T, leaseUUID string, opts testActorOpts) *LeaseActor {
	t.Helper()

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.StopCtx == nil {
		opts.StopCtx = context.Background()
	}
	if opts.WG == nil {
		opts.WG = &sync.WaitGroup{}
	}
	if opts.Inspector == nil {
		opts.Inspector = &mockInstanceInspector{}
	}
	if opts.Diag == nil {
		opts.Diag = &mockDiagnosticsGatherer{}
	}
	if opts.ProvisionStore == nil {
		store := newMockProvisionStore()
		store.put(leaseUUID, &ProvisionState{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusProvisioning,
		})
		opts.ProvisionStore = store
	}
	if opts.Metrics == nil {
		opts.Metrics = mockSMMetrics{}
	}
	if opts.ProvisionWorkFn == nil {
		opts.ProvisionWorkFn = func(_ context.Context, claim shared.OperationIntentClaim) ProvisionWorkOutcome {
			outcome, _ := NewProvisionWorkAmbiguous(errors.New("test provision work not configured"), claim)
			return outcome
		}
	}
	if opts.RestoreWorkFn == nil {
		opts.RestoreWorkFn = func(_ context.Context, claim shared.OperationIntentClaim) ReplaceWorkOutcome {
			outcome, _ := NewAmbiguousRestoreWork(errors.New("test restore work not configured"), claim)
			return outcome
		}
	}
	if opts.MaintenanceWorkFn == nil {
		opts.MaintenanceWorkFn = func(_ context.Context, target shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			outcome, _ := NewAmbiguousMaintenanceWork(
				errors.New("test maintenance work not configured"), target.Intent(),
			)
			return outcome
		}
	}
	if opts.OnTerminated == nil {
		opts.OnTerminated = func(string) {}
	}
	if opts.PersistDiagnosticsFn == nil {
		opts.PersistDiagnosticsFn = func(shared.DiagnosticEntry, []string, map[string]string) {}
	}
	if opts.PersistDiagnosticsWithLogsFn == nil {
		opts.PersistDiagnosticsWithLogsFn = func(shared.DiagnosticEntry, map[string]string) {}
	}
	if opts.SendOperationCallbackFn == nil {
		opts.SendOperationCallbackFn = func(string, string, backend.CallbackStatus, string) {}
	}
	if opts.SendLifecycleFailureFn == nil {
		opts.SendLifecycleFailureFn = func(shared.RuntimeGenerationProof, string) {}
	}
	if opts.SendMaintenanceCallbackFn == nil {
		opts.SendMaintenanceCallbackFn = func(shared.MaintenanceIntentClaim, backend.CallbackStatus, string) {}
	}
	if opts.DoDeprovisionFn == nil {
		opts.DoDeprovisionFn = func(context.Context, ActorCloseScope) error { return nil }
	}

	a := &LeaseActor{
		inbox:               make(chan leaseMessage, leaseActorInboxSize),
		done:                make(chan struct{}),
		retirementRequested: make(chan struct{}),
		workers:             workbarrier.New(),
	}
	a.cfg = LeaseActorConfig{
		LeaseUUID:                    leaseUUID,
		Logger:                       opts.Logger,
		StopCtx:                      opts.StopCtx,
		WG:                           opts.WG,
		WorkerDrainTimeout:           opts.WorkerDrainTimeout,
		Inspector:                    opts.Inspector,
		Diag:                         opts.Diag,
		ProvisionStore:               opts.ProvisionStore,
		Metrics:                      opts.Metrics,
		ProvisionWorkFn:              opts.ProvisionWorkFn,
		RestoreWorkFn:                opts.RestoreWorkFn,
		MaintenanceWorkFn:            opts.MaintenanceWorkFn,
		OnTerminated:                 func(uuid string, _ *LeaseActor) { opts.OnTerminated(uuid) },
		PersistDiagnosticsFn:         opts.PersistDiagnosticsFn,
		PersistDiagnosticsWithLogsFn: opts.PersistDiagnosticsWithLogsFn,
		SendOperationSuccessFn: func(shared.OperationReleaseCommitted) {
			url := ""
			opts.ProvisionStore.UpdateFn(leaseUUID, func(state *ProvisionState) { url = state.CallbackURL })
			opts.SendOperationCallbackFn(leaseUUID, url, backend.CallbackStatusSuccess, "")
		},
		SendOperationFailureFn: func(_ shared.OperationReleaseUncommitted, errMsg string) {
			url := ""
			opts.ProvisionStore.UpdateFn(leaseUUID, func(state *ProvisionState) { url = state.CallbackURL })
			opts.SendOperationCallbackFn(leaseUUID, url, backend.CallbackStatusFailed, errMsg)
		},
		SendLifecycleFailureFn: opts.SendLifecycleFailureFn,
		SendMaintenanceSuccessFn: func(active shared.MaintenanceReleaseActive) {
			claim := active.Intent()
			if !claim.Valid() {
				claim = a.pendingMaintenance
			}
			opts.SendMaintenanceCallbackFn(claim, backend.CallbackStatusSuccess, "")
		},
		SendMaintenanceFailureFn: func(failed shared.MaintenanceReleaseFailure, errMsg string) {
			claim := failed.Intent()
			if !claim.Valid() {
				claim = a.pendingMaintenance
			}
			opts.SendMaintenanceCallbackFn(claim, backend.CallbackStatusFailed, errMsg)
		},
		RecoveryLineage: testRecoveryLineage(t),
		DoDeprovisionFn: opts.DoDeprovisionFn,
	}
	a.leaseUUID = a.cfg.LeaseUUID
	a.sm = newLeaseSM(a)
	a.cfg.Metrics.ActorCreated()
	// INTENTIONALLY skip a.cfg.WG.Go(a.run) — that's the entire point of
	// this no-spawn variant. Tests that use this helper drive the actor
	// manually (e.g., by calling gatherDiagAsync directly on the test
	// goroutine, or by inspecting the inbox without a draining run loop).
	return a
}

func newTestMaintenanceClaim(
	t *testing.T,
	leaseUUID string,
	kind shared.MaintenanceIntentKind,
) shared.MaintenanceIntentClaim {
	t.Helper()
	maintenanceID, err := maintenanceid.New()
	require.NoError(t, err)
	return newTestMaintenanceClaimWithID(t, leaseUUID, kind, maintenanceID)
}

func newTestMaintenanceClaimWithID(
	t *testing.T,
	leaseUUID string,
	kind shared.MaintenanceIntentKind,
	maintenanceID shared.MaintenanceID,
) shared.MaintenanceIntentClaim {
	t.Helper()
	dir := t.TempDir()
	callbacks, releases, storage, gate := newBoundLeaseSMMaintenanceStores(t, dir, "docker-a")
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

	operationID := mustLeaseSMOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleURL := "https://fred.example/callbacks/provision?lifecycle_id=" + operationID.String()
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID,
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		callbackURL,
		lifecycleURL,
	)
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}}
	source := shared.Release{
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		RuntimeAuthority: &authority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}
	appendActiveOperationReleaseForTest(t, callbacks, releases, storage, gate, leaseUUID, source)
	settlement, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	bindLeaseSMMaintenanceExecutor(t, settlement)
	active, sourceClaim, err := settlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	active.Version = 0
	active.Status = "deploying"
	active.CreatedAt = time.Now()
	payload := []byte(nil)
	if kind != shared.MaintenanceIntentRestart {
		payload = active.Manifest
	}
	request, err := settlement.NewMaintenanceRequestAuthority(
		maintenanceID, kind, leaseUUID, lifecycleURL, payload,
	)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, sourceClaim, active)
	require.NoError(t, err)
	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	targetClaim, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = settlement.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	intent := targetClaim.Intent()
	maintenanceAuthorities.Store(intent.MaintenanceID(), testMaintenanceAuthority{
		settlement: settlement,
		callbacks:  callbacks,
		target:     targetClaim,
		releases:   releases,
	})
	t.Cleanup(func() { maintenanceAuthorities.Delete(intent.MaintenanceID()) })
	return intent
}

type testMaintenanceAuthority struct {
	settlement *shared.MaintenanceSettlement
	callbacks  *shared.CallbackStore
	target     shared.MaintenanceReleaseClaim
	releases   *shared.ReleaseStore
}

var maintenanceAuthorities sync.Map

func testMaintenanceTarget(t *testing.T, intent shared.MaintenanceIntentClaim) shared.MaintenanceReleaseClaim {
	t.Helper()
	value, ok := maintenanceAuthorities.Load(intent.MaintenanceID())
	require.True(t, ok, "test maintenance authority is unavailable")
	return value.(testMaintenanceAuthority).target
}

func testMaintenanceSuccess(t *testing.T, intent shared.MaintenanceIntentClaim, projection ReplaceSuccessProjection) ReplaceResult {
	t.Helper()
	projection = completeTestReplaceProjection(projection)
	value, ok := maintenanceAuthorities.Load(intent.MaintenanceID())
	require.True(t, ok, "test maintenance authority is unavailable")
	authority := value.(testMaintenanceAuthority)
	execution, err := authority.settlement.StartMaintenanceExecution(authority.target)
	require.NoError(t, err)
	physical := authority.settlement.ExecuteMaintenance(context.Background(), execution)
	success, ok := physical.(shared.MaintenanceExecutionSuccess)
	require.True(t, ok)
	proof, err := authority.settlement.ActivateMaintenance(success)
	require.NoError(t, err)
	target, ok := proof.TargetRelease()
	require.True(t, ok)
	stack, err := validateCompleteReleaseProjection(target, projection.ContainerIDs, projection.ServiceContainers)
	require.NoError(t, err)
	result := newReplaceSuccessProjection(projection)
	result.authorityKind = replaceAuthorityMaintenance
	result.maintenanceRelease = proof
	result.maintenance = proof.Intent()
	result.release = &target
	result.stackManifest = stack
	return ReplaceResult{success: result}
}

func testMaintenanceFailure(t *testing.T, intent shared.MaintenanceIntentClaim, errValue error, restored, recoverFromSource bool, details ReplaceFailureDetails) ReplaceResult {
	t.Helper()
	value, ok := maintenanceAuthorities.Load(intent.MaintenanceID())
	require.True(t, ok, "test maintenance authority is unavailable")
	authority := value.(testMaintenanceAuthority)
	failure, err := authority.settlement.RefuseMaintenanceExecution(authority.target)
	require.NoError(t, err)
	proof, err := authority.settlement.FailMaintenance(failure, details.Reason, details.CallbackErr)
	require.NoError(t, err)
	result, err := NewMaintenanceReplaceFailure(errValue, restored, recoverFromSource, details, proof)
	require.NoError(t, err)
	return result
}

// appendActiveOperationReleaseForTest seeds an active typed generation through
// the same write-ahead and release-handoff protocol used by production. Actor
// tests must not regain a raw ReleaseStore mutation seam merely to prepare a
// projection fixture.
func appendActiveOperationReleaseForTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
	releases *shared.ReleaseStore,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
	leaseUUID string,
	release shared.Release,
) {
	t.Helper()
	authority, ok := release.RuntimeIdentity()
	require.True(t, ok)
	settlement, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	candidate, err := settlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          authority.CallbackURL(),
		LifecycleCallbackURL: authority.LifecycleCallbackURL(),
		Tenant:               authority.Tenant(),
		ProviderUUID:         authority.ProviderUUID(),
		Items:                release.Items,
		ResourceProfiles:     release.ResourceProfiles,
		EffectiveItems:       release.Items,
		Manifest:             release.Manifest,
	})
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, ok := admission.CreatedClaim()
	require.True(t, ok)
	releaseCandidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	bindLeaseSMOperationExecutor(t, settlement)
	execution, err := settlement.StartOperationExecution(releaseCandidate)
	require.NoError(t, err)
	physical := settlement.ExecuteOperation(context.Background(), execution)
	success, ok := physical.(shared.OperationExecutionSuccess)
	require.True(t, ok)
	committed, err := settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	attestor, err := shared.NewCallbackStorageAttestor(
		callbacks,
		leaseSMCallbackStorageVerifier{storage: storage, gate: gate},
		context.Background(),
	)
	require.NoError(t, err)
	publisher, err := shared.NewCallbackPublisher(shared.CallbackPublisherConfig{
		OperationSettlement: settlement, MaintenanceSettlement: maintenance,
		StorageAttestor: attestor, Logger: slog.Default(),
	})
	require.NoError(t, err)
	require.NoError(t, publisher.PublishOperationSuccessContext(context.Background(), committed))
}

type leaseSMCallbackStorageVerifier struct {
	storage backendidentity.VerifiedStorage
	gate    *backendidentity.StorageAuthorityGate
}

func (v leaseSMCallbackStorageVerifier) StorageIdentity() backendidentity.ID { return v.storage.ID() }

func (v leaseSMCallbackStorageVerifier) StorageAuthorityGate() *backendidentity.StorageAuthorityGate {
	return v.gate
}

func (v leaseSMCallbackStorageVerifier) Verify(context.Context) error { return nil }

type testOperationFixture struct {
	claim      shared.OperationIntentClaim
	candidate  shared.OperationReleaseCandidate
	settlement *shared.OperationSettlement
	releases   *shared.ReleaseStore
}

func newTestOperationFixture(
	t *testing.T,
	leaseUUID string,
	kind shared.OperationIntentKind,
) testOperationFixture {
	t.Helper()
	dir := t.TempDir()
	callbacks, releases, _, _ := newBoundLeaseSMMaintenanceStores(t, dir, "docker-a")
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	operationID := mustLeaseSMOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleURL := "https://fred.example/callbacks/provision?lifecycle_id=" + operationID.String()
	spec := shared.OperationIntentSpec{
		Kind: kind, LeaseUUID: leaseUUID, CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Tenant: "tenant-a", ProviderUUID: "22222222-2222-4222-8222-222222222222",
		Items:            []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
	}
	if kind == shared.OperationIntentRestore {
		spec.SourceLeaseUUID = "33333333-3333-4333-8333-333333333333"
		spec.SourceGeneration = 1
	}
	settlement, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	candidate, err := settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, ok := admission.CreatedClaim()
	require.True(t, ok)
	releaseCandidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	return testOperationFixture{
		claim: claim, candidate: releaseCandidate,
		settlement: settlement, releases: releases,
	}
}

func newTestOperationSuccess(
	t *testing.T,
	leaseUUID string,
	kind shared.OperationIntentKind,
) (shared.OperationIntentClaim, shared.OperationReleaseCommitted, *shared.ReleaseStore) {
	t.Helper()
	fixture := newTestOperationFixture(t, leaseUUID, kind)
	settlement := fixture.settlement
	bindLeaseSMOperationExecutor(t, settlement)
	execution, err := settlement.StartOperationExecution(fixture.candidate)
	require.NoError(t, err)
	physical := settlement.ExecuteOperation(context.Background(), execution)
	success, ok := physical.(shared.OperationExecutionSuccess)
	require.True(t, ok)
	committed, err := settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	return fixture.claim, committed, fixture.releases
}

func newTestOperationFailure(
	t *testing.T,
	leaseUUID string,
	kind shared.OperationIntentKind,
) (shared.OperationIntentClaim, shared.OperationReleaseUncommitted) {
	t.Helper()
	fixture := newTestOperationFixture(t, leaseUUID, kind)
	refused, err := fixture.settlement.RefuseOperationExecution(fixture.candidate)
	require.NoError(t, err)
	failure, err := fixture.settlement.CommitOperationFailure(refused)
	require.NoError(t, err)
	return fixture.claim, failure
}

func newTestRuntimeGenerationProof(
	t *testing.T,
	leaseUUID string,
) shared.RuntimeGenerationProof {
	t.Helper()
	_, _, releases := newTestOperationSuccess(
		t, leaseUUID, shared.OperationIntentProvision,
	)
	proof, err := releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	return proof
}

func testProvisionSuccess(t *testing.T, leaseUUID string, projection ProvisionSuccessProjection) (shared.OperationIntentClaim, ProvisionSuccessResult) {
	t.Helper()
	if len(projection.ContainerIDs) == 0 {
		projection.ContainerIDs = []string{"container-a"}
	}
	if projection.ServiceContainers == nil {
		projection.ServiceContainers = map[string][]string{"app": append([]string(nil), projection.ContainerIDs...)}
	}
	claim, committed, _ := newTestOperationSuccess(t, leaseUUID, shared.OperationIntentProvision)
	result, err := NewProvisionSuccessResult(projection, committed)
	require.NoError(t, err)
	return claim, result
}

func testRestoreSuccess(t *testing.T, leaseUUID string, projection ReplaceSuccessProjection) (shared.OperationIntentClaim, ReplaceResult) {
	t.Helper()
	projection = completeTestReplaceProjection(projection)
	fixture := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore)
	bindLeaseSMOperationExecutor(t, fixture.settlement, projection)
	execution, err := fixture.settlement.StartOperationExecution(fixture.candidate)
	require.NoError(t, err)
	physical := fixture.settlement.ExecuteOperation(context.Background(), execution)
	success, ok := physical.(shared.OperationExecutionSuccess)
	require.True(t, ok)
	committed, err := fixture.settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	outcome, err := NewRestoreWorkSuccess(committed)
	require.NoError(t, err)
	terminal, ok := outcome.(replaceWorkTerminal)
	require.True(t, ok)
	return fixture.claim, terminal.result
}

func completeTestReplaceProjection(projection ReplaceSuccessProjection) ReplaceSuccessProjection {
	if len(projection.ContainerIDs) == 0 {
		projection.ContainerIDs = []string{"container-a"}
	}
	if projection.ServiceContainers == nil {
		projection.ServiceContainers = map[string][]string{"app": append([]string(nil), projection.ContainerIDs...)}
	}
	return projection
}

// TestMockProvisionStore_ConcurrentUpdate validates that the mock's
// closure-under-lock contract holds under race-detector pressure.
// Architect refinement #5 mandate: at least one test exercises
// concurrent UpdateFn + Get under -race -short. Without this test, a
// "real concurrent" mock that's never exercised concurrently wouldn't
// satisfy architect caveat #4 — the closure-atomicity guarantee would
// be effectively unverified.
//
// The FailCount == N assertion is load-bearing: if the closure runs
// outside the lock (or if a "lost update" occurs because two goroutines
// both read the pre-increment value), FailCount comes out less than N.
// Under sync.Mutex + closure-under-lock semantics, every UpdateFn call
// commits its increment, so the final FailCount equals exactly N.
func TestMockProvisionStore_ConcurrentUpdate(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{LeaseUUID: "lease-1", FailCount: 0})

	const N = 100
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			store.UpdateFn("lease-1", func(p *ProvisionState) {
				p.FailCount++
			})
		}()
		go func() {
			defer wg.Done()
			_, _ = store.Get("lease-1")
		}()
	}
	wg.Wait()

	final, ok := store.Get("lease-1")
	if !ok {
		t.Fatal("provision state vanished from store under concurrent access")
	}
	if final.FailCount != N {
		t.Fatalf("FailCount = %d, want %d — closure-under-lock contract violated "+
			"(lost updates indicate the mock is not serializing UpdateFn calls)",
			final.FailCount, N)
	}
}
