package leasesm

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/workbarrier"
)

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

// mockSMMetrics is a no-op SMMetrics implementation for tests that
// don't assert on metric emission. Tests that need to assert on
// specific metric calls can wrap their own counters via the function-
// field stub pattern shown below (see mockInstanceInspector).
type mockSMMetrics struct{}

func (mockSMMetrics) SMTransition(_, _, _ string)  {}
func (mockSMMetrics) ActorCreated()                {}
func (mockSMMetrics) FailingRaceSkipped()          {}
func (mockSMMetrics) WorkerPanic(_ string)         {}
func (mockSMMetrics) ActorPanic()                  {}
func (mockSMMetrics) TerminalEventDropped(_ string) {}
func (mockSMMetrics) ActiveProvisionsInc()         {}
func (mockSMMetrics) ActiveProvisionsDec()         {}

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
	Inspector                    InstanceInspector
	Diag                         DiagnosticsGatherer
	CallbackSender               *shared.CallbackSender
	ProvisionStore               LeaseProvisionStore
	Metrics                      SMMetrics
	OnTerminated                 func(uuid string)
	PersistDiagnosticsFn         func(entry shared.DiagnosticEntry, ids []string, keys map[string]string)
	PersistDiagnosticsWithLogsFn func(entry shared.DiagnosticEntry, logs map[string]string)
	SendCallbackFn               func(uuid, url string, status backend.CallbackStatus, errMsg string)
	DoDeprovisionFn              func(ctx context.Context, leaseUUID string) error
}

// newTestActor constructs a LeaseActor wired to the supplied test
// fixtures. Defaults: discarded logger, context.Background StopCtx, a
// fresh WaitGroup, a fresh mockProvisionStore seeded with a Provisioning
// entry for leaseUUID, mockSMMetrics, no-op InstanceInspector and
// DiagnosticsGatherer, no-op callback / persist / deprovision closures.
//
// Tests that need a real ProvisionState must either set opts.ProvisionStore
// to their own pre-seeded store, OR call store.put(...) before creating
// the actor (the default store is freshly empty but is returned for
// access via the caller's opts.ProvisionStore reference).
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
	if opts.OnTerminated == nil {
		opts.OnTerminated = func(string) {}
	}
	if opts.PersistDiagnosticsFn == nil {
		opts.PersistDiagnosticsFn = func(shared.DiagnosticEntry, []string, map[string]string) {}
	}
	if opts.PersistDiagnosticsWithLogsFn == nil {
		opts.PersistDiagnosticsWithLogsFn = func(shared.DiagnosticEntry, map[string]string) {}
	}
	if opts.SendCallbackFn == nil {
		opts.SendCallbackFn = func(string, string, backend.CallbackStatus, string) {}
	}
	if opts.DoDeprovisionFn == nil {
		opts.DoDeprovisionFn = func(context.Context, string) error { return nil }
	}

	return NewLeaseActor(func(a *LeaseActor) LeaseActorConfig {
		return LeaseActorConfig{
			LeaseUUID:                    leaseUUID,
			Logger:                       opts.Logger,
			StopCtx:                      opts.StopCtx,
			WG:                           opts.WG,
			Inspector:                    opts.Inspector,
			Diag:                         opts.Diag,
			CallbackSender:               opts.CallbackSender,
			ProvisionStore:               opts.ProvisionStore,
			Metrics:                      opts.Metrics,
			OnTerminated:                 opts.OnTerminated,
			PersistDiagnosticsFn:         opts.PersistDiagnosticsFn,
			PersistDiagnosticsWithLogsFn: opts.PersistDiagnosticsWithLogsFn,
			SendCallbackFn:               opts.SendCallbackFn,
			DoDeprovisionFn:              opts.DoDeprovisionFn,
		}
	})
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
	if opts.OnTerminated == nil {
		opts.OnTerminated = func(string) {}
	}
	if opts.PersistDiagnosticsFn == nil {
		opts.PersistDiagnosticsFn = func(shared.DiagnosticEntry, []string, map[string]string) {}
	}
	if opts.PersistDiagnosticsWithLogsFn == nil {
		opts.PersistDiagnosticsWithLogsFn = func(shared.DiagnosticEntry, map[string]string) {}
	}
	if opts.SendCallbackFn == nil {
		opts.SendCallbackFn = func(string, string, backend.CallbackStatus, string) {}
	}
	if opts.DoDeprovisionFn == nil {
		opts.DoDeprovisionFn = func(context.Context, string) error { return nil }
	}

	a := &LeaseActor{
		inbox:   make(chan LeaseMessage, leaseActorInboxSize),
		done:    make(chan struct{}),
		exiting: make(chan struct{}),
		workers: workbarrier.New(),
	}
	a.cfg = LeaseActorConfig{
		LeaseUUID:                    leaseUUID,
		Logger:                       opts.Logger,
		StopCtx:                      opts.StopCtx,
		WG:                           opts.WG,
		Inspector:                    opts.Inspector,
		Diag:                         opts.Diag,
		CallbackSender:               opts.CallbackSender,
		ProvisionStore:               opts.ProvisionStore,
		Metrics:                      opts.Metrics,
		OnTerminated:                 opts.OnTerminated,
		PersistDiagnosticsFn:         opts.PersistDiagnosticsFn,
		PersistDiagnosticsWithLogsFn: opts.PersistDiagnosticsWithLogsFn,
		SendCallbackFn:               opts.SendCallbackFn,
		DoDeprovisionFn:              opts.DoDeprovisionFn,
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
