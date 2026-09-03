package provisioner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

// testHandlerDeps preserves concise fixture construction while production
// HandlerDeps stays capability-narrow. The helper below performs the same
// explicit composition as Manager; none of these compatibility inputs can
// reach a production HandlerSet.
type testHandlerDeps struct {
	ChainClient        ChainClient
	Orchestrator       *ProvisionOrchestrator
	EventOperations    EventOperations
	Operations         CallbackOperations
	Tracker            *testOperationRegistry
	Acknowledger       Acknowledger
	PayloadStore       *payload.Store
	Publisher          message.Publisher
	BackendRouter      BackendRouter
	Placement          CallbackPlacement
	LifecycleAuthority CallbackLifecycleAuthority
	CallbackEvents     CallbackEventSink
	Callbacks          CallbackApplication
}

// denyCallbackLifecycleAuthority is the explicit fail-closed lifecycle port
// for unit fixtures that are not exercising an authorized lifecycle callback.
// Production Manager always supplies its durable placement store; keeping this
// non-nil in the compatibility composer prevents an unrelated missing
// dependency from obscuring the behavior a unit test actually targets.
type denyCallbackLifecycleAuthority struct{}

func (denyCallbackLifecycleAuthority) AuthorizeLifecycle(
	string,
	lifecycle.ID,
) placement.LifecycleAuthorization {
	return placement.LifecycleAuthorization{}
}

func (denyCallbackLifecycleAuthority) RetireLifecycle(
	string,
	lifecycle.ID,
) (placement.LifecycleAuthorization, error) {
	return placement.LifecycleAuthorization{}, nil
}

type testProvisionStartSink struct {
	mu        sync.RWMutex
	publisher message.Publisher
}

func (sink *testProvisionStartSink) setPublisher(publisher message.Publisher) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.publisher = publisher
}

func (sink *testProvisionStartSink) PublishProvisionStarting(leaseUUID string) {
	sink.mu.RLock()
	publisher := sink.publisher
	sink.mu.RUnlock()
	publishLeaseStatusEvent(publisher, leaseUUID, backend.ProvisionStatusProvisioning, "")
}

func composeTestHandlerSet(t testing.TB, deps testHandlerDeps) *HandlerSet {
	t.Helper()

	eventOperations := deps.EventOperations
	callbackOperations := deps.Operations
	if provider := deps.Tracker; provider != nil {
		if eventOperations == nil {
			eventOperations = provider.Operations()
		}
		if callbackOperations == nil {
			callbackOperations = provider.Operations()
		}
	}
	if deps.Orchestrator != nil {
		if eventOperations == nil {
			var ok bool
			eventOperations, ok = deps.Orchestrator.operations.(EventOperations)
			require.True(t, ok, "test provision operations must expose event claims")
		}
		if callbackOperations == nil {
			var ok bool
			callbackOperations, ok = deps.Orchestrator.operations.(CallbackOperations)
			require.True(t, ok, "test provision operations must expose callback claims")
		}
		if deps.Placement == nil {
			deps.Placement = testCallbackPlacement(t, deps.Orchestrator)
		}
		if startSink, ok := deps.Orchestrator.startEvents.(*testProvisionStartSink); ok {
			startSink.setPublisher(deps.Publisher)
		}
	}

	callbacks := deps.Callbacks
	if callbacks == nil && callbackOperations != nil {
		lifecycleAuthority := deps.LifecycleAuthority
		if lifecycleAuthority == nil && deps.Orchestrator != nil {
			lifecycleAuthority, _ = deps.Orchestrator.placementStore.(CallbackLifecycleAuthority)
		}
		if lifecycleAuthority == nil {
			lifecycleAuthority = denyCallbackLifecycleAuthority{}
		}
		callbackEvents := deps.CallbackEvents
		if callbackEvents == nil {
			// Older handler-unit fixtures observe their adapter output as Watermill
			// messages. Tests of the production ordering boundary inject a direct
			// callback sink explicitly, as Manager does.
			callbackEvents = callbackEventSinkFunc(func(leaseUUID string, status backend.ProvisionStatus, failure string) {
				publishLeaseStatusEvent(deps.Publisher, leaseUUID, status, failure)
			})
		}
		var deprovisionObserver CallbackDeprovisionObserver
		if deps.Orchestrator != nil {
			deprovisionObserver = callbackDeprovisionObserverFunc(
				deps.Orchestrator.forgetDeprovisionCandidate,
			)
		}
		var err error
		callbacks, err = newCallbackServiceForTest(CallbackServiceConfig{
			Operations:          callbackOperations,
			Chain:               deps.ChainClient,
			Acknowledger:        deps.Acknowledger,
			Placement:           deps.Placement,
			LifecycleAuthority:  lifecycleAuthority,
			Payloads:            deps.PayloadStore,
			Events:              callbackEvents,
			Backends:            deps.BackendRouter,
			DeprovisionObserver: deprovisionObserver,
		})
		require.NoError(t, err)
	}

	return NewHandlerSet(HandlerDeps{
		ChainClient:     deps.ChainClient,
		Orchestrator:    deps.Orchestrator,
		EventOperations: eventOperations,
		PayloadStore:    deps.PayloadStore,
		Publisher:       deps.Publisher,
		Callbacks:       callbacks,
	})
}

// startTestProvisioning mirrors the event handler's lease-claim discipline so
// unit tests cannot reintroduce an unclaimed backend-call path merely for
// convenience. A busy claim is the same idempotent duplicate outcome the
// handler observes when another operation already owns the lease.
func startTestProvisioning(
	t testing.TB,
	orchestrator *ProvisionOrchestrator,
	ctx context.Context,
	lease *billingtypes.Lease,
	opts ProvisionOpts,
) error {
	t.Helper()
	// Event-driven provisioning is dispatched only after an authoritative chain
	// read observes PENDING. Older unit fixtures omit the protobuf enum field,
	// so normalize that zero value at this test boundary without weakening the
	// production guard (covered directly by the non-pending regression tests).
	dispatchLease := lease
	if lease != nil && lease.State == billingtypes.LEASE_STATE_UNSPECIFIED {
		copy := *lease
		copy.State = billingtypes.LEASE_STATE_PENDING
		dispatchLease = &copy
	}
	claimResult := orchestrator.operations.TryClaimLeaseNow(lease.Uuid)
	if !claimResult.Acquired() {
		if claimResult.Outcome() == operation.LeaseClaimBusy {
			return nil
		}
		return fmt.Errorf("test lease claim failed: outcome %d", claimResult.Outcome())
	}
	claim := claimResult.Claim()
	defer func() {
		require.True(t, orchestrator.operations.ReleaseLease(claim),
			"test must release the exact provisioning lease claim")
	}()
	return orchestrator.StartProvisioningClaimed(ctx, claim, dispatchLease, opts)
}

func testCallbackPlacement(t testing.TB, orchestrator *ProvisionOrchestrator) CallbackPlacement {
	t.Helper()
	placementPort, ok := orchestrator.placementStore.(CallbackPlacement)
	require.True(t, ok, "test placement authority must expose callback settlement")
	return placementPort
}

// newTestPlacementAuthority gives ordinary manager/orchestrator tests the same
// durable, ready-by-explicit-projection dependency required in production.
// Constructor validation tests deliberately call the production constructors
// directly so missing and typed-nil authorities are never papered over.
func newTestPlacementAuthority(t testing.TB) *placement.Store {
	t.Helper()
	store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func projectTestPlacementInventory(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
	projection placement.InventoryProjection,
) placement.ProjectionResult {
	t.Helper()
	require.NotEmpty(t, backendNames, "typed placement projection requires an explicit topology")
	configureTestPlacementTopology(t, store, backendNames)
	if projection.Complete && projection.BackendStorageIdentities == nil {
		projection.BackendStorageIdentities = make(map[string]backendidentity.ID, len(backendNames))
		for _, backendName := range backendNames {
			projection.BackendStorageIdentities[backendName] = testBackendStorageID(backendName)
		}
	}
	if projection.Complete && projection.EmptyBackends == nil {
		nonempty := make(map[string]struct{})
		for _, backendName := range projection.Placements {
			nonempty[backendName] = struct{}{}
		}
		for _, backendNames := range projection.Conflicts {
			for _, backendName := range backendNames {
				nonempty[backendName] = struct{}{}
			}
		}
		projection.EmptyBackends = make([]string, 0, len(backendNames))
		for _, backendName := range backendNames {
			if _, present := nonempty[backendName]; !present {
				projection.EmptyBackends = append(projection.EmptyBackends, backendName)
			}
		}
	}
	fence := store.BeginInventorySession()
	defer store.EndInventorySession(fence)
	result, err := store.ProjectInventory(fence, projection)
	require.NoError(t, err)
	return result
}

type testTopologyConfigurator interface {
	ConfigureBackendTopologyWithStorageIdentities(
		[]string,
		map[string]backendidentity.ID,
	) error
}

func configureTestPlacementTopology(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
) {
	t.Helper()
	configurator, ok := store.(testTopologyConfigurator)
	require.True(t, ok, "test placement authority must expose identity-bearing topology setup")
	identities := make(map[string]backendidentity.ID, len(backendNames))
	for _, backendName := range backendNames {
		identities[backendName] = testBackendStorageID(backendName)
	}
	require.NoError(t, configurator.ConfigureBackendTopologyWithStorageIdentities(
		backendNames, identities,
	))
}

func armTestPlacementTopology(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
) {
	t.Helper()
	projectTestPlacementInventory(t, store, backendNames, placement.InventoryProjection{Complete: true})
	require.True(t, store.CurrentAdmissionBaseline().Valid())
}

func testPlacementCallbackPair(
	t testing.TB,
	id operation.OperationID,
) placement.CallbackPair {
	t.Helper()
	pair, err := makeTestPlacementCallbackPair(id)
	require.NoError(t, err)
	return pair
}

func makeTestPlacementCallbackPair(
	id operation.OperationID,
) (placement.CallbackPair, error) {
	callbackURL, err := BuildCallbackURLForOperation(
		"https://provider.test/callback", id,
	)
	if err != nil {
		return placement.CallbackPair{}, err
	}
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	if err != nil {
		return placement.CallbackPair{}, err
	}
	pair, err := placement.NewCallbackPair(id, callbackURL, lifecycleCallbackURL)
	if err != nil {
		return placement.CallbackPair{}, err
	}
	return pair, nil

}

func mustTestPlacementCallbackPair(id operation.OperationID) placement.CallbackPair {
	pair, err := makeTestPlacementCallbackPair(id)
	if err != nil {
		panic(err)
	}
	return pair
}

func testBackendRequestSnapshot(t testing.TB) placement.BackendRequestSnapshot {
	t.Helper()
	return mustTestBackendRequestSnapshot()
}

func mustTestBackendRequestSnapshot() placement.BackendRequestSnapshot {
	snapshot, err := placement.NewBackendRequestSnapshot(
		"tenant-test", "provider-test",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	if err != nil {
		panic(err)
	}
	return snapshot
}

func beginTestNewPlacementAttempt(
	t testing.TB,
	store PlacementAuthorityStore,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) placement.AttemptToken {
	return beginTestNewPlacementAttemptWithFingerprint(
		t, store, leaseUUID, backendName, operationID, placement.PayloadFingerprint{},
	)
}

func beginTestNewPlacementAttemptWithFingerprint(
	t testing.TB,
	store PlacementAuthorityStore,
	leaseUUID, backendName string,
	operationID operation.OperationID,
	fingerprint placement.PayloadFingerprint,
) placement.AttemptToken {
	return beginTestNewPlacementAttemptWithSnapshot(
		t, store, leaseUUID, backendName, operationID, fingerprint,
		testBackendRequestSnapshot(t),
	)
}

func beginTestNewPlacementAttemptWithSnapshot(
	t testing.TB,
	store PlacementAuthorityStore,
	leaseUUID, backendName string,
	operationID operation.OperationID,
	fingerprint placement.PayloadFingerprint,
	requestSnapshot placement.BackendRequestSnapshot,
) placement.AttemptToken {
	t.Helper()
	baseline := store.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid(), "test placement admission must be armed before beginning an attempt")
	scope, err := store.ScopeAdmission(baseline, []string{backendName})
	require.NoError(t, err)
	token, applied, err := store.BeginNewAttempt(
		scope, leaseUUID, backendName, operationID, fingerprint,
		requestSnapshot,
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())
	return token
}

func seedTestConfirmedPlacements(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
	placements map[string]string,
) {
	t.Helper()
	projectTestPlacementInventory(t, store, backendNames, placement.InventoryProjection{
		Complete:   true,
		Placements: placements,
	})
}

// seedTestTypedConfirmedPlacements establishes confirmed ownership through the
// same write-ahead attempt transition as production. Use it when a fixture
// needs lifecycle callback authority; passive inventory projection deliberately
// cannot manufacture that authority after the typed-capability migration.
func seedTestTypedConfirmedPlacements(
	t testing.TB,
	store PlacementAuthorityStore,
	backendNames []string,
	placements map[string]string,
) {
	t.Helper()
	armTestPlacementTopology(t, store, backendNames)
	operations := operation.NewRegistry()
	for leaseUUID, backendName := range placements {
		claimResult := operations.TryClaimLeaseNow(leaseUUID)
		require.True(t, claimResult.Acquired())
		claim := claimResult.Claim()
		tracked := operations.TryInitiateClaimed(claim, operation.TrackSpec{
			LeaseUUID: leaseUUID,
			Tenant:    "tenant-a",
			Backend:   backendName,
			Kind:      operation.KindProvision,
		})
		require.True(t, tracked.Started())
		initiation := tracked.Capability()
		require.True(t, operations.BeginCall(initiation))
		require.Equal(t, operation.InitiationActivated, operations.Activate(initiation))
		require.True(t, operations.ReleaseLease(claim))
		attempt := beginTestNewPlacementAttempt(
			t, store, leaseUUID, backendName, initiation.ID(),
		)
		confirmed, err := store.ConfirmAttempt(attempt)
		require.NoError(t, err)
		require.True(t, confirmed)
		settlement := operations.TryClaimCallback(leaseUUID, initiation.ID())
		require.True(t, settlement.Claimed())
		require.True(t, operations.FinishSettlement(settlement.Claim()))
	}
}

func deleteTestPlacement(
	t testing.TB,
	store ReconcilerPlacement,
	leaseUUID string,
) {
	t.Helper()
	revision := store.Lookup(leaseUUID).RecordRevision()
	require.True(t, revision.Valid(), "test placement must exist before deletion")
	deleted, err := store.DeleteRecord(revision)
	require.NoError(t, err)
	require.True(t, deleted)
}

func armTestPlacementAdmission(
	t testing.TB,
	store ReconcilerPlacement,
	router BackendRouter,
) {
	t.Helper()
	backendNames := backendTopologyNames(router)
	if len(backendNames) == 0 {
		// Read-only/no-backend fixtures never reach admission. Tests that dispatch
		// provisioning must expose the same topology production routers do.
		return
	}
	armTestPlacementTopology(t, store, backendNames)
}

func newTestManager(
	t testing.TB,
	cfg ManagerConfig,
	router *backend.Router,
	chainClient ChainClient,
) (*Manager, error) {
	t.Helper()
	if cfg.PlacementStore == nil {
		cfg.PlacementStore = newTestPlacementAuthority(t)
	}
	backendNames := backendTopologyNames(router)
	if len(backendNames) > 0 {
		configureTestPlacementTopology(t, cfg.PlacementStore, backendNames)
	}
	// Most historical unit fixtures use short semantic provider labels. Bind the
	// test aggregate explicitly at the constructor boundary while production
	// tests that exercise the real Store call NewManager directly.
	cfg.PlacementStore = &testProviderBoundPlacementAuthority{
		PlacementAuthorityStore: cfg.PlacementStore,
		providerUUID:            cfg.ProviderUUID,
	}
	manager, err := NewManager(cfg, router, chainClient)
	if err != nil {
		return nil, err
	}
	armTestPlacementAdmission(t, cfg.PlacementStore, router)
	return manager, nil
}

type testOperationRegistryProvider interface {
	Operations() *operation.Registry
}

// legacyTestPlacementStore is confined to test fixtures that predate opaque
// placement capabilities. Production code never accepts this raw mutation
// surface; test adapters translate it into ReconcilerPlacement or
// ProvisionPlacement while the fixture suite is migrated mechanically.
type legacyTestPlacementStore interface {
	PlacementView
	SnapshotRevision() uint64
	BeginInventorySnapshot() uint64
	EndInventorySnapshot(revision uint64)
	SetAttempting(leaseUUID, backendName string) (uint64, error)
	SetAttemptingIfNotNewer(leaseUUID, backendName string, maxRevision uint64) (uint64, bool, error)
	Confirm(leaseUUID, backendName string) error
	ConfirmAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	ClearAttempt(leaseUUID, backendName string) error
	ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	Delete(leaseUUID string) error
	DeleteIfRevision(leaseUUID string, revision uint64) (bool, error)
	SetBatchIfNotNewer(placements map[string]string, maxRevision uint64) (map[string]uint64, map[string]struct{}, error)
	SetConflictsIfNotNewer(conflicts map[string][]string, maxRevision uint64) (map[string]uint64, map[string]struct{}, error)
	ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error
}

func newTestProvisionOrchestrator(
	t testing.TB,
	providerUUID, callbackBaseURL string,
	router BackendRouter,
	tracker *testOperationRegistry,
	store any,
) *ProvisionOrchestrator {
	t.Helper()
	require.NotNil(t, tracker)
	require.NotNil(t, tracker.Operations())

	authority := testPlacementAuthority(t, store, router)
	orch, err := NewProvisionOrchestrator(
		providerUUID, callbackBaseURL, router, tracker.Operations(), authority,
		&testProvisionStartSink{},
	)
	require.NoError(t, err)
	return orch
}

func testPlacementAuthority(
	t testing.TB,
	store any,
	router BackendRouter,
) PlacementAuthorityStore {
	t.Helper()
	if store == nil {
		authority := newTestPlacementAuthority(t)
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	if authority, ok := store.(PlacementAuthorityStore); ok {
		require.False(t, isNilPlacementAuthorityStore(authority),
			"typed-nil placement authority must be tested through the production constructor")
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	raw, ok := store.(legacyTestPlacementStore)
	require.True(t, ok, "test placement fixture must expose a typed authority or raw test adapter")
	adapter := &testPlacementAuthorityAdapter{
		legacyTestPlacementStore: raw,
		authority:                newTestPlacementAuthority(t),
		attempts:                 make(map[placement.AttemptToken]testAttemptIdentity),
	}
	armTestPlacementAdmission(t, adapter, router)
	return adapter
}

// testReconcilerPlacement converts legacy observable placement fixtures into a
// typed authority once, then mirrors typed reconciler writes back into the raw
// fixture for assertions. Reconciler itself sees only the production port and
// can consume only capabilities minted by the private durable store.
func testReconcilerPlacement(
	t testing.TB,
	store any,
	router BackendRouter,
) ReconcilerPlacement {
	t.Helper()
	if store == nil {
		authority := newTestPlacementAuthority(t)
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	if authority, ok := store.(ReconcilerPlacement); ok {
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	raw, ok := store.(legacyTestPlacementStore)
	require.True(t, ok, "test placement fixture must expose a typed reconciler port or legacy test adapter")

	base := &testPlacementAuthorityAdapter{
		legacyTestPlacementStore: raw,
		authority:                newTestPlacementAuthority(t),
		attempts:                 make(map[placement.AttemptToken]testAttemptIdentity),
	}
	armTestPlacementAdmission(t, base, router)
	adapter := &testReconcilerPlacementAdapter{
		testPlacementAuthorityAdapter: base,
		inventoryCutoffs:              make(map[placement.InventoryFence]uint64),
		topology:                      backendTopologyNames(router),
		overlay:                       make(map[string]placement.Placement),
	}
	require.NoError(t, adapter.seedRawPlacements())
	return adapter
}

type testReconcilerPlacementAdapter struct {
	*testPlacementAuthorityAdapter

	inventoryMu      sync.Mutex
	inventoryCutoffs map[placement.InventoryFence]uint64
	topology         []string
	// overlay retains deliberately unrepresentable legacy/corrupt fixtures.
	// Their invalid typed revision makes them non-authoritative, while keeping
	// them visible exercises the reconciler's defensive fail-closed branches.
	overlay map[string]placement.Placement
}

func (a *testReconcilerPlacementAdapter) seedRawPlacements() error {
	placements := make(map[string]string)
	conflicts := make(map[string][]string)
	attempts := make(map[string]placement.Placement)
	for leaseUUID, current := range a.legacyTestPlacementStore.List() {
		if !a.representable(current) {
			a.overlay[leaseUUID] = current
			continue
		}
		if current.Conflict {
			conflicts[leaseUUID] = current.ConflictBackends
		} else if current.Backend != "" {
			placements[leaseUUID] = current.Backend
		}
		if current.Attempt != "" {
			attempts[leaseUUID] = current
		}
	}
	if len(placements) != 0 || len(conflicts) != 0 {
		fence := a.authority.BeginInventorySession()
		_, err := a.authority.ProjectInventory(fence, placement.InventoryProjection{
			Placements: placements,
			Conflicts:  conflicts,
		})
		a.authority.EndInventorySession(fence)
		if err != nil {
			return err
		}
	}

	baseline := a.authority.CurrentAdmissionBaseline()
	if !baseline.Valid() {
		return errors.New("test placement admission baseline is not armed")
	}
	scope, err := a.authority.ScopeAdmission(baseline, a.topology)
	if err != nil {
		return err
	}
	id, err := operation.ParseID("00000000-0000-4000-8000-000000000001")
	if err != nil {
		return err
	}
	callbackURL, err := BuildCallbackURLForOperation("https://provider.test/callback", id)
	if err != nil {
		return err
	}
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	if err != nil {
		return err
	}
	callbackPair, err := placement.NewCallbackPair(id, callbackURL, lifecycleCallbackURL)
	if err != nil {
		return err
	}
	requestSnapshot, err := placement.NewBackendRequestSnapshot(
		"tenant-test", "provider-test",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	if err != nil {
		return err
	}
	for leaseUUID, current := range attempts {
		var applied bool
		if current.Backend == "" {
			_, applied, err = a.authority.BeginNewAttempt(
				scope, leaseUUID, current.Attempt, id,
				placement.PayloadFingerprint{}, requestSnapshot, callbackPair,
			)
		} else {
			_, applied, err = a.authority.BeginOwnedAttempt(
				baseline, a.authority.Lookup(leaseUUID).RecordRevision(), current.Attempt, id,
				placement.PayloadFingerprint{}, requestSnapshot, callbackPair,
			)
		}
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("seed test placement attempt for %s: placement changed", leaseUUID)
		}
	}
	return nil
}

func (a *testReconcilerPlacementAdapter) representable(current placement.Placement) bool {
	configured := func(name string) bool {
		return name == "" || slices.Contains(a.topology, name)
	}
	if !configured(current.Backend) || !configured(current.Attempt) {
		return false
	}
	if !current.Conflict {
		return true
	}
	if current.ConflictOwnersUnknown || len(current.ConflictBackends) < 2 {
		return false
	}
	for _, backendName := range current.ConflictBackends {
		if !configured(backendName) {
			return false
		}
	}
	return true
}

func (a *testReconcilerPlacementAdapter) Lookup(leaseUUID string) placement.Placement {
	if current, ok := a.overlay[leaseUUID]; ok {
		return current
	}
	current := a.authority.Lookup(leaseUUID)
	raw := a.legacyTestPlacementStore.Lookup(leaseUUID)
	if raw.State() == current.State() && !raw.SetAt.IsZero() {
		current.SetAt = raw.SetAt
	}
	return current
}

func (a *testReconcilerPlacementAdapter) List() map[string]placement.Placement {
	result := a.authority.List()
	for leaseUUID := range result {
		result[leaseUUID] = a.Lookup(leaseUUID)
	}
	for leaseUUID, current := range a.overlay {
		result[leaseUUID] = current
	}
	return result
}

func (a *testReconcilerPlacementAdapter) BeginInventorySession() placement.InventoryFence {
	cutoff := a.BeginInventorySnapshot()
	fence := a.authority.BeginInventorySession()
	a.inventoryMu.Lock()
	a.inventoryCutoffs[fence] = cutoff
	a.inventoryMu.Unlock()
	return fence
}

func (a *testReconcilerPlacementAdapter) EndInventorySession(fence placement.InventoryFence) {
	a.inventoryMu.Lock()
	cutoff, ok := a.inventoryCutoffs[fence]
	delete(a.inventoryCutoffs, fence)
	a.inventoryMu.Unlock()
	if ok {
		a.EndInventorySnapshot(cutoff)
	}
	a.authority.EndInventorySession(fence)
}

func (a *testReconcilerPlacementAdapter) ProjectInventory(
	fence placement.InventoryFence,
	input placement.InventoryProjection,
) (placement.ProjectionResult, error) {
	a.inventoryMu.Lock()
	cutoff, ok := a.inventoryCutoffs[fence]
	a.inventoryMu.Unlock()
	if !ok {
		return placement.ProjectionResult{}, placement.ErrInvalidInventoryFence
	}

	placements := maps.Clone(input.Placements)
	conflicts := maps.Clone(input.Conflicts)
	_, fencedConflicts, err := a.SetConflictsIfNotNewer(conflicts, cutoff)
	if err != nil {
		return placement.ProjectionResult{}, err
	}
	_, fencedPlacements, err := a.SetBatchIfNotNewer(placements, cutoff)
	if err != nil {
		return placement.ProjectionResult{}, err
	}
	for leaseUUID := range fencedConflicts {
		delete(conflicts, leaseUUID)
	}
	for leaseUUID := range fencedPlacements {
		delete(placements, leaseUUID)
	}

	result, err := a.authority.ProjectInventory(fence, placement.InventoryProjection{
		Complete:                 input.Complete,
		BackendStorageIdentities: maps.Clone(input.BackendStorageIdentities),
		EmptyBackends:            slices.Clone(input.EmptyBackends),
		Placements:               placements,
		Lifecycles:               maps.Clone(input.Lifecycles),
		Conflicts:                conflicts,
	})
	if result.Fenced == nil {
		result.Fenced = make(map[string]struct{}, len(fencedConflicts)+len(fencedPlacements))
	}
	for leaseUUID := range fencedConflicts {
		result.Fenced[leaseUUID] = struct{}{}
	}
	for leaseUUID := range fencedPlacements {
		result.Fenced[leaseUUID] = struct{}{}
	}
	return result, err
}

func (a *testReconcilerPlacementAdapter) DeleteRecord(
	revision placement.RecordRevision,
) (bool, error) {
	leaseUUID := ""
	for candidate, current := range a.authority.List() {
		if current.RecordRevision() == revision {
			leaseUUID = candidate
			break
		}
	}
	if leaseUUID == "" {
		return false, placement.ErrInvalidRecordRevision
	}
	raw := a.legacyTestPlacementStore.Lookup(leaseUUID)
	deleted, err := a.DeleteIfRevision(leaseUUID, raw.Revision())
	if err != nil || !deleted {
		return deleted, err
	}
	return a.authority.DeleteRecord(revision)
}

var _ ReconcilerPlacement = (*testReconcilerPlacementAdapter)(nil)

// testPlacementAuthorityAdapter keeps the extensive legacy placement mock
// assertions useful while exercising production through opaque capabilities.
// The real bbolt store issues and validates every token; successful mutations
// are mirrored into the raw mock only for test observation/error injection.
type testPlacementAuthorityAdapter struct {
	legacyTestPlacementStore
	authority *placement.Store

	mu       sync.Mutex
	attempts map[placement.AttemptToken]testAttemptIdentity
}

func (a *testPlacementAuthorityAdapter) VerifyProviderUUID(providerUUID string) error {
	return a.authority.VerifyProviderUUID(providerUUID)
}

type testProviderBoundPlacementAuthority struct {
	PlacementAuthorityStore
	providerUUID string
}

func (authority *testProviderBoundPlacementAuthority) VerifyProviderUUID(providerUUID string) error {
	if authority == nil || authority.PlacementAuthorityStore == nil ||
		providerUUID == "" || providerUUID != authority.providerUUID {
		return placement.ErrProviderAuthorityMismatch
	}
	return nil
}

func (authority *testProviderBoundPlacementAuthority) ConfigureBackendTopologyWithStorageIdentities(
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
) error {
	configurator, ok := authority.PlacementAuthorityStore.(testTopologyConfigurator)
	if !ok {
		return errors.New("embedded test placement authority cannot configure topology")
	}
	return configurator.ConfigureBackendTopologyWithStorageIdentities(backendNames, storageIDs)
}

type testProviderBoundReconcilerPlacement struct {
	ReconcilerPlacement
	providerUUID string
}

func (authority *testProviderBoundReconcilerPlacement) VerifyProviderUUID(providerUUID string) error {
	if authority == nil || authority.ReconcilerPlacement == nil ||
		providerUUID == "" || providerUUID != authority.providerUUID {
		return placement.ErrProviderAuthorityMismatch
	}
	return nil
}

func (authority *testProviderBoundReconcilerPlacement) ConfigureBackendTopologyWithStorageIdentities(
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
) error {
	configurator, ok := authority.ReconcilerPlacement.(testTopologyConfigurator)
	if !ok {
		return errors.New("embedded test placement authority cannot configure topology")
	}
	return configurator.ConfigureBackendTopologyWithStorageIdentities(backendNames, storageIDs)
}

// constructorPlacementAuthoritySpy uses the real provider-bound store while
// recording whether construction advanced to topology validation.
type constructorPlacementAuthoritySpy struct {
	PlacementAuthorityStore
	topologyChecks int
}

func (authority *constructorPlacementAuthoritySpy) VerifyBackendTopology(names []string) error {
	authority.topologyChecks++
	return authority.PlacementAuthorityStore.VerifyBackendTopology(names)
}

type testAttemptIdentity struct {
	leaseUUID   string
	backendName string
}

func (a *testPlacementAuthorityAdapter) Lookup(leaseUUID string) placement.Placement {
	current := a.legacyTestPlacementStore.Lookup(leaseUUID)
	if current.State() != placement.StateConfirmed || current.Attempt != "" {
		return current
	}
	internal := a.authority.Lookup(leaseUUID)
	if internal.State() == placement.StateAbsent {
		if err := a.projectConfirmed(leaseUUID, current.Backend); err != nil {
			return current
		}
		internal = a.authority.Lookup(leaseUUID)
	}
	if internal.State() == placement.StateConfirmed && internal.Backend == current.Backend {
		return internal
	}
	return current
}

func (a *testPlacementAuthorityAdapter) BeginInventorySession() placement.InventoryFence {
	return a.authority.BeginInventorySession()
}

func (a *testPlacementAuthorityAdapter) EndInventorySession(fence placement.InventoryFence) {
	a.authority.EndInventorySession(fence)
}

func (a *testPlacementAuthorityAdapter) VerifyBackendTopology(names []string) error {
	return a.authority.VerifyBackendTopology(names)
}

func (a *testPlacementAuthorityAdapter) ConfigureBackendTopologyWithStorageIdentities(
	names []string,
	identities map[string]backendidentity.ID,
) error {
	return a.authority.ConfigureBackendTopologyWithStorageIdentities(names, identities)
}

func (a *testPlacementAuthorityAdapter) CurrentAdmissionBaseline() placement.AdmissionBaseline {
	return a.authority.CurrentAdmissionBaseline()
}

func (a *testPlacementAuthorityAdapter) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	return a.authority.ExpectedBackendStorageIdentity(backendName)
}

func (a *testPlacementAuthorityAdapter) ScopeAdmission(
	baseline placement.AdmissionBaseline,
	eligibleNames []string,
) (placement.AdmissionScope, error) {
	return a.authority.ScopeAdmission(baseline, eligibleNames)
}

func (a *testPlacementAuthorityAdapter) ProjectInventory(
	fence placement.InventoryFence,
	input placement.InventoryProjection,
) (placement.ProjectionResult, error) {
	return a.authority.ProjectInventory(fence, input)
}

func (a *testPlacementAuthorityAdapter) projectConfirmed(leaseUUID, backendName string) error {
	fence := a.authority.BeginInventorySession()
	defer a.authority.EndInventorySession(fence)
	_, err := a.authority.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{leaseUUID: backendName},
	})
	return err
}

func (a *testPlacementAuthorityAdapter) BeginNewAttempt(
	scope placement.AdmissionScope,
	leaseUUID, backendName string,
	id operation.OperationID,
	payloadFingerprint placement.PayloadFingerprint,
	requestSnapshot placement.BackendRequestSnapshot,
	callbackPair placement.CallbackPair,
) (placement.AttemptToken, bool, error) {
	if _, err := a.SetAttempting(leaseUUID, backendName); err != nil {
		return placement.AttemptToken{}, false, err
	}
	token, applied, err := a.authority.BeginNewAttempt(
		scope, leaseUUID, backendName, id, payloadFingerprint, requestSnapshot, callbackPair,
	)
	if err != nil || !applied {
		_ = a.ClearAttempt(leaseUUID, backendName)
		return placement.AttemptToken{}, applied, err
	}
	a.mu.Lock()
	a.attempts[token] = testAttemptIdentity{leaseUUID: leaseUUID, backendName: backendName}
	a.mu.Unlock()
	return token, true, nil
}

func (a *testPlacementAuthorityAdapter) BeginOwnedAttempt(
	baseline placement.AdmissionBaseline,
	revision placement.RecordRevision,
	backendName string,
	id operation.OperationID,
	payloadFingerprint placement.PayloadFingerprint,
	requestSnapshot placement.BackendRequestSnapshot,
	callbackPair placement.CallbackPair,
) (placement.AttemptToken, bool, error) {
	// Legacy mock snapshots cannot mint RecordRevision. Mirror their exact raw
	// owner CAS first, then bind the typed attempt to the adapter's private
	// durable authority. Race/CAS tests use placement.Store directly.
	leaseUUID := ""
	for candidate, current := range a.authority.List() {
		if current.RecordRevision() == revision {
			leaseUUID = candidate
			break
		}
	}
	if leaseUUID == "" {
		return placement.AttemptToken{}, false, placement.ErrInvalidRecordRevision
	}
	if _, err := a.SetAttempting(leaseUUID, backendName); err != nil {
		return placement.AttemptToken{}, false, err
	}
	internal := a.authority.Lookup(leaseUUID)
	if internal.State() == placement.StateAbsent {
		if err := a.projectConfirmed(leaseUUID, backendName); err != nil {
			_ = a.ClearAttempt(leaseUUID, backendName)
			return placement.AttemptToken{}, false, err
		}
		internal = a.authority.Lookup(leaseUUID)
	}
	token, applied, err := a.authority.BeginOwnedAttempt(
		baseline, internal.RecordRevision(), backendName, id,
		payloadFingerprint, requestSnapshot, callbackPair,
	)
	if err != nil || !applied {
		_ = a.ClearAttempt(leaseUUID, backendName)
		return placement.AttemptToken{}, applied, err
	}
	a.mu.Lock()
	a.attempts[token] = testAttemptIdentity{leaseUUID: leaseUUID, backendName: backendName}
	a.mu.Unlock()
	return token, true, nil
}

func (a *testPlacementAuthorityAdapter) ConfirmAttempt(token placement.AttemptToken) (bool, error) {
	applied, err := a.authority.ConfirmAttempt(token)
	if err != nil || !applied {
		return applied, err
	}
	identity, ok := a.takeAttempt(token)
	if !ok {
		return false, placement.ErrInvalidAttemptToken
	}
	if err := a.Confirm(identity.leaseUUID, identity.backendName); err != nil {
		return false, err
	}
	return true, nil
}

func (a *testPlacementAuthorityAdapter) RefuseAttempt(token placement.AttemptToken) (bool, error) {
	applied, err := a.authority.RefuseAttempt(token)
	if err != nil || !applied {
		return applied, err
	}
	identity, ok := a.takeAttempt(token)
	if !ok {
		return false, placement.ErrInvalidAttemptToken
	}
	if err := a.ClearAttempt(identity.leaseUUID, identity.backendName); err != nil {
		return false, err
	}
	return true, nil
}

func (a *testPlacementAuthorityAdapter) takeAttempt(token placement.AttemptToken) (testAttemptIdentity, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	identity, ok := a.attempts[token]
	delete(a.attempts, token)
	return identity, ok
}

func (a *testPlacementAuthorityAdapter) ConfirmOperation(
	leaseUUID, backendName string,
	id operation.OperationID,
) (bool, error) {
	applied, err := a.authority.ConfirmOperation(leaseUUID, backendName, id)
	if err != nil || applied || !id.Valid() {
		return applied, err
	}
	// Legacy callback fixtures seed the raw mock directly, so no typed attempt
	// exists in authority. The callback service has already claimed the exact
	// operation ID; mirror that validated test operation into the observable
	// mock without weakening the production store.
	current := a.Lookup(leaseUUID)
	if current.Attempt != backendName {
		return current.Attempt == "" && current.Backend == backendName, nil
	}
	if err := a.Confirm(leaseUUID, backendName); err != nil {
		return false, err
	}
	return true, nil
}

func (a *testPlacementAuthorityAdapter) RefuseOperation(
	leaseUUID, backendName string,
	id operation.OperationID,
) (bool, error) {
	applied, err := a.authority.RefuseOperation(leaseUUID, backendName, id)
	if err != nil || applied || !id.Valid() {
		return applied, err
	}
	if a.Lookup(leaseUUID).Attempt != backendName {
		return false, nil
	}
	if err := a.ClearAttempt(leaseUUID, backendName); err != nil {
		return false, err
	}
	return true, nil
}

func (a *testPlacementAuthorityAdapter) ClaimAttempt(
	leaseUUID string,
	id operation.OperationID,
) (placement.AttemptClaim, bool, error) {
	return a.authority.ClaimAttempt(leaseUUID, id)
}

func (a *testPlacementAuthorityAdapter) ReleaseAttemptClaim(
	claim placement.AttemptClaim,
) bool {
	return a.authority.ReleaseAttemptClaim(claim)
}

func (a *testPlacementAuthorityAdapter) ConfirmClaimedAttempt(
	claim placement.AttemptClaim,
) (bool, error) {
	return a.authority.ConfirmClaimedAttempt(claim)
}

func (a *testPlacementAuthorityAdapter) RefuseClaimedAttempt(
	claim placement.AttemptClaim,
) (bool, error) {
	return a.authority.RefuseClaimedAttempt(claim)
}

func (a *testPlacementAuthorityAdapter) AuthorizeLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) placement.LifecycleAuthorization {
	return a.authority.AuthorizeLifecycle(leaseUUID, id)
}

func (a *testPlacementAuthorityAdapter) RetireLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) (placement.LifecycleAuthorization, error) {
	return a.authority.RetireLifecycle(leaseUUID, id)
}

func (a *testPlacementAuthorityAdapter) DeleteRecord(
	revision placement.RecordRevision,
) (bool, error) {
	return a.authority.DeleteRecord(revision)
}

var _ PlacementAuthorityStore = (*testPlacementAuthorityAdapter)(nil)
