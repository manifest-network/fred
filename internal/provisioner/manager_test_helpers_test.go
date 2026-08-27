package provisioner

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// testHandlerDeps preserves concise fixture construction while production
// HandlerDeps stays capability-narrow. The helper below performs the same
// explicit composition as Manager; none of these compatibility inputs can
// reach a production HandlerSet.
type testHandlerDeps struct {
	ChainClient     ChainClient
	Orchestrator    *ProvisionOrchestrator
	EventOperations EventOperations
	Operations      CallbackOperations
	Tracker         InFlightTracker
	Acknowledger    Acknowledger
	PayloadStore    *payload.Store
	Publisher       message.Publisher
	BackendRouter   BackendRouter
	Placement       CallbackPlacement
	Callbacks       CallbackApplication
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
	if provider, ok := deps.Tracker.(testOperationRegistryProvider); ok {
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
		var deprovisionObserver CallbackDeprovisionObserver
		if deps.Orchestrator != nil {
			deprovisionObserver = callbackDeprovisionObserverFunc(
				deps.Orchestrator.forgetDeprovisionCandidate,
			)
		}
		var err error
		callbacks, err = NewCallbackService(CallbackServiceConfig{
			Operations:   callbackOperations,
			Chain:        deps.ChainClient,
			Acknowledger: deps.Acknowledger,
			Placement:    deps.Placement,
			Payloads:     deps.PayloadStore,
			Events: callbackEventSinkFunc(func(leaseUUID string, status backend.ProvisionStatus, failure string) {
				publishLeaseStatusEvent(deps.Publisher, leaseUUID, status, failure)
			}),
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
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// A handful of callback-only compatibility tests still seed attempts through
	// the concrete legacy adapter. Production orchestration uses the explicit
	// durable baseline armed below instead.
	store.MarkInventoryReady()
	return store
}

func armTestPlacementAdmission(
	t testing.TB,
	store PlacementAuthorityStore,
	router BackendRouter,
) {
	t.Helper()
	backendNames := backendTopologyNames(router)
	if len(backendNames) == 0 {
		// Read-only/no-backend fixtures never reach admission. Tests that dispatch
		// provisioning must expose the same topology production routers do.
		return
	}
	require.NoError(t, store.ConfigureBackendTopology(backendNames))
	fence := store.BeginInventorySession()
	defer store.EndInventorySession(fence)
	_, err := store.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.True(t, store.CurrentAdmissionBaseline().Valid())
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

func newTestProvisionOrchestrator(
	t testing.TB,
	providerUUID, callbackBaseURL string,
	router BackendRouter,
	tracker InFlightTracker,
	store PlacementStore,
) *ProvisionOrchestrator {
	t.Helper()
	provider, ok := tracker.(testOperationRegistryProvider)
	require.True(t, ok, "test tracker must expose its typed operation registry")
	require.NotNil(t, provider.Operations())

	authority := testPlacementAuthority(t, store, router)
	orch, err := NewProvisionOrchestrator(
		providerUUID, callbackBaseURL, router, provider.Operations(), authority,
		&testProvisionStartSink{},
	)
	require.NoError(t, err)
	return orch
}

func testPlacementAuthority(
	t testing.TB,
	store PlacementStore,
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
	adapter := &testPlacementAuthorityAdapter{
		PlacementStore: store,
		authority:      newTestPlacementAuthority(t),
		attempts:       make(map[placement.AttemptToken]testAttemptIdentity),
	}
	armTestPlacementAdmission(t, adapter, router)
	return adapter
}

// testPlacementAuthorityAdapter keeps the extensive legacy placement mock
// assertions useful while exercising production through opaque capabilities.
// The real bbolt store issues and validates every token; successful mutations
// are mirrored into the raw mock only for test observation/error injection.
type testPlacementAuthorityAdapter struct {
	PlacementStore
	authority *placement.Store

	mu       sync.Mutex
	attempts map[placement.AttemptToken]testAttemptIdentity
}

type testAttemptIdentity struct {
	leaseUUID   string
	backendName string
}

func (a *testPlacementAuthorityAdapter) Lookup(leaseUUID string) placement.Placement {
	current := a.PlacementStore.Lookup(leaseUUID)
	if current.State() != placement.StateConfirmed || current.Attempt != "" {
		return current
	}
	internal := a.authority.Lookup(leaseUUID)
	if internal.State() == placement.StateAbsent {
		if err := a.authority.Confirm(leaseUUID, current.Backend); err != nil {
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

func (a *testPlacementAuthorityAdapter) ConfigureBackendTopology(names []string) error {
	return a.authority.ConfigureBackendTopology(names)
}

func (a *testPlacementAuthorityAdapter) CurrentAdmissionBaseline() placement.AdmissionBaseline {
	return a.authority.CurrentAdmissionBaseline()
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

func (a *testPlacementAuthorityAdapter) BeginNewAttempt(
	scope placement.AdmissionScope,
	leaseUUID, backendName string,
	id operation.OperationID,
) (placement.AttemptToken, bool, error) {
	if _, err := a.SetAttempting(leaseUUID, backendName); err != nil {
		return placement.AttemptToken{}, false, err
	}
	token, applied, err := a.authority.BeginNewAttempt(
		scope, leaseUUID, backendName, id,
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
		if err := a.authority.Confirm(leaseUUID, backendName); err != nil {
			_ = a.ClearAttempt(leaseUUID, backendName)
			return placement.AttemptToken{}, false, err
		}
		internal = a.authority.Lookup(leaseUUID)
	}
	token, applied, err := a.authority.BeginOwnedAttempt(
		baseline, internal.RecordRevision(), backendName, id,
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

func (a *testPlacementAuthorityAdapter) BeginAttemptIfNotNewer(
	leaseUUID, backendName string,
	id operation.OperationID,
	fence placement.InventoryFence,
) (placement.AttemptToken, bool, error) {
	return a.authority.BeginAttemptIfNotNewer(leaseUUID, backendName, id, fence)
}

func (a *testPlacementAuthorityAdapter) BeginAttemptFromProjection(
	leaseUUID, backendName string,
	id operation.OperationID,
	proof placement.ProjectionResult,
) (placement.AttemptToken, bool, error) {
	return a.authority.BeginAttemptFromProjection(leaseUUID, backendName, id, proof)
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

func (a *testPlacementAuthorityAdapter) DeleteRecord(
	revision placement.RecordRevision,
) (bool, error) {
	return a.authority.DeleteRecord(revision)
}

var _ PlacementAuthorityStore = (*testPlacementAuthorityAdapter)(nil)
