package provisioner

import (
	"errors"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// testOperationRegistry is a test adapter over the same one-shot facets used
// by production composition. It deliberately retains no *operation.Registry:
// fixtures may bind that private registry either to one placement Store or to
// one standalone SettlementAuthority, but can never retrieve it or sequence
// raw registry transitions.
type testOperationRegistry struct {
	bindPlacement  func(PlacementAuthorityStore) (*placement.OperationCoordinator, error)
	bindStandalone func() (operation.SettlementAuthority, error)

	bindingMu      sync.Mutex
	authority      operation.SettlementAuthority
	authorityBound bool
	runtime        operation.RuntimeController
	coordinator    *placement.OperationCoordinator
	callbackStore  *placement.Store
	startBound     func(testOperationSeed) (operation.OperationID, bool)
	finishBound    func(string, operation.OperationID) bool

	claimsMu sync.Mutex
	claims   map[testOperationKey]testOperationClaim

	seedMu sync.Mutex
	seeds  map[string]testOperationSeed
}

// testOperationSeed remembers pre-construction "in flight" fixture intent.
// Since the production Store now owns its Registry, newTestReconciler replays
// this intent through the real provision application after the Store, backend
// runtime, and provider control plane have been atomically composed.
type testOperationSeed struct {
	leaseUUID   string
	tenant      string
	items       []backend.LeaseItem
	backendName string
}

type testOperationClaimKind uint8

const (
	testOperationClaimInvalid testOperationClaimKind = iota
	testOperationClaimStandaloneCallback
	testOperationClaimDeprovision
)

// testOperationClaim is a closed test-only sum over the purpose-specific
// settlement capabilities used by legacy fixture assertions.
type testOperationClaim struct {
	kind               testOperationClaimKind
	standaloneCallback operation.CallbackClaim
	deprovision        operation.DeprovisionClaim
}

type testOperationKey struct {
	leaseUUID string
	id        operation.OperationID
}

type testInFlightOperation struct {
	LeaseUUID   string
	Tenant      string
	Items       []backend.LeaseItem
	Backend     string
	OperationID operation.OperationID
	StartTime   time.Time
	Kind        operation.Kind
}

func newTestOperationRegistry() *testOperationRegistry {
	observe := func(count int) {
		metrics.InFlightProvisions.Set(float64(count))
	}
	return wrapTestOperationRegistryWithObserver(
		operation.NewRegistryWithCountObserver(observe), observe,
	)
}

func wrapTestOperationRegistry(registry *operation.Registry) *testOperationRegistry {
	return wrapTestOperationRegistryWithObserver(registry, nil)
}

func wrapTestOperationRegistryWithObserver(
	registry *operation.Registry,
	countObserver func(int),
) *testOperationRegistry {
	return &testOperationRegistry{
		bindPlacement: func(store PlacementAuthorityStore) (*placement.OperationCoordinator, error) {
			return store.BindOperationCoordinator(countObserver)
		},
		bindStandalone: registry.BindSettlementAuthority,
		claims:         make(map[testOperationKey]testOperationClaim),
		seeds:          make(map[string]testOperationSeed),
	}
}

// bindPlacementStore consumes the hidden Registry's one-shot settlement lane
// through the real durable composition boundary. Tests receive only the joined
// coordinator and its observation/drain-only runtime facet.
func (registry *testOperationRegistry) bindPlacementStore(
	store PlacementAuthorityStore,
) (*placement.OperationCoordinator, error) {
	if registry == nil || store == nil {
		return nil, errors.New("test placement store is required")
	}
	registry.bindingMu.Lock()
	defer registry.bindingMu.Unlock()
	if registry.coordinator != nil {
		return nil, errors.New("test operation registry placement authority is already bound")
	}
	coordinator, err := registry.bindPlacement(store)
	if err != nil {
		return nil, err
	}
	registry.coordinator = coordinator
	registry.callbackStore = concreteTestCallbackStore(store)
	registry.runtime = coordinator.RuntimeController()
	return coordinator, nil
}

func (registry *testOperationRegistry) standaloneAuthority() (
	operation.SettlementAuthority,
	bool,
) {
	if registry == nil {
		return operation.SettlementAuthority{}, false
	}
	registry.bindingMu.Lock()
	defer registry.bindingMu.Unlock()
	if registry.coordinator != nil {
		return operation.SettlementAuthority{}, false
	}
	if registry.authorityBound {
		return registry.authority, true
	}
	authority, err := registry.bindStandalone()
	if err != nil {
		return operation.SettlementAuthority{}, false
	}
	registry.authority = authority
	registry.authorityBound = true
	registry.runtime = authority.RuntimeController()
	return authority, true
}

func (registry *testOperationRegistry) currentAuthority() (
	operation.SettlementAuthority,
	bool,
) {
	if registry == nil {
		return operation.SettlementAuthority{}, false
	}
	registry.bindingMu.Lock()
	defer registry.bindingMu.Unlock()
	if registry.coordinator != nil || !registry.authorityBound {
		return operation.SettlementAuthority{}, false
	}
	return registry.authority, true
}

func (registry *testOperationRegistry) runtimeController() operation.RuntimeController {
	if registry == nil {
		return operation.RuntimeController{}
	}
	registry.bindingMu.Lock()
	defer registry.bindingMu.Unlock()
	return registry.runtime
}

func (registry *testOperationRegistry) callbackPlacementStore() *placement.Store {
	if registry == nil {
		return nil
	}
	registry.bindingMu.Lock()
	defer registry.bindingMu.Unlock()
	return registry.callbackStore
}

func (registry *testOperationRegistry) TryTrackInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	if len(items) == 0 {
		items = []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}
	}
	seed := testOperationSeed{
		leaseUUID: leaseUUID, tenant: tenant,
		items: append([]backend.LeaseItem(nil), items...), backendName: backendName,
	}
	registry.bindingMu.Lock()
	startBound := registry.startBound
	registry.bindingMu.Unlock()
	if startBound != nil {
		return startBound(seed)
	}
	requested, err := operation.NewProvisionInitiation(
		leaseUUID, tenant, items, backendName,
	)
	if err != nil {
		return operation.OperationID{}, false
	}
	id, tracked := registry.tryTrack(leaseUUID, requested)
	if tracked {
		registry.seedMu.Lock()
		registry.seeds[leaseUUID] = seed
		registry.seedMu.Unlock()
	}
	return id, tracked
}

func (registry *testOperationRegistry) tryTrack(
	leaseUUID string,
	requested operation.ProvisionInitiation,
) (operation.OperationID, bool) {
	if registry == nil || leaseUUID == "" {
		return operation.OperationID{}, false
	}
	authority, ok := registry.standaloneAuthority()
	if !ok {
		return operation.OperationID{}, false
	}
	claimResult := authority.TryClaimLeaseNow(leaseUUID)
	if !claimResult.Acquired() {
		return operation.OperationID{}, false
	}
	claim := claimResult.Claim()
	defer authority.ReleaseLease(claim)
	result := authority.TryInitiateProvisionClaimed(claim, requested)
	if !result.Started() {
		return operation.OperationID{}, false
	}
	initiation := result.Capability()
	dispatch, joined := authority.JoinDispatch(initiation)
	if !joined || !authority.BeginDispatchCall(dispatch) {
		authority.AbortInitiation(initiation)
		return operation.OperationID{}, false
	}
	if authority.ActivateDispatch(dispatch) != operation.InitiationActivated {
		return operation.OperationID{}, false
	}
	return initiation.ID(), true
}

func (registry *testOperationRegistry) TrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) {
	if current, exists := registry.GetInFlight(leaseUUID); exists &&
		!registry.UntrackInFlightIfOperationID(leaseUUID, current.OperationID) {
		return
	}
	_, _ = registry.TryTrackInFlightWithOperationID(
		leaseUUID, tenant, items, backendName,
	)
}

func (registry *testOperationRegistry) operationSeeds() []testOperationSeed {
	if registry == nil {
		return nil
	}
	registry.seedMu.Lock()
	defer registry.seedMu.Unlock()
	result := make([]testOperationSeed, 0, len(registry.seeds))
	for _, seed := range registry.seeds {
		seed.items = append([]backend.LeaseItem(nil), seed.items...)
		result = append(result, seed)
	}
	return result
}

func (registry *testOperationRegistry) UntrackInFlightIfOperationID(
	leaseUUID string,
	id operation.OperationID,
) bool {
	registry.bindingMu.Lock()
	finishBound := registry.finishBound
	registry.bindingMu.Unlock()
	if finishBound != nil {
		return finishBound(leaseUUID, id)
	}
	authority, ok := registry.currentAuthority()
	if !ok {
		return false
	}
	result := authority.TryClaimCallback(leaseUUID, id)
	return result.Claimed() && authority.FinishCallback(result.Claim())
}

func (registry *testOperationRegistry) GetInFlight(
	leaseUUID string,
) (testInFlightOperation, bool) {
	if registry == nil {
		return testInFlightOperation{}, false
	}
	registry.bindingMu.Lock()
	coordinator := registry.coordinator
	registry.bindingMu.Unlock()
	var metadata operation.SettlementMetadata
	var exists bool
	if coordinator != nil {
		metadata, exists = coordinator.Lookup(leaseUUID)
	} else if authority, ok := registry.currentAuthority(); ok {
		metadata, exists = authority.Lookup(leaseUUID)
	}
	if !exists || !metadata.Valid() {
		return testInFlightOperation{}, false
	}
	return testInFlightOperation{
		LeaseUUID:   metadata.LeaseUUID(),
		Tenant:      metadata.Tenant(),
		Items:       metadata.Items(),
		Backend:     metadata.Backend(),
		OperationID: metadata.ID(),
		StartTime:   metadata.StartedAt(),
		Kind:        metadata.Kind(),
	}, true
}

func (registry *testOperationRegistry) IsInFlight(leaseUUID string) bool {
	return registry.runtimeController().Contains(leaseUUID)
}

func (registry *testOperationRegistry) TryClaimInFlight(
	leaseUUID string,
	id operation.OperationID,
) (testInFlightOperation, bool) {
	return registry.claim(leaseUUID, id, false)
}

func (registry *testOperationRegistry) TryClaimInFlightForDeprovision(
	leaseUUID string,
	id operation.OperationID,
) (testInFlightOperation, bool) {
	return registry.claim(leaseUUID, id, true)
}

func (registry *testOperationRegistry) claim(
	leaseUUID string,
	id operation.OperationID,
	deprovision bool,
) (testInFlightOperation, bool) {
	authority, ok := registry.currentAuthority()
	if !ok {
		return testInFlightOperation{}, false
	}
	registry.claimsMu.Lock()
	defer registry.claimsMu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	var metadata operation.SettlementMetadata
	if deprovision {
		result := authority.TryClaimDeprovision(leaseUUID)
		if !result.Claimed() {
			return testInFlightOperation{}, false
		}
		claim := result.Claim()
		metadata = claim.Metadata()
		if metadata.ID() != id {
			authority.ReleaseDeprovision(claim)
			return testInFlightOperation{}, false
		}
		registry.claims[key] = testOperationClaim{
			kind: testOperationClaimDeprovision, deprovision: claim,
		}
	} else {
		result := authority.TryClaimCallback(leaseUUID, id)
		if !result.Claimed() {
			return testInFlightOperation{}, false
		}
		claim := result.Claim()
		metadata = claim.Metadata()
		registry.claims[key] = testOperationClaim{
			kind: testOperationClaimStandaloneCallback, standaloneCallback: claim,
		}
	}
	return testInFlightOperation{
		LeaseUUID:   metadata.LeaseUUID(),
		Tenant:      metadata.Tenant(),
		Items:       metadata.Items(),
		Backend:     metadata.Backend(),
		OperationID: metadata.ID(),
		StartTime:   metadata.StartedAt(),
		Kind:        metadata.Kind(),
	}, true
}

func (registry *testOperationRegistry) ReleaseInFlightClaim(
	leaseUUID string,
	id operation.OperationID,
) bool {
	authority, ok := registry.currentAuthority()
	if !ok {
		return false
	}
	registry.claimsMu.Lock()
	defer registry.claimsMu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	claim, exists := registry.claims[key]
	if !exists {
		return false
	}
	var released bool
	switch claim.kind {
	case testOperationClaimStandaloneCallback:
		released = authority.ReleaseCallback(claim.standaloneCallback)
	case testOperationClaimDeprovision:
		released = authority.ReleaseDeprovision(claim.deprovision)
	case testOperationClaimInvalid:
	}
	if released {
		delete(registry.claims, key)
	}
	return released
}

func (registry *testOperationRegistry) FinishClaimedInFlight(
	leaseUUID string,
	id operation.OperationID,
) bool {
	authority, ok := registry.currentAuthority()
	if !ok {
		return false
	}
	registry.claimsMu.Lock()
	defer registry.claimsMu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	claim, exists := registry.claims[key]
	if !exists {
		return false
	}
	var finished bool
	switch claim.kind {
	case testOperationClaimStandaloneCallback:
		finished = authority.FinishCallback(claim.standaloneCallback)
	case testOperationClaimDeprovision:
		finished = authority.FinishDeprovision(claim.deprovision)
	case testOperationClaimInvalid:
	}
	if finished {
		delete(registry.claims, key)
	}
	return finished
}
