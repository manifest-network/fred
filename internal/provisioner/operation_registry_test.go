package provisioner

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// testOperationRegistry retains only the opaque capabilities needed by older
// package fixtures to arrange and settle Registry state. Production Manager
// owns operation.Registry directly; this test helper is not a second lifecycle
// implementation and never copies mutable registry state.
type testOperationRegistry struct {
	*operation.Registry
	claims map[testOperationKey]operation.SettlementClaim
	mu     sync.Mutex
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

func (tracked testInFlightOperation) RoutingSKU() string {
	if len(tracked.Items) == 0 {
		return ""
	}
	return tracked.Items[0].SKU
}

func newTestOperationRegistry() *testOperationRegistry {
	return wrapTestOperationRegistry(operation.NewRegistryWithCountObserver(func(count int) {
		metrics.InFlightProvisions.Set(float64(count))
	}))
}

func wrapTestOperationRegistry(registry *operation.Registry) *testOperationRegistry {
	return &testOperationRegistry{
		Registry: registry,
		claims:   make(map[testOperationKey]operation.SettlementClaim),
	}
}

func (registry *testOperationRegistry) Operations() *operation.Registry {
	if registry == nil {
		return nil
	}
	return registry.Registry
}

func (registry *testOperationRegistry) TryTrackInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return registry.tryTrack(operation.TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		Kind:      operation.KindProvision,
	})
}

func (registry *testOperationRegistry) TryTrackRestoreInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return registry.tryTrack(operation.TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		Kind:      operation.KindRestore,
	})
}

func (registry *testOperationRegistry) tryTrack(
	spec operation.TrackSpec,
) (operation.OperationID, bool) {
	if registry == nil || registry.Registry == nil || !spec.Valid() {
		return operation.OperationID{}, false
	}
	claimResult := registry.TryClaimLeaseNow(spec.LeaseUUID)
	if !claimResult.Acquired() {
		return operation.OperationID{}, false
	}
	claim := claimResult.Claim()
	defer registry.ReleaseLease(claim)
	result := registry.TryInitiateClaimed(claim, spec)
	if !result.Started() {
		return operation.OperationID{}, false
	}
	initiation := result.Capability()
	if !registry.BeginCall(initiation) ||
		registry.Activate(initiation) != operation.InitiationActivated {
		registry.AbortInitiation(initiation)
		return operation.OperationID{}, false
	}
	return initiation.ID(), true
}

func (registry *testOperationRegistry) TrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) {
	registry.trackProvisionAt(leaseUUID, tenant, items, backendName, time.Now())
}

func (registry *testOperationRegistry) TrackInFlightWithStartTime(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	startedAt time.Time,
) {
	registry.trackProvisionAt(leaseUUID, tenant, items, backendName, startedAt)
}

func (registry *testOperationRegistry) trackProvisionAt(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	startedAt time.Time,
) {
	if registry == nil || registry.Registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if current, exists := registry.Lookup(leaseUUID); exists {
		settlement := registry.TryClaimCallback(leaseUUID, current.ID)
		if !settlement.Claimed() || !registry.FinishSettlement(settlement.Claim()) {
			return
		}
		delete(registry.claims, testOperationKey{leaseUUID: leaseUUID, id: current.ID})
	}
	claimResult := registry.TryClaimLeaseNow(leaseUUID)
	if !claimResult.Acquired() {
		return
	}
	claim := claimResult.Claim()
	defer registry.ReleaseLease(claim)
	result := registry.TryInitiateClaimed(claim, operation.TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		StartedAt: startedAt,
		Kind:      operation.KindProvision,
	})
	if result.Started() {
		initiation := result.Capability()
		if registry.BeginCall(initiation) {
			registry.Activate(initiation)
		}
	}
}

func (registry *testOperationRegistry) TryTrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	_, tracked := registry.TryTrackInFlightWithOperationID(
		leaseUUID, tenant, items, backendName,
	)
	return tracked
}

func (registry *testOperationRegistry) UntrackInFlightIfOperationID(
	leaseUUID string,
	id operation.OperationID,
) bool {
	if registry == nil || registry.Registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	result := registry.TryClaimCallback(leaseUUID, id)
	if !result.Claimed() || !registry.FinishSettlement(result.Claim()) {
		return false
	}
	delete(registry.claims, testOperationKey{leaseUUID: leaseUUID, id: id})
	return true
}

func (registry *testOperationRegistry) UntrackInFlight(leaseUUID string) {
	record, exists := registry.GetInFlight(leaseUUID)
	if exists {
		registry.UntrackInFlightIfOperationID(leaseUUID, record.OperationID)
	}
}

func (registry *testOperationRegistry) PopInFlight(
	leaseUUID string,
) (testInFlightOperation, bool) {
	record, exists := registry.GetInFlight(leaseUUID)
	if !exists || !registry.UntrackInFlightIfOperationID(leaseUUID, record.OperationID) {
		return testInFlightOperation{}, false
	}
	return record, true
}

func (registry *testOperationRegistry) GetInFlight(
	leaseUUID string,
) (testInFlightOperation, bool) {
	if registry == nil || registry.Registry == nil {
		return testInFlightOperation{}, false
	}
	record, exists := registry.Lookup(leaseUUID)
	if !exists {
		return testInFlightOperation{}, false
	}
	return testInFlightOperation{
		LeaseUUID:   record.LeaseUUID,
		Tenant:      record.Tenant,
		Items:       record.Items,
		Backend:     record.Backend,
		OperationID: record.ID,
		StartTime:   record.StartedAt,
		Kind:        record.Kind,
	}, true
}

func (registry *testOperationRegistry) IsInFlight(leaseUUID string) bool {
	return registry != nil && registry.Registry != nil && registry.Contains(leaseUUID)
}

func (registry *testOperationRegistry) InFlightCount() int {
	if registry == nil || registry.Registry == nil {
		return 0
	}
	return registry.Count()
}

func (registry *testOperationRegistry) GetInFlightLeases() []string {
	if registry == nil || registry.Registry == nil {
		return nil
	}
	return registry.LeaseUUIDs()
}

func (registry *testOperationRegistry) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if registry == nil || registry.Registry == nil {
		return 0
	}
	return registry.WaitForDrain(ctx, timeout)
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
	if registry == nil || registry.Registry == nil {
		return testInFlightOperation{}, false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	var result operation.SettlementResult
	if deprovision {
		result = registry.TryClaimDeprovision(leaseUUID, id)
	} else {
		result = registry.TryClaimCallback(leaseUUID, id)
	}
	if !result.Claimed() {
		return testInFlightOperation{}, false
	}
	registry.claims[key] = result.Claim()
	record := result.Record()
	return testInFlightOperation{
		LeaseUUID:   record.LeaseUUID,
		Tenant:      record.Tenant,
		Items:       record.Items,
		Backend:     record.Backend,
		OperationID: record.ID,
		StartTime:   record.StartedAt,
		Kind:        record.Kind,
	}, true
}

func (registry *testOperationRegistry) ReleaseInFlightClaim(
	leaseUUID string,
	id operation.OperationID,
) bool {
	if registry == nil || registry.Registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	claim, exists := registry.claims[key]
	if !exists || !registry.ReleaseSettlement(claim) {
		return false
	}
	delete(registry.claims, key)
	return true
}

func (registry *testOperationRegistry) FinishClaimedInFlight(
	leaseUUID string,
	id operation.OperationID,
) bool {
	if registry == nil || registry.Registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := testOperationKey{leaseUUID: leaseUUID, id: id}
	claim, exists := registry.claims[key]
	if !exists || !registry.FinishSettlement(claim) {
		return false
	}
	delete(registry.claims, key)
	return true
}

var managerTestOperationRegistries sync.Map

func managerTestOperationRegistry(manager *Manager) *testOperationRegistry {
	registry := wrapTestOperationRegistry(manager.operations)
	actual, _ := managerTestOperationRegistries.LoadOrStore(manager.operations, registry)
	return actual.(*testOperationRegistry)
}

func (manager *Manager) TrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) {
	managerTestOperationRegistry(manager).TrackInFlight(leaseUUID, tenant, items, backendName)
}

func (manager *Manager) TryTrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	return managerTestOperationRegistry(manager).TryTrackInFlight(
		leaseUUID, tenant, items, backendName,
	)
}

func (manager *Manager) TryTrackInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return managerTestOperationRegistry(manager).TryTrackInFlightWithOperationID(
		leaseUUID, tenant, items, backendName,
	)
}

func (manager *Manager) TryTrackRestoreInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return managerTestOperationRegistry(manager).TryTrackRestoreInFlightWithOperationID(
		leaseUUID, tenant, items, backendName,
	)
}

func (manager *Manager) UntrackInFlight(leaseUUID string) {
	managerTestOperationRegistry(manager).UntrackInFlight(leaseUUID)
}

func (manager *Manager) PopInFlight(leaseUUID string) (testInFlightOperation, bool) {
	return managerTestOperationRegistry(manager).PopInFlight(leaseUUID)
}

func (manager *Manager) GetInFlight(leaseUUID string) (testInFlightOperation, bool) {
	return managerTestOperationRegistry(manager).GetInFlight(leaseUUID)
}

func (manager *Manager) GetInFlightLeases() []string {
	return manager.operations.LeaseUUIDs()
}
