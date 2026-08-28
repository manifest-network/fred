package provisioner

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// ProvisionKind distinguishes a fresh provision from a restore. A restore IS a
// provisioning operation — it brings a lease's deployment up from retained
// volumes instead of a fresh manifest — so it shares the provisioning metrics
// and is differentiated by an operation label rather than a separate metric,
// per Prometheus naming guidance (sum/avg across the dimension stays meaningful).
type ProvisionKind uint8

const (
	KindProvision ProvisionKind = iota
	KindRestore
)

// InFlightProvision is the temporary compatibility view of an operation.Record.
// New coordination code uses operation.Token and operation.OperationID. This
// view carries the same typed OperationID and never exposes a numeric wire
// token.
type InFlightProvision struct {
	LeaseUUID   string
	Tenant      string
	Items       []backend.LeaseItem
	Backend     string
	OperationID operation.OperationID
	StartTime   time.Time
	Kind        ProvisionKind
}

func (p InFlightProvision) RoutingSKU() string {
	if len(p.Items) == 0 {
		return ""
	}
	return p.Items[0].SKU
}

// InFlightTracker is the temporary compatibility interface used while callers
// migrate to narrow operation.Registry capabilities.
type InFlightTracker interface {
	TryTrackInFlightWithOperationID(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (operation.OperationID, bool)
	SnapshotMutationRevision() uint64
	TryTrackInFlightWithOperationIDIfNotNewer(
		leaseUUID, tenant string,
		items []backend.LeaseItem,
		backendName string,
		snapshotHandle uint64,
	) (operationID operation.OperationID, tracked, snapshotStale bool)
	TryClaimLeaseActionIfNotNewer(leaseUUID string, snapshotHandle uint64) (claimed, snapshotStale bool)
	TryClaimLeaseAction(leaseUUID string) bool
	ReleaseLeaseAction(leaseUUID string) bool
	TryTrackRestoreInFlightWithOperationID(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (operation.OperationID, bool)
	UntrackInFlightIfOperationID(leaseUUID string, operationID operation.OperationID) bool
	TryClaimInFlight(leaseUUID string, operationID operation.OperationID) (InFlightProvision, bool)
	TryClaimInFlightForDeprovision(leaseUUID string, operationID operation.OperationID) (InFlightProvision, bool)
	ReleaseInFlightClaim(leaseUUID string, operationID operation.OperationID) bool
	FinishClaimedInFlight(leaseUUID string, operationID operation.OperationID) bool
	GetInFlight(leaseUUID string) (InFlightProvision, bool)
	IsInFlight(leaseUUID string) bool
	InFlightCount() int
	InFlightCountsByBackend() map[string]int
	GetInFlightLeases() []string
	WaitForDrain(ctx context.Context, timeout time.Duration) int
	GetTimedOutProvisions(timeout time.Duration) []InFlightProvision
}

type legacyOperationKey struct {
	leaseUUID   string
	operationID operation.OperationID
}

type legacyLeaseClaimCapability struct {
	claim          operation.LeaseClaim
	snapshotHandle uint64
	mutated        bool
}

const legacySnapshotWindow = 128

// DefaultInFlightTracker is a compatibility adapter over operation.Registry.
// Registry is the sole owner of operation, revision, and claim state. The maps
// below contain only opaque capabilities issued by Registry; they never copy
// mutable lifecycle state.
type DefaultInFlightTracker struct {
	registry         *operation.Registry
	tokens           map[legacyOperationKey]operation.Token
	leaseClaims      map[string]legacyLeaseClaimCapability
	settlementClaims map[legacyOperationKey]operation.SettlementClaim
	snapshots        map[uint64]operation.TrackerSnapshot
	snapshotOrder    []uint64
	nextSnapshot     uint64
	mu               sync.Mutex
}

func NewInFlightTracker() *DefaultInFlightTracker {
	return &DefaultInFlightTracker{
		registry: operation.NewRegistryWithCountObserver(func(count int) {
			metrics.InFlightProvisions.Set(float64(count))
		}),
		tokens:           make(map[legacyOperationKey]operation.Token),
		leaseClaims:      make(map[string]legacyLeaseClaimCapability),
		settlementClaims: make(map[legacyOperationKey]operation.SettlementClaim),
		snapshots:        make(map[uint64]operation.TrackerSnapshot),
	}
}

// Operations exposes the single typed registry for incremental caller
// migration. The compatibility adapter holds no duplicate lifecycle state.
func (t *DefaultInFlightTracker) Operations() *operation.Registry {
	return t.registry
}

var _ InFlightTracker = (*DefaultInFlightTracker)(nil)

func (t *DefaultInFlightTracker) TryTrackInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return t.tryTrack(leaseUUID, tenant, items, backendName, KindProvision)
}

func (t *DefaultInFlightTracker) TryTrackInFlightWithOperationIDIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	snapshotHandle uint64,
) (operation.OperationID, bool, bool) {
	spec := operationTrackSpec(
		leaseUUID, tenant, items, backendName, KindProvision, time.Time{},
	)
	if !spec.Valid() || t.registry == nil {
		return operation.OperationID{}, false, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureCapabilityMapsLocked()
	leaseCapability, claimed := t.leaseClaims[leaseUUID]
	if !claimed {
		_, knownSnapshot := t.snapshots[snapshotHandle]
		return operation.OperationID{}, false, !knownSnapshot
	}
	if leaseCapability.snapshotHandle != snapshotHandle || leaseCapability.mutated {
		return operation.OperationID{}, false, true
	}

	result := t.registry.TryTrackClaimed(leaseCapability.claim, spec)
	operationID, tracked := t.rememberTrackResultLocked(leaseUUID, result)
	if tracked {
		leaseCapability.mutated = true
		t.leaseClaims[leaseUUID] = leaseCapability
	}
	return operationID, tracked, false
}

func (t *DefaultInFlightTracker) TryTrackRestoreInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return t.tryTrack(leaseUUID, tenant, items, backendName, KindRestore)
}

func (t *DefaultInFlightTracker) tryTrack(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	kind ProvisionKind,
) (operation.OperationID, bool) {
	spec := operationTrackSpec(
		leaseUUID, tenant, items, backendName, kind, time.Time{},
	)
	if !spec.Valid() || t.registry == nil {
		return operation.OperationID{}, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureCapabilityMapsLocked()
	result := t.registry.TryTrack(spec)
	return t.rememberTrackResultLocked(leaseUUID, result)
}

func (t *DefaultInFlightTracker) rememberTrackResultLocked(
	leaseUUID string,
	result operation.TrackResult,
) (operationID operation.OperationID, tracked bool) {
	if !result.Started() {
		return operation.OperationID{}, false
	}
	operationID = result.Token().ID()
	t.forgetLeaseCapabilitiesLocked(leaseUUID)
	t.tokens[legacyOperationKey{leaseUUID: leaseUUID, operationID: operationID}] = result.Token()
	return operationID, true
}

// SnapshotMutationRevision preserves the historical method shape, but its
// uint64 result is an opaque adapter handle rather than a registry revision.
// Unknown and evicted handles always fail closed as stale.
func (t *DefaultInFlightTracker) SnapshotMutationRevision() uint64 {
	if t.registry == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureCapabilityMapsLocked()
	snapshot := t.registry.Snapshot()
	if !snapshot.Valid() {
		return 0
	}

	handle := t.allocateSnapshotHandleLocked()
	t.snapshots[handle] = snapshot
	t.snapshotOrder = append(t.snapshotOrder, handle)
	t.pruneSnapshotsLocked()
	return handle
}

func (t *DefaultInFlightTracker) TryClaimLeaseActionIfNotNewer(
	leaseUUID string,
	snapshotHandle uint64,
) (bool, bool) {
	if leaseUUID == "" || t.registry == nil {
		return false, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureCapabilityMapsLocked()
	snapshot, known := t.snapshots[snapshotHandle]
	if !known {
		return false, true
	}
	result := t.registry.TryClaimLease(leaseUUID, snapshot)
	if !result.Acquired() {
		return false, result.Outcome() == operation.LeaseClaimSnapshotStale
	}
	t.leaseClaims[leaseUUID] = legacyLeaseClaimCapability{
		claim:          result.Claim(),
		snapshotHandle: snapshotHandle,
	}
	return true, false
}

func (t *DefaultInFlightTracker) TryClaimLeaseAction(leaseUUID string) bool {
	if leaseUUID == "" || t.registry == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.ensureCapabilityMapsLocked()
	result := t.registry.TryClaimLeaseNow(leaseUUID)
	if !result.Acquired() {
		return false
	}
	t.leaseClaims[leaseUUID] = legacyLeaseClaimCapability{claim: result.Claim()}
	return true
}

func (t *DefaultInFlightTracker) ReleaseLeaseAction(leaseUUID string) bool {
	if t.registry == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	capability, exists := t.leaseClaims[leaseUUID]
	if !exists || !t.registry.ReleaseLease(capability.claim) {
		return false
	}
	delete(t.leaseClaims, leaseUUID)
	return true
}

func (t *DefaultInFlightTracker) UntrackInFlightIfOperationID(leaseUUID string, operationID operation.OperationID) bool {
	if t.registry == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	key := legacyOperationKey{leaseUUID: leaseUUID, operationID: operationID}
	token, exists := t.tokens[key]
	if !exists || !t.registry.Abort(token) {
		return false
	}
	t.forgetOperationCapabilityLocked(key)
	return true
}

func (t *DefaultInFlightTracker) TryClaimInFlight(
	leaseUUID string,
	operationID operation.OperationID,
) (InFlightProvision, bool) {
	return t.tryClaimInFlight(leaseUUID, operationID, false)
}

func (t *DefaultInFlightTracker) TryClaimInFlightForDeprovision(
	leaseUUID string,
	operationID operation.OperationID,
) (InFlightProvision, bool) {
	return t.tryClaimInFlight(leaseUUID, operationID, true)
}

func (t *DefaultInFlightTracker) tryClaimInFlight(
	leaseUUID string,
	operationID operation.OperationID,
	deprovision bool,
) (InFlightProvision, bool) {
	if t.registry == nil {
		return InFlightProvision{}, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	key := legacyOperationKey{leaseUUID: leaseUUID, operationID: operationID}
	token, exists := t.tokens[key]
	if !exists {
		return InFlightProvision{}, false
	}
	var result operation.SettlementResult
	if deprovision {
		result = t.registry.TryClaimDeprovision(leaseUUID, token.ID())
	} else {
		result = t.registry.TryClaimCallback(leaseUUID, token.ID())
	}
	if !result.Claimed() {
		return InFlightProvision{}, false
	}
	t.settlementClaims[key] = result.Claim()
	return inFlightProvisionFromRecord(result.Record()), true
}

func (t *DefaultInFlightTracker) ReleaseInFlightClaim(leaseUUID string, operationID operation.OperationID) bool {
	if t.registry == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	key := legacyOperationKey{leaseUUID: leaseUUID, operationID: operationID}
	claim, exists := t.settlementClaims[key]
	if !exists || !t.registry.ReleaseSettlement(claim) {
		return false
	}
	delete(t.settlementClaims, key)
	return true
}

func (t *DefaultInFlightTracker) FinishClaimedInFlight(leaseUUID string, operationID operation.OperationID) bool {
	if t.registry == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	key := legacyOperationKey{leaseUUID: leaseUUID, operationID: operationID}
	claim, exists := t.settlementClaims[key]
	if !exists || !t.registry.FinishSettlement(claim) {
		return false
	}
	t.forgetOperationCapabilityLocked(key)
	return true
}

func (t *DefaultInFlightTracker) ensureCapabilityMapsLocked() {
	if t.tokens == nil {
		t.tokens = make(map[legacyOperationKey]operation.Token)
	}
	if t.leaseClaims == nil {
		t.leaseClaims = make(map[string]legacyLeaseClaimCapability)
	}
	if t.settlementClaims == nil {
		t.settlementClaims = make(map[legacyOperationKey]operation.SettlementClaim)
	}
	if t.snapshots == nil {
		t.snapshots = make(map[uint64]operation.TrackerSnapshot)
	}
}

func (t *DefaultInFlightTracker) allocateSnapshotHandleLocked() uint64 {
	for {
		t.nextSnapshot++
		if t.nextSnapshot == 0 {
			continue
		}
		if _, exists := t.snapshots[t.nextSnapshot]; !exists {
			return t.nextSnapshot
		}
	}
}

func (t *DefaultInFlightTracker) pruneSnapshotsLocked() {
	for len(t.snapshotOrder) > legacySnapshotWindow {
		pruneIndex := -1
		for index, handle := range t.snapshotOrder {
			if !t.snapshotInUseLocked(handle) {
				pruneIndex = index
				break
			}
		}
		if pruneIndex < 0 {
			return
		}
		handle := t.snapshotOrder[pruneIndex]
		delete(t.snapshots, handle)
		t.snapshotOrder = append(t.snapshotOrder[:pruneIndex], t.snapshotOrder[pruneIndex+1:]...)
	}
}

func (t *DefaultInFlightTracker) snapshotInUseLocked(handle uint64) bool {
	for _, capability := range t.leaseClaims {
		if capability.snapshotHandle == handle {
			return true
		}
	}
	return false
}

func (t *DefaultInFlightTracker) forgetLeaseCapabilitiesLocked(leaseUUID string) {
	for key := range t.tokens {
		if key.leaseUUID == leaseUUID {
			delete(t.tokens, key)
		}
	}
	for key := range t.settlementClaims {
		if key.leaseUUID == leaseUUID {
			delete(t.settlementClaims, key)
		}
	}
}

func (t *DefaultInFlightTracker) forgetOperationCapabilityLocked(key legacyOperationKey) {
	delete(t.tokens, key)
	delete(t.settlementClaims, key)
}

func (t *DefaultInFlightTracker) IsInFlight(leaseUUID string) bool {
	return t.registry.Contains(leaseUUID)
}

func (t *DefaultInFlightTracker) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	record, exists := t.registry.Lookup(leaseUUID)
	if !exists {
		return InFlightProvision{}, false
	}
	return inFlightProvisionFromRecord(record), true
}

func (t *DefaultInFlightTracker) InFlightCount() int {
	return t.registry.Count()
}

func (t *DefaultInFlightTracker) InFlightCountsByBackend() map[string]int {
	return t.registry.CountsByBackend()
}

func (t *DefaultInFlightTracker) GetInFlightLeases() []string {
	return t.registry.LeaseUUIDs()
}

func (t *DefaultInFlightTracker) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if t.InFlightCount() == 0 {
		return 0
	}
	slog.Info("waiting for in-flight provisions to drain",
		"count", t.InFlightCount(), "timeout", timeout)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			remaining := t.InFlightCount()
			if remaining > 0 {
				slog.Warn("drain interrupted by context cancellation",
					"remaining", remaining, "leases", t.GetInFlightLeases())
			}
			return remaining
		case <-ticker.C:
			count := t.InFlightCount()
			if count == 0 {
				slog.Info("all in-flight provisions drained successfully")
				return 0
			}
			if time.Now().After(deadline) {
				slog.Warn("drain timeout expired with provisions still in-flight",
					"remaining", count, "leases", t.GetInFlightLeases())
				return count
			}
			slog.Debug("waiting for provisions to drain",
				"remaining", count, "time_left", time.Until(deadline).Round(time.Second))
		}
	}
}

func (t *DefaultInFlightTracker) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	t.mu.Lock()
	defer t.mu.Unlock()
	records := t.registry.TimedOut(timeout)
	provisions := make([]InFlightProvision, 0, len(records))
	for _, record := range records {
		provisions = append(provisions, inFlightProvisionFromRecord(record))
	}
	return provisions
}

func operationTrackSpec(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	kind ProvisionKind,
	startedAt time.Time,
) operation.TrackSpec {
	operationKind := operation.KindProvision
	if kind == KindRestore {
		operationKind = operation.KindRestore
	}
	return operation.TrackSpec{
		LeaseUUID: leaseUUID, Tenant: tenant, Items: items, Backend: backendName,
		StartedAt: startedAt, Kind: operationKind,
	}
}

func inFlightProvisionFromRecord(record operation.Record) InFlightProvision {
	kind := KindProvision
	if record.Kind == operation.KindRestore {
		kind = KindRestore
	}
	return InFlightProvision{
		LeaseUUID: record.LeaseUUID, Tenant: record.Tenant, Items: record.Items,
		Backend: record.Backend, OperationID: record.ID,
		StartTime: record.StartedAt, Kind: kind,
	}
}
