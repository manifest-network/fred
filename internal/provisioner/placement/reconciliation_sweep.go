package placement

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/provisioner/leaseitems"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

type reconciliationSweepMarker struct{ _ byte }
type projectedReconciliationSweepMarker struct{ _ byte }
type inventoryResponseMarker struct{ _ byte }

var (
	ErrReconciliationBoundaryStale = errors.New("reconciliation causal boundary is stale")
	ErrReconciliationOperationBusy = errors.New("lease already has a lifecycle operation")
)

// ReconciliationSweep is the indivisible causal boundary for one inventory
// pass. The Store fence, operation boundary, and topology-bound collector
// session are minted together and never exposed separately. End invalidates
// the Store fence; starting another collection invalidates the inventory
// snapshot through the collector epoch.
//
// The zero value is invalid.
type ReconciliationSweep struct {
	coordinator *ReconciliationCoordinator
	marker      *reconciliationSweepMarker
	fence       inventoryFence
	operations  operation.ReconciliationBoundary
	collection  *inventory.Session
	records     map[string]Placement
	baseline    AdmissionBaseline

	mu                sync.Mutex
	sealed            inventory.Snapshot
	positive          bool
	collecting        int
	pendingProvisions map[*inventoryResponseMarker]BackendProvisionInventory
	pendingRetentions map[*inventoryResponseMarker]BackendRetentionInventory
	ended             bool
	projected         bool
}

// CollectProvisionInventory performs one backend read through this exact
// sweep and installs lease-local positive barriers before any returned row can
// escape to policy code. A raw endpoint response therefore cannot be buffered
// independently of the Store fence that prevents concurrent side effects.
func (sweep *ReconciliationSweep) CollectProvisionInventory(
	ctx context.Context,
	backendName string,
) (BackendProvisionInventory, error) {
	if sweep == nil {
		return BackendProvisionInventory{}, inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	if !sweep.validLocked() || sweep.sealed.Present() {
		sweep.mu.Unlock()
		return BackendProvisionInventory{}, inventory.ErrInvalidSession
	}
	coordinator := sweep.coordinator
	sweep.collecting++
	sweep.mu.Unlock()

	result, err := coordinator.collectProvisionInventory(ctx, backendName)
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	sweep.collecting--
	if err != nil {
		return BackendProvisionInventory{}, err
	}
	if !sweep.validLocked() || sweep.sealed.Present() {
		return BackendProvisionInventory{}, inventory.ErrInvalidSession
	}
	if len(result.provisions) != 0 {
		sweep.positive = true
		leaseUUIDs := make([]string, 0, len(result.provisions))
		for _, provision := range result.provisions {
			leaseUUIDs = append(leaseUUIDs, provision.LeaseUUID)
		}
		if err := coordinator.coordinator.store.recordUnprojectedPositives(
			sweep.fence, result.backendName, inventoryPositiveProvision, leaseUUIDs,
		); err != nil {
			return BackendProvisionInventory{}, err
		}
	}
	result.sweep = sweep
	result.marker = sweep.marker
	result.sweepID = sweep.fence.sweepID
	result.receipt = &inventoryResponseMarker{}
	sweep.pendingProvisions[result.receipt] = result
	return result, nil
}

// CollectRetentionInventory is the retention half of
// CollectProvisionInventory. Positive retained-data affinity is fenced before
// the opaque response is returned.
func (sweep *ReconciliationSweep) CollectRetentionInventory(
	ctx context.Context,
	backendName string,
) (BackendRetentionInventory, error) {
	if sweep == nil {
		return BackendRetentionInventory{}, inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	if !sweep.validLocked() || sweep.sealed.Present() {
		sweep.mu.Unlock()
		return BackendRetentionInventory{}, inventory.ErrInvalidSession
	}
	coordinator := sweep.coordinator
	sweep.collecting++
	sweep.mu.Unlock()

	result, err := coordinator.collectRetentionInventory(ctx, backendName)
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	sweep.collecting--
	if err != nil {
		return BackendRetentionInventory{}, err
	}
	if !sweep.validLocked() || sweep.sealed.Present() {
		return BackendRetentionInventory{}, inventory.ErrInvalidSession
	}
	if len(result.retentions) != 0 {
		sweep.positive = true
		leaseUUIDs := make([]string, 0, len(result.retentions))
		for _, retention := range result.retentions {
			leaseUUIDs = append(leaseUUIDs, retention.LeaseUUID)
		}
		if err := coordinator.coordinator.store.recordUnprojectedPositives(
			sweep.fence, result.backendName, inventoryPositiveRetention, leaseUUIDs,
		); err != nil {
			return BackendRetentionInventory{}, err
		}
	}
	result.sweep = sweep
	result.marker = sweep.marker
	result.sweepID = sweep.fence.sweepID
	result.receipt = &inventoryResponseMarker{}
	sweep.pendingRetentions[result.receipt] = result
	return result, nil
}

// BeginSweep constructs one opaque value containing all local authority needed
// by an inventory pass before backend reads begin. Chain inventory is collected
// first because it cannot reveal backend ownership. A caller cannot combine a
// Store fence from one pass with an operation boundary or collector session
// from another.
func (authority *ReconciliationCoordinator) BeginSweep() (*ReconciliationSweep, error) {
	if !authority.Valid() {
		return nil, errors.New("reconciliation coordinator is invalid")
	}
	boundary := authority.coordinator.operations.CaptureReconciliationBoundary()
	if !boundary.Valid() {
		return nil, errors.New("capture reconciliation operation boundary")
	}
	collection := authority.projector.beginCollection()
	if collection == nil {
		return nil, errors.New("begin reconciliation inventory collection")
	}
	// The durable dirty marker is the final local construction step and the
	// first backend-observation step. Boundary capture and collector allocation
	// cannot observe a remote positive, so their failure must not manufacture a
	// restart-wide recovery requirement.
	fence, err := authority.coordinator.store.beginInventorySession()
	if err != nil {
		return nil, fmt.Errorf("begin durable inventory session: %w", err)
	}
	if !fence.valid() {
		return nil, ErrInvalidInventoryFence
	}
	return &ReconciliationSweep{
		coordinator:       authority,
		marker:            &reconciliationSweepMarker{},
		fence:             fence,
		operations:        boundary,
		collection:        collection,
		records:           authority.coordinator.store.List(),
		baseline:          authority.coordinator.store.CurrentAdmissionBaseline(),
		pendingProvisions: make(map[*inventoryResponseMarker]BackendProvisionInventory),
		pendingRetentions: make(map[*inventoryResponseMarker]BackendRetentionInventory),
	}, nil
}

func (sweep *ReconciliationSweep) validLocked() bool {
	return sweep != nil && sweep.coordinator != nil && sweep.coordinator.Valid() &&
		sweep.marker != nil && sweep.fence.valid() && sweep.operations.Valid() &&
		sweep.collection != nil && !sweep.ended
}

func (sweep *ReconciliationSweep) Valid() bool {
	if sweep == nil {
		return false
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	return sweep.validLocked()
}

// End releases the Store inventory fence. It is idempotent and makes every
// later attempt to record, project, or mint an action capability fail closed.
func (sweep *ReconciliationSweep) End() {
	if sweep == nil {
		return
	}
	sweep.mu.Lock()
	if sweep.ended {
		sweep.mu.Unlock()
		return
	}
	sweep.ended = true
	coordinator := sweep.coordinator
	fence := sweep.fence
	report := inventorySessionReport{evidence: sweep.sealed, hasPositive: sweep.positive}
	sweep.mu.Unlock()
	if coordinator != nil && coordinator.Valid() {
		coordinator.coordinator.store.endInventorySession(fence, report)
	}
}

func (sweep *ReconciliationSweep) recordProvision(
	backendName string,
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
) error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 {
		return inventory.ErrInvalidSession
	}
	if len(provisions) != 0 {
		sweep.positive = true
		leaseUUIDs := make([]string, 0, len(provisions))
		for _, provision := range provisions {
			leaseUUIDs = append(leaseUUIDs, provision.LeaseUUID)
		}
		if err := sweep.coordinator.coordinator.store.recordUnprojectedPositives(
			sweep.fence, backendName, inventoryPositiveProvision, leaseUUIDs,
		); err != nil {
			return err
		}
	}
	return sweep.collection.RecordProvision(backendName, storageID, provisions)
}

// BackendInventoryDisposition is the closed outcome of consuming a paired
// backend response. The zero value is invalid and grants no endpoint or
// absence authority.
type BackendInventoryDisposition uint8

const (
	BackendInventoryInvalid BackendInventoryDisposition = iota
	BackendInventoryAuthoritative
	BackendInventoryUntrusted
)

// RecordBackendInventory consumes provision and retention receipts from this
// exact sweep as one transition. Identity, refresh, and cross-endpoint checks
// live here so callers cannot independently choose to trust a malformed half.
func (sweep *ReconciliationSweep) RecordBackendInventory(
	provisionResponse BackendProvisionInventory,
	retentionResponse BackendRetentionInventory,
) (BackendInventoryDisposition, error) {
	if sweep == nil {
		return BackendInventoryInvalid, inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 ||
		provisionResponse.sweep != sweep || provisionResponse.marker != sweep.marker ||
		provisionResponse.sweepID != sweep.fence.sweepID || provisionResponse.receipt == nil ||
		retentionResponse.sweep != sweep || retentionResponse.marker != sweep.marker ||
		retentionResponse.sweepID != sweep.fence.sweepID || retentionResponse.receipt == nil {
		return BackendInventoryInvalid, inventory.ErrInvalidSession
	}
	provision, provisionPending := sweep.pendingProvisions[provisionResponse.receipt]
	retention, retentionPending := sweep.pendingRetentions[retentionResponse.receipt]
	if !provisionPending || !retentionPending ||
		provision.backendName != retention.backendName {
		return BackendInventoryInvalid, inventory.ErrInvalidSession
	}

	authoritative := provision.refreshErr == nil && provision.storageID.Valid() &&
		provision.storageID == retention.storageID
	if expected, bound := sweep.coordinator.coordinator.store.ExpectedBackendStorageIdentity(
		provision.backendName,
	); bound && expected != provision.storageID {
		authoritative = false
	}
	provisioned := make(map[string]struct{}, len(provision.provisions))
	for _, row := range provision.provisions {
		if row.LeaseUUID == "" || row.BackendName != provision.backendName {
			authoritative = false
		}
		if _, duplicate := provisioned[row.LeaseUUID]; duplicate {
			authoritative = false
		}
		provisioned[row.LeaseUUID] = struct{}{}
	}
	retained := retentionLeaseUUIDs(retention.retentions)
	seenRetentions := make(map[string]struct{}, len(retained))
	for _, leaseUUID := range retained {
		if leaseUUID == "" {
			authoritative = false
		}
		if _, duplicate := seenRetentions[leaseUUID]; duplicate {
			authoritative = false
		}
		if _, duplicate := provisioned[leaseUUID]; duplicate {
			authoritative = false
		}
		seenRetentions[leaseUUID] = struct{}{}
	}

	var err error
	disposition := BackendInventoryUntrusted
	if authoritative {
		err = sweep.collection.RecordBackend(
			provision.backendName,
			provision.storageID,
			provision.provisions,
			retained,
		)
		disposition = BackendInventoryAuthoritative
	} else {
		leaseUUIDs := make([]string, 0, len(provisioned)+len(seenRetentions))
		for leaseUUID := range provisioned {
			leaseUUIDs = append(leaseUUIDs, leaseUUID)
		}
		for leaseUUID := range seenRetentions {
			if _, duplicate := provisioned[leaseUUID]; !duplicate {
				leaseUUIDs = append(leaseUUIDs, leaseUUID)
			}
		}
		err = sweep.collection.RecordUntrusted(provision.backendName, leaseUUIDs)
	}
	if err != nil {
		return BackendInventoryInvalid, err
	}
	delete(sweep.pendingProvisions, provisionResponse.receipt)
	delete(sweep.pendingRetentions, retentionResponse.receipt)
	return disposition, nil
}

// RejectProvisionInventory consumes a provision response without granting it
// endpoint authority. Every positive identity is still sealed as untrusted;
// callers cannot turn a rejected response into apparent absence by dropping
// the DTO.
func (sweep *ReconciliationSweep) RejectProvisionInventory(
	response BackendProvisionInventory,
) error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 ||
		response.sweep != sweep || response.marker != sweep.marker ||
		response.sweepID != sweep.fence.sweepID || response.receipt == nil {
		return inventory.ErrInvalidSession
	}
	canonical, pending := sweep.pendingProvisions[response.receipt]
	if !pending {
		return inventory.ErrInvalidSession
	}
	leaseUUIDs := make([]string, 0, len(canonical.provisions))
	for _, provision := range canonical.provisions {
		leaseUUIDs = append(leaseUUIDs, provision.LeaseUUID)
	}
	if err := sweep.collection.RecordUntrusted(canonical.backendName, leaseUUIDs); err != nil {
		return err
	}
	delete(sweep.pendingProvisions, response.receipt)
	return nil
}

func (sweep *ReconciliationSweep) recordRetention(
	backendName string,
	storageID backendidentity.ID,
	leaseUUIDs []string,
) error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 {
		return inventory.ErrInvalidSession
	}
	if len(leaseUUIDs) != 0 {
		sweep.positive = true
		if err := sweep.coordinator.coordinator.store.recordUnprojectedPositives(
			sweep.fence, backendName, inventoryPositiveRetention, leaseUUIDs,
		); err != nil {
			return err
		}
	}
	return sweep.collection.RecordRetention(backendName, storageID, leaseUUIDs)
}

// RejectRetentionInventory is the retention counterpart of
// RejectProvisionInventory.
func (sweep *ReconciliationSweep) RejectRetentionInventory(
	response BackendRetentionInventory,
) error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 ||
		response.sweep != sweep || response.marker != sweep.marker ||
		response.sweepID != sweep.fence.sweepID || response.receipt == nil {
		return inventory.ErrInvalidSession
	}
	canonical, pending := sweep.pendingRetentions[response.receipt]
	if !pending {
		return inventory.ErrInvalidSession
	}
	if err := sweep.collection.RecordUntrusted(
		canonical.backendName, retentionLeaseUUIDs(canonical.retentions),
	); err != nil {
		return err
	}
	delete(sweep.pendingRetentions, response.receipt)
	return nil
}

func retentionLeaseUUIDs(retentions []backend.RetainedLease) []string {
	leaseUUIDs := make([]string, 0, len(retentions))
	for _, retention := range retentions {
		leaseUUIDs = append(leaseUUIDs, retention.LeaseUUID)
	}
	return leaseUUIDs
}

func (sweep *ReconciliationSweep) recordUntrusted(
	backendName string,
	leaseUUIDs []string,
) error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 {
		return inventory.ErrInvalidSession
	}
	if len(leaseUUIDs) != 0 {
		sweep.positive = true
		if err := sweep.coordinator.coordinator.store.recordUnprojectedPositives(
			sweep.fence, backendName, inventoryPositiveUntrusted, leaseUUIDs,
		); err != nil {
			return err
		}
	}
	return sweep.collection.RecordUntrusted(backendName, leaseUUIDs)
}

// SealInventory seals this sweep's exact collection session. The snapshot is
// retained privately; callers cannot transplant it into another fence or
// projection.
func (sweep *ReconciliationSweep) SealInventory() error {
	if sweep == nil {
		return inventory.ErrInvalidSession
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || sweep.sealed.Present() || sweep.collecting != 0 ||
		len(sweep.pendingProvisions) != 0 || len(sweep.pendingRetentions) != 0 {
		return inventory.ErrInvalidSession
	}
	snapshot, err := sweep.collection.Seal()
	if err != nil {
		return err
	}
	sweep.sealed = snapshot
	return nil
}

func (sweep *ReconciliationSweep) InventoryComplete() bool {
	if sweep == nil {
		return false
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	return sweep.validLocked() && sweep.sealed.Present() &&
		sweep.coordinator.projector.complete(sweep.sealed)
}

func (sweep *ReconciliationSweep) WasInFlight(leaseUUID string) bool {
	if sweep == nil {
		return false
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	return sweep.validLocked() && sweep.operations.WasInFlight(leaseUUID)
}

// InitialRecord returns the durable placement observed at this sweep's causal
// boundary. Projection planning cannot splice in a later Store view.
func (sweep *ReconciliationSweep) InitialRecord(leaseUUID string) Placement {
	if !sweep.Valid() {
		return Placement{}
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	return clonePlacement(sweep.records[leaseUUID])
}

// InitialRecords returns a detached copy of the durable placement view bound
// to this sweep. It is observation only and carries no mutation authority.
func (sweep *ReconciliationSweep) InitialRecords() map[string]Placement {
	if !sweep.Valid() {
		return nil
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	return clonePlacements(sweep.records)
}

// ReconciliationProjection contains only policy choices derived from positive
// inventory observations. Negative evidence, completeness, physical identity,
// lifecycle generation, and runtime principal all come exclusively from the
// sealed session retained by ReconciliationSweep.
type ReconciliationProjection struct {
	Placements         map[string]string
	Conflicts          map[string][]string
	UntrustedPositives map[string][]string
}

// ProjectedReconciliationSweep is the only source of lease-action and prune
// capabilities for a successful projection. It closes over the same Store
// fence, operation boundary, and sealed collector snapshot.
type ProjectedReconciliationSweep struct {
	sweep    *ReconciliationSweep
	marker   *projectedReconciliationSweepMarker
	result   projectionResult
	records  map[string]Placement
	baseline AdmissionBaseline
	eligible map[string]struct{}
	complete bool
}

func (sweep *ReconciliationSweep) Project(
	projection ReconciliationProjection,
) (*ProjectedReconciliationSweep, error) {
	if sweep == nil {
		return nil, ErrInvalidInventoryFence
	}
	sweep.mu.Lock()
	defer sweep.mu.Unlock()
	if !sweep.validLocked() || !sweep.sealed.Present() || sweep.projected {
		return nil, ErrInvalidInventoryFence
	}
	// Projection is a one-shot transition. A failed durable projection may have
	// partially advanced the Store's fencing metadata and must be retried only
	// by a new sweep with a fresh inventory snapshot and operation boundary.
	sweep.projected = true
	causalExclusions := make(map[string]struct{})
	for _, leaseUUID := range sweep.sealed.LeaseUUIDs(
		sweep.coordinator.projector.collector.Binding(),
	) {
		if sweep.operations.WasInFlight(leaseUUID) {
			causalExclusions[leaseUUID] = struct{}{}
		}
	}
	retentionPositives := sweep.incompleteRetentionPositives(projection)
	result, err := sweep.coordinator.projector.project(
		sweep.fence,
		inventoryProjection{
			Placements:         maps.Clone(projection.Placements),
			Conflicts:          cloneStringSlices(projection.Conflicts),
			UntrustedPositives: cloneStringSlices(projection.UntrustedPositives),
			retentionPositives: retentionPositives,
			AbsenceEvidence:    sweep.sealed,
			causalExclusions:   causalExclusions,
		},
	)
	if err != nil {
		for _, leaseUUID := range sweep.sealed.LeaseUUIDs(
			sweep.coordinator.projector.collector.Binding(),
		) {
			sweep.coordinator.rememberExcludedObservation(sweep.sealed, leaseUUID)
		}
		return nil, err
	}
	unresolved := maps.Clone(result.unresolvedPositives)
	for leaseUUID := range unresolved {
		sweep.coordinator.rememberExcludedObservation(sweep.sealed, leaseUUID)
	}
	sweep.coordinator.resolveFreshExcludedObservations(sweep.sealed, unresolved)
	identities := sweep.sealed.StorageIdentities(
		sweep.coordinator.projector.collector.Binding(),
	)
	return &ProjectedReconciliationSweep{
		sweep:    sweep,
		marker:   &projectedReconciliationSweepMarker{},
		result:   result,
		records:  sweep.coordinator.coordinator.store.List(),
		baseline: sweep.coordinator.coordinator.store.CurrentAdmissionBaseline(),
		eligible: func() map[string]struct{} {
			eligible := make(map[string]struct{}, len(identities))
			for backendName := range identities {
				eligible[backendName] = struct{}{}
			}
			return eligible
		}(),
		complete: sweep.coordinator.projector.complete(sweep.sealed),
	}, nil
}

// incompleteRetentionPositives derives the conservative retention class from
// the sealed aggregate. Callers choose reconciliation policy for authoritative
// provision rows, but cannot omit a retention positive or promote it to an
// owner merely because a peer backend failed to answer this sweep.
func (sweep *ReconciliationSweep) incompleteRetentionPositives(
	projection ReconciliationProjection,
) map[string][]string {
	binding := sweep.coordinator.projector.collector.Binding()
	if sweep.sealed.Complete(binding) {
		return nil
	}
	result := make(map[string][]string)
	for _, leaseUUID := range sweep.sealed.LeaseUUIDs(binding) {
		reporters := sweep.sealed.RetentionReporters(binding, leaseUUID)
		if len(reporters) == 0 {
			continue
		}
		if owner := projection.Placements[leaseUUID]; owner != "" &&
			len(reporters) == 1 && reporters[0] == owner {
			// An authoritative provision row already accounts for the same
			// backend. The retention adds no distinct conservative candidate.
			continue
		}
		if _, conflicted := projection.Conflicts[leaseUUID]; conflicted {
			// Aggregate validation below proves that the conflict contains every
			// reporter, so no second classification is needed.
			continue
		}
		result[leaseUUID] = reporters
	}
	return result
}

func cloneStringSlices(input map[string][]string) map[string][]string {
	result := make(map[string][]string, len(input))
	for key, values := range input {
		result[key] = slices.Clone(values)
	}
	return result
}

func (projected *ProjectedReconciliationSweep) Valid() bool {
	if projected == nil || projected.sweep == nil || projected.marker == nil {
		return false
	}
	sweep := projected.sweep
	sweep.mu.Lock()
	valid := sweep.validLocked() && sweep.sealed.Present() &&
		sweep.sealed.ValidFor(sweep.coordinator.projector.collector.Binding())
	fence := sweep.fence
	store := sweep.coordinator.coordinator.store
	sweep.mu.Unlock()
	return valid && store.inventoryFenceCurrent(fence)
}

func (projected *ProjectedReconciliationSweep) Complete() bool {
	return projected.Valid() && projected.complete
}

func (projected *ProjectedReconciliationSweep) WasInFlight(leaseUUID string) bool {
	return projected.Valid() && projected.sweep.operations.WasInFlight(leaseUUID)
}

// OperationActiveNow closes the late-start race without exposing the Registry.
// The result can only make cleanup more conservative.
func (projected *ProjectedReconciliationSweep) OperationActiveNow(leaseUUID string) bool {
	return projected.Valid() &&
		projected.sweep.coordinator.coordinator.operations.Contains(leaseUUID)
}

func (projected *ProjectedReconciliationSweep) Record(leaseUUID string) Placement {
	if !projected.Valid() {
		return Placement{}
	}
	return clonePlacement(projected.records[leaseUUID])
}

func (projected *ProjectedReconciliationSweep) Records() map[string]Placement {
	if !projected.Valid() {
		return nil
	}
	return clonePlacements(projected.records)
}

func (projected *ProjectedReconciliationSweep) AdmissionBaseline() AdmissionBaseline {
	if !projected.Valid() {
		return AdmissionBaseline{}
	}
	return projected.baseline
}

func (projected *ProjectedReconciliationSweep) EligibleBackends() []string {
	if !projected.Valid() {
		return nil
	}
	return slices.Sorted(maps.Keys(projected.eligible))
}

func (projected *ProjectedReconciliationSweep) fenced(leaseUUID string) bool {
	if !projected.Valid() {
		return true
	}
	_, fenced := projected.result.Fenced[leaseUUID]
	return fenced
}

func (projected *ProjectedReconciliationSweep) HasPruneAbsence(leaseUUID string) bool {
	if !projected.Valid() {
		return false
	}
	_, present := projected.result.pruneAbsence(leaseUUID)
	return present
}

// PruneTerminalAbsence joins this sweep's private absence proof and operation
// boundary to the chain reader permanently bound at coordinator construction.
// Callers select only a lease key to inspect; they cannot supply a proof,
// claim, reader, record revision, or terminal outcome.
func (projected *ProjectedReconciliationSweep) PruneTerminalAbsence(
	ctx context.Context,
	leaseUUID string,
) PruneResult {
	if !projected.Valid() || ctx == nil || leaseUUID == "" {
		return PruneResult{disposition: PruneDispositionInvalid, err: ErrInvalidRecordRevision}
	}
	authority := projected.sweep.coordinator
	pruner := authority.coordinator.terminalPruner
	if pruner == nil || !pruner.Valid() || projected.sweep.operations.WasInFlight(leaseUUID) {
		return PruneResult{disposition: PruneDispositionEvidenceStale}
	}
	proof, absent := projected.result.pruneAbsence(leaseUUID)
	if !absent {
		return PruneResult{disposition: PruneDispositionEvidenceStale}
	}
	claimResult := authority.coordinator.operations.TryClaimReconciliationLease(
		leaseUUID, projected.sweep.operations,
	)
	if !claimResult.Acquired() {
		return PruneResult{disposition: PruneDispositionEvidenceStale}
	}
	claim := claimResult.Claim()
	defer authority.coordinator.operations.ReleaseLease(claim)
	return pruner.PruneTerminalAbsence(ctx, proof, claim)
}

// RecoverAttempt owns all process-local claims for one exact attempt observed
// in this projected sweep. Restore source ordering and claim release are
// internal, so callers cannot splice a source from another operation or reuse
// the raw claims after recovery.
func (projected *ProjectedReconciliationSweep) RecoverAttempt(
	ctx context.Context,
	leaseUUID string,
) AttemptRecoveryResult {
	if !projected.Valid() || ctx == nil || leaseUUID == "" {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}
	authority := projected.sweep.coordinator
	recovery := authority.coordinator.attemptRecovery
	if recovery == nil || !recovery.Valid() {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}
	record := projected.records[leaseUUID]
	metadata := record.AttemptMetadata()
	if record.Attempt == "" || !metadata.Valid() || record.State() == StateUnusable || record.Conflict {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}
	leaseUUIDs := []string{leaseUUID}
	sourceLeaseUUID := metadata.RestoreSourceLeaseUUID()
	if sourceLeaseUUID != "" {
		if sourceLeaseUUID == leaseUUID {
			return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
		}
		leaseUUIDs = append(leaseUUIDs, sourceLeaseUUID)
	}
	slices.Sort(leaseUUIDs)
	claims := make(map[string]operation.LeaseClaim, len(leaseUUIDs))
	defer func() {
		for _, claim := range claims {
			authority.coordinator.operations.ReleaseLease(claim)
		}
	}()
	for _, currentLeaseUUID := range leaseUUIDs {
		claimResult := authority.coordinator.operations.TryClaimReconciliationLease(
			currentLeaseUUID, projected.sweep.operations,
		)
		if !claimResult.Acquired() {
			return AttemptRecoveryResult{
				outcome: attemptRecoveryPreserved,
				err:     errors.New("attempt recovery crossed the operation boundary"),
			}
		}
		claims[currentLeaseUUID] = claimResult.Claim()
	}
	return recovery.Recover(
		ctx, record.RecordRevision(), claims[leaseUUID], claims[sourceLeaseUUID],
	)
}

// ReconciliationObservationDisposition reports why no live action capability
// was minted. It is detached output and cannot select any transition.
type ReconciliationObservationDisposition uint8

const (
	ReconciliationObservationInvalid ReconciliationObservationDisposition = iota
	ReconciliationObservationStale
	ReconciliationObservationChainError
	ReconciliationObservationChainChanged
	ReconciliationObservationChainLive
	ReconciliationObservationChainUnknownState
	ReconciliationObservationReady
)

// ObservedReconciliationAction binds one exact post-projection record, exact
// live chain lease, and exact Registry lease claim. Backend action methods can
// derive their lease and owner from this value instead of accepting raw target
// strings. The zero value is invalid.
type ObservedReconciliationAction struct {
	projected *ProjectedReconciliationSweep
	claim     operation.LeaseClaim
	lease     billingtypes.Lease
	record    Placement
}

func (action ObservedReconciliationAction) validFor(
	authority *ReconciliationCoordinator,
) bool {
	return action.ownedBy(authority) && action.projected.Valid()
}

func (action ObservedReconciliationAction) ownedBy(
	authority *ReconciliationCoordinator,
) bool {
	return action.projected != nil && action.projected.sweep != nil &&
		action.projected.marker != nil &&
		action.projected.sweep.coordinator == authority && action.claim.Valid() &&
		action.lease.Uuid != "" && action.record.Revision() ==
		action.projected.records[action.lease.Uuid].Revision()
}

func (action ObservedReconciliationAction) Valid() bool {
	return action.projected != nil &&
		action.validFor(action.projected.sweep.coordinator)
}

func (action ObservedReconciliationAction) Lease() billingtypes.Lease {
	if !action.Valid() {
		return billingtypes.Lease{}
	}
	return cloneReconciliationLease(&action.lease)
}

func (action ObservedReconciliationAction) Placement() Placement {
	if !action.Valid() {
		return Placement{}
	}
	return clonePlacement(action.record)
}

// ObserveLiveAction performs the final exact chain read while holding the
// Registry claim derived from this sweep's private operation boundary. Any
// Store change, operation crossing, foreign provider, nil lease, or terminal
// state returns no capability and releases the claim.
func (projected *ProjectedReconciliationSweep) ObserveLiveAction(
	ctx context.Context,
	leaseUUID string,
) (ObservedReconciliationAction, ReconciliationObservationDisposition, error) {
	if !projected.Valid() || ctx == nil || leaseUUID == "" {
		return ObservedReconciliationAction{}, ReconciliationObservationInvalid,
			errors.New("invalid reconciliation observation")
	}
	if projected.sweep.operations.WasInFlight(leaseUUID) {
		return ObservedReconciliationAction{}, ReconciliationObservationStale, nil
	}
	authority := projected.sweep.coordinator
	if err := authority.coordinator.store.leaseSideEffectError(leaseUUID); err != nil {
		return ObservedReconciliationAction{}, ReconciliationObservationStale, nil
	}
	expected := projected.records[leaseUUID]
	current := authority.coordinator.store.Lookup(leaseUUID)
	if current.Revision() != expected.Revision() || current.State() != expected.State() ||
		current.Backend != expected.Backend || current.Attempt != expected.Attempt {
		return ObservedReconciliationAction{}, ReconciliationObservationStale, nil
	}
	claimResult := authority.coordinator.operations.TryClaimReconciliationLease(
		leaseUUID, projected.sweep.operations,
	)
	if !claimResult.Acquired() {
		return ObservedReconciliationAction{}, ReconciliationObservationStale, nil
	}
	claim := claimResult.Claim()
	release := true
	defer func() {
		if release {
			authority.coordinator.operations.ReleaseLease(claim)
		}
	}()
	queryCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	defer cancel()
	observation := authority.controlPlane.observeLease(queryCtx, leaseUUID, "")
	lease, exact := exactLeaseFromObservation(observation)
	if !exact {
		if unknown, ok := observation.(observedLeaseUnknown); ok {
			return ObservedReconciliationAction{}, ReconciliationObservationChainError, unknown.err
		}
		return ObservedReconciliationAction{}, ReconciliationObservationChainChanged, nil
	}
	if lease.State != billingtypes.LEASE_STATE_PENDING &&
		lease.State != billingtypes.LEASE_STATE_ACTIVE {
		return ObservedReconciliationAction{}, ReconciliationObservationChainChanged, nil
	}
	action := ObservedReconciliationAction{
		projected: projected,
		claim:     claim,
		lease:     cloneProviderLease(&lease),
		record:    expected,
	}
	release = false
	return action, ReconciliationObservationReady, nil
}

// ReleaseAction consumes no mutation authority; it only ends the exact
// process-local exclusion held by a successful ObserveLiveAction.
func (authority *ReconciliationCoordinator) ReleaseAction(
	action ObservedReconciliationAction,
) bool {
	if !authority.Valid() || !action.ownedBy(authority) {
		return false
	}
	return authority.coordinator.operations.ReleaseLease(action.claim)
}

// ReconciliationProvisionResult is detached output from Provision. The route
// and dispatch classification are observational; it carries neither the
// Registry claim nor Store attempt token used to produce them.
type ReconciliationProvisionResult struct {
	backendName string
	dispatch    DispatchResult
	err         error
}

func (result ReconciliationProvisionResult) BackendName() string      { return result.backendName }
func (result ReconciliationProvisionResult) Dispatch() DispatchResult { return result.dispatch }
func (result ReconciliationProvisionResult) Err() error               { return result.err }

// Provision owns the complete claim -> route -> initiation -> durable attempt
// -> callback route -> backend call -> settlement protocol for one observed
// lease. The caller supplies only payload bytes and their typed fingerprint;
// lease identity, provider, tenant, items, eligible topology, owner, callback
// origin, operation ID, and backend target are all derived from action's exact
// sweep capability.
func (authority *ReconciliationCoordinator) Provision(
	ctx context.Context,
	action ObservedReconciliationAction,
	payload []byte,
	fingerprint PayloadFingerprint,
) ReconciliationProvisionResult {
	invalid := func(err error) ReconciliationProvisionResult {
		return ReconciliationProvisionResult{err: err}
	}
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		!authority.coordinator.operations.HoldsLeaseClaim(action.claim, action.lease.Uuid) {
		return invalid(ErrReconciliationBoundaryStale)
	}
	if fingerprint.Valid() {
		if payload == nil || sha256.Sum256(payload) != fingerprint.sha256 {
			return invalid(errors.New("reconciliation payload differs from typed fingerprint"))
		}
	} else if payload != nil {
		return invalid(errors.New("payload bytes require a typed fingerprint"))
	}
	lease := action.lease
	if len(lease.Items) == 0 || lease.Items[0].SkuUuid == "" {
		return invalid(errors.New("reconciliation lease has no routable SKU"))
	}
	projected := action.projected
	record := action.record
	baseline := projected.baseline
	if !baseline.Valid() {
		return invalid(ErrInvalidAdmissionScope)
	}
	var owned RecordRevision
	eligible := maps.Clone(projected.eligible)
	if record.State() == StateConfirmed {
		owned = record.RecordRevision()
	}
	allowRecordless := projected.complete ||
		(lease.State == billingtypes.LEASE_STATE_PENDING && len(eligible) > 0)
	if !owned.Valid() && !allowRecordless {
		return invalid(ErrBackendOutsideAdmissionScope)
	}
	var scope AdmissionScope
	var err error
	if !owned.Valid() {
		scope, err = authority.coordinator.store.scopeAdmission(
			baseline, slices.Sorted(maps.Keys(eligible)),
		)
		if err != nil {
			return invalid(err)
		}
	}
	route, err := authority.routeProvision(
		ctx, lease.Uuid, lease.Items[0].SkuUuid, owned, eligible,
		authority.coordinator.operations.CountsByBackend(),
	)
	if err != nil {
		return invalid(err)
	}
	if !route.valid() {
		return invalid(errors.New("no backend available"))
	}
	if !owned.Valid() && !scope.Allows(route.backend()) {
		return invalid(fmt.Errorf(
			"%w: router selected %q outside the scoped healthy set",
			ErrBackendOutsideAdmissionScope, route.backend(),
		))
	}
	items := leaseitems.FromLease(&lease)
	provisionInitiation, err := operation.NewProvisionInitiation(
		lease.Uuid, lease.Tenant, items, route.backend(),
	)
	if err != nil {
		return invalid(err)
	}
	initiated := authority.coordinator.operations.TryInitiateProvisionClaimed(
		action.claim, provisionInitiation,
	)
	if !initiated.Started() {
		if initiated.Outcome() == operation.TrackInvalid {
			return invalid(ErrReconciliationBoundaryStale)
		}
		return invalid(ErrReconciliationOperationBusy)
	}
	initiation := initiated.Capability()
	abort := func(err error) ReconciliationProvisionResult {
		authority.coordinator.operations.AbortInitiation(initiation)
		return invalid(err)
	}
	request, err := authority.coordinator.store.MintBackendRequestSnapshot(
		lease.Tenant, items,
	)
	if err != nil {
		return abort(err)
	}
	var attempt AttemptToken
	var applied bool
	callbacks, err := authority.coordinator.store.callbackRoutes.ForOperation(initiation.ID())
	if err != nil {
		return abort(err)
	}
	if owned.Valid() {
		attempt, applied, err = authority.beginOwnedAttemptForRoute(
			baseline, owned, route, initiation.ID(), fingerprint, request, callbacks,
		)
	} else {
		attempt, applied, err = authority.beginNewAttemptForRoute(
			scope, lease.Uuid, route, initiation.ID(), fingerprint, request, callbacks,
		)
	}
	if err != nil {
		return abort(err)
	}
	if !applied {
		return abort(ErrReconciliationBoundaryStale)
	}
	dispatch, err := authority.coordinator.joinProvisionDispatch(initiation, attempt)
	if err != nil {
		return invalid(err)
	}
	return ReconciliationProvisionResult{
		backendName: route.backend(),
		dispatch:    authority.executeProvision(ctx, dispatch, payload),
	}
}

// ReconcileObservedCustomDomain derives the exact backend, lease, and items
// from one observed capability. A caller cannot apply one lease's chain items
// to another backend record.
func (authority *ReconciliationCoordinator) ReconcileObservedCustomDomain(
	ctx context.Context,
	action ObservedReconciliationAction,
) error {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		!authority.coordinator.operations.HoldsLeaseClaim(action.claim, action.lease.Uuid) ||
		action.record.State() != StateConfirmed {
		return errors.New("invalid observed custom-domain reconciliation action")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.lease.Uuid); err != nil {
		return err
	}
	return authority.reconcileCustomDomain(
		ctx, action.record.Backend, action.lease.Uuid, leaseitems.FromLease(&action.lease),
	)
}

// DeprovisionObserved derives the exact confirmed owner and lease from one
// observed capability. It cannot target an arbitrary backend name or UUID.
func (authority *ReconciliationCoordinator) DeprovisionObserved(
	ctx context.Context,
	action ObservedReconciliationAction,
) error {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		!authority.coordinator.operations.HoldsLeaseClaim(action.claim, action.lease.Uuid) ||
		action.record.State() != StateConfirmed {
		return errors.New("invalid observed deprovision action")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.lease.Uuid); err != nil {
		return err
	}
	return authority.deprovisionExact(ctx, action.record.Backend, action.lease.Uuid)
}

// ObservedOrphanAction binds one provision row from this sweep to a positive
// terminal chain reread and the exact Registry claim. The backend target is
// derived from the sealed inventory snapshot, never supplied to execution.
type ObservedOrphanAction struct {
	projected   *ProjectedReconciliationSweep
	claim       operation.LeaseClaim
	leaseUUID   string
	backendName string
}

func (action ObservedOrphanAction) validFor(authority *ReconciliationCoordinator) bool {
	return action.ownedBy(authority) && action.projected.Valid()
}

func (action ObservedOrphanAction) ownedBy(authority *ReconciliationCoordinator) bool {
	return action.projected != nil && action.projected.sweep != nil &&
		action.projected.marker != nil &&
		action.projected.sweep.coordinator == authority && action.claim.Valid() &&
		action.leaseUUID != "" && action.backendName != ""
}

func (action ObservedOrphanAction) Valid() bool {
	return action.projected != nil && action.validFor(action.projected.sweep.coordinator)
}

func (action ObservedOrphanAction) BackendName() string {
	if !action.Valid() {
		return ""
	}
	return action.backendName
}

// ObserveTerminalOrphan returns an executable orphan capability only when one
// exact provision endpoint in the sealed sweep reported the lease and the
// construction-bound chain reader positively reports the same provider's
// lease terminal while the Registry claim is held.
func (projected *ProjectedReconciliationSweep) ObserveTerminalOrphan(
	ctx context.Context,
	leaseUUID string,
) (ObservedOrphanAction, ReconciliationObservationDisposition, error) {
	if !projected.Valid() || ctx == nil || leaseUUID == "" {
		return ObservedOrphanAction{}, ReconciliationObservationInvalid,
			errors.New("invalid orphan observation")
	}
	if projected.sweep.operations.WasInFlight(leaseUUID) {
		return ObservedOrphanAction{}, ReconciliationObservationStale, nil
	}
	authority := projected.sweep.coordinator
	if err := authority.coordinator.store.leaseSideEffectError(leaseUUID); err != nil {
		return ObservedOrphanAction{}, ReconciliationObservationStale, nil
	}
	binding := authority.projector.collector.Binding()
	var observed inventory.ProvisionObservation
	reporters := 0
	for _, backendName := range projected.EligibleBackends() {
		candidate, present := projected.sweep.sealed.Provision(
			binding, backendName, leaseUUID,
		)
		if !present {
			continue
		}
		observed = candidate
		reporters++
	}
	if reporters != 1 || observed.BackendName() == "" ||
		(observed.ProviderUUID() != "" &&
			observed.ProviderUUID() != authority.coordinator.store.providerUUID) {
		return ObservedOrphanAction{}, ReconciliationObservationStale, nil
	}
	claimResult := authority.coordinator.operations.TryClaimReconciliationLease(
		leaseUUID, projected.sweep.operations,
	)
	if !claimResult.Acquired() {
		return ObservedOrphanAction{}, ReconciliationObservationStale, nil
	}
	claim := claimResult.Claim()
	release := true
	defer func() {
		if release {
			authority.coordinator.operations.ReleaseLease(claim)
		}
	}()
	queryCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	defer cancel()
	observation := authority.controlPlane.observeLease(queryCtx, leaseUUID, "")
	lease, exact := exactLeaseFromObservation(observation)
	if !exact {
		if unknown, ok := observation.(observedLeaseUnknown); ok {
			return ObservedOrphanAction{}, ReconciliationObservationChainError, unknown.err
		}
		return ObservedOrphanAction{}, ReconciliationObservationChainChanged, nil
	}
	switch lease.State {
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED:
	case billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE:
		return ObservedOrphanAction{}, ReconciliationObservationChainLive, nil
	default:
		return ObservedOrphanAction{}, ReconciliationObservationChainUnknownState, nil
	}
	action := ObservedOrphanAction{
		projected:   projected,
		claim:       claim,
		leaseUUID:   leaseUUID,
		backendName: observed.BackendName(),
	}
	release = false
	return action, ReconciliationObservationReady, nil
}

// DeprovisionOrphan derives the exact backend target from the opaque orphan
// observation and releases no authority to the caller.
func (authority *ReconciliationCoordinator) DeprovisionOrphan(
	ctx context.Context,
	action ObservedOrphanAction,
) error {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		!authority.coordinator.operations.HoldsLeaseClaim(action.claim, action.leaseUUID) {
		return errors.New("invalid observed orphan deprovision action")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.leaseUUID); err != nil {
		return err
	}
	return authority.deprovisionExact(ctx, action.backendName, action.leaseUUID)
}

func (authority *ReconciliationCoordinator) ReleaseOrphanAction(
	action ObservedOrphanAction,
) bool {
	if !authority.Valid() || !action.ownedBy(authority) {
		return false
	}
	return authority.coordinator.operations.ReleaseLease(action.claim)
}
