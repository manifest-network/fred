package provisioner

import (
	"context"
	"errors"
	"fmt"
	"slices"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// reconcileInventory is an immutable collection result. Collection reports
// which backend inventories answered; it does not decide whether those facts
// are authoritative or mutate placement state.
type reconcileInventory struct {
	chainLeases map[string]billingtypes.Lease
	pending     int
	active      int

	fleet                       fleetSnapshot
	retentions                  map[string]string
	retentionsAnswered          answeredSet
	retentionsReportedByBackend map[string]map[string]struct{}
	retentionStorageIdentities  map[string]backendidentity.ID
	retentionCollected          map[string]placement.BackendRetentionInventory
	backendStorageIdentities    map[string]backendidentity.ID

	// untrustedPositiveObservations retains the conservative fact that a
	// rejected backend reported a lease, without treating that backend's payload
	// as placement authority. Dropping the fact entirely would turn a malformed
	// or identity-mismatched positive into apparent absence and could authorize a
	// second provision on a healthy sibling during a degraded sweep.
	untrustedPositiveObservations map[string]map[string]struct{}
}

// collectInventory performs backend reads only through the exact typed sweep.
// Each successful endpoint read returns an opaque one-shot receipt after its
// positives are fenced. Paired receipts are classified centrally and every
// unmatched half is consumed as untrusted before sealing, so policy code cannot
// manufacture endpoint or negative authority. No observation becomes lifecycle
// authority until the placement store accepts the semantic projection.
func (r *Reconciler) collectInventory(
	ctx context.Context,
	sweep *placement.ReconciliationSweep,
	pendingLeases []billingtypes.Lease,
	activeLeases []billingtypes.Lease,
) (reconcileInventory, error) {
	if r.coordinator == nil || !r.coordinator.Valid() {
		return reconcileInventory{}, errors.New("inventory projector is not bound")
	}
	if sweep == nil || !sweep.Valid() {
		return reconcileInventory{}, errors.New("reconciliation sweep is not bound")
	}
	chainLeases := make(map[string]billingtypes.Lease, len(pendingLeases)+len(activeLeases))
	for _, lease := range pendingLeases {
		chainLeases[lease.Uuid] = lease
	}
	for _, lease := range activeLeases {
		chainLeases[lease.Uuid] = lease
	}

	inventory := reconcileInventory{
		chainLeases: chainLeases,
		pending:     len(pendingLeases),
		active:      len(activeLeases),
		fleet:       r.fetchFleetSnapshot(ctx, sweep),
	}
	inventory.retentions,
		inventory.retentionsAnswered,
		inventory.retentionsReportedByBackend,
		inventory.retentionStorageIdentities,
		inventory.retentionCollected = r.fetchAllRetentions(ctx, sweep)
	configuredBackends, err := r.coordinator.BackendNames()
	if err != nil {
		return reconcileInventory{}, fmt.Errorf("enumerate configured backends: %w", err)
	}
	inventory.backendStorageIdentities = make(map[string]backendidentity.ID, len(configuredBackends))
	for _, backendName := range configuredBackends {
		provisionResponse, provisionAnswered := inventory.fleet.collectedByBackend[backendName]
		retentionResponse, retentionAnswered := inventory.retentionCollected[backendName]
		var result placement.BackendInventoryResult
		switch {
		case provisionAnswered && retentionAnswered:
			result, err = sweep.RecordBackendInventory(
				provisionResponse, retentionResponse,
			)
		case provisionAnswered:
			err = sweep.RejectProvisionInventory(provisionResponse)
		case retentionAnswered:
			err = sweep.RejectRetentionInventory(retentionResponse)
		}
		if err != nil {
			return reconcileInventory{}, fmt.Errorf(
				"dispose backend inventory evidence for %q: %w", backendName, err,
			)
		}
		switch result.Disposition() {
		case placement.BackendInventoryAuthoritative, placement.BackendInventoryPartial:
			inventory.backendStorageIdentities[backendName] = provisionResponse.StorageID()
			for _, leaseUUID := range result.UntrustedLeaseUUIDs() {
				inventory.rejectLease(backendName, leaseUUID)
			}
			continue
		}
		inventory.rejectBackend(backendName)
	}
	if err := sweep.SealInventory(); err != nil {
		return reconcileInventory{}, fmt.Errorf("seal inventory evidence: %w", err)
	}
	return inventory, nil
}

// rejectLease applies the collector's exclusive conservative-membership arm.
// Raw reporter membership survives, so peer collisions cannot disappear when
// the selected payload is removed from the union.
func (inventory *reconcileInventory) rejectLease(backendName, leaseUUID string) {
	if leaseUUID == "" {
		return
	}
	if inventory.untrustedPositiveObservations == nil {
		inventory.untrustedPositiveObservations = make(map[string]map[string]struct{})
	}
	backends := inventory.untrustedPositiveObservations[leaseUUID]
	if backends == nil {
		backends = make(map[string]struct{})
		inventory.untrustedPositiveObservations[leaseUUID] = backends
	}
	backends[backendName] = struct{}{}
	if rows, exists := inventory.fleet.provisionsByBackend[backendName]; exists {
		inventory.fleet.provisionsByBackend[backendName] = slices.DeleteFunc(rows, func(row backend.ProvisionInfo) bool {
			return row.LeaseUUID == leaseUUID
		})
	}
	if inventory.fleet.provisions[leaseUUID].BackendName == backendName {
		delete(inventory.fleet.provisions, leaseUUID)
	}
	if inventory.retentions[leaseUUID] == backendName {
		delete(inventory.retentions, leaseUUID)
	}
}

func (inventory *reconcileInventory) rejectBackend(backendName string) {
	for leaseUUID := range inventory.fleet.reportedByBackend[backendName] {
		inventory.rejectLease(backendName, leaseUUID)
	}
	for leaseUUID := range inventory.retentionsReportedByBackend[backendName] {
		inventory.rejectLease(backendName, leaseUUID)
	}
	inventory.fleet.markUnanswered(backendName)
	inventory.retentionsAnswered[backendName] = false
	delete(inventory.fleet.storageIdentities, backendName)
	delete(inventory.fleet.provisionsByBackend, backendName)
	delete(inventory.retentionStorageIdentities, backendName)
	delete(inventory.backendStorageIdentities, backendName)
}

// collectChainLeaseInventory bounds each complete paginated state inventory
// independently. Both reads are attempted while the parent remains live so a
// timeout or transport failure in one cannot consume the other's deadline.
// Partial chain state is never returned as reconciliation authority.
func (r *Reconciler) collectChainLeaseInventory(
	ctx context.Context,
) (pending, active []billingtypes.Lease, err error) {
	budget := r.chainInventoryBudget
	if budget <= 0 {
		budget = chainInventoryTimeout
	}

	return r.coordinator.CollectChainInventory(ctx, budget)
}
