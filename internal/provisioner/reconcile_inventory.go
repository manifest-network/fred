package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backendidentity"
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
	backendStorageIdentities    map[string]backendidentity.ID

	// untrustedPositiveObservations retains the conservative fact that a
	// rejected backend reported a lease, without treating that backend's payload
	// as placement authority. Dropping the fact entirely would turn a malformed
	// or identity-mismatched positive into apparent absence and could authorize a
	// second provision on a healthy sibling during a degraded sweep.
	untrustedPositiveObservations map[string]map[string]struct{}
}

func (inventory reconcileInventory) complete() bool {
	if !inventory.fleet.complete ||
		(inventory.retentionsAnswered != nil && !inventory.retentionsAnswered.complete()) ||
		len(inventory.backendStorageIdentities) != len(inventory.fleet.answered) {
		return false
	}
	for backendName := range inventory.fleet.answered {
		if !inventory.backendStorageIdentities[backendName].Valid() {
			return false
		}
	}
	return true
}

// emptyBackendNames returns raw, pre-projection drain evidence. Causal
// filtering in projectPlacementInventory may hide an in-flight lease from the
// durable placement projection, so backend emptiness must be computed from the
// complete provision+retention responses themselves.
func (inventory reconcileInventory) emptyBackendNames() []string {
	if !inventory.complete() {
		return nil
	}
	empty := make([]string, 0, len(inventory.backendStorageIdentities))
	for backendName := range inventory.backendStorageIdentities {
		if len(inventory.fleet.reportedByBackend[backendName]) == 0 &&
			len(inventory.retentionsReportedByBackend[backendName]) == 0 {
			empty = append(empty, backendName)
		}
	}
	slices.Sort(empty)
	return empty
}

// withoutConflictCandidates removes backends whose raw empty response is not
// causally sufficient drain evidence. Most conflicts are formed from two
// positive reporters, which cannot be empty. A contradiction against a durable
// owner or attempt is different: that candidate may have answered empty before
// its delayed remote mutation committed. Persisting it as both empty and a
// conflict candidate would either make the projection self-contradictory or,
// worse, authorize topology removal from an observation ordered before the
// outstanding side effect.
func withoutConflictCandidates(
	emptyBackends []string,
	quarantines ...map[string][]string,
) []string {
	if emptyBackends == nil || len(quarantines) == 0 {
		return emptyBackends
	}
	candidates := make(map[string]struct{})
	for _, quarantine := range quarantines {
		for _, backendNames := range quarantine {
			for _, backendName := range backendNames {
				candidates[backendName] = struct{}{}
			}
		}
	}
	filtered := make([]string, 0, len(emptyBackends))
	for _, backendName := range emptyBackends {
		if _, conflicted := candidates[backendName]; !conflicted {
			filtered = append(filtered, backendName)
		}
	}
	return filtered
}

// collectInventory performs only external reads. Keeping it separate from the
// projector makes the durability boundary visible: no collected observation
// becomes lifecycle authority until the placement store accepts the complete
// projection.
func (r *Reconciler) collectInventory(ctx context.Context) (reconcileInventory, error) {
	pendingLeases, activeLeases, err := r.collectChainLeaseInventory(ctx)
	if err != nil {
		return reconcileInventory{}, err
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
		fleet:       r.fetchFleetSnapshot(ctx),
	}
	inventory.retentions,
		inventory.retentionsAnswered,
		inventory.retentionsReportedByBackend,
		inventory.retentionStorageIdentities = r.fetchAllRetentions(ctx)
	inventory.rejectCrossEndpointDuplicates()
	inventory.reconcileStorageIdentities(r.placementAuthority)
	return inventory, nil
}

// reconcileStorageIdentities admits a backend's evidence only when both
// independent inventory endpoints report the same canonical physical storage
// identity and it matches any durable historical pin. A replacement node is
// therefore one unanswered backend; it cannot pause healthy siblings after a
// baseline already exists.
func (inventory *reconcileInventory) reconcileStorageIdentities(
	resolver interface {
		ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool)
	},
) {
	if inventory == nil {
		return
	}
	inventory.backendStorageIdentities = make(map[string]backendidentity.ID)
	for backendName, provisionAnswered := range inventory.fleet.answered {
		provisionID := inventory.fleet.storageIdentities[backendName]
		retentionID := inventory.retentionStorageIdentities[backendName]
		retentionAnswered := inventory.retentionsAnswered.heard(backendName)
		if !retentionAnswered || !provisionID.Valid() ||
			!retentionID.Valid() || provisionID != retentionID {
			inventory.rejectBackend(backendName)
			if provisionAnswered || retentionAnswered {
				slog.Error("backend inventory storage identity is missing or inconsistent; ignoring both endpoints",
					"backend", backendName,
					"provision_storage_id", provisionID,
					"retention_storage_id", retentionID,
				)
			}
			continue
		}
		if expected, bound := resolver.ExpectedBackendStorageIdentity(backendName); bound && expected != provisionID {
			inventory.rejectBackend(backendName)
			slog.Error("backend storage identity differs from durable placement binding; ignoring backend",
				"backend", backendName,
				"observed_storage_id", provisionID,
				"expected_storage_id", expected,
			)
			continue
		}
		if !provisionAnswered {
			// RefreshState failed, but the stale positive response still carried
			// the same physical identity as the independent retention endpoint.
			// Preserve that conservative affinity without admitting this backend
			// into the complete snapshot or allowing its absence to become
			// authority.
			continue
		}
		inventory.backendStorageIdentities[backendName] = provisionID
	}
}

// rejectCrossEndpointDuplicates treats a backend that reports one lease as
// both active and retained in the same sweep as unanswered on both endpoints.
// The HTTP client already rejects malformed/duplicate identities within each
// complete paginated response; this cross-endpoint check closes the remaining
// gap. Its payload cannot become placement authority, but each positive lease
// identity remains conservative exclusion evidence. The complete flag cannot
// establish or advance the admission baseline.
func (inventory *reconcileInventory) rejectCrossEndpointDuplicates() {
	if inventory == nil {
		return
	}
	for backendName, provisions := range inventory.fleet.reportedByBackend {
		retentions := inventory.retentionsReportedByBackend[backendName]
		if len(provisions) == 0 || len(retentions) == 0 {
			continue
		}
		var duplicates []string
		for leaseUUID := range provisions {
			if _, duplicate := retentions[leaseUUID]; duplicate {
				duplicates = append(duplicates, leaseUUID)
			}
		}
		if len(duplicates) == 0 {
			continue
		}
		slices.Sort(duplicates)
		slog.Error("backend returned contradictory provision and retention inventory; ignoring both endpoints",
			"backend", backendName,
			"duplicate_lease_uuids", duplicates,
		)
		inventory.rejectBackend(backendName)
	}
}

func (inventory *reconcileInventory) rejectBackend(backendName string) {
	if inventory.untrustedPositiveObservations == nil {
		inventory.untrustedPositiveObservations = make(map[string]map[string]struct{})
	}
	recordUntrusted := func(leaseUUID string) {
		if leaseUUID == "" {
			return
		}
		backends := inventory.untrustedPositiveObservations[leaseUUID]
		if backends == nil {
			backends = make(map[string]struct{})
			inventory.untrustedPositiveObservations[leaseUUID] = backends
		}
		backends[backendName] = struct{}{}
	}
	for leaseUUID := range inventory.fleet.reportedByBackend[backendName] {
		recordUntrusted(leaseUUID)
	}
	for leaseUUID := range inventory.retentionsReportedByBackend[backendName] {
		recordUntrusted(leaseUUID)
	}

	inventory.fleet.markUnanswered(backendName)
	inventory.retentionsAnswered[backendName] = false
	delete(inventory.fleet.storageIdentities, backendName)
	delete(inventory.retentionStorageIdentities, backendName)
	delete(inventory.backendStorageIdentities, backendName)
	for leaseUUID, provision := range inventory.fleet.provisions {
		if provision.BackendName == backendName {
			delete(inventory.fleet.provisions, leaseUUID)
		}
	}
	for leaseUUID, owner := range inventory.retentions {
		if owner == backendName {
			delete(inventory.retentions, leaseUUID)
		}
	}
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

	// Derive both deadlines before dispatch so neither paginated list consumes the
	// other's wall-clock budget. The chain client is already shared concurrently
	// by callbacks, the ack batcher, and reconciliation; its query stubs are safe
	// for parallel read RPCs.
	pendingCtx, cancelPending := context.WithTimeout(ctx, budget)
	activeCtx, cancelActive := context.WithTimeout(ctx, budget)
	var pendingErr, activeErr error
	var reads sync.WaitGroup
	reads.Go(func() {
		defer cancelPending()
		pending, pendingErr = r.chainClient.GetPendingLeases(pendingCtx, r.providerUUID)
	})
	reads.Go(func() {
		defer cancelActive()
		active, activeErr = r.chainClient.GetActiveLeasesByProvider(activeCtx, r.providerUUID)
	})
	reads.Wait()

	var inventoryErrors []error
	if pendingErr != nil {
		inventoryErrors = append(inventoryErrors,
			fmt.Errorf("failed to get pending leases: %w", pendingErr))
	}
	if activeErr != nil {
		inventoryErrors = append(inventoryErrors,
			fmt.Errorf("failed to get active leases: %w", activeErr))
	}
	if len(inventoryErrors) > 0 {
		return nil, nil, errors.Join(inventoryErrors...)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return pending, active, nil
}
