package placement

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/util"
)

type reconciliationCoordinatorMarker struct{ _ byte }

// ReconciliationCoordinator is the construction-bound authority for one
// reconciler. It closes over the exact Store/Registry pair and topology-bound
// inventory projector; callers cannot splice lifecycle claims, placement
// writes, projection evidence, or dispatch settlement from different owners.
// Its zero value is invalid.
type ReconciliationCoordinator struct {
	coordinator  *OperationCoordinator
	projector    *inventoryProjector
	issuer       *operationCoordinatorMarker
	marker       *reconciliationCoordinatorMarker
	backends     backendRuntime
	controlPlane *boundProviderControlPlane
	payloads     AttemptPayloadReader
	observe      ProvisionStartObserver

	// absenceUntrusted retains lease-local diagnostics and selective deferral for
	// an excluded positive while this coordinator remains alive. It is not the
	// crash-safety fact: the Store's durable pending-sweep marker survives restart
	// and withholds fresh side-effect capabilities until a complete projection
	// accounts for any observation that may have been lost.
	absenceMu        sync.Mutex
	absenceUntrusted map[string]map[string]struct{}
}

// ReconciliationCoordinator verifies provider and topology identity before
// minting the purpose facet. Recovery collaborators are subsequently bound
// once through BindRecovery while this facet remains inside NewReconciler.
func (execution *ExecutionCoordinator) ReconciliationCoordinator(
	payloads AttemptPayloadReader,
	observe ProvisionStartObserver,
) (*ReconciliationCoordinator, error) {
	if !execution.Valid() {
		return nil, errors.New("backend execution coordinator is required")
	}
	coordinator := execution.coordinator
	backendNames, err := backendNames(execution.backends)
	if err != nil {
		return nil, err
	}
	controlPlane, err := execution.providerControlPlane()
	if err != nil {
		return nil, errors.New("reconciliation provider control plane is required")
	}
	if util.IsNilInterface(payloads) {
		payloads = nil
	}
	if err := coordinator.store.VerifyBackendTopology(backendNames); err != nil {
		return nil, fmt.Errorf("verify reconciler backend topology: %w", err)
	}
	projector, err := coordinator.store.bindInventoryProjector(backendNames)
	if err != nil {
		return nil, fmt.Errorf("bind reconciler inventory projector: %w", err)
	}
	return &ReconciliationCoordinator{
		coordinator:      coordinator,
		projector:        projector,
		issuer:           coordinator.marker,
		marker:           &reconciliationCoordinatorMarker{},
		backends:         execution.backends,
		controlPlane:     controlPlane,
		payloads:         payloads,
		observe:          observe,
		absenceUntrusted: make(map[string]map[string]struct{}),
	}, nil
}

func (authority *ReconciliationCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil &&
		authority.coordinator.Valid() && authority.projector != nil &&
		authority.projector.matchesStore(authority.coordinator.store) &&
		authority.issuer == authority.coordinator.marker && authority.marker != nil &&
		!util.IsNilInterface(authority.backends) && authority.controlPlane != nil &&
		authority.controlPlane.validFor(authority.coordinator.execution) &&
		authority.coordinator.store.callbackRoutes != nil &&
		authority.coordinator.store.callbackRoutes.Valid()
}

// ProviderUUID returns the provider identity permanently bound to the Store.
// It is observational only; reconciliation cannot be constructed with an
// independently supplied provider identity.
func (authority *ReconciliationCoordinator) ProviderUUID() string {
	if !authority.Valid() {
		return ""
	}
	return authority.coordinator.store.providerUUID
}

func (authority *ReconciliationCoordinator) BindRecovery() (*TerminalPruner, *AttemptRecoveryCoordinator, error) {
	if !authority.Valid() {
		return nil, nil, errors.New("reconciliation coordinator is invalid")
	}
	return authority.coordinator.bindReconciliation(
		authority.controlPlane, authority.backends, authority.payloads, authority.projector,
	)
}

// AdmissionReady is a detached startup observation. Admission tokens are only
// minted inside a successful ProjectedReconciliationSweep.
func (authority *ReconciliationCoordinator) AdmissionReady() bool {
	return authority.Valid() && authority.coordinator.store.CurrentAdmissionBaseline().Valid()
}

func (authority *ReconciliationCoordinator) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	if !authority.Valid() {
		return backendidentity.ID{}, false
	}
	return authority.coordinator.store.ExpectedBackendStorageIdentity(backendName)
}

// cloneReconciliationLease detaches mutable slices and timestamps returned by
// chain adapters before they are retained in an opaque action capability.
func cloneReconciliationLease(lease *billingtypes.Lease) billingtypes.Lease {
	return cloneProviderLease(lease)
}

func (authority *ReconciliationCoordinator) rememberExcludedObservation(
	snapshot inventory.Snapshot,
	leaseUUID string,
) {
	if !authority.Valid() || leaseUUID == "" {
		return
	}
	reporters := snapshot.LeaseReporters(authority.projector.collector.Binding(), leaseUUID)
	if len(reporters) == 0 {
		return
	}
	authority.absenceMu.Lock()
	defer authority.absenceMu.Unlock()
	marker := authority.absenceUntrusted[leaseUUID]
	if marker == nil {
		marker = make(map[string]struct{}, len(reporters))
		authority.absenceUntrusted[leaseUUID] = marker
	}
	for _, backendName := range reporters {
		marker[backendName] = struct{}{}
	}
}

func (authority *ReconciliationCoordinator) resolveFreshExcludedObservations(
	snapshot inventory.Snapshot,
	excluded map[string]struct{},
) {
	if !authority.Valid() {
		return
	}
	binding := authority.projector.collector.Binding()
	authority.absenceMu.Lock()
	defer authority.absenceMu.Unlock()
	for leaseUUID, marker := range authority.absenceUntrusted {
		if _, remainsExcluded := excluded[leaseUUID]; remainsExcluded {
			continue
		}
		record := authority.coordinator.store.Lookup(leaseUUID)
		if record.State() != StateConfirmed || record.Attempt != "" {
			continue
		}
		// Account for every remembered reporter AND the current durable owner in
		// this one sealed epoch. The owner may report its provision/retention or
		// prove exact absence; other reporters must prove absence. This consumes
		// only stale diagnostics, never placement, lifecycle, attempt, or conflict
		// authority. A later action still requires its own causal Registry claim.
		reporters := maps.Clone(marker)
		reporters[record.Backend] = struct{}{}
		accounted := true
		for backendName := range reporters {
			if backendName == record.Backend &&
				snapshot.TrustedReporter(binding, backendName, leaseUUID) &&
				!snapshot.UntrustedReporter(binding, backendName, leaseUUID) {
				continue
			}
			storageID, known := authority.coordinator.store.ExpectedBackendStorageIdentity(backendName)
			if !known || !snapshot.OwnerAbsent(binding, backendName, storageID, leaseUUID) {
				accounted = false
				break
			}
		}
		if accounted {
			delete(authority.absenceUntrusted, leaseUUID)
		}
	}
}

func (authority *ReconciliationCoordinator) AbsenceUntrusted(leaseUUID string) bool {
	if !authority.Valid() || leaseUUID == "" {
		return true
	}
	authority.absenceMu.Lock()
	defer authority.absenceMu.Unlock()
	_, present := authority.absenceUntrusted[leaseUUID]
	return present
}

func (authority *ReconciliationCoordinator) AbsenceUntrustedLeaseUUIDs() []string {
	if !authority.Valid() {
		return nil
	}
	authority.absenceMu.Lock()
	defer authority.absenceMu.Unlock()
	return slices.Sorted(maps.Keys(authority.absenceUntrusted))
}

func (authority *ReconciliationCoordinator) RetireTerminalAbsenceUntrusted(
	leaseUUID string,
) {
	if !authority.Valid() || leaseUUID == "" {
		return
	}
	authority.absenceMu.Lock()
	delete(authority.absenceUntrusted, leaseUUID)
	authority.absenceMu.Unlock()
}
