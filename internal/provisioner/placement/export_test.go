package placement

import (
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
)

// RecordProvision, RecordRetention, and RecordUntrusted deliberately exist
// only in the placement package's white-box test binary. Production and
// dependent-package tests must consume opaque responses collected by the
// exact reconciliation sweep.
func (sweep *ReconciliationSweep) RecordProvision(
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

func (sweep *ReconciliationSweep) RecordRetention(
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

func (sweep *ReconciliationSweep) RecordUntrusted(
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

// White-box placement tests intentionally exercise the package-private
// projection machinery. These aliases and wrappers are compiled only into the
// placement package's own test binary; dependent packages must compose a real
// ReconciliationCoordinator and ReconciliationSweep.
type InventoryFence = inventoryFence
type InventoryProjector = inventoryProjector
type InventoryProjection = inventoryProjection
type ProjectionResult = projectionResult

var ErrInventoryProjectorConflict = errInventoryProjectorConflict

func (fence inventoryFence) Valid() bool { return fence.valid() }

func (s *Store) BeginInventorySession() inventoryFence {
	fence, _ := s.beginInventorySession()
	return fence
}

func (s *Store) EndInventorySession(fence inventoryFence) {
	s.endInventorySession(fence, inventorySessionReport{})
}

// deleteRecord permits white-box tests to invalidate an exact record revision
// without granting production code a deletion path around PruneAbsenceProof.
func (s *Store) deleteRecord(revision RecordRevision) (bool, error) {
	if !revision.Valid() || revision.issuer != s.recordIssuer {
		return false, ErrInvalidRecordRevision
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}
	leaseUUID := revision.leaseUUID

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreSourceClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, leaseUUID)
	}
	if s.attemptClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, leaseUUID)
	}
	p, exists := s.cache[leaseUUID]
	if !exists || p.revision != revision.value {
		return false, nil
	}
	if err := s.deleteLocked(leaseUUID, "delete typed placement record"); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) BindInventoryProjector(
	backendNames []string,
) (*inventoryProjector, error) {
	return s.bindInventoryProjector(backendNames)
}

func (projector *inventoryProjector) Valid() bool { return projector.valid() }

func (projector *inventoryProjector) BeginCollection() *inventory.Session {
	return projector.beginCollection()
}

func (projector *inventoryProjector) Complete(snapshot inventory.Snapshot) bool {
	return projector.complete(snapshot)
}

func (projector *inventoryProjector) Project(
	fence inventoryFence,
	projection InventoryProjection,
) (ProjectionResult, error) {
	return projector.project(fence, projection)
}

func (result projectionResult) PruneAbsence(
	leaseUUID string,
) (PruneAbsenceProof, bool) {
	return result.pruneAbsence(leaseUUID)
}

// ConfigureBackendTopologyWithStorageIdentities preserves concise same-package
// fixtures without exposing the identity-only topology bypass in production.
func (s *Store) ConfigureBackendTopologyWithStorageIdentities(
	names []string,
	identities map[string]backendidentity.ID,
) error {
	return s.configureBackendTopologyWithStorageIdentities(names, identities)
}

// ScopeAdmission preserves white-box Store tests while production admission
// scopes are minted only inside purpose coordinators.
func (s *Store) ScopeAdmission(
	baseline AdmissionBaseline,
	backendNames []string,
) (AdmissionScope, error) {
	return s.scopeAdmission(baseline, backendNames)
}
