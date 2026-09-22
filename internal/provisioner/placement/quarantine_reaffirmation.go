package placement

import "github.com/manifest-network/fred/internal/provisioner/inventory"

// quarantineReaffirmation removes only rejected-observation quarantine from an
// already known operation generation. It cannot establish an owner, promote an
// attempt, replace a principal, or complete an operation. Only the Store can
// issue it, from the exact immutable provision row and complete, identity-bound
// evidence for this lease across the configured topology.
type quarantineReaffirmation struct {
	store      *Store
	snapshot   inventory.Snapshot
	fence      inventoryFence
	revision   RecordRevision
	capability lifecycleCapability
}

// observeQuarantineReaffirmationLocked deliberately does not require an idle
// Registry: the unfinished operation is what needs its original authority back.
// The Store revision and claim fences still serialize every actual transition;
// this observation only removes quarantine while preserving those exact facts.
// Caller holds s.mu and enforces the inventory revision and live Store claims.
func (s *Store) observeQuarantineReaffirmationLocked(
	fence inventoryFence,
	snapshot inventory.Snapshot,
	leaseUUID string,
) quarantineReaffirmation {
	if !fence.valid() || fence.issuer != s || fence.epoch != s.authorityEpoch ||
		fence.sweepID != s.pendingInventorySweepID ||
		s.mutationRevisionLocked(leaseUUID) > fence.revision {
		return quarantineReaffirmation{}
	}
	record := s.cache[leaseUUID]
	if record.unusable || len(record.ConflictBackends) != 1 {
		return quarantineReaffirmation{}
	}
	backendName := record.ConflictBackends[0]
	if !record.CanResolveUntrustedPositive(backendName) ||
		(record.Backend != "" && record.Backend != backendName) ||
		(record.Attempt != "" && record.Attempt != backendName) ||
		(record.Backend == "" && record.Attempt == "") ||
		!s.singleReporterObservationLocked(snapshot, leaseUUID).Matches(
			s.inventoryEvidence, leaseUUID, backendName,
		) {
		return quarantineReaffirmation{}
	}
	row, present := snapshot.Provision(s.inventoryEvidence, backendName, leaseUUID)
	if !present {
		return quarantineReaffirmation{}
	}
	generation := sealedLifecycleObservation(row.LifecycleGeneration())
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists || capability.unusable || generation.Kind != LifecycleObservationTyped ||
		(record.Backend != "" && capability.backend != record.Backend) {
		return quarantineReaffirmation{}
	}
	principal := runtimePrincipal{tenant: row.Tenant(), providerUUID: row.ProviderUUID()}
	if !principal.valid() {
		return quarantineReaffirmation{}
	}
	if record.Attempt != "" {
		if !validAttemptMetadata(
			leaseUUID, record.attemptOperationID, record.attemptOperationKind,
			record.attemptRestoreSourceLeaseUUID, record.attemptPayloadFingerprint,
			record.attemptRequestSnapshot, record.attemptCallbackPair,
		) {
			return quarantineReaffirmation{}
		}
		id, err := lifecycleIDForOperation(record.attemptOperationID)
		if err != nil || generation.ID != id || capability.attemptBackend != backendName ||
			capability.attemptID != id || principal != (runtimePrincipal{
			tenant:       record.attemptRequestSnapshot.Tenant(),
			providerUUID: record.attemptRequestSnapshot.ProviderUUID(),
		}) {
			return quarantineReaffirmation{}
		}
	} else if capability.backend != backendName || generation.ID != capability.id ||
		principal != capability.principal {
		return quarantineReaffirmation{}
	}
	return quarantineReaffirmation{
		store: s, snapshot: snapshot, fence: fence,
		revision: s.newRecordRevision(leaseUUID, record.revision), capability: capability,
	}
}

// placementLocked consumes the proof against the same record and capability.
// It copies the original owner, attempt, operation metadata, and lifecycle row;
// ordinary callback/deprovision claims must independently authorize their work.
func (proof quarantineReaffirmation) placementLocked(s *Store) (Placement, bool) {
	if s == nil || proof.store != s || !proof.revision.Valid() ||
		proof.revision.issuer != s.recordIssuer || !proof.snapshot.ValidFor(s.inventoryEvidence) ||
		proof.fence.epoch != s.authorityEpoch || proof.fence.sweepID != s.pendingInventorySweepID {
		return Placement{}, false
	}
	record, present := s.cache[proof.revision.leaseUUID]
	if !present || record.revision != proof.revision.value ||
		s.lifecycleCache[proof.revision.leaseUUID] != proof.capability {
		return Placement{}, false
	}
	record.Conflict = false
	record.ConflictBackends = nil
	record.ConflictOwnersUnknown = false
	record.untrustedPositive = false
	return record, true
}
