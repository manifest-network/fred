package shared

import (
	"crypto/sha256"
	"errors"

	"github.com/manifest-network/fred/internal/backend"
)

func (s *ReleaseStore) DeleteCloseHistory(
	leaseUUID string,
	version int,
	digest [sha256.Size]byte,
) error {
	return s.deleteCloseHistory(CloseIntentClaim{
		entry: closeIntentEntry{
			LeaseUUID:            leaseUUID,
			ActiveReleaseVersion: version,
		},
		activeReleaseDigest: digest,
	})
}

func (s *CallbackStore) NewCloseIntentCandidate(
	spec closeIntentSpec,
) (closeIntentCandidate, error) {
	backendName, storageID := s.journalBackendIdentity("")
	if backendName == "" || !storageID.Valid() {
		return closeIntentCandidate{}, errors.New(
			"close intent candidate requires an identity-bound callback journal",
		)
	}
	return newCloseIntentCandidate(s, spec, backendName, storageID)
}

// These package-local shims keep low-level journal transaction tests focused
// on their wire invariants. They are deliberately absent from production: all
// production close authority crosses CloseSettlement.
func (s *CallbackStore) BeginCloseIntent(candidate closeIntentCandidate) (CloseIntentAdmission, error) {
	if s == nil {
		return CloseIntentAdmission{}, errors.New("callback store is nil")
	}
	unlock := s.lockDeliveryLease(candidate.spec.LeaseUUID)
	defer unlock()
	return s.beginCloseIntentLocked(candidate)
}

func (s *CallbackStore) AdvanceCloseExecutionGeneration(claim CloseIntentClaim) (CloseIntentClaim, error) {
	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	return s.advanceCloseExecutionGenerationLocked(claim)
}

func (s *CallbackStore) ResolveCloseIntent(
	claim CloseIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
	retained bool,
) (CallbackEntry, error) {
	var outcome closeCompletion
	switch {
	case status != backend.CallbackStatusDeprovisioned:
		return CallbackEntry{}, errors.New("invalid completion status")
	case status == backend.CallbackStatusDeprovisioned && retained && !claim.RetainOnClose():
		return CallbackEntry{}, errors.New("cannot retain an unretained close")
	case status == backend.CallbackStatusDeprovisioned && retained:
		outcome = closeCompletionRetained
	case status == backend.CallbackStatusDeprovisioned && !retained:
		outcome = closeCompletionDestroyed
	}
	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	return s.resolveCloseIntentLocked(claim, outcome, errMsg)
}
