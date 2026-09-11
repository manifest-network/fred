package shared

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

// RuntimeGenerationProof is an opaque, store-issued capability for one exact
// active Release generation. It seals the row's version, full digest, runtime
// authority class, and typed operation identity (when present). Both current
// typed releases and valid v0.13 legacy releases can mint it; the zero value is
// invalid.
//
// The proof is observational authority only. It cannot mutate a release or
// choose callback semantics; CallbackPublisher may consume it only for the
// fixed lifecycle-failure observation after a fresh exact ReleaseStore read,
// so a delayed substrate event cannot act on a replacement generation.
type RuntimeGenerationProof struct {
	releases  *ReleaseStore
	claim     ReleaseClaim
	authority ReleaseRuntimeIdentity
}

// ProveRuntimeGeneration reads the active Release and mints an exact generation
// proof from the same bbolt snapshot.
func (s *ReleaseStore) ProveRuntimeGeneration(leaseUUID string) (RuntimeGenerationProof, error) {
	if s == nil {
		return RuntimeGenerationProof{}, errors.New("runtime generation release store is required")
	}
	release, claim, err := s.claimLatestActive(leaseUUID)
	if err != nil {
		return RuntimeGenerationProof{}, err
	}
	authority, ok := release.RuntimeIdentity()
	if !ok {
		return RuntimeGenerationProof{}, errors.New("active release has no exact runtime authority")
	}
	proof := RuntimeGenerationProof{releases: s, claim: claim, authority: authority}
	if !proof.Valid() {
		return RuntimeGenerationProof{}, errors.New("active release produced an invalid runtime generation proof")
	}
	return proof, nil
}

func (proof RuntimeGenerationProof) Valid() bool {
	if proof.releases == nil || !boltStoreIsOpen(proof.releases.boltStore) ||
		proof.claim.issuer != proof.releases || !proof.claim.valid() {
		return false
	}
	switch proof.authority.Class() {
	case ReleaseAuthorityTyped:
		return proof.authority.OperationID().Valid()
	case ReleaseAuthorityLegacy:
		return proof.authority.OperationID().IsZero()
	default:
		return false
	}
}

func (proof RuntimeGenerationProof) LeaseUUID() string {
	if !proof.Valid() {
		return ""
	}
	return proof.claim.LeaseUUID()
}

func (proof RuntimeGenerationProof) AuthorityClass() ReleaseAuthorityClass {
	if !proof.Valid() {
		return 0
	}
	return proof.authority.Class()
}

func (proof RuntimeGenerationProof) OperationID() OperationID {
	if !proof.Valid() {
		return OperationID{}
	}
	return proof.authority.OperationID()
}

// Version returns the exact active release version sealed by this proof. A
// zero result means the proof is invalid. OperationID alone is deliberately
// insufficient as a generation discriminator because maintenance releases
// preserve their originating provision/restore operation ID.
func (proof RuntimeGenerationProof) Version() int {
	if !proof.Valid() {
		return 0
	}
	return proof.claim.Version()
}

// MatchesRelease reports whether release is the exact durable row from which
// this proof was minted. The comparison includes the full serialized digest,
// not only Version or OperationID, so an in-place legacy-authority backfill
// invalidates an observation captured before that write.
func (proof RuntimeGenerationProof) MatchesRelease(release Release) bool {
	if !proof.Valid() || release.Version != proof.claim.Version() {
		return false
	}
	encoded, err := json.Marshal(release)
	if err != nil {
		return false
	}
	return sha256.Sum256(encoded) == proof.claim.Digest()
}

// Reattest proves that this exact row remains the current active Release.
func (proof RuntimeGenerationProof) Reattest() error {
	if !proof.Valid() {
		return errors.New("runtime generation proof is invalid")
	}
	release, current, err := proof.releases.claimLatestActive(proof.claim.LeaseUUID())
	if err != nil {
		return fmt.Errorf("read current runtime generation: %w", err)
	}
	authority, ok := release.RuntimeIdentity()
	if !ok {
		return errors.New("current active release has no exact runtime authority")
	}
	if current.version != proof.claim.version || current.digest != proof.claim.digest ||
		authority.Class() != proof.authority.Class() ||
		authority.OperationID() != proof.authority.OperationID() {
		return errors.New("active runtime generation changed")
	}
	return nil
}
