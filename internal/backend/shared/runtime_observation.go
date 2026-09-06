package shared

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// RuntimeObservationPermit is an opaque, journal-pair-issued capability for
// publishing an autonomous observation about one exact runtime phase. Unlike
// RuntimeGenerationProof, which is intentionally release-only so substrate
// events can be classified without callback authority, this permit also seals
// the aggregate mutation phase observed under the pair's per-lease gate.
//
// A permit can exist only while the exact release generation is active and the
// lease has either no mutation head, the matching successful operation
// receipt, or a failed successor which durably seals this exact active Release
// as its predecessor. Pending operation work, an initial failed operation, and
// every maintenance, close, or closed phase are unrepresentable publication
// authority. PublishLifecycleFailure re-attests both sealed facts, so retaining
// or copying a permit cannot carry an observation across a later phase
// transition or reopened journal pair. The zero value is invalid.
type RuntimeObservationPermit struct {
	callbacks *CallbackStore
	releases  *ReleaseStore
	runtime   RuntimeGenerationProof
	phase     runtimeObservationPhaseWitness
}

type runtimeObservationPhase uint8

const (
	runtimeObservationPhaseInvalid runtimeObservationPhase = iota
	runtimeObservationPhaseNoHead
	runtimeObservationPhaseSucceededOperation
	runtimeObservationPhaseFailedSuccessor
)

// runtimeObservationPhaseWitness is deliberately a closed, comparable value.
// Terminal operation heads are sealed by their full durable digest; a Failed
// successor additionally carries the pair-bound exact predecessor relation.
// No-head is a distinct variant rather than a nil pointer or magic zero digest.
type runtimeObservationPhaseWitness struct {
	kind      runtimeObservationPhase
	digest    [sha256.Size]byte
	operation OperationID
	failed    failedOperationOverRelease
}

func newFailedSuccessorRuntimeObservationWitness(
	head operationLeaseMutationHead,
	predecessor failedOperationOverRelease,
) runtimeObservationPhaseWitness {
	return runtimeObservationPhaseWitness{
		kind:      runtimeObservationPhaseFailedSuccessor,
		digest:    head.headDigest(),
		operation: head.claim.OperationID(),
		failed:    predecessor,
	}
}

func newNoHeadRuntimeObservationWitness() runtimeObservationPhaseWitness {
	return runtimeObservationPhaseWitness{kind: runtimeObservationPhaseNoHead}
}

func newSucceededOperationRuntimeObservationWitness(
	head operationLeaseMutationHead,
) runtimeObservationPhaseWitness {
	return runtimeObservationPhaseWitness{
		kind:      runtimeObservationPhaseSucceededOperation,
		digest:    head.headDigest(),
		operation: head.claim.OperationID(),
	}
}

func (witness runtimeObservationPhaseWitness) validFor(
	proof RuntimeGenerationProof,
) bool {
	switch witness.kind {
	case runtimeObservationPhaseNoHead:
		return witness.digest == ([sha256.Size]byte{}) && witness.operation.IsZero() &&
			witness.failed == (failedOperationOverRelease{})
	case runtimeObservationPhaseSucceededOperation:
		return witness.digest != ([sha256.Size]byte{}) &&
			proof.AuthorityClass() == ReleaseAuthorityTyped &&
			witness.operation.Valid() && witness.operation == proof.OperationID() &&
			witness.failed == (failedOperationOverRelease{})
	case runtimeObservationPhaseFailedSuccessor:
		return witness.digest != ([sha256.Size]byte{}) && witness.operation.Valid() &&
			witness.failed.callbacks != nil &&
			witness.failed.releases == proof.releases &&
			witness.failed.successorDigest == witness.digest &&
			witness.failed.predecessor == proof.claim
	default:
		return false
	}
}

// Valid reports whether the permit still names its exact open journal pair.
// It intentionally does not perform I/O; publication is the consumption point
// which re-attests the sealed release and aggregate phase under the lease gate.
func (permit RuntimeObservationPermit) Valid() bool {
	return permit.callbacks != nil && permit.releases != nil &&
		boltStoreIsOpen(permit.callbacks.boltStore) &&
		boltStoreIsOpen(permit.releases.boltStore) &&
		permit.runtime.Valid() && permit.runtime.releases == permit.releases &&
		(permit.phase.kind != runtimeObservationPhaseFailedSuccessor ||
			permit.phase.failed.callbacks == permit.callbacks) &&
		permit.phase.validFor(permit.runtime)
}

// LeaseUUID returns the lease whose exact runtime and mutation phase the
// permit seals. An invalid permit returns the empty string.
func (permit RuntimeObservationPermit) LeaseUUID() string {
	if !permit.Valid() {
		return ""
	}
	return permit.runtime.LeaseUUID()
}

// AuthorizeRuntimeObservationContext atomically observes the release journal
// and callback aggregate while holding their shared per-lease transition gate.
// It is the only constructor for RuntimeObservationPermit. Cancellation before
// the journal pair is attested creates no capability and changes no durable
// state.
func (p *CallbackPublisher) AuthorizeRuntimeObservationContext(
	ctx context.Context,
	proof RuntimeGenerationProof,
) (RuntimeObservationPermit, error) {
	if ctx == nil {
		return RuntimeObservationPermit{}, errors.New(
			"callback publisher: runtime observation ownership context is required",
		)
	}
	if !p.valid() {
		return RuntimeObservationPermit{}, errors.New("callback publisher is invalid")
	}
	if !proof.Valid() {
		return RuntimeObservationPermit{}, errors.New(
			"callback publisher: active runtime proof is required",
		)
	}
	if proof.releases != p.operations.releases {
		return RuntimeObservationPermit{}, errors.New(
			"callback publisher: runtime generation proof belongs to another journal pair",
		)
	}

	unlock, err := p.store.lockDeliveryLeaseContext(ctx, proof.LeaseUUID())
	if err != nil {
		return RuntimeObservationPermit{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return RuntimeObservationPermit{}, err
	}
	_, phase, err := p.attestRuntimeObservationLocked(proof)
	if err != nil {
		return RuntimeObservationPermit{}, fmt.Errorf(
			"authorize lifecycle runtime observation: %w", err,
		)
	}
	permit := RuntimeObservationPermit{
		callbacks: p.store,
		releases:  p.operations.releases,
		runtime:   proof,
		phase:     phase,
	}
	if !permit.Valid() {
		return RuntimeObservationPermit{}, errors.New(
			"callback publisher produced an invalid runtime observation permit",
		)
	}
	return permit, nil
}

func (p *CallbackPublisher) attestRuntimeObservationLocked(
	proof RuntimeGenerationProof,
) (ReleaseRuntimeIdentity, runtimeObservationPhaseWitness, error) {
	authority, err := p.reattestRuntimeGenerationLocked(proof)
	if err != nil {
		return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, err
	}

	var (
		head    leaseMutationHead
		present bool
	)
	err = p.store.view(func(tx *bolt.Tx) error {
		var readErr error
		head, present, readErr = getLeaseMutationHeadTx(tx, proof.LeaseUUID())
		if readErr != nil {
			return readErr
		}
		return nil
	})
	if err != nil {
		return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, err
	}

	var witness runtimeObservationPhaseWitness
	if !present {
		witness = newNoHeadRuntimeObservationWitness()
	} else {
		operation, ok := head.(operationLeaseMutationHead)
		if !ok {
			return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, fmt.Errorf(
				"runtime observation is blocked by %s mutation phase", head.headKind(),
			)
		}
		switch operation.claim.entry.State {
		case operationIntentSucceeded:
			if !operationHeadMatchesRuntimeIdentity(operation, authority) {
				return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, errors.New(
					"successful operation head does not match the active runtime authority",
				)
			}
			witness = newSucceededOperationRuntimeObservationWitness(operation)
		case operationIntentFailed:
			predecessor, bindErr := bindFailedOperationOverRelease(
				p.store, p.operations.releases, operation, proof.claim,
			)
			if bindErr != nil {
				return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, fmt.Errorf(
					"failed operation is not an exact successor of the active runtime: %w",
					bindErr,
				)
			}
			witness = newFailedSuccessorRuntimeObservationWitness(operation, predecessor)
		default:
			return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, fmt.Errorf(
				"runtime observation is blocked by %s operation phase",
				operation.claim.entry.State,
			)
		}
	}
	if !witness.validFor(proof) {
		return ReleaseRuntimeIdentity{}, runtimeObservationPhaseWitness{}, errors.New(
			"active runtime produced an invalid observation phase witness",
		)
	}
	return authority, witness, nil
}

func operationHeadMatchesRuntimeIdentity(
	head operationLeaseMutationHead,
	authority ReleaseRuntimeIdentity,
) bool {
	entry := head.claim.entry
	return entry != nil && authority.Class() == ReleaseAuthorityTyped &&
		entry.OperationID == authority.OperationID() &&
		entry.Tenant == authority.Tenant() &&
		entry.ProviderUUID == authority.ProviderUUID() &&
		entry.CallbackURL == authority.CallbackURL() &&
		entry.LifecycleCallbackURL == authority.LifecycleCallbackURL()
}

func (p *CallbackPublisher) reattestRuntimeObservationPermitLocked(
	permit RuntimeObservationPermit,
) (ReleaseRuntimeIdentity, error) {
	if !permit.Valid() {
		return ReleaseRuntimeIdentity{}, errors.New("runtime observation permit is invalid")
	}
	if permit.callbacks != p.store || permit.releases != p.operations.releases {
		return ReleaseRuntimeIdentity{}, errors.New(
			"runtime observation permit belongs to another journal pair",
		)
	}
	authority, phase, err := p.attestRuntimeObservationLocked(permit.runtime)
	if err != nil {
		return ReleaseRuntimeIdentity{}, err
	}
	if phase != permit.phase {
		return ReleaseRuntimeIdentity{}, errors.New(
			"runtime mutation phase changed before lifecycle failure publication",
		)
	}
	return authority, nil
}
