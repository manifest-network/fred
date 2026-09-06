package leasesm

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// ProvisionWorkOutcome is the closed result of one exact provision worker.
// A worker cannot report a plain error: every return is either backed by the
// terminal release proof which the actor may publish, or explicitly preserves
// the Started intent for inventory recovery.
type ProvisionWorkOutcome interface {
	provisionWorkOutcome()
	operationID() shared.OperationID
	leaseUUID() string
}

type provisionWorkSuccess struct {
	result ProvisionSuccessResult
}

type provisionWorkFailure struct {
	callbackErr string
	reason      backend.Reason
	err         error
	logs        map[string]string
	proof       shared.OperationReleaseUncommitted
}

type provisionWorkAmbiguous struct {
	err   error
	claim shared.OperationIntentClaim
}

func (provisionWorkSuccess) provisionWorkOutcome()   {}
func (provisionWorkFailure) provisionWorkOutcome()   {}
func (provisionWorkAmbiguous) provisionWorkOutcome() {}

func (o provisionWorkSuccess) operationID() shared.OperationID {
	return o.result.operationRelease.OperationID()
}
func (o provisionWorkSuccess) leaseUUID() string {
	return o.result.operationRelease.LeaseUUID()
}
func (o provisionWorkFailure) operationID() shared.OperationID { return o.proof.OperationID() }
func (o provisionWorkFailure) leaseUUID() string               { return o.proof.LeaseUUID() }
func (o provisionWorkAmbiguous) operationID() shared.OperationID {
	return o.claim.OperationID()
}
func (o provisionWorkAmbiguous) leaseUUID() string { return o.claim.LeaseUUID() }

func NewProvisionWorkSuccess(proof shared.OperationReleaseCommitted) (ProvisionWorkOutcome, error) {
	ready, ok := proof.TargetReady()
	if !ok {
		return nil, errors.New("provision work success requires a committed operation release")
	}
	_, ids, services := ready.Projection()
	result, err := NewProvisionSuccessResult(ProvisionSuccessProjection{
		ContainerIDs: ids, ServiceContainers: services,
	}, proof)
	if err != nil {
		return nil, err
	}
	return provisionWorkSuccess{result: result}, nil
}

func NewProvisionWorkFailure(
	err error,
	callbackErr string,
	reason backend.Reason,
	logs map[string]string,
	proof shared.OperationReleaseUncommitted,
) (ProvisionWorkOutcome, error) {
	if err == nil || !proof.Valid() || proof.Kind() != shared.OperationIntentProvision {
		return nil, errors.New("provision work failure requires an error and exact provision failure proof")
	}
	return provisionWorkFailure{
		callbackErr: callbackErr,
		reason:      reason,
		err:         err,
		logs:        cloneStringMap(logs),
		proof:       proof,
	}, nil
}

func NewProvisionWorkAmbiguous(
	err error,
	claim shared.OperationIntentClaim,
) (ProvisionWorkOutcome, error) {
	if err == nil || !claim.Valid() || claim.Kind() != shared.OperationIntentProvision {
		return nil, errors.New("ambiguous provision work requires an error and exact pending provision")
	}
	return provisionWorkAmbiguous{err: err, claim: claim}, nil
}

func operationAuthorityMatches(
	claim shared.OperationIntentClaim,
	leaseUUID string,
	operationID shared.OperationID,
) bool {
	return claim.Valid() && claim.OperationID().Valid() &&
		claim.OperationID() == operationID && claim.LeaseUUID() == leaseUUID
}

func validateProvisionWorkOutcome(
	outcome ProvisionWorkOutcome,
	claim shared.OperationIntentClaim,
) error {
	if outcome == nil || !claim.Valid() || claim.Kind() != shared.OperationIntentProvision {
		return errors.New("provision work outcome or command authority is invalid")
	}
	if !operationAuthorityMatches(claim, outcome.leaseUUID(), outcome.operationID()) {
		return errors.New("provision work outcome belongs to another operation")
	}
	switch typed := outcome.(type) {
	case provisionWorkSuccess:
		if !typed.result.operationRelease.MatchesIntent(claim) {
			return errors.New("provision success lost its committed release")
		}
	case provisionWorkFailure:
		if typed.err == nil || !typed.proof.MatchesIntent(claim) ||
			typed.proof.Kind() != shared.OperationIntentProvision {
			return errors.New("provision failure lost its terminal release proof")
		}
	case provisionWorkAmbiguous:
		if typed.err == nil || !typed.claim.Valid() ||
			typed.claim.Kind() != shared.OperationIntentProvision {
			return errors.New("ambiguous provision lost its recovery authority")
		}
	default:
		return fmt.Errorf("unknown provision work outcome %T", outcome)
	}
	return nil
}
