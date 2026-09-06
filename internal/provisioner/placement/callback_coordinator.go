package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// TimeoutCoordinator is the least-authority facade for timeout settlement.
type TimeoutCoordinator struct {
	coordinator  *OperationCoordinator
	execution    *ExecutionCoordinator
	issuer       *operationCoordinatorMarker
	controlPlane *boundProviderControlPlane
}

func (execution *ExecutionCoordinator) TimeoutCoordinator() (*TimeoutCoordinator, error) {
	controlPlane, err := execution.providerControlPlane()
	if err != nil {
		return nil, errors.New("valid backend execution coordinator and provider control plane are required")
	}
	coordinator := execution.coordinator
	coordinator.bindMu.Lock()
	defer coordinator.bindMu.Unlock()
	if coordinator.timeout != nil {
		return nil, errors.New("timeout coordinator is already bound")
	}
	authority := &TimeoutCoordinator{
		coordinator: coordinator, execution: execution,
		issuer: coordinator.marker, controlPlane: controlPlane,
	}
	coordinator.timeout = authority
	return authority, nil
}

func (authority *TimeoutCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil &&
		authority.coordinator.Valid() && authority.issuer == authority.coordinator.marker &&
		authority.coordinator.timeout == authority &&
		authority.execution == authority.coordinator.execution &&
		authority.controlPlane != nil && authority.controlPlane.validFor(authority.execution)
}

func (authority *TimeoutCoordinator) TimedOut(timeout time.Duration) []TimeoutCandidate {
	if !authority.Valid() {
		return nil
	}
	return authority.coordinator.timedOut(timeout)
}

func (authority *TimeoutCoordinator) finish(claim TimeoutSettlementClaim) error {
	switch claim.Generation() {
	case OperationGenerationAttempt:
		return authority.coordinator.finishTimeoutPreservingForRedelivery(claim)
	case OperationGenerationConfirmed:
		return authority.coordinator.finishTimeoutRetainingConfirmed(claim)
	case OperationGenerationInvalid:
		return errors.New("timeout claim has invalid durable generation")
	default:
		return fmt.Errorf("timeout claim has unknown durable generation %d", claim.Generation())
	}
}

// TimeoutDisposition is an observational classification of one high-level
// timeout application. Callers cannot use it to select a durable transition.
type TimeoutDisposition uint8

const (
	TimeoutInvalid TimeoutDisposition = iota
	TimeoutSkipped
	TimeoutRetry
	TimeoutRejected
	TimeoutLeaseTerminal
)

// TimeoutResult contains detached consequences from Settle. It carries no
// claim or authority that a caller could use for a second settlement.
type TimeoutResult struct {
	disposition TimeoutDisposition
	metadata    operation.SettlementMetadata
	rejected    uint64
	txHashes    []string
	err         error
}

func (result TimeoutResult) Disposition() TimeoutDisposition        { return result.disposition }
func (result TimeoutResult) Metadata() operation.SettlementMetadata { return result.metadata }
func (result TimeoutResult) Rejected() uint64                       { return result.rejected }
func (result TimeoutResult) TxHashes() []string                     { return slices.Clone(result.txHashes) }
func (result TimeoutResult) Err() error                             { return result.err }
func (result TimeoutResult) Settled() bool {
	return result.disposition == TimeoutRejected || result.disposition == TimeoutLeaseTerminal
}

// Settle is the sole timeout side-effect boundary. It derives the exact
// Registry+Store claim, rejects only that claim's lease through the chain
// dependency bound at construction, and after any mutation error derives
// convergence only from a bounded exact chain reread. It chooses the only
// valid durable generation transition internally.
func (authority *TimeoutCoordinator) Settle(
	ctx context.Context,
	candidate TimeoutCandidate,
) (result TimeoutResult) {
	if !authority.Valid() || ctx == nil || !candidate.Valid() {
		return TimeoutResult{disposition: TimeoutInvalid, err: errors.New("invalid timeout settlement")}
	}
	result.metadata = candidate.Metadata()
	if ctx.Err() != nil {
		result.disposition = TimeoutRetry
		result.err = ctx.Err()
		return result
	}
	admission := authority.coordinator.tryClaimTimeout(candidate)
	if !admission.Claimed() {
		result.disposition = TimeoutSkipped
		result.err = admission.Err()
		return result
	}
	claim := admission.Claim()
	finished := false
	defer func() {
		if !finished {
			authority.coordinator.releaseTimeout(claim)
		}
		if recovered := recover(); recovered != nil {
			result.disposition = TimeoutRetry
			result.err = fmt.Errorf("timeout lease rejection panicked: %v", recovered)
		}
	}()

	result.metadata = claim.Metadata()
	rejected, txHashes, err := authority.controlPlane.rejectLease(
		ctx, result.metadata.LeaseUUID(), "callback timeout",
	)
	result.rejected = rejected
	result.txHashes = slices.Clone(txHashes)
	terminal := false
	if err != nil {
		readCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
		observation := authority.controlPlane.observeLease(
			readCtx, result.metadata.LeaseUUID(), result.metadata.Tenant(),
		)
		cancel()
		lease, exact := exactLeaseFromObservation(observation)
		if !exact {
			result.disposition = TimeoutRetry
			result.err = fmt.Errorf(
				"verify timeout rejection after %w: %w",
				err, exactLeaseObservationError(observation),
			)
			return result
		}
		switch lease.State {
		case billingtypes.LEASE_STATE_ACTIVE,
			billingtypes.LEASE_STATE_CLOSED,
			billingtypes.LEASE_STATE_REJECTED,
			billingtypes.LEASE_STATE_EXPIRED:
			terminal = true
		case billingtypes.LEASE_STATE_PENDING:
			result.disposition = TimeoutRetry
			result.err = fmt.Errorf(
				"verify timeout rejection after %w: chain still reports PENDING", err,
			)
			return result
		default:
			result.disposition = TimeoutRetry
			result.err = fmt.Errorf(
				"verify timeout rejection after %w: chain reports %s",
				err, lease.State.String(),
			)
			return result
		}
	}
	if finishErr := authority.finish(claim); finishErr != nil {
		result.disposition = TimeoutRetry
		result.err = finishErr
		return result
	}
	finished = true
	if terminal {
		result.disposition = TimeoutLeaseTerminal
		result.err = err
		return result
	}
	result.disposition = TimeoutRejected
	return result
}
