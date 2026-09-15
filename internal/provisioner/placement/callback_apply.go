package placement

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const (
	callbackClaimPollInterval = 25 * time.Millisecond
	callbackClaimMaxWait      = 30 * time.Second
	maxCallbackRejectReason   = 256
)

var (
	ErrCallbackChainUnavailable        = errors.New("backend callback chain client is unavailable")
	ErrCallbackAcknowledgerUnavailable = errors.New("backend callback acknowledger is unavailable")
	ErrCallbackAcknowledgeFailed       = errors.New("backend callback acknowledgement failed")
	ErrCallbackSettlementLost          = errors.New("backend callback lost its operation settlement claim")
	ErrCallbackClaimTimeout            = errors.New("timed out waiting for callback settlement claim")
	ErrCallbackRecoveryBusy            = errors.New("backend callback recovery lease is busy")
	ErrCallbackStorageIdentityMissing  = errors.New("backend callback storage identity is required")
	ErrCallbackStorageIdentityMismatch = errors.New("backend callback storage identity does not match placement authority")
	ErrCallbackStorageIdentityUnbound  = errors.New("backend callback storage identity is not durably bound")
	ErrCallbackProofBoundaryMismatch   = errors.New("backend callback proof belongs to another verification boundary")
)

// AuthenticatedCallbackCoordinator owns callback route selection, chain-state
// classification, and joined Registry/Store settlement. A caller supplies only
// an opaque verified request; it cannot choose a claim, generation, outcome,
// backend, or finishing operation.
type AuthenticatedCallbackCoordinator struct {
	coordinator  *OperationCoordinator
	execution    *ExecutionCoordinator
	issuer       *operationCoordinatorMarker
	controlPlane *boundProviderControlPlane
	proofs       hmacauth.CallbackProofConsumer
}

// AuthenticatedCallbackCoordinator derives callback authority from the one
// provider control plane already bound to the backend execution aggregate.
func (execution *ExecutionCoordinator) AuthenticatedCallbackCoordinator(
	proofs hmacauth.CallbackProofConsumer,
) (
	*AuthenticatedCallbackCoordinator,
	error,
) {
	controlPlane, err := execution.providerControlPlane()
	if err != nil || !proofs.Valid() {
		return nil, ErrOperationSettlementGenerationUnavailable
	}
	return &AuthenticatedCallbackCoordinator{
		coordinator:  execution.coordinator,
		execution:    execution,
		issuer:       execution.issuer,
		controlPlane: controlPlane,
		proofs:       proofs,
	}, nil
}

func (coordinator *AuthenticatedCallbackCoordinator) Valid() bool {
	return coordinator != nil && coordinator.coordinator != nil &&
		coordinator.coordinator.Valid() &&
		coordinator.issuer == coordinator.coordinator.marker &&
		coordinator.execution == coordinator.coordinator.execution &&
		coordinator.controlPlane != nil &&
		coordinator.controlPlane.validFor(coordinator.execution) &&
		coordinator.proofs.Valid()
}

type CallbackOperationOutcome uint8

const (
	CallbackOperationNone CallbackOperationOutcome = iota
	CallbackOperationSucceeded
	CallbackOperationFailed
)

type CallbackLifecycleOutcome uint8

const (
	CallbackLifecycleNone CallbackLifecycleOutcome = iota
	CallbackLifecycleApplied
	CallbackLifecycleDropped
	CallbackLifecycleRetryable
)

// CallbackResult contains only observational consequences of an already
// completed application. It carries no Store or Registry capability.
type CallbackResult struct {
	observation             callbackwire.Observation
	authoritativeBackend    string
	operationOutcome        CallbackOperationOutcome
	operationKind           operation.Kind
	operationStartedAt      time.Time
	nonInFlight             bool
	lifecycleOutcome        CallbackLifecycleOutcome
	lifecycleVerdict        LifecycleVerdict
	eventStatus             backend.ProvisionStatus
	eventFailure            string
	hasEvent                bool
	deletePayload           bool
	deprovisionedBackend    string
	deprovisionOwnedSuccess bool
	claimWaitTimedOut       bool
}

func (result CallbackResult) Observation() callbackwire.Observation { return result.observation }
func (result CallbackResult) AuthoritativeBackend() string          { return result.authoritativeBackend }
func (result CallbackResult) OperationOutcome() CallbackOperationOutcome {
	return result.operationOutcome
}
func (result CallbackResult) OperationKind() operation.Kind { return result.operationKind }
func (result CallbackResult) OperationStartedAt() time.Time { return result.operationStartedAt }
func (result CallbackResult) NonInFlight() bool             { return result.nonInFlight }
func (result CallbackResult) LifecycleOutcome() CallbackLifecycleOutcome {
	return result.lifecycleOutcome
}
func (result CallbackResult) LifecycleVerdict() LifecycleVerdict { return result.lifecycleVerdict }
func (result CallbackResult) Event() (backend.ProvisionStatus, string, bool) {
	return result.eventStatus, result.eventFailure, result.hasEvent
}
func (result CallbackResult) DeletePayload() bool { return result.deletePayload }
func (result CallbackResult) DeprovisionedBackend() (string, bool) {
	return result.deprovisionedBackend, result.deprovisionedBackend != ""
}
func (result CallbackResult) DeprovisionOwnedSuccess() bool {
	return result.deprovisionOwnedSuccess
}
func (result CallbackResult) ClaimWaitTimedOut() bool { return result.claimWaitTimedOut }

func (result *CallbackResult) publish(status backend.ProvisionStatus, failure string) {
	result.eventStatus, result.eventFailure, result.hasEvent = status, failure, true
}

type callbackLeaseClass uint8

const (
	callbackLeaseUnknown callbackLeaseClass = iota
	callbackLeasePending
	callbackLeaseActive
	callbackLeaseTerminal
)

func classifyCallbackLease(lease *billingtypes.Lease) callbackLeaseClass {
	if lease == nil {
		return callbackLeaseUnknown
	}
	switch lease.State {
	case billingtypes.LEASE_STATE_PENDING:
		return callbackLeasePending
	case billingtypes.LEASE_STATE_ACTIVE:
		return callbackLeaseActive
	case billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED:
		return callbackLeaseTerminal
	default:
		return callbackLeaseUnknown
	}
}

func callbackLeaseState(lease *billingtypes.Lease) string {
	if lease == nil {
		return "<not found>"
	}
	return lease.State.String()
}

func classifyCallbackObservation(observation exactLeaseObservation) callbackLeaseClass {
	lease, ok := exactLeaseFromObservation(observation)
	if !ok {
		return callbackLeaseUnknown
	}
	return classifyCallbackLease(&lease)
}

func callbackObservationState(observation exactLeaseObservation) string {
	lease, ok := exactLeaseFromObservation(observation)
	if !ok {
		return "unavailable"
	}
	return callbackLeaseState(&lease)
}

func terminalCallbackObservationRepresentsFailure(observation exactLeaseObservation) bool {
	lease, ok := exactLeaseFromObservation(observation)
	return ok && terminalLeaseRepresentsFailure(&lease)
}

func terminalLeaseRepresentsFailure(lease *billingtypes.Lease) bool {
	return lease != nil && lease.State == billingtypes.LEASE_STATE_REJECTED
}

func callbackRejectReason(reason string) string {
	if len(reason) <= maxCallbackRejectReason {
		return reason
	}
	limit := maxCallbackRejectReason - 3
	for limit > 0 && !utf8.RuneStart(reason[limit]) {
		limit--
	}
	return reason[:limit] + "..."
}

// Apply authenticates its own input shape, selects operation/lifecycle/legacy
// semantics from the signed URI, and performs the only permitted settlement.
func (coordinator *AuthenticatedCallbackCoordinator) Apply(
	ctx context.Context,
	request hmacauth.VerifiedRequest,
) (CallbackResult, error) {
	if !coordinator.Valid() {
		return CallbackResult{}, ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.proofs.Accepts(request) {
		return CallbackResult{}, ErrCallbackProofBoundaryMismatch
	}
	callback, err := callbackwire.DecodeVerified(request)
	if err != nil {
		return CallbackResult{}, err
	}
	result := CallbackResult{observation: callback}
	if callback.Selector() == callbackwire.SelectorLifecycle {
		return coordinator.observeLifecycle(callback, result)
	}

	metadata, exists := coordinator.coordinator.Lookup(callback.LeaseUUID())
	if !exists {
		if callback.Selector() == callbackwire.SelectorLegacy {
			return coordinator.observeLifecycle(callback, result)
		}
		handled, recoveryResult, recoveryErr := coordinator.settleDurable(ctx, callback, result)
		if handled || recoveryErr != nil {
			recoveryResult.nonInFlight = true
			return recoveryResult, recoveryErr
		}
		result.nonInFlight = true
		return result, nil
	}
	if callback.Selector() != callbackwire.SelectorOperation ||
		callback.OperationID() != metadata.ID() {
		if callback.Selector() == callbackwire.SelectorLegacy {
			return coordinator.observeLifecycle(callback, result)
		}
		return result, nil
	}
	if err := coordinator.verifyStorage(callback, metadata.Backend()); err != nil {
		return result, err
	}
	result.authoritativeBackend = metadata.Backend()
	if metadata.Settlement() == operation.SettlementDeprovision {
		return coordinator.settleDeprovisionOwned(callback, metadata, result)
	}

	admission, observed, deprovisionOwned, err := coordinator.waitForClaim(ctx, metadata)
	if err != nil {
		result.claimWaitTimedOut = errors.Is(err, ErrCallbackClaimTimeout)
		return result, fmt.Errorf("wait to settle callback for lease %s: %w", callback.LeaseUUID(), err)
	}
	if deprovisionOwned {
		return coordinator.settleDeprovisionOwned(callback, observed, result)
	}
	if !admission.Claimed() {
		return result, nil
	}
	return coordinator.settleClaimed(ctx, callback, admission.Claim(), result)
}

func (coordinator *AuthenticatedCallbackCoordinator) waitForClaim(
	ctx context.Context,
	metadata operation.SettlementMetadata,
) (CallbackSettlementAdmission, operation.SettlementMetadata, bool, error) {
	if result := coordinator.coordinator.tryClaimCallback(metadata.LeaseUUID(), metadata.ID()); result.Claimed() {
		return result, operation.SettlementMetadata{}, false, nil
	} else if result.Outcome() != operation.SettlementBusy {
		return CallbackSettlementAdmission{}, operation.SettlementMetadata{}, false, result.Err()
	}
	started := time.Now()
	ticker := time.NewTicker(callbackClaimPollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(callbackClaimMaxWait)
	defer timer.Stop()
	for {
		current, exists := coordinator.coordinator.Lookup(metadata.LeaseUUID())
		if !exists || current.ID() != metadata.ID() {
			return CallbackSettlementAdmission{}, operation.SettlementMetadata{}, false, nil
		}
		if current.Settlement() == operation.SettlementDeprovision {
			return CallbackSettlementAdmission{}, current, true, nil
		}
		select {
		case <-ctx.Done():
			return CallbackSettlementAdmission{}, operation.SettlementMetadata{}, false, ctx.Err()
		case <-timer.C:
			return CallbackSettlementAdmission{}, operation.SettlementMetadata{}, false,
				fmt.Errorf("%w after %s", ErrCallbackClaimTimeout, time.Since(started))
		case <-ticker.C:
			claim := coordinator.coordinator.tryClaimCallback(metadata.LeaseUUID(), metadata.ID())
			if claim.Claimed() {
				return claim, operation.SettlementMetadata{}, false, nil
			}
			if claim.Outcome() != operation.SettlementBusy {
				return CallbackSettlementAdmission{}, operation.SettlementMetadata{}, false, claim.Err()
			}
		}
	}
}

func (coordinator *AuthenticatedCallbackCoordinator) settleClaimed(
	ctx context.Context,
	callback callbackwire.Observation,
	claim CallbackSettlementClaim,
	result CallbackResult,
) (CallbackResult, error) {
	finished := false
	defer func() {
		if !finished {
			coordinator.coordinator.releaseCallback(claim)
		}
	}()
	metadata := claim.Metadata()
	result.authoritativeBackend = metadata.Backend()
	result.operationKind = metadata.Kind()
	result.operationStartedAt = metadata.StartedAt()
	finishSuccess := func(publish bool) error {
		if err := coordinator.coordinator.finishConfirmedCallback(claim); err != nil {
			return err
		}
		finished = true
		result.operationOutcome = CallbackOperationSucceeded
		if publish {
			result.publish(backend.ProvisionStatusReady, "")
		}
		return nil
	}
	finishFailure := func(reason string, publish, deletePayload bool) error {
		var err error
		switch claim.Generation() {
		case OperationGenerationAttempt:
			err = coordinator.coordinator.finishRefusedCallback(claim)
		case OperationGenerationConfirmed:
			err = coordinator.coordinator.finishCallbackRetainingConfirmed(claim)
		default:
			err = ErrCallbackSettlementLost
		}
		if err != nil {
			return err
		}
		finished = true
		result.operationOutcome = CallbackOperationFailed
		result.deletePayload = deletePayload
		if publish {
			result.publish(backend.ProvisionStatusFailed, reason)
		}
		return nil
	}
	completedSuccess := func(publish bool) (CallbackResult, error) {
		err := finishSuccess(publish)
		return result, err
	}
	completedFailure := func(reason string, publish, deletePayload bool) (CallbackResult, error) {
		err := finishFailure(reason, publish, deletePayload)
		return result, err
	}

	switch callback.Status() {
	case backend.CallbackStatusSuccess:
		_, _, acknowledgeErr := coordinator.controlPlane.acknowledgeLease(ctx, callback.LeaseUUID())
		if acknowledgeErr == nil {
			return completedSuccess(true)
		}
		observation := coordinator.controlPlane.observeLeaseBounded(
			ctx, callback.LeaseUUID(), metadata.Tenant(),
		)
		if observationErr := exactLeaseObservationError(observation); observationErr != nil {
			return result, fmt.Errorf("%w: verify acknowledgement for lease %s after %w: %w", ErrCallbackAcknowledgeFailed, callback.LeaseUUID(), acknowledgeErr, observationErr)
		}
		switch classifyCallbackObservation(observation) {
		case callbackLeaseActive:
			return completedSuccess(true)
		case callbackLeaseTerminal:
			return completedSuccess(false)
		default:
			return result, fmt.Errorf("%w: lease %s is %s after acknowledgement error: %w", ErrCallbackAcknowledgeFailed, callback.LeaseUUID(), callbackObservationState(observation), acknowledgeErr)
		}

	case backend.CallbackStatusFailed:
		reason := callback.Failure()
		if reason == "" {
			reason = "provisioning failed"
		}
		observation := coordinator.controlPlane.observeLease(
			ctx, callback.LeaseUUID(), metadata.Tenant(),
		)
		if observationErr := exactLeaseObservationError(observation); observationErr != nil {
			return result, fmt.Errorf("failed to fetch lease %s: %w", callback.LeaseUUID(), observationErr)
		}
		switch classifyCallbackObservation(observation) {
		case callbackLeaseActive:
			return completedFailure(reason, true, false)
		case callbackLeaseTerminal:
			return completedFailure(reason, terminalCallbackObservationRepresentsFailure(observation), true)
		case callbackLeaseUnknown:
			return result, fmt.Errorf("lease %s has unknown state %s after failure callback", callback.LeaseUUID(), callbackObservationState(observation))
		}
		_, _, rejectErr := coordinator.controlPlane.rejectLease(
			ctx, callback.LeaseUUID(), callbackRejectReason(reason),
		)
		if rejectErr == nil {
			return completedFailure(reason, true, true)
		}
		current := coordinator.controlPlane.observeLeaseBounded(
			ctx, callback.LeaseUUID(), metadata.Tenant(),
		)
		if observationErr := exactLeaseObservationError(current); observationErr != nil {
			return result, fmt.Errorf("verify rejection for lease %s after %w: %w", callback.LeaseUUID(), rejectErr, observationErr)
		}
		switch classifyCallbackObservation(current) {
		case callbackLeaseActive:
			return completedFailure(reason, true, false)
		case callbackLeaseTerminal:
			return completedFailure(reason, terminalCallbackObservationRepresentsFailure(current), true)
		default:
			return result, fmt.Errorf("failed to reject lease %s: chain still reports state %s after rejection error: %w", callback.LeaseUUID(), callbackObservationState(current), rejectErr)
		}

	default:
		return result, fmt.Errorf("invalid operation callback status %q", callback.Status())
	}
}

func (coordinator *AuthenticatedCallbackCoordinator) settleDurable(
	ctx context.Context,
	callback callbackwire.Observation,
	result CallbackResult,
) (bool, CallbackResult, error) {
	// Recovery intentionally leaves operationOutcome at CallbackOperationNone.
	// The pre-restart metric observation may already have happened and durable
	// attempt metadata has no StartedAt, so counting here would both risk a
	// duplicate and manufacture an incomparable duration series.
	if callback.Selector() != callbackwire.SelectorOperation {
		return false, result, nil
	}
	if callback.Status() != backend.CallbackStatusSuccess && callback.Status() != backend.CallbackStatusFailed {
		return false, result, nil
	}
	admission := coordinator.coordinator.tryClaimRecoveryCallback(callback.LeaseUUID(), callback.OperationID())
	if !admission.Claimed() {
		if admission.Outcome() == RecoveryCallbackNotFound {
			return false, result, nil
		}
		if admission.Err() != nil {
			return true, result, admission.Err()
		}
		if admission.Outcome() == RecoveryCallbackBusy {
			return true, result, fmt.Errorf("%w for lease %s", ErrCallbackRecoveryBusy, callback.LeaseUUID())
		}
		return true, result, fmt.Errorf("%w for lease %s", ErrCallbackSettlementLost, callback.LeaseUUID())
	}
	claim := admission.Claim()
	finished := false
	defer func() {
		if !finished {
			coordinator.coordinator.releaseRecoveryCallback(claim)
		}
	}()
	result.authoritativeBackend = claim.Backend()
	request := claim.Metadata().RequestSnapshot()
	if !request.Valid() {
		return true, result, ErrCallbackSettlementLost
	}
	if err := coordinator.verifyStorage(callback, claim.Backend()); err != nil {
		return true, result, err
	}
	finishSuccess := func(publish bool) error {
		if err := coordinator.coordinator.finishConfirmedRecoveryCallback(claim); err != nil {
			return err
		}
		finished = true
		if publish {
			result.publish(backend.ProvisionStatusReady, "")
		}
		return nil
	}
	finishFailure := func(reason string, publish, deletePayload bool) error {
		var err error
		switch claim.Generation() {
		case OperationGenerationAttempt:
			err = coordinator.coordinator.finishRefusedRecoveryCallback(claim)
		case OperationGenerationConfirmed:
			err = coordinator.coordinator.finishRecoveryRetainingConfirmed(claim)
		default:
			err = ErrCallbackSettlementLost
		}
		if err != nil {
			return err
		}
		finished = true
		result.deletePayload = deletePayload
		if publish {
			result.publish(backend.ProvisionStatusFailed, reason)
		}
		return nil
	}
	completedSuccess := func(publish bool) (bool, CallbackResult, error) {
		err := finishSuccess(publish)
		return true, result, err
	}
	completedFailure := func(reason string, publish, deletePayload bool) (bool, CallbackResult, error) {
		err := finishFailure(reason, publish, deletePayload)
		return true, result, err
	}

	switch callback.Status() {
	case backend.CallbackStatusSuccess:
		_, _, acknowledgeErr := coordinator.controlPlane.acknowledgeLease(ctx, callback.LeaseUUID())
		if acknowledgeErr == nil {
			return completedSuccess(true)
		}
		observation := coordinator.controlPlane.observeLeaseBounded(
			ctx, callback.LeaseUUID(), request.Tenant(),
		)
		if observationErr := exactLeaseObservationError(observation); observationErr != nil {
			return true, result, fmt.Errorf("%w: verify acknowledgement for recovered lease %s after %w: %w", ErrCallbackAcknowledgeFailed, callback.LeaseUUID(), acknowledgeErr, observationErr)
		}
		switch classifyCallbackObservation(observation) {
		case callbackLeaseActive:
			return completedSuccess(true)
		case callbackLeaseTerminal:
			return completedSuccess(false)
		default:
			return true, result, fmt.Errorf("%w: recovered lease %s is %s after acknowledgement error: %w", ErrCallbackAcknowledgeFailed, callback.LeaseUUID(), callbackObservationState(observation), acknowledgeErr)
		}

	case backend.CallbackStatusFailed:
		reason := callback.Failure()
		if reason == "" {
			reason = "provisioning failed"
		}
		observation := coordinator.controlPlane.observeLease(
			ctx, callback.LeaseUUID(), request.Tenant(),
		)
		if observationErr := exactLeaseObservationError(observation); observationErr != nil {
			return true, result, observationErr
		}
		switch classifyCallbackObservation(observation) {
		case callbackLeaseActive:
			if claim.HasSameBackendOwner() {
				return completedFailure(reason, false, false)
			}
			return completedFailure(reason, true, false)
		case callbackLeaseTerminal:
			return completedFailure(reason, terminalCallbackObservationRepresentsFailure(observation), true)
		case callbackLeaseUnknown:
			return true, result, fmt.Errorf("recovered lease %s has unknown state %s after failure callback", callback.LeaseUUID(), callbackObservationState(observation))
		}
		_, _, rejectErr := coordinator.controlPlane.rejectLease(
			ctx, callback.LeaseUUID(), callbackRejectReason(reason),
		)
		if rejectErr == nil {
			return completedFailure(reason, true, true)
		}
		current := coordinator.controlPlane.observeLeaseBounded(
			ctx, callback.LeaseUUID(), request.Tenant(),
		)
		if observationErr := exactLeaseObservationError(current); observationErr != nil {
			return true, result, observationErr
		}
		switch classifyCallbackObservation(current) {
		case callbackLeaseActive:
			return completedFailure(reason, true, false)
		case callbackLeaseTerminal:
			return completedFailure(reason, terminalCallbackObservationRepresentsFailure(current), true)
		default:
			return true, result, fmt.Errorf("failed to reject recovered lease %s: chain still reports state %s after rejection error: %w", callback.LeaseUUID(), callbackObservationState(current), rejectErr)
		}
	}
	return false, result, nil
}

func (coordinator *AuthenticatedCallbackCoordinator) observeLifecycle(
	callback callbackwire.Observation,
	result CallbackResult,
) (CallbackResult, error) {
	result.nonInFlight = true
	result.lifecycleOutcome = CallbackLifecycleRetryable
	authorization := coordinator.coordinator.authorizeLifecycle(callback.LeaseUUID(), callback.LifecycleID())
	result.lifecycleVerdict = authorization.Verdict()
	switch authorization.Verdict() {
	case LifecycleVerdictAuthorized:
		result.authoritativeBackend = authorization.Backend()
	case LifecycleVerdictLegacy:
		if callback.Selector() != callbackwire.SelectorLegacy {
			result.lifecycleOutcome = CallbackLifecycleDropped
			return result, nil
		}
		result.authoritativeBackend = authorization.Backend()
	case LifecycleVerdictTeardownOnly:
		if callback.Status() != backend.CallbackStatusDeprovisioned {
			result.lifecycleOutcome = CallbackLifecycleDropped
			return result, nil
		}
		result.authoritativeBackend = authorization.Backend()
	case LifecycleVerdictRetired:
		result.authoritativeBackend = authorization.Backend()
		if err := coordinator.verifyStorage(callback, authorization.Backend()); err != nil {
			return result, err
		}
		result.lifecycleOutcome = CallbackLifecycleDropped
		return result, nil
	case LifecycleVerdictInvalid, LifecycleVerdictMissing,
		LifecycleVerdictStale, LifecycleVerdictUnusable:
		result.lifecycleOutcome = CallbackLifecycleDropped
		return result, nil
	default:
		return result, fmt.Errorf("unknown lifecycle authorization verdict %d", authorization.Verdict())
	}
	if err := coordinator.verifyStorage(callback, result.authoritativeBackend); err != nil {
		return result, err
	}
	if callback.Status() == backend.CallbackStatusDeprovisioned {
		retired, err := coordinator.coordinator.retireLifecycle(callback.LeaseUUID(), callback.LifecycleID())
		if err != nil {
			return result, err
		}
		result.lifecycleVerdict = retired.Verdict()
		if !retired.Retired() || !retired.RetiredNow() {
			result.lifecycleOutcome = CallbackLifecycleDropped
			return result, nil
		}
		result.authoritativeBackend = retired.Backend()
	}
	result.lifecycleOutcome = CallbackLifecycleApplied
	switch callback.Status() {
	case backend.CallbackStatusSuccess:
		result.publish(backend.ProvisionStatusReady, "")
	case backend.CallbackStatusFailed:
		result.publish(backend.ProvisionStatusFailed, callback.Failure())
	case backend.CallbackStatusDeprovisioned:
		if callback.Retained() {
			result.publish(backend.ProvisionStatusRetained, "")
		}
	default:
		result.lifecycleOutcome = CallbackLifecycleDropped
	}
	return result, nil
}

func (coordinator *AuthenticatedCallbackCoordinator) settleDeprovisionOwned(
	callback callbackwire.Observation,
	metadata operation.SettlementMetadata,
	result CallbackResult,
) (CallbackResult, error) {
	result.authoritativeBackend = metadata.Backend()
	if callback.Selector() == callbackwire.SelectorOperation {
		claim, claimed, err := coordinator.coordinator.tryClaimDeprovisionOwnedCallback(
			callback.LeaseUUID(), metadata.ID(),
		)
		if err != nil {
			return result, err
		}
		if !claimed {
			return result, ErrCallbackSettlementLost
		}
		consumed := false
		defer func() {
			if !consumed {
				coordinator.coordinator.releaseDeprovisionOwnedCallback(claim)
			}
		}()
		var applied bool
		switch callback.Status() {
		case backend.CallbackStatusSuccess:
			applied, err = coordinator.coordinator.confirmDeprovisionOwnedCallback(claim)
		case backend.CallbackStatusFailed:
			if claim.Generation() == OperationGenerationAttempt {
				applied, err = coordinator.coordinator.refuseDeprovisionOwnedCallback(claim)
			} else {
				applied, err = coordinator.coordinator.confirmDeprovisionOwnedCallback(claim)
			}
		default:
			return result, fmt.Errorf("invalid exact callback status %q during deprovision", callback.Status())
		}
		if err != nil || !applied {
			if err == nil {
				err = ErrCallbackSettlementLost
			}
			return result, err
		}
		consumed = true
	}

	switch callback.Status() {
	case backend.CallbackStatusDeprovisioned:
		result.deprovisionedBackend = metadata.Backend()
		if callback.Retained() {
			result.publish(backend.ProvisionStatusRetained, "")
		}
	case backend.CallbackStatusFailed:
		result.publish(backend.ProvisionStatusFailed, callback.Failure())
	case backend.CallbackStatusSuccess:
		result.deprovisionOwnedSuccess = true
	}
	return result, nil
}

func (coordinator *AuthenticatedCallbackCoordinator) verifyStorage(
	callback callbackwire.Observation,
	backendName string,
) error {
	if !callback.StorageID().Valid() {
		return fmt.Errorf("%w for lease %s backend %s", ErrCallbackStorageIdentityMissing, callback.LeaseUUID(), backendName)
	}
	expected, bound := coordinator.coordinator.ExpectedBackendStorageIdentity(backendName)
	if !bound || !expected.Valid() {
		return fmt.Errorf("%w for backend %s", ErrCallbackStorageIdentityUnbound, backendName)
	}
	if callback.StorageID() != expected {
		return fmt.Errorf("%w for lease %s backend %s: got %s, expected %s", ErrCallbackStorageIdentityMismatch, callback.LeaseUUID(), backendName, callback.StorageID(), expected)
	}
	return nil
}
