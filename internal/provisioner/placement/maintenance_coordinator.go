package placement

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/util"
)

var ErrInvalidMaintenanceCoordinator = errors.New("maintenance coordinator is invalid")

const maintenanceChainReadTimeout = 10 * time.Second

type maintenanceCoordinatorMarker struct{ _ byte }

// MaintenanceCoordinator is the construction-bound authority for maintenance
// command admission, chain reauthorization, backend-outcome classification,
// durable settlement, recovery, and its matching Registry lease exclusion.
// Its zero value is invalid and its components cannot be independently
// replaced.
type MaintenanceCoordinator struct {
	coordinator  *OperationCoordinator
	issuer       *operationCoordinatorMarker
	marker       *maintenanceCoordinatorMarker
	providerUUID string
	controlPlane *boundProviderControlPlane
	backends     backendRuntime
	payloads     MaintenancePayloadPersister
}

// MaintenanceCoordinator binds the chain reader and provider identity to the
// exact Store+Registry pair. Downstream services receive this purpose facet,
// never independently swappable chain and durable authorities.
func (execution *ExecutionCoordinator) MaintenanceCoordinator(
	payloads MaintenancePayloadPersister,
) (*MaintenanceCoordinator, error) {
	controlPlane, err := execution.providerControlPlane()
	if err != nil {
		return nil, ErrInvalidMaintenanceCoordinator
	}
	if util.IsNilInterface(payloads) {
		payloads = nil
	}
	return &MaintenanceCoordinator{
		coordinator: execution.coordinator, issuer: execution.issuer,
		marker:       &maintenanceCoordinatorMarker{},
		providerUUID: execution.coordinator.store.providerUUID, controlPlane: controlPlane,
		backends: execution.backends, payloads: payloads,
	}, nil
}

func (authority *MaintenanceCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil && authority.marker != nil &&
		authority.coordinator.Valid() && authority.issuer == authority.coordinator.marker &&
		authority.providerUUID != "" && authority.providerUUID == authority.coordinator.store.providerUUID &&
		authority.controlPlane != nil && authority.controlPlane.validFor(authority.coordinator.execution) &&
		!util.IsNilInterface(authority.backends)
}

func (authority *MaintenanceCoordinator) tryClaimLeaseNow(leaseUUID string) operation.LeaseClaimResult {
	if !authority.Valid() {
		return operation.LeaseClaimResult{}
	}
	return authority.coordinator.operations.TryClaimLeaseNow(leaseUUID)
}

func (authority *MaintenanceCoordinator) releaseLease(claim operation.LeaseClaim) bool {
	return authority.Valid() && authority.coordinator.operations.ReleaseLease(claim)
}

// MaintenanceAuthorizationOutcome is a read-only result of exact chain
// authorization. It is never accepted by a mutation method, so observing a
// terminal outcome does not grant the caller settlement authority.
type MaintenanceAuthorizationOutcome uint8

const (
	MaintenanceAuthorizationInvalid MaintenanceAuthorizationOutcome = iota
	MaintenanceAuthorizationGranted
	MaintenanceAuthorizationNotActive
	MaintenanceAuthorizationLeaseEnded
	MaintenanceAuthorizationRevoked
)

// MaintenancePreparation either carries one Store-minted command capability
// or a read-only chain rejection. Its fields are private so callers cannot
// combine a command with authority observed from another lease/coordinator.
type MaintenancePreparation struct {
	issuer   *maintenanceCoordinatorMarker
	prepared PreparedMaintenanceCommand
	outcome  MaintenanceAuthorizationOutcome
	err      error
}

func (result MaintenancePreparation) Authorized() bool {
	return result.issuer != nil && result.outcome == MaintenanceAuthorizationGranted &&
		result.err == nil && result.prepared.Valid()
}

func (result MaintenancePreparation) Command() MaintenanceCommand {
	if !result.Authorized() {
		return MaintenanceCommand{}
	}
	return result.prepared.Command()
}

func (result MaintenancePreparation) Outcome() MaintenanceAuthorizationOutcome { return result.outcome }
func (result MaintenancePreparation) Err() error                               { return result.err }

func (authority *MaintenanceCoordinator) prepareMaintenanceCommand(
	ctx context.Context,
	id maintenanceid.ID,
	leaseUUID, tenant string,
	kind MaintenanceCommandKind,
	payload []byte,
) MaintenancePreparation {
	if !authority.Valid() {
		return MaintenancePreparation{err: ErrInvalidMaintenanceCoordinator}
	}
	outcome, err := authority.authorizeCurrentLease(ctx, leaseUUID, tenant)
	if err != nil || outcome != MaintenanceAuthorizationGranted {
		return MaintenancePreparation{issuer: authority.marker, outcome: outcome, err: err}
	}
	prepared, err := authority.coordinator.store.prepareMaintenanceCommand(id, leaseUUID, kind, payload)
	if err != nil {
		return MaintenancePreparation{issuer: authority.marker, err: err}
	}
	command := prepared.Command()
	if command.Tenant() != tenant || command.ProviderUUID() != authority.providerUUID ||
		command.LeaseUUID() != leaseUUID {
		return MaintenancePreparation{
			issuer: authority.marker, outcome: MaintenanceAuthorizationRevoked,
		}
	}
	if _, err := exactBackend(authority.backends, command.BackendName()); err != nil {
		return MaintenancePreparation{issuer: authority.marker, err: err}
	}
	if command.Kind() == MaintenanceCommandUpdate && authority.payloads == nil {
		return MaintenancePreparation{
			issuer: authority.marker,
			err:    errors.New("update payload persister is unavailable"),
		}
	}
	return MaintenancePreparation{
		issuer: authority.marker, prepared: prepared,
		outcome: MaintenanceAuthorizationGranted,
	}
}

func (authority *MaintenanceCoordinator) authorizeCurrentLease(
	ctx context.Context,
	leaseUUID, tenant string,
) (MaintenanceAuthorizationOutcome, error) {
	readCtx, cancel := context.WithTimeout(ctx, maintenanceChainReadTimeout)
	defer cancel()
	switch observation := authority.controlPlane.observeLease(readCtx, leaseUUID, tenant).(type) {
	case observedLeaseUnauthorized:
		return MaintenanceAuthorizationRevoked, nil
	case observedLeaseUnknown:
		return MaintenanceAuthorizationInvalid, observation.err
	case observedExactLease:
		switch observation.lease.State {
		case billingtypes.LEASE_STATE_ACTIVE:
			return MaintenanceAuthorizationGranted, nil
		case billingtypes.LEASE_STATE_PENDING:
			return MaintenanceAuthorizationNotActive, nil
		case billingtypes.LEASE_STATE_CLOSED,
			billingtypes.LEASE_STATE_REJECTED,
			billingtypes.LEASE_STATE_EXPIRED:
			return MaintenanceAuthorizationLeaseEnded, nil
		default:
			return MaintenanceAuthorizationInvalid,
				errors.New("lease has an unknown state during maintenance authorization")
		}
	default:
		return MaintenanceAuthorizationInvalid, ErrInvalidMaintenanceCoordinator
	}
}

// AuthorizedMaintenanceCommand is chain authority for exactly one still-
// pending durable command. Only ReauthorizeMaintenanceCommand can mint it.
type AuthorizedMaintenanceCommand struct {
	issuer *maintenanceCoordinatorMarker
	claim  MaintenanceCommandClaim
	state  *maintenanceAuthorizationState
}

type maintenanceAuthorizationState struct{ consumed atomic.Bool }

func (authorization AuthorizedMaintenanceCommand) Valid() bool {
	return authorization.issuer != nil && authorization.claim.Valid() &&
		authorization.state != nil && !authorization.state.consumed.Load()
}

func (authorization AuthorizedMaintenanceCommand) Command() MaintenanceCommand {
	if !authorization.Valid() {
		return MaintenanceCommand{}
	}
	return authorization.claim.Command()
}

// MaintenanceReauthorization either carries current chain authority for the
// exact pending command, records a coordinator-selected terminal outcome, or
// reports uncertainty without mutating the Pending record.
type MaintenanceReauthorization struct {
	issuer        *maintenanceCoordinatorMarker
	authorization AuthorizedMaintenanceCommand
	outcome       MaintenanceCommandOutcome
	err           error
}

func (result MaintenanceReauthorization) Authorized() bool {
	return result.issuer != nil && result.err == nil &&
		result.outcome == MaintenanceOutcomePending &&
		result.authorization.Valid() && result.authorization.issuer == result.issuer
}

func (result MaintenanceReauthorization) Authorization() AuthorizedMaintenanceCommand {
	if !result.Authorized() {
		return AuthorizedMaintenanceCommand{}
	}
	return result.authorization
}

func (result MaintenanceReauthorization) Settled() bool {
	return result.issuer != nil && result.err == nil &&
		result.outcome != MaintenanceOutcomePending && !result.authorization.Valid()
}

func (result MaintenanceReauthorization) Outcome() MaintenanceCommandOutcome {
	if !result.Settled() {
		return MaintenanceOutcomePending
	}
	return result.outcome
}

func (result MaintenanceReauthorization) Err() error { return result.err }

func (authority *MaintenanceCoordinator) reauthorizeMaintenanceCommand(
	ctx context.Context,
	claim MaintenanceCommandClaim,
) MaintenanceReauthorization {
	if !authority.Valid() || !claim.Valid() || claim.issuer != authority.coordinator.store {
		return MaintenanceReauthorization{err: ErrInvalidMaintenanceCoordinator}
	}
	command := claim.Command()
	authorizationOutcome, err := authority.authorizeCurrentLease(
		ctx, command.LeaseUUID(), command.Tenant(),
	)
	if err != nil {
		return MaintenanceReauthorization{issuer: authority.marker, err: err}
	}
	var terminal MaintenanceCommandOutcome
	switch authorizationOutcome {
	case MaintenanceAuthorizationGranted:
		if command.ProviderUUID() != authority.providerUUID {
			terminal = MaintenanceOutcomeAuthorityRevoked
		} else if err := authority.coordinator.store.reauthorizeMaintenanceCommand(claim); err != nil {
			return MaintenanceReauthorization{issuer: authority.marker, err: err}
		} else {
			return MaintenanceReauthorization{
				issuer: authority.marker,
				authorization: AuthorizedMaintenanceCommand{
					issuer: authority.marker, claim: claim,
					state: &maintenanceAuthorizationState{},
				},
			}
		}
	case MaintenanceAuthorizationNotActive:
		return MaintenanceReauthorization{
			issuer: authority.marker,
			err:    errors.New("pending maintenance lease has not reached an active state"),
		}
	case MaintenanceAuthorizationLeaseEnded:
		terminal = MaintenanceOutcomeLeaseEnded
	case MaintenanceAuthorizationRevoked:
		terminal = MaintenanceOutcomeAuthorityRevoked
	default:
		return MaintenanceReauthorization{
			issuer: authority.marker,
			err:    errors.New("invalid maintenance chain authorization outcome"),
		}
	}
	if err := authority.settleMaintenanceCommand(claim, terminal); err != nil {
		return MaintenanceReauthorization{issuer: authority.marker, err: err}
	}
	return MaintenanceReauthorization{
		issuer: authority.marker, outcome: terminal,
	}
}

// MaintenanceCall proves both current chain authority and a final exact local
// aggregate check immediately before backend dispatch. A claim or decoded
// record alone cannot be presented for backend-outcome settlement.
type maintenanceCall struct {
	issuer *maintenanceCoordinatorMarker
	claim  MaintenanceCommandClaim
	state  *maintenanceCallState
}

type maintenanceCallState struct{ consumed atomic.Bool }

func (call maintenanceCall) valid() bool {
	return call.issuer != nil && call.claim.Valid() && call.state != nil &&
		!call.state.consumed.Load()
}

func (authority *MaintenanceCoordinator) beginMaintenanceCall(
	authorization AuthorizedMaintenanceCommand,
) (maintenanceCall, error) {
	if !authority.Valid() || !authorization.Valid() ||
		authorization.issuer != authority.marker ||
		authorization.claim.issuer != authority.coordinator.store {
		return maintenanceCall{}, ErrInvalidMaintenanceCoordinator
	}
	if !authorization.state.consumed.CompareAndSwap(false, true) {
		return maintenanceCall{}, ErrInvalidMaintenanceCoordinator
	}
	if err := authority.coordinator.store.reauthorizeMaintenanceCommand(authorization.claim); err != nil {
		return maintenanceCall{}, err
	}
	return maintenanceCall{
		issuer: authority.marker, claim: authorization.claim,
		state: &maintenanceCallState{},
	}, nil
}

// MaintenanceCompletion reports whether the coordinator could derive and
// durably record a terminal outcome from the observed call result. Ambiguous
// errors deliberately leave the exact command Pending.
type MaintenanceCompletion struct {
	outcome  MaintenanceCommandOutcome
	callErr  error
	err      error
	accepted bool
}

func (result MaintenanceCompletion) Settled() bool {
	return result.err == nil && result.outcome != MaintenanceOutcomePending
}
func (result MaintenanceCompletion) Outcome() MaintenanceCommandOutcome {
	if !result.Settled() {
		return MaintenanceOutcomePending
	}
	return result.outcome
}
func (result MaintenanceCompletion) Err() error            { return result.err }
func (result MaintenanceCompletion) CallErr() error        { return result.callErr }
func (result MaintenanceCompletion) BackendAccepted() bool { return result.accepted }

func (authority *MaintenanceCoordinator) completeMaintenanceCall(
	call maintenanceCall,
	observed backend.MaintenanceCallOutcome,
) MaintenanceCompletion {
	callErr := observed.Err()
	if !authority.Valid() || !call.valid() || call.issuer != authority.marker ||
		call.claim.issuer != authority.coordinator.store {
		return MaintenanceCompletion{callErr: callErr, err: ErrInvalidMaintenanceCoordinator}
	}
	if !call.state.consumed.CompareAndSwap(false, true) {
		return MaintenanceCompletion{callErr: callErr, err: ErrInvalidMaintenanceCoordinator}
	}
	outcome, definitive := classifyMaintenanceCall(observed)
	if !definitive {
		return MaintenanceCompletion{callErr: callErr}
	}
	if err := authority.settleMaintenanceCommand(call.claim, outcome); err != nil {
		return MaintenanceCompletion{callErr: callErr, err: err, accepted: outcome == MaintenanceOutcomeAccepted}
	}
	return MaintenanceCompletion{outcome: outcome, callErr: callErr, accepted: outcome == MaintenanceOutcomeAccepted}
}

// ExecuteMaintenance is the only backend execution boundary for an authorized
// command. The backend, request, observed return, payload persistence, and
// settlement classifier cannot be supplied independently by callers.
func (authority *MaintenanceCoordinator) executeMaintenance(
	ctx context.Context,
	authorization AuthorizedMaintenanceCommand,
) MaintenanceCompletion {
	if !authority.Valid() || !authorization.Valid() || authorization.issuer != authority.marker {
		return MaintenanceCompletion{err: ErrInvalidMaintenanceCoordinator}
	}
	command := authorization.Command()
	client, err := exactBackend(authority.backends, command.BackendName())
	if err != nil {
		return MaintenanceCompletion{err: err}
	}
	call, err := authority.beginMaintenanceCall(authorization)
	if err != nil {
		return MaintenanceCompletion{err: err}
	}
	callOutcome := invokeMaintenance(ctx, client, command)
	if !callOutcome.Accepted() {
		return authority.completeMaintenanceCall(call, callOutcome)
	}
	if command.Kind() == MaintenanceCommandUpdate {
		if authority.payloads == nil {
			call.state.consumed.CompareAndSwap(false, true)
			return MaintenanceCompletion{
				accepted: true, err: errors.New("update payload persister is unavailable"),
			}
		}
		if err := authority.payloads.OverwritePayload(command.LeaseUUID(), command.Payload()); err != nil {
			call.state.consumed.CompareAndSwap(false, true)
			return MaintenanceCompletion{
				accepted: true, err: fmt.Errorf("persist accepted update payload: %w", err),
			}
		}
	}
	return authority.completeMaintenanceCall(call, callOutcome)
}

func classifyMaintenanceCall(observed backend.MaintenanceCallOutcome) (MaintenanceCommandOutcome, bool) {
	switch {
	case observed.Accepted():
		return MaintenanceOutcomeAccepted, true
	case observed.NotDispatched():
		return MaintenanceOutcomeBackendUnavailable, true
	case observed.Refused():
		switch observed.Refusal() {
		case backend.MaintenanceRefusalNotProvisioned:
			return MaintenanceOutcomeNotProvisioned, true
		case backend.MaintenanceRefusalInvalidState:
			return MaintenanceOutcomeInvalidState, true
		case backend.MaintenanceRefusalValidation:
			return MaintenanceOutcomeValidationRejected, true
		case backend.MaintenanceRefusalCapacity:
			return MaintenanceOutcomeCapacityRefused, true
		default:
			return MaintenanceOutcomePending, false
		}
	default:
		return MaintenanceOutcomePending, false
	}
}

func (authority *MaintenanceCoordinator) settleMaintenanceCommand(
	claim MaintenanceCommandClaim,
	outcome MaintenanceCommandOutcome,
) error {
	// outcome is derived only by the closed chain/backend classifiers above;
	// no public method accepts it as caller-selected settlement input.
	return authority.coordinator.store.settleMaintenanceCommand(claim, outcome)
}

func (authority *MaintenanceCoordinator) lookupMaintenanceCommand(
	leaseUUID string,
	id maintenanceid.ID,
) (MaintenanceCommandRecord, bool, error) {
	if !authority.Valid() {
		return MaintenanceCommandRecord{}, false, ErrInvalidMaintenanceCoordinator
	}
	return authority.coordinator.store.LookupMaintenanceCommand(leaseUUID, id)
}

func (authority *MaintenanceCoordinator) beginMaintenanceCommand(
	preparation MaintenancePreparation,
) (MaintenanceCommandAdmission, error) {
	if !authority.Valid() || !preparation.Authorized() ||
		preparation.issuer != authority.marker ||
		preparation.prepared.issuer != authority.coordinator.store {
		return MaintenanceCommandAdmission{}, ErrInvalidMaintenanceCoordinator
	}
	return authority.coordinator.store.beginMaintenanceCommand(preparation.prepared)
}

func (authority *MaintenanceCoordinator) pendingMaintenanceCommands() ([]MaintenanceCommandClaim, error) {
	if !authority.Valid() {
		return nil, ErrInvalidMaintenanceCoordinator
	}
	return authority.coordinator.store.pendingMaintenanceCommands()
}
