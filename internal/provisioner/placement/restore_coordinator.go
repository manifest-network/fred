package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/leaseitems"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/util"
)

var ErrInvalidRestoreCoordinator = errors.New("restore coordinator is invalid")

const restoreLeaseReadTimeout = 10 * time.Second

// RestoreApplicationDisposition is the closed result of the construction-bound
// restore transaction. The zero value is invalid.
type RestoreApplicationDisposition uint8

const (
	RestoreApplicationInvalid RestoreApplicationDisposition = iota
	RestoreApplicationAccepted
	RestoreApplicationTargetNotPending
	RestoreApplicationSourceNotFound
	RestoreApplicationSourceUnavailable
	RestoreApplicationAlreadyInProgress
	RestoreApplicationServiceUnavailable
	RestoreApplicationNotRetained
	RestoreApplicationBackendRejected
)

// RestoreApplicationRequest carries only authenticated identities. It cannot
// carry a chain lease, backend, callback destination, or settlement outcome.
type RestoreApplicationRequest struct {
	targetLeaseUUID string
	tenant          string
	sourceLeaseUUID string
}

func NewRestoreApplicationRequest(
	targetLeaseUUID, tenant, sourceLeaseUUID string,
) (RestoreApplicationRequest, error) {
	if targetLeaseUUID == "" || tenant == "" || sourceLeaseUUID == "" ||
		targetLeaseUUID == sourceLeaseUUID {
		return RestoreApplicationRequest{}, errors.New("restore request is incomplete")
	}
	return RestoreApplicationRequest{
		targetLeaseUUID: targetLeaseUUID, tenant: tenant,
		sourceLeaseUUID: sourceLeaseUUID,
	}, nil
}

func (request RestoreApplicationRequest) valid() bool {
	return request.targetLeaseUUID != "" && request.tenant != "" &&
		request.sourceLeaseUUID != "" && request.targetLeaseUUID != request.sourceLeaseUUID
}

// RestoreApplicationResult exposes observations only. Physical outcome and
// durable settlement are derived and applied inside RestoreCoordinator.
type RestoreApplicationResult struct {
	disposition   RestoreApplicationDisposition
	backendName   string
	callErr       error
	err           error
	refusal       backend.RestoreRefusal
	notDispatched bool
}

func (result RestoreApplicationResult) Disposition() RestoreApplicationDisposition {
	return result.disposition
}
func (result RestoreApplicationResult) BackendName() string { return result.backendName }
func (result RestoreApplicationResult) CallErr() error      { return result.callErr }
func (result RestoreApplicationResult) Err() error          { return result.err }
func (result RestoreApplicationResult) DefinitivelyRefused() bool {
	return result.refusal != backend.RestoreRefusalNone
}
func (result RestoreApplicationResult) Refusal() backend.RestoreRefusal {
	return result.refusal
}
func (result RestoreApplicationResult) NotDispatched() bool { return result.notDispatched }

// RestoreCoordinator is the construction-bound authority for one restore
// application service. It closes over one exact Store/Registry pair, so lease
// exclusion, initiation, durable source/target admission, and synchronous
// settlement cannot be assembled from unrelated collaborators. Its zero value
// is invalid.
type RestoreCoordinator struct {
	coordinator  *OperationCoordinator
	issuer       *operationCoordinatorMarker
	backends     backendRuntime
	providerUUID string
	controlPlane *boundProviderControlPlane
	callbacks    *CallbackRouteFactory
	observe      RestoreStartObserver
}

func (execution *ExecutionCoordinator) RestoreCoordinator(
	observe RestoreStartObserver,
) (*RestoreCoordinator, error) {
	controlPlane, err := execution.providerControlPlane()
	if err != nil {
		return nil, ErrInvalidRestoreCoordinator
	}
	return &RestoreCoordinator{
		coordinator: execution.coordinator, issuer: execution.issuer,
		backends:     execution.backends,
		providerUUID: execution.coordinator.store.providerUUID,
		controlPlane: controlPlane, callbacks: execution.callbacks, observe: observe,
	}, nil
}

func (authority *RestoreCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil &&
		authority.coordinator.Valid() && authority.issuer == authority.coordinator.marker &&
		!util.IsNilInterface(authority.backends) && authority.providerUUID != "" &&
		authority.controlPlane != nil && authority.controlPlane.validFor(authority.coordinator.execution) &&
		authority.callbacks.Valid()
}

// ReadLease uses the exact chain reader bound beside this coordinator's
// Store/Registry/backend aggregate.
func (authority *RestoreCoordinator) readLease(
	ctx context.Context,
	leaseUUID, expectedTenant string,
) exactLeaseObservation {
	if !authority.Valid() || leaseUUID == "" {
		return observedLeaseUnknown{err: ErrInvalidRestoreCoordinator}
	}
	readCtx, cancel := context.WithTimeout(ctx, restoreLeaseReadTimeout)
	defer cancel()
	return authority.controlPlane.observeLease(readCtx, leaseUUID, expectedTenant)
}

// ExecuteApplication owns restore authorization, exact source/target
// exclusion, durable write-ahead admission, backend invocation, and settlement.
// No intermediate claim, route, dispatch token, or caller-selected outcome
// crosses this boundary.
func (authority *RestoreCoordinator) ExecuteApplication(
	ctx context.Context,
	request RestoreApplicationRequest,
) RestoreApplicationResult {
	if !authority.Valid() || !request.valid() {
		return RestoreApplicationResult{
			disposition: RestoreApplicationInvalid,
			err:         errors.New("invalid restore application authority"),
		}
	}

	sourceObservation := authority.readLease(
		ctx, request.sourceLeaseUUID, request.tenant,
	)
	switch observed := sourceObservation.(type) {
	case observedExactLease:
		if observed.lease.State != billingtypes.LEASE_STATE_CLOSED {
			return RestoreApplicationResult{disposition: RestoreApplicationNotRetained}
		}
	case observedLeaseUnauthorized:
		return RestoreApplicationResult{
			disposition: RestoreApplicationSourceNotFound,
			err:         errors.New("restore source is not owned by the authenticated tenant and provider"),
		}
	case observedLeaseUnknown, observedLeaseNotFound:
		return RestoreApplicationResult{
			disposition: RestoreApplicationSourceUnavailable,
			err:         fmt.Errorf("read restore source lease: %w", exactLeaseObservationError(observed)),
		}
	default:
		return RestoreApplicationResult{
			disposition: RestoreApplicationSourceUnavailable,
			err:         ErrInvalidRestoreCoordinator,
		}
	}

	source := authority.coordinator.store.Lookup(request.sourceLeaseUUID)
	if source.State() == StateAbsent {
		return RestoreApplicationResult{disposition: RestoreApplicationSourceNotFound}
	}
	if source.State() != StateConfirmed || source.Backend == "" ||
		source.Attempt != "" || !source.RecordRevision().Valid() {
		return RestoreApplicationResult{
			disposition: RestoreApplicationSourceUnavailable,
			err:         ErrRestoreSourceUnavailable,
		}
	}
	firstUUID, secondUUID := request.sourceLeaseUUID, request.targetLeaseUUID
	firstIsSource := true
	if secondUUID < firstUUID {
		firstUUID, secondUUID = secondUUID, firstUUID
		firstIsSource = false
	}
	first := authority.coordinator.operations.TryClaimLeaseNow(firstUUID)
	if !first.Acquired() {
		return restoreClaimApplicationFailure(first.Outcome())
	}
	second := authority.coordinator.operations.TryClaimLeaseNow(secondUUID)
	if !second.Acquired() {
		if !authority.coordinator.operations.ReleaseLease(first.Claim()) {
			slog.Error("failed to release partial restore lease claim", "lease_uuid", firstUUID)
		}
		return restoreClaimApplicationFailure(second.Outcome())
	}
	var sourceClaim, targetClaim operation.LeaseClaim
	if firstIsSource {
		sourceClaim, targetClaim = first.Claim(), second.Claim()
	} else {
		targetClaim, sourceClaim = first.Claim(), second.Claim()
	}
	defer func() {
		if !authority.coordinator.operations.ReleaseLease(targetClaim) {
			slog.Error("failed to release restore target lease claim", "lease_uuid", request.targetLeaseUUID)
		}
		if !authority.coordinator.operations.ReleaseLease(sourceClaim) {
			slog.Error("failed to release restore source lease claim", "lease_uuid", request.sourceLeaseUUID)
		}
	}()

	targetObservation := authority.readLease(
		ctx, request.targetLeaseUUID, request.tenant,
	)
	var targetLease *billingtypes.Lease
	switch target := targetObservation.(type) {
	case observedExactLease:
		targetLease = &target.lease
	case observedLeaseUnauthorized:
		return RestoreApplicationResult{
			disposition: RestoreApplicationInvalid,
			err:         errors.New("restore request is not authorized for current target lease"),
		}
	case observedLeaseUnknown, observedLeaseNotFound:
		return RestoreApplicationResult{
			disposition: RestoreApplicationServiceUnavailable,
			err:         fmt.Errorf("read restore target lease: %w", exactLeaseObservationError(target)),
		}
	default:
		return RestoreApplicationResult{
			disposition: RestoreApplicationServiceUnavailable,
			err:         ErrInvalidRestoreCoordinator,
		}
	}
	if targetLease.State != billingtypes.LEASE_STATE_PENDING {
		return RestoreApplicationResult{disposition: RestoreApplicationTargetNotPending}
	}
	items := leaseitems.FromLease(targetLease)
	restoreInitiation, err := operation.NewRestoreInitiation(
		request.targetLeaseUUID, request.tenant, items,
	)
	if err != nil {
		return RestoreApplicationResult{
			disposition: RestoreApplicationServiceUnavailable, err: err,
		}
	}
	initiated := authority.coordinator.operations.TryInitiateRestoreClaimed(
		targetClaim, restoreInitiation,
	)
	if !initiated.Started() {
		if initiated.Outcome() == operation.TrackBusy {
			return RestoreApplicationResult{disposition: RestoreApplicationAlreadyInProgress}
		}
		return RestoreApplicationResult{
			disposition: RestoreApplicationServiceUnavailable,
			err:         fmt.Errorf("register claimed restore operation: outcome %d", initiated.Outcome()),
		}
	}
	initiation := initiated.Capability()
	abortBeforePlacement := func(cause error) RestoreApplicationResult {
		completion := authority.coordinator.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
			slog.Error("failed to abort restore initiation before placement admission",
				"lease_uuid", request.targetLeaseUUID, "completion", completion)
		}
		return RestoreApplicationResult{
			disposition: RestoreApplicationServiceUnavailable, err: cause,
		}
	}
	requestSnapshot, err := authority.coordinator.store.MintBackendRequestSnapshot(
		targetLease.Tenant, items,
	)
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("bind exact restore backend request: %w", err))
	}
	callbackPair, err := authority.callbacks.ForOperation(initiation.ID())
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("mint restore callback routes: %w", err))
	}
	dispatch, backendName, err := authority.coordinator.admitRestoreDispatch(
		initiation,
		authority.coordinator.store.CurrentAdmissionBaseline(), source.RecordRevision(),
		request.targetLeaseUUID, requestSnapshot, callbackPair,
	)
	if err != nil {
		failure := restoreAdmissionApplicationFailure(err)
		failure.backendName = backendName
		return failure
	}
	settlement := authority.execute(ctx, dispatch)
	if settlement.CallAccepted() || settlement.Superseded() {
		return RestoreApplicationResult{
			disposition: RestoreApplicationAccepted, backendName: backendName,
			err: settlement.Err(),
		}
	}
	callErr := settlement.CallErr()
	if callErr == nil {
		callErr = settlement.Err()
	}
	if settlement.CallErr() == nil && settlement.Err() != nil {
		failure := restoreAdmissionApplicationFailure(settlement.Err())
		failure.backendName = backendName
		return failure
	}
	return RestoreApplicationResult{
		disposition: RestoreApplicationBackendRejected,
		backendName: backendName, callErr: callErr, err: settlement.Err(),
		refusal: settlement.RestoreRefusal(), notDispatched: settlement.CallNotDispatched(),
	}
}

func restoreClaimApplicationFailure(outcome operation.LeaseClaimOutcome) RestoreApplicationResult {
	if outcome == operation.LeaseClaimBusy {
		return RestoreApplicationResult{disposition: RestoreApplicationAlreadyInProgress}
	}
	return RestoreApplicationResult{
		disposition: RestoreApplicationServiceUnavailable,
		err:         fmt.Errorf("claim restore lifecycle leases: outcome %d", outcome),
	}
}

func restoreAdmissionApplicationFailure(err error) RestoreApplicationResult {
	disposition := RestoreApplicationServiceUnavailable
	switch {
	case errors.Is(err, ErrRestoreSourceNotFound):
		disposition = RestoreApplicationSourceNotFound
	case errors.Is(err, ErrRestoreSourceClaimed),
		errors.Is(err, ErrRestoreTargetUnavailable),
		errors.Is(err, ErrAttemptConflict):
		disposition = RestoreApplicationAlreadyInProgress
	case errors.Is(err, ErrRestoreSourceUnavailable):
		disposition = RestoreApplicationSourceUnavailable
	}
	return RestoreApplicationResult{disposition: disposition, err: err}
}

func (authority *RestoreCoordinator) execute(
	ctx context.Context,
	dispatch RestoreDispatch,
) DispatchResult {
	if !authority.Valid() {
		return DispatchResult{err: ErrInvalidRestoreCoordinator}
	}
	if !dispatch.Valid() || dispatch.issuer != authority.issuer {
		return DispatchResult{disposition: DispatchInvalid, err: ErrInvalidRestoreCoordinator}
	}
	claim := dispatch.restore
	backendClient, err := exactBackend(authority.backends, claim.backendName)
	if err != nil {
		return authority.abortInvalidRestore(dispatch, err)
	}
	bound, boundOK := authority.coordinator.bindRestoreBackend(dispatch)
	if !boundOK {
		return authority.abortInvalidRestore(dispatch, errors.New("restore dispatch did not bind its backend"))
	}
	call, calling := authority.coordinator.beginRestoreCall(bound)
	if !calling {
		return authority.abortInvalidRestore(dispatch, errors.New("restore dispatch did not enter calling phase"))
	}
	requestSnapshot := claim.requestSnapshot
	request := backend.RestoreRequest{
		LeaseUUID: claim.targetLeaseUUID, FromLeaseUUID: claim.sourceLeaseUUID,
		Tenant: requestSnapshot.Tenant(), ProviderUUID: requestSnapshot.ProviderUUID(),
		Items:                requestSnapshot.Items(),
		CallbackURL:          claim.callbackPair.OperationURL(),
		LifecycleCallbackURL: claim.callbackPair.LifecycleURL(),
	}
	observeRestoreStart(authority.observe, claim.targetLeaseUUID, claim.backendName)
	return authority.coordinator.completeRestore(call, invokeRestore(ctx, backendClient, request))
}

func (authority *RestoreCoordinator) abortInvalidRestore(
	dispatch RestoreDispatch,
	cause error,
) DispatchResult {
	result := authority.coordinator.abortRestoreDispatch(dispatch)
	if result.err != nil {
		result.err = errors.Join(cause, result.err)
	} else {
		result.err = cause
	}
	return result
}
