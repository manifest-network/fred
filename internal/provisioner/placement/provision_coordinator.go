package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/leaseitems"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/util"
)

var ErrProvisionRouteUnresolvable = errors.New("provision route is unresolvable")

const provisionLeaseReadTimeout = 10 * time.Second

// ProvisionEventDisposition is the closed behavior projection returned by
// ExecuteCurrentLease. The zero value is invalid and never means success.
type ProvisionEventDisposition uint8

const (
	ProvisionEventInvalid ProvisionEventDisposition = iota
	ProvisionEventStarted
	ProvisionEventDuplicate
	ProvisionEventAwaitingPayload
	ProvisionEventLeaseTerminal
	ProvisionEventLeaseActive
	ProvisionEventPayloadUnavailable
	ProvisionEventPayloadInvalid
	ProvisionEventValidationRefused
	ProvisionEventNoBackend
	ProvisionEventUncertain
	ProvisionEventRejected
)

type provisionEventKind uint8

const (
	provisionEventCreated provisionEventKind = iota + 1
	provisionEventPayload
)

// ProvisionEventRequest is an opaque event intent. Callers can supply only
// identity and payload bytes; authoritative lease state and every mutation
// capability are derived internally.
type ProvisionEventRequest struct {
	kind           provisionEventKind
	leaseUUID      string
	expectedTenant string
	loadPayload    func() ([]byte, error)
}

func NewProvisionEventRequest(leaseUUID, expectedTenant string) (ProvisionEventRequest, error) {
	if leaseUUID == "" || expectedTenant == "" {
		return ProvisionEventRequest{}, errors.New("provision event identity is incomplete")
	}
	return ProvisionEventRequest{
		kind: provisionEventCreated, leaseUUID: leaseUUID, expectedTenant: expectedTenant,
	}, nil
}

func NewPayloadProvisionEventRequest(
	leaseUUID, expectedTenant string,
	loadPayload func() ([]byte, error),
) (ProvisionEventRequest, error) {
	if leaseUUID == "" || expectedTenant == "" || loadPayload == nil {
		return ProvisionEventRequest{}, errors.New("payload provision event is incomplete")
	}
	return ProvisionEventRequest{
		kind: provisionEventPayload, leaseUUID: leaseUUID,
		expectedTenant: expectedTenant, loadPayload: loadPayload,
	}, nil
}

func (request ProvisionEventRequest) valid() bool {
	return request.leaseUUID != "" && request.expectedTenant != "" &&
		(request.kind == provisionEventCreated ||
			(request.kind == provisionEventPayload && request.loadPayload != nil))
}

// ProvisionEventResult is an opaque, nil-free result algebra. Lease-derived
// data is exposed only through behavior-specific accessors.
type ProvisionEventResult struct {
	disposition ProvisionEventDisposition
	lease       billingtypes.Lease
	hasLease    bool
	err         error
}

func (result ProvisionEventResult) Disposition() ProvisionEventDisposition {
	return result.disposition
}

func (result ProvisionEventResult) Err() error { return result.err }

// RejectionReason is an observational result, never permission to reject or
// remove payloads. Rejection is positively observed under the lease claim;
// Err still reports a retryable payload cleanup failure independently.
func (result ProvisionEventResult) RejectionReason() string {
	if result.disposition != ProvisionEventRejected {
		return ""
	}
	return result.lease.RejectionReason
}

func (result ProvisionEventResult) LeaseState() (billingtypes.LeaseState, bool) {
	if !result.hasLease {
		return billingtypes.LEASE_STATE_UNSPECIFIED, false
	}
	return result.lease.State, true
}

func (result ProvisionEventResult) MetaHashHex() (string, bool) {
	if !result.hasLease || result.disposition != ProvisionEventAwaitingPayload {
		return "", false
	}
	return hex.EncodeToString(result.lease.MetaHash), true
}

type provisionCoordinatorMarker struct{ _ byte }

// ProvisionCoordinator is the construction-bound authority for event-driven
// provision and deprovision work. It closes over one exact Store/Registry pair;
// its zero value is invalid and neither half can be replaced independently.
type ProvisionCoordinator struct {
	coordinator  *OperationCoordinator
	issuer       *operationCoordinatorMarker
	backends     backendRuntime
	providerUUID string
	controlPlane *boundProviderControlPlane
	callbacks    *CallbackRouteFactory
	deprovision  *deprovisionCoordinator
	observe      ProvisionStartObserver
	marker       *provisionCoordinatorMarker
	payloads     *payload.Store
}

// DeprovisionCompletionObserver is the least-authority capability needed by
// the authenticated callback application after exact settlement. It can retire
// retry memory but cannot claim, route, invoke, or settle teardown work.
type DeprovisionCompletionObserver struct {
	coordinator *deprovisionCoordinator
	issuer      *operationCoordinatorMarker
}

func (authority *ProvisionCoordinator) DeprovisionCompletionObserver() DeprovisionCompletionObserver {
	if !authority.Valid() {
		return DeprovisionCompletionObserver{}
	}
	return DeprovisionCompletionObserver{
		coordinator: authority.deprovision, issuer: authority.issuer,
	}
}

func (observer DeprovisionCompletionObserver) ObserveCallbackDeprovisioned(
	leaseUUID, backendName string,
) {
	if observer.coordinator == nil || observer.coordinator.execution == nil ||
		observer.issuer != observer.coordinator.execution.issuer ||
		leaseUUID == "" || backendName == "" {
		return
	}
	observer.coordinator.forget(leaseUUID, backendName)
}

// ProvisionCoordinatorWithPayloads binds payload cleanup to the same authority
// that owns the chain decision and the lifecycle claim. A message handler
// cannot replace the store or manufacture deletion permission.
func (execution *ExecutionCoordinator) ProvisionCoordinatorWithPayloads(
	observe ProvisionStartObserver,
	payloads *payload.Store,
) (*ProvisionCoordinator, error) {
	controlPlane, err := execution.providerControlPlane()
	if err != nil {
		return nil, errors.New("valid backend execution coordinator and provider control plane are required")
	}
	return &ProvisionCoordinator{
		coordinator: execution.coordinator, issuer: execution.issuer,
		backends: execution.backends, providerUUID: execution.coordinator.store.providerUUID,
		controlPlane: controlPlane, callbacks: execution.callbacks,
		deprovision: execution.deprovision,
		observe:     observe, marker: &provisionCoordinatorMarker{}, payloads: payloads,
	}, nil
}

func (authority *ProvisionCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil &&
		authority.coordinator.Valid() && authority.issuer == authority.coordinator.marker &&
		!util.IsNilInterface(authority.backends) && authority.providerUUID != "" &&
		authority.controlPlane != nil && authority.controlPlane.validFor(authority.coordinator.execution) &&
		authority.callbacks.Valid() &&
		authority.deprovision != nil && authority.marker != nil
}

// Deprovision owns candidate derivation, exact Registry/lease claims, every
// physical call, retry memory, and settlement. The caller supplies only the
// lifecycle subject and cannot choose a backend or terminal outcome.
func (authority *ProvisionCoordinator) Deprovision(ctx context.Context, leaseUUID string) error {
	if !authority.Valid() {
		return ErrDeprovisionExecution
	}
	return authority.deprovision.execute(ctx, leaseUUID)
}

// readLease performs a bounded read through the exact chain authority bound at
// construction. A handler cannot supply an independently sourced lease to a
// backend mutation.
func (authority *ProvisionCoordinator) readLease(
	ctx context.Context,
	leaseUUID, expectedTenant string,
) exactLeaseObservation {
	if !authority.Valid() || leaseUUID == "" {
		return observedLeaseUnknown{err: errors.New("invalid provision lease read")}
	}
	readCtx, cancel := context.WithTimeout(ctx, provisionLeaseReadTimeout)
	defer cancel()
	return authority.controlPlane.observeLease(readCtx, leaseUUID, expectedTenant)
}

func newProvisionEventResult(
	disposition ProvisionEventDisposition,
	lease *billingtypes.Lease,
	err error,
) ProvisionEventResult {
	result := ProvisionEventResult{disposition: disposition, err: err}
	if lease == nil {
		return result
	}
	result.lease = *lease
	result.lease.MetaHash = append([]byte(nil), lease.MetaHash...)
	result.lease.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	result.hasLease = true
	return result
}

// ExecuteCurrentLease is the sole event-driven provision boundary. It owns
// the lifecycle claim, bounded authoritative read, identity and payload
// validation, routing, write-ahead admission, physical call, and settlement.
func (authority *ProvisionCoordinator) ExecuteCurrentLease(
	ctx context.Context,
	event ProvisionEventRequest,
) (result ProvisionEventResult) {
	if !authority.Valid() || !event.valid() {
		return newProvisionEventResult(
			ProvisionEventInvalid, nil, errors.New("invalid event provision authority"),
		)
	}
	claimResult := authority.coordinator.operations.TryClaimLeaseNow(event.leaseUUID)
	if !claimResult.Acquired() {
		if authority.coordinator.operations.Contains(event.leaseUUID) {
			return newProvisionEventResult(ProvisionEventDuplicate, nil, nil)
		}
		return newProvisionEventResult(ProvisionEventUncertain, nil,
			fmt.Errorf("lease %s lifecycle claim is busy", event.leaseUUID))
	}
	leaseClaim := claimResult.Claim()
	defer func() {
		if authority.coordinator.operations.ReleaseLease(leaseClaim) {
			return
		}
		result.err = errors.Join(result.err,
			fmt.Errorf("release exact event lifecycle claim for lease %s", event.leaseUUID))
		result.disposition = ProvisionEventUncertain
	}()

	observation := authority.readLease(ctx, event.leaseUUID, event.expectedTenant)
	switch observed := observation.(type) {
	case observedLeaseUnauthorized:
		return newProvisionEventResult(ProvisionEventUncertain, &observed.lease,
			fmt.Errorf("current lease identity differs from event authority for lease %s", event.leaseUUID))
	case observedLeaseUnknown, observedLeaseNotFound:
		return newProvisionEventResult(ProvisionEventUncertain, nil,
			fmt.Errorf("read current lease %s: %w", event.leaseUUID, exactLeaseObservationError(observed)))
	case observedExactLease:
		lease := observed.lease
		result = authority.executeObservedCurrentLease(ctx, event, lease, leaseClaim)
		if result.disposition == ProvisionEventPayloadInvalid || result.disposition == ProvisionEventValidationRefused {
			return authority.rejectInvalidProvision(ctx, result, leaseClaim)
		}
		return result
	default:
		return newProvisionEventResult(ProvisionEventUncertain, nil,
			errors.New("invalid current lease observation"))
	}
}

func (authority *ProvisionCoordinator) executeObservedCurrentLease(
	ctx context.Context,
	event ProvisionEventRequest,
	observed billingtypes.Lease,
	leaseClaim operation.LeaseClaim,
) ProvisionEventResult {
	lease := &observed
	switch lease.State {
	case billingtypes.LEASE_STATE_PENDING:
	case billingtypes.LEASE_STATE_ACTIVE:
		return newProvisionEventResult(ProvisionEventLeaseActive, lease, nil)
	case billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED:
		return authority.finishTerminalProvision(*lease, leaseClaim)
	default:
		return newProvisionEventResult(ProvisionEventUncertain, lease,
			fmt.Errorf("cannot confirm lease %s state %s", event.leaseUUID, lease.State))
	}

	payloadBytes := []byte(nil)
	payloadFingerprint := PayloadFingerprint{}
	var err error
	if len(lease.MetaHash) != 0 {
		if event.kind != provisionEventPayload {
			return newProvisionEventResult(ProvisionEventAwaitingPayload, lease, nil)
		}
		payloadBytes, err = event.loadPayload()
		if err != nil || payloadBytes == nil {
			if err == nil {
				err = errors.New("payload is not available")
			}
			return newProvisionEventResult(ProvisionEventPayloadUnavailable, lease, err)
		}
		if err = payload.VerifyHash(payloadBytes, lease.MetaHash); err != nil {
			return newProvisionEventResult(ProvisionEventPayloadInvalid, lease, err)
		}
		digest := sha256.Sum256(payloadBytes)
		payloadFingerprint, err = NewPayloadFingerprint(digest[:])
		if err != nil {
			return newProvisionEventResult(ProvisionEventUncertain, lease, err)
		}
	}

	items := leaseitems.FromLease(lease)
	sku := ""
	if len(items) != 0 {
		sku = items[0].SKU
	}
	route, err := authority.routeProvision(
		ctx, lease.Uuid, sku, nil, authority.coordinator.operations.CountsByBackend(),
	)
	if err != nil {
		return newProvisionEventResult(ProvisionEventUncertain, lease, err)
	}
	if !route.valid() {
		return newProvisionEventResult(ProvisionEventNoBackend, lease, nil)
	}

	baseline := authority.coordinator.store.CurrentAdmissionBaseline()
	current := authority.coordinator.store.Lookup(lease.Uuid)
	var admission AdmissionScope
	if current.State() == StateAbsent {
		admission, err = authority.scopeAdmission(baseline)
		if err != nil {
			return newProvisionEventResult(ProvisionEventUncertain, lease, err)
		}
	}
	provisionInitiation, err := operation.NewProvisionInitiation(
		lease.Uuid, lease.Tenant, items, route.backend(),
	)
	if err != nil {
		return newProvisionEventResult(ProvisionEventValidationRefused, lease, err)
	}
	initiated := authority.coordinator.operations.TryInitiateProvisionClaimed(
		leaseClaim, provisionInitiation,
	)
	if !initiated.Started() {
		if initiated.Outcome() == operation.TrackBusy {
			return newProvisionEventResult(ProvisionEventDuplicate, lease, nil)
		}
		return newProvisionEventResult(ProvisionEventUncertain, lease,
			fmt.Errorf("register provision operation: outcome %d", initiated.Outcome()))
	}
	initiation := initiated.Capability()
	abort := func(cause error) ProvisionEventResult {
		completion := authority.coordinator.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted &&
			completion != operation.InitiationFinished &&
			completion != operation.InitiationSettling {
			cause = errors.Join(cause,
				fmt.Errorf("abort provision initiation: outcome %d", completion))
		}
		return newProvisionEventResult(ProvisionEventUncertain, lease, cause)
	}
	callbacks, err := authority.callbacks.ForOperation(initiation.ID())
	if err != nil {
		return abort(err)
	}
	requestSnapshot, err := authority.coordinator.store.MintBackendRequestSnapshot(
		lease.Tenant, items,
	)
	if err != nil {
		return abort(err)
	}

	var (
		dispatch ProvisionDispatch
		set      bool
	)
	switch current.State() {
	case StateAbsent:
		dispatch, set, err = authority.admitNewProvisionDispatch(
			initiation, admission, lease.Uuid, route, payloadFingerprint,
			requestSnapshot, callbacks,
		)
	case StateConfirmed:
		dispatch, set, err = authority.admitOwnedProvisionDispatch(
			initiation, baseline, current.RecordRevision(), route, payloadFingerprint,
			requestSnapshot, callbacks,
		)
	case StateAttempting:
		err = ErrAttemptConflict
	default:
		err = ErrUnusablePlacement
	}
	if err != nil {
		// The admission owner already retired every initiation it accepted.
		// Abort is deliberately idempotent and also covers a route-level refusal
		// before that owner could take the capability.
		aborted := abort(err)
		if errors.Is(err, ErrAttemptConflict) {
			aborted.disposition = ProvisionEventDuplicate
			aborted.err = nil
		}
		return aborted
	}
	if !set {
		current = authority.coordinator.store.Lookup(lease.Uuid)
		aborted := abort(errors.New("placement changed before write-ahead provision attempt"))
		if current.Attempt != "" {
			aborted.disposition = ProvisionEventDuplicate
			aborted.err = nil
		}
		return aborted
	}
	settlement := executeProvision(
		ctx, authority.coordinator, authority.backends, authority.observe,
		dispatch, payloadBytes,
	)
	if errors.Is(settlement.CallErr(), backend.ErrInsufficientResources) {
		verdict := metrics.CapacityVerdictAmbiguous
		if settlement.ProvisionRefusal() == backend.ProvisionRefusalCapacity {
			verdict = metrics.CapacityVerdictCodedRefusal
		}
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(
			route.backend(), verdict,
		).Inc()
	}
	if settlement.Superseded() || settlement.CallAccepted() {
		if settlement.Err() != nil {
			slog.Warn("accepted provision retained recovery evidence",
				"lease_uuid", lease.Uuid, "backend", route.backend(),
				"error", settlement.Err())
		}
		return newProvisionEventResult(ProvisionEventStarted, lease, nil)
	}
	callErr := settlement.CallErr()
	if callErr == nil {
		callErr = settlement.Err()
	}
	if settlement.ProvisionRefusal() == backend.ProvisionRefusalValidation {
		return newProvisionEventResult(ProvisionEventValidationRefused, lease, callErr)
	}
	return newProvisionEventResult(ProvisionEventUncertain, lease, callErr)
}

// provisionRoute is a read-only routing decision minted by the exact runtime
// bound to one ProvisionCoordinator. It intentionally carries no backend
// client, so callers cannot invoke a mutation outside Execute.
type provisionRoute struct {
	issuer      *provisionCoordinatorMarker
	leaseUUID   string
	backendName string
}

func (route provisionRoute) valid() bool {
	return route.issuer != nil && route.leaseUUID != "" && route.backendName != ""
}
func (route provisionRoute) backend() string {
	if !route.valid() {
		return ""
	}
	return route.backendName
}

// RouteProvision owns placement affinity and load-based routing. The same
// bound runtime is later used to resolve the exact durable backend in Execute.
func (authority *ProvisionCoordinator) routeProvision(
	ctx context.Context,
	leaseUUID, sku string,
	eligible map[string]struct{},
	inFlight map[string]int,
) (provisionRoute, error) {
	if !authority.Valid() || leaseUUID == "" {
		return provisionRoute{}, ErrProvisionRouteUnresolvable
	}
	current := authority.coordinator.store.Lookup(leaseUUID)
	var candidate backend.Backend
	var err error
	switch current.State() {
	case StateUnusable:
		return provisionRoute{}, fmt.Errorf("%w: lease %s has unusable placement", ErrProvisionRouteUnresolvable, leaseUUID)
	case StateConfirmed:
		candidate, err = exactBackend(authority.backends, current.Backend)
	case StateAbsent, StateAttempting:
		if eligible != nil {
			candidate = authority.backends.RouteForProvisionAmong(ctx, sku, eligible, inFlight)
		} else {
			candidate = authority.backends.RouteForProvision(ctx, sku, inFlight)
		}
		if util.IsNilInterface(candidate) {
			return provisionRoute{}, nil
		}
		candidate, err = exactBackend(authority.backends, candidate.Name())
	default:
		err = ErrProvisionRouteUnresolvable
	}
	if err != nil {
		return provisionRoute{}, fmt.Errorf("%w: %w", ErrProvisionRouteUnresolvable, err)
	}
	if util.IsNilInterface(candidate) {
		return provisionRoute{}, nil
	}
	return provisionRoute{
		issuer: authority.marker, leaseUUID: leaseUUID, backendName: candidate.Name(),
	}, nil
}

func (authority *ProvisionCoordinator) scopeAdmission(
	baseline AdmissionBaseline,
) (AdmissionScope, error) {
	if !authority.Valid() {
		return AdmissionScope{}, ErrInvalidAdmissionScope
	}
	backendNames, err := backendNames(authority.backends)
	if err != nil {
		return AdmissionScope{}, err
	}
	return authority.coordinator.store.scopeAdmission(baseline, backendNames)
}

func (authority *ProvisionCoordinator) admitNewProvisionDispatch(
	initiation operation.Initiation,
	scope AdmissionScope,
	leaseUUID string,
	route provisionRoute,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (ProvisionDispatch, bool, error) {
	if !authority.Valid() {
		return ProvisionDispatch{}, false, ErrInvalidAttemptToken
	}
	if !route.valid() || route.issuer != authority.marker || route.leaseUUID != leaseUUID {
		return ProvisionDispatch{}, false, ErrProvisionRouteUnresolvable
	}
	return authority.coordinator.admitNewProvisionDispatch(
		initiation, scope, leaseUUID, route.backendName, payload, request, callbacks,
	)
}

func (authority *ProvisionCoordinator) admitOwnedProvisionDispatch(
	initiation operation.Initiation,
	baseline AdmissionBaseline,
	revision RecordRevision,
	route provisionRoute,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (ProvisionDispatch, bool, error) {
	if !authority.Valid() {
		return ProvisionDispatch{}, false, ErrInvalidAttemptToken
	}
	if !route.valid() || route.issuer != authority.marker ||
		revision.leaseUUID != route.leaseUUID {
		return ProvisionDispatch{}, false, ErrProvisionRouteUnresolvable
	}
	return authority.coordinator.admitOwnedProvisionDispatch(
		initiation, baseline, revision, route.backendName, payload, request, callbacks,
	)
}
