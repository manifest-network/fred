package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

const retainedLeaseNotice = "your lease data was retained and can be restored within the grace window: create a fresh PENDING lease of matching shape, then POST /v1/leases/{new_lease_uuid}/restore with from_lease_uuid set to this lease's UUID"

var (
	errCallbackSettlementClaimTimeout     = errors.New("timed out waiting for callback settlement claim")
	errCallbackOperationsUnavailable      = errors.New("backend callback operation registry is unavailable")
	errCallbackChainUnavailable           = errors.New("backend callback chain client is unavailable")
	errCallbackAcknowledgerUnavailable    = errors.New("backend callback acknowledger is unavailable")
	errCallbackPlacementUnavailable       = errors.New("backend callback placement authority is unavailable")
	errCallbackLifecycleUnavailable       = errors.New("backend callback lifecycle authority is unavailable")
	errCallbackSettlementLost             = errors.New("backend callback lost its operation settlement claim")
	errCallbackRecoveryLeaseBusy          = errors.New("backend callback recovery lease is busy")
	errCallbackStorageIdentityUnavailable = errors.New("backend callback storage identity authority is unavailable")
	errCallbackStorageIdentityMissing     = errors.New("backend callback storage identity is required")
	errCallbackStorageIdentityMismatch    = errors.New("backend callback storage identity does not match placement authority")
	errCallbackStorageIdentityUnbound     = errors.New("backend callback storage identity is not durably bound")
	errInvalidCallbackCommand             = errors.New("backend callback command was not constructed by NewCallbackCommand")
)

type callbackLeaseClass uint8

const (
	callbackLeaseUnknown callbackLeaseClass = iota
	callbackLeasePending
	callbackLeaseActive
	callbackLeaseTerminal
)

// classifyCallbackLease interprets an exact lease read for callback
// settlement. A nil result is unknown rather than terminal: x/billing retains
// terminal leases, so absence can mean an RPC node is behind, reset, or pointed
// at the wrong chain. Only a positively observed terminal state may retire
// durable operation/payload evidence. Unknown enum values likewise remain
// retryable so an older provider cannot guess that a new state is safe.
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

func terminalLeaseRepresentsFailure(lease *billingtypes.Lease) bool {
	return lease != nil && lease.State == billingtypes.LEASE_STATE_REJECTED
}

// CallbackApplication is the synchronous callback application boundary. The
// ingress adapter owns decoding and delivery metrics; this service owns
// callback authorization and lifecycle policy.
type CallbackApplication interface {
	HandleCallback(context.Context, CallbackCommand) error
}

// CallbackCommand is the validated application input produced at the
// authenticated transport boundary. Its fields are deliberately private: the
// callback DTO's raw string cannot reach registry or placement consumers.
// Missing identity remains an explicit legacy case, while a present identity
// is one member of the typed operation/lifecycle union. backendName is
// observational metadata only: v0.13 documented it as a metrics label, so
// authority comes from the HMAC-authenticated callback URL plus the exact
// current operation or durable lifecycle record, never from a backend name
// asserted in the JSON body.
type CallbackCommand struct {
	leaseUUID   string
	status      backend.CallbackStatus
	failure     string
	backendName string
	retained    bool
	operationID operation.OperationID
	lifecycleID lifecycle.ID
	storageID   backendidentity.ID
	selector    callbackSelectorKind
	valid       bool
}

// callbackSelectorKind is a tagged union for the mutually exclusive authority
// carried by the authenticated request URI. The invalid zero value grants no
// behavior; legacy is explicit rather than inferred from zero-value IDs.
type callbackSelectorKind uint8

const (
	callbackSelectorInvalid callbackSelectorKind = iota
	callbackSelectorLegacy
	callbackSelectorOperation
	callbackSelectorLifecycle
)

// NewCallbackCommand converts the wire-compatible callback DTO into typed
// application input. An empty OperationID means the query parameter was absent;
// the HTTP boundary rejects every explicitly present malformed value before
// this constructor is called.
func NewCallbackCommand(callback backend.CallbackPayload) (CallbackCommand, error) {
	if callback.LeaseUUID == "" {
		return CallbackCommand{}, errors.New("callback lease UUID is required")
	}
	switch callback.Status {
	case backend.CallbackStatusSuccess,
		backend.CallbackStatusFailed,
		backend.CallbackStatusDeprovisioned:
	default:
		return CallbackCommand{}, fmt.Errorf("invalid callback status %q", callback.Status)
	}
	if callback.Retained && callback.Status != backend.CallbackStatusDeprovisioned {
		return CallbackCommand{}, errors.New("callback retained flag requires deprovisioned status")
	}
	command := CallbackCommand{
		leaseUUID:   callback.LeaseUUID,
		status:      callback.Status,
		failure:     callback.Error,
		backendName: callback.Backend,
		retained:    callback.Retained,
		selector:    callbackSelectorLegacy,
		valid:       true,
	}
	if callback.BackendStorageID != "" {
		storageID, err := backendidentity.Parse(callback.BackendStorageID)
		if err != nil {
			return CallbackCommand{}, fmt.Errorf("parse callback backend storage identity: %w", err)
		}
		command.storageID = storageID
	}
	if callback.OperationID != "" && callback.LifecycleID != "" {
		return CallbackCommand{}, errors.New("callback carries both operation and lifecycle authority")
	}
	if callback.OperationID == "" && callback.LifecycleID == "" {
		return command, nil
	}
	if callback.OperationID != "" {
		id, err := operation.ParseID(callback.OperationID)
		if err != nil {
			return CallbackCommand{}, fmt.Errorf("parse callback operation ID: %w", err)
		}
		if callback.Status == backend.CallbackStatusDeprovisioned {
			return CallbackCommand{}, errors.New(
				"deprovisioned callback requires lifecycle or legacy authority",
			)
		}
		command.operationID = id
		command.selector = callbackSelectorOperation
		return command, nil
	}
	id, err := lifecycle.ParseID(callback.LifecycleID)
	if err != nil {
		return CallbackCommand{}, fmt.Errorf("parse callback lifecycle ID: %w", err)
	}
	command.lifecycleID = id
	command.selector = callbackSelectorLifecycle
	return command, nil
}

// CallbackOperations is the exact process-local authority needed to apply a
// backend callback. In particular, callers cannot remove an operation by lease
// or raw ID: terminal mutation requires the opaque claim returned for
// the matching OperationID.
type CallbackOperations interface {
	Lookup(string) (operation.Record, bool)
	TryClaimCallback(string, operation.OperationID) operation.SettlementResult
	ReleaseSettlement(operation.SettlementClaim) bool
	FinishSettlement(operation.SettlementClaim) bool
	TryClaimCallbackRecoveryLease(string) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
}

// CallbackChain is the chain read/write surface used by failure settlement.
type CallbackChain interface {
	GetLease(context.Context, string) (*billingtypes.Lease, error)
	RejectLeases(context.Context, []string, string) (uint64, []string, error)
}

// CallbackPlacement is the durable placement surface used at callback time.
// The operation methods settle callbacks whose process-local registry record
// is still present. ClaimAttempt is the restart/ambiguous-outcome recovery
// boundary: it derives the backend from the exact durable attempt or promoted
// lifecycle generation and returns an exclusive revision-bound capability that
// fences inventory until chain and placement settlement complete. Recovery
// callers acquire the Registry lease capability first; every provisioning,
// restore, reconciliation, and deprovision competitor uses that same global
// Registry-before-placement order.
type CallbackPlacement interface {
	ConfirmOperation(string, string, operation.OperationID) (bool, error)
	RefuseOperation(string, string, operation.OperationID) (bool, error)
	ClaimAttempt(string, operation.OperationID) (placement.AttemptClaim, bool, error)
	ReleaseAttemptClaim(placement.AttemptClaim) bool
	ConfirmClaimedAttempt(placement.AttemptClaim) (bool, error)
	RefuseClaimedAttempt(placement.AttemptClaim) (bool, error)
}

// CallbackStorageIdentityAuthority resolves the immutable physical-storage
// lineage pinned to an active backend name. Callback body metadata is never
// allowed to select the name; it can only prove it came from the lineage
// already selected by an exact operation or lifecycle capability.
type CallbackStorageIdentityAuthority interface {
	ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool)
}

// CallbackLifecycleAuthority is the durable, revocable authority for
// observation-only callbacks. The zero lifecycle ID selects only an explicitly
// migrated legacy record; it never matches a typed capability.
type CallbackLifecycleAuthority interface {
	AuthorizeLifecycle(string, lifecycle.ID) placement.LifecycleAuthorization
	RetireLifecycle(string, lifecycle.ID) (placement.LifecycleAuthorization, error)
}

// CallbackPayloadStore is the terminal payload cleanup capability.
type CallbackPayloadStore interface {
	Delete(string)
}

// CallbackEventSink receives best-effort status events. Its delivery mechanism
// remains outside the application service so subscriber infrastructure does
// not leak into callback lifecycle policy.
type CallbackEventSink interface {
	PublishCallbackLeaseEvent(string, backend.ProvisionStatus, string)
}

type callbackEventSinkFunc func(string, backend.ProvisionStatus, string)

func (publish callbackEventSinkFunc) PublishCallbackLeaseEvent(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	publish(leaseUUID, status, errMsg)
}

// CallbackBackendResolver is the allowlist needed to keep callback metric
// labels bounded.
type CallbackBackendResolver interface {
	GetBackendByName(string) backend.Backend
}

// CallbackDeprovisionObserver retires process-local retry candidates when a
// backend positively reports teardown.
type CallbackDeprovisionObserver interface {
	ObserveCallbackDeprovisioned(string, string)
}

type callbackDeprovisionObserverFunc func(string, string)

func (observe callbackDeprovisionObserverFunc) ObserveCallbackDeprovisioned(
	leaseUUID, backendName string,
) {
	observe(leaseUUID, backendName)
}

// CallbackServiceConfig wires the callback application service. Operations,
// Chain, Acknowledger, Placement, StorageIdentities, and LifecycleAuthority are
// mandatory: a production service cannot be constructed without every durable
// or security authority needed by one arm of the tagged callback protocol.
type CallbackServiceConfig struct {
	Operations          CallbackOperations
	Chain               CallbackChain
	Acknowledger        Acknowledger
	Placement           CallbackPlacement
	StorageIdentities   CallbackStorageIdentityAuthority
	LifecycleAuthority  CallbackLifecycleAuthority
	Payloads            CallbackPayloadStore
	Events              CallbackEventSink
	Backends            CallbackBackendResolver
	DeprovisionObserver CallbackDeprovisionObserver
	ClaimPollInterval   time.Duration
	ClaimMaxWait        time.Duration
}

// CallbackService applies authenticated backend observations to the exact
// tracked operation and durable placement attempt they name.
type CallbackService struct {
	operations          CallbackOperations
	chain               CallbackChain
	acknowledger        Acknowledger
	placement           CallbackPlacement
	storageIdentities   CallbackStorageIdentityAuthority
	lifecycleAuthority  CallbackLifecycleAuthority
	payloads            CallbackPayloadStore
	events              CallbackEventSink
	backends            CallbackBackendResolver
	deprovisionObserver CallbackDeprovisionObserver
	claimPollInterval   time.Duration
	claimMaxWait        time.Duration
}

// NewCallbackService constructs the fully composed production callback
// application. One service owns both arms of the tagged callback union, and it
// cannot fall back to lease-only, body-supplied, or unauthenticated identity.
func NewCallbackService(cfg CallbackServiceConfig) (*CallbackService, error) {
	if util.IsNilInterface(cfg.Operations) {
		return nil, errCallbackOperationsUnavailable
	}
	if util.IsNilInterface(cfg.Chain) {
		return nil, errCallbackChainUnavailable
	}
	if util.IsNilInterface(cfg.Acknowledger) {
		return nil, errCallbackAcknowledgerUnavailable
	}
	if util.IsNilInterface(cfg.Placement) {
		return nil, errCallbackPlacementUnavailable
	}
	if util.IsNilInterface(cfg.StorageIdentities) {
		return nil, errCallbackStorageIdentityUnavailable
	}
	if util.IsNilInterface(cfg.LifecycleAuthority) {
		return nil, errCallbackLifecycleUnavailable
	}
	return newCallbackService(cfg)
}

func newCallbackService(cfg CallbackServiceConfig) (*CallbackService, error) {
	if util.IsNilInterface(cfg.Operations) {
		return nil, errCallbackOperationsUnavailable
	}
	if util.IsNilInterface(cfg.Chain) {
		cfg.Chain = nil
	}
	if util.IsNilInterface(cfg.Acknowledger) {
		cfg.Acknowledger = nil
	}
	if util.IsNilInterface(cfg.Placement) {
		cfg.Placement = nil
	}
	if util.IsNilInterface(cfg.StorageIdentities) {
		cfg.StorageIdentities = nil
	}
	if util.IsNilInterface(cfg.LifecycleAuthority) {
		cfg.LifecycleAuthority = nil
	}
	if util.IsNilInterface(cfg.Payloads) {
		cfg.Payloads = nil
	}
	if util.IsNilInterface(cfg.Events) {
		cfg.Events = nil
	}
	if util.IsNilInterface(cfg.Backends) {
		cfg.Backends = nil
	}
	if util.IsNilInterface(cfg.DeprovisionObserver) {
		cfg.DeprovisionObserver = nil
	}
	pollInterval := cfg.ClaimPollInterval
	if pollInterval <= 0 {
		pollInterval = callbackSettlementClaimPollInterval
	}
	maxWait := cfg.ClaimMaxWait
	if maxWait <= 0 {
		maxWait = callbackSettlementClaimMaxWait
	}
	return &CallbackService{
		operations:          cfg.Operations,
		chain:               cfg.Chain,
		acknowledger:        cfg.Acknowledger,
		placement:           cfg.Placement,
		storageIdentities:   cfg.StorageIdentities,
		lifecycleAuthority:  cfg.LifecycleAuthority,
		payloads:            cfg.Payloads,
		events:              cfg.Events,
		backends:            cfg.Backends,
		deprovisionObserver: cfg.DeprovisionObserver,
		claimPollInterval:   pollInterval,
		claimMaxWait:        maxWait,
	}, nil
}

// HandleCallback applies one typed callback command.
func (service *CallbackService) HandleCallback(ctx context.Context, command CallbackCommand) error {
	if !command.valid {
		return errInvalidCallbackCommand
	}
	callback := command
	if callback.selector == callbackSelectorLifecycle {
		return service.observeAuthorizedLifecycle(callback)
	}

	record, exists := service.operations.Lookup(callback.leaseUUID)
	if !exists {
		if callback.selector == callbackSelectorLegacy {
			return service.observeAuthorizedLifecycle(callback)
		}
		if service.placement == nil {
			return errCallbackPlacementUnavailable
		}
		handled, err := service.settleDurableAttempt(ctx, callback)
		if handled || err != nil {
			service.recordNonInFlightCallbackReceived(callback)
			return err
		}
		service.observeNonInFlightOperation(callback)
		return nil
	}

	if !callbackMatchesOperation(command, record) {
		if callback.selector == callbackSelectorLegacy {
			// A migrated v0.13 route is observation-only when the current
			// operation requires a typed token. It can never claim that operation.
			return service.observeAuthorizedLifecycle(callback)
		}
		slog.Warn("ignoring callback whose operation token does not match the in-flight operation",
			"lease_uuid", callback.leaseUUID,
			"callback_operation_id", callback.operationID,
			"operation_id", record.ID,
		)
		return nil
	}
	if err := service.verifyStorageIdentity(callback, record.Backend); err != nil {
		return err
	}

	if record.Settlement == operation.SettlementDeprovision {
		return service.handleDeprovisionOwned(callback, record)
	}

	result, observed, deprovisionOwned, err := service.waitForSettlementClaim(ctx, record)
	if err != nil {
		return fmt.Errorf("wait to settle callback for lease %s: %w", callback.leaseUUID, err)
	}
	if deprovisionOwned {
		return service.handleDeprovisionOwned(callback, observed)
	}
	if !result.Claimed() {
		// The exact operation completed or was replaced while the callback waited.
		return nil
	}

	return service.settleClaimed(ctx, callback, result.Record(), result.Claim())
}

func callbackMatchesOperation(command CallbackCommand, record operation.Record) bool {
	switch command.selector {
	case callbackSelectorLegacy:
		return false
	case callbackSelectorOperation:
		return command.operationID == record.ID
	case callbackSelectorInvalid, callbackSelectorLifecycle:
		return false
	default:
		return false
	}
}

func (service *CallbackService) waitForSettlementClaim(
	ctx context.Context,
	record operation.Record,
) (operation.SettlementResult, operation.Record, bool, error) {
	if result := service.operations.TryClaimCallback(record.LeaseUUID, record.ID); result.Claimed() {
		return result, operation.Record{}, false, nil
	} else if result.Outcome() != operation.SettlementBusy {
		return operation.SettlementResult{}, operation.Record{}, false, nil
	}

	started := time.Now()
	timedOut := func() error {
		waited := time.Since(started)
		metrics.CallbackSettlementClaimWaitTimeoutsTotal.Inc()
		slog.Error("timed out waiting for callback settlement claim",
			"lease_uuid", record.LeaseUUID,
			"backend", record.Backend,
			"operation_id", record.ID,
			"waited", waited,
		)
		return fmt.Errorf("%w after %s", errCallbackSettlementClaimTimeout, waited)
	}

	ticker := time.NewTicker(service.claimPollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(service.claimMaxWait)
	defer timer.Stop()

	slog.Warn("callback settlement claim is contended; waiting for exact operation",
		"lease_uuid", record.LeaseUUID,
		"backend", record.Backend,
		"operation_id", record.ID,
		"max_wait", service.claimMaxWait,
	)

	for {
		if time.Since(started) >= service.claimMaxWait {
			return operation.SettlementResult{}, operation.Record{}, false, timedOut()
		}
		current, exists := service.operations.Lookup(record.LeaseUUID)
		if !exists || current.ID != record.ID {
			return operation.SettlementResult{}, operation.Record{}, false, nil
		}
		if current.Settlement == operation.SettlementDeprovision {
			return operation.SettlementResult{}, current, true, nil
		}

		select {
		case <-ctx.Done():
			return operation.SettlementResult{}, operation.Record{}, false, ctx.Err()
		case <-timer.C:
			return operation.SettlementResult{}, operation.Record{}, false, timedOut()
		case <-ticker.C:
			if time.Since(started) >= service.claimMaxWait {
				return operation.SettlementResult{}, operation.Record{}, false, timedOut()
			}
			result := service.operations.TryClaimCallback(record.LeaseUUID, record.ID)
			if result.Claimed() {
				slog.Info("acquired callback settlement claim after contention",
					"lease_uuid", record.LeaseUUID,
					"backend", record.Backend,
					"operation_id", record.ID,
					"waited", time.Since(started),
				)
				return result, operation.Record{}, false, nil
			}
			if result.Outcome() != operation.SettlementBusy {
				return operation.SettlementResult{}, operation.Record{}, false, nil
			}
		}
	}
}

func (service *CallbackService) settleClaimed(
	ctx context.Context,
	callback CallbackCommand,
	record operation.Record,
	claim operation.SettlementClaim,
) (err error) {
	finished := false
	defer func() {
		if !finished {
			service.operations.ReleaseSettlement(claim)
		}
	}()

	slog.Info("processing backend callback",
		"lease_uuid", callback.leaseUUID,
		"tenant", record.Tenant,
		"status", callback.status,
		"backend", record.Backend,
		"operation_id", record.ID,
	)

	recordDuration := func() {
		if !record.StartedAt.IsZero() {
			metrics.ProvisioningDuration.WithLabelValues(
				record.Backend, callbackOperationLabel(record.Kind),
			).Observe(time.Since(record.StartedAt).Seconds())
		}
	}
	var retryableSuccessPlacementErr error
	finish := func() error {
		if !service.operations.FinishSettlement(claim) {
			return fmt.Errorf("%w for lease %s operation %s",
				errCallbackSettlementLost, record.LeaseUUID, record.ID)
		}
		finished = true
		return nil
	}
	completeSuccess := func(publishReady bool) error {
		// A positive backend result is authoritative enough to acknowledge the
		// lease, but the callback must remain retryable until its exact durable
		// placement generation is also recorded. Finishing the volatile
		// operation here would let a 2xx response delete the backend's only
		// replayable evidence after a transient placement-store failure.
		if retryableSuccessPlacementErr != nil {
			return fmt.Errorf(
				"persist acknowledged placement for lease %s operation %s: %w",
				record.LeaseUUID, record.ID, retryableSuccessPlacementErr,
			)
		}
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeSuccess, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		if publishReady {
			service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
		}
		return nil
	}
	completeFailure := func(reason string, publishFailed bool) error {
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeFailed, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		if publishFailed {
			service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, reason)
		}
		return nil
	}

	switch callback.status {
	case backend.CallbackStatusSuccess:
		if service.placement == nil {
			return errCallbackPlacementUnavailable
		}
		applied, placementErr := service.placement.ConfirmOperation(
			callback.leaseUUID, record.Backend, record.ID,
		)
		if placementErr != nil || !applied {
			permanentVerdict := placementErr == nil || isPermanentPlacementVerdict(placementErr)
			if permanentVerdict {
				metrics.CallbackPlacementSemanticConflictsTotal.Inc()
			} else {
				retryableSuccessPlacementErr = placementErr
			}
			slog.Error("failed to confirm placement from authenticated success callback; continuing chain acknowledgement",
				"lease_uuid", callback.leaseUUID,
				"backend", record.Backend,
				"operation_id", record.ID,
				"placement_applied", applied,
				"permanent_semantic_verdict", permanentVerdict,
				"error", placementErr,
			)
		}
		if service.acknowledger == nil {
			return errCallbackAcknowledgerUnavailable
		}
		acknowledged, txHash, acknowledgeErr := service.acknowledger.Acknowledge(ctx, callback.leaseUUID)
		if acknowledgeErr != nil {
			if isTerminalAcknowledgeError(acknowledgeErr) {
				if service.chain == nil {
					return errCallbackChainUnavailable
				}
				lease, leaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
				if leaseErr != nil {
					return fmt.Errorf(
						"%w: verify terminal acknowledgement for lease %s after %w: %w",
						ErrAcknowledgeFailed, callback.leaseUUID, acknowledgeErr, leaseErr,
					)
				}
				switch classifyCallbackLease(lease) {
				case callbackLeaseActive:
					if err := completeSuccess(true); err != nil {
						return err
					}
					slog.Info("lease already acknowledged, skipping",
						"lease_uuid", callback.leaseUUID,
						"tenant", record.Tenant,
						"lease_state", callbackLeaseState(lease),
					)
					return nil

				case callbackLeaseTerminal:
					if err := completeSuccess(false); err != nil {
						return err
					}
					slog.Info("success callback superseded by terminal chain state",
						"lease_uuid", callback.leaseUUID,
						"tenant", record.Tenant,
						"lease_state", callbackLeaseState(lease),
					)
					return nil

				case callbackLeasePending, callbackLeaseUnknown:
					return fmt.Errorf(
						"%w: lease %s is %s after terminal acknowledgement error: %w",
						ErrAcknowledgeFailed, callback.leaseUUID,
						callbackLeaseState(lease), acknowledgeErr,
					)
				}
			}
			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.leaseUUID,
				"tenant", record.Tenant,
				"error", acknowledgeErr,
			)
			return fmt.Errorf("%w: lease %s: %w", ErrAcknowledgeFailed, callback.leaseUUID, acknowledgeErr)
		}
		if err := completeSuccess(true); err != nil {
			return err
		}
		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.leaseUUID,
			"tenant", record.Tenant,
			"operation", callbackOperationLabel(record.Kind),
			"acknowledged", acknowledged,
			"tx_hash", txHash,
		)

	case backend.CallbackStatusFailed:
		if service.chain == nil {
			return errCallbackChainUnavailable
		}
		if service.placement == nil {
			return errCallbackPlacementUnavailable
		}
		reason := callback.failure
		if reason == "" {
			reason = "provisioning failed"
		}
		settleActiveFailure := func() error {
			applied, placementErr := service.placement.RefuseOperation(
				callback.leaseUUID, record.Backend, record.ID,
			)
			if placementErr != nil {
				return fmt.Errorf("refuse placement operation for lease %s: %w",
					callback.leaseUUID, placementErr)
			}
			if !applied {
				slog.Info("active failure did not match a durable placement attempt; preserving placement",
					"lease_uuid", callback.leaseUUID,
					"backend", record.Backend,
					"operation_id", record.ID,
				)
			}
			if err := completeFailure(reason, true); err != nil {
				return err
			}
			slog.Warn("re-provision failed for active lease, deferring to reconciler",
				"lease_uuid", callback.leaseUUID,
				"tenant", record.Tenant,
				"reason", reason,
			)
			return nil
		}
		settleTerminalFailure := func(publishFailed bool) error {
			if service.payloads != nil {
				service.payloads.Delete(callback.leaseUUID)
			}
			applied, placementErr := service.placement.RefuseOperation(
				callback.leaseUUID, record.Backend, record.ID,
			)
			if placementErr != nil {
				permanentVerdict := isPermanentPlacementVerdict(placementErr)
				slog.Warn("failed to clean up terminal lease placement operation",
					"lease_uuid", callback.leaseUUID,
					"backend", record.Backend,
					"operation_id", record.ID,
					"permanent_semantic_verdict", permanentVerdict,
					"error", placementErr,
				)
				if !permanentVerdict {
					return fmt.Errorf(
						"refuse placement operation for terminal lease %s: %w",
						callback.leaseUUID, placementErr,
					)
				}
			} else if !applied {
				slog.Info("terminal failure callback did not match a durable placement attempt; preserving placement",
					"lease_uuid", callback.leaseUUID,
					"backend", record.Backend,
					"operation_id", record.ID,
				)
			}
			return completeFailure(reason, publishFailed)
		}

		lease, leaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
		if leaseErr != nil {
			return fmt.Errorf("failed to fetch lease %s: %w", callback.leaseUUID, leaseErr)
		}
		switch classifyCallbackLease(lease) {
		case callbackLeaseActive:
			return settleActiveFailure()
		case callbackLeaseTerminal:
			if err := settleTerminalFailure(terminalLeaseRepresentsFailure(lease)); err != nil {
				return err
			}
			slog.Info("failure callback found lease already terminal",
				"lease_uuid", callback.leaseUUID,
				"tenant", record.Tenant,
				"lease_state", callbackLeaseState(lease),
				"reason", reason,
			)
			return nil
		case callbackLeaseUnknown:
			return fmt.Errorf("lease %s has unknown state %s after failure callback",
				callback.leaseUUID, callbackLeaseState(lease))
		case callbackLeasePending:
			// Only a positively observed PENDING lease may be rejected below.
		}

		rejected, txHashes, rejectErr := service.chain.RejectLeases(
			ctx, []string{callback.leaseUUID}, truncateRejectReason(reason),
		)
		if rejectErr != nil {
			if !isTerminalAcknowledgeError(rejectErr) {
				return fmt.Errorf("failed to reject lease %s: %w", callback.leaseUUID, rejectErr)
			}

			currentLease, currentLeaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
			if currentLeaseErr != nil {
				return fmt.Errorf(
					"failed to verify terminal reject verdict for lease %s after %w: %w",
					callback.leaseUUID, rejectErr, currentLeaseErr,
				)
			}
			switch classifyCallbackLease(currentLease) {
			case callbackLeaseActive:
				return settleActiveFailure()
			case callbackLeaseTerminal:
				if err := settleTerminalFailure(terminalLeaseRepresentsFailure(currentLease)); err != nil {
					return err
				}
				slog.Info("lease rejection already reached a terminal chain state",
					"lease_uuid", callback.leaseUUID,
					"tenant", record.Tenant,
					"lease_state", callbackLeaseState(currentLease),
					"reason", reason,
					"error", rejectErr,
				)
				return nil
			case callbackLeasePending, callbackLeaseUnknown:
				return fmt.Errorf(
					"failed to reject lease %s: chain still reports state %s after terminal verdict: %w",
					callback.leaseUUID, callbackLeaseState(currentLease), rejectErr,
				)
			}
		}
		if err := settleTerminalFailure(true); err != nil {
			return err
		}
		slog.Info("lease rejected after provisioning failure",
			"lease_uuid", callback.leaseUUID,
			"tenant", record.Tenant,
			"rejected", rejected,
			"tx_hashes", txHashes,
			"reason", reason,
		)

	case backend.CallbackStatusDeprovisioned:
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeError, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		if callback.retained {
			service.publish(callback.leaseUUID, backend.ProvisionStatusRetained, retainedLeaseNotice)
		}
		slog.Warn("deprovision callback settled an in-flight provisioning operation",
			"lease_uuid", callback.leaseUUID,
			"tenant", record.Tenant,
			"backend", record.Backend,
			"operation_id", record.ID,
			"retained", callback.retained,
		)

	default:
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeError, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		slog.Warn("unknown callback status, treating as terminal",
			"lease_uuid", callback.leaseUUID,
			"tenant", record.Tenant,
			"status", callback.status,
		)
	}
	return nil
}

// settleDurableAttempt recovers a provision/restore callback whose ephemeral
// operation record is gone but whose exact write-ahead operation identity is
// still durable. The Registry lease claim is acquired first, matching the
// global order used by provision, restore, reconciliation, and deprovision;
// the store claim follows and both are held until chain and placement settle.
// Inventory projection sees the store claim and fences the lease, so a failure
// callback cannot reject the chain concurrently with positive inventory.
//
// Chain settlement intentionally precedes consuming unresolved authority. If
// a chain RPC or placement write fails, or the process exits between them, the
// backend's durable outbox can retry against the still-current generation.
// Chain acknowledge/reject operations are idempotent and terminal errors are
// verified with an exact lease read before the placement claim is consumed.
func (service *CallbackService) settleDurableAttempt(
	ctx context.Context,
	callback CallbackCommand,
) (handled bool, err error) {
	if callback.selector != callbackSelectorOperation {
		return false, nil
	}
	switch callback.status {
	case backend.CallbackStatusSuccess, backend.CallbackStatusFailed:
		// Only definitive provision/restore outcomes may consume durable authority.
	default:
		return false, nil
	}
	leaseClaimResult := service.operations.TryClaimCallbackRecoveryLease(callback.leaseUUID)
	if !leaseClaimResult.Acquired() {
		return true, fmt.Errorf("%w for lease %s (outcome %d)",
			errCallbackRecoveryLeaseBusy, callback.leaseUUID, leaseClaimResult.Outcome())
	}
	leaseClaim := leaseClaimResult.Claim()
	defer func() {
		if service.operations.ReleaseLease(leaseClaim) {
			return
		}
		releaseErr := fmt.Errorf("%w: release recovered lease capability for %s",
			errCallbackSettlementLost, callback.leaseUUID)
		if err == nil {
			err = releaseErr
		} else {
			err = errors.Join(err, releaseErr)
		}
	}()

	claim, claimed, claimErr := service.placement.ClaimAttempt(
		callback.leaseUUID, callback.operationID,
	)
	if claimErr != nil {
		return true, fmt.Errorf("claim durable callback attempt for lease %s: %w",
			callback.leaseUUID, claimErr)
	}
	if !claimed {
		return false, nil
	}

	finished := false
	defer func() {
		if !finished {
			service.placement.ReleaseAttemptClaim(claim)
		}
	}()
	callback.backendName = claim.Backend()
	if err := service.verifyStorageIdentity(callback, callback.backendName); err != nil {
		return true, err
	}
	finish := func(success bool) error {
		var (
			applied  bool
			applyErr error
		)
		if success {
			applied, applyErr = service.placement.ConfirmClaimedAttempt(claim)
		} else {
			applied, applyErr = service.placement.RefuseClaimedAttempt(claim)
		}
		if applyErr != nil {
			return applyErr
		}
		if !applied {
			return fmt.Errorf("%w for lease %s operation %s",
				errCallbackSettlementLost, callback.leaseUUID, callback.operationID)
		}
		finished = true
		return nil
	}
	completeFailure := func(reason string, publish bool, deletePayload bool) error {
		if deletePayload && service.payloads != nil {
			service.payloads.Delete(callback.leaseUUID)
		}
		if err := finish(false); err != nil {
			return fmt.Errorf("refuse recovered placement attempt for lease %s: %w",
				callback.leaseUUID, err)
		}
		if publish {
			service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, reason)
		}
		return nil
	}

	slog.Info("recovering backend callback from durable placement generation",
		"lease_uuid", callback.leaseUUID,
		"backend", callback.backendName,
		"operation_id", callback.operationID,
		"status", callback.status,
	)
	switch callback.status {
	case backend.CallbackStatusSuccess:
		if service.acknowledger == nil {
			return true, errCallbackAcknowledgerUnavailable
		}
		_, _, acknowledgeErr := service.acknowledger.Acknowledge(ctx, callback.leaseUUID)
		publishReady := true
		if acknowledgeErr != nil {
			if !isTerminalAcknowledgeError(acknowledgeErr) {
				return true, fmt.Errorf("%w: lease %s: %w",
					ErrAcknowledgeFailed, callback.leaseUUID, acknowledgeErr)
			}
			if service.chain == nil {
				return true, errCallbackChainUnavailable
			}
			lease, leaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
			if leaseErr != nil {
				return true, fmt.Errorf(
					"%w: verify terminal acknowledgement for recovered lease %s after %w: %w",
					ErrAcknowledgeFailed, callback.leaseUUID, acknowledgeErr, leaseErr,
				)
			}
			switch classifyCallbackLease(lease) {
			case callbackLeaseActive:
				// The acknowledgement already landed.
			case callbackLeaseTerminal:
				// Preserve positive backend ownership for terminal cleanup without
				// publishing a stale Ready observation.
				publishReady = false
			case callbackLeasePending, callbackLeaseUnknown:
				return true, fmt.Errorf(
					"%w: recovered lease %s is %s after terminal acknowledgement error: %w",
					ErrAcknowledgeFailed, callback.leaseUUID,
					callbackLeaseState(lease), acknowledgeErr,
				)
			}
		}
		if err := finish(true); err != nil {
			return true, fmt.Errorf("confirm recovered placement attempt for lease %s: %w",
				callback.leaseUUID, err)
		}
		if publishReady {
			service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
		}
		return true, nil

	case backend.CallbackStatusFailed:
		if service.chain == nil {
			return true, errCallbackChainUnavailable
		}
		reason := callback.failure
		if reason == "" {
			reason = "provisioning failed"
		}
		lease, leaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
		if leaseErr != nil {
			return true, fmt.Errorf("failed to fetch recovered lease %s: %w",
				callback.leaseUUID, leaseErr)
		}
		switch classifyCallbackLease(lease) {
		case callbackLeaseActive:
			if claim.HasSameBackendOwner() {
				// ACTIVE chain state plus positive store-owned backend evidence is
				// stronger than a delayed failure. Consume only an unresolved
				// marker; never reject, publish Failed, or demote the live owner.
				if err := finish(false); err != nil {
					return true, fmt.Errorf(
						"retire stale recovered failure for lease %s: %w",
						callback.leaseUUID, err,
					)
				}
				slog.Info("ignored recovered failure for active positively confirmed owner",
					"lease_uuid", callback.leaseUUID,
					"backend", callback.backendName,
					"operation_id", callback.operationID,
				)
				return true, nil
			}
			return true, completeFailure(reason, true, false)
		case callbackLeaseTerminal:
			return true, completeFailure(
				reason, terminalLeaseRepresentsFailure(lease), true,
			)
		case callbackLeaseUnknown:
			return true, fmt.Errorf("recovered lease %s has unknown state %s after failure callback",
				callback.leaseUUID, callbackLeaseState(lease))
		case callbackLeasePending:
			// Only a positively observed PENDING lease may be rejected below.
		}

		_, _, rejectErr := service.chain.RejectLeases(
			ctx, []string{callback.leaseUUID}, truncateRejectReason(reason),
		)
		if rejectErr != nil {
			if !isTerminalAcknowledgeError(rejectErr) {
				return true, fmt.Errorf("failed to reject recovered lease %s: %w",
					callback.leaseUUID, rejectErr)
			}
			currentLease, currentLeaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
			if currentLeaseErr != nil {
				return true, fmt.Errorf(
					"failed to verify terminal reject verdict for recovered lease %s after %w: %w",
					callback.leaseUUID, rejectErr, currentLeaseErr,
				)
			}
			switch classifyCallbackLease(currentLease) {
			case callbackLeaseActive:
				return true, completeFailure(reason, true, false)
			case callbackLeaseTerminal:
				return true, completeFailure(
					reason, terminalLeaseRepresentsFailure(currentLease), true,
				)
			case callbackLeasePending, callbackLeaseUnknown:
				return true, fmt.Errorf(
					"failed to reject recovered lease %s: chain still reports state %s after terminal verdict: %w",
					callback.leaseUUID, callbackLeaseState(currentLease), rejectErr,
				)
			}
		}
		return true, completeFailure(reason, true, true)

	default:
		return false, nil
	}
}

func (service *CallbackService) observeNonInFlightOperation(callback CallbackCommand) {
	service.recordNonInFlightCallbackReceived(callback)
	// No exact process-local operation, unresolved durable attempt, or current
	// confirmed lifecycle generation matches this callback. Its ID is therefore
	// stale rather than lease-scoped mutation authority.
	slog.Warn("ignoring callback for an operation that is no longer current",
		"lease_uuid", callback.leaseUUID,
		"backend", callback.backendName,
		"operation_id", callback.operationID,
		"status", callback.status,
	)
}

// recordNonInFlightCallbackReceived preserves the existing metric's ingress
// meaning independently of the lifecycle policy decision that follows. The
// body backend remains reporting metadata only; authorization may replace it
// with a store-owned backend for application, but must not rewrite what the
// compatibility counter has historically described.
func (service *CallbackService) recordNonInFlightCallbackReceived(
	callback CallbackCommand,
) string {
	backendLabel := sanitizeCallbackBackend(service.backends, callback.backendName)
	// CallbackCommand is the safe construction boundary: its unexported status
	// field can contain only the three closed protocol values, so no defensive
	// catch-all Prometheus series is reachable here.
	statusLabel := string(callback.status)
	if backendLabel == labelBackendInvalid {
		slog.Warn("sanitized callback label to bounded value",
			"lease_uuid", callback.leaseUUID,
			"received_backend", callback.backendName,
			"received_status", callback.status,
		)
	}
	metrics.NonInFlightCallbacksTotal.WithLabelValues(backendLabel, statusLabel).Inc()
	return statusLabel
}

// observeAuthorizedLifecycle validates a revocable, placement-store-backed
// lifecycle capability before publishing an observation. The zero ID reaches
// only an explicitly migrated legacy record; a zero selector can never match a
// typed current capability. Stale and missing capabilities are acknowledged as
// harmless no-ops so a backend can discard obsolete durable outbox records.
func (service *CallbackService) observeAuthorizedLifecycle(callback CallbackCommand) error {
	statusLabel := service.recordNonInFlightCallbackReceived(callback)
	outcomeLabel := metrics.LifecycleCallbackOutcomeRetryable
	verdictLabel := metrics.LifecycleCallbackVerdictUnavailable
	// Every lifecycle callback receives exactly one terminal classification.
	// Retryable is the fail-closed default: only an explicit application or
	// terminal no-op below may narrow it to applied or dropped.
	defer func() {
		metrics.LifecycleCallbackOutcomesTotal.WithLabelValues(
			outcomeLabel, verdictLabel, statusLabel,
		).Inc()
	}()

	if service.lifecycleAuthority == nil {
		return errCallbackLifecycleUnavailable
	}
	id := callback.lifecycleID
	authorization := service.lifecycleAuthority.AuthorizeLifecycle(callback.leaseUUID, id)
	verdictLabel = lifecycleCallbackVerdictLabel(authorization.Verdict())
	switch authorization.Verdict() {
	case placement.LifecycleVerdictAuthorized:
		// The durable capability, never body metadata, selects the backend.
		callback.backendName = authorization.Backend()
	case placement.LifecycleVerdictLegacy:
		if callback.selector != callbackSelectorLegacy {
			slog.Warn("ignoring typed lifecycle callback for legacy lease capability",
				"lease_uuid", callback.leaseUUID,
				"lifecycle_id", callback.lifecycleID,
			)
			outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
			return nil
		}
		callback.backendName = authorization.Backend()
	case placement.LifecycleVerdictTeardownOnly:
		if callback.status != backend.CallbackStatusDeprovisioned {
			slog.Info("ignoring runtime observation for teardown-only lifecycle capability",
				"lease_uuid", callback.leaseUUID,
				"lifecycle_id", callback.lifecycleID,
				"status", callback.status,
			)
			outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
			return nil
		}
		callback.backendName = authorization.Backend()
	case placement.LifecycleVerdictRetired:
		callback.backendName = authorization.Backend()
		if err := service.verifyStorageIdentity(callback, callback.backendName); err != nil {
			return err
		}
		slog.Info("ignoring duplicate callback for retired lifecycle capability",
			"lease_uuid", callback.leaseUUID,
			"backend", authorization.Backend(),
			"lifecycle_id", callback.lifecycleID,
		)
		outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
		return nil
	case placement.LifecycleVerdictInvalid,
		placement.LifecycleVerdictMissing,
		placement.LifecycleVerdictStale,
		placement.LifecycleVerdictUnusable:
		slog.Warn("ignoring callback without current lifecycle authority",
			"lease_uuid", callback.leaseUUID,
			"received_backend", callback.backendName,
			"lifecycle_id", callback.lifecycleID,
			"verdict", authorization.Verdict(),
		)
		outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
		return nil
	default:
		return fmt.Errorf("unknown lifecycle authorization verdict %d", authorization.Verdict())
	}
	if err := service.verifyStorageIdentity(callback, callback.backendName); err != nil {
		return err
	}

	// Consume terminal teardown authority before publishing its best-effort
	// observation. This is the exact CAS boundary: if a newer lifecycle rotated
	// between authorization and retirement, the stale teardown emits nothing;
	// racing duplicates publish at most once. A durable write failure remains
	// retryable and therefore returns 503 through the HTTP adapter.
	if callback.status == backend.CallbackStatusDeprovisioned {
		retired, err := service.lifecycleAuthority.RetireLifecycle(callback.leaseUUID, id)
		if err != nil {
			return fmt.Errorf("retire lifecycle capability for lease %s: %w", callback.leaseUUID, err)
		}
		if !retired.Retired() {
			verdictLabel = lifecycleCallbackVerdictLabel(retired.Verdict())
			slog.Info("lifecycle capability changed before terminal teardown retirement",
				"lease_uuid", callback.leaseUUID,
				"lifecycle_id", callback.lifecycleID,
				"verdict", retired.Verdict(),
			)
			outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
			return nil
		}
		if !retired.RetiredNow() {
			verdictLabel = metrics.LifecycleCallbackVerdictRetired
			outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
			return nil
		}
		callback.backendName = retired.Backend()
	}

	if callback.status == backend.CallbackStatusSuccess {
		// A lifecycle capability may report successful restart/update completion,
		// but it remains observation-only: it publishes Ready without settling an
		// operation or mutating durable placement/chain state. Provision and
		// restore success still require the exact operation route.
		outcomeLabel = metrics.LifecycleCallbackOutcomeApplied
		service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
		return nil
	}

	if service.observeLifecycle(callback) {
		outcomeLabel = metrics.LifecycleCallbackOutcomeApplied
	} else {
		outcomeLabel = metrics.LifecycleCallbackOutcomeDropped
	}
	return nil
}

func (service *CallbackService) verifyStorageIdentity(
	callback CallbackCommand,
	backendName string,
) error {
	if service.storageIdentities == nil {
		return errCallbackStorageIdentityUnavailable
	}
	if !callback.storageID.Valid() {
		return fmt.Errorf("%w for lease %s backend %s",
			errCallbackStorageIdentityMissing, callback.leaseUUID, backendName)
	}
	expected, bound := service.storageIdentities.ExpectedBackendStorageIdentity(backendName)
	if !bound || !expected.Valid() {
		return fmt.Errorf("%w for backend %s", errCallbackStorageIdentityUnbound, backendName)
	}
	if callback.storageID != expected {
		return fmt.Errorf("%w for lease %s backend %s: got %s, expected %s",
			errCallbackStorageIdentityMismatch, callback.leaseUUID, backendName,
			callback.storageID, expected)
	}
	return nil
}

// observeLifecycle owns only subscriber-visible status. It never claims an
// operation or mutates placement/chain state.
func (service *CallbackService) observeLifecycle(callback CallbackCommand) bool {
	switch callback.status {
	case backend.CallbackStatusFailed:
		service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, callback.failure)
		return true
	case backend.CallbackStatusDeprovisioned:
		if callback.retained {
			service.publish(callback.leaseUUID, backend.ProvisionStatusRetained, retainedLeaseNotice)
		}
		return true
	default:
		slog.Warn("unexpected callback status for non-in-flight lease",
			"lease_uuid", callback.leaseUUID,
			"status", callback.status,
		)
		return false
	}
}

func lifecycleCallbackVerdictLabel(verdict placement.LifecycleVerdict) string {
	switch verdict {
	case placement.LifecycleVerdictAuthorized:
		return metrics.LifecycleCallbackVerdictAuthorized
	case placement.LifecycleVerdictLegacy:
		return metrics.LifecycleCallbackVerdictLegacy
	case placement.LifecycleVerdictTeardownOnly:
		return metrics.LifecycleCallbackVerdictTeardownOnly
	case placement.LifecycleVerdictRetired:
		return metrics.LifecycleCallbackVerdictRetired
	case placement.LifecycleVerdictInvalid:
		return metrics.LifecycleCallbackVerdictInvalid
	case placement.LifecycleVerdictMissing:
		return metrics.LifecycleCallbackVerdictMissing
	case placement.LifecycleVerdictStale:
		return metrics.LifecycleCallbackVerdictStale
	case placement.LifecycleVerdictUnusable:
		return metrics.LifecycleCallbackVerdictUnusable
	default:
		return metrics.LifecycleCallbackVerdictUnknown
	}
}

// handleDeprovisionOwned consumes exact placement evidence without touching
// chain state while close owns the process-local operation. Returning 2xx
// without this durable mutation would let the backend delete its only exact
// completion while Deprovision later removes the volatile Registry record,
// leaving an Attempt that absence is intentionally forbidden to clear.
//
// Legacy callbacks remain observation-only: their tokenless URL does not carry
// the exact operation capability required to mutate durable placement.
func (service *CallbackService) handleDeprovisionOwned(
	callback CallbackCommand,
	record operation.Record,
) error {
	if callback.selector == callbackSelectorOperation {
		if service.placement == nil {
			return errCallbackPlacementUnavailable
		}
		var (
			applied bool
			err     error
		)
		switch callback.status {
		case backend.CallbackStatusSuccess:
			applied, err = service.placement.ConfirmOperation(
				callback.leaseUUID, record.Backend, record.ID,
			)
		case backend.CallbackStatusFailed:
			applied, err = service.placement.RefuseOperation(
				callback.leaseUUID, record.Backend, record.ID,
			)
		default:
			return fmt.Errorf("invalid exact callback status %q during deprovision", callback.status)
		}
		if err != nil {
			return fmt.Errorf("settle deprovision-owned placement attempt for lease %s: %w",
				callback.leaseUUID, err)
		}
		slog.Info("settled exact placement evidence while deprovision owns operation",
			"lease_uuid", callback.leaseUUID,
			"backend", record.Backend,
			"operation_id", record.ID,
			"status", callback.status,
			"applied", applied,
		)
	}

	switch callback.status {
	case backend.CallbackStatusDeprovisioned:
		service.forgetDeprovisionCandidate(callback.leaseUUID, record.Backend)
		if callback.retained {
			service.publish(callback.leaseUUID, backend.ProvisionStatusRetained, retainedLeaseNotice)
		}
	case backend.CallbackStatusFailed:
		service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, callback.failure)
	case backend.CallbackStatusSuccess:
		metrics.CallbackDeprovisionOwnedSuccessTotal.Inc()
		slog.Warn("ignoring success callback emitted while deprovision owns the operation",
			"lease_uuid", callback.leaseUUID,
			"backend", record.Backend,
			"operation_id", record.ID,
		)
	}
	slog.Info("observed callback for deprovision-owned in-flight operation",
		"lease_uuid", callback.leaseUUID,
		"backend", record.Backend,
		"operation_id", record.ID,
		"status", callback.status,
		"retained", callback.retained,
	)
	return nil
}

func (service *CallbackService) publish(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	if service.events == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				metrics.LifecycleEventCallback,
			).Inc()
			slog.Error("callback lifecycle event sink panicked; preserving terminal callback settlement",
				"lease_uuid", leaseUUID,
				"status", status,
				"event", metrics.LifecycleEventCallback,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	service.events.PublishCallbackLeaseEvent(leaseUUID, status, errMsg)
}

func (service *CallbackService) forgetDeprovisionCandidate(leaseUUID, backendName string) {
	if service.deprovisionObserver != nil {
		service.deprovisionObserver.ObserveCallbackDeprovisioned(leaseUUID, backendName)
	}
}

func callbackOperationLabel(kind operation.Kind) string {
	if kind == operation.KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}

// isPermanentPlacementVerdict reports semantic placement decisions that the
// same callback can never repair by retrying. They remain distinct from
// storage/I/O failures for operator metrics.
func isPermanentPlacementVerdict(err error) bool {
	return errors.Is(err, placement.ErrInvalidPlacement) ||
		errors.Is(err, placement.ErrAttemptConflict) ||
		errors.Is(err, placement.ErrBackendConflict) ||
		errors.Is(err, placement.ErrUnusablePlacement) ||
		errors.Is(err, placement.ErrAttemptMismatch)
}

func sanitizeCallbackBackend(resolver CallbackBackendResolver, name string) string {
	if name == "" {
		return labelBackendUnknown
	}
	if resolver == nil || resolver.GetBackendByName(name) == nil {
		return labelBackendInvalid
	}
	return name
}

var (
	_ CallbackApplication = (*CallbackService)(nil)
	_ CallbackOperations  = (*operation.Registry)(nil)
)
