package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

const retainedLeaseNotice = "your lease data was retained and can be restored within the grace window: create a fresh PENDING lease of matching shape, then POST /v1/leases/{new_lease_uuid}/restore with from_lease_uuid set to this lease's UUID"

var (
	errCallbackSettlementClaimTimeout  = errors.New("timed out waiting for callback settlement claim")
	errCallbackOperationsUnavailable   = errors.New("backend callback operation registry is unavailable")
	errCallbackChainUnavailable        = errors.New("backend callback chain client is unavailable")
	errCallbackAcknowledgerUnavailable = errors.New("backend callback acknowledger is unavailable")
	errCallbackPlacementUnavailable    = errors.New("backend callback placement authority is unavailable")
	errCallbackSettlementLost          = errors.New("backend callback lost its operation settlement claim")
	errInvalidCallbackCommand          = errors.New("backend callback command was not constructed by NewCallbackCommand")
)

// CallbackApplication is the application boundary used by the Watermill
// adapter. The handler owns decoding and delivery metrics; this service owns
// callback authorization and lifecycle policy.
type CallbackApplication interface {
	HandleCallback(context.Context, CallbackCommand) error
}

// CallbackCommand is the validated application input produced at the
// authenticated transport boundary. Its fields are deliberately private: the
// callback DTO's raw string cannot reach registry or placement consumers.
// Missing operation identity remains an explicit legacy case, while a present
// identity is always a valid opaque OperationID. backendName is observational
// metadata only: v0.13 documented it as a metrics label, so lifecycle authority
// comes from the HMAC-authenticated callback URL and exact current operation,
// never from a backend name asserted in the JSON body.
type CallbackCommand struct {
	leaseUUID   string
	status      backend.CallbackStatus
	failure     string
	backendName string
	retained    bool
	operationID operation.OperationID
	token       bool
	valid       bool
}

// NewCallbackCommand converts the wire-compatible callback DTO into typed
// application input. An empty OperationID means the query parameter was absent;
// the HTTP boundary rejects every explicitly present malformed value before
// this constructor is called.
func NewCallbackCommand(callback backend.CallbackPayload) (CallbackCommand, error) {
	command := CallbackCommand{
		leaseUUID:   callback.LeaseUUID,
		status:      callback.Status,
		failure:     callback.Error,
		backendName: callback.Backend,
		retained:    callback.Retained,
		valid:       true,
	}
	if callback.OperationID == "" {
		return command, nil
	}
	id, err := operation.ParseID(callback.OperationID)
	if err != nil {
		return CallbackCommand{}, fmt.Errorf("parse callback operation ID: %w", err)
	}
	command.operationID = id
	command.token = true
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
}

// CallbackChain is the chain read/write surface used by failure settlement.
type CallbackChain interface {
	GetLease(context.Context, string) (*billingtypes.Lease, error)
	RejectLeases(context.Context, []string, string) (uint64, []string, error)
}

// CallbackPlacement is the durable placement surface used at callback time.
// Both methods require the exact typed operation identity persisted before the
// backend call, so a delayed same-backend callback cannot settle a newer
// attempt.
type CallbackPlacement interface {
	ConfirmOperation(string, string, operation.OperationID) (bool, error)
	RefuseOperation(string, string, operation.OperationID) (bool, error)
}

// CallbackPayloadStore is the terminal payload cleanup capability.
type CallbackPayloadStore interface {
	Delete(string)
}

// CallbackEventSink receives best-effort status events. Its implementation is
// intentionally outside the application service so Watermill publishing does
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

// CallbackServiceConfig wires the callback application service. Operations is
// mandatory. Other capabilities are checked at the first path that needs them,
// which permits status-only callbacks during partial startup and keeps missing
// mutation authority fail-closed.
type CallbackServiceConfig struct {
	Operations          CallbackOperations
	Chain               CallbackChain
	Acknowledger        Acknowledger
	Placement           CallbackPlacement
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
	payloads            CallbackPayloadStore
	events              CallbackEventSink
	backends            CallbackBackendResolver
	deprovisionObserver CallbackDeprovisionObserver
	claimPollInterval   time.Duration
	claimMaxWait        time.Duration
}

// NewCallbackService constructs a callback application service. It rejects a
// missing operation registry because no callback, including a tokenless legacy
// callback, may authorize lifecycle mutation without a current operation.
func NewCallbackService(cfg CallbackServiceConfig) (*CallbackService, error) {
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
	record, exists := service.operations.Lookup(callback.leaseUUID)
	if !exists {
		service.observeNonInFlight(callback)
		return nil
	}

	if !callbackMatchesOperation(command, record) {
		if !callback.token {
			// A tokenless callback can only be a best-effort lifecycle
			// observation. In particular, it must not claim or finish this
			// token-required operation. Still publish it: docker-backend can
			// observe a runtime failure immediately after its exact success
			// callback receives HTTP 200, before asynchronous application
			// settlement has retired the operation record.
			service.observeTokenlessLifecycle(callback)
			return nil
		}
		slog.Warn("ignoring callback whose operation token does not match the in-flight operation",
			"lease_uuid", callback.leaseUUID,
			"callback_operation_id", callback.operationID,
			"operation_id", record.ID,
			"token_required", record.TokenRequired,
		)
		return nil
	}

	if record.Settlement == operation.SettlementDeprovision {
		service.observeDeprovisionOwned(callback, record)
		return nil
	}

	result, observed, deprovisionOwned, err := service.waitForSettlementClaim(ctx, record)
	if err != nil {
		return fmt.Errorf("wait to settle callback for lease %s: %w", callback.leaseUUID, err)
	}
	if deprovisionOwned {
		service.observeDeprovisionOwned(callback, observed)
		return nil
	}
	if !result.Claimed() {
		// The exact operation completed or was replaced while the callback waited.
		return nil
	}

	return service.settleClaimed(ctx, callback, result.Record(), result.Claim())
}

func callbackMatchesOperation(command CallbackCommand, record operation.Record) bool {
	if !command.token {
		return !record.TokenRequired
	}
	return command.operationID == record.ID
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
	finish := func() error {
		if !service.operations.FinishSettlement(claim) {
			return fmt.Errorf("%w for lease %s operation %s",
				errCallbackSettlementLost, record.LeaseUUID, record.ID)
		}
		finished = true
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
				if err := finish(); err != nil {
					return err
				}
				recordDuration()
				metrics.ProvisioningTotal.WithLabelValues(
					metrics.OutcomeSuccess, record.Backend, callbackOperationLabel(record.Kind),
				).Inc()
				service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
				slog.Info("lease already acknowledged, skipping",
					"lease_uuid", callback.leaseUUID,
					"tenant", record.Tenant,
				)
				return nil
			}
			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.leaseUUID,
				"tenant", record.Tenant,
				"error", acknowledgeErr,
			)
			return fmt.Errorf("%w: lease %s: %w", ErrAcknowledgeFailed, callback.leaseUUID, acknowledgeErr)
		}
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeSuccess, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
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
		lease, leaseErr := service.chain.GetLease(ctx, callback.leaseUUID)
		if leaseErr != nil {
			return fmt.Errorf("failed to fetch lease %s: %w", callback.leaseUUID, leaseErr)
		}
		if lease != nil && lease.State == billingtypes.LEASE_STATE_ACTIVE {
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
			if err := finish(); err != nil {
				return err
			}
			recordDuration()
			metrics.ProvisioningTotal.WithLabelValues(
				metrics.OutcomeFailed, record.Backend, callbackOperationLabel(record.Kind),
			).Inc()
			service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, reason)
			slog.Warn("re-provision failed for active lease, deferring to reconciler",
				"lease_uuid", callback.leaseUUID,
				"tenant", record.Tenant,
				"reason", reason,
			)
			return nil
		}

		rejected, txHashes, rejectErr := service.chain.RejectLeases(
			ctx, []string{callback.leaseUUID}, truncateRejectReason(reason),
		)
		if rejectErr != nil {
			return fmt.Errorf("failed to reject lease %s: %w", callback.leaseUUID, rejectErr)
		}
		if service.payloads != nil {
			service.payloads.Delete(callback.leaseUUID)
		}
		applied, placementErr := service.placement.RefuseOperation(
			callback.leaseUUID, record.Backend, record.ID,
		)
		if placementErr != nil {
			slog.Warn("failed to clean up rejected lease placement operation",
				"lease_uuid", callback.leaseUUID,
				"backend", record.Backend,
				"operation_id", record.ID,
				"error", placementErr,
			)
		} else if !applied {
			slog.Info("rejected callback did not match a durable placement attempt; preserving placement",
				"lease_uuid", callback.leaseUUID,
				"backend", record.Backend,
				"operation_id", record.ID,
			)
		}
		if err := finish(); err != nil {
			return err
		}
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(
			metrics.OutcomeFailed, record.Backend, callbackOperationLabel(record.Kind),
		).Inc()
		service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, reason)
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

func (service *CallbackService) observeNonInFlight(callback CallbackCommand) {
	backendLabel := sanitizeCallbackBackend(service.backends, callback.backendName)
	statusLabel := sanitizeCallbackStatus(callback.status)
	if backendLabel == labelBackendInvalid || statusLabel == labelStatusOther {
		slog.Warn("sanitized callback label to bounded value",
			"lease_uuid", callback.leaseUUID,
			"received_backend", callback.backendName,
			"received_status", callback.status,
		)
	}
	metrics.NonInFlightCallbacksTotal.WithLabelValues(backendLabel, statusLabel).Inc()
	if callback.token {
		// An operation-scoped callback is authority only while its exact
		// operation is still present. Once that capability is gone, the same
		// payload is indistinguishable from a delayed result for an older
		// attempt; publishing Ready/Failed (or retiring teardown state) would let
		// stale wire data overwrite the current lifecycle.
		slog.Warn("ignoring callback for an operation that is no longer current",
			"lease_uuid", callback.leaseUUID,
			"backend", callback.backendName,
			"operation_id", callback.operationID,
			"status", callback.status,
		)
		return
	}
	service.observeTokenlessLifecycle(callback)
	slog.Info("published status event for non-in-flight callback",
		"lease_uuid", callback.leaseUUID,
		"operation_id", callback.operationID,
		"status", callback.status,
	)
}

// observeTokenlessLifecycle applies only the observation surface shared with
// v0.13 callbacks. It never claims an operation, mutates placement/chain state,
// or retires a deprovision candidate, so calling it while a typed operation is
// current cannot accidentally settle that operation.
func (service *CallbackService) observeTokenlessLifecycle(callback CallbackCommand) {
	switch callback.status {
	case backend.CallbackStatusSuccess:
		service.publish(callback.leaseUUID, backend.ProvisionStatusReady, "")
	case backend.CallbackStatusFailed:
		service.publish(callback.leaseUUID, backend.ProvisionStatusFailed, callback.failure)
	case backend.CallbackStatusDeprovisioned:
		// A tokenless callback with no current operation is a v0.13 status
		// observation only. Its body-supplied backend was historically metrics
		// metadata, not lifecycle authority, so it cannot retire a current
		// process-local deprovision retry candidate.
		if callback.retained {
			service.publish(callback.leaseUUID, backend.ProvisionStatusRetained, retainedLeaseNotice)
		}
	default:
		slog.Warn("unexpected callback status for non-in-flight lease",
			"lease_uuid", callback.leaseUUID,
			"status", callback.status,
		)
		return
	}
}

func (service *CallbackService) observeDeprovisionOwned(
	callback CallbackCommand,
	record operation.Record,
) {
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
}

func (service *CallbackService) publish(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	if service.events != nil {
		service.events.PublishCallbackLeaseEvent(leaseUUID, status, errMsg)
	}
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
