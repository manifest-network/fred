package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

const retainedLeaseNotice = "your lease data was retained and can be restored within the grace window: create a fresh PENDING lease of matching shape, then POST /v1/leases/{new_lease_uuid}/restore with from_lease_uuid set to this lease's UUID"

var (
	errCallbackOperationsUnavailable   = errors.New("backend callback operation coordinator is unavailable")
	errCallbackChainUnavailable        = placement.ErrCallbackChainUnavailable
	errCallbackAcknowledgerUnavailable = placement.ErrCallbackAcknowledgerUnavailable
	errCallbackRecoveryLeaseBusy       = placement.ErrCallbackRecoveryBusy
	errCallbackStorageIdentityMissing  = placement.ErrCallbackStorageIdentityMissing
	errCallbackStorageIdentityMismatch = placement.ErrCallbackStorageIdentityMismatch
)

// CallbackApplication accepts only opaque evidence minted by the HMAC
// verifier. A decoded DTO is intentionally not an application capability.
type CallbackApplication interface {
	HandleCallback(context.Context, hmacauth.VerifiedRequest) error
}

// CallbackChain is retained as the provisioner-facing name for the exact
// chain capability now bound inside the placement coordinator.
type CallbackChain = placement.CallbackChain

type CallbackPayloadStore interface{ Delete(string) }

type CallbackEventSink interface {
	PublishCallbackLeaseEvent(string, backend.ProvisionStatus, string)
}

type callbackEventSinkFunc func(string, backend.ProvisionStatus, string)

func (publish callbackEventSinkFunc) PublishCallbackLeaseEvent(
	leaseUUID string, status backend.ProvisionStatus, errMsg string,
) {
	publish(leaseUUID, status, errMsg)
}

// CallbackBackendCatalog is observational: it keeps metric labels bounded and
// never exposes a backend client or selects settlement authority.
type CallbackBackendCatalog interface {
	HasBackend(string) bool
}

var _ CallbackBackendCatalog = (*backend.Router)(nil)

type CallbackDeprovisionObserver interface {
	ObserveCallbackDeprovisioned(string, string)
}

// CallbackServiceConfig contains only observational consequence sinks. Chain,
// acknowledgement, Registry, and Store authority are already inseparably bound
// inside Coordinator.
type CallbackServiceConfig struct {
	Coordinator         *placement.AuthenticatedCallbackCoordinator
	Payloads            CallbackPayloadStore
	Events              CallbackEventSink
	Backends            CallbackBackendCatalog
	DeprovisionObserver CallbackDeprovisionObserver
}

// CallbackService applies the non-authoritative consequences returned after
// the coordinator has completed or declined settlement.
type CallbackService struct {
	coordinator         *placement.AuthenticatedCallbackCoordinator
	payloads            CallbackPayloadStore
	events              CallbackEventSink
	backends            CallbackBackendCatalog
	deprovisionObserver CallbackDeprovisionObserver
}

func NewCallbackService(cfg CallbackServiceConfig) (*CallbackService, error) {
	if cfg.Coordinator == nil || !cfg.Coordinator.Valid() {
		return nil, errCallbackOperationsUnavailable
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
	return &CallbackService{
		coordinator: cfg.Coordinator, payloads: cfg.Payloads,
		events: cfg.Events, backends: cfg.Backends,
		deprovisionObserver: cfg.DeprovisionObserver,
	}, nil
}

func (service *CallbackService) HandleCallback(
	ctx context.Context,
	request hmacauth.VerifiedRequest,
) error {
	result, err := service.coordinator.Apply(ctx, request)
	service.observeResult(result)
	if errors.Is(err, placement.ErrCallbackAcknowledgeFailed) {
		return fmt.Errorf("%w: %w", ErrAcknowledgeFailed, err)
	}
	return err
}

func (service *CallbackService) observeResult(result placement.CallbackResult) {
	callback := result.Observation()
	if result.NonInFlight() {
		statuses := nonInFlightCallbacks(service.backends, callback.BackendName())
		if counter, ok := callbackStatusCounter(statuses, callback.Status()); ok {
			counter.Inc()
		}
	}
	if result.LifecycleOutcome() != placement.CallbackLifecycleNone {
		verdicts := callbackLifecycleOutcomeCounters(result.LifecycleOutcome())
		statuses := lifecycleCallbackVerdictCounters(verdicts, result.LifecycleVerdict())
		if counter, ok := callbackStatusCounter(statuses, callback.Status()); ok {
			counter.Inc()
		}
	}
	if result.OperationOutcome() != placement.CallbackOperationNone {
		backendName := result.AuthoritativeBackend()
		operationLabel := callbackOperationLabel(result.OperationKind())
		if !result.OperationStartedAt().IsZero() {
			metrics.ProvisioningDuration.WithLabelValues(
				backendName, operationLabel,
			).Observe(time.Since(result.OperationStartedAt()).Seconds())
		}
		outcomes := provisioningOutcomes(metrics.ForBackend(backendName).Provisioning, result.OperationKind())
		outcome := outcomes.Failed
		if result.OperationOutcome() == placement.CallbackOperationSucceeded {
			outcome = outcomes.Success
		}
		outcome.Inc()
	}
	if result.DeletePayload() && service.payloads != nil {
		service.payloads.Delete(callback.LeaseUUID())
	}
	if backendName, ok := result.DeprovisionedBackend(); ok && service.deprovisionObserver != nil {
		service.deprovisionObserver.ObserveCallbackDeprovisioned(callback.LeaseUUID(), backendName)
	}
	if result.DeprovisionOwnedSuccess() {
		metrics.CallbackDeprovisionOwnedSuccessTotal.Inc()
	}
	if result.ClaimWaitTimedOut() {
		metrics.CallbackSettlementClaimWaitTimeoutsTotal.Inc()
	}
	if status, failure, ok := result.Event(); ok {
		if status == backend.ProvisionStatusRetained {
			failure = retainedLeaseNotice
		}
		service.publish(callback.LeaseUUID(), status, failure)
	}
}

func (service *CallbackService) publish(
	leaseUUID string, status backend.ProvisionStatus, errMsg string,
) {
	if service.events == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanics.Callback.Inc()
			slog.Error("callback lifecycle event sink panicked; preserving terminal callback settlement",
				"lease_uuid", leaseUUID, "status", status,
				"event", metrics.LifecycleEventCallback, "panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	service.events.PublishCallbackLeaseEvent(leaseUUID, status, errMsg)
}

func callbackLifecycleOutcomeCounters(outcome placement.CallbackLifecycleOutcome) metrics.LifecycleCallbackVerdictCounters {
	switch outcome {
	case placement.CallbackLifecycleApplied:
		return metrics.LifecycleCallbackOutcomes.Applied
	case placement.CallbackLifecycleDropped:
		return metrics.LifecycleCallbackOutcomes.Dropped
	default:
		return metrics.LifecycleCallbackOutcomes.Retryable
	}
}

func lifecycleCallbackVerdictCounters(
	verdicts metrics.LifecycleCallbackVerdictCounters, verdict placement.LifecycleVerdict,
) metrics.CallbackStatusCounters {
	switch verdict {
	case placement.LifecycleVerdictAuthorized:
		return verdicts.Authorized
	case placement.LifecycleVerdictLegacy:
		return verdicts.Legacy
	case placement.LifecycleVerdictTeardownOnly:
		return verdicts.TeardownOnly
	case placement.LifecycleVerdictRetired:
		return verdicts.Retired
	case placement.LifecycleVerdictInvalid:
		return verdicts.Invalid
	case placement.LifecycleVerdictMissing:
		return verdicts.Missing
	case placement.LifecycleVerdictStale:
		return verdicts.Stale
	case placement.LifecycleVerdictUnusable:
		return verdicts.Unusable
	default:
		return verdicts.Unknown
	}
}

// callbackStatusCounter is the series of a callback status. The callback wire
// admits only these three statuses, so every observation has one.
func callbackStatusCounter(
	statuses metrics.CallbackStatusCounters, status backend.CallbackStatus,
) (prometheus.Counter, bool) {
	switch status {
	case backend.CallbackStatusSuccess:
		return statuses.Success, true
	case backend.CallbackStatusFailed:
		return statuses.Failed, true
	case backend.CallbackStatusDeprovisioned:
		return statuses.Deprovisioned, true
	default:
		return nil, false
	}
}

func provisioningOutcomes(
	operations metrics.ProvisioningCounters, kind operation.Kind,
) metrics.ProvisioningOutcomeCounters {
	if kind == operation.KindRestore {
		return operations.Restore
	}
	return operations.Provision
}

func callbackOperationLabel(kind operation.Kind) string {
	if kind == operation.KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}

// nonInFlightCallbacks is the reporting backend's series, or the shared
// unknown or invalid ones when the claimed name is empty or not configured,
// so a callback cannot mint a series.
func nonInFlightCallbacks(catalog CallbackBackendCatalog, name string) metrics.CallbackStatusCounters {
	if name == "" {
		return metrics.NonInFlightCallbacksUnknownBackend
	}
	if catalog == nil || !catalog.HasBackend(name) {
		return metrics.NonInFlightCallbacksInvalidBackend
	}
	return metrics.ForBackend(name).NonInFlightCallbacks
}

var _ CallbackApplication = (*CallbackService)(nil)
