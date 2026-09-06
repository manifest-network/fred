package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

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
		backendLabel := sanitizeCallbackBackend(service.backends, callback.BackendName())
		metrics.NonInFlightCallbacksTotal.WithLabelValues(
			backendLabel, string(callback.Status()),
		).Inc()
	}
	if result.LifecycleOutcome() != placement.CallbackLifecycleNone {
		metrics.LifecycleCallbackOutcomesTotal.WithLabelValues(
			callbackLifecycleOutcomeLabel(result.LifecycleOutcome()),
			lifecycleCallbackVerdictLabel(result.LifecycleVerdict()),
			string(callback.Status()),
		).Inc()
	}
	if result.OperationOutcome() != placement.CallbackOperationNone {
		backendName := result.AuthoritativeBackend()
		operationLabel := callbackOperationLabel(result.OperationKind())
		if !result.OperationStartedAt().IsZero() {
			metrics.ProvisioningDuration.WithLabelValues(
				backendName, operationLabel,
			).Observe(time.Since(result.OperationStartedAt()).Seconds())
		}
		outcome := metrics.OutcomeFailed
		if result.OperationOutcome() == placement.CallbackOperationSucceeded {
			outcome = metrics.OutcomeSuccess
		}
		metrics.ProvisioningTotal.WithLabelValues(outcome, backendName, operationLabel).Inc()
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
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				metrics.LifecycleEventCallback,
			).Inc()
			slog.Error("callback lifecycle event sink panicked; preserving terminal callback settlement",
				"lease_uuid", leaseUUID, "status", status,
				"event", metrics.LifecycleEventCallback, "panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	service.events.PublishCallbackLeaseEvent(leaseUUID, status, errMsg)
}

func callbackLifecycleOutcomeLabel(outcome placement.CallbackLifecycleOutcome) string {
	switch outcome {
	case placement.CallbackLifecycleApplied:
		return metrics.LifecycleCallbackOutcomeApplied
	case placement.CallbackLifecycleDropped:
		return metrics.LifecycleCallbackOutcomeDropped
	default:
		return metrics.LifecycleCallbackOutcomeRetryable
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

func callbackOperationLabel(kind operation.Kind) string {
	if kind == operation.KindRestore {
		return metrics.OperationRestore
	}
	return metrics.OperationProvision
}

func sanitizeCallbackBackend(catalog CallbackBackendCatalog, name string) string {
	if name == "" {
		return labelBackendUnknown
	}
	if catalog == nil || !catalog.HasBackend(name) {
		return labelBackendInvalid
	}
	return name
}

var _ CallbackApplication = (*CallbackService)(nil)
