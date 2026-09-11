// Package restore coordinates restoration of a fresh lease from retained data.
//
// The package is a transport adapter: transports authenticate and parse
// requests, while placement.RestoreCoordinator owns source authorization,
// routing, operation registration, durable write-ahead placement, the backend
// call, and settlement. Neither HTTP status codes nor raw placement revisions
// cross this boundary.
package restore

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

// Outcome is the transport-independent result of a restore command. Its zero
// value is deliberately not success, so an unhandled result fails closed.
type Outcome uint8

const (
	OutcomeInvalid Outcome = iota
	OutcomeAccepted
	OutcomeTargetNotPending
	OutcomeSourceNotFound
	OutcomeSourceUnavailable
	OutcomeAlreadyInProgress
	OutcomeServiceUnavailable
	OutcomeNotRetained
	OutcomeBackendInvalidState
	OutcomeInsufficientResources
	OutcomeBackendUnavailable
	OutcomeTierTooSmall
	OutcomeInvalidRequest
	OutcomeBackendMalformedResponse
	OutcomeInternalFailure
)

// Result is the typed result contract returned by Service. Detail contains
// only detail extracted from a backend error envelope; transports must still
// sanitize and bound it before relaying it to a tenant. Cause is available for
// operator logging and must never be written directly to a tenant response.
type Result struct {
	Outcome     Outcome
	BackendName string
	detail      string
	cause       error
}

// Accepted reports whether the backend accepted asynchronous restore work.
func (result Result) Accepted() bool { return result.Outcome == OutcomeAccepted }

// Detail returns endpoint-provided detail from a validated error envelope.
func (result Result) Detail() string { return result.detail }

// Cause returns the operator-facing error underlying a non-success result.
func (result Result) Cause() error { return result.cause }

// Command contains authenticated restore identities, never a trusted lease
// snapshot. Service authorizes SourceLeaseUUID before acquiring any exclusive
// capability, then re-reads TargetLeaseUUID under exact source and target
// lifecycle claims before deriving backend request data.
type Command struct {
	TargetLeaseUUID string
	Tenant          string
	SourceLeaseUUID string
}

// LeaseReader supplies authoritative chain ownership and target-state reads.
// It aliases the dependency bound into placement.RestoreCoordinator so
// adapters can name the port without creating a second composition seam.
type LeaseReader = placement.RestoreLeaseReader

// EventSink receives ordered best-effort lifecycle hints around a dispatched
// restore, including terminal compensation for a synchronous refusal.
type EventSink interface {
	Publish(event backend.LeaseStatusEvent)
}

// Config contains immutable service dependencies.
type Config struct {
	Coordinator *placement.RestoreCoordinator
	Events      EventSink
}

// Service maps the construction-bound restore application's closed result to
// the API-facing result and publishes best-effort lifecycle hints.
type Service struct {
	coordinator *placement.RestoreCoordinator
	events      EventSink
}

// NewService validates the capability graph up front. A partially wired
// restore path cannot be constructed and therefore cannot contact a backend.
func NewService(config Config) (*Service, error) {
	if config.Coordinator == nil || !config.Coordinator.Valid() {
		return nil, errors.New("restore chain/runtime-bound coordinator is required")
	}

	events := config.Events
	if util.IsNilInterface(events) {
		events = nil
	}
	return &Service{
		coordinator: config.Coordinator,
		events:      events,
	}, nil
}

// Execute delegates the complete restore transaction to the bound application
// capability, then projects its typed result for the transport layer.
func (service *Service) Execute(ctx context.Context, command Command) Result {
	request, err := placement.NewRestoreApplicationRequest(
		command.TargetLeaseUUID, command.Tenant, command.SourceLeaseUUID,
	)
	if err != nil {
		return Result{Outcome: OutcomeInvalid, cause: err}
	}
	execution := service.coordinator.ExecuteApplication(ctx, request)
	backendName := execution.BackendName()
	if errors.Is(execution.CallErr(), backend.ErrInsufficientResources) {
		verdict := metrics.CapacityVerdictAmbiguous
		if execution.Refusal() == backend.RestoreRefusalCapacity {
			verdict = metrics.CapacityVerdictCodedRefusal
		}
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendName, verdict).Inc()
	}
	if execution.Disposition() == placement.RestoreApplicationAccepted {
		if execution.Err() != nil {
			slog.Error("accepted restore retained durable recovery evidence",
				"lease_uuid", command.TargetLeaseUUID, "backend", backendName,
				"error", execution.Err())
		}
		return service.accepted(command, backendName)
	}
	result := Result{BackendName: backendName, cause: execution.Err()}
	switch execution.Disposition() {
	case placement.RestoreApplicationInvalid:
		result.Outcome = OutcomeInvalid
	case placement.RestoreApplicationTargetNotPending:
		result.Outcome = OutcomeTargetNotPending
	case placement.RestoreApplicationSourceNotFound:
		result.Outcome = OutcomeSourceNotFound
	case placement.RestoreApplicationSourceUnavailable:
		result.Outcome = OutcomeSourceUnavailable
	case placement.RestoreApplicationAlreadyInProgress:
		result.Outcome = OutcomeAlreadyInProgress
	case placement.RestoreApplicationNotRetained:
		result.Outcome = OutcomeNotRetained
	case placement.RestoreApplicationBackendRejected:
		result = classifyBackendResult(execution)
		result.BackendName = backendName
	default:
		result.Outcome = OutcomeServiceUnavailable
	}
	if execution.DefinitivelyRefused() {
		service.publishEventBestEffort(backendName, metrics.LifecycleEventRestoreRefused,
			backend.LeaseStatusEvent{
				LeaseUUID: command.TargetLeaseUUID,
				Status:    backend.ProvisionStatusFailed, Error: "restore did not start",
				Timestamp: time.Now(),
			})
	}
	return result
}

func (service *Service) accepted(command Command, backendName string) Result {
	slog.Info("lease restore initiated",
		"lease_uuid", command.TargetLeaseUUID,
		"from_lease", command.SourceLeaseUUID,
		"tenant", command.Tenant,
		"backend", backendName,
	)
	return Result{Outcome: OutcomeAccepted, BackendName: backendName}
}

func (service *Service) publishEventBestEffort(
	backendName string,
	eventName string,
	event backend.LeaseStatusEvent,
) {
	if service.events == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				eventName,
			).Inc()
			slog.Error("restore lifecycle event sink panicked; continuing lifecycle settlement",
				"lease_uuid", event.LeaseUUID,
				"backend", backendName,
				"event", eventName,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	service.events.Publish(event)
}

// classifyBackendResult accepts the opaque placement result rather than a raw
// error plus a freely pairable enum. RestoreApplicationResult's fields are
// private to placement, so only that causal boundary can supply a refusal.
func classifyBackendResult(execution placement.RestoreApplicationResult) Result {
	err := execution.CallErr()
	result := Result{Outcome: OutcomeInternalFailure, cause: err}
	if detail, ok := backend.Detail(err); ok {
		result.detail = detail
	}
	switch execution.Refusal() {
	case backend.RestoreRefusalNotRetained:
		result.Outcome = OutcomeNotRetained
		return result
	case backend.RestoreRefusalInvalidState:
		result.Outcome = OutcomeBackendInvalidState
		return result
	case backend.RestoreRefusalCapacity:
		result.Outcome = OutcomeInsufficientResources
		return result
	case backend.RestoreRefusalDemoteDataExceedsTier:
		result.Outcome = OutcomeTierTooSmall
		return result
	case backend.RestoreRefusalValidation:
		result.Outcome = OutcomeInvalidRequest
		return result
	}
	// Only retryable availability projections remain error-derived. Permanent
	// tenant and lease-state verdicts require the sealed refusal above. In
	// particular, a legacy/custom backend can return any public sentinel; its
	// conservative ambiguous outcome must not turn that string-shaped evidence
	// into a tenant-facing statement about backend state.
	switch {
	case errors.Is(err, backend.ErrInsufficientResources):
		result.Outcome = OutcomeInsufficientResources
	case errors.Is(err, backend.ErrCircuitOpen):
		result.Outcome = OutcomeBackendUnavailable
	case errors.Is(err, backend.ErrBackendUpgradeRequired),
		errors.Is(err, backend.ErrBackendStorageIdentityUnbound):
		result.Outcome = OutcomeBackendUnavailable
	case errors.Is(err, backend.ErrMalformedErrorBody):
		result.Outcome = OutcomeBackendMalformedResponse
	}
	return result
}
