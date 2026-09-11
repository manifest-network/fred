// Package maintenance is the authenticated API adapter for the
// construction-bound placement maintenance application.
package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// Kind is the closed set of public maintenance commands.
type Kind uint8

const (
	KindInvalid Kind = iota
	KindRestart
	KindUpdate
)

// Command is the authenticated input supplied by the API boundary.
type Command struct {
	ID        maintenanceid.ID
	LeaseUUID string
	Tenant    string
	Kind      Kind
	Payload   []byte
}

// Outcome is the exhaustive API-facing result. Zero is invalid.
type Outcome uint8

const (
	OutcomeInvalid Outcome = iota
	OutcomeAccepted
	OutcomeNotFound
	OutcomeNoLongerActive
	OutcomeForbidden
	OutcomeAlreadyInProgress
	OutcomeCommandConflict
	OutcomeBackendInvalidState
	OutcomeBackendValidation
	OutcomeServiceUnavailable
	OutcomeInternalFailure
)

type Result struct {
	outcome Outcome
	cause   error
	detail  string
}

func (result Result) Outcome() Outcome { return result.outcome }
func (result Result) Detail() string   { return result.detail }
func (result Result) Cause() error     { return result.cause }

// NewResult is a test-only constructor for cross-package API service fakes.
// This deliberately supported exception preserves the closed, validated Result
// contract without exposing its fields or weakening the consumer's service port.
// It grants diagnostic output only, never settlement or mutation authority.
func NewResult(outcome Outcome, cause error) Result {
	switch outcome {
	case OutcomeAccepted, OutcomeNotFound, OutcomeNoLongerActive, OutcomeForbidden,
		OutcomeAlreadyInProgress, OutcomeCommandConflict, OutcomeBackendInvalidState,
		OutcomeBackendValidation, OutcomeServiceUnavailable, OutcomeInternalFailure:
		return Result{outcome: outcome, cause: cause}
	default:
		return Result{outcome: OutcomeInternalFailure, cause: errors.New("invalid maintenance result")}
	}
}

type OrderedEvents interface {
	DispatchWithOrderedSettlement(
		backend.LeaseStatusEvent,
		func() (accepted bool, err error),
	) (bool, error)
}

type Config struct {
	Coordinator *placement.MaintenanceCoordinator
	Events      OrderedEvents
}

// Service retains only the high-level application capability. It cannot
// acquire a lease claim, choose a backend, invoke a client, or select a
// durable terminal outcome itself.
type Service struct {
	application *placement.MaintenanceApplication
}

func NewService(cfg Config) (*Service, error) {
	if cfg.Coordinator == nil || !cfg.Coordinator.Valid() {
		return nil, errors.New("maintenance service requires a chain/runtime-bound coordinator")
	}
	application, err := cfg.Coordinator.Application(cfg.Events, 0)
	if err != nil {
		return nil, err
	}
	return &Service{application: application}, nil
}

func (service *Service) Execute(ctx context.Context, command Command) Result {
	if service == nil || service.application == nil {
		return Result{outcome: OutcomeInternalFailure, cause: errors.New("maintenance service is unavailable")}
	}
	kind := placement.MaintenanceCommandKind(0)
	switch command.Kind {
	case KindRestart:
		kind = placement.MaintenanceCommandRestart
	case KindUpdate:
		kind = placement.MaintenanceCommandUpdate
	}
	request, err := placement.NewMaintenanceApplicationRequest(
		command.ID, command.LeaseUUID, command.Tenant, kind, command.Payload,
	)
	if err != nil {
		return Result{outcome: OutcomeInternalFailure, cause: err}
	}
	result := service.application.Execute(ctx, request)
	return resultFromApplication(result)
}

func resultFromApplication(result placement.MaintenanceApplicationResult) Result {
	outcome := OutcomeInternalFailure
	switch result.Outcome() {
	case placement.MaintenanceApplicationAccepted:
		outcome = OutcomeAccepted
	case placement.MaintenanceApplicationNotFound:
		outcome = OutcomeNotFound
	case placement.MaintenanceApplicationNoLongerActive:
		outcome = OutcomeNoLongerActive
	case placement.MaintenanceApplicationForbidden:
		outcome = OutcomeForbidden
	case placement.MaintenanceApplicationAlreadyInProgress:
		outcome = OutcomeAlreadyInProgress
	case placement.MaintenanceApplicationCommandConflict:
		outcome = OutcomeCommandConflict
	case placement.MaintenanceApplicationBackendInvalidState:
		outcome = OutcomeBackendInvalidState
	case placement.MaintenanceApplicationBackendValidation:
		outcome = OutcomeBackendValidation
	case placement.MaintenanceApplicationServiceUnavailable:
		outcome = OutcomeServiceUnavailable
	case placement.MaintenanceApplicationInternalFailure,
		placement.MaintenanceApplicationInvalid:
		outcome = OutcomeInternalFailure
	}
	return Result{outcome: outcome, cause: result.Err(), detail: result.Detail()}
}

func (service *Service) RecoverPending(ctx context.Context) error {
	if service == nil || service.application == nil {
		return errors.New("maintenance service is unavailable")
	}
	return service.application.RecoverPending(ctx)
}

func (service *Service) Start(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("maintenance recovery interval must be positive")
	}
	if err := service.RecoverPending(ctx); err != nil {
		slog.Warn("pending maintenance recovery pass incomplete", "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := service.RecoverPending(ctx); err != nil {
				slog.Warn("pending maintenance recovery pass incomplete", "error", err)
			}
		}
	}
}
