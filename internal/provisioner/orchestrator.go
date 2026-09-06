package provisioner

import (
	"context"
	"errors"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// ProvisionOpts contains optional parameters for provisioning.
type ProvisionOpts struct {
	Payload     []byte // Optional deployment payload
	PayloadHash string // Optional hex-encoded SHA-256 hash of payload
}

func capacityVerdictLabel(refusal backend.ProvisionRefusal) string {
	if refusal == backend.ProvisionRefusalCapacity {
		return metrics.CapacityVerdictCodedRefusal
	}
	return metrics.CapacityVerdictAmbiguous
}

// ProvisionOrchestrator is a narrow adapter around the construction-bound
// placement application. Routing, write-ahead admission, backend invocation,
// and settlement remain inside ProvisionCoordinator and cannot be sequenced
// independently by handlers.
type ProvisionOrchestrator struct {
	coordinator *placement.ProvisionCoordinator
	marker      *provisionOrchestratorMarker
}

type provisionOrchestratorMarker struct{ _ byte }

// HandlerEventCoordinator is the single construction-bound capability used by
// lease and payload handlers. It keeps process-local exclusion and the exact
// provision/deprovision implementation inseparable.
type HandlerEventCoordinator struct {
	orchestrator *ProvisionOrchestrator
	issuer       *provisionOrchestratorMarker
}

func (o *ProvisionOrchestrator) HandlerEvents() *HandlerEventCoordinator {
	if o == nil || o.coordinator == nil || !o.coordinator.Valid() || o.marker == nil {
		return nil
	}
	return &HandlerEventCoordinator{orchestrator: o, issuer: o.marker}
}

func (events *HandlerEventCoordinator) Valid() bool {
	return events != nil && events.orchestrator != nil && events.issuer != nil &&
		events.orchestrator.marker == events.issuer &&
		events.orchestrator.coordinator != nil && events.orchestrator.coordinator.Valid()
}

func (events *HandlerEventCoordinator) startFromCurrentLease(
	ctx context.Context,
	request placement.ProvisionEventRequest,
) placement.ProvisionEventResult {
	if !events.Valid() {
		return placement.ProvisionEventResult{}
	}
	return events.orchestrator.coordinator.ExecuteCurrentLease(ctx, request)
}

func (events *HandlerEventCoordinator) rejectProvisionResult(
	ctx context.Context,
	result placement.ProvisionEventResult,
	reason string,
) error {
	if !events.Valid() {
		return errors.New("handler event coordinator is invalid")
	}
	_, _, err := events.orchestrator.coordinator.RejectProvisionResult(ctx, result, reason)
	return err
}

func (events *HandlerEventCoordinator) Deprovision(ctx context.Context, leaseUUID string) error {
	if !events.Valid() {
		return errors.New("handler event coordinator is invalid")
	}
	err := events.orchestrator.coordinator.Deprovision(ctx, leaseUUID)
	if err == nil {
		return nil
	}
	result := errors.Join(ErrDeprovisionFailed, err)
	if errors.Is(err, placement.ErrDeprovisionAuthorityUnresolvable) {
		result = errors.Join(result, ErrPlacementUnresolvable)
	}
	return result
}

// ErrPlacementStoreUnavailable means a placement-dependent write path was
// invoked without durable placement storage. Such paths must fail closed before
// contacting a backend.
var ErrPlacementStoreUnavailable = errors.New("placement store is unavailable")

// NewProvisionOrchestrator creates a capability-safe ProvisionOrchestrator.
// The supplied coordinator already binds durable placement, lifecycle
// exclusion, chain authorization, callback issuance, and one backend runtime.
func NewProvisionOrchestrator(
	coordinator *placement.ProvisionCoordinator,
) (*ProvisionOrchestrator, error) {
	if coordinator == nil || !coordinator.Valid() {
		return nil, errors.New("joined provision dispatch authority is required")
	}
	return &ProvisionOrchestrator{
		coordinator: coordinator,
		marker:      &provisionOrchestratorMarker{},
	}, nil
}

// Deprovision executes the construction-bound teardown transaction. Backend
// candidates, exact claims, retry memory, physical calls, and settlement all
// remain inside placement.ProvisionCoordinator.
func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string) error {
	if o == nil || o.coordinator == nil || !o.coordinator.Valid() {
		return ErrDeprovisionFailed
	}
	err := o.coordinator.Deprovision(ctx, leaseUUID)
	if err == nil {
		return nil
	}
	result := errors.Join(ErrDeprovisionFailed, err)
	if errors.Is(err, placement.ErrDeprovisionAuthorityUnresolvable) {
		result = errors.Join(result, ErrPlacementUnresolvable)
	}
	return result
}
