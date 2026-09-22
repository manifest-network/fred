package placement

import "context"

// DeprovisionEventDisposition distinguishes completed close dispatch from a
// constructor-observed wait and an ordinary failure. Deferred is never success.
type DeprovisionEventDisposition uint8

const (
	DeprovisionEventInvalid DeprovisionEventDisposition = iota
	DeprovisionEventCompleted
	DeprovisionEventDeferred
	DeprovisionEventFailed
)

// DeprovisionDeferralReason identifies the closed set of retryable boundaries.
type DeprovisionDeferralReason string

const (
	DeprovisionDeferredInventory          DeprovisionDeferralReason = "inventory_pending"
	DeprovisionDeferredLifecycle          DeprovisionDeferralReason = "lifecycle_busy"
	DeprovisionDeferredBackendUnavailable DeprovisionDeferralReason = "backend_unavailable"
)

// DeferredDeprovision grants only another attempt at the same lifecycle subject
// through the same coordinator. It carries no backend, placement, lease claim,
// or settlement authority; Retry must reacquire every current fence.
type DeferredDeprovision struct {
	coordinator *deprovisionCoordinator
	leaseUUID   string
	reason      DeprovisionDeferralReason
}

func (deferred DeferredDeprovision) Valid() bool {
	if deferred.coordinator == nil || deferred.coordinator.execution == nil ||
		!deferred.coordinator.execution.Valid() || deferred.leaseUUID == "" {
		return false
	}
	switch deferred.reason {
	case DeprovisionDeferredInventory, DeprovisionDeferredLifecycle, DeprovisionDeferredBackendUnavailable:
		return true
	default:
		return false
	}
}

func (deferred DeferredDeprovision) LeaseUUID() string {
	if !deferred.Valid() {
		return ""
	}
	return deferred.leaseUUID
}

func (deferred DeferredDeprovision) Reason() DeprovisionDeferralReason {
	if !deferred.Valid() {
		return ""
	}
	return deferred.reason
}

func (deferred DeferredDeprovision) Retry(ctx context.Context) DeprovisionEventResult {
	if !deferred.Valid() {
		return deprovisionEventFromError(ErrDeprovisionExecution)
	}
	return deferred.coordinator.executeEvent(ctx, deferred.leaseUUID)
}

// DeprovisionEventResult is issued only by construction-bound close execution.
// A caller cannot turn an arbitrary backend error into a scheduled deferral.
type DeprovisionEventResult struct {
	disposition DeprovisionEventDisposition
	deferred    DeferredDeprovision
	err         error
}

func (result DeprovisionEventResult) Disposition() DeprovisionEventDisposition {
	return result.disposition
}

func (result DeprovisionEventResult) Err() error { return result.err }

func (result DeprovisionEventResult) Deferred() DeferredDeprovision {
	if result.disposition != DeprovisionEventDeferred || !result.deferred.Valid() {
		return DeferredDeprovision{}
	}
	return result.deferred
}

func deprovisionEventFromError(err error) DeprovisionEventResult {
	if err == nil {
		return DeprovisionEventResult{disposition: DeprovisionEventCompleted}
	}
	return DeprovisionEventResult{disposition: DeprovisionEventFailed, err: err}
}

func (coordinator *deprovisionCoordinator) deferredEvent(
	leaseUUID string,
	reason DeprovisionDeferralReason,
	err error,
) DeprovisionEventResult {
	return DeprovisionEventResult{
		disposition: DeprovisionEventDeferred,
		deferred: DeferredDeprovision{
			coordinator: coordinator, leaseUUID: leaseUUID, reason: reason,
		},
		err: err,
	}
}
