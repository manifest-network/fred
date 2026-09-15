package operation

import (
	"context"
	"time"
)

// RuntimeController is the narrow process-lifecycle facet retained by Manager.
// It can observe and drain work but cannot claim, initiate, or settle an
// operation. Its zero value is invalid.
type RuntimeController struct {
	registry *Registry
	marker   *settlementAuthorityMarker
}

// RuntimeController derives the observation-and-drain-only facet bound to the
// same Registry. An invalid authority produces an invalid controller.
func (authority SettlementAuthority) RuntimeController() RuntimeController {
	if !authority.valid() {
		return RuntimeController{}
	}
	return RuntimeController(authority)
}

func (runtime RuntimeController) valid() bool {
	return runtime.registry != nil && runtime.marker != nil &&
		runtime.registry.settlementAuthority == runtime.marker
}

// Contains reports whether leaseUUID has a tracked operation.
func (runtime RuntimeController) Contains(leaseUUID string) bool {
	return runtime.valid() && runtime.registry.contains(leaseUUID)
}

// Count returns the number of tracked operations.
func (runtime RuntimeController) Count() int {
	if !runtime.valid() {
		return 0
	}
	return runtime.registry.count()
}

// PendingWorkCount returns the number of operations and exclusive lease
// actions that shutdown must allow to finish.
func (runtime RuntimeController) PendingWorkCount() int {
	if !runtime.valid() {
		return 0
	}
	return runtime.registry.pendingWorkCount()
}

// WaitForDrain waits until all pending work finishes, ctx is canceled, or the
// timeout elapses and returns the exact number of remaining work items.
func (runtime RuntimeController) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if !runtime.valid() {
		return 0
	}
	return runtime.registry.waitForDrain(ctx, timeout)
}

// PendingLeaseUUIDs returns the detached union of operation and lease-action
// claim identities for shutdown diagnostics.
func (runtime RuntimeController) PendingLeaseUUIDs() []string {
	if !runtime.valid() {
		return nil
	}
	return runtime.registry.pendingLeaseUUIDs()
}

// BeginDrain irreversibly closes ordinary operation and lease-action
// admission while allowing existing work and authenticated callback recovery
// to finish.
func (runtime RuntimeController) BeginDrain() {
	if runtime.valid() {
		runtime.registry.beginDrain()
	}
}
