package docker

import (
	"context"
	"errors"
	"time"
)

// backgroundMaintenanceCoordinator is the only background substrate service
// retained by Backend. Its surface is deliberately made entirely of complete,
// target-free workflows: callers may request a convergence pass, but cannot
// choose a lease, container, volume, project, or tenant for the underlying raw
// writer.
//
// The function closures are assembled once, alongside the raw mutation sinks,
// in storage_mutation_guard.go. The raw sinks and the per-target helpers they
// close over are not fields of Backend and cannot be recovered from this value.
type backgroundMaintenanceCoordinator struct {
	recoverInterruptedVolumesFn func(context.Context) error
	recoverClosedLeasesFn       func(context.Context) (map[string]struct{}, error)
	reconcileRetentionsFn       func(context.Context) error
	reconcileRestoringRecordsFn func(context.Context) error
	reconcileVolumeQuotasFn     func(context.Context) error
	cleanupOrphanedNetworksFn   func(context.Context)
	reapExpiredRetentionsFn     func(context.Context) (int, error)
	runRetentionSweepFn         func(context.Context) error
	enableInlineVolumeDeletesFn func(context.Context)
	retryHeldVolumeDeletesFn    func(context.Context) volumeDeleteHoldPassReport
}

// retryHeldVolumeDeletes runs one hold-executor pass over the manager's due
// held deletions. The targets come from the manager's own hold table, never
// from the caller.
func (c *backgroundMaintenanceCoordinator) retryHeldVolumeDeletes(ctx context.Context) volumeDeleteHoldPassReport {
	if c == nil || c.retryHeldVolumeDeletesFn == nil {
		return volumeDeleteHoldPassReport{}
	}
	return c.retryHeldVolumeDeletesFn(ctx)
}

var errBackgroundMaintenanceUnavailable = errors.New("background maintenance coordinator is unavailable")

// enableInlineVolumeDeletes switches the volume manager out of Start's
// deferral: a first-time deletion may then run inline under its budget.
func (c *backgroundMaintenanceCoordinator) enableInlineVolumeDeletes(ctx context.Context) {
	if c == nil || c.enableInlineVolumeDeletesFn == nil {
		return
	}
	c.enableInlineVolumeDeletesFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) recoverInterruptedVolumes(ctx context.Context) error {
	if c == nil || c.recoverInterruptedVolumesFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.recoverInterruptedVolumesFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) recoverClosedLeases(
	ctx context.Context,
) (map[string]struct{}, error) {
	if c == nil || c.recoverClosedLeasesFn == nil {
		return nil, errBackgroundMaintenanceUnavailable
	}
	return c.recoverClosedLeasesFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) reconcileRetentions(ctx context.Context) error {
	if c == nil || c.reconcileRetentionsFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.reconcileRetentionsFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) reconcileRestoringRecords(ctx context.Context) error {
	if c == nil || c.reconcileRestoringRecordsFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.reconcileRestoringRecordsFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) reconcileVolumeQuotas(ctx context.Context) error {
	if c == nil || c.reconcileVolumeQuotasFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.reconcileVolumeQuotasFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) cleanupOrphanedNetworks(ctx context.Context) {
	if c == nil || c.cleanupOrphanedNetworksFn == nil {
		return
	}
	c.cleanupOrphanedNetworksFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) reapExpiredRetentions(
	ctx context.Context,
) (int, error) {
	if c == nil || c.reapExpiredRetentionsFn == nil {
		return 0, errBackgroundMaintenanceUnavailable
	}
	return c.reapExpiredRetentionsFn(ctx)
}

func (c *backgroundMaintenanceCoordinator) runRetentionSweep(ctx context.Context) error {
	if c == nil || c.runRetentionSweepFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.runRetentionSweepFn(ctx)
}

// The following named function types are passed only down the lexical call
// tree of one fixed workflow. Unlike the removed residual facade, no value of
// these types is retained by Backend or returned to a caller.
type backgroundVolumeRename func(context.Context, string, string) error
type backgroundVolumeQuota func(context.Context, string, int64) (volumeQuotaOutcome, error)
type backgroundContainerRemove func(context.Context, string) error
type backgroundTenantNetworkRemove func(context.Context, string) (tenantNetworkRemoval, error)

// backgroundHeldVolumeDeleteRetry retries one held deletion through the
// storage-mutation bracket. Only the hold executor's pass receives one.
type backgroundHeldVolumeDeleteRetry func(context.Context, string) error

// backgroundTeardownCapability is captured privately by the retention
// reconciliation closure. It satisfies teardownMutationCapability without
// publishing a caller-addressable teardown service on Backend.
type backgroundTeardownCapability struct {
	downFn   func(context.Context, string, time.Duration) error
	removeFn backgroundContainerRemove
}

func (c backgroundTeardownCapability) composeDown(
	ctx context.Context,
	leaseUUID string,
	timeout time.Duration,
) error {
	if c.downFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.downFn(ctx, leaseUUID, timeout)
}

func (c backgroundTeardownCapability) removeContainer(ctx context.Context, id string) error {
	if c.removeFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.removeFn(ctx, id)
}

// backgroundVolumeDestroyCapability is likewise private to exact retention
// workflows. volumeOp remains the single ownership/recheck choke point in
// front of the irreversible writer.
type backgroundVolumeDestroyCapability struct {
	destroyFn func(context.Context, string) error
}

func (c backgroundVolumeDestroyCapability) destroyVolume(ctx context.Context, id string) error {
	if c.destroyFn == nil {
		return errBackgroundMaintenanceUnavailable
	}
	return c.destroyFn(ctx, id)
}

func (c backgroundVolumeDestroyCapability) canDestroyVolumes() bool {
	return c.destroyFn != nil
}

// Fixed Backend entry points preserve the existing workflow names while
// making their construction-bound coordinator the only route to mutation.
func (b *Backend) recoverInterruptedVolumeMutations(ctx context.Context) error {
	return b.backgroundMaintenance.recoverInterruptedVolumes(ctx)
}

func (b *Backend) recoverClosedLeaseSubstrate(
	ctx context.Context,
) (map[string]struct{}, error) {
	return b.backgroundMaintenance.recoverClosedLeases(ctx)
}

func (b *Backend) reconcileRetentions(ctx context.Context) error {
	return b.backgroundMaintenance.reconcileRetentions(ctx)
}

func (b *Backend) reconcileVolumeQuotas(ctx context.Context) error {
	return b.backgroundMaintenance.reconcileVolumeQuotas(ctx)
}

func (b *Backend) cleanupOrphanedNetworks(ctx context.Context) {
	b.backgroundMaintenance.cleanupOrphanedNetworks(ctx)
}

func (b *Backend) reapExpiredRetentions(ctx context.Context) (int, error) {
	return b.backgroundMaintenance.reapExpiredRetentions(ctx)
}

func (b *Backend) runRetentionSweep(ctx context.Context) error {
	return b.backgroundMaintenance.runRetentionSweep(ctx)
}
