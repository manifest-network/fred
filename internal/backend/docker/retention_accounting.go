package docker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// leaseDiskMB sums the declared SKU disk reservation (profile.DiskMB * Quantity)
// for a lease's items. Unknown SKUs (e.g. an operator removed a profile after
// the lease was retained) contribute 0 and are skipped silently — a rare edge
// that would only undercount a stale record.
func (b *Backend) leaseDiskMB(items []backend.LeaseItem) int64 {
	var mb int64
	for _, item := range items {
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			continue
		}
		mb += profile.DiskMB * int64(item.Quantity)
	}
	return mb
}

// computeRetainedDiskMB derives the retained-disk projection from the retention
// store: the sum of leaseDiskMB over ACTIVE records. Returns both the total MB
// and the active-record count (for the retained_volumes gauge). This is the
// single source of truth (bbolt) — never an independently-mutated counter.
// Restoring records are excluded (their bytes move to the live pool via the
// restored lease's TryAllocate).
func (b *Backend) computeRetainedDiskMB() (mb int64, count int, err error) {
	if b.retentionStore == nil {
		return 0, 0, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		if e.Status != shared.RetentionStatusActive {
			continue
		}
		count++
		mb += b.leaseDiskMB(e.Items)
	}
	return mb, count, nil
}

// refreshRetentionAccounting recomputes the retained-disk projection and pushes
// it to the admission pool and the gauges. Call at every retention transition
// (close, reap, evict), on recover, and on the periodic sweep tick. On a store
// error it logs and returns WITHOUT mutating the projection — keeping the last
// good value, since an under-count would over-admit (the dangerous direction).
func (b *Backend) refreshRetentionAccounting() {
	mb, count, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute retained disk accounting; keeping last value", "error", err)
		return
	}
	b.pool.SetRetainedDisk(mb)
	updateRetentionMetrics(mb, count)
}

// breachRetentionCap reports whether retaining a lease of the given items would
// push the provider-global retained footprint over max_retained_disk_mb.
// 0 = unlimited (never breaches). Reads the CACHED pool.Stats().RetainedDiskMB
// (not a fresh recompute), so under concurrent multi-lease closes the cap may be
// transiently overshot by one lease's worth, self-healing at the next
// recompute (sweep / reconcile loop) — consistent with the level-triggered
// drift-to-one-tick model.
func (b *Backend) breachRetentionCap(items []backend.LeaseItem) bool {
	if b.cfg.MaxRetainedDiskMB <= 0 {
		return false
	}
	return b.pool.Stats().RetainedDiskMB+b.leaseDiskMB(items) > b.cfg.MaxRetainedDiskMB
}

// destroyOnRefuseToRetain destroys a closing lease's still-canonical volumes
// when the retained cap is breached (refuse-to-retain). Logs + increments the
// refusal counter; returns any destroy errors to merge into the caller's
// volumeErrs. Only the closing lease's own volumes are touched — no other
// tenant's in-grace data is ever evicted.
func (b *Backend) destroyOnRefuseToRetain(ctx context.Context, canonical []string, leaseUUID, tenant string, logger *slog.Logger) []error {
	logger.Warn("retention refused: provider retained-capacity cap reached; destroying volumes instead of retaining",
		"lease_uuid", leaseUUID, "tenant", tenant, "cap_mb", b.cfg.MaxRetainedDiskMB)
	retentionRefusedTotal.Inc()
	var errs []error
	for _, c := range canonical {
		if derr := b.volumes.Destroy(ctx, c); derr != nil {
			logger.Error("retention-refused destroy failed", "volume", c, "error", derr)
			errs = append(errs, fmt.Errorf("retention-refused destroy %s: %w", c, derr))
		}
	}
	return errs
}
