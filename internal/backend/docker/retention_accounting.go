package docker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"syscall"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// diskOverProvisioned reports whether the configured total disk pool exceeds the
// data filesystem's total capacity (both in MB). Pure, so it is unit-testable;
// the statfs read lives in warnIfOverProvisioned.
func diskOverProvisioned(totalDiskMB, fsTotalMB int64) bool {
	return totalDiskMB > fsTotalMB
}

// retentionCapNeedsTenantLever reports whether a per-provider retained cap is set
// without a per-tenant count cap AND retention is enabled. In that state one
// well-funded tenant can fill the entire retained pool, degrading every other
// tenant's close to refuse-to-retain (an availability DoS on the retention
// feature). Gated on RetainOnClose so it does not fire when nothing can ever be
// retained (the warn would be misleading noise). Pure, so it is unit-testable;
// the WARN is emitted by the caller (New).
func retentionCapNeedsTenantLever(cfg Config) bool {
	return cfg.RetainOnClose && cfg.MaxRetainedDiskMB > 0 && cfg.MaxRetainedLeasesPerTenant == 0
}

// retentionCapSetButDisabled reports whether any retention cap knob
// (max_retained_disk_mb or max_retained_leases_per_tenant) is configured while
// retain_on_close=false. In that state nothing is ever retained, so the cap has
// no effect — the operator likely misconfigured the backend (e.g., copied a
// retain-enabled template but forgot to set retain_on_close=true). Pure, so it
// is unit-testable; the WARN is emitted by the caller (New).
func retentionCapSetButDisabled(cfg Config) bool {
	return !cfg.RetainOnClose && (cfg.MaxRetainedDiskMB > 0 || cfg.MaxRetainedLeasesPerTenant > 0)
}

// warnIfOverProvisioned logs a WARN when total_disk_mb exceeds the data
// filesystem's TOTAL capacity (statfs f_blocks). The hard-quota-sum admission
// model only guarantees no tenant ENOSPC when total_disk_mb <= usable capacity
// (XFS bhard is a ceiling, not a physical reservation). Note f_blocks is the
// post-mkfs total — it still INCLUDES root-reserved blocks and any non-fred
// consumers (Docker image layers, logs), so this is a coarse upper-bound check:
// operators should leave headroom below it (see §2 invariant). WARN-only:
// capacity legitimately fluctuates and a hard refusal would turn a benign
// mis-size into an outage.
func (b *Backend) warnIfOverProvisioned() {
	if b.cfg.VolumeDataPath == "" {
		return // no stateful volumes configured; nothing to check
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(b.cfg.VolumeDataPath, &st); err != nil {
		b.logger.Warn("capacity check: statfs failed", "path", b.cfg.VolumeDataPath, "error", err)
		return
	}
	// Compute in uint64 (st.Blocks is uint64) to avoid int64 overflow on extreme
	// capacities; the MB result is small, so the final conversion is safe.
	fsTotalMB := int64(st.Blocks * uint64(st.Bsize) / bytesPerMiB)
	if diskOverProvisioned(b.cfg.TotalDiskMB, fsTotalMB) {
		b.logger.Warn("total_disk_mb exceeds the data filesystem's total capacity: over-commit risk (retained+live volumes can exhaust physical disk → tenant ENOSPC). Size total_disk_mb at or below usable capacity, leaving headroom for root-reserved blocks and non-fred consumers.",
			"total_disk_mb", b.cfg.TotalDiskMB, "fs_total_mb", fsTotalMB, "path", b.cfg.VolumeDataPath)
	}
}

// leaseDiskMB sums the declared SKU disk reservation (profile.DiskMB * Quantity)
// for a lease's items. Items whose SKU no longer resolves (e.g. an operator
// removed/renamed a profile after the lease was retained) contribute 0 and their
// SKU is returned in `unresolved` so the caller can warn — accounting silently
// undercounting would risk over-admission. A non-positive Quantity (a corrupt
// record or an upstream uint→int overflow) likewise contributes 0 rather than a
// negative term, so invalid data can never push the projection in the undercount
// (over-admit) direction.
func (b *Backend) leaseDiskMB(items []backend.LeaseItem) (mb int64, unresolved []string) {
	for _, item := range items {
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			unresolved = append(unresolved, item.SKU)
			continue
		}
		if item.Quantity > 0 {
			mb += profile.DiskMB * int64(item.Quantity)
		}
	}
	return mb, unresolved
}

// computeRetainedDiskMB derives the retained-disk projection from the retention
// store: the sum of leaseDiskMB over ACTIVE records. Returns both the total MB
// and the active-record count (for the retained_leases gauge). This is the
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
	unknown := make(map[string]struct{})
	for _, e := range entries {
		if e.Status != shared.RetentionStatusActive {
			continue
		}
		count++
		emb, eunres := b.leaseDiskMB(e.Items)
		mb += emb
		for _, s := range eunres {
			unknown[s] = struct{}{}
		}
	}
	if len(unknown) > 0 {
		skus := make([]string, 0, len(unknown))
		for s := range unknown {
			skus = append(skus, s)
		}
		sort.Strings(skus)
		b.logger.Warn("retained record references unknown SKU profile(s); retained-disk accounting UNDERCOUNTS (risk of over-admission/ENOSPC) until the profile is restored or the retained records are reconciled",
			"unknown_skus", skus)
	}
	return mb, count, nil
}

// computeReapingDiskMB derives the reaping (pending-destroy) footprint from the
// retention store: the sum of leaseDiskMB over REAPING records, plus their count.
// These bytes are still physically on disk, so they count toward the admission
// projection (refreshRetentionAccounting adds them to SetRetainedDisk) — but NOT
// toward breachRetentionCap, whose true result DESTROYS data (over-counting a
// destroy gate is the dangerous direction). (ENG-376)
func (b *Backend) computeReapingDiskMB() (mb int64, count int, err error) {
	if b.retentionStore == nil {
		return 0, 0, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return 0, 0, err
	}
	unknown := make(map[string]struct{})
	for _, e := range entries {
		if e.Status != shared.RetentionStatusReaping {
			continue
		}
		count++
		emb, eunres := b.leaseDiskMB(e.Items)
		mb += emb
		for _, s := range eunres {
			unknown[s] = struct{}{}
		}
	}
	if len(unknown) > 0 {
		skus := make([]string, 0, len(unknown))
		for s := range unknown {
			skus = append(skus, s)
		}
		sort.Strings(skus)
		// Same hazard as computeRetainedDiskMB: an unresolved SKU contributes 0, so a
		// reaping record on a removed/renamed profile UNDERCOUNTS the admission pool
		// (reaping is added to SetRetainedDisk) → over-admission/ENOSPC risk. Warn so an
		// operator can restore the profile; the reaping footprint is transient, so this
		// is rarer than the active case but the direction is identically dangerous.
		b.logger.Warn("reaping record references unknown SKU profile(s); admission accounting UNDERCOUNTS the pending-destroy footprint (risk of over-admission/ENOSPC) until the profile is restored",
			"unknown_skus", skus)
	}
	return mb, count, nil
}

// refreshRetentionAccounting recomputes the retained-disk projection and pushes
// it to the admission pool and the gauges. Call at every retention transition
// (close, reap, evict), on recover, and on the periodic sweep tick.
//
// The recompute-from-store and SetRetainedDisk are serialized under
// retentionAccountingMu so concurrent refreshes cannot interleave: without the
// mutex a stale snapshot's Set could land after a fresher one, causing the
// projection to under-count retained bytes → over-admit live allocations
// (ENOSPC risk). On a store error the lock is still held across the return so
// the last valid value stands without a concurrent stale Set overwriting it.
//
// NOTE: breachRetentionCap calls computeRetainedDiskMB directly (a read for an
// admission decision) and must NOT take this mutex — only the writer does.
func (b *Backend) refreshRetentionAccounting() {
	b.retentionAccountingMu.Lock()
	defer b.retentionAccountingMu.Unlock()
	activeMB, activeCount, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute retained disk accounting; keeping last value", "error", err)
		return
	}
	reapingMB, reapingCount, err := b.computeReapingDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute reaping disk accounting; keeping last value", "error", err)
		return
	}
	// Admission pool = active + reaping (reaping bytes are still on disk → counting
	// them prevents over-admit/ENOSPC; this gate only DENIES provisions, so an
	// over-count is safe). breachRetentionCap stays active-only (it DESTROYS).
	b.pool.SetRetainedDisk(activeMB + reapingMB)
	updateRetentionMetrics(activeMB+reapingMB, activeCount, reapingMB, reapingCount)
}

// breachRetentionCap reports whether retaining a lease of the given items would
// push the provider-global retained footprint over max_retained_disk_mb.
// 0 = unlimited (never breaches). It recomputes the retained total from the
// retention store (the source of truth) rather than the cached pool projection,
// because the cache lags store mutations (reap/claim) — a stale-HIGH cache could
// otherwise wrongly refuse-to-retain and DESTROY a closing lease's volumes that
// fit under the true cap. On a store read error it fails OPEN (does not refuse):
// refuse-to-retain destroys data, so uncertainty must never trigger it.
func (b *Backend) breachRetentionCap(items []backend.LeaseItem) bool {
	if b.cfg.MaxRetainedDiskMB <= 0 {
		return false
	}
	incoming, _ := b.leaseDiskMB(items)
	retained, _, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("retention cap check: store read failed; not refusing (data-safe)", "error", err)
		return false
	}
	return retained+incoming > b.cfg.MaxRetainedDiskMB
}

// shouldRefuseRetention decides whether a closing lease must be refused retention
// due to the provider cap. When the cap is unlimited (MaxRetainedDiskMB <= 0) it
// returns false immediately, skipping the per-close retention-store read.
// Otherwise it returns false when the lease ALREADY has any retention record
// (active OR restoring): an active record means its footprint is already counted
// in retainedDisk and the cap was honored on first write (re-deciding would
// double-count on a retry); a restoring record means an in-flight restore owns
// the lease's still-canonical volumes, so destroying them here would race the
// restore and bypass the safe PutActiveMerged ok=false defer. A retention-store
// read error causes an early false return (fail-open): refuse-to-retain DESTROYS
// the closing lease's volumes, so under any uncertainty the data-safe direction
// is to NOT refuse — an unreadable active/restoring record must not be treated
// as "no record" and trigger irreversible destruction.
func (b *Backend) shouldRefuseRetention(leaseUUID string, items []backend.LeaseItem) bool {
	if b.cfg.MaxRetainedDiskMB <= 0 {
		return false // unlimited: never refuse, and skip the per-close retention-store read
	}
	// Refuse-to-retain DESTROYS the closing lease's volumes, so the data-safe
	// direction under any uncertainty is to NOT refuse. A read error means we
	// cannot rule out an existing record (active = cap already honored on a prior
	// attempt; restoring = a restore owns it), so fall through to the normal retain
	// path, which never destroys (PutActiveMerged succeeds, or errors → defers).
	rec, err := b.retentionStore.Get(leaseUUID)
	if err != nil {
		b.logger.Warn("retention store read failed in refuse-to-retain decision; not refusing (data-safe)",
			"lease_uuid", leaseUUID, "error", err)
		return false
	}
	if rec != nil {
		return false
	}
	return b.breachRetentionCap(items)
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
