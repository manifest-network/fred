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
	return cfg.RetainOnClose && cfg.MaxRetainedDiskMB > 0 &&
		cfg.MaxRetainedLeasesPerTenant == 0 && cfg.MaxRetainedDiskMBPerTenant == 0
}

// retentionCapSetButDisabled reports whether any retention cap knob
// (max_retained_disk_mb or max_retained_leases_per_tenant) is configured while
// retain_on_close=false. In that state nothing is ever retained, so the cap has
// no effect — the operator likely misconfigured the backend (e.g., copied a
// retain-enabled template but forgot to set retain_on_close=true). Pure, so it
// is unit-testable; the WARN is emitted by the caller (New).
func retentionCapSetButDisabled(cfg Config) bool {
	return !cfg.RetainOnClose && (cfg.MaxRetainedDiskMB > 0 || cfg.MaxRetainedLeasesPerTenant > 0 ||
		cfg.MaxRetainedDiskMBPerTenant > 0 || len(cfg.RetentionTenantBudgets) > 0 ||
		cfg.RetentionPartitionSource != "")
}

// retentionPartitionSourceWithoutBudgets reports whether a partition source is
// configured with retention enabled but no aggregator allowlist — every
// extraction is skipped budget-first, so partitioning is inert. Probably a staged
// rollout (source landed before the budgets); WARN, not error. Pure, so it is
// unit-testable; the WARN is emitted by the caller (New).
func retentionPartitionSourceWithoutBudgets(cfg Config) bool {
	return cfg.RetainOnClose && cfg.RetentionPartitionSource != "" && len(cfg.RetentionTenantBudgets) == 0
}

// retentionPartitionBudgetWithoutSource reports whether some budget enables
// partitions (max_partitions > 0) while no source is configured — the sub-caps
// are dead config. Elevation-only budgets (max_partitions == 0) are fine without
// a source and do NOT fire this. Pure, so it is unit-testable; the WARN is
// emitted by the caller (New).
func retentionPartitionBudgetWithoutSource(cfg Config) bool {
	if !cfg.RetainOnClose || cfg.RetentionPartitionSource != "" {
		return false
	}
	for _, b := range cfg.RetentionTenantBudgets {
		if b.MaxPartitions > 0 {
			return true
		}
	}
	return false
}

// retentionPartitionWindowCannotRoll reports whether some budget's disk sub-cap
// binds before its count window can roll — per_partition_max_disk_mb <
// (per_partition_max_leases + 1) × largestSKUDiskMB() — so at worst-case lease
// sizes the partition fills on disk and refuses (destroys) incoming closes while
// old records rot. WARN, not error: the largest SKU may be unrepresentative of
// the aggregator's actual mix. Gated on RetainOnClose (mirroring the other two
// partition predicates) so it stays silent when nothing is ever retained — the
// retain-off case is already covered by retentionCapSetButDisabled. Pure, so it
// is unit-testable; the WARN is emitted by the caller (New).
func retentionPartitionWindowCannotRoll(cfg Config) bool {
	if !cfg.RetainOnClose {
		return false
	}
	largest := cfg.largestSKUDiskMB()
	if largest <= 0 {
		return false
	}
	for _, b := range cfg.RetentionTenantBudgets {
		if b.PerPartitionMaxDiskMB > 0 && b.PerPartitionMaxLeases > 0 &&
			b.PerPartitionMaxDiskMB < int64(b.PerPartitionMaxLeases+1)*largest {
			return true
		}
	}
	return false
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

// warnUnknownSKUs logs (once, sorted) the SKU names that failed to resolve while
// summing a retention projection. An unresolved SKU contributes 0 to the sum, so
// the projection UNDERCOUNTS — the over-admission/ENOSPC direction — until the
// profile is restored or the affected records are reconciled. context names the
// projection for the operator (e.g. "retained", "reaping"). No-op on an empty set.
func warnUnknownSKUs(logger *slog.Logger, unknown map[string]struct{}, context string) {
	if len(unknown) == 0 {
		return
	}
	skus := make([]string, 0, len(unknown))
	for s := range unknown {
		skus = append(skus, s)
	}
	sort.Strings(skus)
	logger.Warn("retention record references unknown SKU profile(s); disk accounting UNDERCOUNTS (risk of over-admission/ENOSPC) until the profile is restored or the records are reconciled",
		"context", context, "unknown_skus", skus)
}

// computeRetainedDiskMB derives the retained-disk projection from the retention
// store: the sum of leaseDiskMB over ACTIVE records. Returns the total MB, the
// active-record count (for the retained_leases gauge), and the number of distinct
// non-empty (tenant, partition) buckets across ACTIVE+RESTORING records (for the
// retention_partitions gauge — same scope as the enforced write-time bound). This
// is the single source of truth (bbolt) — never an independently-mutated counter.
// Restoring records are excluded from the mb/count sums (their bytes move to the
// live pool via the restored lease's TryAllocate) but included in the partition
// count so the gauge tracks buckets that still hold reserved space.
func (b *Backend) computeRetainedDiskMB() (mb int64, count int, partitions int, err error) {
	if b.retentionStore == nil {
		return 0, 0, 0, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return 0, 0, 0, err
	}
	unknown := make(map[string]struct{})
	distinct := make(map[string]struct{})
	for _, e := range entries {
		if e.Partition != "" && (e.Status == shared.RetentionStatusActive || e.Status == shared.RetentionStatusRestoring) {
			distinct[e.Tenant+"\x00"+e.Partition] = struct{}{}
		}
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
	warnUnknownSKUs(b.logger, unknown, "retained")
	return mb, count, len(distinct), nil
}

// computeReapingDiskMB derives the reaping (pending-destroy) footprint from the
// retention store: the sum of leaseDiskMB over REAPING records, plus their count.
// These bytes are still physically on disk, so they count toward the admission
// projection (refreshRetentionAccounting adds them to SetRetainedDisk) — but NOT
// toward breachRetentionCaps, whose true result DESTROYS data (over-counting a
// destroy gate is the dangerous direction). (ENG-376)
func (b *Backend) computeReapingDiskMB() (mb int64, count int, err error) {
	if b.retentionStore == nil {
		return 0, 0, nil
	}
	entries, err := b.retentionStore.ListReaping()
	if err != nil {
		return 0, 0, err
	}
	unknown := make(map[string]struct{})
	for _, e := range entries {
		count++
		emb, eunres := b.leaseDiskMB(e.Items)
		mb += emb
		for _, s := range eunres {
			unknown[s] = struct{}{}
		}
	}
	// Same hazard as computeRetainedDiskMB: an unresolved SKU contributes 0, so a
	// reaping record on a removed/renamed profile UNDERCOUNTS the admission pool
	// (reaping is added to SetRetainedDisk) → over-admission/ENOSPC risk. The
	// reaping footprint is transient, so this is rarer than the active case but the
	// direction is identically dangerous.
	warnUnknownSKUs(b.logger, unknown, "reaping")
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
// NOTE: breachRetentionCaps reads the store directly (a read for an admission
// decision) and must NOT take this mutex — only the writer does.
func (b *Backend) refreshRetentionAccounting() {
	b.retentionAccountingMu.Lock()
	defer b.retentionAccountingMu.Unlock()
	activeMB, activeCount, partitionCount, err := b.computeRetainedDiskMB()
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
	// over-count is safe). breachRetentionCaps stays active-only (it DESTROYS).
	b.pool.SetRetainedDisk(activeMB + reapingMB)
	updateRetentionMetrics(activeMB+reapingMB, activeCount, reapingMB, reapingCount, partitionCount)
}

// logRetentionBudgetSanity reports, per configured budget, the tenant's current
// ACTIVE retained holdings vs the budget. One indexed ListByTenant per budget
// entry — budgeted tenants are few (the aggregator allowlist), so this is a
// negligible boot cost. This is the mechanism that catches budget typos,
// offboarding edits, and undersized onboarding BEFORE the first destructive
// close (evictions are additionally bounded by the per-close batch rail). The
// disk term refuses-to-retain rather than evicting, so an over-holdings disk
// budget is the operator's cue to raise the budget or expect closes destroyed.
func (b *Backend) logRetentionBudgetSanity() {
	if b.retentionStore == nil {
		return
	}
	for tenant, bud := range b.cfg.RetentionTenantBudgets {
		mine, err := b.retentionStore.ListByTenant(tenant)
		if err != nil {
			b.logger.Warn("retention budget sanity: list failed", "tenant", tenant, "error", err)
			continue
		}
		var count int
		var mb int64
		unresolved := make(map[string]struct{})
		for _, e := range mine {
			if e.Status != shared.RetentionStatusActive {
				continue
			}
			count++
			emb, eunres := b.leaseDiskMB(e.Items)
			mb += emb
			for _, s := range eunres {
				unresolved[s] = struct{}{}
			}
		}
		overCount := count > bud.MaxRetainedLeases
		overDisk := mb > bud.MaxRetainedDiskMB
		// An unresolved SKU contributes 0 to active_mb, so the disk comparison is an
		// UNDERCOUNT: name the offending SKU(s) on the line itself so a "within
		// budget" reading is not silently wrong. (The store-wide UNDERCOUNT warning
		// already fired in refreshRetentionAccounting one call earlier at boot, so
		// this is per-tenant attribution, not a duplicate global alarm.)
		line := []any{
			"tenant", tenant, "active_count", count, "budget_count", bud.MaxRetainedLeases,
			"active_mb", mb, "budget_mb", bud.MaxRetainedDiskMB,
		}
		if len(unresolved) > 0 {
			skus := make([]string, 0, len(unresolved))
			for s := range unresolved {
				skus = append(skus, s)
			}
			sort.Strings(skus)
			line = append(line, "unresolved_skus", skus)
		}
		if overCount || overDisk {
			// Name the breached dimension(s): a count breach evicts (batch-railed),
			// a disk breach refuses-to-retain — different operator responses.
			b.logger.Warn("retention budget below tenant's current holdings: next closes will evict (count, batch-railed) or be refused-to-retain (disk)",
				append(line, "over_count", overCount, "over_disk", overDisk)...)
			continue
		}
		b.logger.Info("retention budget sanity", line...)
	}
}

// retentionBudget is the EFFECTIVE cap set for one tenant. Everything is
// provider-set; the tenant supplies only the partition key.
type retentionBudget struct {
	CountCap      int   // L1 aggregate count; 0 = unlimited (defaults only — a budget can't express it)
	DiskCapMB     int64 // L1 aggregate disk;  0 = unlimited (defaults only)
	MaxPartitions int   // 0 = partition labels collapse to "" (non-aggregator or elevation-only)
	PerPartCount  int   // 0 = no L2 count sub-cap
	PerPartDiskMB int64 // 0 = no L2 disk sub-cap
}

// resolveTenantRetentionBudget returns the effective retention budget for a
// tenant: a listed aggregator budget when present (I2 opt-in elevation — and it
// may also LOWER a tenant below defaults), otherwise the L1 defaults. Pure O(1);
// resolved once per close and threaded to every consumer.
func resolveTenantRetentionBudget(cfg Config, tenant string) retentionBudget {
	if b, ok := cfg.RetentionTenantBudgets[tenant]; ok {
		return retentionBudget{
			CountCap: b.MaxRetainedLeases, DiskCapMB: b.MaxRetainedDiskMB,
			MaxPartitions: b.MaxPartitions, PerPartCount: b.PerPartitionMaxLeases,
			PerPartDiskMB: b.PerPartitionMaxDiskMB,
		}
	}
	return retentionBudget{CountCap: cfg.MaxRetainedLeasesPerTenant, DiskCapMB: cfg.MaxRetainedDiskMBPerTenant}
}

// boundPartition applies the write-time distinct-partition bound for an
// allowlisted (aggregator) tenant. It is COLLAPSE-ONLY and can never fail the
// close: every uncertainty degrades to the "" default whole-tenant bucket. The
// snapshot is the tenant's ListByTenant output (snapErr its read error), shared
// with the eviction passes so no extra store read is taken.
//
// Distinct non-"" partitions are counted over ACTIVE + RESTORING records; reaping
// is excluded so a stuck destroy-retry loop cannot starve legitimate new labels.
// A consequence: the total STORED distinct labels may transiently exceed the
// bound while tombstones drain — harmless, since no index or metric carries
// partition values.
func (b *Backend) boundPartition(tenant, partition string, budget retentionBudget, snapshot []shared.RetentionEntry, snapErr error, logger *slog.Logger) string {
	if partition == "" || budget.MaxPartitions <= 0 {
		return "" // nothing to bound / non-aggregator (budget-first extraction already skipped)
	}
	if snapErr != nil {
		retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonStoreError).Inc()
		retentionCapCheckFailedTotal.WithLabelValues(capCheckBound).Inc()
		logger.Warn("partition bound: tenant snapshot unavailable; collapsing to default bucket (close proceeds)",
			"tenant", tenant, "error", snapErr)
		return ""
	}
	distinct := make(map[string]struct{})
	for _, e := range snapshot {
		if e.Partition == "" || e.Status == shared.RetentionStatusReaping {
			continue
		}
		distinct[e.Partition] = struct{}{}
	}
	if _, exists := distinct[partition]; exists {
		return partition // existing bucket: admit, no new cardinality
	}
	if len(distinct) >= budget.MaxPartitions {
		retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonOverLimit).Inc()
		logger.Warn("partition bound: distinct-partition ceiling reached; collapsing to default bucket",
			"tenant", tenant, "partition", shared.TruncatePartitionRaw(partition), "max_partitions", budget.MaxPartitions)
		return ""
	}
	return partition
}

// breachRetentionCaps reports whether retaining a lease of the given items would
// push the retained footprint over any of the three disk caps: L0 global
// (max_retained_disk_mb), L1 per-tenant aggregate (budget.DiskCapMB), or L2
// per-partition (budget.PerPartDiskMB). It returns the scope of the FIRST cap
// tripped (checked global→tenant→partition) or ("", false). The L2 term applies
// only to a non-empty partition (I6: the "" default bucket is never L2-capped).
// Each cap of 0 = unlimited; when all three are unlimited it does zero store I/O.
//
// It recomputes ACTIVE-only sums from the retention store (the source of truth)
// rather than the cached pool projection, because the cache lags store mutations
// (reap/claim/evict) — a stale-HIGH cache could otherwise wrongly refuse-to-retain
// and DESTROY a closing lease's volumes that fit under the true cap. Reaping (and
// restoring) records are excluded: this gate DESTROYS, and over-counting a destroy
// gate is the dangerous direction. On a store read error it fails OPEN (does not
// refuse) and counts the fail-open: refuse-to-retain destroys data, so uncertainty
// must never trigger it.
func (b *Backend) breachRetentionCaps(tenant, partition string, items []backend.LeaseItem, budget retentionBudget) (scope string, breached bool) {
	l0, l1, l2 := b.cfg.MaxRetainedDiskMB, budget.DiskCapMB, budget.PerPartDiskMB
	if partition == "" {
		l2 = 0 // I6: the default bucket is governed solely by L0/L1
	}
	if l0 <= 0 && l1 <= 0 && l2 <= 0 {
		return "", false // no disk cap at any scope: zero store I/O (legacy fast path)
	}
	incoming, _ := b.leaseDiskMB(items)
	// globalMB (the only cross-tenant sum) is consumed solely under l0 > 0; the L1
	// and L2 sums are both accumulated inside `e.Tenant == tenant`. So when the
	// global cap is off, only the closing tenant's records matter and the indexed
	// per-tenant read suffices — avoiding an O(all retained records) full-store
	// scan on this per-close path. This gate DESTROYS on breach, so the safe
	// direction is under-counting; the index read (skips deleted, re-validates the
	// tenant) can only ever under-count vs a racing writer, never over-count.
	var entries []shared.RetentionEntry
	var err error
	if l0 > 0 {
		entries, err = b.retentionStore.List() // L0 needs the store-wide global sum
	} else {
		entries, err = b.retentionStore.ListByTenant(tenant) // only L1/L2 (tenant-scoped) are live
	}
	if err != nil {
		b.logger.Warn("retention cap check: store read failed; not refusing (data-safe)", "error", err)
		retentionCapCheckFailedTotal.WithLabelValues(capCheckBreach).Inc()
		return "", false
	}
	var globalMB, tenantMB, partMB int64
	unknown := make(map[string]struct{})
	for _, e := range entries {
		if e.Status != shared.RetentionStatusActive {
			continue
		}
		emb, eunres := b.leaseDiskMB(e.Items)
		for _, s := range eunres {
			unknown[s] = struct{}{}
		}
		globalMB += emb
		if e.Tenant == tenant {
			tenantMB += emb
			if l2 > 0 && e.Partition == partition {
				partMB += emb
			}
		}
	}
	warnUnknownSKUs(b.logger, unknown, "retained")
	switch {
	case l0 > 0 && globalMB+incoming > l0:
		return refuseScopeGlobal, true
	case l1 > 0 && tenantMB+incoming > l1:
		return refuseScopeTenant, true
	case l2 > 0 && partMB+incoming > l2:
		return refuseScopePartition, true
	default:
		return "", false
	}
}

// shouldRefuseRetention decides whether a closing lease must be refused retention
// due to a disk cap at any scope, returning the tripped cap's scope. When no disk
// cap applies at any scope (all three ≤ 0) it returns ("", false) immediately,
// skipping the per-close retention-store read — this reduces exactly to today's
// unlimited fast path for legacy configs. Otherwise it returns ("", false) when
// the lease ALREADY has any retention record (active OR restoring): an active
// record means its footprint is already counted in retainedDisk and the caps were
// honored on first write (re-deciding would double-count on a retry); a restoring
// record means an in-flight restore owns the lease's still-canonical volumes, so
// destroying them here would race the restore and bypass the safe PutActiveMerged
// ok=false defer. A retention-store read error causes an early ("", false) return
// (fail-open): refuse-to-retain DESTROYS the closing lease's volumes, so under any
// uncertainty the data-safe direction is to NOT refuse — an unreadable
// active/restoring record must not be treated as "no record" and trigger
// irreversible destruction.
func (b *Backend) shouldRefuseRetention(leaseUUID, tenant, partition string, items []backend.LeaseItem, budget retentionBudget) (scope string, refuse bool) {
	l2 := budget.PerPartDiskMB
	if partition == "" {
		l2 = 0 // I6
	}
	if b.cfg.MaxRetainedDiskMB <= 0 && budget.DiskCapMB <= 0 && l2 <= 0 {
		return "", false // no disk cap at any scope: never refuse, skip the per-close store read
	}
	// Refuse-to-retain DESTROYS the closing lease's volumes, so the data-safe
	// direction under any uncertainty is to NOT refuse. A read error means we
	// cannot rule out an existing record (active = caps already honored on a prior
	// attempt; restoring = a restore owns it), so fall through to the normal retain
	// path, which never destroys (PutActiveMerged succeeds, or errors → defers).
	rec, err := b.retentionStore.Get(leaseUUID)
	if err != nil {
		b.logger.Warn("retention store read failed in refuse-to-retain decision; not refusing (data-safe)",
			"lease_uuid", leaseUUID, "error", err)
		retentionCapCheckFailedTotal.WithLabelValues(capCheckRefuseGet).Inc()
		return "", false
	}
	if rec != nil {
		return "", false
	}
	return b.breachRetentionCaps(tenant, partition, items, budget)
}

// destroyOnRefuseToRetain destroys a closing lease's still-canonical volumes when
// a disk cap is breached (refuse-to-retain), at the scope reported by
// shouldRefuseRetention. Logs (with scope + truncated partition + the tripped cap
// value) and increments the scoped refusal counter; the bare retentionRefusedTotal
// is bumped ONLY for the L0-global scope so its deployed meaning (and the alert
// keyed on it) is preserved. Returns any destroy errors to merge into the caller's
// volumeErrs. Only the closing lease's own volumes are touched — disk caps at any
// level never evict another tenant's (or partition's) in-grace data.
func (b *Backend) destroyOnRefuseToRetain(ctx context.Context, canonical []string, leaseUUID, tenant, partition, scope string, logger *slog.Logger) []error {
	var capMB int64
	switch scope {
	case refuseScopeGlobal:
		capMB = b.cfg.MaxRetainedDiskMB
	case refuseScopeTenant:
		capMB = resolveTenantRetentionBudget(b.cfg, tenant).DiskCapMB
	case refuseScopePartition:
		capMB = resolveTenantRetentionBudget(b.cfg, tenant).PerPartDiskMB
	}
	logger.Warn("retention refused: retained-capacity cap reached; destroying volumes instead of retaining",
		"lease_uuid", leaseUUID, "tenant", tenant, "partition", shared.TruncatePartitionRaw(partition),
		"scope", scope, "cap_mb", capMB)
	if scope == refuseScopeGlobal {
		retentionRefusedTotal.Inc() // deployed L0-global-only meaning preserved
	}
	retentionRefusedByScopeTotal.WithLabelValues(scope).Inc()
	var errs []error
	for _, c := range canonical {
		if derr := b.volumes.Destroy(ctx, c); derr != nil {
			logger.Error("retention-refused destroy failed", "volume", c, "error", derr)
			errs = append(errs, fmt.Errorf("retention-refused destroy %s: %w", c, derr))
		}
	}
	return errs
}
