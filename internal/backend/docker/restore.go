package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/util"
)

// retainedVolumePrefix is the namespace soft-deleted volumes are renamed into.
// It keeps the leading "fred-" so listVolumeIDs still enumerates the dir, but the
// distinct "retained" token makes cleanupOrphanedVolumes' expected-set match miss it.
const retainedVolumePrefix = "fred-retained-"

// canonicalVolumeName is the live volume name a provision/restore mounts.
// MUST match setupVolBinds / deprovision / cleanupOrphanedVolumes exactly.
func canonicalVolumeName(leaseUUID, serviceName string, idx int) string {
	return fmt.Sprintf("fred-%s-%s-%d", leaseUUID, serviceName, idx)
}

// retainedName maps a canonical volume name to its retained-namespace name.
func retainedName(canonical string) string {
	return "fred-retained-" + strings.TrimPrefix(canonical, "fred-")
}

// canonicalFromRetained is the inverse of retainedName.
func canonicalFromRetained(retained string) string {
	return "fred-" + strings.TrimPrefix(retained, "fred-retained-")
}

// retainedToNewCanonical maps a retained volume name fred-retained-{orig}-{svc}-{idx}
// to the new lease's canonical name fred-{newLease}-{svc}-{idx} during restore adopt.
func retainedToNewCanonical(retained, originalLease, newLease string) string {
	suffix := strings.TrimPrefix(retained, "fred-retained-"+originalLease+"-") // {svc}-{idx}
	return "fred-" + newLease + "-" + suffix
}

// isRetainedVolume reports whether a volume id is a soft-delete tombstone.
func isRetainedVolume(id string) bool {
	return strings.HasPrefix(id, retainedVolumePrefix)
}

// leaseVolumePrefix is the on-disk name prefix of all of a lease's canonical
// volumes (used to enumerate a closing lease's actual volumes). It cannot match
// "fred-retained-..." or another lease's volumes.
func leaseVolumePrefix(leaseUUID string) string {
	return "fred-" + leaseUUID + "-"
}

// renameIfPresent is a best-effort reconcile rename. Per the RenameVolume
// contract (volume.go), RenameVolume errors for the conflict case (BOTH names
// present) and the missing case (NEITHER present), as well as for a real
// Docker-daemon failure; the only-new-exists case is a benign no-op that
// returns nil (idempotent success). It logs and RETURNS the error so callers
// can decide whether the failure is fatal to their step (e.g. the restoring-arm
// rollback must NOT advance the record if a re-quarantine rename actually
// failed, or the still-canonical volume would be reaped). reconcileRetentions'
// active arm tolerates the error because cleanupOrphanedVolumes independently
// protects retention-record canonicals.
func (b *Backend) renameIfPresent(oldName, newName string) error {
	if err := b.volumes.RenameVolume(oldName, newName); err != nil {
		b.logger.Warn("reconcile rename skipped", "old", oldName, "new", newName, "error", err)
		return err
	}
	return nil
}

// reconcileRetentions repairs crash-interrupted soft-deletes/restores. MUST run
// AFTER recoverState (so b.provisions reflects live containers) and BEFORE
// cleanupOrphanedVolumes (so a mid-rename canonical dir is moved back into the
// fred-retained- namespace before the orphan reaper could destroy it).
func (b *Backend) reconcileRetentions(ctx context.Context) error {
	if b.retentionStore == nil {
		return nil
	}
	all, err := b.retentionStore.List()
	if err != nil {
		return err
	}
	for _, e := range all {
		switch e.Status {
		case shared.RetentionStatusActive:
			// Crash after Put before rename: a canonical volume may still be on disk.
			// On rename failure we log and keep going — cleanupOrphanedVolumes
			// independently protects this record's canonical from the reaper.
			for _, retained := range e.RetainedVolumeNames {
				canonical := canonicalFromRetained(retained)
				if rerr := b.renameIfPresent(canonical, retained); rerr != nil {
					b.logger.Warn("reconcile: re-quarantine of active canonical failed (cleanup protection covers it)",
						"lease_uuid", e.OriginalLeaseUUID, "canonical", canonical, "error", rerr)
				}
			}
		case shared.RetentionStatusRestoring:
			b.reconcileRestoring(ctx, e)
		}
	}
	return nil
}

// reconcileRestoring finalizes or rolls back an interrupted/failed restore,
// conservatively (defers to an in-flight restore; generation-CAS rollback).
func (b *Backend) reconcileRestoring(ctx context.Context, e shared.RetentionEntry) {
	b.provisionsMu.RLock()
	p, live := b.provisions[e.NewLeaseUUID]
	var status backend.ProvisionStatus
	if live {
		status = p.Status
	}
	b.provisionsMu.RUnlock()

	if live && status == backend.ProvisionStatusReady {
		_ = b.retentionStore.Delete(e.OriginalLeaseUUID) // restore finished; drop leftover record
		return
	}
	if live && (status == backend.ProvisionStatusProvisioning || status == backend.ProvisionStatusRestarting) {
		// A live provision at Provisioning OR Restarting is a restore that is
		// genuinely in flight — doRestore's terminal defer owns the record, so
		// defer to it rather than racing it. Restore() reserves the new-lease
		// provision at Provisioning (step b) and only reaches Restarting once the
		// actor processes evRestoreRequested; the record is already `restoring`
		// (ClaimForRestore, step d) throughout that window. A PERIODIC sweep that
		// lands in the Provisioning sub-window must NOT treat the in-flight restore
		// as orphaned — doing so would re-quarantine the just-adopted volumes and
		// spuriously fail the restore.
		//
		// Neither status can arise at STARTUP: recoverState rebuilds provisions from
		// live containers and only yields container-derived statuses (Ready/Failed);
		// Provisioning/Restarting are in-memory operation states preserved solely
		// from a pre-existing overlay entry, which a cold start does not have. So a
		// restore that crashed mid-flight is (correctly) rolled back by the orphaned
		// arm below.
		return
	}
	// Orphaned (crash/failed): tear down any orphaned project, re-quarantine the
	// adopted volumes back to the retained namespace, then CAS the record to active.
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(e.NewLeaseUUID), stopTimeout); derr != nil {
		b.logger.Warn("reconcile: compose down failed (continuing)", "lease_uuid", e.NewLeaseUUID, "error", derr)
	}
	// Re-quarantine each adopted volume. A REAL rename failure (not a benign
	// no-op) means the volume may still be canonical-named: we must NOT advance
	// the record to active or drop the provision, or cleanupOrphanedVolumes (and
	// future sweeps) could destroy still-live data. Leave the record restoring so
	// the next startup retries; the provision stays so its expected-set entry
	// keeps protecting the data in the interim.
	failed := false
	for _, retained := range e.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, e.OriginalLeaseUUID, e.NewLeaseUUID)
		if rerr := b.renameIfPresent(newCanonical, retained); rerr != nil {
			failed = true
		}
	}
	if failed {
		b.logger.Warn("reconcile: re-quarantine rename failed; leaving record restoring for next startup",
			"lease_uuid", e.OriginalLeaseUUID, "new_lease_uuid", e.NewLeaseUUID)
		return
	}
	if ok, err := b.retentionStore.RevertToActive(e.OriginalLeaseUUID, e.Generation); err != nil {
		b.logger.Error("reconcile: revert restoring->active failed", "lease_uuid", e.OriginalLeaseUUID, "error", err)
	} else if ok {
		// Re-count retained BEFORE releasing the new lease's live allocation so the
		// footprint is continuously counted (transient double-count = over-deny = safe),
		// mirroring rollbackRestoreAdoption's success arm. Without this, the new-lease
		// live pool allocation lingered after removeProvision (map delete only), leaving
		// retained F + live F = 2F in steady state (ENG-360 Fix #1).
		b.refreshRetentionAccounting()
		// Derive and release the new lease's live allocation ids using the same
		// {newLease}-{svc}-{idx} scheme as Restore()'s TryAllocateAdopt loop.
		var liveIDs []string
		for _, item := range e.Items {
			for i := range item.Quantity {
				liveIDs = append(liveIDs, fmt.Sprintf("%s-%s-%d", e.NewLeaseUUID, item.ServiceName, i))
			}
		}
		releaseAll(b.pool, liveIDs)
		updateResourceMetrics(b.pool.Stats())
		b.removeProvision(e.NewLeaseUUID)
	}
}

// evictRetentionsToCap hard-deletes the CLOSING TENANT's oldest ACTIVE records
// until at most (maxPerTenant-1) of that tenant's remain (making room for one more).
// Never touches another tenant's records. No-op when maxPerTenant<=0.
//
// excludeLease is the closing lease's OriginalLeaseUUID: it is skipped entirely
// (never counted, sorted, or evicted). On a soft-delete retry the closing lease
// may already have its own ACTIVE record from a prior attempt; without this
// exclusion the cap eviction could destroy the lease's own in-progress record =
// data loss.
func (b *Backend) evictRetentionsToCap(ctx context.Context, tenant string, maxPerTenant int, excludeLease string) error {
	if b.retentionStore == nil || maxPerTenant <= 0 || tenant == "" {
		return nil
	}
	mine, err := b.retentionStore.ListByTenant(tenant)
	if err != nil {
		return err
	}
	var active []shared.RetentionEntry
	for _, e := range mine {
		if e.OriginalLeaseUUID == excludeLease {
			continue // never evict the closing lease's own record
		}
		if e.Status == shared.RetentionStatusActive {
			active = append(active, e)
		}
	}
	if len(active) < maxPerTenant {
		return nil
	}
	sort.Slice(active, func(i, j int) bool { return active[i].CreatedAt.Before(active[j].CreatedAt) })
	// Evict oldest-first until maxPerTenant-1 remain (making room for one new entry).
	// DeleteIfActive removes the record IN-TXN before we destroy its volumes, so a
	// ClaimForRestore racing between the ListByTenant snapshot above and this loop
	// can never have its record evicted out from under it (TOCTOU-safe): a now-
	// restoring record returns deleted=false and is skipped.
	for i := 0; i <= len(active)-maxPerTenant; i++ {
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap", "tenant", tenant, "lease_uuid", active[i].OriginalLeaseUUID)
		names, deleted, err := b.retentionStore.DeleteIfActive(active[i].OriginalLeaseUUID)
		if err != nil {
			return err
		}
		if !deleted {
			continue // concurrently claimed for restore (or already gone) — skip
		}
		destroyFailed := false
		for _, name := range names {
			if derr := b.volumes.Destroy(ctx, name); derr != nil {
				b.logger.Error("evict: destroy retained volume failed", "volume", name, "error", derr)
				destroyFailed = true
			}
		}
		if destroyFailed {
			// A Destroy failed AFTER DeleteIfActive atomically removed the record,
			// which would otherwise leave the volume with NO record (never reaped/
			// enumerable = permanent leak). Re-record the snapshot entry so the volume
			// stays tracked and the next sweep/eviction retries (mirrors
			// reapExpiredRetentions). Bounded, accepted re-claim possibility: the
			// re-recorded active entry becomes restore-claimable again until reaped —
			// same trade-off the reaper makes. (Idempotent: already-destroyed names no-op.)
			if perr := b.retentionStore.Put(active[i]); perr != nil {
				b.logger.Error("evict: re-record for retry failed; volume(s) leaked until manual cleanup", "lease_uuid", active[i].OriginalLeaseUUID, "error", perr)
			}
		}
	}
	b.refreshRetentionAccounting()
	return nil
}

// volumeRootUnverifiable reports whether a volume-root probe means the orphan
// reconcile must skip this pass (fail-safe). exists/statErr come from pathExists:
// an absent root (false,nil) OR any stat error (false,err — permission denied,
// EIO, …) is unverifiable. Deliberately NOT an os.IsNotExist-only check: an
// unreadable root is as uncertain as a missing one (kubelet #72257 hazard).
func volumeRootUnverifiable(exists bool, statErr error) bool {
	return statErr != nil || !exists
}

// allVolumesAbsent reports whether none of names is in the present set. An empty
// name set is vacuously absent (covers legacy zero-volume records).
func allVolumesAbsent(names []string, present map[string]bool) bool {
	for _, n := range names {
		if present[n] {
			return false
		}
	}
	return true
}

// reconcileOrphanedRetentions prunes ACTIVE retention records whose every
// RetainedVolumeName has been absent from the node for >= N consecutive periodic
// sweeps (ENG-370 — records orphaned when their backing volumes vanish out-of-band).
//
// Fail-safe by construction: any uncertainty skips the whole pass and resets the
// in-memory confirmation streaks, because the gated action is DELETION — discarding
// a record throws away the only restore handle for a volume that may merely be
// transiently unlisted (a missing volume root makes listVolumeIDs return
// empty-with-no-error; an unreadable root is caught by the G2 gate below). Streaks
// are in-memory so a cold restart can never prune on
// its first sweep (boot-before-mount fail-safe). Returns the number pruned.
//
// No ctx: the prune does no context-bound IO (volumes are already gone, so there is
// nothing to Destroy), unlike reapExpiredRetentions which Destroys under ctx.
func (b *Backend) reconcileOrphanedRetentions() (int, error) {
	if b.retentionStore == nil {
		return 0, nil
	}
	n := b.cfg.RetentionOrphanConfirmations
	if n <= 0 {
		// Kill-switch (0 = disabled). Log once per sweep so a flip-to-0 is not silent.
		b.logger.Info("orphan retention reconcile disabled (retention_orphan_confirmations=0)")
		retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled).Inc()
		return 0, nil
	}

	// G2 — warm-view gate. A configured-but-absent/unreadable volume root makes the
	// volume enumeration untrustworthy. Skip + reset streaks. An unconfigured root
	// (noop manager) is allowed through here; the per-record verifiability check below
	// handles it.
	rootConfigured := b.cfg.VolumeDataPath != ""
	if rootConfigured {
		exists, statErr := pathExists(b.cfg.VolumeDataPath)
		if volumeRootUnverifiable(exists, statErr) {
			b.logger.Warn("orphan retention reconcile skipped: volume data root absent or unreadable (fail-safe)",
				"path", b.cfg.VolumeDataPath, "error", statErr)
			b.orphanStreaks = map[string]int{}
			retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipRootUnverifiable).Inc()
			return 0, nil
		}
	}

	// G1 — a failed enumeration is uncertainty, not "no volumes". Skip + reset.
	// No local log: returning err lets the cleanup loop (StartCleanupLoop) log it
	// once per failing sweep rather than twice — matching the sibling
	// reapExpiredRetentions, which bare-returns store errors. (A persistent failure
	// therefore logs once per tick, i.e. hourly; the metric is the precise alerting
	// signal.)
	existing, err := b.volumes.List()
	if err != nil {
		b.orphanStreaks = map[string]int{}
		retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError).Inc()
		return 0, err
	}
	present := make(map[string]bool, len(existing))
	for _, v := range existing {
		present[v] = true
	}

	recs, err := b.retentionStore.List()
	if err != nil {
		b.orphanStreaks = map[string]int{}
		retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipStoreError).Inc()
		return 0, err
	}

	next := make(map[string]int, len(b.orphanStreaks))
	var pruned int
	for _, e := range recs {
		if e.Status != shared.RetentionStatusActive {
			continue // never touch a restoring record (volumes renamed away → would look absent)
		}
		if !allVolumesAbsent(e.RetainedVolumeNames, present) {
			continue // a volume is present → not orphaned → streak resets (omit from next)
		}
		if !rootConfigured && len(e.RetainedVolumeNames) > 0 {
			continue // unverifiable without a configured root → never prune
		}
		streak := b.orphanStreaks[e.OriginalLeaseUUID] + 1
		if streak < n {
			next[e.OriginalLeaseUUID] = streak // not yet confirmed; carry forward
			continue
		}
		// Confirmed across >= n consecutive sweeps. Prune via the ACTIVE-only CAS so a
		// concurrent restore (active→restoring) is never clobbered. Volumes are already
		// gone — nothing to Destroy.
		_, deleted, derr := b.retentionStore.DeleteIfActive(e.OriginalLeaseUUID)
		switch {
		case derr != nil:
			b.logger.Error("orphan retention reconcile: delete failed", "lease_uuid", e.OriginalLeaseUUID, "error", derr)
			next[e.OriginalLeaseUUID] = streak // keep streak; retry next sweep
		case !deleted:
			// deleted=false: record no longer ACTIVE-and-present — concurrently restore-claimed
			// (active→restoring) OR already removed (e.g. cap-eviction). Benign either way; the
			// other path owns it. Drop the streak (omit from next); don't prune.
			retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipRaced).Inc()
		default:
			pruned++
			retentionOrphansPrunedTotal.Inc()
			// Per-record at DEBUG: the first cleanup can prune a large backlog (~14k on
			// dev) in a single sweep, so an aggregate INFO below carries the signal
			// without flooding the log; the metric is the precise per-record count.
			b.logger.Debug("pruned orphaned retention record (all retained volumes confirmed absent)",
				"lease_uuid", e.OriginalLeaseUUID, "confirmations", streak)
		}
	}
	b.orphanStreaks = next
	if pruned > 0 {
		b.logger.Info("pruned orphaned retention records (backing volumes confirmed absent)", "count", pruned)
	}
	return pruned, nil
}

// reapExpiredRetentions hard-deletes retained volumes past RetentionMaxAge.
// Returns the count of records FULLY reaped (all volumes destroyed AND the
// record removed). If a Destroy fails after ReapIfExpired atomically removed the
// record, the (still-expired) entry is re-recorded so the next sweep retries the
// destroy; that record is NOT counted as reaped.
func (b *Backend) reapExpiredRetentions(ctx context.Context) (int, error) {
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListExpired(b.cfg.RetentionMaxAge)
	if err != nil {
		return 0, err
	}
	var n int
	for _, e := range candidates {
		names, err := b.retentionStore.ReapIfExpired(e.OriginalLeaseUUID, b.cfg.RetentionMaxAge)
		if err != nil {
			b.logger.Error("reap: store error", "lease_uuid", e.OriginalLeaseUUID, "error", err)
			continue
		}
		if names == nil {
			continue // concurrently claimed/changed since the snapshot — skip
		}
		destroyFailed := false
		for _, name := range names {
			if derr := b.volumes.Destroy(ctx, name); derr != nil {
				b.logger.Error("reap: destroy volume", "volume", name, "error", derr)
				destroyFailed = true
			}
		}
		if destroyFailed {
			// A Destroy failed AFTER ReapIfExpired atomically removed the record.
			// Re-record the (still-expired) entry so the next sweep retries the
			// destroy; ClaimForRestore rejects expired records, so it cannot be
			// adopted in the meantime. (Idempotent: already-destroyed names no-op.)
			if perr := b.retentionStore.Put(e); perr != nil {
				b.logger.Error("reap: re-record for retry failed; volume(s) leaked until manual cleanup", "lease_uuid", e.OriginalLeaseUUID, "error", perr)
			}
			continue // not counted as reaped
		}
		n++
	}
	b.refreshRetentionAccounting()
	return n, nil
}

// runRetentionSweep is the PERIODIC reaper body: reap expired, reconcile any
// restoring records (a running-process backstop for restores that failed since
// the last tick), and prune ACTIVE records whose volumes have been absent for
// >= N consecutive sweeps (orphan reconcile, ENG-370). The BOOT path does NOT
// call this: at startup reconcileRetentions
// (before cleanup) handles restoring records and an eager reapExpiredRetentions
// (after cleanup) handles expired ones, so restoring records aren't double-reconciled.
func (b *Backend) runRetentionSweep(ctx context.Context) error {
	if _, err := b.reapExpiredRetentions(ctx); err != nil {
		return err
	}
	if b.retentionStore == nil {
		return nil
	}
	recs, err := b.retentionStore.ListRestoring()
	if err != nil {
		return err
	}
	for _, e := range recs {
		b.reconcileRestoring(ctx, e)
	}
	// ENG-370: prune orphaned records BEFORE ENG-360's accounting refresh so the
	// retained-disk projection reflects this sweep's prunes. The refresh runs even
	// when the prune returns a fail-safe error (the prune mutated nothing in that
	// case, but the reaper above may have, and refresh is keep-last-value on a store
	// read error).
	_, orphanErr := b.reconcileOrphanedRetentions()
	b.refreshRetentionAccounting()
	return orphanErr
}

// retentionSweepInterval is the pure gating decision for the periodic sweep.
// Returns (interval, enabled). The sweep runs when reaping is enabled OR
// retention is in use (RetainOnClose), so a failed restore rollback's
// restoring-record reconcile happens at runtime — not only at process restart.
// reapExpiredRetentions itself no-ops when RetentionMaxAge<=0, so in the
// retain-only mode the sweep just performs the restoring-reconcile.
func (b *Backend) retentionSweepInterval() (time.Duration, bool) {
	if b.retentionStore == nil {
		return 0, false
	}
	if b.cfg.RetentionMaxAge <= 0 && !b.cfg.RetainOnClose {
		return 0, false // nothing to reap, nothing to reconcile
	}
	interval := b.cfg.RetentionReapInterval
	if interval <= 0 {
		interval = b.cfg.RetentionMaxAge
	}
	if interval <= 0 {
		interval = time.Hour // RetentionMaxAge==0 + reap-interval unset: still reconcile restores hourly
	}
	return interval, true
}

// startRetentionReaper runs the periodic sweep on the backend's lifecycle goroutine.
func (b *Backend) startRetentionReaper() {
	interval, enabled := b.retentionSweepInterval()
	if !enabled {
		return
	}
	b.wg.Go(func() {
		util.StartCleanupLoop(b.stopCtx, interval, func() error {
			return b.runRetentionSweep(b.stopCtx)
		}, "retention", func(any) { metrics.CleanupPanicsTotal.WithLabelValues("retention").Inc() })
	})
}

// ---------------------------------------------------------------------------
// Restore as a first-class backend operation (ENG-325, Task 7b)
// ---------------------------------------------------------------------------

// totalQuantity sums the per-item quantities of a lease item set.
func totalQuantity(items []backend.LeaseItem) int {
	total := 0
	for _, item := range items {
		total += item.Quantity
	}
	return total
}

// itemsShapeMatch reports nil iff a and b carry identical service-name →
// summed-quantity maps. A restore's new-lease items must match the retained
// set's shape exactly (the volumes are addressed by serviceName×instanceIndex),
// so a divergence is a validation error.
func itemsShapeMatch(a, b []backend.LeaseItem) error {
	shape := func(items []backend.LeaseItem) map[string]int {
		m := make(map[string]int, len(items))
		for _, it := range items {
			m[it.ServiceName] += it.Quantity
		}
		return m
	}
	sa, sb := shape(a), shape(b)
	if len(sa) != len(sb) {
		return fmt.Errorf("restore items shape mismatch: retained has %d services, request has %d", len(sa), len(sb))
	}
	for svc, q := range sa {
		if sb[svc] != q {
			return fmt.Errorf("restore items shape mismatch for service %q: retained quantity %d, request %d", svc, q, sb[svc])
		}
	}
	return nil
}

// releaseAll releases every pool allocation id (best-effort, idempotent).
func releaseAll(pool *shared.ResourcePool, ids []string) {
	for _, id := range ids {
		pool.Release(id)
	}
}

// adoptRetainedVolumes renames each retained volume (fred-retained-<orig>-…)
// to its new-lease canonical name (fred-<newLease>-…). It is driven off the
// record's RetainedVolumeNames (the ACTUAL on-disk volumes enumerated at
// soft-delete), NOT a Items×Quantity re-derivation: a stateless service (no
// managed volume) has no retained name, so deriving from Items would attempt a
// rename of a volume that never existed and fail the whole restore. Returns the
// first error; the caller fully rolls back on failure. RenameVolume is
// synchronous, so no context is threaded.
func (b *Backend) adoptRetainedVolumes(newLease string, rec *shared.RetentionEntry) error {
	for _, retained := range rec.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, rec.OriginalLeaseUUID, newLease)
		if err := b.volumes.RenameVolume(retained, newCanonical); err != nil {
			return fmt.Errorf("adopt volume %s -> %s: %w", retained, newCanonical, err)
		}
	}
	return nil
}

// Restore adopts a soft-deleted lease's retained volumes into a NEW lease and
// brings up its stack from the retained manifest (ENG-325). The new lease is
// reserved at Provisioning and driven through the existing replace machinery via
// evRestoreRequested (Provisioning→Restarting→Ready|Failed).
//
// The flow is the reviewed Rev 5 design; ordering is load-bearing:
//
//	(a) validate against the retained record (read-only),
//	(b) reserve the new-lease provision at Provisioning (reject if live),
//	(c) allocate pool slots,
//	(d) ATOMICALLY claim active→restoring (closes the prelude-vs-reaper race),
//	(e) adopt: rename retained→canonical (full rollback on failure),
//	(f) hand off to the actor; doRestore's terminal defer owns success/failure/panic.
//
// Synchronous errors (validation, already-provisioned, insufficient resources,
// not-retained, not-restorable) are returned to the caller; asynchronous outcomes
// flow via the lease callback.
func (b *Backend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID, "from_lease", req.FromLeaseUUID, "tenant", req.Tenant)

	if b.retentionStore == nil {
		return backend.ErrNotRetained
	}

	// (a) Validate against the retained record (read-only; the authoritative
	// claim is atomic, step d).
	rec, err := b.retentionStore.Get(req.FromLeaseUUID)
	if err != nil {
		return fmt.Errorf("read retention store: %w", err)
	}
	if rec == nil || rec.Tenant != req.Tenant { // collapse not-found + cross-tenant into one
		if rec != nil {
			logger.Warn("restore tenant mismatch", "entry_tenant", rec.Tenant)
		}
		return backend.ErrNotRetained
	}
	// Boundary normalization (same contract as Provision/Update): a legacy
	// single-service lease arrives with ServiceName="" from the chain, but the
	// retained record's Items were normalized to defaultServiceName ("app") at
	// Provision time. Without normalizing here the shape check below would
	// deterministically mismatch ("app" vs ""), making restore impossible for
	// every single-service lease. Mutates req.Items in place (shared backing
	// array), exactly like restart_update.go's preflight.
	if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: req.Items}); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	if err := itemsShapeMatch(rec.Items, req.Items); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	// Defensive provider cross-check: the reservation uses rec.ProviderUUID, but a
	// retained record for a different provider should never be restorable here.
	// Skip when the request omits it (req.ProviderUUID == "").
	if req.ProviderUUID != "" && req.ProviderUUID != rec.ProviderUUID {
		logger.Warn("restore provider mismatch", "entry_provider", rec.ProviderUUID, "request_provider", req.ProviderUUID)
		return fmt.Errorf("%w: provider mismatch", backend.ErrValidation)
	}
	// A retained record always carries a StackManifest with at least one service
	// (written at soft-delete); a nil/empty one is a corrupt record — reject rather
	// than nil-deref below.
	if rec.StackManifest == nil || len(rec.StackManifest.Services) == 0 || len(rec.Items) == 0 {
		logger.Error("restore: corrupt retained record (nil/empty manifest or no items)")
		return fmt.Errorf("%w: corrupt retained record", backend.ErrValidation)
	}
	profiles := map[string]SKUProfile{}
	for _, item := range rec.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		p, perr := b.cfg.GetSKUProfile(item.SKU)
		if perr != nil {
			return fmt.Errorf("%w: %w", backend.ErrValidation, perr)
		}
		profiles[item.SKU] = p
	}
	for svc, m := range rec.StackManifest.Services {
		// A nil service entry is corruption (provision/recovery validate manifests);
		// guard it so a tampered record fails cleanly instead of nil-derefing m.Image.
		if m == nil {
			logger.Error("restore: corrupt retained record (nil service entry)", "service", svc)
			return fmt.Errorf("%w: service %s: nil manifest in retained record", backend.ErrValidation, svc)
		}
		if ierr := shared.ValidateImage(m.Image, b.cfg.AllowedRegistries); ierr != nil {
			return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svc, ierr)
		}
	}

	// (b) Reserve the new-lease entry at Status=Provisioning. (7a permits
	// evRestoreRequested from Provisioning.) Reject if already provisioned.
	b.provisionsMu.Lock()
	if _, exists := b.provisions[req.LeaseUUID]; exists {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID)
	}
	b.provisions[req.LeaseUUID] = recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:         req.LeaseUUID,
			Tenant:            rec.Tenant,
			ProviderUUID:      rec.ProviderUUID,
			SKU:               rec.Items[0].SKU,
			Status:            backend.ProvisionStatusProvisioning,
			Quantity:          totalQuantity(rec.Items),
			CreatedAt:         time.Now(),
			FailCount:         0,
			LastError:         "",
			CallbackURL:       req.CallbackURL,
			Items:             slices.Clone(rec.Items),
			ContainerIDs:      make([]string, 0),
			StackManifest:     rec.StackManifest,
			ServiceContainers: nil,
		},
		volumeCleanupAttempts: 0,
	}.materialize()
	b.provisionsMu.Unlock()

	// (c) Allocate pool slots.
	var allocatedIDs []string
	for _, item := range rec.Items {
		for i := range item.Quantity {
			id := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
			if aerr := b.pool.TryAllocateAdopt(id, item.SKU, rec.Tenant); aerr != nil {
				releaseAll(b.pool, allocatedIDs)
				updateResourceMetrics(b.pool.Stats())
				b.removeProvision(req.LeaseUUID)
				return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, aerr)
			}
			allocatedIDs = append(allocatedIDs, id)
		}
	}
	// Refresh the resource gauges now that the allocation succeeded, mirroring
	// Provision/Deprovision's on-success refresh. The rollback paths re-refresh
	// after releaseAll, so this only stands as the live value on the success arm.
	updateResourceMetrics(b.pool.Stats())

	// (d) ATOMIC claim active->restoring (closes the prelude-vs-reaper race).
	// Nothing renamed yet.
	claimed, err := b.retentionStore.ClaimForRestore(req.FromLeaseUUID, req.LeaseUUID, b.cfg.RetentionMaxAge)
	if err != nil {
		releaseAll(b.pool, allocatedIDs)
		updateResourceMetrics(b.pool.Stats())
		b.removeProvision(req.LeaseUUID)
		switch {
		case errors.Is(err, shared.ErrNoRetention):
			return backend.ErrNotRetained
		case errors.Is(err, shared.ErrNotRestorable):
			return fmt.Errorf("%w: %w", backend.ErrInvalidState, err)
		default:
			return fmt.Errorf("claim retention: %w", err)
		}
	}

	// Claim flipped the record active→restoring (drops it from the active
	// projection); the live allocation above already counts the bytes, so this
	// keeps the gauge/projection consistent without an under-count window.
	b.refreshRetentionAccounting()

	// (e) Adopt: rename retained->canonical. On failure, full rollback. The
	// worker never ran, so no actor terminal transition is coming — drop the
	// reservation (dropProvision=true). Timed as the restore "adopt" phase: it
	// is the only re-deploy work outside the async worker (doReplaceContainers),
	// so it must be measured here, in the synchronous prelude, to rule the
	// rename in/out as a contributor to restore latency.
	adoptStart := time.Now()
	if err := b.adoptRetainedVolumes(req.LeaseUUID, claimed); err != nil {
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, true, logger)
		return fmt.Errorf("adopt retained volumes: %w", err)
	}
	replacePhaseDurationSeconds.WithLabelValues("restore", phaseAdopt).Observe(time.Since(adoptStart).Seconds())

	// (f) Hand off to the actor; doRestore's terminal defer owns
	// success/failure/panic.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doRestore(opCtx, req.LeaseUUID, claimed, profiles, allocatedIDs, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.RestoreRequestedMsg{
		Cancel: opCancel, Work: work, Ack: ack, CallbackURL: req.CallbackURL,
	}); routeErr != nil {
		opCancel()
		// Worker never ran; no actor transition will flip Status — drop the
		// reservation (dropProvision=true).
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, true, logger)
		return routeErr
	}
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		// ackOrAbort returned !accepted: the actor rejected the message (it never
		// fired evRestoreRequested) OR we abandoned on cancellation without the
		// actor committing — either way no terminal transition owns this
		// provision, so drop the reservation (dropProvision=true).
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, true, logger)
		return err
	}
	return nil
}

// doRestore is the restore worker (runs on the lease actor's replace-worker
// goroutine). It brings up the new lease's stack from the retained manifest via
// doReplaceContainers with NoComposeRollback (no prior containers to recover to).
//
// Its terminal defer is the SOLE owner of the success/failure/panic outcome for
// the retention record:
//   - success (resultRet.Err==nil): delete the retained record (data adopted).
//   - failure (resultRet.Err!=nil): roll back the adoption (re-quarantine the
//     volumes, revert the record to active).
//   - panic: a panic leaves resultRet.Err==nil; force the failure path so we
//     never delete the record while the lease is not Ready. Convert panic→Failed.
//
// In BOTH the failure and panic cases the rollback does NOT drop the provision
// (dropProvision=false): doRestore returns an errored ReplaceResult, so the actor
// will fire evReplaceFailed and onEnterFailedFromReplace flips Status=Failed and
// emits the failure callback — which reads CallbackURL from the still-present
// provision. The lease then settles as a Failed entry, exactly like a failed
// restart/update. (Dropping it here would race that transition and silently lose
// the callback; the periodic reaper / a subsequent op reconciles the entry.)
func (b *Backend) doRestore(ctx context.Context, leaseUUID string, rec *shared.RetentionEntry,
	profiles map[string]SKUProfile, allocatedIDs []string, logger *slog.Logger) (resultRet leasesm.ReplaceResult) {
	restoreStart := time.Now()
	defer func() {
		// N2: a panic leaves resultRet.Err==nil; force the failure path so we never
		// delete the record while the lease is not Ready. Convert panic -> Failed.
		if r := recover(); r != nil {
			logger.Error("restore worker panicked", "recover", r)
			b.rollbackRestoreAdoption(ctx, leaseUUID, allocatedIDs, rec, false, logger)
			// Mirror spawnReplaceWorker's own panic recovery (lease_actor.go) and the
			// normal doReplace* failure shape: populate top-level CallbackErr AND
			// Failure.{Operation,CallbackErr,LastError} so the actor's evReplaceFailed
			// carries a non-empty ReplaceFailureInfo (otherwise the tenant callback is
			// empty/unhelpful).
			msg := fmt.Sprintf("restore panic: %v", r)
			resultRet = leasesm.ReplaceResult{
				Err:         errors.New(msg),
				Restored:    false,
				CallbackErr: leasesm.ErrMsgInternal,
				Failure: leasesm.ReplaceFailureInfo{
					Operation:   "restore",
					CallbackErr: leasesm.ErrMsgInternal,
					LastError:   msg,
				},
			}
			return
		}
		if resultRet.Err == nil {
			// Record the restore re-deploy worker latency on success only (mirrors
			// the loadtest's success-only rs_restore_duration). The synchronous adopt
			// phase ran before this worker; restore_duration_seconds covers the
			// async re-deploy worker span, which is the ~3-4x cost ENG-357 targets.
			restoreDurationSeconds.Observe(time.Since(restoreStart).Seconds())
			if delErr := b.retentionStore.Delete(rec.OriginalLeaseUUID); delErr != nil {
				logger.Warn("restore ok but failed to delete retention record", "error", delErr)
			}
			b.refreshRetentionAccounting()
			return
		}
		b.rollbackRestoreAdoption(ctx, leaseUUID, allocatedIDs, rec, false, logger)
	}()
	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID: leaseUUID, Stack: rec.StackManifest, Items: rec.Items, Profiles: profiles,
		OldContainerIDs: nil, ServiceContainers: nil, Operation: "restore", NoComposeRollback: true, Logger: logger,
		OnSuccess: func(p *leasesm.ProvisionState) { p.StackManifest = rec.StackManifest },
	})
}

// rollbackRestoreAdoption is the idempotent compensating teardown for an adopted
// restore. N1: compose.Down the new project FIRST (stop containers on the
// bind-mounted volumes) BEFORE renaming volumes back — otherwise a still-running
// container holds the volume's bind mount open. It then re-quarantines each
// adopted volume to the retained namespace, releases the pool allocations, and
// CAS-reverts the record to active.
//
// dropProvision controls the new-lease reservation:
//   - true  (synchronous paths: adopt failure, route failure, ack abort): no
//     actor terminal transition is coming, so the reservation would leak — remove it.
//   - false (worker failure/panic from doRestore's defer): the actor WILL fire
//     evReplaceFailed; onEnterFailedFromReplace must read CallbackURL from the
//     still-present provision to emit the failure callback, then flips it to
//     Failed. Removing it here would race that transition and drop the callback.
//
// A REAL re-quarantine rename failure (not a benign no-op) means an adopted
// volume may still be canonical-named under the new lease, so the on-disk state
// no longer matches the record. Mirroring reconcileRestoring, we then LEAVE the
// record restoring (do NOT RevertToActive, do NOT removeProvision) and return:
// the next reconcile sweep retries the re-quarantine safely, and meanwhile the
// provision's expected-set entry (cleanupOrphanedVolumes' restoring arm) protects
// the canonical volume from the orphan reaper. Reverting here would make that
// still-live data eligible for cleanup/reaping.
func (b *Backend) rollbackRestoreAdoption(ctx context.Context, leaseUUID string,
	allocatedIDs []string, rec *shared.RetentionEntry, dropProvision bool, logger *slog.Logger) {
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); derr != nil {
		logger.Warn("restore rollback: compose down failed (continuing)", "error", derr)
	}
	failed := false
	for _, retained := range rec.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, rec.OriginalLeaseUUID, leaseUUID)
		if rerr := b.renameIfPresent(newCanonical, retained); rerr != nil {
			failed = true
		}
	}
	if failed {
		// Re-quarantine rename failed: the bytes remain on disk under the new-lease
		// canonical name and the record stays 'restoring' for the next reconcile
		// sweep. KEEP the live allocation counted (do NOT releaseAll) — releasing
		// while the bytes persist and the restoring record is excluded from the
		// retained projection would under-count → over-admit. The dead lease's live
		// allocation is reclaimed when it is deprovisioned / on recover.
		logger.Warn("restore rollback: re-quarantine rename failed; leaving record restoring + live counted for reconcile sweep",
			"lease_uuid", rec.OriginalLeaseUUID, "new_lease_uuid", leaseUUID)
		return
	}
	// Re-quarantine succeeded: revert the record to active and re-count it as
	// retained BEFORE releasing the live allocation, so the footprint is counted
	// continuously (transient double-count = over-deny = safe), mirroring the
	// deprovision retain hand-off.
	if ok, err := b.retentionStore.RevertToActive(rec.OriginalLeaseUUID, rec.Generation); err != nil {
		logger.Error("restore rollback: revert record failed", "error", err)
	} else if !ok {
		logger.Warn("restore rollback: record generation changed; reaper will reconcile", "lease_uuid", rec.OriginalLeaseUUID)
	}
	b.refreshRetentionAccounting() // retained += F (record now active)
	releaseAll(b.pool, allocatedIDs)
	updateResourceMetrics(b.pool.Stats())
	if dropProvision {
		b.removeProvision(leaseUUID)
	}
}
