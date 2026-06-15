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

// destroyRetained hard-deletes one record's volumes then the record itself.
// Used by the per-tenant cap eviction and the grace reaper.
func (b *Backend) destroyRetained(ctx context.Context, e shared.RetentionEntry) error {
	for _, name := range e.RetainedVolumeNames {
		if err := b.volumes.Destroy(ctx, name); err != nil {
			return fmt.Errorf("destroy retained volume %s: %w", name, err)
		}
	}
	return b.retentionStore.Delete(e.OriginalLeaseUUID)
}

// renameIfPresent is a best-effort reconcile rename: RenameVolume errors both
// for the benign no-op/conflict cases (both or neither name present) AND for a
// real Docker-daemon failure. It logs and RETURNS the error so callers can
// decide whether the failure is fatal to their step (e.g. the restoring-arm
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
	if live && status == backend.ProvisionStatusRestarting {
		// Restarting only arises during the PERIODIC reaper sweep (Task 6) while
		// fred is running and a restore is genuinely in flight — doRestore's
		// terminal defer owns the record, so defer to it. At STARTUP this branch
		// cannot fire: recoverState rebuilds provisions from live containers and
		// never yields Restarting, so a restore that crashed mid-flight is
		// (correctly) rolled back by the orphaned-arm below.
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
	for _, item := range e.Items {
		for i := range item.Quantity {
			newCanonical := canonicalVolumeName(e.NewLeaseUUID, item.ServiceName, i)
			origRetained := retainedName(canonicalVolumeName(e.OriginalLeaseUUID, item.ServiceName, i))
			if rerr := b.renameIfPresent(newCanonical, origRetained); rerr != nil {
				failed = true
			}
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
		b.removeProvision(e.NewLeaseUUID)
	}
}

// evictRetentionsToCap hard-deletes the CLOSING TENANT's oldest ACTIVE records
// until at most (maxPerTenant-1) of that tenant's remain (making room for one more).
// Never touches another tenant's records. No-op when maxPerTenant<=0.
func (b *Backend) evictRetentionsToCap(ctx context.Context, tenant string, maxPerTenant int) error {
	if b.retentionStore == nil || maxPerTenant <= 0 || tenant == "" {
		return nil
	}
	mine, err := b.retentionStore.ListByTenant(tenant)
	if err != nil {
		return err
	}
	var active []shared.RetentionEntry
	for _, e := range mine {
		if e.Status == shared.RetentionStatusActive {
			active = append(active, e)
		}
	}
	if len(active) < maxPerTenant {
		return nil
	}
	sort.Slice(active, func(i, j int) bool { return active[i].CreatedAt.Before(active[j].CreatedAt) })
	// Evict oldest-first until maxPerTenant-1 remain (making room for one new entry).
	for i := 0; i <= len(active)-maxPerTenant; i++ {
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap", "tenant", tenant, "lease_uuid", active[i].OriginalLeaseUUID)
		if err := b.destroyRetained(ctx, active[i]); err != nil {
			return err
		}
	}
	return nil
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
	return n, nil
}

// runRetentionSweep is the PERIODIC reaper body: reap expired + reconcile any
// restoring records (a running-process backstop for restores that failed since
// the last tick). The BOOT path does NOT call this: at startup reconcileRetentions
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
	return nil
}

// startRetentionReaper runs the periodic sweep on the backend's lifecycle goroutine.
func (b *Backend) startRetentionReaper() {
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return
	}
	interval := b.cfg.RetentionReapInterval
	if interval <= 0 {
		interval = b.cfg.RetentionMaxAge
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
// to its new-lease canonical name (fred-<newLease>-…). Returns the first error;
// the caller fully rolls back on failure. RenameVolume is synchronous, so no
// context is threaded.
func (b *Backend) adoptRetainedVolumes(newLease string, rec *shared.RetentionEntry) error {
	for _, item := range rec.Items {
		for i := range item.Quantity {
			retained := retainedName(canonicalVolumeName(rec.OriginalLeaseUUID, item.ServiceName, i))
			canonical := canonicalVolumeName(newLease, item.ServiceName, i)
			if err := b.volumes.RenameVolume(retained, canonical); err != nil {
				return fmt.Errorf("adopt volume %s -> %s: %w", retained, canonical, err)
			}
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
	// A retained record always carries a StackManifest (written at soft-delete);
	// a nil one is a corrupt record — reject rather than nil-deref below.
	if rec.StackManifest == nil || len(rec.Items) == 0 {
		logger.Error("restore: corrupt retained record (nil manifest or no items)")
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
		if ierr := shared.ValidateImage(m.Image, b.cfg.AllowedRegistries); ierr != nil {
			return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svc, ierr)
		}
	}

	// (b) Reserve the new-lease entry at Status=Provisioning, tagged
	// restoringFrom. (7a permits evRestoreRequested from Provisioning.) Reject
	// if already provisioned.
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
		restoringFrom:         req.FromLeaseUUID,
	}.materialize()
	b.provisionsMu.Unlock()

	// (c) Allocate pool slots.
	var allocatedIDs []string
	for _, item := range rec.Items {
		for i := range item.Quantity {
			id := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
			if aerr := b.pool.TryAllocate(id, item.SKU, rec.Tenant); aerr != nil {
				releaseAll(b.pool, allocatedIDs)
				updateResourceMetrics(b.pool.Stats())
				b.removeProvision(req.LeaseUUID)
				return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, aerr)
			}
			allocatedIDs = append(allocatedIDs, id)
		}
	}

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

	// (e) Adopt: rename retained->canonical. On failure, full rollback. The
	// worker never ran, so no actor terminal transition is coming — drop the
	// reservation (dropProvision=true).
	if err := b.adoptRetainedVolumes(req.LeaseUUID, claimed); err != nil {
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, true, logger)
		return fmt.Errorf("adopt retained volumes: %w", err)
	}

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
	defer func() {
		// N2: a panic leaves resultRet.Err==nil; force the failure path so we never
		// delete the record while the lease is not Ready. Convert panic -> Failed.
		if r := recover(); r != nil {
			logger.Error("restore worker panicked", "recover", r)
			b.rollbackRestoreAdoption(ctx, leaseUUID, allocatedIDs, rec, false, logger)
			resultRet = leasesm.ReplaceResult{Err: fmt.Errorf("restore panic: %v", r), Restored: false}
			return
		}
		if resultRet.Err == nil {
			if delErr := b.retentionStore.Delete(rec.OriginalLeaseUUID); delErr != nil {
				logger.Warn("restore ok but failed to delete retention record", "error", delErr)
			}
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
func (b *Backend) rollbackRestoreAdoption(ctx context.Context, leaseUUID string,
	allocatedIDs []string, rec *shared.RetentionEntry, dropProvision bool, logger *slog.Logger) {
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); derr != nil {
		logger.Warn("restore rollback: compose down failed (continuing)", "error", derr)
	}
	for _, item := range rec.Items {
		for i := range item.Quantity {
			newCanonical := canonicalVolumeName(leaseUUID, item.ServiceName, i)
			origRetained := retainedName(canonicalVolumeName(rec.OriginalLeaseUUID, item.ServiceName, i))
			_ = b.renameIfPresent(newCanonical, origRetained)
		}
	}
	releaseAll(b.pool, allocatedIDs)
	updateResourceMetrics(b.pool.Stats())
	if ok, err := b.retentionStore.RevertToActive(rec.OriginalLeaseUUID, rec.Generation); err != nil {
		logger.Error("restore rollback: revert record failed", "error", err)
	} else if !ok {
		logger.Warn("restore rollback: record generation changed; reaper will reconcile", "lease_uuid", rec.OriginalLeaseUUID)
	}
	if dropProvision {
		b.removeProvision(leaseUUID)
	}
}
