package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Deprovision is the public shim: it routes the request through the lease's
// actor so that container-death and deprovision messages serialize per lease.
// Routing forces a Ready/Failing/Failed → Deprovisioning SM transition whose
// Failing.OnExit cancels the in-flight diag goroutine — the structural
// suppression of stale Failed callbacks.
func (b *Backend) Deprovision(ctx context.Context, leaseUUID string) error {
	reply := make(chan error, 1)
	if err := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.DeprovisionMsg{Ctx: ctx, Reply: reply}); err != nil {
		return err
	}
	return b.waitForReply(ctx, reply)
}

// handleDeprovision (lease-actor message handler) moved to
// internal/backend/shared/leasesm/lease_actor.go at PR5b-2 BC.
// doDeprovision (Backend method below) stays here; the substrate-agnostic
// SM/actor reaches it via cfg.DoDeprovisionFn.

// doDeprovision releases resources for a lease. Must be idempotent.
// For multi-unit leases, removes all containers.
// Returns an error if any container removal fails for a reason other than
// the container already being gone (which is handled idempotently by
// RemoveContainer).
//
// On partial failure (some containers removed, some stuck), the provision
// is kept in the map with Status=Failed and ContainerIDs narrowed to only
// the failed removals. Resource pool allocations are NOT released on this
// branch — the volumes are still on disk and the lease is retried — so the
// reservation keeps counting until a terminal success or give-up releases it.
// On retry, only the stuck containers are attempted.
func (b *Backend) doDeprovision(ctx context.Context, leaseUUID string) error {
	logger := b.logger.With("lease_uuid", leaseUUID)

	// Mark Deprovisioning before removing containers (the in-memory marker lets
	// Provision's status guard reject concurrent re-provision during the removal
	// window). Capture the teardown inputs inside the closure; the metric Dec is
	// a side effect kept OUTSIDE the closure (UpdateFn no-side-effect contract).
	var (
		wasReady      bool
		containerIDs  []string
		items         []backend.LeaseItem
		tenant        string
		callbackURL   string
		providerUUID  string
		stackManifest *manifest.StackManifest
		// volumesRetained is best-effort ground truth: set true only when the
		// soft-delete path renamed all volumes into the retained namespace
		// without error. Carried to the terminal deprovisioned callback so a
		// connected tenant gets a low-latency retained hint. (Named distinctly
		// from the inner `retained []string` volume-name slice below.)
		volumesRetained bool
	)
	exists := b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
		wasReady = p.Status == backend.ProvisionStatusReady
		p.Status = backend.ProvisionStatusDeprovisioning
		containerIDs = append([]string(nil), p.ContainerIDs...)
		items = append([]backend.LeaseItem(nil), p.Items...)
		tenant = p.Tenant
		callbackURL = p.CallbackURL
		providerUUID = p.ProviderUUID
		stackManifest = p.StackManifest
	})
	if !exists {
		// Already deprovisioned (no live container / in-flight op for this lease).
		// Still purge any stranded releases.db history before returning: a lease whose
		// container was already gone at on-chain close reaches doDeprovision via the
		// lease_closed event but short-circuits here ~400 lines before the terminal
		// releaseStore.Delete, leaving a stale "active" record that audit-lease-status
		// flags until the 90-day RemoveOlderThan TTL. (ENG-410)
		b.purgeReleaseHistory(leaseUUID, logger)
		return nil
	}
	// Decrement activeProvisions on the Ready→Deprovisioning transition so the
	// gauge stays accurate even if Deprovision later fails partially.
	if wasReady {
		activeProvisions.Dec()
	}

	// Remove all containers via Compose Down for atomic cleanup; fall back
	// to individual RemoveContainer if Compose fails (e.g., compose project
	// metadata went missing). After Tasks 4-6 every provision is stack-
	// shaped, so the per-container fallback only fires under genuine
	// substrate failure rather than as the steady-state legacy path.
	var errs []error
	var failedIDs []string
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if downErr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); downErr != nil {
		logger.Warn("compose down failed, falling back to individual removal", "error", downErr)
		for _, containerID := range containerIDs {
			if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
				logger.Error("failed to remove container", "container_id", leasesm.ShortID(containerID), "error", err)
				errs = append(errs, fmt.Errorf("container %s: %w", leasesm.ShortID(containerID), err))
				failedIDs = append(failedIDs, containerID)
			} else {
				logger.Info("container removed", "container_id", leasesm.ShortID(containerID))
			}
		}
	} else {
		logger.Info("compose down completed", "project", composeProjectName(leaseUUID))
	}

	// releaseLive releases all pool allocations for this lease and updates
	// resource metrics. On the non-retain path it is called AFTER all volumes
	// are destroyed without error (mirroring the refuse-to-retain arm); on the
	// retain path it is deferred until after refreshRetentionAccounting counts
	// the retained record, so the footprint is never momentarily uncounted while
	// the renamed volume persists on disk (no over-admit gap).
	releaseLive := func() {
		for _, item := range items {
			for i := range item.Quantity {
				b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
			}
		}
		updateResourceMetrics(b.pool.Stats())
	}
	retaining := b.cfg.RetainOnClose && b.retentionStore != nil
	// Non-retain release happens AFTER successful volume destroy (see the
	// else branch below). A teardown that only partially succeeds (container
	// or volume error) keeps resources counted for the retry rather than
	// freeing them while stuck containers still run or volumes remain on disk.
	//
	// Retaining close: keep the live allocation counted until the retained
	// record is recorded+refreshed (or the volumes are destroyed) — see the
	// deferred hand-off below — so the footprint is never momentarily
	// uncounted while the renamed volume persists on disk (prevents a
	// concurrent over-admit / ENOSPC).

	// releaseLiveOnRetainPath is set true at retain-path terminal points where
	// the closing lease's footprint F is either recorded-as-retained or
	// destroyed. The deferred hand-off releases live AFTER the retained
	// projection is refreshed, ensuring overlap, never a gap. On error paths
	// (no record written, volumes still canonical on disk) it stays false so
	// the live allocation keeps counting the bytes.
	var releaseLiveOnRetainPath bool

	// Retained set may have changed (this close may have added a retained
	// record below, or a prior attempt did); refresh after the volume branch.
	// For the retain path, also release live AFTER refresh (overlap, no gap).
	//
	// Gated on `retaining`: the non-retain else branch only destroys this lease's
	// own canonical volumes and never touches the retention store, so the retained
	// projection cannot change on a non-retain close — skip the O(#retained) bbolt
	// List() scan entirely on that (hot) path. releaseLiveOnRetainPath is only ever
	// set inside the retain branch, and non-retain releases live inline after a
	// successful destroy, so nothing is missed by returning early here.
	defer func() {
		if !retaining {
			return
		}
		b.refreshRetentionAccounting()
		if releaseLiveOnRetainPath {
			releaseLive()
		}
	}()

	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers.
		var diagSnap shared.DiagnosticEntry
		b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			diagSnap = leasesm.DiagnosticSnapshot(p)
		})
		// Unlike the initial-mark migration, do NOT early-return on UpdateFn==false:
		// still persist diagnostics and surface the error. If the entry is gone,
		// diagSnap is zero-value and persistDiagnostics no-ops on its empty guard.
		b.persistDiagnostics(diagSnap, failedIDs)
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// Destroy managed volumes for all instances — or soft-delete them into the
	// retained namespace when RetainOnClose is true.
	var volumeErrs []error
	if b.cfg.RetainOnClose && b.retentionStore != nil {
		// Enumerate the lease's ACTUAL managed volumes (ground truth — no SKU guess).
		all, listErr := b.volumes.List()
		if listErr != nil {
			volumeErrs = append(volumeErrs, fmt.Errorf("list volumes for retention: %w", listErr))
		}
		var canonical []string
		prefix := leaseVolumePrefix(leaseUUID)
		for _, id := range all {
			if strings.HasPrefix(id, prefix) { // excludes fred-retained-* and other leases
				canonical = append(canonical, id)
			}
		}
		// RETRY-SAFE MERGE: on a retry after a partial rename, b.volumes.List no
		// longer returns the volumes already renamed to fred-retained-{lease}-… on
		// the prior attempt, so `canonical` only covers the STILL-canonical ones.
		// PutActiveMerged unions the retained names of the still-canonical set with
		// any existing ACTIVE record's RetainedVolumeNames (single txn), so a retry
		// never drops already-retained volumes (which would leak them) or overwrites
		// the prior record with a shorter list — and never clobbers a record that a
		// concurrent restore claimed (active→restoring) mid-flight.

		// ENG-406: reclaim writable-path-only volumes instead of retaining them.
		// A canonical volume whose only content is the ephemeral _wp/ scaffolding
		// (no declared-VOLUME data) preserves nothing restorable — restore reseeds
		// _wp from the image regardless (ENG-367 contract) — so retaining it only
		// pollutes a per-tenant slot, the retained-disk budget, and leaves a
		// fred-retained-* dir. Destroy those now (isWritablePathOnly is conservative:
		// it returns true only for PROVABLY _wp-only volumes, so a stateful volume is
		// never destroyed) and retain the rest. Only the VOLUME NAMES are narrowed
		// (retainCanonical → RetainedVolumeNames); the record's Items and
		// StackManifest MUST stay the FULL set (see the record write below).
		retainCanonical := make([]string, 0, len(canonical))
		for _, c := range canonical {
			if b.isWritablePathOnly(c) {
				if err := b.volumes.Destroy(ctx, c); err != nil {
					// Destroy failed → the volume is still canonical on disk. Record
					// the error so the lease stays Failed and retries (re-detecting
					// and re-destroying it); do NOT add it to retainCanonical — it
					// must never be retained.
					logger.Error("failed to reclaim writable-path-only volume", "volume", c, "error", err)
					volumeErrs = append(volumeErrs, fmt.Errorf("reclaim writable-path-only volume %s: %w", c, err))
				} else {
					retentionWritablePathReclaimedTotal.Inc()
					logger.Info("reclaimed writable-path-only volume on close", "volume", c)
				}
				continue
			}
			retainCanonical = append(retainCanonical, c)
		}
		// durableItems = the per-instance retained footprint, used ONLY for the cap
		// check (shouldRefuseRetention), NOT for the record. Each item's Quantity is
		// narrowed to the number of its instances actually retained: classification is
		// per-volume, so a Quantity>1 service can have a SUBSET of instances retained
		// (e.g. one instance's host path hits a transient ReadDir error → retained
		// conservatively, others reclaimed). The cap-refuse action is DESTROY, so the
		// cap input must NOT over-count — counting a service's full Quantity when only
		// some instances are retained could spuriously breach the cap and destroy the
		// retained durable volumes. The persisted record keeps the FULL items (below);
		// over-counting THERE feeds an admission DENY gate (safe), whereas
		// under-counting the record would over-admit (ENG-360/376).
		retainSet := make(map[string]struct{}, len(retainCanonical))
		for _, c := range retainCanonical {
			retainSet[c] = struct{}{}
		}
		durableItems := make([]backend.LeaseItem, 0, len(items))
		for _, item := range items {
			retained := 0
			for i := range item.Quantity {
				if _, ok := retainSet[canonicalVolumeName(leaseUUID, item.ServiceName, i)]; ok {
					retained++
				}
			}
			if retained > 0 {
				durItem := item
				durItem.Quantity = retained
				durableItems = append(durableItems, durItem)
			}
		}

		switch {
		case len(retainCanonical) == 0:
			// No durable (declared-VOLUME) data remains to retain: the lease was
			// stateless, its volumes were already renamed on a prior attempt, or
			// they were all writable-path-only and just reclaimed above. Release
			// live only when nothing errored — a List error or a failed wp-only
			// destroy leaves bytes on disk, so keep live counted (flag stays false)
			// and let the retry re-attempt.
			if len(volumeErrs) == 0 {
				releaseLiveOnRetainPath = true
			}
		case b.shouldRefuseRetention(leaseUUID, durableItems):
			volumeErrs = append(volumeErrs, b.destroyOnRefuseToRetain(ctx, retainCanonical, leaseUUID, tenant, logger)...)
			if len(volumeErrs) == 0 {
				// Every byte is gone — the refused stateful volumes here AND any
				// writable-path-only volumes reclaimed before the switch — so release
				// the live allocation. Guard on the OVERALL error count (not just new
				// errors from destroyOnRefuseToRetain): a pre-switch wp-only Destroy
				// failure already in volumeErrs leaves bytes on disk, so keep live
				// counted and let the retry re-attempt rather than under-count
				// (over-admit/ENOSPC). Consistent with the other release-live arms.
				releaseLiveOnRetainPath = true
			}
		default:
			// existing retain logic — UNCHANGED, just moved under `default:`
			retained := make([]string, 0, len(retainCanonical))
			for _, c := range retainCanonical {
				retained = append(retained, retainedName(c))
			}
			// Best-effort cap room BEFORE the write. Dropping the standalone Get-guard
			// means a rare wasted eviction here if PutActiveMerged then defers (a
			// restore raced in): acceptable — it evicts the tenant's oldest, which the
			// next attempt would evict anyway.
			if err := b.evictRetentionsToCap(ctx, tenant, b.cfg.MaxRetainedLeasesPerTenant, leaseUUID); err != nil {
				logger.Warn("retention cap eviction failed", "tenant", tenant, "error", err)
			}
			// Hydrate a nil StackManifest from the release store so the retained data
			// stays API-restorable. A cold-start recover restores the manifest
			// best-effort (recover.go) and leaves it nil if the active release is
			// missing/unparseable/store-nil; Restore rejects a nil-manifest record as
			// corrupt, so without this the volumes are retained but un-restorable.
			// Mirror recover.go's LatestActive + ParsePayload guard exactly.
			if stackManifest == nil && b.releaseStore != nil {
				if rel, relErr := b.releaseStore.LatestActive(leaseUUID); relErr == nil && rel != nil && len(rel.Manifest) > 0 {
					if stackM, payloadErr := manifest.ParsePayload(rel.Manifest); payloadErr != nil {
						logger.Warn("soft-delete: failed to parse release manifest for retention hydration", "error", payloadErr)
					} else {
						stackManifest = stackM
					}
				}
			}
			if stackManifest == nil {
				// Still nil after hydration: preserve the data (write the record) but
				// warn loudly that it cannot be restored through the API.
				logger.Warn("soft-delete: retained data will NOT be API-restorable (no manifest for lease); volumes preserved for manual recovery",
					"lease_uuid", leaseUUID)
			}

			// RECORD-FIRST + ATOMIC: PutActiveMerged persists the active record (with
			// the MERGED retained set) before any rename in ONE bbolt txn. CreatedAt
			// (grace clock) and Generation (CAS) are preserved across retries by the
			// store. ok=false means a restore claimed the record concurrently — defer.
			// Items and StackManifest MUST be the FULL lease set, NOT narrowed to the
			// retained (durable) subset. Restore validates the record against the
			// chain's full item set (itemsShapeMatch, restore.go) — the chain never
			// saw the wp-only reclaim — so a narrowed Items would make EVERY restore
			// fail (shape mismatch), stranding the retained stateful volume until the
			// reaper destroys it: unrecoverable tenant data loss. On restore the
			// reclaimed wp-only services simply get a fresh volume (RetainedVolumeNames
			// omits them), reseeded from the image — exactly the ENG-367 contract. Only
			// RetainedVolumeNames is narrowed to the durable volumes actually retained.
			base := shared.RetentionEntry{
				OriginalLeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: providerUUID,
				Items: items, StackManifest: stackManifest, CallbackURL: callbackURL,
				RetainedVolumeNames: retained, Status: shared.RetentionStatusActive,
				CreatedAt: time.Now(), Generation: 0,
			}
			ok, err := b.retentionStore.PutActiveMerged(base)
			switch {
			case err != nil:
				logger.Error("failed to write retention record", "lease_uuid", leaseUUID, "error", err)
				volumeErrs = append(volumeErrs, fmt.Errorf("write retention record: %w", err))
			case !ok:
				// A restore claimed the record (active→restoring) between our volume
				// enumeration and the write. Renaming or reverting now would corrupt the
				// restore rollback's generation-CAS. Defer — keep the lease Failed so the
				// volume-cleanup retry re-attempts after the restore resolves (the record
				// is back to active, or gone if the restore succeeded).
				logger.Warn("soft-delete deferred: record claimed for restore concurrently; will retry")
				volumeErrs = append(volumeErrs, fmt.Errorf("retention record for %s is being restored; deferring", leaseUUID))
			default:
				// Only the STILL-canonical volumes need renaming; the already-retained
				// ones (from a prior attempt) are done.
				for _, c := range retainCanonical {
					if err := b.volumes.RenameVolume(c, retainedName(c)); err != nil {
						logger.Error("failed to retain volume", "volume", c, "error", err)
						volumeErrs = append(volumeErrs, fmt.Errorf("retain volume %s: %w", c, err))
					}
				}
				if len(volumeErrs) == 0 {
					volumesRetained = true
					// All renames succeeded: F is now recorded-as-retained in the
					// store (PutActiveMerged) and volumes live under fred-retained-*
					// names. Signal the deferred hand-off to release live AFTER
					// refresh — bytes stay continuously counted, no gap.
					releaseLiveOnRetainPath = true
					logger.Info("soft-deleted lease volumes", "lease_uuid", leaseUUID, "retained", len(retained))
				}
			}
		}
	} else {
		for _, item := range items {
			for i := range item.Quantity {
				volumeID := canonicalVolumeName(leaseUUID, item.ServiceName, i)
				if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
					logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
					volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
				}
			}
		}
		if len(volumeErrs) == 0 {
			// All volumes destroyed — bytes are gone, so release the live allocation
			// now. Releasing only on success (not before the loop) keeps the footprint
			// counted while a failed Destroy leaves bytes on disk and the lease is kept
			// Failed for retry, preventing an over-admit/ENOSPC window. Symmetric with
			// the retain arms (releaseLiveOnRetainPath set only when
			// len(volumeErrs) == 0).
			releaseLive()
		}
	}

	if len(volumeErrs) > 0 {
		// ENG-285: VolumeCleanupAttempts is a docker-private wrapper field (not
		// on ProvisionState, unreachable through the substrate-agnostic UpdateFn
		// seam), so its increment stays a short direct provisionsMu span. The
		// ProvisionState writes that follow (ContainerIDs/Status/LastError) route
		// through the actor's single-writer store seam. Splitting the former
		// atomic read-modify-write into these two critical sections is safe
		// because recoverState's Deprovisioning preserve-case (recover.go,
		// ENG-193) keeps the in-flight entry by pointer across its wholesale map
		// swap — both sections operate on the same *provision, so the increment
		// is never lost to a rebuilt-fresh struct.
		var attempts int
		var entryExists bool
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.VolumeCleanupAttempts++
			attempts = p.VolumeCleanupAttempts
			entryExists = true
		}
		b.provisionsMu.Unlock()
		if !entryExists {
			// Defensive: the entry existed at the initial Deprovisioning mark
			// (else doDeprovision returned early at the !exists guard) and the
			// lease actor owns it through teardown, so it should still be here.
			// If a concurrent path removed it mid-flight, there's nothing left
			// to update.
			return fmt.Errorf("volume cleanup failed: %w", errors.Join(volumeErrs...))
		}

		var diagSnap shared.DiagnosticEntry
		if attempts >= maxVolumeCleanupAttempts {
			// Too many failed attempts — give up and remove the provision.
			// The leaked volumes require manual cleanup by the operator.
			//
			// Persist the abandoned footprint as a reaping tombstone BEFORE releasing
			// live, so the bytes hand off live→reaping with no uncounted gap. (ENG-376)
			b.recordGiveUpLeak(leaseUUID, tenant, providerUUID, items, logger)
			// Release live UNCONDITIONALLY here: the provision is about to be
			// deleted and `return nil`, so no retry can ever run to free it.
			// On the retain path the flag is still false, so without this the
			// live allocation would leak forever and — if a record was written —
			// double-count the footprint as both live and retained (2F), wedging
			// any later restore. On the non-retain path, live was not freed
			// before the destroy loop (it is only freed when all destroys succeed);
			// a destroy failure that reaches give-up means the volume is abandoned
			// for manual cleanup and the provision is deleted — this call performs
			// the real release. pool.Release is idempotent on an absent/already-
			// released id, so this is always safe.
			releaseLive()
			b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
				p.ContainerIDs = nil // containers are gone
				p.LastError = fmt.Sprintf("volume cleanup failed after %d attempts: %s",
					attempts, errors.Join(volumeErrs...))
				diagSnap = leasesm.DiagnosticSnapshot(p)
			})
			b.provisionStore.Delete(leaseUUID)

			// Persist diagnostics before losing the provision so operators
			// can see the final error via the diagnostics API.
			b.persistDiagnostics(diagSnap, nil)

			// Perform the same cleanup as the normal success path.
			b.purgeReleaseHistory(leaseUUID, logger)
			if b.cfg.IsNetworkIsolation() {
				if err := b.releaseTenantNetwork(ctx, tenant); err != nil {
					logger.Warn("failed to remove tenant network", "tenant", tenant, "error", err)
				}
			}
			deprovisionsTotal.Inc()

			logger.Error("MANUAL CLEANUP REQUIRED: volume cleanup failed after max attempts, giving up",
				"attempts", attempts,
				"errors", errors.Join(volumeErrs...),
			)

			// Volume leak: operator must clean up manually. Not a retain-success.
			b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, "volume cleanup exhausted", false)
			return nil
		}

		// Under the limit — keep provision visible for retry.
		b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
			p.ContainerIDs = nil // containers are gone
			p.Status = backend.ProvisionStatusFailed
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
			diagSnap = leasesm.DiagnosticSnapshot(p)
		})
		// Persist diagnostics outside the lock so failure state survives
		// a process restart (no containers remain to recover from).
		b.persistDiagnostics(diagSnap, nil)
		return fmt.Errorf("volume cleanup failed: %w", errors.Join(volumeErrs...))
	}

	// Clean up release history
	b.purgeReleaseHistory(leaseUUID, logger)

	// All containers and volumes removed — delete via the store seam. The map
	// entry is the *provision wrapper, so GC drops the docker-private
	// VolumeCleanupAttempts alongside ProvisionState when the entry goes.
	b.provisionStore.Delete(leaseUUID)

	// Clean up tenant network if isolation is enabled. releaseTenantNetwork
	// scans b.provisions under a per-tenant mutex and skips removal if any
	// other lease still references this tenant, so a concurrent provision on
	// the same tenant cannot have its network yanked between Ensure and
	// ContainerCreate.
	if b.cfg.IsNetworkIsolation() {
		if err := b.releaseTenantNetwork(ctx, tenant); err != nil {
			logger.Warn("failed to remove tenant network", "tenant", tenant, "error", err)
		}
	}

	deprovisionsTotal.Inc()
	logger.Info("deprovisioned", "containers_removed", len(containerIDs))

	// Terminal success: carry the best-effort retained flag (true only when all
	// volumes were soft-deleted into the retained namespace without error).
	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusDeprovisioned, "", volumesRetained)
	return nil
}

// purgeReleaseHistory best-effort deletes a lease's releases.db history. It is a
// no-op when no release store is configured or the key is already absent. Shared by
// doDeprovision's terminal-success, give-up, and already-deprovisioned (!exists) paths
// so "delete this lease's release history" has one implementation. The !exists path is
// what lets a deprovision RPC for an already-containerless lease (e.g. a lease_closed
// event delivered after the container was gone) still purge its stale "active" record
// instead of stranding it until the 90-day RemoveOlderThan TTL. (ENG-410)
func (b *Backend) purgeReleaseHistory(leaseUUID string, logger *slog.Logger) {
	if b.releaseStore == nil {
		return
	}
	if err := b.releaseStore.Delete(leaseUUID); err != nil {
		logger.Warn("failed to delete release history", "error", err)
	}
}

// recordGiveUpLeak handles a deprovision give-up's abandoned on-disk footprint. It
// always increments retentionLeakedTotal (the observable backstop). When a retention
// store is configured it also writes a reaping tombstone for the lease's still-on-disk
// volumes (ground-truthed from disk; canonical names derived from items on List error)
// so the footprint keeps counting in the admission projection and the retention sweep
// auto-retries the destroy — turning a permanent manual-only leak into a self-healing
// one. PutReaping is idempotent and refuses to clobber an active/restoring record, so a
// footprint an existing record already counts is left untouched. (ENG-376)
func (b *Backend) recordGiveUpLeak(leaseUUID, tenant, providerUUID string, items []backend.LeaseItem, logger *slog.Logger) {
	retentionLeakedTotal.Inc()
	if b.retentionStore == nil {
		return // no projection to correct; metric + the give-up log are the record
	}
	var leaked []string
	if all, err := b.volumes.List(); err == nil {
		cprefix := leaseVolumePrefix(leaseUUID) // fred-{lease}-
		rprefix := retainedName(cprefix)        // fred-retained-{lease}-
		for _, id := range all {
			if strings.HasPrefix(id, cprefix) || strings.HasPrefix(id, rprefix) {
				leaked = append(leaked, id)
			}
		}
	} else {
		logger.Warn("give-up leak: volume list failed; deriving canonical + retained names from items", "error", err)
		for _, item := range items {
			for i := range item.Quantity {
				canonical := canonicalVolumeName(leaseUUID, item.ServiceName, i)
				// Record BOTH namespaces. A retain-path give-up may have already renamed
				// some volumes into fred-retained-* before failing, so the on-disk name is
				// unknown when List() is unavailable. destroyReapingVolumes treats a missing
				// volume as an idempotent no-op, so recording both guarantees whichever
				// exists is destroyed before the tombstone is deleted. Without the retained
				// name, the sweep would "succeed" against the non-existent canonical name and
				// drop the tombstone while the fred-retained-* volume persists untracked —
				// reintroducing the exact under-count/leak this path fixes.
				leaked = append(leaked, canonical, retainedName(canonical))
			}
		}
	}
	if len(leaked) == 0 {
		return // nothing on disk to account for
	}
	rec := shared.RetentionEntry{
		OriginalLeaseUUID:   leaseUUID,
		Tenant:              tenant,
		ProviderUUID:        providerUUID,
		Items:               items,
		RetainedVolumeNames: leaked,
		Status:              shared.RetentionStatusReaping,
		CreatedAt:           time.Now(),
	}
	if ok, err := b.retentionStore.PutReaping(rec); err != nil {
		logger.Error("give-up leak: failed to record reaping tombstone; footprint UNTRACKED until manual cleanup", "lease_uuid", leaseUUID, "error", err)
	} else if !ok {
		logger.Info("give-up leak: an active/restoring record already counts this footprint; no tombstone written", "lease_uuid", leaseUUID)
	}
	// Reflect the new tombstone immediately (the deferred refresh only runs on the
	// retain path; a non-retain give-up would otherwise wait for the next sweep).
	b.refreshRetentionAccounting()
}
