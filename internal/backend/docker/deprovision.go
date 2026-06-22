package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
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
// the failed removals. Resource pool allocations are still released (the
// lease is being abandoned). On retry, only the stuck containers are
// attempted.
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
		// Already deprovisioned — idempotent success.
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
	// resource metrics. On the non-retain path it is called immediately (below);
	// on the retain path it is deferred until after refreshRetentionAccounting
	// counts the retained record, so the footprint is never momentarily
	// uncounted while the renamed volume persists on disk (no over-admit gap).
	releaseLive := func() {
		for _, item := range items {
			for i := range item.Quantity {
				b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
			}
		}
		updateResourceMetrics(b.pool.Stats())
	}
	retaining := b.cfg.RetainOnClose && b.retentionStore != nil
	if !retaining {
		// Non-retain close: volumes will be destroyed below; release live now
		// (UNCHANGED from prior behaviour for this path).
		releaseLive()
	}
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
	defer func() {
		b.refreshRetentionAccounting()
		if retaining && releaseLiveOnRetainPath {
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
		if len(canonical) == 0 {
			// No canonical volumes remain under this lease (all were already renamed
			// into fred-retained-* on a prior attempt, or the lease had no stateful
			// volumes). No canonical bytes are on disk under the live name, so it is
			// safe to release the live allocation.
			releaseLiveOnRetainPath = true
		} else if b.shouldRefuseRetention(leaseUUID, items) {
			prevErrCount := len(volumeErrs)
			volumeErrs = append(volumeErrs, b.destroyOnRefuseToRetain(ctx, canonical, leaseUUID, tenant, logger)...)
			if len(volumeErrs) == prevErrCount {
				// All canonical volumes destroyed without error — bytes are gone,
				// so it is safe to release the live allocation.
				releaseLiveOnRetainPath = true
			}
		} else {
			// existing retain logic — moved here UNCHANGED, just re-indented one level
			retained := make([]string, 0, len(canonical))
			for _, c := range canonical {
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
				for _, c := range canonical {
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
			if b.releaseStore != nil {
				if err := b.releaseStore.Delete(leaseUUID); err != nil {
					logger.Warn("failed to delete release history", "error", err)
				}
			}
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
	if b.releaseStore != nil {
		if err := b.releaseStore.Delete(leaseUUID); err != nil {
			logger.Warn("failed to delete release history", "error", err)
		}
	}

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
