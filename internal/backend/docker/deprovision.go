package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
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
		wasReady     bool
		containerIDs []string
		items        []backend.LeaseItem
		tenant       string
		callbackURL  string
	)
	exists := b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
		wasReady = p.Status == backend.ProvisionStatusReady
		p.Status = backend.ProvisionStatusDeprovisioning
		containerIDs = append([]string(nil), p.ContainerIDs...)
		items = append([]backend.LeaseItem(nil), p.Items...)
		tenant = p.Tenant
		callbackURL = p.CallbackURL
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

	// Release resource pool allocations regardless of outcome — the lease
	// is being abandoned and these resources should be freed. Allocation IDs
	// are always service-aware now ({lease}-{service}-{idx}); the legacy
	// {lease}-{idx} scheme is gone from the live path. Task 9's recover-time
	// migration releases / re-allocates legacy allocs as part of converting
	// on-disk artifacts.
	for _, item := range items {
		for i := range item.Quantity {
			b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
		}
	}
	// Update gauges immediately after releasing allocations so metrics stay
	// accurate on every path (partial failure, volume-cleanup retry, success).
	updateResourceMetrics(b.pool.Stats())

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

	// Destroy managed volumes for all instances. Volume IDs are always
	// service-aware now (fred-{lease}-{service}-{idx}); the legacy
	// fred-{lease}-{idx} scheme is gone from the live path. Task 9's
	// recover-time migration renames pre-existing legacy volumes onto
	// the new naming convention before this code sees them.
	var volumeErrs []error
	for _, item := range items {
		for i := range item.Quantity {
			volumeID := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)
			if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
				logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
				volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
			}
		}
	}

	if len(volumeErrs) > 0 {
		// ENG-232: this block stays a single direct provisionsMu span (NOT routed
		// through the store seam) because VolumeCleanupAttempts is a docker-private
		// wrapper field (not on ProvisionState, unreachable through UpdateFn) and is
		// incremented atomically with ContainerIDs/Status — seaming the
		// ProvisionState writes would split one atomic read-modify-write into two.
		// The give-up-branch delete stays inline for the same reason (calling
		// store.Delete here would re-enter provisionsMu). Tracked: ENG-285.
		var diagSnap shared.DiagnosticEntry
		// attempts is captured INSIDE the b.provisionsMu Lock span (after
		// the increment) so we can use the correct value at logger.Error
		// below — which runs AFTER the Unlock and AFTER the provision
		// entry has been deleted in the give-up branch. The other reads
		// in this block (the if-guard and the LastError fmt.Sprintf) also
		// use the captured value to make the lock-held-state dependency
		// explicit and prevent future regressions where someone moves a
		// read outside the Lock.
		var attempts int
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.VolumeCleanupAttempts++
			attempts = p.VolumeCleanupAttempts
			p.ContainerIDs = nil // containers are gone

			if attempts >= maxVolumeCleanupAttempts {
				// Too many failed attempts — give up and remove the provision.
				// The leaked volumes require manual cleanup by the operator.
				p.LastError = fmt.Sprintf("volume cleanup failed after %d attempts: %s",
					attempts, errors.Join(volumeErrs...))
				diagSnap = leasesm.DiagnosticSnapshot(&p.ProvisionState)
				delete(b.provisions, leaseUUID)
				b.provisionsMu.Unlock()

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

				// Volume leak: operator must clean up manually.
				b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, "volume cleanup exhausted")
				return nil
			}

			// Under the limit — keep provision visible for retry.
			p.Status = backend.ProvisionStatusFailed
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
			diagSnap = leasesm.DiagnosticSnapshot(&p.ProvisionState)
		}
		b.provisionsMu.Unlock()
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

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusDeprovisioned, "")
	return nil
}
