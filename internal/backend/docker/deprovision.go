package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Deprovision is the public shim: it routes the request through the lease's
// actor so that container-death and deprovision messages serialize per lease.
// Routing forces a Ready/Failing/Failed → Deprovisioning SM transition whose
// Failing.OnExit cancels the in-flight diag goroutine — the structural
// suppression of stale Failed callbacks.
func (b *Backend) Deprovision(ctx context.Context, leaseUUID string) error {
	reply := make(chan error, 1)
	if err := b.routeToLeaseBlocking(ctx, leaseUUID, deprovisionMsg{ctx: ctx, reply: reply}); err != nil {
		return err
	}
	return b.waitForReply(ctx, reply)
}

// handleDeprovision runs inside the lease actor's message handler. It fires
// the SM transition then runs the work synchronously, returning the outcome
// on the deprovisionMsg's reply channel.
func (a *leaseActor) handleDeprovision(ctx context.Context) error {
	// Attempt the SM transition. If it's not permitted, check whether the
	// provision is already gone (idempotent success) or we're in an unexpected
	// state (surface the error).
	if err := a.sm.Fire(ctx, evDeprovisionRequested); err != nil {
		a.backend.provisionsMu.RLock()
		_, exists := a.backend.provisions[a.leaseUUID]
		a.backend.provisionsMu.RUnlock()
		if !exists {
			a.terminated = true
			return nil
		}
		a.backend.logger.Warn("deprovision transition denied by SM",
			"lease_uuid", a.leaseUUID, "error", err)
		// Fall through to the work anyway — the SM may not know every state
		// (partial port). The work itself is idempotent.
	}
	err := a.backend.doDeprovision(ctx, a.leaseUUID)
	// If the provision entry was fully removed (success path), signal the
	// run loop to exit so a subsequent re-provision with the same UUID
	// creates a fresh actor instead of being Ignored by a stale SM.
	a.backend.provisionsMu.RLock()
	_, exists := a.backend.provisions[a.leaseUUID]
	a.backend.provisionsMu.RUnlock()
	if !exists {
		a.terminated = true
	}
	return err
}

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

	b.provisionsMu.Lock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		// Already deprovisioned - idempotent success
		return nil
	}
	// Mark Deprovisioning before removing containers. The in-memory marker
	// lets Provision's status guard reject concurrent re-provision attempts
	// during the removal window (die events from RemoveContainer are dropped
	// structurally by the SM's Deprovisioning.Ignore(evContainerDied)).
	//
	// Decrement activeProvisions on the Ready→Deprovisioning transition so
	// the gauge stays accurate even if Deprovision fails partially and the
	// provision ends up Failed on retry.
	wasReady := prov.Status == backend.ProvisionStatusReady
	prov.Status = backend.ProvisionStatusDeprovisioning
	if wasReady {
		activeProvisions.Dec()
	}
	isStack := prov.IsStack()
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	items := append([]backend.LeaseItem(nil), prov.Items...)
	quantity := prov.Quantity
	tenant := prov.Tenant
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()

	// Remove all containers.
	// For stacks, use Compose Down for atomic cleanup; fall back to individual
	// removal if Compose fails. For single-container leases, use RemoveContainer.
	var errs []error
	var failedIDs []string
	if isStack {
		stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
		if downErr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); downErr != nil {
			logger.Warn("compose down failed, falling back to individual removal", "error", downErr)
			for _, containerID := range containerIDs {
				if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
					logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
					errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
					failedIDs = append(failedIDs, containerID)
				} else {
					logger.Info("container removed", "container_id", shortID(containerID))
				}
			}
		} else {
			logger.Info("compose down completed", "project", composeProjectName(leaseUUID))
		}
	} else {
		for _, containerID := range containerIDs {
			if err := b.docker.RemoveContainer(ctx, containerID); err != nil {
				logger.Error("failed to remove container", "container_id", shortID(containerID), "error", err)
				errs = append(errs, fmt.Errorf("container %s: %w", shortID(containerID), err))
				failedIDs = append(failedIDs, containerID)
			} else {
				logger.Info("container removed", "container_id", shortID(containerID))
			}
		}
	}

	// Release resource pool allocations regardless of outcome — the lease
	// is being abandoned and these resources should be freed.
	if isStack {
		for _, item := range items {
			for i := range item.Quantity {
				b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
			}
		}
	} else {
		for i := range quantity {
			b.pool.Release(fmt.Sprintf("%s-%d", leaseUUID, i))
		}
	}
	// Update gauges immediately after releasing allocations so metrics stay
	// accurate on every path (partial failure, volume-cleanup retry, success).
	updateResourceMetrics(b.pool.Stats())

	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers
		// so the reconciler (or a retry) can see and re-attempt them.
		var diagSnap shared.DiagnosticEntry
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			diagSnap = diagnosticSnapshot(p)
		}
		b.provisionsMu.Unlock()
		b.persistDiagnostics(diagSnap, failedIDs)
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// Destroy managed volumes for all instances.
	var volumeErrs []error
	if isStack {
		for _, item := range items {
			for i := range item.Quantity {
				volumeID := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)
				if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
					logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
					volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
				}
			}
		}
	} else {
		for i := range quantity {
			volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
			if volErr := b.volumes.Destroy(ctx, volumeID); volErr != nil {
				logger.Error("failed to destroy volume", "volume_id", volumeID, "error", volErr)
				volumeErrs = append(volumeErrs, fmt.Errorf("volume %s: %w", volumeID, volErr))
			}
		}
	}

	if len(volumeErrs) > 0 {
		var diagSnap shared.DiagnosticEntry
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok {
			p.VolumeCleanupAttempts++
			p.ContainerIDs = nil // containers are gone

			if p.VolumeCleanupAttempts >= maxVolumeCleanupAttempts {
				// Too many failed attempts — give up and remove the provision.
				// The leaked volumes require manual cleanup by the operator.
				p.LastError = fmt.Sprintf("volume cleanup failed after %d attempts: %s",
					p.VolumeCleanupAttempts, errors.Join(volumeErrs...))
				diagSnap = diagnosticSnapshot(p)
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
					"attempts", p.VolumeCleanupAttempts,
					"errors", errors.Join(volumeErrs...),
				)

				// Volume leak: operator must clean up manually.
				b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, "volume cleanup exhausted")
				return nil
			}

			// Under the limit — keep provision visible for retry.
			p.Status = backend.ProvisionStatusFailed
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
			diagSnap = diagnosticSnapshot(p)
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

	// All containers and volumes removed — delete provision from map.
	b.provisionsMu.Lock()
	delete(b.provisions, leaseUUID)
	b.provisionsMu.Unlock()

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
