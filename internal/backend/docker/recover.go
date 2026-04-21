package docker

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// recoverState rebuilds in-memory state from Docker containers.
// Handles multi-unit leases by grouping containers by lease UUID.
// Merges with existing state to preserve in-flight provisions.
//
// Serialized by recoverMu to prevent concurrent calls (from the
// reconcile loop and RefreshState) from duplicating transition
// detection and failure callbacks.
func (b *Backend) recoverState(ctx context.Context) error {
	b.recoverMu.Lock()
	defer b.recoverMu.Unlock()

	containers, err := b.docker.ListManagedContainers(ctx)
	if err != nil {
		return err
	}

	allocsByLease := make(map[string][]shared.ResourceAllocation)
	recovered := make(map[string]*provision)
	skippedUnknownSKU := 0

	// Group containers by lease UUID
	for _, c := range containers {
		// Skip containers without required labels
		if c.LeaseUUID == "" || c.SKU == "" {
			b.logger.Warn("skipping container with missing labels", "container_id", shortID(c.ContainerID))
			continue
		}

		// Look up SKU profile for resource allocation
		profile, err := b.cfg.GetSKUProfile(c.SKU)
		if err != nil {
			b.logger.Error("skipping container with unknown SKU — container is running but untracked",
				"container_id", shortID(c.ContainerID),
				"sku", c.SKU,
			)
			skippedUnknownSKU++
			continue
		}

		// Check if we already have a provision record for this lease
		prov, exists := recovered[c.LeaseUUID]
		if !exists {
			prov = &provision{
				LeaseUUID:    c.LeaseUUID,
				Tenant:       c.Tenant,
				ProviderUUID: c.ProviderUUID,
				SKU:          c.SKU,
				Image:        c.Image,
				Status:       containerStatusToProvisionStatus(c.Status),
				CreatedAt:    c.CreatedAt,
				FailCount:    c.FailCount,
				CallbackURL:  c.CallbackURL,
				ContainerIDs: make([]string, 0),
			}

			// Restore manifest from the last successful (active) release so
			// restart/update work after a cold start (manifest is not stored
			// in labels). Using LatestActive avoids picking up a failed
			// release (e.g., a failed update to a newer image).
			if b.releaseStore != nil {
				if rel, relErr := b.releaseStore.LatestActive(c.LeaseUUID); relErr == nil && rel != nil && len(rel.Manifest) > 0 {
					// Use ParsePayload to auto-detect single vs stack manifest.
					legacyM, stackM, payloadErr := ParsePayload(rel.Manifest)
					switch {
					case payloadErr != nil:
						b.logger.Warn("failed to parse recovered manifest",
							"lease_uuid", c.LeaseUUID, "error", payloadErr)
					case stackM != nil:
						prov.StackManifest = stackM
					case legacyM != nil:
						prov.Manifest = legacyM
					}
				}
			}

			recovered[c.LeaseUUID] = prov
		}

		// Add container ID to the provision
		prov.ContainerIDs = append(prov.ContainerIDs, c.ContainerID)
		prov.Quantity = len(prov.ContainerIDs)

		// Build ServiceContainers map and Items for stack containers.
		if c.ServiceName != "" {
			if prov.ServiceContainers == nil {
				prov.ServiceContainers = make(map[string][]string)
			}
			prov.ServiceContainers[c.ServiceName] = append(prov.ServiceContainers[c.ServiceName], c.ContainerID)

			// Rebuild Items from container labels (SKU + ServiceName per container).
			// Use a dedup map keyed by service name since multiple containers
			// belong to the same item.
			found := false
			for idx := range prov.Items {
				if prov.Items[idx].ServiceName == c.ServiceName {
					prov.Items[idx].Quantity = len(prov.ServiceContainers[c.ServiceName])
					found = true
					break
				}
			}
			if !found {
				prov.Items = append(prov.Items, backend.LeaseItem{
					SKU:         c.SKU,
					Quantity:    1,
					ServiceName: c.ServiceName,
				})
			}
		}

		// Use the highest FailCount across containers. Labels are normally
		// identical, but can diverge after a partial re-provision.
		if c.FailCount > prov.FailCount {
			prov.FailCount = c.FailCount
		}

		// If any container is not ready, mark the whole provision as not ready
		status := containerStatusToProvisionStatus(c.Status)
		if status != backend.ProvisionStatusReady && prov.Status == backend.ProvisionStatusReady {
			prov.Status = status
		}

		// Use instance-specific allocation ID, grouped by lease for filtering.
		// Stack uses service-aware IDs: {leaseUUID}-{serviceName}-{instanceIndex}
		var instanceID string
		if c.ServiceName != "" {
			instanceID = fmt.Sprintf("%s-%s-%d", c.LeaseUUID, c.ServiceName, c.InstanceIndex)
		} else {
			instanceID = fmt.Sprintf("%s-%d", c.LeaseUUID, c.InstanceIndex)
		}
		allocsByLease[c.LeaseUUID] = append(allocsByLease[c.LeaseUUID], shared.ResourceAllocation{
			LeaseUUID: instanceID,
			Tenant:    c.Tenant,
			SKU:       c.SKU,
			CPUCores:  profile.CPUCores,
			MemoryMB:  profile.MemoryMB,
			DiskMB:    profile.DiskMB,
		})
	}

	// Merge with existing state and detect status transitions.
	b.provisionsMu.Lock()

	// Detect ready→failed transitions: containers that were running but have since crashed.
	// We must notify Fred so the lease doesn't remain active for a dead container.
	var failedLeases []string
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusReady {
			if rec, ok := recovered[uuid]; ok && rec.Status == backend.ProvisionStatusFailed {
				// Carry over FailCount and increment for this failure.
				rec.FailCount = existing.FailCount + 1
				rec.LastError = errMsgContainerExited
				failedLeases = append(failedLeases, uuid)
				b.logger.Warn("container crashed after provisioning",
					"lease_uuid", uuid,
					"tenant", existing.Tenant,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// Cold-start correction: provisions recovered as failed with no prior
	// in-memory state have a FailCount from the container label that was
	// written at creation time (before this failure occurred). Increment
	// it to account for the failure evidenced by the container being dead.
	var coldStartFailed []string
	for uuid, rec := range recovered {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				rec.FailCount++
				rec.LastError = errMsgContainerExited
				coldStartFailed = append(coldStartFailed, uuid)
				b.logger.Info("cold-start: adjusted FailCount for already-failed provision",
					"lease_uuid", uuid,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// Preserve provisions without containers that need to remain visible
	// to fred's reconciler.
	for uuid, existing := range b.provisions {
		if rec, hasContainers := recovered[uuid]; hasContainers {
			if existing.Status == backend.ProvisionStatusProvisioning || existing.Status == backend.ProvisionStatusRestarting || existing.Status == backend.ProvisionStatusUpdating {
				// In-flight re-provision: the containers in recovered belong to the
				// previous (failed) provision and carry stale FailCount labels. The
				// old containers are being removed by Provision() concurrently.
				// Preserve the in-flight entry with its correct FailCount so the
				// next container creation picks up the right value.
				recovered[uuid] = existing
			} else if existing.FailCount > rec.FailCount {
				// Container labels carry the FailCount from creation time. When
				// recoverState increments FailCount in-memory (e.g., ready→failed
				// transition), subsequent recoverState calls re-read the stale label
				// value. Preserve the higher in-memory count to prevent regression.
				rec.FailCount = existing.FailCount
			}
			continue // Already recovered from containers
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// In-flight operation that hasn't produced containers yet — preserve it.
			recovered[uuid] = existing
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers have been cleaned up (e.g., after
			// a failed re-provision attempt). Preserve so fred's reconciler can
			// see the failure and its FailCount for retry/close decisions.
			recovered[uuid] = existing
		}
	}
	b.provisions = recovered

	// Build the final allocations list, excluding leases with in-flight
	// operations (provisioning/restarting/updating). Their old containers
	// are being cleaned up concurrently; including stale allocations would
	// race with TryAllocate in Provision/Restart/Update.
	var allocations []shared.ResourceAllocation
	for uuid, allocs := range allocsByLease {
		if prov, ok := recovered[uuid]; ok {
			switch prov.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				continue
			}
		}
		allocations = append(allocations, allocs...)
	}
	b.pool.Reset(allocations)

	// Snapshot aggregate stats from the recovered map before releasing the lock.
	// After unlock, `recovered` aliases `b.provisions` and concurrent goroutines
	// may modify both the map and the pointed-to provision structs.
	var readyCount float64
	totalContainers := 0
	leaseCount := len(recovered)
	activeTenants := make(map[string]bool, len(recovered))
	for _, p := range recovered {
		if p.Status == backend.ProvisionStatusReady {
			readyCount++
		}
		totalContainers += len(p.ContainerIDs)
		if p.Tenant != "" {
			activeTenants[p.Tenant] = true
		}
	}
	b.provisionsMu.Unlock()

	// Reset the active provisions gauge from the recovered map. Without this,
	// the gauge drifts (and can go negative) because Inc/Dec are only called
	// during normal Provision/Deprovision, but recoverState replaces the map.
	activeProvisions.Set(readyCount)
	updateResourceMetrics(b.pool.Stats())

	// Gather diagnostics for failed leases (I/O outside the lock).
	allFailed := slices.Concat(failedLeases, coldStartFailed)
	failedDiagnostics := make(map[string]string, len(allFailed))
	for _, uuid := range allFailed {
		b.provisionsMu.RLock()
		prov, ok := b.provisions[uuid]
		if !ok {
			b.provisionsMu.RUnlock()
			continue
		}
		containerIDs := append([]string(nil), prov.ContainerIDs...)
		b.provisionsMu.RUnlock()

		for _, cid := range containerIDs {
			info, inspErr := b.docker.InspectContainer(ctx, cid)
			if inspErr != nil {
				b.logger.Warn("failed to inspect container during diagnostics gathering", "lease", uuid, "container_id", shortID(cid), "error", inspErr)
				continue
			}
			if containerStatusToProvisionStatus(info.Status) == backend.ProvisionStatusFailed {
				failedDiagnostics[uuid] = b.containerFailureDiagnostics(ctx, cid, info)
				break
			}
		}
	}

	// Update LastError with enriched diagnostics. Writes are gated on
	// Status==Failed so a concurrent Deprovision/Provision-re-attempt/Restart
	// that took ownership during the diag window doesn't get its fresh
	// LastError clobbered with this failure's data. Same principle as the
	// suppress-callback-on-status-change loop below.
	if len(failedDiagnostics) > 0 {
		b.provisionsMu.Lock()
		for uuid, diag := range failedDiagnostics {
			if prov, ok := b.provisions[uuid]; ok && prov.Status == backend.ProvisionStatusFailed {
				prov.LastError = errMsgContainerExited + ": " + diag
			}
		}
		b.provisionsMu.Unlock()
	}

	// Snapshot diagnostics under lock, then persist outside (I/O). Same
	// Status==Failed gate so we don't persist a snapshot that aliases the new
	// owner's ContainerIDs (which would cause persistDiagnostics to fetch logs
	// from containers unrelated to the original failure).
	type diagItem struct {
		entry        shared.DiagnosticEntry
		containerIDs []string
		keys         map[string]string
	}
	diagItems := make([]diagItem, 0, len(allFailed))
	b.provisionsMu.RLock()
	for _, uuid := range allFailed {
		if prov, ok := b.provisions[uuid]; ok && prov.Status == backend.ProvisionStatusFailed {
			diagItems = append(diagItems, diagItem{
				entry:        diagnosticSnapshot(prov),
				containerIDs: append([]string(nil), prov.ContainerIDs...),
				keys:         containerLogKeys(prov),
			})
		}
	}
	b.provisionsMu.RUnlock()
	for _, item := range diagItems {
		b.persistDiagnostics(item.entry, item.containerIDs, item.keys)
	}

	// Send failure callbacks with hardcoded message — never includes logs
	// or dynamic data. Full diagnostics are in prov.LastError for
	// authenticated API access.
	//
	// Re-check status under RLock before each send: the preceding diag gather
	// and persist loop holds no lock, so a concurrent Deprovision (or Provision
	// re-attempt, Restart) may have flipped Failed into another state.
	// Whichever operation took over will emit its own terminal callback.
	//
	// Snapshot the callback URL under the same RLock as the status check, then
	// send via sendCallbackWithURL so the send cannot race with a concurrent
	// delete (which would yield a noisy "no callback URL" warning) or reinsert
	// (which could route this lease's Failed send to a different URL).
	for _, uuid := range failedLeases {
		b.provisionsMu.RLock()
		var curStatus backend.ProvisionStatus
		var callbackURL string
		var failCount int
		present := false
		if p, ok := b.provisions[uuid]; ok {
			present = true
			curStatus = p.Status
			callbackURL = p.CallbackURL
			failCount = p.FailCount
		}
		b.provisionsMu.RUnlock()
		if !present || curStatus != backend.ProvisionStatusFailed {
			b.logger.Info("suppressing Failed callback: another operation took over",
				"lease_uuid", uuid,
				"observed_status", curStatus,
				"present", present,
				"fail_count", failCount,
			)
			continue
		}
		b.sendCallbackWithURL(uuid, callbackURL, backend.CallbackStatusFailed, errMsgContainerExited)
	}

	stats := b.pool.Stats()
	logAttrs := []any{
		"leases", leaseCount,
		"containers", totalContainers,
		"cpu_allocated", stats.AllocatedCPU,
		"memory_allocated_mb", stats.AllocatedMemoryMB,
	}
	if skippedUnknownSKU > 0 {
		logAttrs = append(logAttrs, "untracked_unknown_sku", skippedUnknownSKU)
	}
	b.logger.Info("state recovered", logAttrs...)

	// Clean up orphaned tenant networks if network isolation is enabled
	if b.cfg.IsNetworkIsolation() {
		b.cleanupOrphanedNetworks(ctx, activeTenants)
	}

	return nil
}

// cleanupOrphanedVolumes destroys volumes on disk that have no matching provision.
// This catches volumes leaked by crashes between volume creation and container creation,
// or between container removal and volume destruction. Called once at startup after
// recoverState populates the provision map.
func (b *Backend) cleanupOrphanedVolumes(ctx context.Context) error {
	volumeIDs, err := b.volumes.List()
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	if len(volumeIDs) == 0 {
		return nil
	}

	// Build set of expected volume IDs from recovered provisions.
	expected := make(map[string]bool)
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		if prov.IsStack() {
			for _, item := range prov.Items {
				for i := range item.Quantity {
					expected[fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)] = true
				}
			}
		} else {
			for i := range prov.Quantity {
				expected[fmt.Sprintf("fred-%s-%d", leaseUUID, i)] = true
			}
		}
	}
	b.provisionsMu.RUnlock()

	var orphanCount, failCount int
	for _, id := range volumeIDs {
		if expected[id] {
			continue
		}
		b.logger.Info("destroying orphaned volume", "volume_id", id)
		if err := b.volumes.Destroy(ctx, id); err != nil {
			b.logger.Error("failed to destroy orphaned volume", "volume_id", id, "error", err)
			failCount++
		} else {
			orphanCount++
		}
	}
	if orphanCount > 0 || failCount > 0 {
		b.logger.Info("orphaned volume cleanup complete", "destroyed", orphanCount, "failed", failCount)
	}
	return nil
}

// cleanupOrphanedNetworks removes managed networks whose tenant has no active provisions.
// activeTenants is a cheap precheck to skip tenants obviously still in use;
// releaseTenantNetwork re-validates against a live b.provisions scan under
// the per-tenant mutex, so this path is safe against a Provision() arriving
// concurrently with reconcile.
func (b *Backend) cleanupOrphanedNetworks(ctx context.Context, activeTenants map[string]bool) {
	networks, err := b.docker.ListManagedNetworks(ctx)
	if err != nil {
		b.logger.Warn("failed to list managed networks for cleanup", "error", err)
		return
	}

	for _, n := range networks {
		tenant := n.Labels[LabelTenant]
		if tenant != "" && !activeTenants[tenant] && len(n.Containers) == 0 {
			if err := b.releaseTenantNetwork(ctx, tenant); err != nil {
				b.logger.Warn("failed to remove orphaned network", "network", n.Name, "error", err)
			} else {
				b.logger.Info("removed orphaned tenant network", "network", n.Name, "tenant", tenant)
			}
		}
	}
}

// reconcileLoop periodically reconciles the in-memory state with Docker.
// Note: WaitGroup.Done is handled by the caller via wg.Go() (Go 1.25+).
func (b *Backend) reconcileLoop() {
	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := b.recoverState(ctx); err != nil {
					b.logger.Error("reconciliation failed", "error", err)
					reconciliationTotal.WithLabelValues("error").Inc()
				} else {
					reconciliationTotal.WithLabelValues("success").Inc()
					reconcilerLastSuccessTimestamp.SetToCurrentTime()
				}
			}()
		}
	}
}

// containerEventLoop subscribes to Docker container "die" events and triggers
// immediate failure handling. This provides near-instant detection of container
// crashes, complementing the 5-minute reconcileLoop safety net.
func (b *Backend) containerEventLoop() {
	for {
		select {
		case <-b.stopCtx.Done():
			return
		default:
		}

		eventCh, errCh := b.docker.ContainerEvents(b.stopCtx)

	consume:
		for {
			select {
			case <-b.stopCtx.Done():
				return
			case event, ok := <-eventCh:
				if !ok {
					break consume
				}
				if event.Action == "die" {
					b.handleContainerDeath(event.ContainerID)
				}
			case err, ok := <-errCh:
				if !ok {
					break consume
				}
				b.logger.Warn("container event stream error, reconnecting", "error", err)
				break consume
			}
		}

		// Backoff before reconnecting to avoid tight loop on persistent errors.
		select {
		case <-b.stopCtx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// handleContainerDeath processes a single container death event. If the
// container belongs to a lease in Ready status, it transitions the lease to
// Failed and sends a callback — the same transition that recoverState
// performs, but for a single container in real time.
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}

	// Only transition ready→failed. Other states (provisioning, restarting,
	// updating, deprovisioning, already failed) are managed by other code paths.
	// Deprovisioning in particular: Deprovision() sets this status before
	// calling RemoveContainer, so die events emitted during that removal land
	// here and must be skipped.
	// Read status under lock to avoid data race with concurrent writers.
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if !exists || prov.Status != backend.ProvisionStatusReady {
		b.provisionsMu.RUnlock()
		return
	}
	b.provisionsMu.RUnlock()

	// Defensive: verify the container is actually dead via inspect.
	// Docker events can be duplicated or arrive out of order.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := b.docker.InspectContainer(ctx, containerID)
	if err != nil {
		b.logger.Warn("failed to inspect container after die event",
			"container_id", shortID(containerID),
			"lease_uuid", leaseUUID,
			"error", err,
		)
		return
	}
	if containerStatusToProvisionStatus(info.Status) != backend.ProvisionStatusFailed {
		return // Container restarted or state changed before we got here
	}

	// Transition to failed under write lock.
	b.provisionsMu.Lock()
	// Re-check status under write lock to avoid racing with recoverState or
	// another die event for a multi-container lease.
	currentProv, exists := b.provisions[leaseUUID]
	if !exists || currentProv.Status != backend.ProvisionStatusReady {
		b.provisionsMu.Unlock()
		return
	}
	currentProv.Status = backend.ProvisionStatusFailed
	currentProv.FailCount++
	failCount := currentProv.FailCount
	currentProv.LastError = errMsgContainerExited
	// Decrement the gauge under the same lock as the status flip so the
	// Ready→Failed transition is counted exactly once even if a concurrent
	// Deprovision preempts the callback below.
	activeProvisions.Dec()
	b.provisionsMu.Unlock()

	// Gather diagnostics (I/O outside the lock, same pattern as recoverState).
	// All writes below are gated on Status==Failed: if a concurrent Deprovision
	// or Provision re-attempt has taken ownership, its fresh LastError / new
	// ContainerIDs must not be overwritten with the old failure's data, and
	// the diagnostic snapshot must not persist logs attributed to the new
	// owner. Same principle as the suppress-callback-on-status-change below.
	diag := b.containerFailureDiagnostics(ctx, containerID, info)
	if diag != "" {
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok && p.Status == backend.ProvisionStatusFailed {
			p.LastError = errMsgContainerExited + ": " + diag
		}
		b.provisionsMu.Unlock()
	}

	// Persist diagnostics (snapshot under lock, I/O outside).
	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	b.provisionsMu.RLock()
	if p, ok := b.provisions[leaseUUID]; ok && p.Status == backend.ProvisionStatusFailed {
		diagSnap = diagnosticSnapshot(p)
		diagContainerIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = containerLogKeys(p)
	}
	b.provisionsMu.RUnlock()
	if diagSnap.LeaseUUID != "" {
		b.persistDiagnostics(diagSnap, diagContainerIDs, diagKeys)
	}

	// Re-check status before firing the terminal Failed callback. The diag+persist
	// above runs without the lock, leaving a window for a concurrent Deprovision
	// (or Provision re-attempt, Restart, etc.) to flip Failed into another state.
	// When that happens, whichever operation took over will emit its own terminal
	// callback; ours would be a spurious duplicate of an event Fred already knows
	// about. Suppress but log, so operators can see the frequency.
	//
	// Snapshot the callback URL under the same RLock as the status check so the
	// subsequent send cannot race with a concurrent delete (noisy "no callback
	// URL" warn) or reinsert (routing this lease's Failed send to a different URL).
	b.provisionsMu.RLock()
	var curStatus backend.ProvisionStatus
	var callbackURL string
	present := false
	if p, ok := b.provisions[leaseUUID]; ok {
		present = true
		curStatus = p.Status
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.RUnlock()
	if !present || curStatus != backend.ProvisionStatusFailed {
		b.logger.Info("suppressing Failed callback: another operation took over",
			"lease_uuid", leaseUUID,
			"container_id", shortID(containerID),
			"observed_status", curStatus,
			"present", present,
			"fail_count", failCount,
		)
		return
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, errMsgContainerExited)

	logAttrs := []any{
		"lease_uuid", leaseUUID,
		"container_id", shortID(containerID),
		"fail_count", failCount,
	}
	if info.ServiceName != "" {
		logAttrs = append(logAttrs, "service_name", info.ServiceName)
	}
	b.logger.Warn("container death detected via events API", logAttrs...)
}

// findLeaseByContainerID returns the lease UUID and true if a provision
// containing the given container ID is found. Returns ("", false) otherwise.
// Called under no lock; acquires read lock internally.
//
// O(N*M) linear scan over all leases and their containers. A reverse index
// would be O(1) but adds sync overhead across provision/deprovision/restart/
// update/recover. Fine at expected scale (hundreds of leases, 1-10 containers).
func (b *Backend) findLeaseByContainerID(containerID string) (string, bool) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	for uuid, prov := range b.provisions {
		for _, cid := range prov.ContainerIDs {
			if cid == containerID {
				return uuid, true
			}
		}
	}
	return "", false
}
