package docker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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

	// Recover-time migration pre-pass. Groups any legacy single-service
	// containers by lease and produces a per-lease migration plan. Runs
	// BEFORE the main recovery loop so all in-memory provision state
	// observed by Update / Restart paths is post-migration consistent
	// (per QA's Task 6 carry-over note: prov.Items mutation must not
	// race ahead of prov.ServiceContainers population).
	//
	// Each plan is then executed atomically per lease by
	// executeLegacyMigration in the loop below. Any per-lease
	// failure aborts startup with operator-actionable guidance —
	// fred refuses to run with half-migrated state because the
	// stack-only downstream code can't drive a mixed cohort.
	legacyPlans, planErr := b.planLegacyMigrations(ctx, containers)
	if planErr != nil {
		return fmt.Errorf("plan legacy migrations: %w", planErr)
	}
	for _, plan := range legacyPlans {
		b.logger.Info("legacy lease migration planned",
			"lease_uuid", plan.LeaseUUID,
			"tenant", plan.Tenant,
			"sku", plan.SKU,
			"instances", len(plan.Instances),
		)
	}
	if len(legacyPlans) > 0 {
		// Execute each plan atomically per lease. Any per-lease failure
		// aborts startup with operator-actionable guidance — fred refuses
		// to run with half-migrated state because the stack-only code
		// downstream can't drive a mixed cohort.
		for _, plan := range legacyPlans {
			if err := b.executeLegacyMigration(ctx, plan, b.logger); err != nil {
				b.logger.Error("legacy migration failed; fred refuses to start with unmigrated legacy containers — "+
					"investigate the failure cause and re-run fred (migration is idempotent), "+
					"or deprovision the lease manually if data loss is acceptable",
					"lease_uuid", plan.LeaseUUID, "error", err)
				return fmt.Errorf("legacy migration failed: lease %s: %w", plan.LeaseUUID, err)
			}
		}
		// Re-list managed containers: migration changed every container's
		// name + label set, so the slice captured at the top of this
		// function is stale and the main loop below would otherwise see
		// the old names.
		refreshed, err := b.docker.ListManagedContainers(ctx)
		if err != nil {
			return fmt.Errorf("re-list managed containers after migration: %w", err)
		}
		containers = refreshed
	}

	allocsByLease := make(map[string][]shared.ResourceAllocation)
	building := make(map[string]*recoveredProvision)
	// firstExitedByLease[uuid] is the container ID of the first container we
	// observed in an exited state for that lease. Used to fire containerDiedMsg
	// into the actor for Ready→Failed transitions so the SM handles the
	// callback emission via its Failing→Failed flow.
	firstExitedByLease := make(map[string]string)
	skippedUnknownSKU := 0

	// Group containers by lease UUID
	for _, c := range containers {
		// Skip containers without required labels
		if c.LeaseUUID == "" || c.SKU == "" {
			b.logger.Warn("skipping container with missing labels", "container_id", leasesm.ShortID(c.ContainerID))
			continue
		}

		// Skip migration -prev remnants. These are legacy containers renamed
		// by executeLegacyMigration's rollback window — they still carry the
		// fred.lease_uuid + fred.managed labels but no fred.service_name, so
		// without this guard the legacy-single-item branch below would
		// process them as live leases (inflating prov.Quantity and appending
		// a spurious LeaseItem{ServiceName:""}). The post-grace goroutine
		// removes them, but recover can run inside the grace window or
		// after an interrupted shutdown leaves orphans. isLegacyContainer
		// (migrate.go) applies the same exclusion at planning time; this
		// is the matching exclusion at recovery time.
		if strings.HasSuffix(c.Name, "-prev") {
			continue
		}

		// Look up SKU profile for resource allocation
		profile, err := b.cfg.GetSKUProfile(c.SKU)
		if err != nil {
			b.logger.Error("skipping container with unknown SKU — container is running but untracked",
				"container_id", leasesm.ShortID(c.ContainerID),
				"sku", c.SKU,
			)
			skippedUnknownSKU++
			continue
		}

		// Check if we already have a provision record for this lease
		prov, exists := building[c.LeaseUUID]
		if !exists {
			prov = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:         c.LeaseUUID,
					Tenant:            c.Tenant,
					ProviderUUID:      c.ProviderUUID,
					SKU:               c.SKU,
					Status:            containerStatusToProvisionStatus(c.Status),
					Quantity:          0, // set from ContainerIDs below
					CreatedAt:         c.CreatedAt,
					FailCount:         c.FailCount,
					LastError:         "", // populated by cold-start/transition logic below
					CallbackURL:       c.CallbackURL,
					Items:             nil, // rebuilt from labels below
					ContainerIDs:      make([]string, 0),
					StackManifest:     nil, // restored below
					ServiceContainers: nil, // rebuilt from labels below
				},
				volumeCleanupAttempts: 0,
			}

			// Restore manifest from the last successful (active) release so
			// restart/update work after a cold start (manifest is not stored
			// in labels). Using LatestActive avoids picking up a failed
			// release (e.g., a failed update to a newer image).
			//
			// ParsePayload always returns a *StackManifest (legacy flat
			// payloads are auto-wrapped under DefaultServiceName). After
			// Task 9's recover-time migration runs, every recovered
			// provision is stack-form on disk, so the populated field is
			// always prov.StackManifest.
			if b.releaseStore != nil {
				if rel, relErr := b.releaseStore.LatestActive(c.LeaseUUID); relErr == nil && rel != nil && len(rel.Manifest) > 0 {
					stackM, payloadErr := manifest.ParsePayload(rel.Manifest)
					if payloadErr != nil {
						b.logger.Warn("failed to parse recovered manifest",
							"lease_uuid", c.LeaseUUID, "error", payloadErr)
					} else {
						prov.StackManifest = stackM
					}
				}
			}

			building[c.LeaseUUID] = prov
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
					// CustomDomain is per-service: all instance containers
					// of a service carry byte-identical labels. Trust the
					// first one we recovered; later iterations are no-ops.
					if prov.Items[idx].CustomDomain == "" && c.CustomDomain != "" {
						prov.Items[idx].CustomDomain = c.CustomDomain
					}
					found = true
					break
				}
			}
			if !found {
				prov.Items = append(prov.Items, backend.LeaseItem{
					SKU:          c.SKU,
					Quantity:     1,
					ServiceName:  c.ServiceName,
					CustomDomain: c.CustomDomain,
				})
			}
		} else if len(prov.Items) == 0 {
			// Legacy single-item lease: rebuild prov.Items[0] from this
			// (only) container's labels so Restart/Update can re-emit the
			// secondary router. Idempotent across recovery iterations
			// because legacy provisions hold one container.
			prov.Items = append(prov.Items, backend.LeaseItem{
				SKU:          c.SKU,
				Quantity:     1,
				CustomDomain: c.CustomDomain,
			})
		}

		// Use the highest FailCount across containers. Labels are normally
		// identical, but can diverge after a partial re-provision.
		if c.FailCount > prov.FailCount {
			prov.FailCount = c.FailCount
		}

		// If any container is not ready, mark the whole provision as not ready.
		// Also track the first exited container — recoverState fires
		// containerDiedMsg with this ID for Ready→Failed transitions so the
		// SM's Failing state handles the callback emission.
		status := containerStatusToProvisionStatus(c.Status)
		if status != backend.ProvisionStatusReady && prov.Status == backend.ProvisionStatusReady {
			prov.Status = status
		}
		if status == backend.ProvisionStatusFailed {
			if _, already := firstExitedByLease[c.LeaseUUID]; !already {
				firstExitedByLease[c.LeaseUUID] = c.ContainerID
			}
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

	// Detect ready→failed transitions: containers that were running but have
	// since crashed. We hand off to the SM by firing containerDiedMsg on the
	// actor *after* the merge. Status stays Ready in the building value so the
	// actor's guard sees the pre-transition state and permits evContainerDied;
	// FailCount and LastError are populated by the SM's Failing entry action.
	var failedLeases []string
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusReady {
			if rec, ok := building[uuid]; ok && rec.Status == backend.ProvisionStatusFailed {
				rec.Status = backend.ProvisionStatusReady
				rec.FailCount = existing.FailCount
				rec.LastError = existing.LastError
				failedLeases = append(failedLeases, uuid)
				b.logger.Warn("container crashed after provisioning",
					"lease_uuid", uuid,
					"tenant", existing.Tenant,
				)
			}
		}
	}

	// Cold-start correction: provisions recovered as failed with no prior
	// in-memory state carry a creation-time FailCount label. Increment it to
	// account for the failure evidenced by the dead container. The baseline
	// LastError rides the materialized value.
	var coldStartFailed []string
	for uuid, rec := range building {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				rec.FailCount++
				rec.LastError = leasesm.ErrMsgContainerExited
				coldStartFailed = append(coldStartFailed, uuid)
				b.logger.Info("cold-start: adjusted FailCount for already-failed provision",
					"lease_uuid", uuid,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// FailCount anti-regression on rebuilt entries: a re-list after an in-memory
	// increment would otherwise regress FailCount to the stale label. Preserve
	// the higher in-memory value. Skipped for in-flight statuses (preserved
	// wholesale below).
	for uuid, rec := range building {
		existing, ok := b.provisions[uuid]
		if !ok {
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// preserved wholesale below
		default:
			if existing.FailCount > rec.FailCount {
				rec.FailCount = existing.FailCount
			}
		}
	}

	// Publish: materialize every rebuilt entry into a fresh *provision (the only
	// path a recoveredProvision reaches b.provisions). A fresh struct clears
	// stale fields (LastError, VolumeCleanupAttempts) exactly as the prior
	// fresh-&provision{}+swap did.
	final := make(map[string]*provision, len(building))
	for uuid, rec := range building {
		final[uuid] = rec.materialize()
	}

	// Overlay existing entries that must be preserved: the actor / deprovision
	// goroutine owns their live state, so reuse the live *provision pointer
	// (no off-actor field mutation).
	for uuid, existing := range b.provisions {
		if _, hasContainers := building[uuid]; hasContainers {
			// By-design (ENG-414): only the in-flight statuses below are preserved
			// here. Ready and Failing/Failed deliberately fall through to the
			// container-derived (materialized) value, so a crashed-then-running lease
			// recovers to Ready (locked by TestRecoverState_FailCountAntiRegression).
			// recoverState cannot distinguish that legitimate recovery from the narrow
			// race where the actor set Failing/Failed (via an event-loop die) AFTER our
			// pre-merge ListManagedContainers snapshot still showed the container
			// running — so an actor-set Failing/Failed can be momentarily overwritten
			// with Ready. This is accepted: it self-heals (the in-flight diag goroutine
			// completes → evDiagGathered → Failed and rewrites Status; or, once the dead
			// container is GC'd, the no-containers branch below drops the phantom-Ready
			// entry) and emits NO duplicate failure callback — Failing and Failed both
			// Ignore(evContainerDied) (lease_sm.go) and the SM's internal state
			// (NewStateMachine, not external storage) is unaffected by this map swap.
			switch existing.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				// In-flight re-provision: the rebuilt containers belong to the
				// previous (failed) provision; keep the in-flight entry so the
				// next container creation picks up the right FailCount.
				final[uuid] = existing
			case backend.ProvisionStatusDeprovisioning:
				// The deprovision goroutine owns this lease; do not resurrect it
				// to a container-derived status (ENG-193 explicit case).
				final[uuid] = existing
			}
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// In-flight operation that hasn't produced containers yet.
			final[uuid] = existing
		case backend.ProvisionStatusFailing:
			// Failing is transient (container-death detected, diag goroutine not
			// yet fired DiagGathered). Normalize to Failed so retry paths (which
			// require Status == Failed) can proceed. Build the kept entry as a
			// value — no in-place mutation of the published struct.
			rec := recoveredFromProvision(existing)
			rec.Status = backend.ProvisionStatusFailed
			final[uuid] = rec.materialize()
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers are gone — preserve so the
			// reconciler sees the failure and its FailCount.
			final[uuid] = existing
		case backend.ProvisionStatusDeprovisioning:
			// Owned by the in-flight deprovision goroutine; preserve untouched
			// (ENG-193 explicit case — previously dropped on recovery).
			final[uuid] = existing
		}
	}
	b.provisions = final

	// Build the final allocations list, excluding leases with in-flight
	// operations (provisioning/restarting/updating). Their old containers
	// are being cleaned up concurrently; including stale allocations would
	// race with TryAllocate in Provision/Restart/Update.
	var allocations []shared.ResourceAllocation
	for uuid, allocs := range allocsByLease {
		if prov, ok := final[uuid]; ok {
			switch prov.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				continue
			}
		}
		allocations = append(allocations, allocs...)
	}
	b.pool.Reset(allocations)

	// Snapshot aggregate stats from the recovered map before releasing the lock.
	// After unlock, `final` aliases `b.provisions` and concurrent goroutines
	// may modify both the map and the pointed-to provision structs.
	var readyCount float64
	totalContainers := 0
	leaseCount := len(final)
	activeTenants := make(map[string]bool, len(final))
	for _, p := range final {
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
	b.refreshRetentionAccounting()

	// Gather diagnostics for cold-start failures only. Ready→Failed
	// transitions (failedLeases) are handled by the SM's Failing state,
	// whose OnEntry action spawns the async diag goroutine — same code
	// path as a live container-death event.
	allFailed := coldStartFailed
	// failedDiag carries the gathered diagnostic together with the instance
	// identity (CreatedAt) captured at snapshot time, so the write loop below
	// can verify it is still enriching the SAME failed instance the diag was
	// gathered from (ENG-193 code-review #1).
	type failedDiag struct {
		diag      string
		createdAt time.Time
	}
	failedDiagnostics := make(map[string]failedDiag, len(allFailed))
	for _, uuid := range allFailed {
		b.provisionsMu.RLock()
		prov, ok := b.provisions[uuid]
		if !ok {
			b.provisionsMu.RUnlock()
			continue
		}
		containerIDs := append([]string(nil), prov.ContainerIDs...)
		// Capture the instance identity under the SAME RLock that snapshots
		// containerIDs, so the (CreatedAt, containerIDs) pair is consistent.
		createdAt := prov.CreatedAt
		b.provisionsMu.RUnlock()

		for _, cid := range containerIDs {
			state, inspErr := b.inspector.InspectInstance(ctx, cid)
			if inspErr != nil {
				b.logger.Warn("failed to inspect container during diagnostics gathering", "lease", uuid, "container_id", leasesm.ShortID(cid), "error", inspErr)
				continue
			}
			// Mirror the "terminally gone?" decision from the SM guard:
			// PhaseExited and PhaseFailed cover the Docker statuses that
			// previously mapped to ProvisionStatusFailed in
			// containerStatusToProvisionStatus.
			if state != nil && (state.Phase == leasesm.PhaseExited || state.Phase == leasesm.PhaseFailed) {
				failedDiagnostics[uuid] = failedDiag{
					diag:      b.gatherer.GatherDiagnostics(ctx, cid, state),
					createdAt: createdAt,
				}
				break
			}
		}
	}

	// Route the enriched LastError through the store seam (UpdateFn) so recover
	// holds no raw b.provisions field access. The Status==Failed re-check stays
	// INSIDE the closure: a concurrent Deprovision/Provision-retry/Restart that
	// took ownership during the diag window must not get its fresh LastError
	// clobbered with this failure's data (ENG-193).
	for uuid, fd := range failedDiagnostics {
		enriched := leasesm.ErrMsgContainerExited + ": " + fd.diag
		createdAt := fd.createdAt
		b.provisionStore.UpdateFn(uuid, func(p *leasesm.ProvisionState) {
			// Only enrich the SAME failed instance the diag was gathered from: a
			// Provision-retry that replaced the lease during the diag I/O window
			// gets a fresh CreatedAt, so its LastError is not clobbered with this
			// failure's data (ENG-193 code-review #1).
			if p.Status == backend.ProvisionStatusFailed && p.CreatedAt.Equal(createdAt) {
				p.LastError = enriched
			}
		})
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
				entry:        leasesm.DiagnosticSnapshot(&prov.ProvisionState),
				containerIDs: append([]string(nil), prov.ContainerIDs...),
				keys:         leasesm.ContainerLogKeys(&prov.ProvisionState),
			})
		}
	}
	b.provisionsMu.RUnlock()
	for _, item := range diagItems {
		b.persistDiagnostics(item.entry, item.containerIDs, item.keys)
	}

	// Hand off Ready→Failed transitions to each lease's actor. The SM's
	// Ready→Failing→Failed flow gathers diagnostics via the async goroutine
	// (same code path as a live container-death event) and emits the
	// terminal Failed callback from Failed.OnEntryFrom(evDiagGathered).
	// Callback suppression on concurrent Deprovision is handled
	// structurally by Failing.OnExit.
	for _, uuid := range failedLeases {
		containerID, ok := firstExitedByLease[uuid]
		if !ok {
			// Shouldn't happen: if we detected a Ready→Failed transition,
			// some container for this lease was observed as exited.
			b.logger.Warn("ready→failed transition detected but no exited container",
				"lease_uuid", uuid)
			continue
		}
		if !b.routeToLease(uuid, leasesm.ContainerDiedMsg{ContainerID: containerID}) {
			dieEventDroppedTotal.WithLabelValues("reconcile").Inc()
			b.logger.Warn("die event dropped during reconcile dispatch; reconciler will re-detect",
				"lease_uuid", uuid, "container_id", leasesm.ShortID(containerID))
		}
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
// leaseHasActiveRelease reports whether the lease that owns volume id
// (fred-{uuid}-{service}-{idx}) still has an active release record. Used to keep
// the orphan reaper from destroying a live lease's data when its containers were
// removed out-of-band (ENG-505). Returns false (i.e. treat as reapable) when
// there is no release store or the name doesn't carry a parseable lease UUID.
func (b *Backend) leaseHasActiveRelease(volumeID string) bool {
	if b.releaseStore == nil {
		return false
	}
	leaseUUID, ok := leaseUUIDFromVolumeName(volumeID)
	if !ok {
		return false
	}
	rel, err := b.releaseStore.LatestActive(leaseUUID)
	if err != nil {
		// Fail safe: a transient release-store read error must NOT let the reaper
		// destroy a volume that may have an active release. Keep it; the next boot
		// retries. This mirrors the retention-store fail-safe in cleanupOrphaned-
		// Volumes below (a read failure skips destruction rather than risking it).
		b.logger.Warn("cleanupOrphanedVolumes: release-store read failed; keeping volume (fail-safe)", "volume_id", volumeID, "error", err)
		return true
	}
	return rel != nil
}

// leaseUUIDFromVolumeName extracts the lease UUID from a managed volume name of
// the form fred-{uuid}-{service}-{idx}, where {uuid} is the canonical 36-char
// form. Returns ("", false) if id does not match that shape.
func leaseUUIDFromVolumeName(id string) (string, bool) {
	const prefix = "fred-"
	rest, ok := strings.CutPrefix(id, prefix)
	if !ok {
		return "", false
	}
	// {uuid}(36) + "-" + {service} + "-" + {idx}
	if len(rest) < 37 || rest[36] != '-' {
		return "", false
	}
	candidate := rest[:36]
	if _, err := uuid.Parse(candidate); err != nil {
		return "", false
	}
	return candidate, true
}

func (b *Backend) cleanupOrphanedVolumes(ctx context.Context) error {
	volumeIDs, err := b.volumes.List()
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	if len(volumeIDs) == 0 {
		return nil
	}

	// Build set of expected volume IDs from recovered provisions. Volume
	// IDs are always service-aware now (fred-{lease}-{service}-{idx});
	// Task 9's recover-time migration renames any pre-existing legacy
	// fred-{lease}-{idx} volumes onto this convention before the main
	// loop sees them.
	expected := make(map[string]bool)
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		for _, item := range prov.Items {
			for i := range item.Quantity {
				expected[fmt.Sprintf("fred-%s-%s-%d", leaseUUID, item.ServiceName, i)] = true
			}
		}
	}
	b.provisionsMu.RUnlock()

	// Protect retention-record canonicals from the reaper. reconcileRetentions
	// runs immediately before this in Start and re-quarantines crash-stranded
	// canonical volumes back to the fred-retained- namespace — but if any of
	// those renames FAILED (real Docker error, not a benign no-op), the volume
	// is still canonical-named, not fred-retained-, and not in any live
	// provision's expected set, so the loop below would destroy it = permanent
	// data loss. Add those canonicals (active arm: the not-yet-renamed canonical;
	// restoring arm: the adopted/in-flight new-lease canonical) to the expected
	// set so an incomplete rename can never be reaped.
	if b.retentionStore != nil {
		if recs, rerr := b.retentionStore.List(); rerr != nil {
			// FAIL SAFE: we cannot read the retention store, so we cannot build the
			// protected-canonical set. Proceeding to the destroy loop could reap a
			// retained canonical we couldn't protect = permanent data loss. Skip
			// orphan destruction entirely this run; the next boot retries.
			b.logger.Error("cleanupOrphanedVolumes: retention read failed; skipping orphan destruction this run (fail-safe)", "error", rerr)
			return nil
		} else {
			for _, e := range recs {
				switch e.Status {
				case shared.RetentionStatusActive:
					for _, retained := range e.RetainedVolumeNames {
						expected[canonicalFromRetained(retained)] = true // protect a not-yet-renamed canonical
					}
				case shared.RetentionStatusRestoring:
					for _, retained := range e.RetainedVolumeNames {
						expected[retainedToNewCanonical(retained, e.OriginalLeaseUUID, e.NewLeaseUUID)] = true // adopted/in-flight new-lease canonical
						expected[canonicalFromRetained(retained)] = true                                       // original-lease canonical (un-retained volume from a partial soft-delete)
					}
				}
			}
		}
	}

	var orphanCount, failCount int
	for _, id := range volumeIDs {
		if isRetainedVolume(id) {
			continue
		}
		if expected[id] {
			continue
		}
		if b.leaseHasActiveRelease(id) {
			// A successfully-provisioned lease keeps an active release until it is
			// cleanly deprovisioned, so a volume whose lease still has one is not a
			// create-crash leak — its containers were merely removed out-of-band
			// (e.g. an operator `docker prune`). Reaping it would silently destroy
			// retained tenant data (ENG-505). Over-keeping a stale volume is the safe
			// direction here; a genuine leak has no release.
			b.logger.Warn("cleanupOrphanedVolumes: lease still has an active release; not reaping its volume", "volume_id", id)
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
					if leaseUUID, found := b.findLeaseByContainerID(event.ContainerID); found {
						if !b.routeToLease(leaseUUID, leasesm.ContainerDiedMsg{ContainerID: event.ContainerID}) {
							dieEventDroppedTotal.WithLabelValues("event_loop").Inc()
							b.logger.Warn("die event dropped at event loop dispatch; reconciler will re-detect",
								"lease_uuid", leaseUUID, "container_id", leasesm.ShortID(event.ContainerID))
						}
					}
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
