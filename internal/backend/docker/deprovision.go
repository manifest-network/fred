package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

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
	if err := b.requireMutationAdmission(ctx, "deprovision"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	unlockCommand := b.commandFence.Lock(leaseUUID)
	defer unlockCommand()
	if err := b.ensureCommittedRestoreDestinationForClose(leaseUUID); err != nil {
		return err
	}
	command, reply, err := leasesm.NewDeprovisionCommand(ctx)
	if err != nil {
		return err
	}
	if err := b.routeToLeaseBlocking(ctx, leaseUUID, command); err != nil {
		return err
	}
	return b.waitForReply(ctx, reply.Result())
}

// handoffCommittedRestoreToClose consumes a lingering restore finalizer only
// after the close intent has durably copied destination identity, topology,
// callbacks, and release fence. This ordering makes a crash on either side safe:
// before the handoff the restore finalizer owns recovery; afterwards the close
// journal owns teardown and can resume without any container survivor.
func (b *Backend) handoffCommittedRestoreToClose(
	leaseUUID string,
	closeClaim shared.CloseIntentClaim,
	hasCloseIntent bool,
) error {
	if b.retentionStore == nil {
		return nil
	}
	source, err := b.retentionStore.RestoringSourceByDestination(leaseUUID)
	if err != nil {
		return fmt.Errorf("read restore ownership before close admission: %w", err)
	}
	if source == nil {
		return nil
	}
	if !hasCloseIntent || closeClaim.CleanupOnly() {
		return fmt.Errorf("close of restored destination %q requires a full durable close intent", leaseUUID)
	}
	if closeClaim.Backend() != b.Name() || closeClaim.BackendStorageID() != b.storageIdentity ||
		closeClaim.Tenant() != source.Tenant || closeClaim.ProviderUUID() != source.ProviderUUID ||
		!slices.Equal(closeClaim.Items(), source.DestinationItems) ||
		!slices.Equal(closeClaim.ResourceProfiles(), source.DestinationResourceProfiles) {
		return fmt.Errorf(
			"close intent authority differs from restore finalizer for destination %q",
			leaseUUID,
		)
	}
	if source.StackManifest == nil {
		return fmt.Errorf("restore finalizer for destination %q has no manifest", leaseUUID)
	}
	manifestBytes, err := json.Marshal(source.StackManifest)
	if err != nil {
		return fmt.Errorf("marshal restore finalizer manifest: %w", err)
	}
	if !bytes.Equal(closeClaim.Manifest(), manifestBytes) {
		return fmt.Errorf("close intent manifest differs from restore finalizer for destination %q", leaseUUID)
	}
	if source.DestinationCallbackURL != "" || source.DestinationLifecycleCallbackURL != "" {
		resolvedCallbackURL, resolvedLifecycleCallbackURL, resolveErr :=
			backend.ResolveMaintenanceCallbackURLs(
				source.DestinationCallbackURL,
				source.DestinationLifecycleCallbackURL,
				closeClaim.LifecycleCallbackURL(),
			)
		if resolveErr != nil ||
			resolvedCallbackURL != closeClaim.CallbackURL() ||
			resolvedLifecycleCallbackURL != closeClaim.LifecycleCallbackURL() {
			return fmt.Errorf("close intent callback pair differs from restore finalizer for destination %q", leaseUUID)
		}
	}
	committed, err := b.restoreDestinationCommitted(*source)
	if err != nil {
		return fmt.Errorf("validate restore commit before close handoff: %w", err)
	}
	if !committed {
		return fmt.Errorf("restore destination %q is not durably committed", leaseUUID)
	}
	if err := b.deleteRestoreFinalizerStrict(leaseUUID, source); err != nil {
		return fmt.Errorf("handoff restore finalizer to close intent: %w", err)
	}
	return nil
}

// ensureCommittedRestoreDestinationForClose is the pre-journal close gate. It
// prevents BeginCloseIntent from preempting an uncommitted restore and creating
// two incompatible durable owners. Once this succeeds, a crash after close
// admission is safe because the exact active Release already owns destination
// bytes and the close claim can take over the finalizer on recovery.
func (b *Backend) ensureCommittedRestoreDestinationForClose(leaseUUID string) error {
	if b.retentionStore == nil {
		return nil
	}
	source, err := b.retentionStore.RestoringSourceByDestination(leaseUUID)
	if err != nil {
		return fmt.Errorf("read restore ownership before close admission: %w", err)
	}
	if source == nil {
		return nil
	}
	committed, err := b.restoreDestinationCommitted(*source)
	if err != nil || !committed {
		return fmt.Errorf(
			"%w: restore destination %q has not durably committed ownership",
			backend.ErrInvalidState, leaseUUID,
		)
	}
	operation, err := b.currentRestoreOperation(*source)
	if err != nil && !errors.Is(err, shared.ErrOperationIntentMissing) {
		return fmt.Errorf(
			"%w: restore destination %q has conflicting operation authority: %w",
			backend.ErrInvalidState, leaseUUID, err,
		)
	}
	switch state := operation.(type) {
	case shared.OperationIntentClaim:
		proof, proofErr := b.operationSettlement.ProveCommittedOperation(state)
		if proofErr != nil {
			return fmt.Errorf("prove committed restore before close admission: %w", proofErr)
		}
		if b.callbackPublisher == nil {
			return errors.New("callback publisher is required")
		}
		if err := b.callbackPublisher.PublishOperationSuccessContext(b.stopCtx, proof); err != nil {
			return fmt.Errorf("settle committed restore before close admission: %w", err)
		}
	case shared.OperationFailed:
		return fmt.Errorf(
			"%w: restore destination %q has contradictory committed and failed outcomes",
			backend.ErrInvalidState, leaseUUID,
		)
	}
	b.provisionsMu.RLock()
	projection := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	if projection == nil {
		return fmt.Errorf(
			"%w: restore destination %q has no live projection for close callback authority",
			backend.ErrInvalidState,
			leaseUUID,
		)
	}
	resolvedCallbackURL, resolvedLifecycleCallbackURL, resolveErr :=
		backend.ResolveMaintenanceCallbackURLs(
			source.DestinationCallbackURL,
			source.DestinationLifecycleCallbackURL,
			projection.LifecycleCallbackURL,
		)
	if resolveErr != nil ||
		resolvedCallbackURL != projection.CallbackURL ||
		resolvedLifecycleCallbackURL != projection.LifecycleCallbackURL {
		return fmt.Errorf(
			"%w: restore destination %q live callback authority differs from its committed lineage",
			backend.ErrInvalidState, leaseUUID,
		)
	}
	return nil
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
// branch — the volumes are still on disk and the durable close is retried — so
// the reservation keeps counting until exact terminal evidence releases it.
// On retry, only the stuck containers are attempted.
func (b *Backend) doDeprovision(ctx context.Context, actorScope leasesm.ActorCloseScope) error {
	if b.recoveryCoordinator == nil {
		return errors.New("deprovision requires lease recovery authority")
	}
	leaseUUID := actorScope.LeaseUUID()
	if leaseUUID == "" {
		return errors.New("deprovision requires active actor-owned close authority")
	}
	return b.recoveryCoordinator.WithActorClose(
		actorScope,
		func(recoveryScope shared.LeaseRecoveryScope) error {
			return b.doDeprovisionScoped(ctx, recoveryScope, leaseUUID)
		},
	)
}

func (b *Backend) doDeprovisionScoped(
	ctx context.Context,
	recoveryScope shared.LeaseRecoveryScope,
	leaseUUID string,
) error {
	logger := b.logger.With("lease_uuid", leaseUUID)
	if b.closeSettlement == nil || b.callbackStore == nil {
		return errors.New("deprovision requires durable close settlement")
	}
	b.provisionsMu.RLock()
	_, projectionExists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	if err := b.settleCommittedOperationBeforeClose(leaseUUID); err != nil {
		return err
	}
	if err := b.settleMaintenanceBeforeClose(leaseUUID); err != nil {
		return err
	}
	claim, found, err := b.acquireCloseIntent(ctx, leaseUUID, projectionExists)
	if err != nil {
		return err
	}
	if !found {
		if projectionExists {
			return errors.New("deprovision could not establish durable close authority")
		}
		return nil
	}
	if claim.CleanupOnly() == projectionExists {
		return fmt.Errorf(
			"durable close intent projection mismatch: cleanup_only=%t projection_exists=%t",
			claim.CleanupOnly(), projectionExists,
		)
	}
	if err := b.handoffCommittedRestoreToClose(leaseUUID, claim, true); err != nil {
		return err
	}

	var outcome shared.CloseExecutionOutcome
	if claim.ExecutionGeneration().Valid() {
		outcome, err = b.closeSettlement.RecoverCloseExecution(ctx, recoveryScope, claim)
		if err != nil {
			return fmt.Errorf("classify recovered close execution: %w", err)
		}
		if pending, ok := outcome.(shared.CloseExecutionPending); ok {
			if !pending.RetryableNow() {
				return fmt.Errorf("recovered close remains ambiguous: %w", pending.Cause())
			}
			execution, retryErr := b.closeSettlement.RetryCloseExecution(pending)
			if retryErr != nil {
				return fmt.Errorf("start retryable close generation: %w", retryErr)
			}
			outcome = b.closeSettlement.ExecuteClose(ctx, execution)
		}
	}
	if outcome == nil {
		execution, startErr := b.closeSettlement.StartCloseExecution(claim)
		if startErr != nil {
			return fmt.Errorf("start durable close execution: %w", startErr)
		}
		outcome = b.closeSettlement.ExecuteClose(ctx, execution)
	}

	var terminalOutcome shared.CloseTerminalOutcome
	switch terminal := outcome.(type) {
	case shared.CloseExecutionDestroyed:
		terminalOutcome = terminal
		err = b.completeCloseOutcome(terminal)
	case shared.CloseExecutionRetained:
		terminalOutcome = terminal
		err = b.completeCloseOutcome(terminal)
	case shared.CloseExecutionPending:
		err = terminal.Cause()
		if err == nil {
			err = errors.New("close execution remains pending")
		}
		b.markClosePending(leaseUUID, err)
		return err
	default:
		return fmt.Errorf("unknown close execution outcome %T", outcome)
	}
	if err != nil {
		b.markClosePending(leaseUUID, err)
		return err
	}

	if b.cfg.IsNetworkIsolation() && terminalOutcome.Tenant() != "" {
		// The coordinator owns selection as well as mutation. A close caller can
		// request a convergence pass, but cannot target a tenant network directly.
		b.cleanupOrphanedNetworks(ctx)
	}
	deprovisionsTotal.Inc()
	logger.Info("deprovisioned")
	return nil
}

func (b *Backend) completeCloseOutcome(
	outcome shared.CloseTerminalOutcome,
) error {
	b.recoverySnapshotMu.RLock()
	defer b.recoverySnapshotMu.RUnlock()
	if _, retained := outcome.(shared.CloseExecutionRetained); retained {
		if err := b.refreshRetentionAccountingChecked(); err != nil {
			return fmt.Errorf("refresh retained close accounting: %w", err)
		}
	}
	if err := b.failureDiagnostics.PublishCloseFailure(outcome); err != nil {
		return fmt.Errorf("publish interrupted close diagnostics: %w", err)
	}
	if _, err := b.closeSettlement.CompleteClose(outcome); err != nil {
		return fmt.Errorf("complete durable close: %w", err)
	}
	b.provisionStore.Delete(outcome.LeaseUUID())
	b.releaseLeaseAllocations(outcome.LeaseUUID(), outcome.Items())
	if b.callbackSender != nil {
		b.callbackSender.NotifyPendingCallbacks()
	}
	return nil
}

func (b *Backend) markClosePending(leaseUUID string, cause error) {
	var diagSnap shared.DiagnosticEntry
	b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		p.LastError = fmt.Sprintf("close execution remains pending: %v", cause)
		p.Reason = backend.ReasonCleanupFailed
		p.Message = backend.MsgCleanupFailed
		diagSnap = leasesm.DiagnosticSnapshot(p)
	})
	b.persistDiagnostics(diagSnap, nil)
}

// doClosePhysical is the single construction-bound close workflow. Every
// target comes from subject; the caller cannot supply a lease, mutator, or
// terminal retained/destroyed choice.
func (b *Backend) doClosePhysical(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.ClosePhysicalSubject,
) error {
	leaseUUID := subject.LeaseUUID()
	logger := b.logger.With("lease_uuid", leaseUUID)
	closeClaim := subject.Intent()

	// Durable close authority, not the mutable projection, supplies every
	// identity/topology input. The projection contributes only recorded IDs to
	// the rediscovering teardown fallback.
	var containerIDs []string
	b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
		p.Status = backend.ProvisionStatusDeprovisioning
		containerIDs = append([]string(nil), p.ContainerIDs...)
	})
	items := closeClaim.Items()
	resourceProfiles := closeClaim.ResourceProfiles()
	tenant := closeClaim.Tenant()
	stackManifest, err := manifest.ParsePayload(closeClaim.Manifest())
	if err != nil {
		return fmt.Errorf("parse durable close manifest: %w", err)
	}
	// Remove all containers via Compose Down for atomic cleanup; fall back to
	// per-container removal if Compose fails (e.g., compose project metadata went
	// missing). After Tasks 4-6 every provision is stack-shaped, so the fallback only
	// fires under genuine substrate failure rather than as the steady-state legacy
	// path. The fallback RE-DISCOVERS the containers by label instead of walking
	// containerIDs: this record is empty for any lease that never reached Ready — a
	// failed restore's provision names none of the containers its compose Up created —
	// so a recorded-list fallback silently removes nothing exactly when a container
	// leaked (ENG-647). containerIDs is still passed and unioned in.
	var errs []error
	failedIDs, teardownErr := b.cleanupCloseContainers(ctx, mutations, subject, containerIDs)
	if teardownErr != nil {
		errs = append(errs, teardownErr)
	}

	retaining := closeClaim.RetainOnClose() && b.retentionStore != nil
	// A teardown that only partially succeeds keeps resources counted for the
	// retry rather than freeing them while stuck containers still run or volumes
	// remain on disk.
	//
	// Retaining close: keep the live allocation counted until the retained
	// record is recorded+refreshed (or the volumes are destroyed) — see the
	// deferred hand-off below — so the footprint is never momentarily
	// uncounted while the renamed volume persists on disk (prevents a
	// concurrent over-admit / ENOSPC).

	// claimedLeftBehind is set when a volume claimed by an IN-FLIGHT RESTORE is
	// deliberately left on disk (below). Its bytes must then stay reserved, because a
	// restoring record is counted by NEITHER projection — computeRetainedDiskMB skips
	// every non-active status and the admission pool is active+reaping — so the only
	// thing counting them is this closing lease's live allocation. Releasing it would
	// leave a real footprint counted by nobody and let admission over-commit against
	// it. reconcileRestoring takes the reservation over: its orphaned arm
	// re-quarantines the volume, RollbackRestoring makes the record active (so the
	// retained projection counts it again), and only THEN does it releaseAll the same
	// {lease}-{svc}-{idx} ids — re-counted before released, never a gap. pool.Release
	// is idempotent, so the hand-off is safe even though both paths name the same ids.
	// (ENG-647, PR #217 review.)
	var claimedLeftBehind bool

	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers.
		var diagSnap shared.DiagnosticEntry
		b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			p.Reason = backend.ReasonCleanupFailed
			p.Message = backend.MsgCleanupFailed
			diagSnap = leasesm.DiagnosticSnapshot(p)
		})
		// Correlation log so operators can still find the verbose detail (redacted
		// from the tenant-facing Message) by lease_uuid (ENG-508).
		logger.Warn("provision failed (verbose detail retained operator-side)",
			"lease_uuid", leaseUUID, "reason", backend.ReasonCleanupFailed, "detail", errors.Join(errs...))
		// Unlike the initial-mark migration, do NOT early-return on UpdateFn==false:
		// still persist diagnostics and surface the error. If the entry is gone,
		// diagSnap is zero-value and persistDiagnostics no-ops on its empty guard.
		b.persistDiagnostics(diagSnap, failedIDs)
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}

	// Destroy managed volumes for all instances — or soft-delete them into the
	// retained namespace when RetainOnClose is true.
	var volumeErrs []error
	// Ownership for this whole close, resolved once (volume_destroy.go). A volume an
	// IN-FLIGHT restore has adopted into THIS lease's namespace looks like ours by name
	// and is not: destroying it — or re-retaining it under this lease, which leaves the
	// original record naming paths that no longer exist — permanently kills that lease's
	// restore (ENG-647). Both volume arms below ask the same table, so neither can be
	// fixed without the other, which is how this used to go wrong.
	op := b.volumeOp(leaseUUID, logger)
	switch {
	case retaining:
		// Enumerate the lease's ACTUAL managed volumes (ground truth — no SKU guess).
		all, listErr := b.volumes.ListForProof(ctx)
		if listErr != nil {
			volumeErrs = append(volumeErrs, fmt.Errorf("list volumes for retention: %w", listErr))
		}
		prefixed := make([]string, 0, len(all))
		prefix := leaseVolumePrefix(leaseUUID)
		for _, id := range all {
			if strings.HasPrefix(id, prefix) { // excludes fred-retained-* and other leases
				prefixed = append(prefixed, id)
			}
		}
		// Asked as a QUERY, not a guarded action, because the destructive verb on this
		// arm is RenameVolume: retaining a foreign volume under
		// fred-retained-{this lease}-* strands the original record just as permanently as
		// destroying it would. Fail-safe on an unresolvable table — we cannot tell ours
		// from theirs, so we touch neither and the lease stays Failed for retry.
		canonical, foreign, claimErr := op.partition(prefixed)
		if claimErr != nil {
			logger.Error("deprovision: cannot establish volume ownership; skipping volume teardown this attempt",
				"error", claimErr)
			volumeErrs = append(volumeErrs, fmt.Errorf("identify restore-claimed volumes: %w", claimErr))
			break
		}
		// reconcileRestoring owns re-quarantining a foreign volume back to
		// fred-retained-{original}-*; it is not an error here, but the bytes are still on
		// disk, so this lease keeps holding their reservation.
		claimedLeftBehind = len(foreign) > 0
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
				// Routed through the choke point like every other destroy. `c` came from
				// partition against the same cached table, so the re-check is free and
				// cannot refuse — but going around it is exactly how this site's guard
				// stayed transitive, holding only because it iterated an
				// already-filtered slice (ENG-658).
				if rep := op.destroy(mutations, ctx, destroySiteDeprovisionReclaim, c); rep.leftOnDisk() {
					// The volume is still canonical on disk. Record the error so the
					// lease stays Failed and retries (re-detecting and re-destroying it);
					// do NOT add it to retainCanonical — it must never be retained.
					if err := rep.err(); err != nil {
						volumeErrs = append(volumeErrs, fmt.Errorf("reclaim writable-path-only volume %s: %w", c, err))
					} else {
						claimedLeftBehind = true
					}
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
			// Strict post-work classification decides between a genuinely
			// stateless destroy and an exact prior retained generation. Workflow
			// control flow never selects the terminal callback.
		default:
			// Hydrate a nil StackManifest from the release store BEFORE anything
			// reads it: the partition extractor and the persisted record must see
			// the SAME manifest. A cold-start recover restores the manifest
			// best-effort (recover.go) and leaves it nil if the active release is
			// missing/unparseable/store-nil; Restore rejects a nil-manifest record
			// as corrupt, so without this the volumes are retained but un-restorable.
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

			// Budget-first partition resolution: a non-aggregator (MaxPartitions==0)
			// never extracts — no manifest walk, no WARN, no counter — so its close
			// stays byte-identical to the legacy whole-tenant path. The tenant
			// snapshot is read at most once here and shared by the partition bound
			// AND the count-cap eviction below (the disk gate re-reads the store
			// itself, AFTER eviction, so it sees the post-eviction state).
			budget := resolveTenantRetentionBudget(b.cfg, tenant)
			var (
				tenantSnapshot     []shared.RetentionEntry
				evictionCandidates []shared.ActiveRetentionCandidate
				snapErr            error
			)
			if budget.MaxPartitions > 0 || budget.CountCap > 0 || budget.PerPartCount > 0 {
				tenantSnapshot, evictionCandidates, snapErr =
					b.retentionStore.ListTenantRetentionCandidates(tenant)
			}
			partition := ""
			if budget.MaxPartitions > 0 {
				var reason, rawDetail string
				partition, reason, rawDetail = shared.ExtractPartition(b.partitionSource, shared.PartitionInputs{Manifest: stackManifest})
				switch {
				case reason != "":
					retentionPartitionCollapsedTotal.WithLabelValues(reason).Inc()
					// rawDetail is pre-truncated by ExtractPartition (no site may log
					// the untruncated tenant-supplied value); logger carries lease_uuid.
					logger.Warn("retention partition collapsed to default bucket",
						"tenant", tenant, "reason", reason, "partition_raw", rawDetail)
				case partition != "":
					// boundPartition is collapse-only: it can only return partition
					// unchanged or "" (over-limit / snapshot-error), never a fault.
					partition = b.boundPartition(tenant, partition, budget, tenantSnapshot, snapErr, logger)
				}
				if partition != "" {
					retentionPartitionStampedTotal.Inc()
				}
			}

			// Count-cap eviction runs BEFORE the disk-refusal gate (two-level: L2
			// per-partition, then L1 per-tenant aggregate). The disk gate recomputes
			// ACTIVE-only sums from the store, so eviction (ACTIVE→REAPING) shrinks
			// what it sees: a full rolling window rolls instead of jamming into
			// refuse-forever. Best-effort cap room — a wasted eviction (the disk gate
			// still refuses below, or the record write later defers on a restore
			// race) only evicts the tenant's oldest, which the next close would evict
			// anyway. Fail-open on a snapshot read error: eviction never blocks a close.
			if snapErr != nil {
				retentionCapCheckFailedTotal.WithLabelValues(capCheckEvict).Inc()
				logger.Warn("retention cap eviction skipped: tenant snapshot unavailable (fail-open)", "tenant", tenant, "error", snapErr)
			} else if err := b.evictRetentionsToCap(ctx, tenant, budget, partition, evictionCandidates, leaseUUID); err != nil {
				retentionCapCheckFailedTotal.WithLabelValues(capCheckEvict).Inc()
				logger.Warn("retention cap eviction failed", "tenant", tenant, "error", err)
			}

			if scope, refuse := b.shouldRefuseRetentionWithResourceProfiles(
				leaseUUID, tenant, partition, durableItems, resourceProfiles, budget,
			); refuse {
				rep := b.destroyOnRefuseToRetain(mutations, ctx, op, retainCanonical, leaseUUID, tenant, partition, scope, logger)
				if err := rep.err(); err != nil {
					volumeErrs = append(volumeErrs, err)
				}
				claimedLeftBehind = claimedLeftBehind || len(rep.Claimed) > 0
				break
			}
			// Retain: the closing lease fits under both caps (count eviction ran
			// above; the disk gate did not refuse).
			retained := make([]string, 0, len(retainCanonical))
			for _, c := range retainCanonical {
				retained = append(retained, retainedName(c))
			}

			// RECORD-FIRST + ATOMIC: CloseSettlement.RecordRetention persists the active record (with
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
			ok, retentionWriteErr := b.closeSettlement.RecordRetention(
				closeClaim, partition, retained,
			)
			switch {
			case retentionWriteErr != nil:
				logger.Error("failed to write retention record", "lease_uuid", leaseUUID, "error", retentionWriteErr)
				volumeErrs = append(volumeErrs, fmt.Errorf("write retention record: %w", retentionWriteErr))
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
					if err := mutations.renameVolume(ctx, c, retainedName(c)); err != nil {
						logger.Error("failed to retain volume", "volume", c, "error", err)
						volumeErrs = append(volumeErrs, fmt.Errorf("retain volume %s: %w", c, err))
					}
				}
				if len(volumeErrs) == 0 {
					logger.Info("soft-deleted lease volumes", "lease_uuid", leaseUUID, "retained", len(retained))
				}
			}
		}
	default:
		names := make([]string, 0, len(items))
		for _, item := range items {
			for i := range item.Quantity {
				names = append(names, canonicalVolumeName(leaseUUID, item.ServiceName, i))
			}
		}
		// One call, one ownership resolution. A refused name is another lease's data
		// adopted under ours by an in-flight restore: reconcileRestoring re-quarantines
		// it once its rollback can complete, so we leave it (ENG-647). An unprovable
		// table surfaces through rep.err() and keeps the lease Failed for retry.
		rep := op.destroy(mutations, ctx, destroySiteDeprovisionDestroy, names...)
		if err := rep.err(); err != nil {
			volumeErrs = append(volumeErrs, err)
		}
		claimedLeftBehind = claimedLeftBehind || len(rep.Claimed) > 0
	}

	if len(volumeErrs) > 0 {
		joinedVolumeErr := errors.Join(volumeErrs...)
		var diagSnap shared.DiagnosticEntry
		b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
			p.ContainerIDs = nil // containers are gone
			p.Status = backend.ProvisionStatusFailed
			p.LastError = fmt.Sprintf("volume cleanup failed: %s", errors.Join(volumeErrs...))
			p.Reason = backend.ReasonCleanupFailed
			p.Message = backend.MsgCleanupFailed
			diagSnap = leasesm.DiagnosticSnapshot(p)
		})
		// Correlation log so operators can still find the verbose detail (redacted
		// from the tenant-facing Message) by lease_uuid (ENG-508).
		logger.Warn("provision failed (verbose detail retained operator-side)",
			"lease_uuid", leaseUUID, "reason", backend.ReasonCleanupFailed, "detail", errors.Join(volumeErrs...))
		// Persist diagnostics outside the lock so failure state survives
		// a process restart (no containers remain to recover from).
		b.persistDiagnostics(diagSnap, nil)
		return fmt.Errorf("volume cleanup failed: %w", joinedVolumeErr)
	}
	// No local boolean can finalize the close. Even a wholly successful workflow
	// returns through the strict construction-bound inventory classifier.
	_ = claimedLeftBehind
	return nil
}

func (b *Backend) releaseLeaseAllocations(leaseUUID string, items []backend.LeaseItem) {
	for _, item := range items {
		for i := range item.Quantity {
			b.pool.Release(fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, i))
		}
	}
	updateResourceMetrics(b.pool.Stats())
}
