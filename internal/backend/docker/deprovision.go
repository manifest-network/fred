package docker

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
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
	reply := make(chan error, 1)
	if err := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.DeprovisionMsg{Ctx: ctx, Reply: reply}); err != nil {
		return err
	}
	return b.waitForReply(ctx, reply)
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
	if _, err := b.currentRestoreIntent(*source); err != nil {
		return fmt.Errorf(
			"%w: restore destination %q has conflicting operation authority: %w",
			backend.ErrInvalidState, leaseUUID, err,
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
		wasReady             bool
		containerIDs         []string
		items                []backend.LeaseItem
		resourceProfiles     []shared.SKUResourceSnapshot
		tenant               string
		callbackURL          string
		lifecycleCallbackURL string
		providerUUID         string
		stackManifest        *manifest.StackManifest
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
		lifecycleCallbackURL = p.LifecycleCallbackURL
		providerUUID = p.ProviderUUID
		stackManifest = p.StackManifest
	})
	if err := b.settleCommittedOperationBeforeClose(leaseUUID); err != nil {
		return err
	}
	if err := b.settleMaintenanceBeforeClose(leaseUUID); err != nil {
		return err
	}
	closeClaim, hasCloseIntent, closeErr := b.acquireCloseIntent(
		ctx,
		leaseUUID,
		exists,
		tenant,
		providerUUID,
		items,
		stackManifest,
		callbackURL,
		lifecycleCallbackURL,
	)
	if closeErr != nil {
		return closeErr
	}
	if hasCloseIntent && closeClaim.CleanupOnly() == exists {
		return fmt.Errorf(
			"durable close intent projection mismatch: cleanup_only=%t projection_exists=%t",
			closeClaim.CleanupOnly(), exists,
		)
	}
	if err := b.handoffCommittedRestoreToClose(leaseUUID, closeClaim, hasCloseIntent); err != nil {
		return err
	}
	if hasCloseIntent && !closeClaim.CleanupOnly() {
		// Recovery and retries consume the immutable admission snapshot, never
		// mutable labels or a partially-updated in-memory projection.
		tenant = closeClaim.Tenant()
		providerUUID = closeClaim.ProviderUUID()
		items = closeClaim.Items()
		resourceProfiles = closeClaim.ResourceProfiles()
		callbackURL = closeClaim.CallbackURL()
		lifecycleCallbackURL = closeClaim.LifecycleCallbackURL()
		if payload := closeClaim.Manifest(); len(payload) > 0 {
			stack, err := manifest.ParsePayload(payload)
			if err != nil {
				return fmt.Errorf("parse durable close manifest: %w", err)
			}
			stackManifest = stack
		}
	}
	retireReleaseHistory := func() error {
		if hasCloseIntent {
			return b.purgeCloseReleaseHistory(closeClaim)
		}
		return b.purgeReleaseHistory(leaseUUID)
	}
	// A close can preempt Provision/Restore after its durable intent commit but
	// before the worker publishes the in-memory provision reservation. Settle
	// that exact operation even when no projection exists yet. This must precede
	// the idempotent-absence return as well as every lifecycle observation below;
	// otherwise a live-process intent can remain wedged until a restart.
	if b.callbackStore != nil && !hasCloseIntent {
		if _, err := b.callbackStore.FailOperationIntentIfPresent(
			leaseUUID, "operation preempted by deprovision",
		); err != nil {
			return fmt.Errorf("persist preempted operation completion before deprovision: %w", err)
		}
	}
	if !exists {
		// A missing in-memory projection is not proof that the substrate is empty.
		// Production's cleanup-only journal owns a bounded release snapshot and must
		// consume any stale containers and canonical volumes before retiring that
		// authority. The callback-store-less fallback retains the historical
		// release/rollback-only behavior used by isolated tests.
		var rollbackErr error
		if hasCloseIntent {
			closeClaim, rollbackErr = b.cleanupCloseWithoutProjection(ctx, closeClaim, logger)
		} else {
			rollbackErr = b.removeCommittedLegacyRollbackRemnants(ctx, leaseUUID, logger)
		}
		if rollbackErr != nil {
			return fmt.Errorf("complete unprojected close cleanup before release purge: %w", rollbackErr)
		}
		if hasCloseIntent {
			if err := b.resolveCloseIntent(
				closeClaim,
				backend.CallbackStatusDeprovisioned,
				"",
				false,
				func() error {
					if err := retireReleaseHistory(); err != nil {
						return fmt.Errorf("retire release history after containerless deprovision: %w", err)
					}
					return nil
				},
				func() { b.releaseLeaseAllocations(leaseUUID, closeClaim.Items()) },
			); err != nil {
				return err
			}
		} else if err := retireReleaseHistory(); err != nil {
			return fmt.Errorf("retire release history after containerless deprovision: %w", err)
		}
		return nil
	}
	resolvedLifecycleCallbackURL := lifecycleCallbackURL
	if !hasCloseIntent {
		var resolveErr error
		resolvedLifecycleCallbackURL, resolveErr = backend.ResolveLifecycleCallbackURL(
			callbackURL, lifecycleCallbackURL,
		)
		if resolveErr != nil {
			logger.Error("cannot derive lifecycle callback URL; suppressing observational callback", "error", resolveErr)
			resolvedLifecycleCallbackURL = ""
		}
	}
	// Decrement activeProvisions on the Ready→Deprovisioning transition so the
	// gauge stays accurate even if Deprovision later fails partially.
	if wasReady {
		activeProvisions.Dec()
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
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	failedIDs, teardownErr := b.teardownLeaseContainers(ctx, leaseUUID, containerIDs, stopTimeout,
		teardownOpDeprovision, logger)
	if teardownErr != nil {
		errs = append(errs, teardownErr)
	} else if cleanupErr := func() error {
		if hasCloseIntent {
			return b.removeCloseRollbackTargets(ctx, closeClaim, logger)
		}
		return b.removeCommittedLegacyRollbackRemnants(ctx, leaseUUID, logger)
	}(); cleanupErr != nil {
		errs = append(errs, fmt.Errorf("remove legacy rollback containers before release purge: %w", cleanupErr))
	}

	// releaseLive releases all pool allocations for this lease and updates
	// resource metrics. Successful durable closes call it inside their terminal
	// snapshot transaction; retain error paths use the deferred hand-off below.
	releaseLive := func() {
		b.releaseLeaseAllocations(leaseUUID, items)
	}
	retainRequested := b.cfg.RetainOnClose
	if hasCloseIntent {
		retainRequested = closeClaim.RetainOnClose()
	}
	retaining := retainRequested && b.retentionStore != nil
	// A teardown that only partially succeeds keeps resources counted for the
	// retry rather than freeing them while stuck containers still run or volumes
	// remain on disk.
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

	// claimedLeftBehind is set when a volume claimed by an IN-FLIGHT RESTORE is
	// deliberately left on disk (below). Its bytes must then stay reserved, because a
	// restoring record is counted by NEITHER projection — computeRetainedDiskMB skips
	// every non-active status and the admission pool is active+reaping — so the only
	// thing counting them is this closing lease's live allocation. Releasing it would
	// leave a real footprint counted by nobody and let admission over-commit against
	// it. reconcileRestoring takes the reservation over: its orphaned arm
	// re-quarantines the volume, RevertToActiveWithResourceProfiles makes the record active (so the
	// retained projection counts it again), and only THEN does it releaseAll the same
	// {lease}-{svc}-{idx} ids — re-counted before released, never a gap. pool.Release
	// is idempotent, so the hand-off is safe even though both paths name the same ids.
	// (ENG-647, PR #217 review.)
	var claimedLeftBehind bool

	// Retained set may have changed (this close may have added a retained
	// record below, or a prior attempt did); refresh after the volume branch.
	// For the retain path, also release live AFTER refresh (overlap, no gap).
	//
	// Gated on `retaining`: the non-retain else branch only destroys this lease's
	// own canonical volumes and never touches the retention store, so the retained
	// projection cannot change on a non-retain close — skip the O(#retained) bbolt
	// List() scan entirely on that (hot) path. releaseLiveOnRetainPath is only ever
	// set inside the retain branch, and non-retain releases live inline after a
	// successful terminal settlement, so nothing is missed by returning early here.
	retentionTerminalHandoffDone := false
	defer func() {
		if !retaining || retentionTerminalHandoffDone || hasCloseIntent {
			return
		}
		if err := b.refreshRetentionAccountingChecked(); err != nil {
			return
		}
		if releaseLiveOnRetainPath && !claimedLeftBehind {
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
		all, listErr := b.volumes.List()
		if listErr != nil {
			volumeErrs = append(volumeErrs, fmt.Errorf("list volumes for retention: %w", listErr))
		}
		// Keep the same inventory as a set for the zero-canonical retry proof below.
		// Re-listing after the partition/retention checks would introduce a second
		// point-in-time view and could turn a concurrent disappearance into a false
		// retained-success callback.
		allSet := make(map[string]struct{}, len(all))
		prefixed := make([]string, 0, len(all))
		prefix := leaseVolumePrefix(leaseUUID)
		for _, id := range all {
			allSet[id] = struct{}{}
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
				if rep := op.destroy(ctx, destroySiteDeprovisionReclaim, c); rep.leftOnDisk() {
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
			// No durable (declared-VOLUME) data remains to retain: the lease was
			// stateless, its volumes were already renamed on a prior attempt, or they
			// were all writable-path-only and just reclaimed above. An ACTIVE record
			// disambiguates the crash-after-rename case, but it is only proof when it
			// belongs to this lease and every recorded retained volume was present in
			// the authoritative inventory above. Anything less keeps the close journal
			// unresolved: otherwise a retry could publish retained:false despite durable
			// retained data, or retained:true for missing/corrupt data.
			if len(volumeErrs) == 0 {
				record, err := b.retentionStore.Get(leaseUUID)
				switch {
				case err != nil:
					volumeErrs = append(volumeErrs, fmt.Errorf("read retention record after zero-canonical close: %w", err))
				case record == nil:
					// Preserve the existing stateless / writable-path-only /
					// refuse-to-retain behavior: no record means there is no
					// durable retained result to report.
					releaseLiveOnRetainPath = true
				default:
					if proofErr := validateCompletedRetention(
						leaseUUID,
						tenant,
						providerUUID,
						items,
						resourceProfiles,
						record,
						allSet,
					); proofErr != nil {
						volumeErrs = append(volumeErrs, proofErr)
					} else {
						volumesRetained = true
						releaseLiveOnRetainPath = true
						logger.Info("confirmed prior soft-delete completion", "retained", len(record.RetainedVolumeNames))
					}
				}
			}
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
				tenantSnapshot []shared.RetentionEntry
				snapErr        error
			)
			if budget.MaxPartitions > 0 || budget.CountCap > 0 || budget.PerPartCount > 0 {
				tenantSnapshot, snapErr = b.retentionStore.ListByTenant(tenant)
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
			} else if err := b.evictRetentionsToCap(ctx, tenant, budget, partition, tenantSnapshot, leaseUUID); err != nil {
				retentionCapCheckFailedTotal.WithLabelValues(capCheckEvict).Inc()
				logger.Warn("retention cap eviction failed", "tenant", tenant, "error", err)
			}

			if scope, refuse := b.shouldRefuseRetentionWithResourceProfiles(
				leaseUUID, tenant, partition, durableItems, resourceProfiles, budget,
			); refuse {
				rep := b.destroyOnRefuseToRetain(ctx, op, retainCanonical, leaseUUID, tenant, partition, scope, logger)
				if err := rep.err(); err != nil {
					volumeErrs = append(volumeErrs, err)
				}
				claimedLeftBehind = claimedLeftBehind || len(rep.Claimed) > 0
				if len(volumeErrs) == 0 && !claimedLeftBehind {
					// Every byte is gone — the refused stateful volumes here AND any
					// writable-path-only volumes reclaimed before the switch — so release
					// the live allocation. Guard on the OVERALL error count (not just new
					// errors from destroyOnRefuseToRetain): a pre-switch wp-only Destroy
					// failure already in volumeErrs leaves bytes on disk, so keep live
					// counted and let the retry re-attempt rather than under-count
					// (over-admit/ENOSPC). Consistent with the other release-live arms.
					// A refused volume is not an error but leaves bytes on disk just the
					// same, so it holds the reservation for the same reason (ENG-647).
					releaseLiveOnRetainPath = true
				}
				break
			}
			// Retain: the closing lease fits under both caps (count eviction ran
			// above; the disk gate did not refuse).
			retained := make([]string, 0, len(retainCanonical))
			for _, c := range retainCanonical {
				retained = append(retained, retainedName(c))
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
				Items: items, ResourceProfiles: shared.CloneSKUResourceSnapshot(resourceProfiles),
				StackManifest: stackManifest, CallbackURL: callbackURL,
				RetainedVolumeNames: retained, Status: shared.RetentionStatusActive,
				Partition: partition,
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
					if err := b.mutationAdapter().renameVolume(ctx, c, retainedName(c)); err != nil {
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
		rep := op.destroy(ctx, destroySiteDeprovisionDestroy, names...)
		if err := rep.err(); err != nil {
			volumeErrs = append(volumeErrs, err)
		}
		claimedLeftBehind = claimedLeftBehind || len(rep.Claimed) > 0
	}

	if len(volumeErrs) > 0 {
		joinedVolumeErr := errors.Join(volumeErrs...)
		authorityErr := b.terminalStorageAuthorityError()
		if authorityErr != nil ||
			errors.Is(joinedVolumeErr, backendidentity.ErrIdentityDrift) ||
			errors.Is(joinedVolumeErr, backendidentity.ErrMutationOutcomeAmbiguous) {
			// A failed storage attestation after a raw mutation means neither success
			// nor failure is known. Retry exhaustion is not evidence: consuming the
			// close/release finalizers here would make a process-local timeout or mount
			// drift indistinguishable from completed cleanup. Preserve the durable
			// attempt budget unchanged for a freshly-attested process to classify.
			cause := errors.Join(joinedVolumeErr, authorityErr)
			var diagSnap shared.DiagnosticEntry
			b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
				p.ContainerIDs = nil
				p.Status = backend.ProvisionStatusFailed
				p.LastError = fmt.Sprintf("volume cleanup storage outcome is ambiguous: %s", cause)
				p.Reason = backend.ReasonCleanupFailed
				p.Message = backend.MsgCleanupFailed
				diagSnap = leasesm.DiagnosticSnapshot(p)
			})
			logger.Error("volume cleanup storage authority unresolved; preserving close finalizers",
				"error", cause)
			b.persistDiagnostics(diagSnap, nil)
			return fmt.Errorf("volume cleanup storage authority unresolved: %w", cause)
		}

		// Production persists this counter in the close journal before deciding
		// whether to give up, so a crash cannot reset the retry budget. The
		// docker-private field remains a live-process mirror and the fallback for
		// isolated tests whose ephemeral backend has no durable callback store.
		var attempts int
		var entryExists bool
		if hasCloseIntent {
			refreshed, err := b.callbackStore.IncrementCloseCleanupAttempts(closeClaim)
			if err != nil {
				return fmt.Errorf("persist close cleanup attempt: %w", err)
			}
			closeClaim = refreshed
			attempts = refreshed.CleanupAttempts()
			b.provisionsMu.Lock()
			if p, ok := b.provisions[leaseUUID]; ok {
				p.VolumeCleanupAttempts = attempts
				entryExists = true
			}
			b.provisionsMu.Unlock()
		} else {
			b.provisionsMu.Lock()
			if p, ok := b.provisions[leaseUUID]; ok {
				p.VolumeCleanupAttempts++
				attempts = p.VolumeCleanupAttempts
				entryExists = true
			}
			b.provisionsMu.Unlock()
		}
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
			// Persist the abandoned footprint as a reaping tombstone BEFORE terminal
			// settlement releases live, so the bytes hand off live→reaping with no
			// uncounted gap (ENG-376).
			// The write is unconditional now: the record states the footprint's SIZE and
			// authorizes no destroy, so it no longer depends on an ownership table that a
			// degraded store cannot resolve — which is what used to make this hand-off
			// silently drop the accounting altogether (ENG-676).
			accountingErr := b.recordGiveUpLeak(
				leaseUUID, tenant, providerUUID, items, resourceProfiles, logger,
			)
			if accountingErr != nil && hasCloseIntent {
				b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
					p.ContainerIDs = nil
					p.Status = backend.ProvisionStatusFailed
					p.LastError = fmt.Sprintf("cannot establish counted give-up footprint: %v", accountingErr)
					p.Reason = backend.ReasonCleanupFailed
					p.Message = backend.MsgCleanupFailed
					diagSnap = leasesm.DiagnosticSnapshot(p)
				})
				b.persistDiagnostics(diagSnap, nil)
				return fmt.Errorf("record counted give-up footprint: %w", accountingErr)
			}
			if accountingErr != nil {
				// Callback-store-less backends exist only in narrow unit seams. Preserve
				// their historical best-effort terminal behavior without weakening the
				// durable production close path above.
				logger.Warn("give-up footprint has no durable accounting owner", "error", accountingErr)
			}
			b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
				p.ContainerIDs = nil // containers are gone
				p.LastError = fmt.Sprintf("volume cleanup failed after %d attempts: %s",
					attempts, errors.Join(volumeErrs...))
				p.Reason = backend.ReasonVolumeCleanupExhausted
				p.Message = backend.MsgVolumeCleanupExhausted
				diagSnap = leasesm.DiagnosticSnapshot(p)
			})
			// Correlation log so operators can still find the verbose detail (redacted
			// from the tenant-facing Message) by lease_uuid (ENG-508).
			logger.Warn("provision failed (verbose detail retained operator-side)",
				"lease_uuid", leaseUUID, "reason", backend.ReasonVolumeCleanupExhausted, "detail", errors.Join(volumeErrs...))
			// Persist diagnostics before losing the provision so operators
			// can see the final error via the diagnostics API.
			b.persistDiagnostics(diagSnap, nil)

			if hasCloseIntent {
				if err := b.resolveCloseIntent(
					closeClaim,
					backend.CallbackStatusFailed,
					"volume cleanup exhausted",
					false,
					func() error {
						if err := b.refreshRetentionAccountingChecked(); err != nil {
							return fmt.Errorf("refresh counted give-up footprint: %w", err)
						}
						if err := retireReleaseHistory(); err != nil {
							return fmt.Errorf("retire release history after volume-cleanup give-up: %w", err)
						}
						return nil
					},
					func() {
						// The tombstone/active record is the new footprint owner. Refresh
						// and complete live hand-off in the same recovery snapshot as the
						// durable settlement and volatile projection deletion.
						b.provisionStore.Delete(leaseUUID)
						releaseLive()
						retentionTerminalHandoffDone = retaining
					},
				); err != nil {
					return err
				}
			} else {
				if err := retireReleaseHistory(); err != nil {
					return fmt.Errorf("retire release history after volume-cleanup give-up: %w", err)
				}
				b.provisionStore.Delete(leaseUUID)
				releaseLive()
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
			if !hasCloseIntent {
				b.sendLifecycleCallbackWithURL(leaseUUID, resolvedLifecycleCallbackURL, backend.CallbackStatusFailed, "volume cleanup exhausted", false)
			}
			return nil
		}

		// Under the limit — keep provision visible for retry.
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
		return fmt.Errorf("volume cleanup failed: %w", errors.Join(volumeErrs...))
	}

	if hasCloseIntent {
		if err := b.resolveCloseIntent(
			closeClaim,
			backend.CallbackStatusDeprovisioned,
			"",
			volumesRetained,
			func() error {
				if retaining {
					if err := b.refreshRetentionAccountingChecked(); err != nil {
						return fmt.Errorf("refresh retained close accounting: %w", err)
					}
				}
				if err := retireReleaseHistory(); err != nil {
					return fmt.Errorf("retire release history after deprovision: %w", err)
				}
				return nil
			},
			func() {
				b.provisionStore.Delete(leaseUUID)
				switch {
				case retaining:
					if releaseLiveOnRetainPath && !claimedLeftBehind {
						releaseLive()
					}
					retentionTerminalHandoffDone = true
				case !claimedLeftBehind:
					releaseLive()
				}
			},
		); err != nil {
			var diagSnap shared.DiagnosticEntry
			b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
				p.Status = backend.ProvisionStatusFailed
				p.LastError = fmt.Sprintf("close finalization failed: %v", err)
				p.Reason = backend.ReasonCleanupFailed
				p.Message = backend.MsgCleanupFailed
				diagSnap = leasesm.DiagnosticSnapshot(p)
			})
			b.persistDiagnostics(diagSnap, nil)
			return err
		}
	} else {
		if err := retireReleaseHistory(); err != nil {
			var diagSnap shared.DiagnosticEntry
			b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
				p.Status = backend.ProvisionStatusFailed
				p.LastError = fmt.Sprintf("close finalization failed: %v", err)
				p.Reason = backend.ReasonCleanupFailed
				p.Message = backend.MsgCleanupFailed
				diagSnap = leasesm.DiagnosticSnapshot(p)
			})
			b.persistDiagnostics(diagSnap, nil)
			return fmt.Errorf("retire release history after deprovision: %w", err)
		}
		b.provisionStore.Delete(leaseUUID)
		if !retaining && !claimedLeftBehind {
			releaseLive()
		}
	}

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
	if !hasCloseIntent {
		b.sendLifecycleCallbackWithURL(leaseUUID, resolvedLifecycleCallbackURL, backend.CallbackStatusDeprovisioned, "", volumesRetained)
	}
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

// validateCompletedRetention proves that a zero-canonical-volume retain retry
// is the terminal half of an earlier successful soft-delete. The retention row
// is durable intent; the volume inventory is physical completion evidence. A
// caller may report retained=true only when both agree exactly enough to prove
// that every volume promised by the row still exists under this lease's
// retained namespace.
func validateCompletedRetention(
	leaseUUID string,
	tenant string,
	providerUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	record *shared.RetentionEntry,
	physicalVolumes map[string]struct{},
) error {
	if record == nil {
		return errors.New("retention completion proof requires a record")
	}
	if record.OriginalLeaseUUID != leaseUUID {
		return fmt.Errorf(
			"retention record identity mismatch after zero-canonical close: got %q, want %q",
			record.OriginalLeaseUUID,
			leaseUUID,
		)
	}
	if record.Status != shared.RetentionStatusActive {
		return fmt.Errorf(
			"retention record for %s is not active after zero-canonical close: %q",
			leaseUUID,
			record.Status,
		)
	}
	if len(record.RetainedVolumeNames) == 0 {
		return fmt.Errorf("active retention record for %s has no retained volume names", leaseUUID)
	}

	expectedPrefix := retainedVolumePrefix + leaseUUID + "-"
	for _, name := range record.RetainedVolumeNames {
		if !strings.HasPrefix(name, expectedPrefix) || len(name) == len(expectedPrefix) {
			return fmt.Errorf(
				"active retention record for %s has malformed or foreign volume name %q",
				leaseUUID,
				name,
			)
		}
		if _, exists := physicalVolumes[name]; !exists {
			return fmt.Errorf(
				"active retention record for %s references missing retained volume %q",
				leaseUUID,
				name,
			)
		}
	}
	if record.Tenant != tenant || record.ProviderUUID != providerUUID {
		return fmt.Errorf("active retention record for %s has divergent lease identity", leaseUUID)
	}
	if !slices.Equal(record.Items, items) {
		return fmt.Errorf("active retention record for %s has divergent items", leaseUUID)
	}
	if !slices.Equal(record.ResourceProfiles, resourceProfiles) {
		return fmt.Errorf("active retention record for %s has divergent resource snapshot", leaseUUID)
	}
	return nil
}

// purgeReleaseHistory deletes a lease's releases.db history as a required
// deprovision finalizer. It is a no-op when no release store is configured or the
// key is already absent. Shared by
// doDeprovision's terminal-success, give-up, and already-deprovisioned (!exists) paths
// so "delete this lease's release history" has one implementation. The !exists path is
// what lets a deprovision RPC for an already-containerless lease (e.g. a lease_closed
// event delivered after the container was gone) still purge its stale "active" record
// instead of stranding it until the 90-day RemoveOlderThan TTL. (ENG-410)
func (b *Backend) purgeReleaseHistory(leaseUUID string) error {
	if b.releaseStore == nil {
		return nil
	}
	if err := b.releaseStore.Delete(leaseUUID); err != nil {
		return fmt.Errorf("delete release history: %w", err)
	}
	return nil
}

// purgeCloseReleaseHistory consumes the release fence captured by the durable
// close capability. The bbolt transaction is idempotent after deletion but
// refuses to erase a history whose selected release changed after admission.
func (b *Backend) purgeCloseReleaseHistory(claim shared.CloseIntentClaim) error {
	if b.releaseStore == nil {
		if claim.ActiveReleaseVersion() != 0 {
			return fmt.Errorf("durable close has a release fence but no release store")
		}
		return nil
	}
	if err := b.releaseStore.DeleteCloseHistory(
		claim.LeaseUUID(),
		claim.ActiveReleaseVersion(),
		claim.ActiveReleaseDigest(),
	); err != nil {
		return fmt.Errorf("delete release history under close fence: %w", err)
	}
	return nil
}

// recordGiveUpLeak handles a deprovision give-up's abandoned on-disk footprint. When a
// retention store is configured it writes a reaping tombstone recording the SIZE of that
// footprint, so it keeps counting in the admission projection and the retention sweep
// auto-retries the destroy — turning a permanent manual-only leak into a self-healing one.
// PutReaping is idempotent and refuses to clobber an active/restoring record. The
// refusal is accepted only after an exact re-read proves an ACTIVE row already
// counts this footprint; RESTORING is not part of the retained-disk projection.
//
// It records a FACT, never a plan (ENG-676). The give-up releases the lease's pool
// allocation and deletes its provision, so this record is the only thing left counting the
// abandoned bytes — and it must therefore be writable whether or not ownership can be
// resolved right now. The volumes to destroy are re-derived by the finalizer from disk on
// every sweep, so nothing here has to be computed and nothing can fail to be computed.
//
// Give-up tombstones deliberately carry Partition "" (the default bucket): they are
// reaping-from-birth — never eviction-ordered, never restorable, never counted by any
// L2 term — and this is the maximally-degraded path (degraded ⇒ default bucket).
func (b *Backend) recordGiveUpLeak(
	leaseUUID string,
	tenant string,
	providerUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	logger *slog.Logger,
) error {
	retentionLeakedTotal.Inc()
	if b.retentionStore == nil {
		return errors.New("give-up footprint requires a retention store")
	}
	// Items is the whole point of this record and the only field the projection reads:
	// computeReapingDiskMB sums leaseDiskMB(e.Items) over reaping records and
	// refreshRetentionAccounting folds that into SetRetainedDisk. It is the FULL lease item
	// set, not a subset — the give-up abandons the whole footprint.
	//
	// The caller refreshes the retained projection inside the close-snapshot terminal
	// critical section. A failed write or refresh is non-terminal: the durable close,
	// release authority, volatile projection, and live allocation all remain. That
	// overlap is deliberate; give-up must never turn an unreadable retention store into
	// an uncounted on-disk footprint.
	//
	// RetainedVolumeNames is deliberately EMPTY, and that is the fix (ENG-676). This used
	// to enumerate the lease's volumes and partition them through the ownership table so
	// the record could double as a destroy list, which meant that when the table could not
	// be read — a corrupt page, an EIO, the very condition a give-up is most likely to be
	// reached under — it recorded NOTHING at all and returned. The accounting died with the
	// plan: bytes on disk, pool key released, provision deleted, no record, admission
	// over-committing against real disk permanently. Separating the two removes the failure
	// mode rather than handling it. The finalizer re-derives the footprint from disk on
	// every pass (destroyReapingVolumes), so there is nothing to compute here and nothing
	// that can fail to be computed.
	//
	// PutReaping is idempotent and refuses to clobber active/restoring records. A
	// refusal still requires the exact counted-row proof below. (ENG-376)
	//
	rec := shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            tenant,
		ProviderUUID:      providerUUID,
		Items:             items,
		ResourceProfiles:  shared.CloneSKUResourceSnapshot(resourceProfiles),
		Status:            shared.RetentionStatusReaping,
		CreatedAt:         time.Now(),
	}
	ok, err := b.retentionStore.PutReaping(rec)
	if err != nil {
		logger.Error("give-up leak: failed to record reaping tombstone; preserving live close authority", "lease_uuid", leaseUUID, "error", err)
		return fmt.Errorf("write reaping tombstone: %w", err)
	}
	if ok {
		return nil
	}

	// PutReaping returns false for both ACTIVE and RESTORING rows. Only ACTIVE is
	// included in the admission projection; RESTORING deliberately is not, so it
	// cannot take over the live reservation. Re-read and validate the exact row
	// instead of treating every refusal as accounting evidence.
	existing, err := b.retentionStore.Get(leaseUUID)
	if err != nil {
		return fmt.Errorf("read existing give-up accounting record: %w", err)
	}
	if existing == nil {
		return errors.New("reaping tombstone refused without an existing accounting record")
	}
	if existing.OriginalLeaseUUID != leaseUUID {
		return fmt.Errorf(
			"existing give-up accounting record belongs to lease %q, want %q",
			existing.OriginalLeaseUUID,
			leaseUUID,
		)
	}
	if existing.Status != shared.RetentionStatusActive &&
		existing.Status != shared.RetentionStatusReaping {
		return fmt.Errorf("existing give-up record is not counted: status %q", existing.Status)
	}
	if len(existing.Items) == 0 || !slices.Equal(existing.Items, items) {
		return errors.New("existing give-up accounting record has divergent or empty items")
	}
	if len(resourceProfiles) > 0 && !slices.Equal(existing.ResourceProfiles, resourceProfiles) {
		return errors.New("existing give-up accounting record has divergent or missing resource snapshot")
	}
	if existing.Tenant != tenant || existing.ProviderUUID != providerUUID {
		return errors.New("existing give-up accounting record has divergent lease identity")
	}
	logger.Info("give-up leak: an existing counted record owns the footprint; no tombstone written",
		"lease_uuid", leaseUUID,
		"status", existing.Status,
	)
	return nil
}
