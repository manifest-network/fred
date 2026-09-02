package docker

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/util"
)

// retainedVolumePrefix is the namespace soft-deleted volumes are renamed into.
// It keeps the leading "fred-" so listVolumeIDs still enumerates the dir, but the
// distinct "retained" token makes cleanupOrphanedVolumes' expected-set match miss it.
const retainedVolumePrefix = "fred-retained-"

// canonicalVolumeName is the live volume name a provision/restore mounts.
// Every managed volume name in the backend is built here — setupVolBinds, the close
// path, the legacy migration, and the owner table (volume_destroy.go) all call it rather
// than repeating the format string, which is how the orphan reaper's copy came to drift.
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
func (b *Backend) renameIfPresent(ctx context.Context, oldName, newName string) error {
	if err := b.mutationAdapter().renameVolume(ctx, oldName, newName); err != nil {
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
	var reconcileErrs []error
	// One enumeration for the whole boot walk; the reaping arm below is its only user, and
	// it is resolved lazily so a store with no reaping records pays nothing.
	//
	// This walk is longer-lived than a sweep — the restoring arm runs compose teardowns and
	// re-quarantine RENAMES — so it is worth stating why a snapshot survives it. The
	// dangerous shape would be a rename that moves a volume INTO a reaping lease's namespace
	// after the snapshot: the derived set would miss it, every listed name would already be
	// gone, and the record would be dropped while bytes remained. It cannot happen here,
	// because a re-quarantine renames back to the ORIGINAL lease's retained namespace, and a
	// lease cannot be both original-of-a-restore and reaping at once — the store is keyed by
	// OriginalLeaseUUID, so it holds exactly one record per lease. Every other staleness is
	// self-correcting: a volume that appeared is simply not destroyed this pass, and one that
	// vanished makes its destroy an idempotent no-op.
	idx := b.newManagedVolumeIndex()
	for _, e := range all {
		switch e.Status {
		case shared.RetentionStatusActive:
			// Crash after Put before rename: a canonical volume may still be on disk.
			// On rename failure we log and keep going — cleanupOrphanedVolumes
			// independently protects this record's canonical from the reaper.
			for _, retained := range e.RetainedVolumeNames {
				canonical := canonicalFromRetained(retained)
				if rerr := b.renameIfPresent(ctx, canonical, retained); rerr != nil {
					b.logger.Warn("reconcile: re-quarantine of active canonical failed (cleanup protection covers it)",
						"lease_uuid", e.OriginalLeaseUUID, "canonical", canonical, "error", rerr)
					reconcileErrs = append(reconcileErrs, fmt.Errorf(
						"re-quarantine active retention %q volume %q: %w",
						e.OriginalLeaseUUID, canonical, rerr,
					))
				}
			}
		case shared.RetentionStatusRestoring:
			if rerr := b.reconcileRestoring(ctx, e); rerr != nil {
				reconcileErrs = append(reconcileErrs, rerr)
			}
		case shared.RetentionStatusReaping:
			// Finalizer retry at boot: re-attempt destroy of any record stranded
			// reaping by a prior crash/destroy-failure; delete it when confirmed gone.
			b.destroyReapingVolumes(ctx, idx, e.OriginalLeaseUUID)
		}
	}
	return errors.Join(reconcileErrs...)
}

// reconcileRestoring finalizes or rolls back an interrupted/failed restore,
// conservatively (defers to an in-flight restore; generation-CAS rollback).
func (b *Backend) reconcileRestoring(ctx context.Context, e shared.RetentionEntry) error {
	return b.reconcileRestoringWithAuthority(ctx, e)
}

// ensureRestoreDestinationUnowned rejects a new lease generation while a
// restoring source finalizer still owns the destination's canonical volume
// namespace. Provision and Restore use this stricter guard: even a committed
// destination remains an existing lease and must not be replaced through a new
// creation path.
func (b *Backend) ensureRestoreDestinationUnowned(destinationLease string) error {
	if b.retentionStore == nil {
		return nil
	}
	source, err := b.retentionStore.RestoringSourceByDestination(destinationLease)
	if err != nil {
		return fmt.Errorf("read restore destination ownership: %w", err)
	}
	if source != nil {
		return fmt.Errorf(
			"%w: destination lease %q remains owned by a pending restore finalizer",
			backend.ErrInvalidState, destinationLease,
		)
	}
	return nil
}

// ensureRestoreDestinationRestartAvailable admits an identity-preserving
// Restart only
// after the exact active Release proves destination ownership committed and the
// restore operation intent is settled. The source finalizer intentionally stays
// durable while a committed destination is Failed/absent: it carries identity
// across repeated restarts, but must not permanently prevent repair. Update and
// custom-domain changes are intentionally excluded because they create a newer
// topology that the still-original finalizer cannot durably identify after a
// crash; callers must first complete a plain Restart so Ready finalization can
// consume the row.
func (b *Backend) ensureRestoreDestinationRestartAvailable(destinationLease string) error {
	if b.retentionStore == nil {
		return nil
	}
	source, err := b.retentionStore.RestoringSourceByDestination(destinationLease)
	if err != nil {
		return fmt.Errorf("read restore destination ownership: %w", err)
	}
	if source == nil {
		return nil
	}
	committed, err := b.restoreDestinationCommitted(*source)
	if err != nil {
		return fmt.Errorf(
			"%w: validate committed restore destination %q: %w",
			backend.ErrInvalidState, destinationLease, err,
		)
	}
	if !committed {
		return fmt.Errorf(
			"%w: destination lease %q remains owned by an uncommitted restore finalizer",
			backend.ErrInvalidState, destinationLease,
		)
	}
	intent, err := b.currentRestoreIntent(*source)
	if err != nil {
		return fmt.Errorf(
			"%w: read restore operation settlement for %q: %w",
			backend.ErrInvalidState, destinationLease, err,
		)
	}
	if intent != nil {
		return fmt.Errorf(
			"%w: destination lease %q restore operation is not settled",
			backend.ErrInvalidState, destinationLease,
		)
	}
	return nil
}

func (b *Backend) reconcileRestoringWithAuthority(
	ctx context.Context,
	e shared.RetentionEntry,
) error {
	if e.Status != shared.RetentionStatusRestoring ||
		e.OriginalLeaseUUID == "" ||
		e.NewLeaseUUID == "" ||
		e.OriginalLeaseUUID == e.NewLeaseUUID ||
		e.Generation <= 0 {
		return fmt.Errorf(
			"invalid restoring authority: source=%q destination=%q status=%q generation=%d",
			e.OriginalLeaseUUID, e.NewLeaseUUID, e.Status, e.Generation,
		)
	}
	// The entire decision and mutation are one per-destination critical section,
	// not just the Ready finalizer. In particular, a sweep that snapshots Failed
	// must not tear down/re-quarantine outside the fence while Restart makes the
	// same destination Ready: that stale rollback could move a live volume after
	// success ownership was finalized. CommandFence is a ref-counted keyed
	// registry, so a slow Docker teardown blocks only this exact lease.
	unlockCommand := b.commandFence.Lock(e.NewLeaseUUID)
	defer unlockCommand()

	// The row passed by reconcileRetentions came from a batch snapshot taken
	// before this per-destination fence. Re-establish exact durable authority
	// before any release write, teardown, rename, callback settlement, or source
	// handback. A worker or an earlier sweep may already have consumed it while
	// this goroutine waited for the command fence.
	current, err := b.retentionStore.Get(e.OriginalLeaseUUID)
	if err != nil {
		return fmt.Errorf("re-read restore source finalizer %q: %w", e.OriginalLeaseUUID, err)
	}
	if current == nil || current.Status != shared.RetentionStatusRestoring ||
		current.NewLeaseUUID != e.NewLeaseUUID || current.Generation != e.Generation {
		return nil
	}
	e = *current

	intent, err := b.currentRestoreIntent(e)
	if err != nil {
		return fmt.Errorf("read exact restore intent for destination %q: %w", e.NewLeaseUUID, err)
	}

	b.provisionsMu.RLock()
	p, live := b.provisions[e.NewLeaseUUID]
	var status backend.ProvisionStatus
	var recordedIDs []string
	var liveItems []backend.LeaseItem
	if live {
		status = p.Status
		liveItems = slices.Clone(p.Items)
		// Snapshot (not alias) under the lock, mirroring doDeprovision. Usually EMPTY
		// here — Restore reserves the provision with no ContainerIDs and only the
		// success paths fill them in — which is precisely why the teardown below
		// re-discovers rather than trusting this (ENG-647). It is non-empty for a
		// provision recoverState rebuilt from live containers, so it is still worth
		// passing.
		recordedIDs = slices.Clone(p.ContainerIDs)
	}
	b.provisionsMu.RUnlock()
	// Restore terminal handlers publish Ready/Failed before synchronously
	// persisting their callback. Check actor activity only after both the journal
	// and provision snapshots: false now proves that a handler which published
	// either terminal status has finished its callback attempt. InboxDepth also
	// covers an accepted message that has been enqueued but not started yet.
	if b.leaseActorProcessingOrQueued(e.NewLeaseUUID) {
		return nil
	}

	if live && status == backend.ProvisionStatusReady {
		// A Ready projection is exact live ownership evidence. Commit (or verify)
		// its active Release, settle an operation whose actor-side callback
		// persistence failed, and only then consume the source finalizer. If either
		// write fails, the finalizer remains a level-triggered retry owner.
		if err := b.ensureRestoredReleaseStrict(e.NewLeaseUUID, &e, liveItems); err != nil {
			return fmt.Errorf("finalize Ready restore destination %q: %w", e.NewLeaseUUID, err)
		}
		if intent != nil {
			if _, err := b.callbackStore.ResolveOperationIntent(
				*intent, backend.CallbackStatusSuccess, "",
			); err != nil {
				return fmt.Errorf("settle committed restore intent for %q: %w", e.NewLeaseUUID, err)
			}
		}
		if err := b.deleteRestoreFinalizerStrict(e.NewLeaseUUID, &e); err != nil {
			return fmt.Errorf("delete committed restore finalizer for %q: %w", e.NewLeaseUUID, err)
		}
		return nil
	}
	if live && status != backend.ProvisionStatusFailed {
		// A non-terminal projection may have a worker in the gap after writing its
		// Release but before enqueueing the actor's terminal message. The actor can
		// be momentarily idle in that gap, so status—not Release existence—is the
		// authoritative defer signal.
		return nil
	}

	committed, err := b.restoreDestinationCommitted(e)
	if err != nil {
		return fmt.Errorf("validate restore commit for destination %q: %w", e.NewLeaseUUID, err)
	}
	if committed {
		// The active Release is the write-ahead commit marker. Containers may have
		// failed or disappeared after that commit; that is a destination runtime
		// failure, never authority to hand the adopted bytes back to the source.
		// Keep the source finalizer while the destination is non-Ready because it is
		// the durable tenant/provider identity needed to reconstruct Failed safely
		// across repeated restarts. A successful Restart/Update (or Close) later
		// consumes it through finalizeRestoredLeaseStrict.
		if intent != nil {
			if _, err := b.callbackStore.ResolveOperationIntent(
				*intent, backend.CallbackStatusSuccess, "",
			); err != nil {
				return fmt.Errorf("settle committed restore intent for %q: %w", e.NewLeaseUUID, err)
			}
		}
		return nil
	}

	// Orphaned (crash/failed): tear down any orphaned project, re-quarantine the
	// adopted volumes back to the retained namespace, then CAS the record to active.
	//
	// The teardown is a PRECONDITION for everything below it, not a best-effort
	// courtesy, so a failure ends the pass (ENG-647). Two reasons, both fatal:
	//   - The re-quarantine renames move the volume dirs back into the retained
	//     namespace, and a surviving container holds them by INODE, so it would go on
	//     writing into data the record then advertises as frozen.
	//   - Reverting the record and dropping the provision would strand the containers
	//     where nothing can see them: processOrphan only walks ListProvisions (which
	//     ranges b.provisions, the map we would have just deleted from), and
	//     cleanupOrphanedVolumes enumerates fred's bind-mount tree, never Docker's
	//     anonymous-volume store. Their anonymous volumes then accumulate forever
	//     (ENG-372).
	// So: no partial rollback. Leave the record restoring, keep the provision and its
	// pool allocation, and let the next sweep/boot retry — the same shape as the
	// re-quarantine failure below, and the same finalizer contract the Ready arm above
	// honors via finalizeRestoredLease (ENG-523). The wait is safe: a restoring record
	// is not reapable (ListExpired/MarkReapingIfExpired both require ACTIVE) and
	// cleanupOrphanedVolumes protects its canonicals. It is NOT time-bounded, though —
	// that same expiry exemption means the tenant cannot re-request the restore
	// (ClaimForRestoreWithAuthority refuses a restoring record) until a sweep gets a clean teardown,
	// so a sustained failure here is an operator signal, not a self-healing state.
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if _, derr := b.teardownLeaseContainers(ctx, e.NewLeaseUUID, recordedIDs, stopTimeout,
		teardownOpRestoreReconcile, b.logger.With("lease_uuid", e.NewLeaseUUID)); derr != nil {
		b.logger.Warn("reconcile: teardown failed; leaving record restoring for the next sweep",
			"lease_uuid", e.OriginalLeaseUUID, "new_lease_uuid", e.NewLeaseUUID, "error", derr)
		return fmt.Errorf("reconcile restoring retention %q teardown: %w", e.OriginalLeaseUUID, derr)
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
		if rerr := b.renameIfPresent(ctx, newCanonical, retained); rerr != nil {
			failed = true
		}
	}
	if failed {
		b.logger.Warn("reconcile: re-quarantine rename failed; leaving record restoring for next startup",
			"lease_uuid", e.OriginalLeaseUUID, "new_lease_uuid", e.NewLeaseUUID)
		return fmt.Errorf("reconcile restoring retention %q: re-quarantine remains incomplete",
			e.OriginalLeaseUUID)
	}
	// Restore's Create path applies the destination tier's quota to each adopted
	// volume. A failed promotion therefore leaves a larger physical quota than
	// the immutable source record accounts for. Restore the exact source quota
	// before handing authority back to that record; if usage no longer fits, or
	// either measurement/application is uncertain, keep both the restoring
	// finalizer and the live reservation. That is over-counted but cannot admit
	// unaccounted bytes.
	resourceProfiles, err := b.restoreRetainedVolumeQuotas(ctx, &e)
	if err != nil {
		b.logger.Error("reconcile: unable to restore source volume quotas; leaving record restoring",
			"lease_uuid", e.OriginalLeaseUUID,
			"new_lease_uuid", e.NewLeaseUUID,
			"error", err,
		)
		return fmt.Errorf("reconcile restoring retention %q quotas: %w", e.OriginalLeaseUUID, err)
	}
	// Once teardown, re-quarantine, and source-quota proof are complete, this
	// destination can no longer succeed. Settle its exact failed operation before
	// handing the durable row back to Active. A callback-store failure therefore
	// leaves Restoring + the live reservation as a level-triggered retry vehicle;
	// moving this after the CAS would strand an Existing intent until restart.
	if err := b.settleRolledBackRestoreIntent(e, intent); err != nil {
		return fmt.Errorf("settle rolled-back restore intent for %q: %w", e.NewLeaseUUID, err)
	}
	// Derive the destination allocation ids using the same
	// {newLease}-{svc}-{idx} scheme Restore used for TryAllocateAdoptAll.
	var liveIDs []string
	for _, item := range e.Items {
		for i := range item.Quantity {
			liveIDs = append(liveIDs, fmt.Sprintf("%s-%s-%d", e.NewLeaseUUID, item.ServiceName, i))
		}
	}
	ok, err := b.revertRestoreSourceWithAccounting(&e, e.NewLeaseUUID, resourceProfiles, liveIDs)
	if err != nil {
		b.logger.Error("reconcile: revert restoring->active failed", "lease_uuid", e.OriginalLeaseUUID, "error", err)
		return fmt.Errorf("reconcile restoring retention %q finalizer: %w", e.OriginalLeaseUUID, err)
	}
	if ok {
		b.removeProvision(e.NewLeaseUUID)
		return nil
	}
	return fmt.Errorf("reconcile restoring retention %q lost generation %d authority",
		e.OriginalLeaseUUID, e.Generation)
}

// settleRolledBackRestoreIntent retries failure settlement when a prior
// pre-acceptance Resolve failed. Its caller has already proved teardown,
// re-quarantine, and source quotas, so the operation is definitively failed and
// no worker can later publish the destination Ready. It runs before the source
// handback CAS so a callback-store failure leaves the Restoring row available
// for the next level-triggered sweep. An absent matching fence is the common
// no-op path.
func (b *Backend) settleRolledBackRestoreIntent(
	e shared.RetentionEntry,
	intent *shared.OperationIntentClaim,
) error {
	if intent == nil {
		return nil
	}
	if b.callbackStore == nil {
		return errors.New("callback store is required to settle rolled-back restore intent")
	}
	if intent.SourceGeneration() != e.Generation {
		return fmt.Errorf(
			"pending restore intent generation %d differs from rolled-back source generation %d",
			intent.SourceGeneration(), e.Generation,
		)
	}
	if _, err := b.callbackStore.ResolveOperationIntent(
		*intent, backend.CallbackStatusFailed, interruptedOperationFailure,
	); err != nil {
		return fmt.Errorf("resolve interrupted restore intent: %w", err)
	}
	return nil
}

func (b *Backend) leaseActorProcessingOrQueued(leaseUUID string) bool {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	actor, exists := b.actors[leaseUUID]
	return exists && (actor.CurrentMessageStart() != 0 || actor.InboxDepth() != 0)
}

// currentRestoreIntent re-reads operation authority while the caller holds the
// destination command fence. Batch snapshots are not admissible here: a restore
// can create or settle its intent while a retention sweep waits for that fence.
// Any claim that touches only one side (or a different generation) is conflicting
// durable authority and therefore fails closed.
func (b *Backend) currentRestoreIntent(e shared.RetentionEntry) (*shared.OperationIntentClaim, error) {
	if b.callbackStore == nil {
		return nil, nil
	}
	claims, err := b.callbackStore.ListOperationIntents()
	if err != nil {
		return nil, err
	}
	var exact *shared.OperationIntentClaim
	for i := range claims {
		claim := claims[i]
		touchesSource := claim.SourceLeaseUUID() == e.OriginalLeaseUUID
		touchesDestination := claim.LeaseUUID() == e.NewLeaseUUID
		if !touchesSource && !touchesDestination {
			continue
		}
		if claim.Kind() != shared.OperationIntentRestore ||
			!touchesSource || !touchesDestination ||
			claim.SourceGeneration() != e.Generation {
			return nil, fmt.Errorf(
				"%s intent for lease %q conflicts with source %q destination %q generation %d",
				claim.Kind(), claim.LeaseUUID(), e.OriginalLeaseUUID, e.NewLeaseUUID, e.Generation,
			)
		}
		if exact != nil {
			return nil, fmt.Errorf("multiple operation intents own restore destination %q", e.NewLeaseUUID)
		}
		if err := b.validateRestoreIntentAuthority(claim, e); err != nil {
			return nil, err
		}
		exact = &claim
	}
	return exact, nil
}

func (b *Backend) validateRestoreIntentAuthority(
	claim shared.OperationIntentClaim,
	e shared.RetentionEntry,
) error {
	if claim.Backend() != b.Name() || claim.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"restore intent belongs to backend %q storage %s, not backend %q storage %s",
			claim.Backend(), claim.BackendStorageID(), b.Name(), b.storageIdentity,
		)
	}
	if claim.Tenant() != e.Tenant || claim.ProviderUUID() != e.ProviderUUID {
		return errors.New("restore intent tenant/provider differs from source finalizer authority")
	}
	if !slices.Equal(claim.Items(), e.DestinationItems) ||
		!slices.Equal(claim.EffectiveItems(), e.DestinationItems) ||
		!slices.Equal(claim.ResourceProfiles(), e.DestinationResourceProfiles) {
		return errors.New("restore intent topology or resource profiles differ from source finalizer authority")
	}
	if e.DestinationCallbackURL != "" || e.DestinationLifecycleCallbackURL != "" {
		if claim.CallbackURL() != e.DestinationCallbackURL ||
			claim.LifecycleCallbackURL() != e.DestinationLifecycleCallbackURL {
			return errors.New("restore intent callback pair differs from source finalizer authority")
		}
	}
	if e.DestinationOperationID != "" && claim.OperationID() != e.DestinationOperationID {
		return errors.New("restore intent operation ID differs from source finalizer authority")
	}
	if e.StackManifest == nil {
		return errors.New("restore source finalizer has no destination manifest")
	}
	manifestBytes, err := json.Marshal(e.StackManifest)
	if err != nil {
		return fmt.Errorf("marshal restore source finalizer manifest: %w", err)
	}
	if !bytes.Equal(claim.Manifest(), manifestBytes) {
		return errors.New("restore intent manifest differs from source finalizer authority")
	}
	expectedHealthServices := make([]string, 0, len(e.StackManifest.Services))
	for service, serviceManifest := range e.StackManifest.Services {
		if serviceManifest != nil && serviceManifest.HasActiveHealthCheck() {
			expectedHealthServices = append(expectedHealthServices, service)
		}
	}
	slices.Sort(expectedHealthServices)
	if !slices.Equal(claim.HealthCheckServices(), expectedHealthServices) {
		return errors.New("restore intent health-check authority differs from source finalizer manifest")
	}
	return nil
}

// restoreDestinationCommitted reports whether the exact destination generation
// was durably committed before its source finalizer could be consumed. A
// mismatching active Release is not absence: it is conflicting ownership and
// must fail closed rather than authorizing rollback of possibly-live bytes.
func (b *Backend) restoreDestinationCommitted(e shared.RetentionEntry) (bool, error) {
	if b.releaseStore == nil {
		return false, errors.New("release store is required")
	}
	active, err := b.releaseStore.LatestActive(e.NewLeaseUUID)
	if err != nil {
		return false, fmt.Errorf("read active destination release: %w", err)
	}
	if active == nil {
		return false, nil
	}
	matches, err := restoreReleaseMatchesAuthority(active, e)
	if err != nil {
		return false, err
	}
	if !matches {
		return false, errors.New("active destination release differs from restore finalizer authority")
	}
	return true, nil
}

func restoreReleaseMatchesAuthority(
	active *shared.Release,
	e shared.RetentionEntry,
) (bool, error) {
	if active == nil {
		return false, nil
	}
	if e.StackManifest == nil {
		return false, errors.New("restore source finalizer has no destination manifest")
	}
	manifestBytes, err := json.Marshal(e.StackManifest)
	if err != nil {
		return false, fmt.Errorf("marshal restore destination manifest: %w", err)
	}
	switch {
	case e.DestinationOperationID == "":
		if active.OperationID != "" || active.RuntimeAuthority != nil {
			return false, errors.New("legacy restore finalizer cannot own a typed destination release")
		}
	case !e.DestinationOperationID.Valid() || !active.OperationID.Valid() ||
		active.OperationID != e.DestinationOperationID || active.RuntimeAuthority == nil:
		return false, errors.New("active destination runtime authority has no exact valid restore operation ID")
	case !releaseRuntimeAuthorityMatchesRetention(active.RuntimeAuthority, e):
		return false, errors.New("active destination runtime authority differs from restore finalizer")
	}
	return bytes.Equal(active.Manifest, manifestBytes) &&
		slices.Equal(active.Items, e.DestinationItems) &&
		slices.Equal(active.ResourceProfiles, e.DestinationResourceProfiles), nil
}

func releaseRuntimeAuthorityMatchesRetention(
	authority *shared.ReleaseRuntimeAuthority,
	e shared.RetentionEntry,
) bool {
	if authority == nil ||
		authority.OperationID() != e.DestinationOperationID ||
		authority.Tenant() != e.Tenant ||
		authority.ProviderUUID() != e.ProviderUUID {
		return false
	}
	resolvedCallbackURL, resolvedLifecycleCallbackURL, err :=
		backend.ResolveMaintenanceCallbackURLs(
			e.DestinationCallbackURL,
			e.DestinationLifecycleCallbackURL,
			authority.LifecycleCallbackURL(),
		)
	return err == nil &&
		resolvedCallbackURL == authority.CallbackURL() &&
		resolvedLifecycleCallbackURL == authority.LifecycleCallbackURL()
}

// maxRetentionEvictionsPerClose is the per-pass batch rail: a cap reduction
// (a budget edit/removal, a config rollback) can otherwise schedule hundreds of
// SYNCHRONOUS volume destroys inside one close. Bounded eviction converges over
// subsequent closes while the count cap temporarily overshoots — the
// established ceilings-not-gates posture (count caps are DoS ceilings, not
// exact allocation gates).
const maxRetentionEvictionsPerClose = 32

const (
	evictLevelAggregate = "aggregate" // L1: per-tenant count cap
	evictLevelPartition = "partition" // L2: per-partition sub-cap
)

// evictRetentionsToCap enforces the two count caps for a CLOSING lease against
// the caller's tenant snapshot, oldest-first, tenant-local. L2 first: within the
// closing lease's partition — the "" default bucket is NEVER L2-capped (I6) —
// down to PerPartCount-1. L1 second: across ALL of the tenant's partitions, down
// to CountCap-1; L1 always runs on the whole tenant set regardless of labels
// (partitions sub-divide a tenant's budget, they never raise it). Count caps
// never refuse — they evict; only the disk caps refuse (breachRetentionCaps).
//
// excludeLease is the closing lease's OriginalLeaseUUID: it is skipped entirely
// at BOTH levels (never counted, sorted, or evicted). On a soft-delete retry the
// closing lease may already have its own ACTIVE record from a prior attempt;
// without this exclusion the cap eviction could destroy the lease's own
// in-progress record = data loss.
//
// The snapshot is the caller's ListByTenant output (shared with boundPartition),
// never re-read here: the close path takes exactly one tenant snapshot. Each
// successfully-marked record is pruned from the in-memory snapshot between passes
// so an L2 eviction also counts toward L1; the disk gate re-reads the store
// afterwards, so it sees the post-eviction state. refreshRetentionAccounting runs
// only when a pass engaged.
func (b *Backend) evictRetentionsToCap(ctx context.Context, tenant string, budget retentionBudget,
	partition string, snapshot []shared.RetentionEntry, excludeLease string) error {
	if b.retentionStore == nil || tenant == "" || (budget.CountCap <= 0 && budget.PerPartCount <= 0) {
		return nil
	}
	var active []shared.RetentionEntry
	for _, e := range snapshot {
		if e.OriginalLeaseUUID == excludeLease {
			continue // never evict the closing lease's own record
		}
		if e.Status == shared.RetentionStatusActive {
			active = append(active, e)
		}
	}
	// Deterministic total order: oldest-first, equal CreatedAt broken by
	// ascending UUID. Given the same store state the evicted set is a pure
	// function — previously equal-timestamp order was unspecified.
	sort.SliceStable(active, func(i, j int) bool {
		if !active[i].CreatedAt.Equal(active[j].CreatedAt) {
			return active[i].CreatedAt.Before(active[j].CreatedAt)
		}
		return active[i].OriginalLeaseUUID < active[j].OriginalLeaseUUID
	})

	// attempted becomes true once either pass ENGAGES (commits evictions). Refresh
	// the cached pool projection / gauges from a defer so it runs on every return
	// path: a pass can mark records ACTIVE→REAPING and then return a store error on
	// a later record, and those already-committed transitions must be reflected
	// rather than lagging until the next close or the periodic sweep.
	attempted := false
	defer func() {
		if attempted {
			b.refreshRetentionAccounting()
		}
	}()
	if budget.PerPartCount > 0 && partition != "" { // the "" default bucket is never L2-capped (I6)
		var part []shared.RetentionEntry
		for _, e := range active {
			if e.Partition == partition {
				part = append(part, e)
			}
		}
		marked, passRan, err := b.evictOldest(ctx, part, evictLevelPartition, budget.PerPartCount)
		attempted = attempted || passRan // engaged even if a later record errored
		if err != nil {
			return err
		}
		if len(marked) > 0 {
			// Prune the L2-evicted records from the snapshot so they count toward
			// L1 too (a per-partition eviction is also an aggregate eviction). This
			// is the standard in-place filter; pruned[i] is only ever written at an
			// index already read from active.
			pruned := active[:0]
			for _, e := range active {
				if _, gone := marked[e.OriginalLeaseUUID]; !gone {
					pruned = append(pruned, e)
				}
			}
			active = pruned
		}
	}
	if budget.CountCap > 0 {
		_, passRan, err := b.evictOldest(ctx, active, evictLevelAggregate, budget.CountCap)
		attempted = attempted || passRan // engaged even if a later record errored
		if err != nil {
			return err
		}
	}
	return nil
}

// evictOldest marks-reaping and destroys the oldest records of `ordered`
// (already sorted oldest-first) down to the level's cap, bounded by the
// per-close batch rail. Per-record protocol: MarkReapingIfActive is the atomic
// active→reaping CAS (TOCTOU-safe — a record concurrently claimed for restore
// returns ok=false and is skipped, no compensation; under-evict is the safe
// direction). The record is the finalizer tombstone: removed from the active cap
// set immediately (making room) but still counted in the admission pool until
// its volumes are confirmed gone. The eviction counter fires AFTER the ok-guard
// and independent of the destroy outcome — an increment means "evicted from the
// active set (marked reaping)", not "destroyed" — so a concurrently
// restore-claimed record (ok=false, skipped) is never counted (ENG-407) — then
// destroyReapingVolumes runs the finalizer (ENG-376). L1 evictions bump
// retentionEvictedTotal (its deployed per-tenant meaning); L2 bump
// retentionPartitionEvictedTotal. capValue is the configured cap for the level,
// carried into the WARN. Returns the marked UUIDs (so the caller can prune its
// snapshot between passes) and whether the pass engaged at all.
func (b *Backend) evictOldest(ctx context.Context, ordered []shared.RetentionEntry, level string, capValue int) (map[string]struct{}, bool, error) {
	keep := capValue - 1 // count caps make room for one more: keep = cap-1
	toEvict := len(ordered) - keep
	if toEvict <= 0 {
		return nil, false, nil
	}
	if toEvict > maxRetentionEvictionsPerClose {
		b.logger.Warn("retention eviction batch rail engaged: capping evictions this close; remainder converges on subsequent closes",
			"level", level, "backlog", toEvict, "batch", maxRetentionEvictionsPerClose)
		toEvict = maxRetentionEvictionsPerClose
	}
	marked := make(map[string]struct{}, toEvict)
	// ONE enumeration for the whole batch. This loop runs synchronously inside a lease
	// close and evicts up to maxRetentionEvictionsPerClose records, so a per-record
	// os.ReadDir of the volume root would put O(batch x volumes) directory work on a
	// tenant-facing path.
	idx := b.newManagedVolumeIndex()
	for i := 0; i < toEvict; i++ {
		e := ordered[i]
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap",
			"tenant", e.Tenant, "lease_uuid", e.OriginalLeaseUUID, "level", level, "cap", capValue,
			"partition", shared.TruncatePartitionRaw(e.Partition))
		// The names this returns are the record's stored list; the finalizer derives its
		// own from disk (destroyReapingVolumes), so they are deliberately discarded. The
		// CAS itself is what matters here — active→reaping must be atomic so the record is
		// never deleted before its volumes are confirmed gone.
		_, ok, merr := b.retentionStore.MarkReapingIfActive(e.OriginalLeaseUUID)
		if merr != nil {
			return marked, true, merr
		}
		if !ok {
			continue // concurrently claimed for restore (or already gone) — skip
		}
		if level == evictLevelAggregate {
			retentionEvictedTotal.Inc() // deployed meaning: per-tenant aggregate (L1) evictions
		} else {
			retentionPartitionEvictedTotal.Inc()
		}
		marked[e.OriginalLeaseUUID] = struct{}{}
		b.destroyReapingVolumes(ctx, idx, e.OriginalLeaseUUID)
	}
	return marked, true, nil
}

// destroyReapingVolumes destroys the on-disk footprint of a reaping record and, ONLY if
// all destroys succeed and none had to be skipped, Delete()s the record. Returns true iff
// the record was fully reaped (deleted). On any destroy failure it LEAVES the record
// reaping (the finalizer retry) and bumps retentionLeakedTotal — the footprint stays
// counted and the next sweep retries. Idempotent: an already-gone volume no-ops, and a
// Delete failure leaves the record reaping for a later retry (no under-count). (ENG-376)
//
// THE DESTROY SET IS DERIVED, NOT STORED (ENG-676). A reaping record states one fact —
// this lease's footprint is abandoned, and Items says how big it is, which is what
// computeReapingDiskMB sums into the admission projection. It does NOT carry the authority
// to destroy anything. The set of volumes to remove is re-derived here on every pass, from
// the two sources that actually know: the lease's namespace on disk
// (fred-{lease}-* and fred-retained-{lease}-*) intersected with the ownership table, which
// is the same "destroy only what nothing claims" question cleanupOrphanedVolumes asks
// globally — this is that question scoped to one lease.
//
// Deriving rather than replaying a stored list is what makes the accounting survive a
// degraded store. When the writer could not resolve ownership it used to record NOTHING,
// and since the record was both the destroy plan and the accounting unit, refusing to
// write the plan silently discarded the accounting too: bytes on disk, no pool key, no
// record, admission over-committing against real disk permanently (ENG-676). Now the
// writer records the fact unconditionally and never computes a plan, so there is no
// failure mode in which the fact is lost. It is also what this repo already does
// everywhere else — level-triggered reconciliation, deriving the work from current state
// rather than replaying a list captured when the state was last legible.
//
// It subsumes the ENG-659 hazard instead of mitigating it. A tombstone is persisted in
// bbolt and outlives the process, so a provider upgrading from a pre-ENG-647 build carries
// records written before the write-time guard existed — records that can name another
// lease's adopted data (while a restore of A into B is in flight, A's data wears B's
// canonical names). Those stored names are now never consulted at all, by any producer or
// any vintage, so there is nothing left to re-check.
//
// Every destroy still goes through volumeOp.destroy (volume_destroy.go), which resolves
// ownership from the live provision map plus the retention store and re-checks the live
// claim under the volume's stripe (ENG-681). This function's job is to translate that
// primitive's per-name verdicts into the record's lifecycle, which is the part only the
// finalizer knows: refused means keep the record, failed means keep it and count a leak,
// all-gone means Delete.
//
// Fail-safe on an unprovable claim set OR an unreadable volume root: destroy NOTHING this
// pass and KEEP the record, mirroring cleanupOrphanedVolumes' "skip orphan destruction
// this run" (recover.go). We cannot tell this record's own leak from another lease's
// adopted data, and only one of those two mistakes is reversible. Waiting costs nothing:
// the record IS the retry vehicle, so the next sweep re-attempts without a reboot. The
// record is dropped only on the positive fact that the footprint is gone.
func (b *Backend) destroyReapingVolumes(ctx context.Context, idx *managedVolumeIndex, orig string) bool {
	logger := b.logger.With("lease_uuid", orig)
	if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
		// A prior raw mutation retained typed recovery evidence and withdrew this
		// Backend instance. In particular, do not let a fresh inventory hide an
		// XFS delete-stage and turn "final name absent" into permission to delete
		// the reaping record. Startup recovery in a fresh process owns the next
		// classification.
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		logger.Error("reaping: backend storage recovery is pending; keeping the record", "error", authorityErr)
		return false
	}
	// owner "" — a tombstone is a scheduled destroy, not an assertion of ownership, so it
	// is entitled to exactly the volumes NOTHING claims. That also refuses a name a LIVE
	// provision holds, which is reachable when a tombstoned lease is later re-provisioned
	// (chain-ACTIVE, no provision → the reconciler re-provisions it, and the sweep would
	// otherwise reap the fresh volume out from under it).
	op := b.volumeOp("", logger)

	names, derr := idx.footprint(orig)
	if derr != nil {
		// Cannot enumerate the volume root, so "no volumes" and "cannot see the volumes"
		// are indistinguishable — and one of those two readings deletes the record that is
		// both the retry vehicle and the accounting for bytes still on disk. Same reason
		// and same reason-label as an unreadable claim table below.
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		logger.Error("reaping: cannot enumerate volumes; destroyed nothing this pass (record stays reaping for retry)",
			"error", derr)
		return false
	}
	if len(names) == 0 {
		// Positive fact: the volume root is readable and the lease's namespace is empty, so
		// the footprint really is gone and the record has nothing left to account for. This
		// is also the terminal state of a legacy stateless record, which never named a
		// volume in the first place.
		if delErr := b.retentionStore.Delete(orig); delErr != nil {
			logger.Warn("reaping: footprint already gone but record delete failed; next sweep retries", "error", delErr)
			return false
		}
		return true
	}

	rep := op.destroy(ctx, destroySiteReaping, names...)

	if len(rep.Unproven) > 0 {
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		b.logger.Error("reaping: ownership unprovable; destroyed nothing this pass (record stays reaping for retry)",
			"lease_uuid", orig, "error", rep.err())
		return false
	}
	// Count the refusal by HOW it resolves, not merely that it happened — the deployed
	// stuck-reaping alert triages on this label, and the two paths need different
	// operator action. Still one increment per reason per attempt (never per volume), so
	// the series stays summable with the rest of reapSkipReasons.
	var restoreHeld, ownerHeld bool
	for _, name := range rep.Claimed {
		switch rep.ClaimedBy[name].kind {
		case claimAdopted, claimRestoreSrc:
			// An in-flight restore adopted another lease's retained data under this name.
			// Destroying it is unrecoverable loss and kills that lease's restore.
			// reconcileRestoring re-quarantines it back to fred-retained-* once its
			// rollback can complete, after which the name is absent and the destroy is
			// the idempotent no-op that finally drops the record.
			restoreHeld = true
		default:
			// A live provision (or another lease's retained record) holds it: the
			// tombstone outlived its lease and the reconciler re-provisioned it. Nothing
			// to unblock — this clears when that lease is next closed cleanly, and the
			// record is correctly kept in the meantime.
			ownerHeld = true
		}
	}
	if restoreHeld {
		retentionReapSkipsTotal.WithLabelValues(reapSkipRestoreClaimed).Inc()
	}
	if ownerHeld {
		retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed).Inc()
	}
	skipped := len(rep.Claimed)
	if len(rep.Errs) > 0 {
		// Checked before the skip arm so a pass that both skipped and failed counts once,
		// as the leak — the more actionable of the two.
		retentionLeakedTotal.Inc()
		b.logger.Warn("reaping: volume(s) still on disk; record left reaping for retry (footprint stays counted)",
			"lease_uuid", orig, "error", rep.err())
		return false
	}
	if skipped > 0 {
		// Deliberate, not a failure — no retentionLeakedTotal (see the metric's doc). The
		// record is NOT deleted: its name list IS the destroy list, so a listed name still
		// on disk means dropping the record would drop both the retry vehicle and the
		// reaping projection that counts those bytes — an under-count, the exact ENG-376
		// invariant. Nor is the list narrowed to the skipped names: Destroy no-ops on an
		// already-gone name, so re-attempting the whole list next sweep is free and avoids
		// a CAS-less rewrite of a tombstone another path may be racing.
		//
		// The message follows the same split as the counters above, for the same reason:
		// the two holds resolve differently, and telling an operator to wait for a
		// rollback that does not exist is how a live lease's data gets reclaimed by hand.
		msg := "reaping: record kept (restore-claimed volume(s) left on disk); the restore's rollback resolves it"
		switch {
		case restoreHeld && ownerHeld:
			msg = "reaping: record kept (volume(s) held by both an in-flight restore and a live provision); nothing to reclaim"
		case ownerHeld:
			msg = "reaping: record kept (volume(s) held by a live provision); this clears when that lease is next closed cleanly — do NOT reclaim by hand"
		}
		b.logger.Warn(msg, "lease_uuid", orig, "skipped", skipped, "names", len(names))
		return false
	}
	// CONFIRM BEFORE DROPPING THE RECORD. Every destroy above reported success, but a
	// destroy is an os.RemoveAll that deliberately treats an already-absent path as done —
	// so "all succeeded" is also what a vanished mount looks like. If the root went away
	// after the enumeration, each name was removed from a filesystem that is no longer
	// there, and deleting the record here would drop the only accounting for volumes that
	// come back with the mount (ENG-687).
	//
	// A fresh index is the confirmation: it re-reads the root, which re-applies the
	// absent-root and identity guards, and re-derives this lease's footprint. Empty and
	// error-free is the positive fact this record's deletion has always needed; anything
	// else keeps it for the next sweep, which costs one retry and no data. Deliberately not
	// the op's cached index — reusing the snapshot we are trying to check would confirm
	// nothing.
	confirm := b.newManagedVolumeIndex()
	switch remaining, verr := confirm.footprint(orig); {
	case verr != nil:
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		logger.Error("reaping: destroys reported success but the footprint could not be re-confirmed; "+
			"keeping the record (is the volume root still mounted?)", "error", verr)
		return false
	case len(remaining) > 0:
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		logger.Error("reaping: destroys reported success but volumes are still present; keeping the record",
			"remaining", len(remaining))
		return false
	}

	if derr := b.retentionStore.Delete(orig); derr != nil {
		logger.Warn("reaping: destroy ok but record delete failed; next sweep retries", "error", derr)
		return false
	}
	return true
}

// reapingFootprint enumerates the volumes currently on disk in a reaping lease's
// namespace — both the canonical fred-{lease}-* names and the quarantined
// fred-retained-{lease}-* ones. It is the ground truth a reaping record deliberately does
// NOT carry (see destroyReapingVolumes): the record says a footprint is abandoned, this
// says what that footprint actually is right now.
//
// Both namespaces are enumerated because a give-up can strand a lease part-way through the
// retain path's renames, leaving some volumes canonical and some already quarantined. The
// stored-list design could not know which, so recordGiveUpLeak used to write BOTH spellings
// of every name and rely on the destroy being an idempotent no-op for whichever did not
// exist — a workaround for not being able to look. Looking is simpler and exact.
//
// An error means the volume root could not be enumerated, which the caller must treat as
// uncertainty and never as "no volumes": the caller's response to an empty set is to DELETE
// the record. That distinction is guaranteed at the syscall (listVolumeIDs returns ENOENT
// rather than an empty slice), so it needs no separate stat here — and therefore has no
// window between a probe and a read for the root to vanish in. Ownership is deliberately
// not consulted either: that is volumeOp.destroy's job, and asking it twice would be the
// "two definitions that must agree" this file has spent several tickets collapsing.
//
// The enumeration is resolved at most ONCE per index, and every caller that loops over
// records shares one — a full os.ReadDir of the volume root per record would be O(R×V) per
// pass, which is worst exactly where it hurts most: evictOldest runs up to 32 records
// synchronously inside a lease close. Same lazy-once shape as volumeOp.claims, for the
// same reason.
type managedVolumeIndex struct {
	b        *Backend
	resolved bool
	all      []string
	err      error
}

// newManagedVolumeIndex starts one pass's view of the node's managed volumes. Scope it to a
// single sweep/close and let it go: like the ownership table, it is a point-in-time answer,
// and the failure mode this family guards against is a collector acting on a stale one.
// Staleness within a pass is safe in both directions — a volume that appears after the
// snapshot is simply not destroyed, and one that disappears makes its destroy an idempotent
// no-op — and the per-name ownership check still runs at destroy time under the volume's
// stripe regardless (ENG-681).
func (b *Backend) newManagedVolumeIndex() *managedVolumeIndex {
	return &managedVolumeIndex{b: b}
}

// footprint returns the volumes currently on disk in a reaping lease's namespace — both the
// canonical fred-{lease}-* names and the quarantined fred-retained-{lease}-* ones.
//
// Both namespaces are enumerated because a give-up can strand a lease part-way through the
// retain path's renames, leaving some volumes canonical and some already quarantined. The
// stored-list design could not know which, so recordGiveUpLeak used to write BOTH spellings
// of every name and rely on the destroy being an idempotent no-op for whichever did not
// exist — a workaround for not being able to look. Looking is simpler and exact.
func (i *managedVolumeIndex) footprint(orig string) ([]string, error) {
	if !i.resolved {
		i.all, i.err = i.b.volumes.List()
		i.resolved = true
	}
	if i.err != nil {
		return nil, fmt.Errorf("list volumes: %w", i.err)
	}
	cprefix := leaseVolumePrefix(orig) // fred-{lease}-
	rprefix := retainedName(cprefix)   // fred-retained-{lease}-
	names := make([]string, 0, len(i.all))
	for _, id := range i.all {
		if strings.HasPrefix(id, cprefix) || strings.HasPrefix(id, rprefix) {
			names = append(names, id)
		}
	}
	return names, nil
}

// volumeRootUnverifiable reports whether a volume-root probe means the orphan
// reconcile must skip this pass (fail-safe). exists/statErr come from pathExists:
// an absent root (false,nil) OR any stat error (false,err — permission denied,
// EIO, …) is unverifiable. Deliberately NOT an os.IsNotExist-only check: an
// unreadable root is as uncertain as a missing one (kubelet #72257 hazard).
func volumeRootUnverifiable(exists bool, statErr error) bool {
	return statErr != nil || !exists
}

// allVolumesAbsent reports whether none of the retained names — nor their
// canonical (not-yet-renamed) form — is present on disk. A deprovision give-up
// leaves the volume under its canonical fred-{lease}-* name while the record
// lists the fred-retained-* names; checking both keeps the pruner from deleting a
// record whose data is still on disk, which a later boot would then destroy
// (ENG-501). An empty name set is vacuously absent (covers legacy zero-volume
// records).
func allVolumesAbsent(names []string, present map[string]bool) bool {
	for _, n := range names {
		if present[n] || present[canonicalFromRetained(n)] {
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
		// Kill-switch (0 = disabled). DEBUG-level (not INFO): the sweep cadence is
		// configurable, so an INFO every sweep would be sustained noise when pruning is
		// intentionally disabled. The retentionOrphanSkipsTotal{reason="disabled"} counter
		// is the always-on, queryable "feature is off" signal.
		b.logger.Debug("orphan retention reconcile disabled (retention_orphan_confirmations=0)")
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipDisabled).Inc()
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
			retentionOrphanSkipsTotal.WithLabelValues(orphanSkipRootUnverifiable).Inc()
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
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError).Inc()
		return 0, err
	}
	present := make(map[string]bool, len(existing))
	for _, v := range existing {
		present[v] = true
	}

	recs, err := b.retentionStore.List()
	if err != nil {
		b.orphanStreaks = map[string]int{}
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipStoreError).Inc()
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
			retentionOrphanSkipsTotal.WithLabelValues(orphanSkipRaced).Inc()
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
// record removed). Each expired active record is atomically transitioned to
// reaping before its volumes are destroyed, so a destroy failure leaves the
// record as a counted finalizer tombstone rather than creating an under-count.
func (b *Backend) reapExpiredRetentions(ctx context.Context) (int, error) {
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListExpired(b.cfg.RetentionMaxAge)
	if err != nil {
		return 0, err
	}
	var n int
	idx := b.newManagedVolumeIndex() // one enumeration for the whole reap pass
	for _, e := range candidates {
		// Atomic active→reaping (the record is NEVER deleted before its volumes are
		// confirmed gone, so a destroy failure cannot drop a still-on-disk footprint).
		// Stored names discarded — the finalizer derives the footprint from disk. Only the
		// atomic transition matters here (see MarkReapingIfActive above).
		_, ok, merr := b.retentionStore.MarkReapingIfExpired(e.OriginalLeaseUUID, b.cfg.RetentionMaxAge)
		if merr != nil {
			b.logger.Error("reap: store error", "lease_uuid", e.OriginalLeaseUUID, "error", merr)
			continue
		}
		if !ok {
			continue // concurrently claimed/changed since the snapshot — skip
		}
		if b.destroyReapingVolumes(ctx, idx, e.OriginalLeaseUUID) {
			n++
		}
	}
	b.refreshRetentionAccounting()
	return n, nil
}

// retryReapingRecords re-attempts destruction of every reaping record's volumes
// (the finalizer retry) and deletes each record whose volumes are confirmed gone.
// Runs on the periodic sweep AND at boot. Fail-closed: on a store List error the
// records are kept (footprint keeps counting). It deliberately does NOT call
// refreshRetentionAccounting itself — the CALLER owns the refresh (runRetentionSweep
// refreshes at the end; the boot path refreshes via reapExpiredRetentions/recoverState).
// A new caller MUST refresh after invoking this. (ENG-376)
func (b *Backend) retryReapingRecords(ctx context.Context) error {
	if b.retentionStore == nil {
		return nil
	}
	recs, err := b.retentionStore.ListReaping()
	if err != nil {
		return err
	}
	idx := b.newManagedVolumeIndex() // one enumeration for the whole retry pass
	for _, e := range recs {
		b.destroyReapingVolumes(ctx, idx, e.OriginalLeaseUUID)
	}
	return nil
}

// runRetentionSweep is the PERIODIC reaper body: reap expired + retry any reaping
// records (the finalizer retry, ENG-376) + reconcile any restoring records (a
// running-process backstop for restores that failed since the last tick) + prune
// ACTIVE records whose volumes have been absent for >= N consecutive sweeps (orphan
// reconcile, ENG-370). The BOOT path does NOT call this: at startup reconcileRetentions
// (before cleanup) handles restoring and reaping records and an eager
// reapExpiredRetentions (after cleanup) handles expired ones, so they aren't
// double-reconciled.
//
// EVERY STAGE RUNS, AND THE ERRORS ARE JOINED (ENG-680). This function used to
// bare-return on the first error, which made a persistently unreadable retention.db
// invisible: List/ListExpired/ListReaping/ListRestoring are one filter() over one bucket
// (shared/retention.go), so they share failure modes exactly — a corrupt page or an EIO
// fails the FIRST enumerator and the sweep was over. reconcileOrphanedRetentions never
// ran, which made its retention_orphan_skips_total{reason="store_error"} arm dead code
// under precisely the condition it was written to report, and the accounting refresh
// never ran either. The one trace was a slog.Error per tick, hourly at the default
// cadence, behind no metric at all.
//
// Running the later stages after an earlier failure is safe because every stage is a
// read-then-act pass that already fails safe on its own read: reapExpiredRetentions and
// retryReapingRecords enumerate nothing and so destroy nothing, and
// reconcileOrphanedRetentions re-reads both the volume list and the store itself, resets
// its confirmation streaks and bails on either error. A stage cannot be handed a partial
// view by a sibling — none of them share derived state — so "run anyway" strictly adds
// information and can never add a destroy. That is the same direction the rest of this
// file takes: destroy only on a positive fact, never on an error or an empty list.
func (b *Backend) runRetentionSweep(ctx context.Context) error {
	if err := b.requireStorageIdentity(ctx); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	var errs []error
	if _, err := b.reapExpiredRetentions(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reap expired: %w", err))
	}
	// A nil store means no record can exist, so the remaining stages have nothing to read.
	// Not an early RETURN any more: the accounting refresh and the outcome record below
	// are unconditional. (Unreachable in practice — retentionSweepInterval gates the
	// reaper off entirely when the store is nil — but the guard is what makes that a
	// belt-and-braces fact rather than a dependency.)
	if b.retentionStore != nil {
		if err := b.retryReapingRecords(ctx); err != nil {
			errs = append(errs, fmt.Errorf("retry reaping: %w", err))
		}
		if recs, err := b.retentionStore.ListRestoring(); err != nil {
			errs = append(errs, fmt.Errorf("list restoring: %w", err))
		} else {
			for _, e := range recs {
				// A per-record failure deliberately parks the finalizer for the
				// next sweep and remains a logged operational delay. Startup uses
				// reconcileRetentions and propagates the same error when a durable
				// restore intent needs exact classification.
				_ = b.reconcileRestoring(ctx, e)
			}
		}
		// ENG-370: prune orphaned records BEFORE ENG-360's accounting refresh so the
		// retained-disk projection reflects this sweep's prunes. The refresh runs even
		// when the prune returns a fail-safe error (the prune mutated nothing in that
		// case, but the reaper above may have, and refresh is keep-last-value on a store
		// read error).
		if _, err := b.reconcileOrphanedRetentions(); err != nil {
			errs = append(errs, fmt.Errorf("reconcile orphans: %w", err))
		}
	}
	b.refreshRetentionAccounting()

	// Exactly one outcome per pass — see retentionSweepTotal's doc for why that property
	// is load-bearing. The error still goes back to StartCleanupLoop, which logs it with
	// every failing stage named; the counter is what an alert can actually key on.
	err := errors.Join(errs...)
	if err != nil {
		retentionSweepTotal.WithLabelValues(sweepOutcomeError).Inc()
		return err
	}
	retentionSweepTotal.WithLabelValues(sweepOutcomeSuccess).Inc()
	return nil
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
		}, "retention", func(any) { background.CleanupPanicsTotal.WithLabelValues("retention").Inc() })
	})
}

// ---------------------------------------------------------------------------
// Restore as a first-class backend operation (ENG-325, Task 7b)
// ---------------------------------------------------------------------------

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

// checkDemoteFit refuses a restore whose new SKU shrinks a service's disk
// cap below the retained volume's measured footprint. It is READ-ONLY and
// runs in the synchronous prelude BEFORE any side effect (reserve / pool /
// claim / adopt), so a refusal leaves the retained record and volumes
// untouched — no rollback. The retained volumes are from a closed lease with
// no running container, so the footprint is static (no TOCTOU on size); a
// concurrent reaper flipping the record active→reaping is handled by the
// later atomic ClaimForRestoreWithAuthority (loser → ErrNotRestorable). Do NOT cache the
// Usage result or move this gate after the claim.
//
// For each service: a clear promote/same-tier (new ≥ old, both resolvable)
// skips measurement (the cap only grows). Otherwise every retained stateful
// volume of that service must satisfy usage ≤ newDiskMB×MiB (equality OK).
// Over-measuring is the SAFE direction here (refuse = preserve data); see
// the design spec §5.6. An unmeasurable volume or a demote to an ephemeral
// (DiskMB=0) tier with retained data is refused.
func (b *Backend) checkDemoteFit(ctx context.Context, rec *shared.RetentionEntry,
	newItems []backend.LeaseItem, newProfiles map[string]SKUProfile, logger *slog.Logger) error {
	resourceProfiles, err := b.snapshotResourceProfiles(newItems, newProfiles)
	if err != nil {
		return fmt.Errorf("%w: snapshot restore resource profiles: %w", backend.ErrValidation, err)
	}
	return b.checkDemoteFitWithResourceProfiles(ctx, rec, newItems, resourceProfiles, logger)
}

func (b *Backend) checkDemoteFitWithResourceProfiles(
	ctx context.Context,
	rec *shared.RetentionEntry,
	newItems []backend.LeaseItem,
	newResourceProfiles []shared.SKUResourceSnapshot,
	logger *slog.Logger,
) error {
	retained := make(map[string]struct{}, len(rec.RetainedVolumeNames))
	for _, n := range rec.RetainedVolumeNames {
		retained[n] = struct{}{}
	}
	newResources, err := resourceSnapshotMap(newItems, newResourceProfiles)
	if err != nil {
		return fmt.Errorf("%w: invalid restore resource authority: %w", backend.ErrValidation, err)
	}

	var oldResourceProfiles []shared.SKUResourceSnapshot
	if len(rec.ResourceProfiles) > 0 {
		oldResourceProfiles = rec.ResourceProfiles
	} else {
		// True v0.13 rows have no immutable authority. Resolve once for this
		// read-only gate. A failed restore's rollback freezes the same source
		// authority before handing the retention row back; a successful restore
		// deletes the source finalizer. An unavailable old SKU remains unknown and
		// therefore takes the conservative measurement/refusal path below.
		oldResourceProfiles, _ = b.resolveResourceProfiles(rec.Items)
	}
	oldResources := make(map[string]shared.SKUResourceSnapshot)
	if len(oldResourceProfiles) > 0 {
		oldResources, err = resourceSnapshotMap(rec.Items, oldResourceProfiles)
		if err != nil {
			return fmt.Errorf("%w: invalid retained resource authority: %w", backend.ErrInvalidState, err)
		}
	}
	oldSKU := make(map[string]string, len(rec.Items))
	for _, it := range rec.Items {
		oldSKU[it.ServiceName] = it.SKU
	}
	backendKind := b.volumes.Kind()
	for _, it := range newItems {
		newResourcesForSKU, ok := newResources[it.SKU]
		if !ok {
			return fmt.Errorf("%w: unknown SKU %q", backend.ErrValidation, it.SKU)
		}
		newDiskMB, diskErr := newResourcesForSKU.EffectiveDiskMB()
		if diskErr != nil {
			return fmt.Errorf("%w: invalid resource authority for SKU %q: %w",
				backend.ErrValidation, it.SKU, diskErr)
		}
		// Promote/same-tier optimization: compare against the immutable profile
		// captured when the source became retained. Legacy records without a
		// snapshot fall back to the current configuration; an unresolved legacy
		// SKU conservatively takes the measurement path.
		oldResourcesForSKU, oldProfileOK := oldResources[oldSKU[it.ServiceName]]
		oldDiskMB := int64(0)
		if oldProfileOK {
			oldDiskMB, diskErr = oldResourcesForSKU.EffectiveDiskMB()
			if diskErr != nil {
				return fmt.Errorf("%w: invalid retained resource authority for service %q: %w",
					backend.ErrInvalidState, it.ServiceName, diskErr)
			}
		}
		durableSourceToScratch := oldResourcesForSKU.DiskMB > 0 &&
			newResourcesForSKU.DiskMB == 0
		if oldProfileOK && !durableSourceToScratch && newDiskMB >= oldDiskMB {
			continue
		}
		for i := range it.Quantity {
			name := retainedName(canonicalVolumeName(rec.OriginalLeaseUUID, it.ServiceName, i))
			if _, isStateful := retained[name]; !isStateful {
				continue // stateless instance: no retained volume to check
			}
			// Scratch is intentionally non-stateful: a durable source volume cannot
			// be restored into a diskless destination even if its ephemeral allowance
			// happens to be numerically large enough. Exact retained scratch, however,
			// may restore to another scratch row and is measured against the new pinned
			// allowance just like any other physical quota.
			destinationIsScratch := newResourcesForSKU.DiskMB == 0
			sourceMayBeDurable := !oldProfileOK || oldResourcesForSKU.DiskMB > 0
			if newDiskMB <= 0 || (destinationIsScratch && sourceMayBeDurable) {
				restoreDemoteRefusedTotal.WithLabelValues(backendKind, "ephemeral_tier").Inc()
				return fmt.Errorf("%w: service %q: cannot restore stateful data into an ephemeral (disk_mb=0) tier",
					backend.ErrDemoteDataExceedsTier, it.ServiceName)
			}
			usage, uerr := b.volumes.Usage(ctx, name)
			if uerr != nil {
				reason := "unmeasurable_read_error"
				if errors.Is(uerr, errors.ErrUnsupported) {
					reason = "unmeasurable_backend"
				}
				restoreDemoteRefusedTotal.WithLabelValues(backendKind, reason).Inc()
				// uerr can embed host paths and raw command output (e.g. btrfs
				// qgroup show against b.dataPath); log it for operators but keep
				// it OUT of the returned error, which docker-backend/fred-api
				// forward verbatim to the (untrusted) tenant in the 422 body.
				logger.Warn("restore demote refused: cannot measure retained volume usage",
					"service", it.ServiceName, "volume", name, "reason", reason, "error", uerr)
				return fmt.Errorf("%w: service %q: unable to verify retained data fits the requested tier",
					backend.ErrDemoteDataExceedsTier, it.ServiceName)
			}
			if newDiskMB > math.MaxInt64/bytesPerMiB {
				return fmt.Errorf("%w: service %q: disk_mb cap overflows byte accounting",
					backend.ErrValidation, it.ServiceName)
			}
			capBytes := newDiskMB * bytesPerMiB
			if usage > capBytes {
				restoreDemoteRefusedTotal.WithLabelValues(backendKind, "measured_exceeds").Inc()
				logger.Warn("restore demote refused: retained data exceeds smaller tier",
					"service", it.ServiceName, "volume", name, "used_bytes", usage, "tier_disk_mb", newDiskMB, "cap_bytes", capBytes)
				return fmt.Errorf("%w: service %q: %d bytes used exceeds disk_mb=%d cap (%d bytes)",
					backend.ErrDemoteDataExceedsTier, it.ServiceName, usage, newDiskMB, capBytes)
			}
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
// first error; the caller fully rolls back on failure.
func (b *Backend) adoptRetainedVolumes(ctx context.Context, newLease string, rec *shared.RetentionEntry) error {
	for _, retained := range rec.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, rec.OriginalLeaseUUID, newLease)
		if err := b.mutationAdapter().renameVolume(ctx, retained, newCanonical); err != nil {
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
	if err := b.requireMutationAdmission(ctx, "restore"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	unlockCommand := b.commandFence.Lock(req.LeaseUUID)
	defer unlockCommand()
	logger := b.logger.With("lease_uuid", req.LeaseUUID, "from_lease", req.FromLeaseUUID, "tenant", req.Tenant)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(
		req.CallbackURL, req.LifecycleCallbackURL,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	req.LifecycleCallbackURL = lifecycleCallbackURL
	if exactRetry, err := b.probeOperationIntent(req.LeaseUUID, req.CallbackURL); err != nil {
		return err
	} else if exactRetry {
		return nil
	}
	if err := b.ensureRestoreDestinationUnowned(req.LeaseUUID); err != nil {
		return err
	}

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
	// Exact retries were handled by the durable intent probe above. Every new
	// admission must start from Active; predicting a generation for an existing
	// Restoring/Reaping row creates a second operation intent that can never own
	// the source CAS and can wedge both retries. Reject before normalization,
	// projection reservation, or intent creation so this path is side-effect free.
	if rec.Status != shared.RetentionStatusActive {
		return fmt.Errorf("%w: retained source status is %q", backend.ErrInvalidState, rec.Status)
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
	restoreQuantity, err := backend.ValidateOperationQuantities(req.Items)
	if err != nil {
		return err
	}
	if err := itemsShapeMatch(rec.Items, req.Items); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	if err := validateComposeServiceNames(req.Items); err != nil {
		return fmt.Errorf("%w: retained topology cannot form an injective Compose project: %w", backend.ErrValidation, err)
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
	for _, item := range req.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		p, perr := b.cfg.GetSKUProfile(item.SKU)
		if perr != nil {
			return fmt.Errorf("%w: %w", backend.ErrValidation, perr)
		}
		profiles[item.SKU] = p
	}
	resourceProfiles, err := b.snapshotResourceProfiles(req.Items, profiles)
	if err != nil {
		return fmt.Errorf("%w: snapshot restore resource profiles: %w", backend.ErrValidation, err)
	}
	resourcesBySKU, err := resourceSnapshotMap(req.Items, resourceProfiles)
	if err != nil {
		return fmt.Errorf("%w: validate restore resource profiles: %w", backend.ErrValidation, err)
	}
	// Demote fit-gate (read-only; BEFORE any side effect — reserve/pool/claim/
	// adopt). A refusal leaves the retained record and volumes untouched.
	if err := b.checkDemoteFitWithResourceProfiles(ctx, rec, req.Items, resourceProfiles, logger); err != nil {
		return err
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
	var healthCheckServices []string
	for service, serviceManifest := range rec.StackManifest.Services {
		if serviceManifest.HasActiveHealthCheck() {
			healthCheckServices = append(healthCheckServices, service)
		}
	}
	slices.Sort(healthCheckServices)
	restoreManifestPayload, err := json.Marshal(rec.StackManifest)
	if err != nil {
		return fmt.Errorf("marshal restore manifest for durable intent: %w", err)
	}
	intent, proceed, err := b.beginOperationIntent(
		shared.OperationIntentRestore,
		req.LeaseUUID,
		req.CallbackURL,
		req.LifecycleCallbackURL,
		rec.Tenant,
		rec.ProviderUUID,
		req.Items,
		resourceProfiles,
		req.Items,
		healthCheckServices,
		restoreManifestPayload,
		req.FromLeaseUUID,
		rec.Generation+1,
	)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}
	if intent == nil {
		return errors.New("created restore operation intent returned no claim")
	}
	if err := b.checkOperationReleaseCapacity(*intent); err != nil {
		return b.refuseOperationIntent(intent, fmt.Errorf(
			"%w: reserve restore success release: %w",
			backend.ErrInsufficientResources,
			err,
		))
	}

	// (b) Reserve the new-lease entry at Status=Provisioning. (7a permits
	// evRestoreRequested from Provisioning.) Reject if already provisioned.
	b.provisionsMu.Lock()
	if _, exists := b.provisions[req.LeaseUUID]; exists {
		b.provisionsMu.Unlock()
		return b.refuseOperationIntent(intent,
			fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID))
	}
	b.provisions[req.LeaseUUID] = recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            req.LeaseUUID,
			Tenant:               rec.Tenant,
			ProviderUUID:         rec.ProviderUUID,
			SKU:                  req.Items[0].SKU,
			Status:               backend.ProvisionStatusProvisioning,
			Quantity:             restoreQuantity,
			CreatedAt:            time.Now(),
			FailCount:            0,
			LastError:            "",
			Reason:               "", // fresh reservation, no failure
			Message:              "",
			CallbackURL:          req.CallbackURL,
			LifecycleCallbackURL: req.LifecycleCallbackURL,
			Items:                slices.Clone(req.Items),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
			ContainerIDs:         make([]string, 0),
			StackManifest:        rec.StackManifest,
			ServiceContainers:    nil,
		},
		resourceProfiles:      shared.CloneSKUResourceSnapshot(resourceProfiles),
		volumeCleanupAttempts: 0,
	}.materialize()
	b.provisionsMu.Unlock()

	// (c) Reserve pool slots for the restore atomically. Restore adopts existing
	// volumes (rename, not fresh disk), so disk capacity is gated once on the
	// AGGREGATE promote delta — the growth of the lease's total DiskMB above its
	// already-committed retained footprint — while CPU/mem/tenant are gated per
	// instance. TryAllocateAdoptAll does the whole reservation under a single pool
	// lock: the gate is EXACT (no per-volume double-count of the retained bytes
	// still in the projection until ClaimForRestoreWithAuthority), ATOMIC (no concurrent
	// provision/restore can slip disk in between the check and the reservations),
	// and CONSISTENT (the pool computes the new total from its own resolver, so it
	// cannot under-gate against the reservation). A fitting multi-volume promote is
	// admitted and the pool cannot be over-committed (ENG-545).
	//
	// We pass only the OLD retained footprint. New records carry an immutable
	// profile snapshot, so a later SKU removal/resize cannot reprice already-held
	// bytes. Legacy records fall back to the current config; an unresolved legacy
	// SKU contributes zero, making the promote delta larger (over-deny, never
	// over-admit).
	oldDiskMB, oldUnresolved, oldDiskErr := b.retentionEntryDiskMB(*rec)
	if oldDiskErr != nil {
		b.removeProvision(req.LeaseUUID)
		return b.refuseOperationIntent(intent, fmt.Errorf(
			"%w: retained resource footprint is invalid: %w", backend.ErrInvalidState, oldDiskErr,
		))
	}
	if len(oldUnresolved) > 0 {
		logger.Warn("restore disk gate: retained record references unresolved SKU profile(s); retained footprint undercounted, admission is more conservative",
			"retained_unresolved_skus", oldUnresolved)
	}
	adoptInstances := make([]shared.ResolvedAdoptInstance, 0, restoreQuantity)
	for _, item := range req.Items {
		for i := range item.Quantity {
			adoptInstances = append(adoptInstances, shared.ResolvedAdoptInstance{
				ID:        fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i),
				Resources: resourcesBySKU[item.SKU],
			})
		}
	}
	if aerr := b.pool.TryAllocateAdoptAllResolved(adoptInstances, rec.Tenant, oldDiskMB); aerr != nil {
		b.removeProvision(req.LeaseUUID)
		return b.refuseOperationIntent(intent,
			fmt.Errorf("%w: %w", backend.ErrInsufficientResources, aerr))
	}
	allocatedIDs := make([]string, len(adoptInstances))
	for i, in := range adoptInstances {
		allocatedIDs[i] = in.ID
	}
	// Refresh the resource gauges now that the allocation succeeded, mirroring
	// Provision/Deprovision's on-success refresh. The rollback paths re-refresh
	// after releaseAll, so this only stands as the live value on the success arm.
	updateResourceMetrics(b.pool.Stats())

	// (d) ATOMIC claim active->restoring (closes the prelude-vs-reaper race).
	// Nothing renamed yet.
	claimed, err := b.retentionStore.ClaimForRestoreWithAuthorityAt(
		req.FromLeaseUUID,
		req.LeaseUUID,
		b.cfg.RetentionMaxAge,
		req.Items,
		resourceProfiles,
		intent.OperationID(),
		req.CallbackURL,
		req.LifecycleCallbackURL,
		intent.CreatedAt(),
	)
	if err != nil {
		releaseAll(b.pool, allocatedIDs)
		updateResourceMetrics(b.pool.Stats())
		b.removeProvision(req.LeaseUUID)
		switch {
		case errors.Is(err, shared.ErrNoRetention):
			return b.refuseOperationIntent(intent, backend.ErrNotRetained)
		case errors.Is(err, shared.ErrNotRestorable):
			return b.refuseOperationIntent(intent, fmt.Errorf("%w: %w", backend.ErrInvalidState, err))
		default:
			return b.refuseOperationIntent(intent, fmt.Errorf("claim retention: %w", err))
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
	if err := b.adoptRetainedVolumes(ctx, req.LeaseUUID, claimed); err != nil {
		return b.rollbackUnacceptedRestoreAdoption(
			ctx,
			req.LeaseUUID,
			allocatedIDs,
			claimed,
			*intent,
			fmt.Errorf("adopt retained volumes: %w", err),
			logger,
		)
	}
	replacePhaseDurationSeconds.WithLabelValues("restore", phaseAdopt).Observe(time.Since(adoptStart).Seconds())

	// (f) Hand off to the actor; doRestore's terminal defer owns
	// success/failure/panic.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doRestore(opCtx, req.LeaseUUID, claimed, req.Items, resourceProfiles, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.RestoreRequestedMsg{
		Cancel:               opCancel,
		Work:                 work,
		Ack:                  ack,
		CallbackURL:          req.CallbackURL,
		LifecycleCallbackURL: req.LifecycleCallbackURL,
	}); routeErr != nil {
		opCancel()
		// Worker never ran; no actor transition will flip Status — drop the
		// reservation (dropProvision=true).
		return b.rollbackUnacceptedRestoreAdoption(
			ctx, req.LeaseUUID, allocatedIDs, claimed, *intent, routeErr, logger,
		)
	}
	acceptance, err := b.awaitAsyncAcceptance(ctx, ack)
	switch acceptance {
	case asyncAcceptanceAccepted:
		return nil
	case asyncAcceptanceUnknown:
		return fmt.Errorf("restore acceptance is unknown; durable recovery retained: %s", err.Error())
	case asyncAcceptanceRejected:
		opCancel()
		// An explicit actor rejection proves it never fired evRestoreRequested,
		// so no terminal transition owns the reservation.
		return b.rollbackUnacceptedRestoreAdoption(
			ctx, req.LeaseUUID, allocatedIDs, claimed, *intent, err, logger,
		)
	default:
		return fmt.Errorf("invalid restore acceptance state %d", acceptance)
	}
}

// doRestore is the restore worker (runs on the lease actor's replace-worker
// goroutine). It brings up the new lease's stack from the retained manifest via
// doReplaceContainers with NoComposeRollback (no prior containers to recover to).
//
// Its terminal defer is the SOLE owner of the success/failure/panic outcome for
// the retention record:
//   - success (resultRet.Err==nil): delete the retained record (data adopted).
//   - failure (resultRet.Err!=nil): physically roll back the adoption and leave
//     the source Restoring until the actor durably records its Failed callback;
//     the retention sweep then completes the source-authority handback.
//   - panic: a panic leaves resultRet.Err==nil; force the failure path so we
//     never delete the record while the lease is not Ready. Convert panic→Failed.
//
// In BOTH failure cases the preparation does not drop the provision or live
// allocation: doRestore returns an errored ReplaceResult, so the actor fires
// evReplaceFailed, reads CallbackURL from that projection, and persists the
// operation completion. The same Failed projection is fenced from maintenance
// by the Restoring row until reconciliation settles any surviving intent and
// hands capacity back make-before-break.
func (b *Backend) doRestore(ctx context.Context, leaseUUID string, rec *shared.RetentionEntry,
	newItems []backend.LeaseItem, resourceProfiles []shared.SKUResourceSnapshot, logger *slog.Logger) (resultRet leasesm.ReplaceResult) {
	restoreStart := time.Now()
	defer func() {
		// N2: a panic leaves resultRet.Err==nil; force the failure path so we never
		// delete the record while the lease is not Ready. Convert panic -> Failed.
		if r := recover(); r != nil {
			logger.Error("restore worker panicked", "recover", r)
			// Count the outcome BEFORE the fallible rollback so a panic inside
			// rollbackRestoreAdoption can't bypass the increment (the success branch
			// likewise counts before its fallible Delete).
			restoresTotal.WithLabelValues("failure").Inc()
			b.prepareAcceptedRestoreAdoptionRollback(ctx, leaseUUID, rec, logger)
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
					Reason:      backend.ReasonInternal,
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
			restoresTotal.WithLabelValues("success").Inc()
			// Record the new lease's active release, then finalize the restore. The
			// restoring retention record is the adopted volume's finalizer, so it is
			// dropped only once the release is durably recorded (ENG-523).
			b.finalizeRestoredLease(leaseUUID, rec, newItems, logger)
			b.refreshRetentionAccounting()
			return
		}
		// Count the outcome before the fallible rollback (see the panic branch).
		restoresTotal.WithLabelValues("failure").Inc()
		b.prepareAcceptedRestoreAdoptionRollback(ctx, leaseUUID, rec, logger)
	}()
	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID: leaseUUID, Stack: rec.StackManifest, Items: newItems, ResourceProfiles: resourceProfiles,
		Operation: "restore", NoComposeRollback: true, Logger: logger,
		OnSuccess: func(p *leasesm.ProvisionState) { p.StackManifest = rec.StackManifest },
	})
}

// finalizeRestoredLease records the NEW lease's active release, then — and only
// then — drops the restoring retention record. recoverState rehydrates
// prov.StackManifest ONLY from releaseStore.LatestActive, and Restart hard-fails on
// a nil manifest, so the release must be written for the restored lease to survive a
// backend restart. The lease is already Ready here, so it is written directly as
// active (mirrors provision.go's on-success Append).
//
// The retention record is the adopted volume's FINALIZER (Kubernetes-style): while
// it exists (restoring), cleanupOrphanedVolumes protects the adopted canonical
// volume (recover.go's restoring arm) and reconcileRestoring finalizes it once the
// lease is confirmed Ready. Dropping the finalizer BEFORE the release is durably
// recorded would leave the lease with NEITHER record, so a later boot's orphan
// reaper — which keys on the release record (leaseHasActiveRelease) — would destroy
// the still-live tenant data (ENG-523). So the record is deleted only once the
// release Append succeeds; on any failure (Append error, marshal error, or no
// release store) it is LEFT restoring. reconcileRestoring re-invokes this helper on
// its next sweep once the lease is Ready (the retry path), so a transient failure
// self-heals; the next Update re-provision also re-records the release. It is
// idempotent — an already-durable release is not re-appended.
//
// Leaving the record lingering is safe post-ENG-512: reconcileRestoring never tears
// a running lease down (Ready->Delete; every other live state defers), so a stale
// restoring record can no longer re-quarantine a healthy lease's volumes — which is
// what previously forced the unconditional Delete here. (ENG-433 / ENG-523)
func (b *Backend) finalizeRestoredLease(
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
	logger *slog.Logger,
) {
	if err := b.ensureRestoredReleaseStrict(leaseUUID, rec, effectiveItems); err != nil {
		// Keep the finalizer: the adopted volume stays protected until a later
		// reconcileRestoring sweep or an Update durably records the release and drops the
		// record. Tradeoff while it lingers (only under a sustained release-store outage):
		// the ORIGINAL lease UUID reports Retained (info.go maps restoring→retained) even
		// though the restore is done, and a Restore-retry from the original is rejected
		// (ClaimForRestoreWithAuthority needs Active). Both self-heal once the store recovers and the
		// record is dropped; restoreFinalizerPendingTotal makes each initial kept-
		// pending event observable. Reconciliation retries do not re-increment it.
		restoreFinalizerPendingTotal.Inc()
		logger.Warn("restore ok but destination Release is not durable; keeping retention record as the adopted volume's finalizer (ENG-523)",
			"lease_uuid", leaseUUID, "original_lease_uuid", rec.OriginalLeaseUUID, "error", err)
	}
}

// finalizeRestoredLeaseStrict is the exact, idempotent commit used when a
// caller must know whether ownership transferred before it may continue (for
// example, close admission). nil means both the destination release and source
// finalizer deletion are durable. Any error leaves the source record in place,
// so callers can fail closed without guessing which lease owns the bytes.
func (b *Backend) finalizeRestoredLeaseStrict(
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
) error {
	if err := b.ensureRestoredReleaseStrict(leaseUUID, rec, effectiveItems); err != nil {
		return err
	}
	return b.deleteRestoreFinalizerStrict(leaseUUID, rec)
}

// ensureRestoredReleaseStrict durably commits destination ownership without
// consuming the source finalizer. Callback settlement paths use this first,
// settle the exact operation second, and only then delete the finalizer; every
// crash boundary therefore leaves a level-triggered retry owner.
func (b *Backend) ensureRestoredReleaseStrict(
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
) error {
	if rec == nil {
		return fmt.Errorf("restore source finalizer is required")
	}
	if leaseUUID == "" || rec.OriginalLeaseUUID == "" {
		return fmt.Errorf("restore source and destination lease UUIDs are required")
	}
	if rec.Status != shared.RetentionStatusRestoring || rec.NewLeaseUUID != leaseUUID {
		return fmt.Errorf(
			"restore source finalizer does not own destination %q (status=%q new_lease_uuid=%q)",
			leaseUUID, rec.Status, rec.NewLeaseUUID,
		)
	}
	if rec.OriginalLeaseUUID == leaseUUID {
		return fmt.Errorf("restore source and destination lease UUIDs must differ")
	}
	if rec.Generation <= 0 {
		return fmt.Errorf("restore source finalizer generation must be positive")
	}
	if rec.StackManifest == nil {
		return fmt.Errorf("restored manifest is required")
	}
	if _, err := backend.ValidateOperationQuantities(rec.Items); err != nil {
		return fmt.Errorf("validate restore source items: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(effectiveItems); err != nil {
		return fmt.Errorf("validate restored effective items: %w", err)
	}
	if len(rec.DestinationItems) > 0 || len(rec.DestinationResourceProfiles) > 0 {
		if _, err := backend.ValidateOperationQuantities(rec.DestinationItems); err != nil {
			return fmt.Errorf("validate restore destination authority items: %w", err)
		}
		if err := itemsShapeMatch(rec.Items, rec.DestinationItems); err != nil {
			return fmt.Errorf("validate restored item shape: %w", err)
		}
		if err := validateDockerResourceProfiles(
			rec.DestinationItems,
			rec.DestinationResourceProfiles,
		); err != nil {
			return fmt.Errorf("validate restore destination authority resource profiles: %w", err)
		}
		if err := manifest.ValidateStackAgainstItems(rec.StackManifest, rec.DestinationItems); err != nil {
			return fmt.Errorf("validate restored manifest topology: %w", err)
		}
	}
	if b.releaseStore == nil {
		return fmt.Errorf("release store is required")
	}
	if b.retentionStore == nil {
		return fmt.Errorf("retention store is required")
	}
	b.provisionsMu.RLock()
	provision, exists := b.provisions[leaseUUID]
	if !exists || !slices.Equal(provision.Items, effectiveItems) {
		b.provisionsMu.RUnlock()
		return fmt.Errorf("restored live provision does not match finalizer items")
	}
	if provision.Tenant != rec.Tenant || provision.ProviderUUID != rec.ProviderUUID {
		b.provisionsMu.RUnlock()
		return fmt.Errorf(
			"restored live provision identity does not match finalizer tenant/provider",
		)
	}
	if provision.StackManifest == nil {
		b.provisionsMu.RUnlock()
		return fmt.Errorf("restored live provision manifest is required")
	}
	liveItems := slices.Clone(provision.Items)
	liveManifestBytes, marshalLiveErr := json.Marshal(provision.StackManifest)
	liveManifest := provision.StackManifest
	liveResourceProfiles := shared.CloneSKUResourceSnapshot(provision.ResourceProfiles)
	b.provisionsMu.RUnlock()
	if marshalLiveErr != nil {
		return fmt.Errorf("marshal restored live provision manifest: %w", marshalLiveErr)
	}
	if err := itemsShapeMatch(rec.Items, liveItems); err != nil {
		return fmt.Errorf("validate restored live item shape: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(liveManifest, liveItems); err != nil {
		return fmt.Errorf("validate restored live manifest topology: %w", err)
	}
	if err := validateDockerResourceProfiles(liveItems, liveResourceProfiles); err != nil {
		return fmt.Errorf("validate restored resource profiles: %w", err)
	}

	// A newer successful maintenance operation may already have durably published
	// the live generation while this restore finalizer lingered. That exact active
	// Release is sufficient ownership authority. Otherwise append only from the
	// destination snapshot atomically bound into the source retention claim —
	// never from live state or mutable SKU configuration. This is the crash-safe
	// bridge when the restore succeeded, its first Release append failed, and the
	// operation intent has already been consumed.
	existing, err := b.releaseStore.LatestActive(leaseUUID)
	if err != nil {
		return fmt.Errorf("read restored active release: %w", err)
	}
	existingOwnsLive := existing != nil &&
		existing.OperationID == rec.DestinationOperationID &&
		bytes.Equal(existing.Manifest, liveManifestBytes) &&
		slices.Equal(existing.Items, liveItems) &&
		slices.Equal(existing.ResourceProfiles, liveResourceProfiles) &&
		((rec.DestinationOperationID == "" && existing.RuntimeAuthority == nil) ||
			(rec.DestinationOperationID.Valid() && existing.RuntimeAuthority != nil &&
				existing.OperationID == rec.DestinationOperationID &&
				releaseRuntimeAuthorityMatchesRetention(existing.RuntimeAuthority, *rec)))
	if !existingOwnsLive {
		authorityItems := slices.Clone(rec.DestinationItems)
		authorityProfiles := shared.CloneSKUResourceSnapshot(rec.DestinationResourceProfiles)
		if len(authorityItems) == 0 || len(authorityProfiles) == 0 {
			return fmt.Errorf("restore source finalizer has no exact destination authority and no active release owns the live generation")
		}
		if _, err := backend.ValidateOperationQuantities(authorityItems); err != nil {
			return fmt.Errorf("validate restore destination authority items: %w", err)
		}
		if err := itemsShapeMatch(rec.Items, authorityItems); err != nil {
			return fmt.Errorf("validate restore destination authority shape: %w", err)
		}
		if err := validateDockerResourceProfiles(authorityItems, authorityProfiles); err != nil {
			return fmt.Errorf("validate restore destination authority resource profiles: %w", err)
		}
		if err := manifest.ValidateStackAgainstItems(rec.StackManifest, authorityItems); err != nil {
			return fmt.Errorf("validate restore destination authority manifest topology: %w", err)
		}
		authorityManifestBytes, marshalErr := json.Marshal(rec.StackManifest)
		if marshalErr != nil {
			return fmt.Errorf("marshal restore destination authority manifest: %w", marshalErr)
		}
		if !slices.Equal(effectiveItems, authorityItems) || !slices.Equal(liveItems, authorityItems) {
			return fmt.Errorf("restored live provision items do not match durable destination authority")
		}
		if !slices.Equal(liveResourceProfiles, authorityProfiles) {
			return fmt.Errorf("restored live provision resource profiles do not match durable destination authority")
		}
		if !bytes.Equal(liveManifestBytes, authorityManifestBytes) {
			return fmt.Errorf("restored live provision manifest does not match durable destination authority")
		}
		runtimeAuthority, authorityErr := releaseRuntimeAuthorityForOperation(
			rec.DestinationOperationID,
			rec.Tenant,
			rec.ProviderUUID,
			rec.DestinationCallbackURL,
			rec.DestinationLifecycleCallbackURL,
		)
		if authorityErr != nil {
			return fmt.Errorf("construct restore destination runtime authority: %w", authorityErr)
		}
		if err := b.releaseStore.AppendActive(leaseUUID, shared.Release{
			Manifest:         authorityManifestBytes,
			Image:            "stack",
			OperationID:      rec.DestinationOperationID,
			Items:            authorityItems,
			ResourceProfiles: authorityProfiles,
			RuntimeAuthority: runtimeAuthority,
			Status:           "active",
			CreatedAt:        rec.RestoringSince,
		}); err != nil {
			return fmt.Errorf("record restored active release: %w", err)
		}
	}
	return nil
}

func (b *Backend) deleteRestoreFinalizerStrict(
	leaseUUID string,
	rec *shared.RetentionEntry,
) error {
	if rec == nil {
		return fmt.Errorf("restore source finalizer is required")
	}
	deleted, err := b.retentionStore.DeleteIfRestoring(
		rec.OriginalLeaseUUID,
		leaseUUID,
		rec.Generation,
	)
	if err != nil {
		return fmt.Errorf("delete restore source finalizer: %w", err)
	}
	if !deleted {
		// Another idempotent finalizer may have won after the exact active
		// release became durable. Absence is therefore success; any surviving
		// record is a changed authority that this stale snapshot must not consume.
		current, readErr := b.retentionStore.Get(rec.OriginalLeaseUUID)
		if readErr != nil {
			return fmt.Errorf("verify restore source finalizer after lost delete authority: %w", readErr)
		}
		if current != nil {
			return fmt.Errorf(
				"restore source finalizer changed before delete (status=%q new_lease_uuid=%q generation=%d)",
				current.Status, current.NewLeaseUUID, current.Generation,
			)
		}
	}
	return nil
}

type retainedQuotaTarget struct {
	name   string
	diskMB int64
}

// restoreRetainedVolumeQuotas restores the exact pre-restore quota on every
// adopted volume after it has been re-quarantined and before the source
// retention record becomes Active again.
//
// A promote restore raises the physical quota through volumeManager.Create.
// Merely renaming the volume back does not undo that change on btrfs, XFS, or
// ZFS. RevertToActiveWithResourceProfiles would then publish the smaller immutable source footprint
// while the filesystem still permits growth to the larger destination cap.
// This helper closes that under-accounting window by requiring three proofs:
// the durable snapshot maps every retained volume to one exact old cap, current
// usage fits that cap, and the volume manager successfully reapplies it. The
// caller must leave the record Restoring and its live allocation counted on any
// error.
func (b *Backend) restoreRetainedVolumeQuotas(
	ctx context.Context,
	rec *shared.RetentionEntry,
) ([]shared.SKUResourceSnapshot, error) {
	if rec == nil {
		return nil, errors.New("restore source retention record is required")
	}
	if _, err := backend.ValidateOperationQuantities(rec.Items); err != nil {
		return nil, fmt.Errorf("validate restore source quantities: %w", err)
	}

	// New records already carry immutable authority. For a pre-snapshot row,
	// resolve the currently configured source SKUs once and return that canonical
	// snapshot to the caller. The caller applies these exact caps and commits this
	// same value in its generation CAS; a crash before the CAS leaves Restoring
	// and live-counted, while a successful CAS makes physical and durable truth
	// observable atomically. An unavailable legacy SKU fails closed.
	resourceProfiles := shared.CloneSKUResourceSnapshot(rec.ResourceProfiles)
	if len(resourceProfiles) == 0 {
		var err error
		resourceProfiles, err = b.resolveResourceProfiles(rec.Items)
		if err != nil {
			return nil, fmt.Errorf("resolve legacy restore source resource profiles: %w", err)
		}
	}
	if err := validateDockerResourceProfiles(rec.Items, resourceProfiles); err != nil {
		return nil, fmt.Errorf("validate restore source resource profiles: %w", err)
	}
	if len(rec.RetainedVolumeNames) == 0 {
		return resourceProfiles, nil
	}

	// RetainedVolumeNames is authoritative for which stateful instances actually
	// survived close. Derive the allowed names from Items only to bind each of
	// those names to its immutable SKU profile; never invent volumes for stateless
	// instances or writable-path-only reclamation gaps.
	unmatched := make(map[string]struct{}, len(rec.RetainedVolumeNames))
	for _, name := range rec.RetainedVolumeNames {
		if name == "" {
			return nil, errors.New("restore source contains an empty retained volume name")
		}
		if _, duplicate := unmatched[name]; duplicate {
			return nil, fmt.Errorf("restore source contains duplicate retained volume %q", name)
		}
		unmatched[name] = struct{}{}
	}

	targets := make([]retainedQuotaTarget, 0, len(unmatched))
	for _, item := range rec.Items {
		resources, ok := shared.LookupSKUResourceSnapshotRow(resourceProfiles, item.SKU)
		if !ok {
			// Validation proved exact coverage. Keep this guard adjacent to the
			// authority-consuming code so a future validator change fails closed.
			return nil, fmt.Errorf("restore source resource profiles omit SKU %q", item.SKU)
		}
		for i := range item.Quantity {
			name := retainedName(canonicalVolumeName(rec.OriginalLeaseUUID, item.ServiceName, i))
			if _, retained := unmatched[name]; !retained {
				continue
			}
			diskMB, diskErr := resources.EffectiveDiskMB()
			if diskErr != nil {
				return nil, fmt.Errorf("retained volume %q has invalid resource authority: %w", name, diskErr)
			}
			if diskMB <= 0 {
				return nil, fmt.Errorf("retained volume %q maps to SKU %q with no durable or scratch disk authority",
					name, item.SKU)
			}
			if diskMB > math.MaxInt64/bytesPerMiB {
				return nil, fmt.Errorf("retained volume %q old disk_mb cap overflows byte accounting", name)
			}
			targets = append(targets, retainedQuotaTarget{name: name, diskMB: diskMB})
			delete(unmatched, name)
		}
	}
	if len(unmatched) > 0 {
		names := make([]string, 0, len(unmatched))
		for name := range unmatched {
			names = append(names, name)
		}
		slices.Sort(names)
		return nil, fmt.Errorf("restore source retained volumes do not match its immutable item topology: %v", names)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].name < targets[j].name })

	// Measure the complete set before changing any quota. The volumes are frozen:
	// teardown completed before re-quarantine, so no tenant writer can race this
	// fit proof. A partial EnsureQuota failure is still safe and retryable, but
	// avoiding avoidable partial updates makes operator recovery clearer.
	for _, target := range targets {
		usage, err := b.volumes.Usage(ctx, target.name)
		if err != nil {
			b.logger.Warn("restore rollback cannot verify retained volume usage",
				"lease_uuid", rec.OriginalLeaseUUID,
				"volume", target.name,
				"volume_backend", b.volumes.Kind(),
				"error", err,
			)
			return nil, fmt.Errorf("cannot verify usage for retained volume %q", target.name)
		}
		capBytes := target.diskMB * bytesPerMiB
		if usage < 0 {
			return nil, fmt.Errorf("retained volume %q reported invalid negative usage", target.name)
		}
		if usage > capBytes {
			b.logger.Error("restore rollback data exceeds immutable source quota",
				"lease_uuid", rec.OriginalLeaseUUID,
				"volume", target.name,
				"used_bytes", usage,
				"source_disk_mb", target.diskMB,
				"source_cap_bytes", capBytes,
			)
			return nil, fmt.Errorf("retained volume %q usage exceeds its immutable source quota", target.name)
		}
	}

	for _, target := range targets {
		if err := b.mutationAdapter().ensureVolumeQuota(ctx, target.name, target.diskMB); err != nil {
			b.logger.Error("restore rollback cannot apply immutable source quota",
				"lease_uuid", rec.OriginalLeaseUUID,
				"volume", target.name,
				"volume_backend", b.volumes.Kind(),
				"source_disk_mb", target.diskMB,
				"error", err,
			)
			return nil, fmt.Errorf("cannot restore immutable source quota for retained volume %q", target.name)
		}
	}
	return resourceProfiles, nil
}

// revertRestoreSourceWithAccounting is the make-before-break commit from a
// destination live reservation back to retained source ownership. It
// pessimistically adds the exact source footprint to the pool before the store
// CAS, while serialized with every projection refresh. Once the CAS succeeds,
// a failed full refresh is harmless: the conservative pre-addition already
// covers these bytes, so the destination allocations can be released and any
// later refresh/restart converges to the durable Active row.
func (b *Backend) revertRestoreSourceWithAccounting(
	rec *shared.RetentionEntry,
	newLeaseUUID string,
	resourceProfiles []shared.SKUResourceSnapshot,
	allocatedIDs []string,
) (bool, error) {
	if rec == nil {
		return false, errors.New("restore source retention record is required")
	}
	exact := *rec
	exact.ResourceProfiles = shared.CloneSKUResourceSnapshot(resourceProfiles)
	handoffMB, unresolved, err := b.retentionEntryDiskMB(exact)
	if err != nil {
		return false, fmt.Errorf("size restore rollback accounting handoff: %w", err)
	}
	if len(unresolved) > 0 {
		return false, fmt.Errorf("restore rollback accounting has unresolved SKUs: %v", unresolved)
	}

	b.retentionAccountingMu.Lock()
	defer b.retentionAccountingMu.Unlock()
	previousRetainedMB := b.pool.Stats().RetainedDiskMB
	conservativeRetainedMB, err := addLeaseDiskMB(previousRetainedMB, handoffMB, 1)
	if err != nil {
		return false, fmt.Errorf("reserve restore rollback retained accounting: %w", err)
	}
	if err := b.pool.SetRetainedDisk(conservativeRetainedMB); err != nil {
		return false, fmt.Errorf("reserve restore rollback retained accounting: %w", err)
	}

	ok, commitErr := b.retentionStore.RevertToActiveWithResourceProfiles(
		rec.OriginalLeaseUUID, newLeaseUUID, rec.Generation, resourceProfiles,
	)
	if commitErr != nil || !ok {
		// The durable owner did not change, so undo only our conservative add.
		// No projection writer can interleave while retentionAccountingMu is held.
		if rollbackErr := b.pool.SetRetainedDisk(previousRetainedMB); rollbackErr != nil {
			commitErr = errors.Join(commitErr, fmt.Errorf(
				"restore prior retained accounting after failed ownership CAS: %w", rollbackErr,
			))
		}
		return ok, commitErr
	}

	if refreshErr := b.refreshRetentionAccountingCheckedLocked(); refreshErr != nil {
		b.logger.Warn("restore rollback retained projection refresh failed; conservative direct handoff remains counted",
			"lease_uuid", rec.OriginalLeaseUUID,
			"new_lease_uuid", newLeaseUUID,
			"retained_disk_mb", conservativeRetainedMB,
			"error", refreshErr,
		)
	}
	// The durable Active row and either the checked projection or the conservative
	// pre-addition now count the source bytes. Releasing live allocations cannot
	// create an under-count, even when the store became unreadable after its CAS.
	releaseAll(b.pool, allocatedIDs)
	updateResourceMetrics(b.pool.Stats())
	return true, nil
}

// prepareRestoreAdoptionRollback is the physical half of the idempotent
// compensation for an adopted restore. N1: compose.Down the new project FIRST
// (stop containers on the bind-mounted volumes) BEFORE renaming volumes back —
// otherwise a still-running container holds the volume's bind mount open. It
// then re-quarantines each adopted volume and proves/reapplies the source quota.
// It never changes durable source ownership or releases the live allocation;
// the accepted and unaccepted wrappers choose the correct settlement owner.
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
// record restoring (do NOT RevertToActiveWithResourceProfiles, do NOT removeProvision) and return:
// the next reconcile sweep retries the re-quarantine safely, and meanwhile the
// provision's expected-set entry (cleanupOrphanedVolumes' restoring arm) protects
// the canonical volume from the orphan reaper. Reverting here would make that
// still-live data eligible for cleanup/reaping.
//
// Make-before-break (ENG-376 site 4): every failure leaves the live allocation
// counted. reconcileRestoring's orphaned arm resumes preparation and performs
// the exact retained-accounting handoff only after journal settlement succeeds.
func (b *Backend) prepareRestoreAdoptionRollback(ctx context.Context, leaseUUID string,
	rec *shared.RetentionEntry, dropProvision bool, logger *slog.Logger,
) ([]shared.SKUResourceSnapshot, bool) {
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	b.provisionsMu.RLock()
	var recordedIDs []string
	if p, ok := b.provisions[leaseUUID]; ok {
		recordedIDs = slices.Clone(p.ContainerIDs)
	}
	b.provisionsMu.RUnlock()
	// Label the two arms apart. They are the same call but not the same event: the worker
	// arm BLOCKS on a failed teardown (below), while the prelude arm discards the error
	// and completes the rollback, so counting both as restore_rollback would make the
	// wedge case unqueryable and put "nothing is wrong" samples in the alerting series
	// (ENG-647). The prelude callers are themselves entered on caller-context
	// cancellation and then hand that same dead context to the teardown, so a failure
	// there is routinely just a canceled request, not a leak.
	teardownOp := teardownOpRestoreRollback
	if dropProvision {
		teardownOp = teardownOpRestorePrelude
	}
	if _, derr := b.teardownLeaseContainers(ctx, leaseUUID, recordedIDs, stopTimeout,
		teardownOp, logger); derr != nil && !dropProvision {
		// Same precondition as reconcileRestoring's orphaned arm: a surviving container
		// holds the adopted volumes by inode, so re-quarantining them now would let it
		// write into data the record calls frozen (ENG-647). Leave the record restoring
		// with the live allocation counted; the reconcile sweep retries the whole
		// rollback, and doRestore's caller still gets its errored ReplaceResult, so the
		// actor's evReplaceFailed → Failed transition and its callback are unaffected.
		//
		// dropProvision==true is deliberately EXEMPT. It means the worker never ran
		// (adopt/route/ack failure in the synchronous prelude), so no compose Up
		// happened and there is nothing to strand — a Down error there is a wedged
		// daemon, not a leak. Bailing would leave a Provisioning provision behind a
		// restoring record, which reconcileRestoring's in-flight guard then defers on
		// forever: a wedge only a restart clears. Dropping the reservation is the only
		// way that lease ever becomes clean again.
		logger.Warn("restore rollback: teardown failed; leaving record restoring for the reconcile sweep",
			"lease_uuid", rec.OriginalLeaseUUID, "new_lease_uuid", leaseUUID, "error", derr)
		return nil, false
	}
	failed := false
	for _, retained := range rec.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, rec.OriginalLeaseUUID, leaseUUID)
		if rerr := b.renameIfPresent(ctx, newCanonical, retained); rerr != nil {
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
		if dropProvision {
			// The actor rejected (or was never reached), so Provisioning has no
			// legitimate writer left. Remove this Backend-owned reservation marker;
			// the restoring record protects both canonical and retained names, and
			// its exact destination authority lets recovery preserve accounting.
			b.removeProvision(leaseUUID)
		}
		return nil, false
	}
	resourceProfiles, quotaErr := b.restoreRetainedVolumeQuotas(ctx, rec)
	if quotaErr != nil {
		// The names are safely back in the retained namespace, but publishing the
		// source record would under-account a promoted quota. Preserve the live
		// reservation. A synchronous prelude has no actor transition coming, so
		// remove only its in-memory Provisioning guard to let the periodic restoring
		// reconciler retry this exact quota proof; the pool allocation remains held
		// until that retry durably reactivates the source.
		logger.Error("restore rollback: unable to restore source volume quotas; leaving record restoring + live counted",
			"lease_uuid", rec.OriginalLeaseUUID,
			"new_lease_uuid", leaseUUID,
			"error", quotaErr,
		)
		if dropProvision {
			b.removeProvision(leaseUUID)
		}
		return nil, false
	}
	return resourceProfiles, true
}

// completeRestoreAdoptionRollback performs the make-before-break ownership
// handback after prepareRestoreAdoptionRollback proved that no destination
// container or promoted quota remains. Callers that own a pre-actor restore
// must durably settle its failed operation before entering this function.
func (b *Backend) completeRestoreAdoptionRollback(
	leaseUUID string,
	allocatedIDs []string,
	rec *shared.RetentionEntry,
	resourceProfiles []shared.SKUResourceSnapshot,
	dropProvision bool,
	logger *slog.Logger,
) bool {
	// Re-quarantine succeeded. The helper pre-counts the exact retained footprint,
	// commits source authority, and only then releases live allocations while
	// serialized with projection refreshes.
	ok, rerr := b.revertRestoreSourceWithAccounting(rec, leaseUUID, resourceProfiles, allocatedIDs)
	if rerr != nil {
		retentionLeakedTotal.Inc()
		logger.Error("restore rollback: revert record failed; keeping live allocation counted until reconcile resumes the revert",
			"lease_uuid", rec.OriginalLeaseUUID, "error", rerr)
		if dropProvision {
			b.removeProvision(leaseUUID)
		}
		return false
	}
	if !ok {
		// Lost authority is not proof that the replacement owner is already reflected
		// in the pool. Keep the live term rather than risk an under-count; recovery
		// rebuilds it from current durable ownership.
		logger.Warn("restore rollback: record generation changed; preserving live accounting for recovery",
			"lease_uuid", rec.OriginalLeaseUUID)
		if dropProvision {
			b.removeProvision(leaseUUID)
		}
		return false
	}
	if dropProvision {
		b.removeProvision(leaseUUID)
	}
	return true
}

// prepareAcceptedRestoreAdoptionRollback performs physical and quota rollback
// for a restore whose worker crossed the actor acceptance boundary. It
// deliberately does not hand the source back to Active or release the live
// allocation: the actor has not yet durably settled its Failed callback. Its
// Failed transition leaves the provision projection in place, after which the
// level-triggered retention sweep settles any surviving intent and performs the
// exact make-before-break handback. This keeps the Restoring row as retry
// authority if callback persistence fails.
func (b *Backend) prepareAcceptedRestoreAdoptionRollback(
	ctx context.Context,
	leaseUUID string,
	rec *shared.RetentionEntry,
	logger *slog.Logger,
) {
	_, _ = b.prepareRestoreAdoptionRollback(
		ctx, leaseUUID, rec, false, logger,
	)
}

// rollbackUnacceptedRestoreAdoption compensates a synchronous Restore failure
// for which no actor worker can publish Ready. It intentionally settles the
// exact failed operation after physical/quota cleanup but before the source
// handback CAS. If settlement fails, the Restoring record and live allocation
// remain a level-triggered retry owner; only the dead Provisioning projection is
// removed so the periodic reconciler can enter its orphaned rollback arm.
func (b *Backend) rollbackUnacceptedRestoreAdoption(
	ctx context.Context,
	leaseUUID string,
	allocatedIDs []string,
	rec *shared.RetentionEntry,
	intent shared.OperationIntentClaim,
	cause error,
	logger *slog.Logger,
) error {
	if cause == nil {
		return errors.New("unaccepted restore rollback requires a failure cause")
	}
	if intent.Kind() != shared.OperationIntentRestore {
		return fmt.Errorf("unaccepted restore rollback received %s intent", intent.Kind())
	}
	resourceProfiles, prepared := b.prepareRestoreAdoptionRollback(
		ctx, leaseUUID, rec, true, logger,
	)
	if !prepared {
		return fmt.Errorf("restore failed; durable source cleanup remains pending: %w", cause)
	}
	if err := b.settleUnacceptedRestoreIntent(intent); err != nil {
		b.removeProvision(leaseUUID)
		return fmt.Errorf("restore failed; durable operation settlement remains pending: %s: %w", cause.Error(), err)
	}
	if !b.completeRestoreAdoptionRollback(
		leaseUUID, allocatedIDs, rec, resourceProfiles, true, logger,
	) {
		return fmt.Errorf("restore failed; durable source handback remains pending: %w", cause)
	}
	return cause
}
