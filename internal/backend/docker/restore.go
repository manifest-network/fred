package docker

import (
	"bytes"
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
// It keeps the leading "fred-" so complete managed-volume inventories include it,
// while the distinct "retained" token separates durable retained data from a live mount.
const retainedVolumePrefix = "fred-retained-"

// canonicalVolumeName is the live volume name a provision/restore mounts.
// Every managed volume name in the backend is built here — setupVolBinds, the
// close path, restore, and the owner table (volume_destroy.go) all call it
// rather than repeating the format string, which is how the orphan reaper's
// copy came to drift.
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
// failed, or the still-canonical volume would remain exposed). reconcileRetentions'
// active arm tolerates the error because the exact active record continues to claim
// both its retained name and canonical counterpart; the next sweep retries the rename.
func (b *Backend) renameIfPresentUsing(
	ctx context.Context,
	renameVolume backgroundVolumeRename,
	oldName, newName string,
) error {
	if renameVolume == nil {
		return errBackgroundMaintenanceUnavailable
	}
	if err := renameVolume(ctx, oldName, newName); err != nil {
		b.logger.Warn("reconcile rename skipped", "old", oldName, "new", newName, "error", err)
		return err
	}
	return nil
}

// reconcileRetentions repairs crash-interrupted soft-deletes/restores. It runs
// after recoverState so b.provisions reflects live containers. Unattributed managed
// volumes are never destroyed by inference; exact retention records remain the only
// authority for these rename/finalization steps.
func (b *Backend) reconcileRetentionsUsing(
	ctx context.Context,
	renameVolume backgroundVolumeRename,
	teardown teardownMutationCapability,
	destroyVolumes volumeDestroyMutationCapability,
	ensureQuota backgroundVolumeQuota,
) error {
	if renameVolume == nil || teardown == nil || destroyVolumes == nil || ensureQuota == nil {
		return errBackgroundMaintenanceUnavailable
	}
	if b.retentionStore == nil {
		return nil
	}
	all, err := b.retentionStore.List()
	if err != nil {
		return err
	}
	var reconcileErrs []error
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
	for _, e := range all {
		switch e.Status {
		case shared.RetentionStatusActive:
			// Crash after Put before rename: a canonical volume may still be on disk.
			// On rename failure we log and keep going: the active retention record
			// continues to claim its canonical counterpart and a later sweep retries.
			for _, retained := range e.RetainedVolumeNames {
				canonical := canonicalFromRetained(retained)
				if rerr := b.renameIfPresentUsing(ctx, renameVolume, canonical, retained); rerr != nil {
					b.logger.Warn("reconcile: re-quarantine of active canonical failed (cleanup protection covers it)",
						"lease_uuid", e.OriginalLeaseUUID, "canonical", canonical, "error", rerr)
					reconcileErrs = append(reconcileErrs, fmt.Errorf(
						"re-quarantine active retention %q volume %q: %w",
						e.OriginalLeaseUUID, canonical, rerr,
					))
				}
			}
		case shared.RetentionStatusRestoring:
			if rerr := b.reconcileRestoringWithAuthorityUsing(
				ctx, e, renameVolume, teardown, destroyVolumes, ensureQuota,
			); rerr != nil {
				reconcileErrs = append(reconcileErrs, rerr)
			}
		}
	}
	// A reaping DTO is deliberately insufficient to resume destruction. Select
	// exact tombstone proofs after the potentially long restoring walk, then
	// carry each unchanged proof through physical cleanup and finalizer delete.
	reaping, proofErr := b.retentionStore.ListReapingProofs()
	if proofErr != nil {
		reconcileErrs = append(reconcileErrs, fmt.Errorf("list reaping authority: %w", proofErr))
	} else {
		idx := b.newManagedVolumeIndex()
		for _, proof := range reaping {
			b.destroyReapingVolumesUsing(ctx, idx, proof, destroyVolumes)
		}
	}
	return errors.Join(reconcileErrs...)
}

// restoreRecoveryPlan is a closed set of decisions. In particular, destructive
// rollback is represented only by the two types carrying durable failed or
// still-pending authority; a missing row can never be converted into one.
type restoreRecoveryPlan interface{ restoreRecoveryPlan() }

// The two plan families are distinct capabilities: code that can commit a
// destination cannot be passed to the destructive rollback path, and vice
// versa. The unexported markers keep both sets closed to this package.
type restoreCommitPlan interface {
	restoreRecoveryPlan
	restoreCommitPlan()
}

type restoreRollbackPlan interface {
	restoreRecoveryPlan
	restoreRollbackPlan()
}

type restoreCommitPending struct {
	claim            shared.OperationIntentClaim
	releaseCommitted bool
}
type restoreCommitFromRelease struct{}
type restoreAwaitOperation struct{ claim shared.OperationIntentClaim }
type restoreRollbackFailed struct{ outcome shared.OperationFailed }

func (restoreCommitPending) restoreRecoveryPlan()     {}
func (restoreCommitFromRelease) restoreRecoveryPlan() {}
func (restoreAwaitOperation) restoreRecoveryPlan()    {}
func (restoreRollbackFailed) restoreRecoveryPlan()    {}

func (restoreCommitPending) restoreCommitPlan()     {}
func (restoreCommitFromRelease) restoreCommitPlan() {}
func (restoreRollbackFailed) restoreRollbackPlan()  {}

func (b *Backend) planRestoreRecovery(
	e shared.RetentionEntry,
	operation shared.OperationRecoveryState,
	releaseCommitted bool,
) (restoreRecoveryPlan, error) {
	switch state := operation.(type) {
	case nil:
		if releaseCommitted {
			return restoreCommitFromRelease{}, nil
		}
		return nil, fmt.Errorf("restore destination %q has neither operation outcome nor committed release",
			e.NewLeaseUUID)
	case shared.OperationSucceeded:
		if !releaseCommitted {
			return nil, fmt.Errorf(
				"restore destination %q has successful operation outcome without its exact active Release",
				e.NewLeaseUUID,
			)
		}
		return restoreCommitFromRelease{}, nil
	case shared.OperationFailed:
		if releaseCommitted {
			return nil, fmt.Errorf(
				"restore destination %q has contradictory committed Release and failed operation outcome",
				e.NewLeaseUUID,
			)
		}
		return restoreRollbackFailed{outcome: state}, nil
	case shared.OperationIntentClaim:
		if releaseCommitted {
			return restoreCommitPending{claim: state, releaseCommitted: true}, nil
		}
		// The operation coordinator is the sole owner of uncommitted Pending
		// substrate. It applies the visibility window, performs any exact cleanup,
		// and publishes a sealed Succeeded or Failed outcome. Retention recovery
		// must not independently reinterpret one inventory snapshot as rollback
		// authority while an accepted Docker Create can still become visible.
		return restoreAwaitOperation{claim: state}, nil
	default:
		return nil, fmt.Errorf("restore destination %q has unknown durable operation state %T",
			e.NewLeaseUUID, operation)
	}
}

func (b *Backend) commitRecoveredRestore(
	ctx context.Context,
	recoveryScope shared.LeaseRecoveryScope,
	e shared.RetentionEntry,
	live bool,
	status backend.ProvisionStatus,
	liveItems []backend.LeaseItem,
	plan restoreCommitPlan,
) error {
	liveReady := live && status == backend.ProvisionStatusReady
	var operationRelease *shared.OperationReleaseCandidate
	switch decision := plan.(type) {
	case restoreCommitPending:
		candidate, err := b.operationSettlement.PrepareOperationRelease(decision.claim)
		if err != nil {
			return fmt.Errorf("prepare pending restore release authority: %w", err)
		}
		operationRelease = &candidate
	case restoreCommitFromRelease:
		// The exact active Release is already the commit authority.
	default:
		return fmt.Errorf("restore commit cannot execute plan %T", plan)
	}
	// Validate a surviving Ready generation before any repair write. A terminal
	// Success proves the operation outcome, but it does not authorize a newer or
	// divergent live manifest; the finalizer and exact live projection must agree
	// before either can become an active Release.
	if liveReady {
		if _, err := b.ensureRestoredReleaseStrict(
			ctx, recoveryScope, e.NewLeaseUUID, &e, liveItems, operationRelease,
		); err != nil {
			return fmt.Errorf("validate Ready restore destination %q: %w", e.NewLeaseUUID, err)
		}
	}
	switch decision := plan.(type) {
	case restoreCommitPending:
		if !decision.releaseCommitted && (!live || status != backend.ProvisionStatusReady) {
			return fmt.Errorf("pending restore destination %q lacks Ready commit evidence", e.NewLeaseUUID)
		}
	case restoreCommitFromRelease:
		// The exact active Release is already the irreversible commit.
	default:
		return fmt.Errorf("restore commit cannot execute plan %T", plan)
	}
	if decision, ok := plan.(restoreCommitPending); ok {
		committed, err := b.operationSettlement.ProveCommittedOperation(decision.claim)
		if err != nil {
			return fmt.Errorf("prove committed restore operation for %q: %w", e.NewLeaseUUID, err)
		}
		if b.callbackPublisher == nil {
			return errors.New("callback publisher is required")
		}
		if err := b.callbackPublisher.PublishOperationSuccessContext(ctx, committed); err != nil {
			return fmt.Errorf("settle committed restore operation for %q: %w", e.NewLeaseUUID, err)
		}
	}
	if liveReady {
		if err := b.deleteRestoreFinalizerStrict(e.NewLeaseUUID, &e); err != nil {
			return fmt.Errorf("delete committed restore finalizer for %q: %w", e.NewLeaseUUID, err)
		}
	}
	return nil
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
	operation, err := b.currentRestoreOperation(*source)
	if err != nil {
		if errors.Is(err, shared.ErrOperationIntentMissing) {
			// The exact Release is already the irreversible ownership proof;
			// a later durable maintenance/close successor may have retired the
			// historical operation row.
			return nil
		}
		return fmt.Errorf(
			"%w: read restore operation settlement for %q: %w",
			backend.ErrInvalidState, destinationLease, err,
		)
	}
	if _, pending := operation.(shared.OperationIntentClaim); pending {
		return fmt.Errorf(
			"%w: destination lease %q restore operation is not settled",
			backend.ErrInvalidState, destinationLease,
		)
	}
	if _, failed := operation.(shared.OperationFailed); failed {
		return fmt.Errorf(
			"%w: destination lease %q has contradictory committed and failed restore outcomes",
			backend.ErrInvalidState, destinationLease,
		)
	}
	return nil
}

func (b *Backend) reconcileRestoringWithAuthorityUsing(
	ctx context.Context,
	e shared.RetentionEntry,
	renameVolume backgroundVolumeRename,
	teardown teardownMutationCapability,
	destroyVolumes volumeDestroyMutationCapability,
	ensureQuota backgroundVolumeQuota,
) error {
	if renameVolume == nil || teardown == nil || destroyVolumes == nil || ensureQuota == nil {
		return errBackgroundMaintenanceUnavailable
	}
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
	if b.recoveryCoordinator == nil {
		return errors.New("restore recovery coordinator is required")
	}
	acquired, recoveryErr := b.recoveryCoordinator.WithLease(
		ctx, e.NewLeaseUUID,
		func(recoveryScope shared.LeaseRecoveryScope) error {

			// The row passed by reconcileRetentions came from a batch snapshot taken
			// before this per-destination fence and actor-quiescence claim. Re-establish
			// exact durable authority
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

			b.provisionsMu.RLock()
			p, live := b.provisions[e.NewLeaseUUID]
			var status backend.ProvisionStatus
			var liveItems []backend.LeaseItem
			if live {
				status = p.Status
				liveItems = slices.Clone(p.Items)
			}
			b.provisionsMu.RUnlock()

			committed, err := b.restoreDestinationCommitted(e)
			if err != nil {
				return fmt.Errorf("validate restore commit for destination %q: %w", e.NewLeaseUUID, err)
			}
			operation, operationErr := b.currentRestoreOperation(e)
			if operationErr != nil && !errors.Is(operationErr, shared.ErrOperationIntentMissing) {
				return fmt.Errorf("read exact restore operation for destination %q: %w",
					e.NewLeaseUUID, operationErr)
			}
			if operationErr != nil && !committed {
				return fmt.Errorf("read exact restore operation for uncommitted destination %q: %w",
					e.NewLeaseUUID, operationErr)
			}
			plan, err := b.planRestoreRecovery(e, operation, committed)
			if err != nil {
				return err
			}
			switch decision := plan.(type) {
			case restoreCommitPending:
				if err := b.commitRecoveredRestore(
					ctx, recoveryScope, e, live, status, liveItems, decision,
				); err != nil {
					return err
				}
				return nil
			case restoreCommitFromRelease:
				if err := b.commitRecoveredRestore(
					ctx, recoveryScope, e, live, status, liveItems, decision,
				); err != nil {
					return err
				}
				return nil
			case restoreAwaitOperation:
				// Pending substrate remains owned by operation recovery. This pass
				// cannot mint rollback authority from another inventory observation.
				return nil
			case restoreRollbackFailed:
				return b.handbackFailedRestore(ctx, recoveryScope, e, decision, renameVolume, ensureQuota)
			default:
				return fmt.Errorf("restore destination %q produced unknown recovery plan %T",
					e.NewLeaseUUID, plan)
			}
		},
	)
	if recoveryErr != nil {
		return recoveryErr
	}
	if !acquired {
		return nil
	}
	return nil
}

// handbackFailedRestore owns the complete failed-attempt cleanup and source
// handback under the destination recovery fence. Its store-issued receipt binds
// capture, exact removal, and absence classification to that attempt. Only this
// workflow proceeds from destination vacancy to volume renames, quota restoration,
// and the source/accounting CAS; it exports no intermediate cleanup permission.
func (b *Backend) handbackFailedRestore(
	ctx context.Context,
	scope shared.LeaseRecoveryScope,
	e shared.RetentionEntry,
	plan restoreRollbackFailed,
	renameVolume backgroundVolumeRename,
	ensureQuota backgroundVolumeQuota,
) error {
	if plan.outcome == nil {
		return errors.New("restore cleanup requires a failed operation")
	}
	outcome := plan.outcome
	if err := b.validateRestoreOperationAuthority(outcome, e); err != nil {
		return fmt.Errorf("bind failed restore source handback: %w", err)
	}
	receipts, err := b.operationSettlement.ListFailedOperationReceipts()
	if err != nil {
		return fmt.Errorf("read exact failed restore receipt: %w", err)
	}
	index := slices.IndexFunc(receipts, func(receipt shared.FailedOperationReceipt) bool {
		return receipt.OperationID() == outcome.OperationID() && receipt.LeaseUUID() == outcome.LeaseUUID()
	})
	if index < 0 {
		return errors.New("failed restore cleanup has no exact durable receipt")
	}
	receipt := receipts[index]
	if receipt.Kind() != shared.OperationIntentRestore || receipt.LeaseUUID() != outcome.LeaseUUID() ||
		receipt.CallbackURL() != outcome.CallbackURL() || receipt.LifecycleCallbackURL() != outcome.LifecycleCallbackURL() ||
		receipt.Backend() != outcome.Backend() || receipt.BackendStorageID() != outcome.BackendStorageID() ||
		receipt.Tenant() != outcome.Tenant() || receipt.ProviderUUID() != outcome.ProviderUUID() {
		return errors.New("failed restore receipt differs from exact rollback authority")
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cleanupErr := func() error {
		if err := b.operationSettlement.CleanupFailedOperationReceipt(cleanupCtx, scope, receipt); err != nil {
			return fmt.Errorf("retire exact failed restore targets: %w", err)
		}
		// A different callback generation is not ours to remove. It still blocks
		// handback: moving mounted volumes would expose retained source data to
		// a container whose ownership this failed receipt cannot prove.
		containers, err := b.strictIdentityBoundOperationInventory(cleanupCtx)
		if err != nil {
			return fmt.Errorf("verify restore destination is empty before handback: %w", err)
		}
		for _, container := range containers {
			if container.LeaseUUID == outcome.LeaseUUID() {
				return fmt.Errorf("restore destination container %q still prevents source handback", container.ContainerID)
			}
		}
		return nil
	}()
	cleanupOutcome := teardownOutcomeRecovered
	if cleanupErr != nil {
		cleanupOutcome = teardownOutcomeFailed
	}
	teardownFallbackTotal.WithLabelValues(teardownOpRestoreReconcile, cleanupOutcome).Inc()
	if cleanupErr != nil {
		return cleanupErr
	}
	// Re-quarantine each adopted volume. A REAL rename failure (not a benign
	// no-op) means the volume may still be canonical-named: we must NOT advance
	// the record to active or drop the provision: doing either would discard the
	// exact recovery/finalizer authority for still-live data. Leave the record
	// restoring so the next sweep retries, and keep the provision projection.
	failed := false
	for _, retained := range e.RetainedVolumeNames {
		newCanonical := retainedToNewCanonical(retained, e.OriginalLeaseUUID, e.NewLeaseUUID)
		if rerr := b.renameIfPresentUsing(ctx, renameVolume, newCanonical, retained); rerr != nil {
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
	resourceProfiles, err := b.restoreRetainedVolumeQuotasUsing(ctx, &e, ensureQuota)
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
	// leaves Restoring + the live reservation as a level-triggered retry vehicle.
	// If the subsequent handback CAS fails, the durable OperationFailed state
	// makes the next pass select the same rollback plan without consulting stale
	// projection state.
	//
	// Keep operation settlement, Restoring→Active handback, pool release, and
	// projection removal indivisible from recovery publication. Physical teardown
	// and re-quarantine above need no snapshot lock because the Restoring row is
	// still durable authority throughout them.
	b.recoverySnapshotMu.RLock()
	defer b.recoverySnapshotMu.RUnlock()
	if err := b.settleRolledBackRestoreOperation(e, plan); err != nil {
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
	if !ok {
		return fmt.Errorf("reconcile restoring retention %q lost generation %d authority",
			e.OriginalLeaseUUID, e.Generation)
	}
	b.removeProvision(e.NewLeaseUUID)
	return nil
}

// settleRolledBackRestoreOperation accepts only the sealed OperationFailed
// outcome selected by the operation coordinator. Retention recovery cannot
// turn a Pending row or an inventory observation into destructive authority.
func (b *Backend) settleRolledBackRestoreOperation(
	e shared.RetentionEntry,
	plan restoreRollbackPlan,
) error {
	failed, ok := plan.(restoreRollbackFailed)
	if !ok {
		return fmt.Errorf("restore rollback cannot settle plan %T", plan)
	}
	if failed.outcome.SourceGeneration() != e.Generation {
		return fmt.Errorf(
			"failed restore outcome generation %d differs from source generation %d",
			failed.outcome.SourceGeneration(), e.Generation,
		)
	}
	return nil
}

// currentRestoreOperation re-reads the sealed pending/succeeded/failed state
// while the caller holds both destination fences. Batch snapshots are not
// admissible here: a restore can transition its durable state while a retention
// sweep waits. Any pending claim that touches only one side (or a different
// generation) is conflicting authority and therefore fails closed.
func (b *Backend) currentRestoreOperation(e shared.RetentionEntry) (shared.OperationRecoveryState, error) {
	if b.operationSettlement == nil {
		return nil, errors.New("operation settlement is required to recover a restore operation")
	}
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return nil, err
	}
	var exactPending bool
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
		if exactPending {
			return nil, fmt.Errorf("multiple operation intents own restore destination %q", e.NewLeaseUUID)
		}
		if err := b.validateRestoreOperationAuthority(claim, e); err != nil {
			return nil, err
		}
		exactPending = true
	}
	probe, err := b.operationSettlement.NewOperationIntentProbe(
		e.NewLeaseUUID, e.DestinationCallbackURL,
	)
	if err != nil {
		return nil, fmt.Errorf("construct restore operation recovery probe: %w", err)
	}
	state, err := b.operationSettlement.LookupOperationRecovery(probe)
	if err != nil {
		return nil, err
	}
	switch state.(type) {
	case shared.OperationIntentClaim:
	case shared.OperationSucceeded:
	case shared.OperationFailed:
	default:
		return nil, fmt.Errorf("restore destination %q has unknown operation state %T",
			e.NewLeaseUUID, state)
	}
	if err := b.validateRestoreOperationAuthority(state, e); err != nil {
		return nil, err
	}
	return state, nil
}

// validateRestoreOperationAuthority joins a sealed operation state to a
// Restoring row decoded by RetentionStore. Keep destination and backend/storage
// checks here: the source-first callers have not performed an exact point probe.
// The remaining comparisons express cross-journal relations, not row validation:
//   - Both decoders bind OperationID to CallbackURL and require the exact derived
//     LifecycleCallbackURL. Comparing CallbackURL therefore compares all three.
//   - Restoring-row decoding requires a nonempty source and positive generation.
//     Operation decoding forbids provision source metadata, so matching these
//     fields also proves restore kind.
//   - Bound retention construction requires a valid manifest. Even a nil DTO
//     marshals to JSON null, which the manifest comparison rejects because a
//     sealed operation requires a valid stack manifest, before Services is read.
//
// These ingestion prerequisites are tested separately from the semantic join in
// restore_authority_guard_test.go; malformed DTOs are not sealed capabilities.
func (b *Backend) validateRestoreOperationAuthority(
	claim shared.OperationRecoveryState,
	e shared.RetentionEntry,
) error {
	if claim.LeaseUUID() != e.NewLeaseUUID ||
		claim.SourceLeaseUUID() != e.OriginalLeaseUUID ||
		claim.SourceGeneration() != e.Generation {
		return errors.New("restore operation state differs from source/destination generation authority")
	}
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
	if claim.CallbackURL() != e.DestinationCallbackURL {
		return errors.New("restore intent callback pair differs from source finalizer authority")
	}
	manifestBytes, err := json.Marshal(e.StackManifest)
	if err != nil {
		return fmt.Errorf("marshal restore source finalizer manifest: %w", err)
	}
	if !bytes.Equal(claim.Manifest(), manifestBytes) {
		return errors.New("restore intent manifest differs from source finalizer authority")
	}
	expectedHealthServices := make([]string, 0, len(e.StackManifest.Services))
	// Matching the sealed, validated manifest also proves every service is
	// nonnil; ParsePayload validates the complete map before issuing a claim.
	for service, serviceManifest := range e.StackManifest.Services {
		if serviceManifest.HasActiveHealthCheck() {
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
	case e.DestinationOperationID.IsZero():
		if !active.OperationID.IsZero() || active.RuntimeAuthority != nil {
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
// durable Active-to-Reaping transitions inside one close. Bounded eviction
// converges over subsequent closes while the count cap temporarily overshoots.
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
// The snapshot is the Active-candidate half of the caller's single
// ListTenantRetentionCandidates view (the read DTO half is shared with
// boundPartition), never re-read here. Each
// successfully-marked record is pruned from the in-memory snapshot between
// passes so an L2 eviction also counts toward L1. This function never mutates
// another lease's substrate: it only hands durable Reaping ownership to the
// periodic retention reaper. refreshRetentionAccounting runs only when a pass
// engaged, so the still-physical Reaping footprint remains counted.
func (b *Backend) evictRetentionsToCap(ctx context.Context, tenant string, budget retentionBudget,
	partition string, snapshot []shared.ActiveRetentionCandidate, excludeLease string) error {
	if b.retentionStore == nil || tenant == "" || (budget.CountCap <= 0 && budget.PerPartCount <= 0) {
		return nil
	}
	var active []shared.ActiveRetentionCandidate
	for _, candidate := range snapshot {
		e := candidate.Entry()
		if e.OriginalLeaseUUID == excludeLease {
			continue // never evict the closing lease's own record
		}
		active = append(active, candidate)
	}
	// Deterministic total order: oldest-first, equal CreatedAt broken by
	// ascending UUID. Given the same store state the evicted set is a pure
	// function — previously equal-timestamp order was unspecified.
	sort.SliceStable(active, func(i, j int) bool {
		left, right := active[i].Entry(), active[j].Entry()
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.OriginalLeaseUUID < right.OriginalLeaseUUID
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
		var part []shared.ActiveRetentionCandidate
		for _, candidate := range active {
			e := candidate.Entry()
			if e.Partition == partition {
				part = append(part, candidate)
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
			for _, candidate := range active {
				e := candidate.Entry()
				if _, gone := marked[e.OriginalLeaseUUID]; !gone {
					pruned = append(pruned, candidate)
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

// evictOldest transfers the oldest Active records to durable Reaping ownership.
// It deliberately performs no physical destroy: close execution is bound to one
// exact ClosePhysicalSubject and cannot safely mutate another lease's substrate.
// The periodic retention reaper consumes the Reaping proof and performs that
// lease's separately fenced physical cleanup.
func (b *Backend) evictOldest(_ context.Context, ordered []shared.ActiveRetentionCandidate, level string, capValue int) (map[string]struct{}, bool, error) {
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
	for i := 0; i < toEvict; i++ {
		candidate := ordered[i]
		e := candidate.Entry()
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap",
			"tenant", e.Tenant, "lease_uuid", e.OriginalLeaseUUID, "level", level, "cap", capValue,
			"partition", shared.TruncatePartitionRaw(e.Partition))
		// The names this returns are the record's stored list; the finalizer derives its
		// own from disk (destroyReapingVolumes), so they are deliberately discarded. The
		// CAS itself is what matters here — active→reaping must be atomic so the record is
		// never deleted before its volumes are confirmed gone.
		_, ok, merr := b.retentionStore.BeginReaping(candidate)
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
// (fred-{lease}-* and fred-retained-{lease}-*) intersected with the ownership table.
// Crucially, the exact reaping tombstone scopes both the lease and destructive authority;
// there is no inference-driven global orphan destroyer.
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
// The operation journal now prevents the ENG-659 collision by construction. Every current
// Reaping row descends from a completed close, whose permanent per-lease mutation head
// prevents a restore operation from later acquiring that lease as its destination. In the
// other order, the pending restore operation prevents close admission. The finalizer still
// checks the owner table because a live provision or unreadable authority must always win
// over destructive cleanup.
//
// Every destroy still goes through volumeOp.destroy (volume_destroy.go), which resolves
// ownership from the live provision map plus the retention store and re-checks the live
// claim under the volume's stripe (ENG-681). This function's job is to translate that
// primitive's per-name verdicts into the record's lifecycle, which is the part only the
// finalizer knows: refused means keep the record, failed means keep it and count a leak,
// all-gone means Delete.
//
// Fail-safe on an unprovable claim set OR an unreadable volume root: destroy NOTHING this
// pass and KEEP the record. We cannot tell this record's own leak from another lease's
// adopted data, and only one of those two mistakes is reversible. Waiting costs nothing:
// the record IS the retry vehicle, so the next sweep re-attempts without a reboot. The
// record is dropped only on the positive fact that the footprint is gone.
func (b *Backend) destroyReapingVolumesUsing(
	ctx context.Context,
	idx *managedVolumeIndex,
	reapingProof shared.ReapingRetentionProof,
	destroyVolumes volumeDestroyMutationCapability,
) bool {
	orig := reapingProof.Entry().OriginalLeaseUUID
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
	if !reapingProof.Valid() || orig == "" {
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		logger.Error("reaping: exact tombstone authority is unavailable; keeping the record")
		return false
	}
	// owner "" — a tombstone is a scheduled destroy, not an assertion of ownership, so it
	// is entitled to exactly the volumes NOTHING claims. That also refuses a name a LIVE
	// provision holds, which is reachable when a tombstoned lease is later re-provisioned
	// (chain-ACTIVE, no provision → the reconciler re-provisions it, and the sweep would
	// otherwise reap the fresh volume out from under it).
	op := b.volumeOp("", logger)

	names, derr := idx.footprint(ctx, orig)
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
		if deleted, delErr := b.retentionStore.DeleteReaped(reapingProof); delErr != nil {
			logger.Warn("reaping: footprint already gone but record delete failed; next sweep retries", "error", delErr)
			return false
		} else if !deleted {
			logger.Warn("reaping: exact tombstone disappeared before delete; next sweep reclassifies")
			return false
		}
		return true
	}

	rep := op.destroy(destroyVolumes, ctx, destroySiteReaping, names...)

	if len(rep.Unproven) > 0 {
		retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable).Inc()
		b.logger.Error("reaping: ownership unprovable; destroyed nothing this pass (record stays reaping for retry)",
			"lease_uuid", orig, "error", rep.err())
		return false
	}
	// A restore cannot acquire destination authority for a lease that has crossed its
	// permanent close boundary, so a reaping namespace and restore adoption are mutually
	// exclusive by construction. The only reachable positive claim is a live lease that
	// the reconciler re-provisioned after this tombstone was written.
	if len(rep.Claimed) > 0 {
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
		// record is NOT deleted: a claimed name still on disk means dropping the record
		// would drop both the retry vehicle and the reaping projection that counts those
		// bytes — an under-count, the exact ENG-376 invariant. The next sweep re-derives
		// the footprint, so no mutable destroy list is needed.
		b.logger.Warn(
			"reaping: record kept (volume(s) held by a live provision); "+
				"this clears when that lease is next closed cleanly — do NOT reclaim by hand",
			"lease_uuid", orig, "skipped", skipped, "names", len(names),
		)
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
	switch remaining, verr := confirm.footprint(ctx, orig); {
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

	if deleted, derr := b.retentionStore.DeleteReaped(reapingProof); derr != nil {
		logger.Warn("reaping: destroy ok but record delete failed; next sweep retries", "error", derr)
		return false
	} else if !deleted {
		logger.Warn("reaping: exact tombstone disappeared before delete; next sweep reclassifies")
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
func (i *managedVolumeIndex) footprint(ctx context.Context, orig string) ([]string, error) {
	if !i.resolved {
		i.all, i.err = i.b.volumes.ListForProof(ctx)
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

// newRetentionOrphanPruner binds a complete physical inventory to the durable
// retention store once. Sweep callers receive no row selector or absence proof.
func newRetentionOrphanPruner(b *Backend) (*shared.RetentionOrphanPruner, error) {
	inventory, err := shared.BindRetentionOrphanVolumeInventory(b.volumes.ListForProof)
	if err != nil {
		return nil, err
	}
	return shared.NewRetentionOrphanPruner(
		b.retentionStore,
		b.cfg.RetentionOrphanConfirmations,
		b.cfg.VolumeDataPath != "",
		inventory,
	)
}

func (b *Backend) reconcileOrphanedRetentionsUsing(ctx context.Context) (int, error) {
	if b.retentionStore == nil {
		return 0, nil
	}
	if b.orphanPruner == nil {
		return 0, errors.New("retention orphan pruning requires a construction-bound pruner")
	}
	result, err := b.orphanPruner.Sweep(ctx)
	switch result.SkipReason {
	case shared.RetentionOrphanSkipDisabled:
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipDisabled).Inc()
	case shared.RetentionOrphanSkipInventoryError:
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError).Inc()
	case shared.RetentionOrphanSkipStoreError:
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipStoreError).Inc()
	}
	if result.Raced > 0 {
		retentionOrphanSkipsTotal.WithLabelValues(orphanSkipRaced).Add(float64(result.Raced))
	}
	if result.Pruned > 0 {
		retentionOrphansPrunedTotal.Add(float64(result.Pruned))
		b.logger.Info("pruned orphaned retention records (backing volumes confirmed absent)", "count", result.Pruned)
	}
	return result.Pruned, err
}

// reapExpiredRetentions hard-deletes retained volumes past RetentionMaxAge.
// Returns the count of records FULLY reaped (all volumes destroyed AND the
// record removed). Each expired active record is atomically transitioned to
// reaping before its volumes are destroyed, so a destroy failure leaves the
// record as a counted finalizer tombstone rather than creating an under-count.
func (b *Backend) reapExpiredRetentionsUsing(
	ctx context.Context,
	destroyVolumes volumeDestroyMutationCapability,
) (int, error) {
	if destroyVolumes == nil {
		return 0, errBackgroundMaintenanceUnavailable
	}
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListExpiredCandidates(b.cfg.RetentionMaxAge)
	if err != nil {
		return 0, err
	}
	var n int
	idx := b.newManagedVolumeIndex() // one enumeration for the whole reap pass
	for _, candidate := range candidates {
		e := candidate.Entry()
		// Consume the exact listed Active candidate (the record is NEVER deleted before its volumes are
		// confirmed gone, so a destroy failure cannot drop a still-on-disk footprint).
		// Stored names discarded — the finalizer derives the footprint from disk. Only the
		// atomic transition matters here (see BeginReaping above).
		reapingProof, ok, merr := b.retentionStore.BeginExpiredReaping(candidate, b.cfg.RetentionMaxAge)
		if merr != nil {
			b.logger.Error("reap: store error", "lease_uuid", e.OriginalLeaseUUID, "error", merr)
			continue
		}
		if !ok {
			continue // concurrently claimed/changed since the snapshot — skip
		}
		if b.destroyReapingVolumesUsing(ctx, idx, reapingProof, destroyVolumes) {
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
func (b *Backend) retryReapingRecordsUsing(
	ctx context.Context,
	destroyVolumes volumeDestroyMutationCapability,
) error {
	if destroyVolumes == nil {
		return errBackgroundMaintenanceUnavailable
	}
	if b.retentionStore == nil {
		return nil
	}
	proofs, err := b.retentionStore.ListReapingProofs()
	if err != nil {
		return err
	}
	idx := b.newManagedVolumeIndex() // one enumeration for the whole retry pass
	for _, proof := range proofs {
		b.destroyReapingVolumesUsing(ctx, idx, proof, destroyVolumes)
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
func (b *Backend) runRetentionSweepUsing(
	ctx context.Context,
	renameVolume backgroundVolumeRename,
	teardown teardownMutationCapability,
	destroyVolumes volumeDestroyMutationCapability,
	ensureQuota backgroundVolumeQuota,
) error {
	if renameVolume == nil || teardown == nil || destroyVolumes == nil || ensureQuota == nil {
		return errBackgroundMaintenanceUnavailable
	}
	if err := b.requireStorageIdentity(ctx); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	var errs []error
	if _, err := b.reapExpiredRetentionsUsing(ctx, destroyVolumes); err != nil {
		errs = append(errs, fmt.Errorf("reap expired: %w", err))
	}
	// A nil store means no record can exist, so the remaining stages have nothing to read.
	// Not an early RETURN any more: the accounting refresh and the outcome record below
	// are unconditional. (Unreachable in practice — retentionSweepInterval gates the
	// reaper off entirely when the store is nil — but the guard is what makes that a
	// belt-and-braces fact rather than a dependency.)
	if b.retentionStore != nil {
		if err := b.retryReapingRecordsUsing(ctx, destroyVolumes); err != nil {
			errs = append(errs, fmt.Errorf("retry reaping: %w", err))
		}
		if recs, err := b.retentionStore.ListRestoring(); err != nil {
			errs = append(errs, fmt.Errorf("list restoring: %w", err))
		} else {
			for _, e := range recs {
				// A per-record failure deliberately parks the finalizer for the
				// next sweep, but it is still part of this sweep's outcome. Keep
				// reconciling independent records and join every exact failure so
				// monitoring cannot report success while a restore is wedged.
				if err := b.reconcileRestoringWithAuthorityUsing(
					ctx, e, renameVolume, teardown, destroyVolumes, ensureQuota,
				); err != nil {
					errs = append(errs, fmt.Errorf(
						"reconcile restoring source %q destination %q: %w",
						e.OriginalLeaseUUID, e.NewLeaseUUID, err,
					))
				}
			}
		}
		// ENG-370: prune orphaned records BEFORE ENG-360's accounting refresh so the
		// retained-disk projection reflects this sweep's prunes. The refresh runs even
		// when the prune returns a fail-safe error (the prune mutated nothing in that
		// case, but the reaper above may have, and refresh is keep-last-value on a store
		// read error).
		if _, err := b.reconcileOrphanedRetentionsUsing(ctx); err != nil {
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
// later atomic RestoreSettlement.ClaimForRestore (loser → ErrNotRestorable). Do NOT cache the
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

// Restore adopts a soft-deleted lease's retained volumes into a NEW lease and
// brings up its stack from the retained manifest (ENG-325). The new lease is
// reserved at Provisioning and driven through the existing replace machinery via
// evRestoreRequested (Provisioning→Restarting→Ready|Failed).
//
// Ordering is load-bearing:
//
//	(a) validate against the retained record (read-only),
//	(b) under the recovery snapshot fence, commit the exact operation intent,
//	(c) reserve the new-lease provision and allocate pool slots,
//	(d) ATOMICALLY claim active→restoring (closes the prelude-vs-reaper race),
//	(e) hand off to the actor before any substrate mutation,
//	(f) the Started executor adopts the volumes and brings up the stack.
//
// Construction-bound settlement owns terminal outcomes. An ambiguous worker
// result preserves the exact Started intent, restoring finalizer, and live
// reservation until operation recovery proves completion or rolls back adoption.
//
// Synchronous errors (validation, already-provisioned, insufficient resources,
// not-retained, not-restorable) are returned to the caller; asynchronous outcomes
// flow via the lease callback.
func (b *Backend) Restore(ctx context.Context, request backend.RestoreRequest) error {
	req := newRestoreOperationInput(request)
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
	// every single-service lease. The private request type owns its backing array,
	// so normalization cannot mutate the caller's DTO while preparing async work.
	if err := req.normalizeItems(); err != nil {
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
	// Bridge admission into durable Restoring authority as one recovery-visible
	// transition. A recovery snapshot that began before this lock cannot observe
	// transient destination substrate or accounting: Restore waits until that
	// snapshot is published before creating its intent. Once admitted, the intent
	// and then the Restoring row protect every pre-claim side effect across crashes.
	b.recoverySnapshotMu.RLock()
	recoverySnapshotHeld := true
	defer func() {
		if recoverySnapshotHeld {
			b.recoverySnapshotMu.RUnlock()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
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
	if !intent.Valid() {
		return errors.New("created restore operation intent returned no claim")
	}
	err = b.checkOperationReleaseCapacity(intent)
	if err != nil {
		return b.refuseOperationIntent(intent, fmt.Errorf(
			"%w: reserve restore success release: %w",
			backend.ErrInsufficientResources,
			err,
		))
	}
	restoreClaimCandidate, err := b.restoreSettlement.PrepareRestoreClaim(intent)
	if err != nil {
		return b.refuseOperationIntent(intent, fmt.Errorf(
			"prepare restore source claim: %w",
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
			ActiveReleaseVersion: 0,
			ActiveOperationID:    shared.OperationID{},
			Items:                slices.Clone(req.Items),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
			ContainerIDs:         make([]string, 0),
			StackManifest:        rec.StackManifest,
			ServiceContainers:    nil,
		},
	}.materialize()
	b.provisionsMu.Unlock()

	// (c) Reserve pool slots for the restore atomically. Restore adopts existing
	// volumes (rename, not fresh disk), so disk capacity is gated once on the
	// AGGREGATE promote delta — the growth of the lease's total DiskMB above its
	// already-committed retained footprint — while CPU/mem/tenant are gated per
	// instance. TryAllocateAdoptAll does the whole reservation under a single pool
	// lock: the gate is EXACT (no per-volume double-count of the retained bytes
	// still in the projection until RestoreSettlement.ClaimForRestore), ATOMIC (no concurrent
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
	claimedProof, err := b.restoreSettlement.ClaimForRestore(
		restoreClaimCandidate,
		b.cfg.RetentionMaxAge,
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
	claimed := claimedProof.Entry()
	b.recoverySnapshotMu.RUnlock()
	recoverySnapshotHeld = false

	// Claim flipped the record active→restoring (drops it from the active
	// projection); the live allocation above already counts the bytes, so this
	// keeps the gauge/projection consistent without an under-count window.
	b.refreshRetentionAccounting()

	// (e) Hand off before adopting any volume. The construction-bound restore
	// handler first durably advances the exact operation to Started, then derives
	// every rename from that opaque subject. An actor rejection therefore has no
	// physical side effect to unwind.
	opCtx, opCancel := b.shutdownAwareContext()
	command, ack, commandErr := leasesm.NewRestoreCommand(opCtx, intent)
	if commandErr != nil {
		opCancel()
		return b.rollbackUnacceptedRestoreAdoption(
			req.LeaseUUID, allocatedIDs, &claimed, intent, commandErr, logger,
		)
	}
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, command); routeErr != nil {
		opCancel()
		// Worker never ran; no actor transition will flip Status — drop the
		// reservation (dropProvision=true).
		return b.rollbackUnacceptedRestoreAdoption(
			req.LeaseUUID, allocatedIDs, &claimed, intent, routeErr, logger,
		)
	}
	acceptance, err := b.awaitAsyncAcceptance(ctx, ack.Result())
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
			req.LeaseUUID, allocatedIDs, &claimed, intent, err, logger,
		)
	default:
		return fmt.Errorf("invalid restore acceptance state %d", acceptance)
	}
}

// finalizeRestoredLease records the NEW lease's active release, then — and only
// then — drops the restoring retention record. recoverState rehydrates
// prov.StackManifest ONLY from releaseStore.LatestActive, and Restart hard-fails on
// a nil manifest, so the release must be written for the restored lease to survive a
// backend restart. The lease is already Ready here, so it is written directly as
// active (mirrors provision.go's on-success Append).
//
// The retention record is the adopted volume's FINALIZER (Kubernetes-style): while
// it exists (restoring), the ownership table claims the adopted canonical volume and
// reconcileRestoring finalizes it once the
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
	ctx context.Context,
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
	operationRelease *shared.OperationReleaseCandidate,
	logger *slog.Logger,
) shared.OperationReleaseCommitted {
	committed, err := b.ensureRestoredReleaseStrict(
		ctx, shared.LeaseRecoveryScope{}, leaseUUID, rec, effectiveItems, operationRelease,
	)
	if err != nil {
		// Keep the finalizer: the adopted volume stays protected until a later
		// reconcileRestoring sweep or an Update durably records the release and drops the
		// record. Tradeoff while it lingers (only under a sustained release-store outage):
		// the ORIGINAL lease UUID reports Retained (info.go maps restoring→retained) even
		// though the restore is done, and a Restore-retry from the original is rejected
		// (RestoreSettlement.ClaimForRestore needs Active). Both self-heal once the store recovers and the
		// record is dropped; restoreFinalizerPendingTotal makes each initial kept-
		// pending event observable. Reconciliation retries do not re-increment it.
		restoreFinalizerPendingTotal.Inc()
		logger.Warn("restore ok but destination Release is not durable; keeping retention record as the adopted volume's finalizer (ENG-523)",
			"lease_uuid", leaseUUID, "original_lease_uuid", rec.OriginalLeaseUUID, "error", err)
		return shared.OperationReleaseCommitted{}
	}
	return committed
}

// finalizeRestoredLeaseStrict is the exact, idempotent commit used when a
// caller must know whether ownership transferred before it may continue (for
// example, close admission). nil means both the destination release and source
// finalizer deletion are durable. Any error leaves the source record in place,
// so callers can fail closed without guessing which lease owns the bytes.
func (b *Backend) finalizeRestoredLeaseStrict(
	ctx context.Context,
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
) error {
	if _, err := b.ensureRestoredReleaseStrict(
		ctx, shared.LeaseRecoveryScope{}, leaseUUID, rec, effectiveItems, nil,
	); err != nil {
		return err
	}
	return b.deleteRestoreFinalizerStrict(leaseUUID, rec)
}

// ensureRestoredReleaseStrict durably commits destination ownership without
// consuming the source finalizer. Callback settlement paths use this first,
// settle the exact operation second, and only then delete the finalizer; every
// crash boundary therefore leaves a level-triggered retry owner.
func (b *Backend) ensureRestoredReleaseStrict(
	ctx context.Context,
	recoveryScope shared.LeaseRecoveryScope,
	leaseUUID string,
	rec *shared.RetentionEntry,
	effectiveItems []backend.LeaseItem,
	operationRelease *shared.OperationReleaseCandidate,
) (shared.OperationReleaseCommitted, error) {
	if rec == nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restore source finalizer is required")
	}
	if leaseUUID == "" || rec.OriginalLeaseUUID == "" {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restore source and destination lease UUIDs are required")
	}
	if rec.Status != shared.RetentionStatusRestoring || rec.NewLeaseUUID != leaseUUID {
		return shared.OperationReleaseCommitted{}, fmt.Errorf(
			"restore source finalizer does not own destination %q (status=%q new_lease_uuid=%q)",
			leaseUUID, rec.Status, rec.NewLeaseUUID,
		)
	}
	if rec.OriginalLeaseUUID == leaseUUID {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restore source and destination lease UUIDs must differ")
	}
	if rec.Generation <= 0 {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restore source finalizer generation must be positive")
	}
	if rec.StackManifest == nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restored manifest is required")
	}
	if _, err := backend.ValidateOperationQuantities(rec.Items); err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore source items: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(effectiveItems); err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored effective items: %w", err)
	}
	if len(rec.DestinationItems) > 0 || len(rec.DestinationResourceProfiles) > 0 {
		if _, err := backend.ValidateOperationQuantities(rec.DestinationItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority items: %w", err)
		}
		if err := itemsShapeMatch(rec.Items, rec.DestinationItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored item shape: %w", err)
		}
		if err := validateDockerResourceProfiles(
			rec.DestinationItems,
			rec.DestinationResourceProfiles,
		); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority resource profiles: %w", err)
		}
		if err := manifest.ValidateStackAgainstItems(rec.StackManifest, rec.DestinationItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored manifest topology: %w", err)
		}
	}
	if b.releaseStore == nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("release store is required")
	}
	if b.retentionStore == nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("retention store is required")
	}
	b.provisionsMu.RLock()
	provision, exists := b.provisions[leaseUUID]
	if !exists || !slices.Equal(provision.Items, effectiveItems) {
		b.provisionsMu.RUnlock()
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restored live provision does not match finalizer items")
	}
	if provision.Tenant != rec.Tenant || provision.ProviderUUID != rec.ProviderUUID {
		b.provisionsMu.RUnlock()
		return shared.OperationReleaseCommitted{}, fmt.Errorf(
			"restored live provision identity does not match finalizer tenant/provider",
		)
	}
	if provision.StackManifest == nil {
		b.provisionsMu.RUnlock()
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restored live provision manifest is required")
	}
	liveItems := slices.Clone(provision.Items)
	liveManifestBytes, marshalLiveErr := json.Marshal(provision.StackManifest)
	liveManifest := provision.StackManifest
	liveResourceProfiles := shared.CloneSKUResourceSnapshot(provision.ResourceProfiles)
	b.provisionsMu.RUnlock()
	if marshalLiveErr != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("marshal restored live provision manifest: %w", marshalLiveErr)
	}
	if err := itemsShapeMatch(rec.Items, liveItems); err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored live item shape: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(liveManifest, liveItems); err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored live manifest topology: %w", err)
	}
	if err := validateDockerResourceProfiles(liveItems, liveResourceProfiles); err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restored resource profiles: %w", err)
	}

	// A newer successful maintenance operation may already have durably published
	// the live generation while this restore finalizer lingered. That exact active
	// Release is sufficient ownership authority. Otherwise append only from the
	// destination snapshot atomically bound into the source retention claim —
	// never from live state or mutable SKU configuration. This is the crash-safe
	// bridge when the restore succeeded and its first Release append failed. The
	// durable successful operation outcome authorizes this branch independently.
	existing, err := b.releaseStore.LatestActive(leaseUUID)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("read restored active release: %w", err)
	}
	existingOwnsLive := existing != nil &&
		existing.OperationID == rec.DestinationOperationID &&
		bytes.Equal(existing.Manifest, liveManifestBytes) &&
		slices.Equal(existing.Items, liveItems) &&
		slices.Equal(existing.ResourceProfiles, liveResourceProfiles) &&
		((rec.DestinationOperationID.IsZero() && existing.RuntimeAuthority == nil) ||
			(rec.DestinationOperationID.Valid() && existing.RuntimeAuthority != nil &&
				existing.OperationID == rec.DestinationOperationID &&
				releaseRuntimeAuthorityMatchesRetention(existing.RuntimeAuthority, *rec)))
	if !existingOwnsLive {
		authorityItems := slices.Clone(rec.DestinationItems)
		authorityProfiles := shared.CloneSKUResourceSnapshot(rec.DestinationResourceProfiles)
		if len(authorityItems) == 0 || len(authorityProfiles) == 0 {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("restore source finalizer has no exact destination authority and no active release owns the live generation")
		}
		if _, err := backend.ValidateOperationQuantities(authorityItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority items: %w", err)
		}
		if err := itemsShapeMatch(rec.Items, authorityItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority shape: %w", err)
		}
		if err := validateDockerResourceProfiles(authorityItems, authorityProfiles); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority resource profiles: %w", err)
		}
		if err := manifest.ValidateStackAgainstItems(rec.StackManifest, authorityItems); err != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("validate restore destination authority manifest topology: %w", err)
		}
		authorityManifestBytes, marshalErr := json.Marshal(rec.StackManifest)
		if marshalErr != nil {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("marshal restore destination authority manifest: %w", marshalErr)
		}
		if !slices.Equal(effectiveItems, authorityItems) || !slices.Equal(liveItems, authorityItems) {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("restored live provision items do not match durable destination authority")
		}
		if !slices.Equal(liveResourceProfiles, authorityProfiles) {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("restored live provision resource profiles do not match durable destination authority")
		}
		if !bytes.Equal(liveManifestBytes, authorityManifestBytes) {
			return shared.OperationReleaseCommitted{}, fmt.Errorf("restored live provision manifest does not match durable destination authority")
		}
		if operationRelease == nil {
			return shared.OperationReleaseCommitted{}, errors.New(
				"restore destination has no sealed operation release authority",
			)
		}
	}
	if operationRelease == nil {
		return shared.OperationReleaseCommitted{}, nil
	}
	physical, err := b.operationSettlement.RecoverOperationExecution(
		ctx, recoveryScope, operationRelease.Intent(),
	)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("classify restored active release: %w", err)
	}
	ready, ok := physical.(shared.OperationExecutionSuccess)
	if !ok {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("restored operation is not exactly Ready (%T)", physical)
	}
	committed, err := b.operationSettlement.CommitOperationSuccess(ready)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("record restored active release: %w", err)
	}
	return committed, nil
}

func (b *Backend) deleteRestoreFinalizerStrict(
	leaseUUID string,
	rec *shared.RetentionEntry,
) error {
	if rec == nil {
		return fmt.Errorf("restore source finalizer is required")
	}
	if rec.NewLeaseUUID != leaseUUID {
		return fmt.Errorf("restore source finalizer belongs to destination %q", rec.NewLeaseUUID)
	}
	proof, err := b.retentionStore.ProveRestoringSnapshot(*rec)
	if err != nil {
		if errors.Is(err, shared.ErrNoRetention) {
			return nil
		}
		return fmt.Errorf("prove exact restore source finalizer: %w", err)
	}
	deleted, err := b.retentionStore.DeleteRestoring(proof)
	if err != nil {
		return fmt.Errorf("delete restore source finalizer: %w", err)
	}
	if !deleted {
		return errors.New("exact restore source finalizer was not deleted")
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
// ZFS. RollbackRestoring would then publish the smaller immutable source footprint
// while the filesystem still permits growth to the larger destination cap.
// This helper closes that under-accounting window by requiring three proofs:
// the durable snapshot maps every retained volume to one exact old cap, current
// usage fits that cap, and the volume manager successfully reapplies it. The
// caller must leave the record Restoring and its live allocation counted on any
// error.
func (b *Backend) restoreRetainedVolumeQuotasUsing(
	ctx context.Context,
	rec *shared.RetentionEntry,
	ensureQuota backgroundVolumeQuota,
) ([]shared.SKUResourceSnapshot, error) {
	if ensureQuota == nil {
		return nil, errBackgroundMaintenanceUnavailable
	}
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
		if err := ensureQuota(ctx, target.name, target.diskMB); err != nil {
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

	if rec.NewLeaseUUID != newLeaseUUID {
		return false, fmt.Errorf("restore finalizer belongs to destination %q", rec.NewLeaseUUID)
	}
	proof, proofErr := b.retentionStore.ProveRestoringSnapshot(*rec)
	var commitErr error
	if proofErr != nil {
		commitErr = fmt.Errorf("prove exact restore rollback authority: %w", proofErr)
	} else {
		_, commitErr = b.retentionStore.RollbackRestoring(proof, resourceProfiles)
	}
	if commitErr != nil {
		// The durable owner did not change, so undo only our conservative add.
		// No projection writer can interleave while retentionAccountingMu is held.
		if rollbackErr := b.pool.SetRetainedDisk(previousRetainedMB); rollbackErr != nil {
			commitErr = errors.Join(commitErr, fmt.Errorf(
				"restore prior retained accounting after failed ownership CAS: %w", rollbackErr,
			))
		}
		return false, commitErr
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

// completeRestoreAdoptionRollback performs the make-before-break ownership
// handback after proving that no destination container or promoted quota
// remains. Callers that own a pre-actor restore
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

// rollbackUnacceptedRestoreAdoption compensates a synchronous Restore failure
// for which no actor worker can publish Ready. It intentionally settles the
// exact failed operation after physical/quota cleanup but before the source
// handback CAS. If settlement fails, the Restoring record and live allocation
// remain a level-triggered retry owner; only the dead Provisioning projection is
// removed so the periodic reconciler can enter its orphaned rollback arm.
func (b *Backend) rollbackUnacceptedRestoreAdoption(
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
	// Actor rejection occurs before StartOperationExecution. The restore handler
	// therefore cannot have renamed a volume, changed a quota, or invoked Compose.
	// Rolling back by touching Docker here would manufacture a substrate effect
	// outside the Started protocol. Reconstruct only the immutable accounting
	// snapshot needed for the durable source handback.
	resourceProfiles := shared.CloneSKUResourceSnapshot(rec.ResourceProfiles)
	if len(resourceProfiles) == 0 {
		var resolveErr error
		resourceProfiles, resolveErr = b.resolveResourceProfiles(rec.Items)
		if resolveErr != nil {
			b.removeProvision(leaseUUID)
			return fmt.Errorf("restore failed; resolve source accounting for refusal: %w", resolveErr)
		}
	}
	if err := validateDockerResourceProfiles(rec.Items, resourceProfiles); err != nil {
		b.removeProvision(leaseUUID)
		return fmt.Errorf("restore failed; validate source accounting for refusal: %w", err)
	}
	// Physical cleanup above is bridged by the Restoring row. From exact
	// operation settlement through source-authority handback, pool release, and
	// projection removal, recovery must observe either the complete before-state
	// or the complete after-state rather than resurrecting the destination from a
	// stale Docker inventory snapshot.
	b.recoverySnapshotMu.RLock()
	defer b.recoverySnapshotMu.RUnlock()
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
