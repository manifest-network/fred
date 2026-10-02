package docker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/util"
)

// Held volume deletions (ENG-1117).
//
// A deletion that the XFS manager could not finish for a reason confined to
// that one volume is held instead of stopping the Backend. The delete stage
// (the parent-durable typed directory name) is the only durable record; the
// hold itself lives in the manager's memory and is rebuilt from the stage at
// startup. While held:
//
//   - the name cannot be created or renamed, and its project ID stays
//     reserved;
//   - removal phase: tenant bytes may remain and the final path's absence is
//     not yet durable. Destroy answers ErrVolumeDeleteHeld, so every caller
//     keeps its own durable authority (the close stays pending, the reaping
//     record stays, the operation intent stays), and ListForProof keeps
//     listing the name, so no consumer can infer completion from absence;
//   - residual phase: the final path is durably gone and only the stage and
//     the project remain, pending the zero-usage proof, the limit clear and
//     the stage removal. Destroy answers nil after an Lstat proves the final
//     path absent, the caller settles, and the admission pool counts the
//     project's footprint instead (heldResidualMB);
//   - unsized phase: the final path was observed gone, a caller may already
//     have settled (a previous process could have answered it), and the
//     project's footprint is not known. Destroy still answers
//     ErrVolumeDeleteHeld and ListForProof still lists the name, and the
//     admission pool withholds disk until the footprint is known: an unsized
//     footprint is never counted as zero.
//
// Only the background hold executor runs held work, through
// RetryHeldVolumeDelete in the storage-mutation bracket under the lease's
// namespace lock.

const (
	// volumeDeleteHoldInterval paces the hold executor. Its first pass runs as
	// soon as Start returns.
	volumeDeleteHoldInterval = 30 * time.Second
	// volumeDeleteHoldPassBudget bounds one pass over the due holds.
	volumeDeleteHoldPassBudget = 60 * time.Second
	// volumeDeleteHoldSlice bounds one hold's attempt, so one large tree
	// cannot hold its lease's namespace, or the pass, for long; an attempt cut
	// short is held with reason deadline and stays due.
	volumeDeleteHoldSlice = 15 * time.Second
	// volumeDeleteHoldMinSlice is the least budget worth starting an attempt.
	volumeDeleteHoldMinSlice = time.Second
	// volumeDeleteHoldResumeBudget bounds the close resumes a pass enqueues
	// for leases whose holds stopped holding their caller.
	volumeDeleteHoldResumeBudget = 30 * time.Second
	// volumeDeleteHoldComponent labels the executor's recovered panics.
	volumeDeleteHoldComponent = "docker_volume_delete_hold"
)

// volumeDeleteHoldView is a read-only copy of one hold. It grants nothing: the
// executor's retry re-reads the hold under the manager's lock.
type volumeDeleteHoldView struct {
	volume      managedVolumeName
	stage       string
	projectID   uint32
	phase       xfsDeleteHoldPhaseKind
	footprintMB int64
	reason      volumeDeleteHoldReason
	attempts    int
	since       time.Time
	lastAttempt time.Time
	nextAttempt time.Time
}

// phaseLabel is the hold's phase as reported in logs and metrics.
func (v volumeDeleteHoldView) phaseLabel() string { return v.phase.label() }

// holdsCaller reports whether a destroy of this hold's name answers
// ErrVolumeDeleteHeld, keeping its caller pending: the removal and unsized
// phases.
func (v volumeDeleteHoldView) holdsCaller() bool { return v.phase != holdPhaseResidual }

// Phase labels of a held deletion, in logs and metrics.
const (
	volumeDeleteHoldPhaseRemoval  = "removal"
	volumeDeleteHoldPhaseUnsized  = "unsized"
	volumeDeleteHoldPhaseResidual = "residual"
)

// volumeDeleteHoldPhases are every phase label, for metric pre-initialization.
var volumeDeleteHoldPhases = []string{
	volumeDeleteHoldPhaseRemoval, volumeDeleteHoldPhaseUnsized, volumeDeleteHoldPhaseResidual,
}

// volumeDeleteHoldSnapshot is one point-in-time view of a manager's pending
// deletions, and of the names its memory still tracks. Managers that never
// stage a deletion return the zero value, which reports nothing pending and
// nothing held.
type volumeDeleteHoldSnapshot struct {
	// pending holds every managed name with a delete stage, held or not.
	pending map[string]struct{}
	// holds holds the held subset, by managed name.
	holds map[string]volumeDeleteHoldView
	// staged holds every managed name with a create or delete stage.
	staged map[string]struct{}
	// mapped holds every managed name with a project mapping: a volume that
	// exists, or whose deletion has not released its project yet.
	mapped map[string]struct{}
}

// deletePending reports whether name has a delete stage, held or in flight.
// Such a name must never be retained, recreated or requota'd.
func (s volumeDeleteHoldSnapshot) deletePending(name string) bool {
	_, ok := s.pending[name]
	return ok
}

// callerHeld reports whether name is held in a phase that keeps its caller
// pending (removal or unsized), where a destroy answers ErrVolumeDeleteHeld
// without any work.
func (s volumeDeleteHoldSnapshot) callerHeld(name string) bool {
	hold, ok := s.holds[name]
	return ok && hold.holdsCaller()
}

// residualHeld reports whether name is held in the residual phase: its caller
// has settled, and admission counts its project's footprint.
func (s volumeDeleteHoldSnapshot) residualHeld(name string) bool {
	hold, ok := s.holds[name]
	return ok && hold.phase == holdPhaseResidual
}

// heldDeletionAccount is the admission pool's view of the held deletions
// whose caller may have settled: the summed footprint of the residual ones,
// and how many are unsized. An unsized hold is never counted as zero: while
// any exists, disk admission is withheld.
type heldDeletionAccount struct {
	residualMB int64
	unsized    int
}

// admissionAccount sums the residual footprints, saturating rather than
// wrapping, and counts the unsized holds.
func (s volumeDeleteHoldSnapshot) admissionAccount() heldDeletionAccount {
	var account heldDeletionAccount
	for _, hold := range s.holds {
		switch hold.phase {
		case holdPhaseUnsized:
			account.unsized++
		case holdPhaseResidual:
			if hold.footprintMB > math.MaxInt64-account.residualMB {
				account.residualMB = math.MaxInt64
				continue
			}
			account.residualMB += hold.footprintMB
		}
	}
	return account
}

// phaseCounts counts the holds in each phase.
func (s volumeDeleteHoldSnapshot) phaseCounts() map[string]int {
	counts := make(map[string]int, len(volumeDeleteHoldPhases))
	for _, phase := range volumeDeleteHoldPhases {
		counts[phase] = 0
	}
	for _, hold := range s.holds {
		counts[hold.phaseLabel()]++
	}
	return counts
}

// closeSlotState is the state of one managed-volume slot of a close or an
// operation, as far as held deletions are concerned.
type closeSlotState uint8

const (
	// closeSlotDone: nothing of the slot remains to wait for. It was
	// destroyed, its held deletion is residual (its caller settled), it was
	// retained, or it never had a volume (a stateless service).
	closeSlotDone closeSlotState = iota
	// closeSlotHeld: its deletion is held with the caller pending; only the
	// hold executor can finish it.
	closeSlotHeld
	// closeSlotRemaining: the volume still exists, or has a stage that no
	// hold owns: work the close itself still has to do or observe.
	closeSlotRemaining
)

// slotState classifies the slot whose canonical volume name is canonical,
// from memory only. A slot can also be held under its retained name.
func (s volumeDeleteHoldSnapshot) slotState(canonical string) closeSlotState {
	retained := retainedName(canonical)
	switch {
	case s.callerHeld(canonical) || s.callerHeld(retained):
		return closeSlotHeld
	case s.residualHeld(canonical) || s.residualHeld(retained):
		return closeSlotDone
	case s.stagedName(canonical) || s.stagedName(retained):
		return closeSlotRemaining
	case s.mappedName(canonical):
		return closeSlotRemaining
	default:
		// A retained name with a mapping is a retained volume: that slot is
		// done for this close.
		return closeSlotDone
	}
}

func (s volumeDeleteHoldSnapshot) stagedName(name string) bool {
	_, ok := s.staged[name]
	return ok
}

func (s volumeDeleteHoldSnapshot) mappedName(name string) bool {
	_, ok := s.mapped[name]
	return ok
}

// awaitsOnlyHeldDeletes reports whether the slots named by their canonical
// volume names wait on nothing but held deletions: at least one is held, and
// every other is done. It is the one predicate behind the close-churn skip,
// the HTTP short-circuit, the close-age gauges and the stopped drain proof.
func (s volumeDeleteHoldSnapshot) awaitsOnlyHeldDeletes(canonical []string) bool {
	held := 0
	for _, name := range canonical {
		switch s.slotState(name) {
		case closeSlotRemaining:
			return false
		case closeSlotHeld:
			held++
		}
	}
	return held > 0
}

// dueInOrder returns the holds due at now: unsized holds first, since they
// withhold disk admission, then least recently attempted first
// (never-attempted first), then by name, so one pass's budget rotates over
// every hold instead of starving the ones a busy hold keeps behind it.
func (s volumeDeleteHoldSnapshot) dueInOrder(now time.Time) []volumeDeleteHoldView {
	due := make([]volumeDeleteHoldView, 0, len(s.holds))
	for _, hold := range s.holds {
		if !now.Before(hold.nextAttempt) {
			due = append(due, hold)
		}
	}
	slices.SortFunc(due, func(a, b volumeDeleteHoldView) int {
		if aUnsized, bUnsized := a.phase == holdPhaseUnsized, b.phase == holdPhaseUnsized; aUnsized != bUnsized {
			if aUnsized {
				return -1
			}
			return 1
		}
		if c := a.lastAttempt.Compare(b.lastAttempt); c != 0 {
			return c
		}
		return strings.Compare(a.volume.value(), b.volume.value())
	})
	return due
}

// afterVolumeDestroy publishes, before a destroy's caller can act on its
// answer, any held-deletion phase change the destroy made: a deletion that
// just became residual settles its caller, so its project's footprint must
// already be in the admission pool. Every destroy entry point in the
// composition file calls it. A failed publication fails the destroy, so the
// caller keeps its own accounting and retries.
func (b *Backend) afterVolumeDestroy(destroyErr error) error {
	if err := b.refreshHeldResidualAccounting(); err != nil {
		return errors.Join(destroyErr, fmt.Errorf("account held volume deletions: %w", err))
	}
	return destroyErr
}

// destroyPrecheckVerdict is PrecheckDestroy's closed answer. Its zero value is
// the conservative one: take the namespace lock and call Destroy.
type destroyPrecheckVerdict uint8

const (
	// destroyPrecheckNeedsLock: the manager cannot answer from its own state.
	destroyPrecheckNeedsLock destroyPrecheckVerdict = iota
	// destroyPrecheckHeld: the name is held in a phase that keeps its caller
	// pending (removal or unsized); the destroy answers ErrVolumeDeleteHeld
	// without the lock and without any work.
	destroyPrecheckHeld
	// destroyPrecheckGone: an identity-bound Lstat proved the final path absent,
	// and the name has either a residual hold or no stage and no project
	// mapping; the destroy answers nil without the lock.
	destroyPrecheckGone
)

// precheckDestroy answers a destroy of id from the manager's own state,
// without the lease's namespace lock (ENG-1117): a hold that keeps its caller
// pending answers ErrVolumeDeleteHeld, and a positively absent name with nothing pending
// answers nil. answered is false when the destroy must take the lock. It is
// what keeps a close, the reaper, or HTTP Deprovision from waiting behind the
// hold executor's slice on a name that needs no work. A malformed name is
// answered with its parse error, as the locked destroy would.
func (b *Backend) precheckDestroy(id string) (answered bool, err error) {
	name, parseErr := parseManagedVolumeName(id)
	if parseErr != nil {
		return true, fmt.Errorf("destroy volume %q: %w", id, parseErr)
	}
	switch verdict, heldErr := b.volumes.PrecheckDestroy(name); verdict {
	case destroyPrecheckHeld:
		return true, heldErr
	case destroyPrecheckGone:
		return true, nil
	default:
		return false, nil
	}
}

// volumeDeleteHoldPassReport is what one executor pass did.
type volumeDeleteHoldPassReport struct {
	// attempted counts the holds the pass retried.
	attempted int
	// leftRemoval names the holds that stopped holding their caller during
	// the pass (completed, or durably gone, sized and residual): their callers
	// can now settle.
	leftRemoval []managedVolumeName
}

// runVolumeDeleteHoldPass retries the due holds, least recently attempted
// first, one bounded slice each, until ctx (the pass budget) runs out. Each
// retry runs through retry, which the composition binds to the storage-mutation
// bracket under the hold's lease namespace; it never takes the global
// recovery gate, so one hold never stalls another lease.
func (b *Backend) runVolumeDeleteHoldPass(
	ctx context.Context,
	now time.Time,
	retry backgroundHeldVolumeDeleteRetry,
) volumeDeleteHoldPassReport {
	var report volumeDeleteHoldPassReport
	if retry == nil {
		return report
	}
	for _, hold := range b.volumes.VolumeDeleteHolds().dueInOrder(now) {
		if ctx.Err() != nil {
			break
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < volumeDeleteHoldMinSlice {
			break
		}
		slice, cancel := context.WithTimeout(ctx, volumeDeleteHoldSlice)
		err := retry(slice, hold.volume.value())
		cancel()
		report.attempted++
		if err != nil && !errors.Is(err, ErrVolumeDeleteHeld) {
			b.logger.Warn("held volume delete retry failed",
				"volume_id", hold.volume.value(), "delete_stage", hold.stage, "error", err)
		}
		if hold.holdsCaller() && !b.volumes.VolumeDeleteHolds().callerHeld(hold.volume.value()) {
			report.leftRemoval = append(report.leftRemoval, hold.volume)
		}
	}
	return report
}

// volumeDeleteHoldLoop is the hold executor, the only runner of held
// deletion work. It runs on the Backend's lifetime, first as soon as it
// starts, then on volumeDeleteHoldInterval.
func (b *Backend) volumeDeleteHoldLoop() {
	ticker := time.NewTicker(volumeDeleteHoldInterval)
	defer ticker.Stop()
	for b.stopCtx.Err() == nil {
		b.runVolumeDeleteHoldIteration()
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runVolumeDeleteHoldIteration is one executor pass inside the cleanup panic
// boundary: retry the due holds under the pass budget, resume the closes whose
// holds stopped holding their caller, and sample the gauges.
func (b *Backend) runVolumeDeleteHoldIteration() {
	util.RunCleanupIteration(func() error {
		passCtx, cancel := context.WithTimeout(b.stopCtx, volumeDeleteHoldPassBudget)
		report := b.backgroundMaintenance.retryHeldVolumeDeletes(passCtx)
		cancel()
		b.resumeClosesAfterHeldDeletes(report.leftRemoval)
		b.sampleVolumeDeleteHoldMetrics()
		return nil
	}, volumeDeleteHoldComponent, func(any) {
		background.CleanupPanicsTotal.WithLabelValues(volumeDeleteHoldComponent).Inc()
	})
}

// resumeClosesAfterHeldDeletes enqueues a close resume for each lease whose
// held deletion just stopped holding its caller, so the close settles now instead
// of on the next reconcile. A lease whose command fence is busy is skipped:
// the live command holding it observes the same state.
func (b *Backend) resumeClosesAfterHeldDeletes(names []managedVolumeName) {
	if len(names) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(b.stopCtx, volumeDeleteHoldResumeBudget)
	defer cancel()
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		// A retained name belongs to the lease it was retained from, as in
		// closeAwaitsHeldDeletes.
		value := name.value()
		if isRetainedVolume(value) {
			value = canonicalFromRetained(value)
		}
		leaseUUID, ok := leaseUUIDFromVolumeName(value)
		if !ok {
			continue
		}
		if _, dup := seen[leaseUUID]; dup {
			continue
		}
		seen[leaseUUID] = struct{}{}
		if ctx.Err() != nil {
			return
		}
		if _, err := b.tryResumeRecoveredClose(ctx, leaseUUID); err != nil {
			b.logger.Warn("close resume after a held volume delete remains pending",
				"lease_uuid", leaseUUID, "error", err)
		}
	}
}

// leaseSlotNames returns the canonical volume name of every slot of a lease's
// items, one per instance, whether or not that instance ever had a volume.
func leaseSlotNames(leaseUUID string, items []backend.LeaseItem) []string {
	var names []string
	for _, item := range items {
		for i := range item.Quantity {
			names = append(names, canonicalVolumeName(leaseUUID, item.ServiceName, i))
		}
	}
	return names
}

// closeAwaitsHeldDeletes reports, from memory only, whether a pending close is
// waiting on nothing but held deletions: every REMAINING managed volume slot
// of its items is held with its caller pending (awaitsOnlyHeldDeletes; a slot
// already destroyed, residual, retained, or never created counts as done), and
// its projection records no container. Retrying such a close would only
// advance its durable generation and rewrite its diagnostics; the hold
// executor resumes it when one of its holds stops holding its caller. Only a
// projection can vouch that no container remains, so a close without one
// (cleanup-only) is never skipped: a remaining container could be the very
// writer keeping a hold from finishing.
func (b *Backend) closeAwaitsHeldDeletes(claim shared.CloseIntentClaim, holds volumeDeleteHoldSnapshot) bool {
	if !holds.awaitsOnlyHeldDeletes(leaseSlotNames(claim.LeaseUUID(), claim.Items())) {
		return false
	}
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	projection := b.provisions[claim.LeaseUUID()]
	return projection != nil && len(projection.ContainerIDs) == 0
}

// sampleVolumeDeleteHoldMetrics projects the manager's holds into the
// Backend-owned gauge.
func (b *Backend) sampleVolumeDeleteHoldMetrics() {
	for phase, count := range b.volumes.VolumeDeleteHolds().phaseCounts() {
		volumeDeleteHolds.WithLabelValues(phase).Set(float64(count))
	}
}
