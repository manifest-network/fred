package docker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

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
//     project's footprint instead (heldResidualMB).
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
	// for leases whose holds left the removal phase.
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
	residual    bool
	footprintMB int64
	reason      volumeDeleteHoldReason
	attempts    int
	since       time.Time
	lastAttempt time.Time
	nextAttempt time.Time
}

// phaseLabel is the hold's phase as reported in logs and metrics.
func (v volumeDeleteHoldView) phaseLabel() string {
	if v.residual {
		return volumeDeleteHoldPhaseResidual
	}
	return volumeDeleteHoldPhaseRemoval
}

// Phase labels of a held deletion, in logs and metrics.
const (
	volumeDeleteHoldPhaseRemoval  = "removal"
	volumeDeleteHoldPhaseResidual = "residual"
)

// volumeDeleteHoldSnapshot is one point-in-time view of a manager's pending
// deletions. Managers that never stage a deletion return the zero value,
// which reports nothing pending and nothing held.
type volumeDeleteHoldSnapshot struct {
	// pending holds every managed name with a delete stage, held or not.
	pending map[string]struct{}
	// holds holds the held subset, by managed name.
	holds map[string]volumeDeleteHoldView
}

// deletePending reports whether name has a delete stage, held or in flight.
// Such a name must never be retained, recreated or requota'd.
func (s volumeDeleteHoldSnapshot) deletePending(name string) bool {
	_, ok := s.pending[name]
	return ok
}

// removalHeld reports whether name is held in the removal phase, where a
// destroy answers ErrVolumeDeleteHeld without any work.
func (s volumeDeleteHoldSnapshot) removalHeld(name string) bool {
	hold, ok := s.holds[name]
	return ok && !hold.residual
}

// residualFootprintMB is the admission term for residual holds: the sum of
// their projects' footprints, saturating rather than wrapping.
func (s volumeDeleteHoldSnapshot) residualFootprintMB() int64 {
	var total int64
	for _, hold := range s.holds {
		if !hold.residual {
			continue
		}
		if hold.footprintMB > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += hold.footprintMB
	}
	return total
}

// phaseCounts counts the holds in each phase.
func (s volumeDeleteHoldSnapshot) phaseCounts() (removal, residual int) {
	for _, hold := range s.holds {
		if hold.residual {
			residual++
		} else {
			removal++
		}
	}
	return removal, residual
}

// dueInOrder returns the holds due at now, least recently attempted first
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
	// destroyPrecheckHeld: the name is held in the removal phase; the destroy
	// answers ErrVolumeDeleteHeld without the lock and without any work.
	destroyPrecheckHeld
	// destroyPrecheckGone: an identity-bound Lstat proved the final path absent,
	// and the name has either a residual hold or no stage and no project
	// mapping; the destroy answers nil without the lock.
	destroyPrecheckGone
)

// precheckDestroy answers a destroy of id from the manager's own state,
// without the lease's namespace lock (ENG-1117): a removal-phase hold answers
// ErrVolumeDeleteHeld, and a positively absent name with nothing pending
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
	// leftRemoval names the holds that left the removal phase during the pass
	// (completed, or durably gone and residual): their callers can now settle.
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
		if !hold.residual && !b.volumes.VolumeDeleteHolds().removalHeld(hold.volume.value()) {
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
// holds left the removal phase, and sample the gauges.
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
// held deletion just left the removal phase, so the close settles now instead
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

// closeAwaitsHeldDeletes reports, from memory only, whether a pending close is
// waiting on nothing but held deletions: every managed volume name of its
// items is held in the removal phase, and its projection records no
// container. Retrying such a close would only advance its durable generation
// and rewrite its diagnostics; the hold executor resumes it when one of its
// holds leaves the removal phase. Only a projection can vouch that no
// container remains, so a close without one (cleanup-only) is never skipped:
// a remaining container could be the very writer keeping a hold from
// finishing.
func (b *Backend) closeAwaitsHeldDeletes(claim shared.CloseIntentClaim, holds volumeDeleteHoldSnapshot) bool {
	items := claim.Items()
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		for i := range item.Quantity {
			name := canonicalVolumeName(claim.LeaseUUID(), item.ServiceName, i)
			if !holds.removalHeld(name) && !holds.removalHeld(retainedName(name)) {
				return false
			}
		}
	}
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	projection := b.provisions[claim.LeaseUUID()]
	return projection != nil && len(projection.ContainerIDs) == 0
}

// sampleVolumeDeleteHoldMetrics projects the manager's holds into the
// Backend-owned gauge.
func (b *Backend) sampleVolumeDeleteHoldMetrics() {
	removal, residual := b.volumes.VolumeDeleteHolds().phaseCounts()
	volumeDeleteHolds.WithLabelValues(volumeDeleteHoldPhaseRemoval).Set(float64(removal))
	volumeDeleteHolds.WithLabelValues(volumeDeleteHoldPhaseResidual).Set(float64(residual))
}
