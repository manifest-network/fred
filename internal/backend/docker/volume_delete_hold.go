package docker

import (
	"math"
	"slices"
	"strings"
	"time"
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
