package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
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
//     the stage removal. The admission account includes the project's
//     footprint (heldResidualMB) from the moment the hold enters the phase;
//     the pool counts it from the next successful publication, which also
//     acknowledges the hold. Destroy answers nil after an Lstat proves the
//     final path absent, but that answer reaches its caller only through
//     afterVolumeDestroy, which publishes first. Every reader that infers
//     completion without a destroy of its own (ListForProof, PrecheckDestroy,
//     the close-wait predicates) treats the hold like the removal phase until
//     it is acknowledged. So no caller releases its own accounting before the
//     pool counts the project instead (make before break);
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

// volumeDeleteDeferral is Backend.Start's deferral of a volume manager's
// first-time deletions. While one is open, a first-time Destroy mints its
// delete stage and hands the deletion to the hold executor without attempting
// it, so that Start never waits on a tenant tree. Start opens it at its entry
// and ends it when the executor starts, or when Start returns before that.
//
// A manager is constructed with no deferral open, so a manager that no
// Backend is starting (an offline tool, or a test driving a manager directly)
// deletes inline under its short budget and never depends on an executor it
// does not have. Only DeferDeletesUntilExecutorRuns opens a deferral, and the
// composition file is its only production caller (forbidigo); only the
// returned value ends it. The zero value defers nothing.
type volumeDeleteDeferral struct {
	release func()
}

// End ends the deferral. Ending it again, or ending the zero value, does
// nothing.
func (d volumeDeleteDeferral) End() {
	if d.release != nil {
		d.release()
	}
}

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
	// volumeDeleteHoldWorkers bounds how many attempts one pass runs at once.
	// Two attempts never share a lease (they would contend for its namespace
	// lock), so one very large deletion cannot serialize every other lease's.
	volumeDeleteHoldWorkers = 2
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
	// contentRemoved: the last attempt removed content of the volume. It is
	// the only evidence that an interrupted attempt moved the deletion
	// forward without changing its phase.
	contentRemoved bool
	since          time.Time
	lastAttempt    time.Time
	nextAttempt    time.Time
	// callerSettled: the hold is residual and an admission publication
	// counted its footprint (xfsDeleteHold.settlesCaller).
	callerSettled bool
	// residualSeq names the hold's current entry into the residual phase.
	residualSeq uint64
}

// phaseLabel is the hold's phase as reported in logs and metrics.
func (v volumeDeleteHoldView) phaseLabel() string { return v.phase.label() }

// holdsCaller reports whether a destroy of this hold's name answers
// ErrVolumeDeleteHeld, keeping its caller pending: the removal and unsized
// phases, and a residual hold no admission publication has counted yet.
func (v volumeDeleteHoldView) holdsCaller() bool { return !v.callerSettled }

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

// callerHeld reports whether name is held with its caller pending (removal,
// unsized, or residual and not yet counted), where a destroy answers
// ErrVolumeDeleteHeld without any work.
func (s volumeDeleteHoldSnapshot) callerHeld(name string) bool {
	hold, ok := s.holds[name]
	return ok && hold.holdsCaller()
}

// settledResidual reports whether name is held in the residual phase with
// its caller settled: admission counts its project's footprint.
func (s volumeDeleteHoldSnapshot) settledResidual(name string) bool {
	hold, ok := s.holds[name]
	return ok && hold.callerSettled
}

// residualAccountingToken names one entry of one hold into the residual
// phase, counted by an admission publication. Only admissionAccount builds
// one.
type residualAccountingToken struct {
	volume string
	seq    uint64
}

// heldDeletionAccount is the admission pool's view of the held deletions
// whose caller may have settled or may settle: the summed footprint of the
// residual ones, how many are unsized, and the residual ones not yet
// acknowledged as counted. An unsized hold is never counted as zero: while
// any exists, disk admission is withheld.
type heldDeletionAccount struct {
	residualMB     int64
	unsized        int
	unacknowledged []residualAccountingToken
}

// admissionAccount sums the residual footprints, saturating rather than
// wrapping, counts the unsized holds, and names the residual holds that a
// successful publication of this account may acknowledge.
func (s volumeDeleteHoldSnapshot) admissionAccount() heldDeletionAccount {
	var account heldDeletionAccount
	for _, hold := range s.holds {
		switch hold.phase {
		case holdPhaseUnsized:
			account.unsized++
		case holdPhaseResidual:
			if !hold.callerSettled {
				account.unacknowledged = append(account.unacknowledged,
					residualAccountingToken{volume: hold.volume.value(), seq: hold.residualSeq})
			}
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
	// destroyed, its held deletion is residual and counted (its caller
	// settled), it was retained, or it never had a volume (a stateless
	// service).
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
	case s.settledResidual(canonical) || s.settledResidual(retained):
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

// dueInOrder returns the holds due at now, least recently attempted first
// (never-attempted first), then by name, so one pass's budget rotates over
// every hold instead of starving the ones a busy hold keeps behind it;
// unsized holds and the others then alternate (interleaveUnsized).
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
	return interleaveUnsized(due)
}

// interleaveUnsized reorders due, already sorted oldest first, so that
// unsized holds and the holds of every other phase alternate, an unsized one
// first. Unsized holds lead because they withhold disk admission, but they
// never take more than every other dispatch while another hold is due: they
// stay due on every pass, so absolute priority would let enough of them fill
// every pass's budget and starve the removal and residual holds whose callers
// stay pending until they finish. Each class keeps its oldest-first order.
func interleaveUnsized(due []volumeDeleteHoldView) []volumeDeleteHoldView {
	var unsized, others []volumeDeleteHoldView
	for _, hold := range due {
		if hold.phase == holdPhaseUnsized {
			unsized = append(unsized, hold)
		} else {
			others = append(others, hold)
		}
	}
	ordered := make([]volumeDeleteHoldView, 0, len(due))
	for len(unsized) > 0 || len(others) > 0 {
		if len(unsized) > 0 {
			ordered = append(ordered, unsized[0])
			unsized = unsized[1:]
		}
		if len(others) > 0 {
			ordered = append(ordered, others[0])
			others = others[1:]
		}
	}
	return ordered
}

// afterVolumeDestroy publishes, before a destroy's caller can act on its
// answer, any held-deletion phase change the destroy made: a deletion that
// just became residual settles its caller, so its project's footprint must
// already be in the admission pool, and the publication acknowledges it for
// every other reader too. Every destroy entry point in the composition file
// calls it. A failed publication fails the destroy, so the caller keeps its
// own accounting and retries.
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
	// destroyPrecheckHeld: the name is held with its caller pending (removal,
	// unsized, or residual and not yet counted); the destroy answers
	// ErrVolumeDeleteHeld without the lock and without any work.
	destroyPrecheckHeld
	// destroyPrecheckGone: an identity-bound Lstat proved the final path absent,
	// and the name has either a counted residual hold or no stage and no
	// project mapping; the destroy answers nil without the lock.
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
	// The locked path refuses every destroy once storage authority latched
	// (authorizeStorageMutation); the lock-free answer must refuse as well, or
	// a caller could settle on a memory-and-Lstat "gone" after the latch.
	if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
		return true, fmt.Errorf("destroy volume %q: %w", id, authorityErr)
	}
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
	// the pass (completed, or residual and acknowledged as counted): their
	// callers can now settle. A hold acknowledged by any other publication
	// owes its lease a resume instead (publishRetainedDiskLocked).
	leftRemoval []managedVolumeName
	// progressed is set when an attempt reached the manager and moved its
	// hold forward: it completed, changed phase, or ran out of its slice
	// after removing content. The executor then starts its next pass at once
	// instead of waiting for its interval.
	progressed bool
}

// volumeDeleteHoldExecutorState is the hold executor's own memory: when it
// last dispatched each hold, so that a hold whose attempts fail before they
// reach the manager (and so never update the hold's lastAttempt) still
// rotates behind the others; and the leases whose close resume found the
// lease busy, retried on the next pass.
type volumeDeleteHoldExecutorState struct {
	mu             sync.Mutex
	lastDispatched map[string]time.Time
	pendingResumes map[string]struct{}
}

// dispatched records that hold name was handed to an attempt at now.
func (s *volumeDeleteHoldExecutorState) dispatched(name string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastDispatched == nil {
		s.lastDispatched = make(map[string]time.Time)
	}
	s.lastDispatched[name] = now
}

// order sorts due in place for dispatch: by the later of the manager's last
// attempt and this executor's last dispatch, oldest first, then by name, with
// unsized holds and the others alternating (interleaveUnsized). It forgets
// the dispatch times of names no longer held.
func (s *volumeDeleteHoldExecutorState) order(due []volumeDeleteHoldView, held volumeDeleteHoldSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.lastDispatched {
		if _, ok := held.holds[name]; !ok {
			delete(s.lastDispatched, name)
		}
	}
	last := func(hold volumeDeleteHoldView) time.Time {
		if dispatched := s.lastDispatched[hold.volume.value()]; dispatched.After(hold.lastAttempt) {
			return dispatched
		}
		return hold.lastAttempt
	}
	slices.SortStableFunc(due, func(a, b volumeDeleteHoldView) int {
		if c := last(a).Compare(last(b)); c != 0 {
			return c
		}
		return strings.Compare(a.volume.value(), b.volume.value())
	})
	copy(due, interleaveUnsized(due))
}

// takeResumes returns, and forgets, the leases whose close resume is owed.
func (s *volumeDeleteHoldExecutorState) takeResumes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	leases := slices.Sorted(maps.Keys(s.pendingResumes))
	s.pendingResumes = nil
	return leases
}

// oweResume records a lease whose close resume must be retried.
func (s *volumeDeleteHoldExecutorState) oweResume(leaseUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingResumes == nil {
		s.pendingResumes = make(map[string]struct{})
	}
	s.pendingResumes[leaseUUID] = struct{}{}
}

// holdDispatchQueue hands one pass's due holds to its workers in order, and
// never two holds of the same lease at once.
type holdDispatchQueue struct {
	mu    sync.Mutex
	ready *sync.Cond
	queue []volumeDeleteHoldView
	busy  map[string]struct{}
}

func newHoldDispatchQueue(due []volumeDeleteHoldView) *holdDispatchQueue {
	q := &holdDispatchQueue{queue: due, busy: make(map[string]struct{})}
	q.ready = sync.NewCond(&q.mu)
	return q
}

// next returns the first queued hold whose lease no attempt is working on,
// waiting while every queued hold's lease is busy. It reports false once the
// queue is empty or ctx (the pass budget) can no longer fit an attempt.
func (q *holdDispatchQueue) next(ctx context.Context) (volumeDeleteHoldView, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if len(q.queue) == 0 || ctx.Err() != nil {
			return volumeDeleteHoldView{}, false
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < volumeDeleteHoldMinSlice {
			return volumeDeleteHoldView{}, false
		}
		for i, hold := range q.queue {
			lease := managedVolumeLeaseUUID(hold.volume)
			if _, busy := q.busy[lease]; busy {
				continue
			}
			q.queue = slices.Delete(q.queue, i, i+1)
			q.busy[lease] = struct{}{}
			return hold, true
		}
		// Every queued hold's lease has an attempt running; each ends within
		// its slice and wakes this worker.
		q.ready.Wait()
	}
}

// finish releases hold's lease to the next queued hold.
func (q *holdDispatchQueue) finish(hold volumeDeleteHoldView) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.busy, managedVolumeLeaseUUID(hold.volume))
	q.ready.Broadcast()
}

// runVolumeDeleteHoldPass retries the due holds, one bounded slice each, until
// ctx (the pass budget) runs out: least recently tried first, unsized holds
// alternating with the others (interleaveUnsized), up to
// volumeDeleteHoldWorkers at once on different leases. Each
// retry runs through retry, which the composition binds to the
// storage-mutation bracket under the hold's lease namespace; it never takes the
// global recovery gate, so one hold never stalls another lease.
func (b *Backend) runVolumeDeleteHoldPass(
	ctx context.Context,
	now time.Time,
	retry backgroundHeldVolumeDeleteRetry,
) volumeDeleteHoldPassReport {
	var report volumeDeleteHoldPassReport
	if retry == nil {
		return report
	}
	held := b.volumes.VolumeDeleteHolds()
	due := held.dueInOrder(now)
	b.holdExecutor.order(due, held)
	queue := newHoldDispatchQueue(due)
	var reportMu sync.Mutex
	var workers sync.WaitGroup
	for range volumeDeleteHoldWorkers {
		workers.Go(func() {
			for {
				hold, ok := queue.next(ctx)
				if !ok {
					return
				}
				b.holdExecutor.dispatched(hold.volume.value(), time.Now())
				attempt := b.attemptHeldVolumeDelete(ctx, hold, retry)
				queue.finish(hold)
				reportMu.Lock()
				report.attempted++
				report.progressed = report.progressed || attempt.progressed
				if attempt.releasedCaller {
					report.leftRemoval = append(report.leftRemoval, hold.volume)
				}
				reportMu.Unlock()
			}
		})
	}
	workers.Wait()
	slices.SortFunc(report.leftRemoval, func(a, b managedVolumeName) int {
		return strings.Compare(a.value(), b.value())
	})
	return report
}

// heldDeleteAttempt is what one attempt did to its hold.
type heldDeleteAttempt struct {
	// progressed: the manager recorded the attempt and the hold moved
	// forward (see volumeDeleteHoldPassReport.progressed).
	progressed bool
	// releasedCaller: the hold held its caller before the attempt and no
	// longer does.
	releasedCaller bool
}

// attemptHeldVolumeDelete runs one bounded attempt of hold inside its own
// panic boundary, since it runs on a pass worker goroutine, and classifies
// what it did from the manager's hold table before and after.
func (b *Backend) attemptHeldVolumeDelete(
	ctx context.Context,
	hold volumeDeleteHoldView,
	retry backgroundHeldVolumeDeleteRetry,
) heldDeleteAttempt {
	var attempt heldDeleteAttempt
	util.RunCleanupIteration(func() error {
		slice, cancel := context.WithTimeout(ctx, volumeDeleteHoldSlice)
		err := retry(slice, hold.volume.value())
		cancel()
		if err != nil && !errors.Is(err, ErrVolumeDeleteHeld) {
			b.logger.Warn("held volume delete retry failed",
				"volume_id", hold.volume.value(), "delete_stage", hold.stage, "error", err)
		}
		after, stillHeld := b.volumes.VolumeDeleteHolds().holds[hold.volume.value()]
		attempt = classifyHeldDeleteAttempt(hold, after, stillHeld, time.Now())
		return nil
	}, volumeDeleteHoldComponent, func(any) {
		background.CleanupPanicsTotal.WithLabelValues(volumeDeleteHoldComponent).Inc()
	})
	return attempt
}

// classifyHeldDeleteAttempt compares a hold before and after one attempt. An
// attempt that never reached the manager leaves attempts unchanged and is no
// progress, so an executor whose retries fail early waits for its interval
// instead of spinning. Within a phase, an interrupted attempt is progress only
// when it removed content: a slice spent waiting on the quota subsystem (the
// zero-usage proof, the limit clear) moved nothing forward, so the next pass
// waits for the interval too.
func classifyHeldDeleteAttempt(
	before, after volumeDeleteHoldView,
	stillHeld bool,
	now time.Time,
) heldDeleteAttempt {
	if !stillHeld {
		return heldDeleteAttempt{progressed: true, releasedCaller: before.holdsCaller()}
	}
	if after.stage != before.stage || after.attempts <= before.attempts {
		return heldDeleteAttempt{}
	}
	return heldDeleteAttempt{
		progressed: after.phase != before.phase ||
			(after.reason.keepsProgressing() && after.contentRemoved && !now.Before(after.nextAttempt)),
		releasedCaller: before.holdsCaller() && !after.holdsCaller(),
	}
}

// volumeDeleteHoldLoop is the hold executor, the only runner of held
// deletion work. It runs on the Backend's lifetime: first as soon as it
// starts, then on volumeDeleteHoldInterval, except that a pass which moved a
// hold forward is followed at once by the next. It is work-conserving within
// each pass's budget and each attempt's slice, and every pass rotates over all
// due holds, so a large deletion gets consecutive slices without starving the
// others.
func (b *Backend) volumeDeleteHoldLoop() {
	ticker := time.NewTicker(volumeDeleteHoldInterval)
	defer ticker.Stop()
	for b.stopCtx.Err() == nil {
		if b.runVolumeDeleteHoldIteration() {
			continue
		}
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runVolumeDeleteHoldIteration is one executor pass inside the cleanup panic
// boundary: retry the due holds under the pass budget, resume the closes whose
// holds stopped holding their caller, and sample the gauges. It reports
// whether the pass moved a hold forward.
func (b *Backend) runVolumeDeleteHoldIteration() (progressed bool) {
	util.RunCleanupIteration(func() error {
		passCtx, cancel := context.WithTimeout(b.stopCtx, volumeDeleteHoldPassBudget)
		report := b.backgroundMaintenance.retryHeldVolumeDeletes(passCtx)
		cancel()
		b.resumeClosesAfterHeldDeletes(report.leftRemoval)
		b.sampleVolumeDeleteHoldMetrics()
		progressed = report.progressed
		return nil
	}, volumeDeleteHoldComponent, func(any) {
		background.CleanupPanicsTotal.WithLabelValues(volumeDeleteHoldComponent).Inc()
	})
	return progressed
}

// resumeClosesAfterHeldDeletes resumes the close of each lease whose held
// deletion just stopped holding its caller, so the close settles now instead
// of on the next reconcile, together with every resume an earlier pass owed.
// A lease whose command fence is busy is owed again on the next pass: the
// command holding it may have answered from the state before the hold ended.
func (b *Backend) resumeClosesAfterHeldDeletes(names []managedVolumeName) {
	leases := b.holdExecutor.takeResumes()
	for _, name := range names {
		if leaseUUID, ok := heldDeletionLease(name); ok {
			leases = append(leases, leaseUUID)
		}
	}
	if len(leases) == 0 {
		return
	}
	slices.Sort(leases)
	leases = slices.Compact(leases)
	ctx, cancel := context.WithTimeout(b.stopCtx, volumeDeleteHoldResumeBudget)
	defer cancel()
	for _, leaseUUID := range leases {
		if ctx.Err() != nil {
			b.holdExecutor.oweResume(leaseUUID)
			continue
		}
		resumed, err := b.tryResumeRecoveredClose(ctx, leaseUUID)
		switch {
		case err != nil:
			b.logger.Warn("close resume after a held volume delete remains pending",
				"lease_uuid", leaseUUID, "error", err)
		case !resumed:
			b.holdExecutor.oweResume(leaseUUID)
		}
	}
}

// heldDeletionLease returns the lease whose close a held deletion of name can
// keep pending. A retained name belongs to the lease it was retained from, as
// in closeAwaitsHeldDeletes.
func heldDeletionLease(name managedVolumeName) (string, bool) {
	value := name.value()
	if isRetainedVolume(value) {
		value = canonicalFromRetained(value)
	}
	return leaseUUIDFromVolumeName(value)
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
// already destroyed, residual and counted, retained, or never created counts
// as done), and
// no container of the lease remains. Retrying such a close would only advance
// its durable generation and rewrite its diagnostics; the hold executor
// resumes it when one of its holds stops holding its caller.
//
// A remaining container could be the very writer keeping a hold from
// finishing, and only a close retry removes it, so the container half needs
// positive evidence: the projection records no container, or, for a
// cleanup-only close (which has no projection), this process's own attempt of
// this close intent completed its container teardown (closeTeardownFacts). A
// cleanup-only close is therefore retried once after every start before it
// can be skipped. A projected close whose projection is missing is never
// skipped.
func (b *Backend) closeAwaitsHeldDeletes(claim shared.CloseIntentClaim, holds volumeDeleteHoldSnapshot) bool {
	if !holds.awaitsOnlyHeldDeletes(leaseSlotNames(claim.LeaseUUID(), claim.Items())) {
		return false
	}
	if claim.CleanupOnly() {
		return b.closeTeardowns.completed(claim)
	}
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	projection := b.provisions[claim.LeaseUUID()]
	return projection != nil && len(projection.ContainerIDs) == 0
}

// closeTeardownFacts is the memory of which close intents this process tore
// down: the last attempt of the intent removed every container it observed
// for the lease. It is keyed by lease and bound to the intent's creation time,
// so a fact can only vouch for the intent it was recorded under. A restart
// forgets every fact.
type closeTeardownFacts struct {
	mu      sync.Mutex
	cleared map[string]time.Time
}

// record stores whether this attempt of claim completed its container
// teardown; a failed teardown forgets an earlier success.
func (f *closeTeardownFacts) record(claim shared.CloseIntentClaim, completed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !completed {
		delete(f.cleared, claim.LeaseUUID())
		return
	}
	if f.cleared == nil {
		f.cleared = make(map[string]time.Time)
	}
	f.cleared[claim.LeaseUUID()] = claim.CreatedAt()
}

// completed reports whether this process's last attempt of exactly claim
// completed its container teardown.
func (f *closeTeardownFacts) completed(claim shared.CloseIntentClaim) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	created, ok := f.cleared[claim.LeaseUUID()]
	return ok && created.Equal(claim.CreatedAt())
}

// forget drops the fact of a lease whose close completed.
func (f *closeTeardownFacts) forget(leaseUUID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.cleared, leaseUUID)
}

// sampleVolumeDeleteHoldMetrics projects the manager's holds into the
// Backend-owned gauge.
func (b *Backend) sampleVolumeDeleteHoldMetrics() {
	for phase, count := range b.volumes.VolumeDeleteHolds().phaseCounts() {
		volumeDeleteHolds.WithLabelValues(phase).Set(float64(count))
	}
}
