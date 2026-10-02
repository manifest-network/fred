package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// The XFS half of held volume deletions (ENG-1117); volume_delete_hold.go has
// the overview.
//
// One cleanup attempt of a parent-durable delete stage ends in exactly one
// deleteStageOutcome: completed, held, or latched. Only failures on a reviewed
// allowlist can be held, and the allowlist is expressed in the type system: an
// allowlisted return site wraps its error with holdable(reason, err), the only
// constructor of xfsDeleteHoldCause, and classifyXFSDeleteStageCleanup is a
// total function whose default arm latches. An error that did not pass through
// holdable, or whose chain carries an ambiguous outcome or a recovery-pending
// authority contradiction, therefore cannot become a hold.

const (
	// liveXFSDeleteBudget bounds the inline cleanup of a first-time Destroy
	// once the hold executor runs. A deletion that needs longer is held and the
	// executor finishes it. The budget also honors caller cancellation and the
	// Backend's lifetime (the storage-mutation bracket joins both).
	liveXFSDeleteBudget = 10 * time.Second
	// xfsQuotaClearTimeout bounds the single limit-clear quotactl. It runs
	// detached from cancellation so it is never killed half-way, and starts only
	// while the attempt's budget remains.
	xfsQuotaClearTimeout = 10 * time.Second
	// xfsResidualRowReadTimeout bounds the one report-row read per recovered
	// stage that Start makes to size a residual hold.
	xfsResidualRowReadTimeout = 5 * time.Second
	// volumeDeleteHoldInitialBackoff and volumeDeleteHoldMaxBackoff space the
	// executor's retries of a hold that is not making progress.
	volumeDeleteHoldInitialBackoff = 30 * time.Second
	volumeDeleteHoldMaxBackoff     = 30 * time.Minute
)

// volumeDeleteHoldReason is the closed set of reasons a deletion can be held.
// The type is unexported and only these constants exist; there is no way to
// make one up from a string.
type volumeDeleteHoldReason uint8

const (
	// holdReasonRecovered: the stage was found at startup; no attempt yet.
	holdReasonRecovered volumeDeleteHoldReason = iota + 1
	// holdReasonDeadline: the attempt's budget ran out (or Start deferred it).
	holdReasonDeadline
	// holdReasonStopped: the caller or the Backend stopped the attempt.
	holdReasonStopped
	// holdReasonRemovalFailed: removing tenant content failed with an errno
	// fstree does not classify (EIO, EMFILE, ...).
	holdReasonRemovalFailed
	// holdReasonTreeChanged: the tree moved under the removal.
	holdReasonTreeChanged
	// holdReasonCrossDevice: the tree crosses a filesystem or mount boundary.
	holdReasonCrossDevice
	// holdReasonUndeletable: an entry cannot be removed (EPERM, EACCES, EBUSY,
	// EROFS), e.g. an operator-set immutable flag or a host mount.
	holdReasonUndeletable
	// holdReasonCutRefused: the tree is deeper than the removal's bound and the
	// cut, or the cut anchor's detach, was refused.
	holdReasonCutRefused
	// holdReasonWriterActive: the emptied volume gained content.
	holdReasonWriterActive
	// holdReasonFinalRemovalFailed: the emptied volume directory is still there
	// after its rmdir.
	holdReasonFinalRemovalFailed
	// holdReasonUsageUnprovable: the zero-usage proof could not be read.
	holdReasonUsageUnprovable
	// holdReasonUsageNonzero: the project still charges blocks or inodes.
	holdReasonUsageNonzero
	// holdReasonQuotaClearFailed: the limit clear failed; the limits may
	// already be cleared.
	holdReasonQuotaClearFailed
	// holdReasonStageRemovalFailed: the stage is still there after its rmdir;
	// the limits are already cleared.
	holdReasonStageRemovalFailed
)

// String returns the reason's stable log and runbook label.
func (r volumeDeleteHoldReason) String() string {
	switch r {
	case holdReasonRecovered:
		return "recovered"
	case holdReasonDeadline:
		return "deadline"
	case holdReasonStopped:
		return "stopped"
	case holdReasonRemovalFailed:
		return "removal_failed"
	case holdReasonTreeChanged:
		return "tree_changed"
	case holdReasonCrossDevice:
		return "cross_device"
	case holdReasonUndeletable:
		return "undeletable"
	case holdReasonCutRefused:
		return "cut_refused"
	case holdReasonWriterActive:
		return "writer_active"
	case holdReasonFinalRemovalFailed:
		return "final_removal_failed"
	case holdReasonUsageUnprovable:
		return "usage_unprovable"
	case holdReasonUsageNonzero:
		return "usage_nonzero"
	case holdReasonQuotaClearFailed:
		return "quota_clear_failed"
	case holdReasonStageRemovalFailed:
		return "stage_removal_failed"
	default:
		return "unknown"
	}
}

// keepsProgressing reports whether a hold with this reason stays due: its
// last attempt was interrupted or never made, not refused, so the next pass
// continues where it stopped instead of backing off.
func (r volumeDeleteHoldReason) keepsProgressing() bool {
	switch r {
	case holdReasonRecovered, holdReasonDeadline, holdReasonStopped:
		return true
	default:
		return false
	}
}

// holdReasonForContext maps an interrupted attempt to its reason.
func holdReasonForContext(err error) volumeDeleteHoldReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return holdReasonDeadline
	}
	return holdReasonStopped
}

// holdReasonForTreeRemoval maps a failed fstree removal to its reason, from the
// same total classification the tree-removal metric uses. Only context errors
// mean the attempt was interrupted; every other class is a refusal.
func holdReasonForTreeRemoval(class treeRemovalClass) volumeDeleteHoldReason {
	switch class {
	case treeRemovalDeadline:
		return holdReasonDeadline
	case treeRemovalCanceled:
		return holdReasonStopped
	case treeRemovalCrossDevice:
		return holdReasonCrossDevice
	case treeRemovalCutRefused:
		return holdReasonCutRefused
	case treeRemovalTreeChanged:
		return holdReasonTreeChanged
	case treeRemovalUndeletable:
		return holdReasonUndeletable
	default:
		return holdReasonRemovalFailed
	}
}

// xfsDeleteHoldCause marks a cleanup error as confined to one volume's
// deletion. holdable is its only constructor and classifyXFSDeleteStageCleanup
// its only reader.
type xfsDeleteHoldCause struct {
	reason volumeDeleteHoldReason
	err    error
}

// holdable marks err, returned from a reviewed allowlisted site of the
// delete-stage cleanup, as holdable for reason. Nothing else can make an error
// holdable.
func holdable(reason volumeDeleteHoldReason, err error) *xfsDeleteHoldCause {
	if err == nil {
		err = fmt.Errorf("xfs volume deletion held (%s)", reason)
	}
	return &xfsDeleteHoldCause{reason: reason, err: err}
}

func (c *xfsDeleteHoldCause) Error() string { return c.err.Error() }

func (c *xfsDeleteHoldCause) Unwrap() error { return c.err }

// errInlineDeleteDeferred is the cause of a first-time delete that Start hands
// to the hold executor without attempting it.
var errInlineDeleteDeferred = errors.New("deletion deferred to the hold executor until startup completes")

// errDeleteStageRecovered is the cause of a hold registered for a delete stage
// found at startup.
var errDeleteStageRecovered = errors.New("delete stage recovered at startup")

// xfsProjectQuotaRow is one strictly parsed numeric `xfs_quota report -p` row
// for an exact project, in the report's units (1 KiB blocks, or inodes). Only
// parseXfsReportRow builds one.
type xfsProjectQuotaRow struct {
	projID uint32
	// found is false when the report has no row: no initialized dquot, so no
	// usage and no limit.
	found bool
	used  int64
	// hard is the hard limit, 0 for none. limitsKnown is false when the row
	// carried no limit columns.
	hard        int64
	limitsKnown bool
}

// residualFootprintMB bounds what a residual hold's project can still charge
// to the disk: its block hard limit, or its used blocks when they are larger
// or the project has no limit, in MiB rounded up. residualFootprintFromRow,
// the only constructor, builds it only from a parsed block row, so a footprint
// can never be a guess.
type residualFootprintMB struct {
	mb int64
}

// residualFootprintFromRow sizes a residual hold from its project's block row.
// It reports false when the row did not carry its limits.
func residualFootprintFromRow(row xfsProjectQuotaRow) (residualFootprintMB, bool) {
	if !row.found {
		return residualFootprintMB{mb: 0}, true
	}
	if !row.limitsKnown {
		return residualFootprintMB{}, false
	}
	kib := max(row.used, row.hard)
	return residualFootprintMB{mb: (kib + 1023) / 1024}, true
}

// xfsDeleteHoldPhase is the closed phase of a hold: removal, or residual with
// the footprint the admission pool counts. A residual phase cannot exist
// without a footprint built from a parsed quota row.
type xfsDeleteHoldPhase struct {
	residual  bool
	footprint residualFootprintMB
}

func removalHoldPhase() xfsDeleteHoldPhase { return xfsDeleteHoldPhase{} }

func residualHoldPhase(footprint residualFootprintMB) xfsDeleteHoldPhase {
	return xfsDeleteHoldPhase{residual: true, footprint: footprint}
}

func (p xfsDeleteHoldPhase) label() string {
	if p.residual {
		return volumeDeleteHoldPhaseResidual
	}
	return volumeDeleteHoldPhaseRemoval
}

// xfsDeleteHold is one held deletion, keyed by its volume in the manager's
// hold table under x.mu. The delete stage directory is its durable record.
type xfsDeleteHold struct {
	stage    xfsDeleteStageName
	phase    xfsDeleteHoldPhase
	reason   volumeDeleteHoldReason
	attempts int
	// failures counts consecutive attempts that ended with a reason that backs
	// off; an interrupted attempt resets it.
	failures    int
	since       time.Time
	lastAttempt time.Time
	nextAttempt time.Time
}

// newXFSDeleteHold is the only constructor of a hold record, and it accepts
// only a held outcome, which only classifyXFSDeleteStageCleanup produces from a
// holdable cause. Every hold therefore traces back to an allowlisted site.
func newXFSDeleteHold(outcome deleteStageOutcome, now time.Time) (*xfsDeleteHold, bool) {
	if outcome.kind != deleteStageHeld {
		return nil, false
	}
	return &xfsDeleteHold{
		stage: outcome.stage, phase: outcome.phase, reason: outcome.reason,
		since: now, nextAttempt: now,
	}, true
}

func (h *xfsDeleteHold) view() volumeDeleteHoldView {
	return volumeDeleteHoldView{
		volume:      h.stage.volumeID,
		stage:       h.stage.value(),
		projectID:   h.stage.projID,
		residual:    h.phase.residual,
		footprintMB: h.phase.footprint.mb,
		reason:      h.reason,
		attempts:    h.attempts,
		since:       h.since,
		lastAttempt: h.lastAttempt,
		nextAttempt: h.nextAttempt,
	}
}

// volumeDeleteHoldBackoff is the wait after the failures-th consecutive
// attempt that did not make progress: 30 s doubling, capped at 30 min.
func volumeDeleteHoldBackoff(failures int) time.Duration {
	backoff := volumeDeleteHoldInitialBackoff
	for range max(failures-1, 0) {
		backoff *= 2
		if backoff >= volumeDeleteHoldMaxBackoff {
			return volumeDeleteHoldMaxBackoff
		}
	}
	return backoff
}

// xfsDeleteAttempt records how far one cleanup attempt got, as facts the
// attempt itself observed.
type xfsDeleteAttempt struct {
	// finalSeen: the attempt observed the final volume directory present.
	finalSeen bool
	// absenceDurable: the attempt observed the final path absent and synced
	// the parent, so the absence is durable.
	absenceDurable bool
	// footprint is set once the attempt parsed the project's block row.
	footprint      residualFootprintMB
	footprintKnown bool
}

// deleteStageOutcomeKind is the closed set of cleanup outcomes.
type deleteStageOutcomeKind uint8

const (
	deleteStageCompleted deleteStageOutcomeKind = iota + 1
	deleteStageHeld
	deleteStageLatched
)

// deleteStageOutcome is the result of one delete-stage cleanup attempt.
// classifyXFSDeleteStageCleanup is its only producer, and result its only
// conversion into the manager's error contract.
type deleteStageOutcome struct {
	kind   deleteStageOutcomeKind
	stage  xfsDeleteStageName
	phase  xfsDeleteHoldPhase
	reason volumeDeleteHoldReason
	err    error
}

// classifyXFSDeleteStageCleanup is the total classification of one attempt's
// error, and the only place a hold cause is read. Its default arm is the latch.
// previous is the hold before the attempt, or nil.
func classifyXFSDeleteStageCleanup(
	stage xfsDeleteStageName,
	attempt xfsDeleteAttempt,
	previous *xfsDeleteHold,
	err error,
) deleteStageOutcome {
	switch {
	case err == nil:
		return deleteStageOutcome{kind: deleteStageCompleted, stage: stage}
	case errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous),
		errors.Is(err, ErrVolumeMutationRecoveryPending):
		return deleteStageOutcome{kind: deleteStageLatched, stage: stage, err: err}
	}
	var cause *xfsDeleteHoldCause
	if !errors.As(err, &cause) {
		return deleteStageOutcome{kind: deleteStageLatched, stage: stage, err: err}
	}
	return deleteStageOutcome{
		kind:   deleteStageHeld,
		stage:  stage,
		phase:  holdPhaseAfter(attempt, previous),
		reason: cause.reason,
		err:    err,
	}
}

// holdPhaseAfter is the phase a hold has after an attempt. Residual needs both
// a durable absence of the final path and a footprint parsed from the
// project's report row; without a known footprint the hold stays in the
// removal phase, where the caller keeps its own accounting, because settling
// the caller while under-counting the project is never allowed.
func holdPhaseAfter(attempt xfsDeleteAttempt, previous *xfsDeleteHold) xfsDeleteHoldPhase {
	switch {
	case attempt.absenceDurable:
		if attempt.footprintKnown {
			return residualHoldPhase(attempt.footprint)
		}
		if previous != nil && previous.phase.residual {
			return previous.phase
		}
		return removalHoldPhase()
	case attempt.finalSeen:
		return removalHoldPhase()
	case previous != nil:
		return previous.phase
	default:
		return removalHoldPhase()
	}
}

// finalPathObservation is one Lstat of a volume's final path.
type finalPathObservation uint8

const (
	// finalPathNotObserved: the caller does not settle on this outcome.
	finalPathNotObserved finalPathObservation = iota
	finalPathAbsent
	finalPathPresent
	finalPathUnknown
)

func (o finalPathObservation) absent() bool { return o == finalPathAbsent }

// observeFinalPathAtRoot reads the final path through a root the caller holds
// inside the storage-mutation bracket, whose post-check re-attests identity.
func observeFinalPathAtRoot(root *os.Root, name managedVolumeName) finalPathObservation {
	_, err := root.Lstat(name.value())
	switch {
	case err == nil:
		return finalPathPresent
	case errors.Is(err, fs.ErrNotExist):
		return finalPathAbsent
	default:
		return finalPathUnknown
	}
}

// observeFinalPathPinned reads the final path without the bracket, so it binds
// the read to the pinned data-root identity itself: the Lstat goes through the
// same descriptor whose device and inode match the pin. An unpinned or
// replaced root yields finalPathUnknown, never a confident absence.
func (x *xfsVolumeManager) observeFinalPathPinned(name managedVolumeName) finalPathObservation {
	dir, err := os.Open(x.dataPath)
	if err != nil {
		return finalPathUnknown
	}
	defer func() { _ = dir.Close() }()
	var root unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &root); err != nil {
		return finalPathUnknown
	}
	if !x.rootWatch.pinnedTo(root.Dev, root.Ino) {
		return finalPathUnknown
	}
	var entry unix.Stat_t
	err = unix.Fstatat(int(dir.Fd()), name.value(), &entry, unix.AT_SYMLINK_NOFOLLOW)
	switch {
	case err == nil:
		return finalPathPresent
	case errors.Is(err, unix.ENOENT):
		return finalPathAbsent
	default:
		return finalPathUnknown
	}
}

// result is the one conversion of a cleanup outcome into the manager's error
// contract:
//
//   - completed: nil;
//   - held: ErrVolumeDeleteHeld wrapping the cleanup's chain, except that a
//     residual hold settles a Destroy caller (nil) when final proves the final
//     path absent, and latches when final finds it present again;
//   - latched: ErrVolumeMutationRecoveryPending.
//
// The hold executor passes finalPathNotObserved: it settles nothing.
func (o deleteStageOutcome) result(final finalPathObservation) error {
	switch o.kind {
	case deleteStageCompleted:
		return nil
	case deleteStageHeld:
		if o.phase.residual {
			switch final {
			case finalPathAbsent:
				return nil
			case finalPathPresent:
				return fmt.Errorf(
					"%w: xfs volume %q exists again while delete-stage %q holds only its project",
					ErrVolumeMutationRecoveryPending, o.stage.volumeID.value(), o.stage.value(),
				)
			}
		}
		return fmt.Errorf("%w (phase=%s reason=%s): %w", ErrVolumeDeleteHeld, o.phase.label(), o.reason, o.err)
	default:
		if errors.Is(o.err, ErrVolumeMutationRecoveryPending) {
			return o.err
		}
		return fmt.Errorf("%w: xfs delete-stage %q remains authoritative: %w",
			ErrVolumeMutationRecoveryPending, o.stage.value(), o.err)
	}
}

// heldResidual reports whether the outcome is a residual hold, the only kind
// whose Destroy result needs a fresh observation of the final path.
func (o deleteStageOutcome) heldResidual() bool {
	return o.kind == deleteStageHeld && o.phase.residual
}

// outcomeLabel is the volume_delete_outcomes_total label of the outcome.
func (o deleteStageOutcome) outcomeLabel() string {
	switch {
	case o.kind == deleteStageCompleted:
		return volumeDeleteOutcomeCompleted
	case o.kind == deleteStageHeld && o.phase.residual:
		return volumeDeleteOutcomeHeldResidual
	case o.kind == deleteStageHeld:
		return volumeDeleteOutcomeHeldRemoval
	default:
		return volumeDeleteOutcomeLatched
	}
}

// settleXFSDeleteStageCleanup classifies one attempt's error and records the
// outcome in the hold table, the outcome counter and the log, before the
// attempt's caller sees it.
func (x *xfsVolumeManager) settleXFSDeleteStageCleanup(
	stage xfsDeleteStageName,
	attempt xfsDeleteAttempt,
	err error,
) deleteStageOutcome {
	return x.recordXFSDeleteStageOutcome(stage, attempt, err, true)
}

// recordXFSDeleteStageOutcome is settleXFSDeleteStageCleanup's body. attempted
// is false when no cleanup ran (Start's deferral of a first-time delete, and
// its sizing of recovered holds): that counts no attempt, no backoff and no
// outcome.
func (x *xfsVolumeManager) recordXFSDeleteStageOutcome(
	stage xfsDeleteStageName,
	attempt xfsDeleteAttempt,
	err error,
	attempted bool,
) deleteStageOutcome {
	now := time.Now()
	x.mu.Lock()
	volume := stage.volumeID.value()
	previous := x.deleteHolds[volume]
	if previous != nil && previous.stage != stage {
		previous = nil
	}
	outcome := classifyXFSDeleteStageCleanup(stage, attempt, previous, err)
	var (
		hold    volumeDeleteHoldView
		changed bool
	)
	switch outcome.kind {
	case deleteStageCompleted:
		delete(x.deleteHolds, volume)
		delete(x.verifiedDeleteStages, stage)
	case deleteStageHeld:
		current := previous
		if current == nil {
			current, _ = newXFSDeleteHold(outcome, now)
		}
		changed = previous == nil || previous.phase != outcome.phase || previous.reason != outcome.reason
		current.phase = outcome.phase
		current.reason = outcome.reason
		if attempted {
			current.attempts++
			current.lastAttempt = now
			if outcome.reason.keepsProgressing() {
				current.failures = 0
				current.nextAttempt = now
			} else {
				current.failures++
				current.nextAttempt = now.Add(volumeDeleteHoldBackoff(current.failures))
			}
		}
		if x.deleteHolds == nil {
			x.deleteHolds = make(map[string]*xfsDeleteHold)
		}
		x.deleteHolds[volume] = current
		hold = current.view()
	}
	x.mu.Unlock()

	if attempted {
		volumeDeleteOutcomesTotal.WithLabelValues(outcome.outcomeLabel()).Inc()
	}
	switch outcome.kind {
	case deleteStageCompleted:
		if previous != nil {
			x.logger.Info("held volume delete completed",
				"volume_id", volume, "delete_stage", stage.value(), "project_id", stage.projID,
				"attempts", previous.attempts+1)
		}
	case deleteStageHeld:
		level := x.logger.Debug
		if changed {
			level = x.logger.Warn
		}
		level("volume delete held",
			"volume_id", volume, "delete_stage", stage.value(), "project_id", stage.projID,
			"phase", hold.phaseLabel(), "reason", hold.reason.String(), "attempts", hold.attempts,
			"next_attempt", hold.nextAttempt, "error", outcome.err)
	default:
		x.logger.Error("volume delete cannot proceed; storage authority must be recovered by a fresh start",
			"volume_id", volume, "delete_stage", stage.value(), "project_id", stage.projID, "error", outcome.err)
	}
	return outcome
}

// registeredHoldOutcome reports the outcome a Destroy of name answers from an
// existing hold, without any filesystem work. It is the classification of an
// attempt that did nothing, so it keeps the hold's phase and reason.
func (x *xfsVolumeManager) registeredHoldOutcome(name managedVolumeName) (deleteStageOutcome, bool) {
	x.mu.Lock()
	hold, ok := x.deleteHolds[name.value()]
	var current xfsDeleteHold
	if ok {
		current = *hold
	}
	x.mu.Unlock()
	if !ok {
		return deleteStageOutcome{}, false
	}
	cause := holdable(current.reason, fmt.Errorf("xfs volume %q is held under delete-stage %q",
		name.value(), current.stage.value()))
	return classifyXFSDeleteStageCleanup(current.stage, xfsDeleteAttempt{}, &current, cause), true
}

// recoveredXFSDeleteHold is the hold Start registers for a delete stage found
// on disk: removal phase, reason recovered, due at once.
func recoveredXFSDeleteHold(stage xfsDeleteStageName, now time.Time) (*xfsDeleteHold, bool) {
	outcome := classifyXFSDeleteStageCleanup(stage, xfsDeleteAttempt{}, nil,
		holdable(holdReasonRecovered, errDeleteStageRecovered))
	return newXFSDeleteHold(outcome, now)
}

// holdPhaseOf returns the phase of stage's hold, if it has one.
func (x *xfsVolumeManager) holdPhaseOf(stage xfsDeleteStageName) (xfsDeleteHoldPhase, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	hold, ok := x.deleteHolds[stage.volumeID.value()]
	if !ok || hold.stage != stage {
		return xfsDeleteHoldPhase{}, false
	}
	return hold.phase, true
}

// EnableInlineVolumeDeletes lets first-time deletions run inline under
// liveXFSDeleteBudget. The Backend calls it once its hold executor runs.
func (x *xfsVolumeManager) EnableInlineVolumeDeletes() {
	x.inlineDeletes.Store(true)
}

// RetryHeldVolumeDelete runs one attempt of the held deletion of id under the
// caller's context, which the hold executor bounds to one slice. A name that is
// no longer held needs no work.
func (x *xfsVolumeManager) RetryHeldVolumeDelete(ctx context.Context, id string) error {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return fmt.Errorf("validate xfs volume ID for held delete: %w", err)
	}
	x.mu.Lock()
	hold, held := x.deleteHolds[volumeID.value()]
	var stage xfsDeleteStageName
	if held {
		stage = hold.stage
	}
	x.mu.Unlock()
	if !held {
		return nil
	}
	outcome := x.cleanupXFSDeleteStageWith(
		ctx, stage, removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot,
	)
	return outcome.result(finalPathNotObserved)
}

// VolumeDeleteHolds returns the manager's pending deletions and holds.
func (x *xfsVolumeManager) VolumeDeleteHolds() volumeDeleteHoldSnapshot {
	x.mu.Lock()
	defer x.mu.Unlock()
	snapshot := volumeDeleteHoldSnapshot{
		pending: make(map[string]struct{}, len(x.durableDeleteStages)+len(x.recoveredDeleteStages)),
		holds:   make(map[string]volumeDeleteHoldView, len(x.deleteHolds)),
	}
	for volume := range x.durableDeleteStages {
		snapshot.pending[volume] = struct{}{}
	}
	for volume := range x.recoveredDeleteStages {
		snapshot.pending[volume] = struct{}{}
	}
	for volume, hold := range x.deleteHolds {
		snapshot.pending[volume] = struct{}{}
		snapshot.holds[volume] = hold.view()
	}
	return snapshot
}

// PrecheckDestroy answers a destroy without the namespace lock when the
// manager's own state is enough. See volumeReader.
func (x *xfsVolumeManager) PrecheckDestroy(name managedVolumeName) (destroyPrecheckVerdict, error) {
	if outcome, held := x.registeredHoldOutcome(name); held {
		if !outcome.phase.residual {
			return destroyPrecheckHeld, outcome.result(finalPathNotObserved)
		}
		if x.observeFinalPathPinned(name).absent() {
			return destroyPrecheckGone, nil
		}
		return destroyPrecheckNeedsLock, nil
	}
	// Observe before reading the maps: a create or delete that begins before
	// the Lstat leaves a mapping or stage the check below then sees.
	if !x.observeFinalPathPinned(name).absent() {
		return destroyPrecheckNeedsLock, nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.pendingMutationNamesLocked(name)) != 0 {
		return destroyPrecheckNeedsLock, nil
	}
	if _, mapped := x.volumeToID[name.value()]; mapped {
		return destroyPrecheckNeedsLock, nil
	}
	for _, owner := range x.activeIDs {
		if owner == name.value() {
			return destroyPrecheckNeedsLock, nil
		}
	}
	return destroyPrecheckGone, nil
}

// RequireNoUnheldVolumeMutations is Start's gate; see volumeReader. Every
// create stage, and every delete stage without a hold for that exact stage,
// refuses.
func (x *xfsVolumeManager) RequireNoUnheldVolumeMutations(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	x.mu.Lock()
	var names []string
	for _, recovered := range x.recoveredStages {
		names = append(names, recovered.stage.value())
	}
	for _, stage := range x.durableStages {
		names = append(names, stage.value())
	}
	unheld := func(stage xfsDeleteStageName) bool {
		hold, ok := x.deleteHolds[stage.volumeID.value()]
		return !ok || hold.stage != stage
	}
	for _, recovered := range x.recoveredDeleteStages {
		if unheld(recovered.stage) {
			names = append(names, recovered.stage.value())
		}
	}
	for _, stage := range x.durableDeleteStages {
		if unheld(stage) {
			names = append(names, stage.value())
		}
	}
	x.mu.Unlock()
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	return fmt.Errorf("xfs volume root contains interrupted creates or unheld deletes: %s", strings.Join(names, ", "))
}

// removalVisibleDeleteNamesLocked returns every name with a delete stage whose
// final path's absence is not yet durable: the stage is in flight, or held in
// the removal phase. ListForProof unions them with the on-disk listing so that
// no consumer reads such a name's absence as completion. Residual names are
// settled; the admission pool accounts for them instead. The caller holds x.mu.
func (x *xfsVolumeManager) removalVisibleDeleteNamesLocked() []string {
	var names []string
	add := func(stage xfsDeleteStageName) {
		if hold, ok := x.deleteHolds[stage.volumeID.value()]; ok && hold.stage == stage && hold.phase.residual {
			return
		}
		names = append(names, stage.volumeID.value())
	}
	for _, stage := range x.durableDeleteStages {
		add(stage)
	}
	for _, recovered := range x.recoveredDeleteStages {
		add(recovered.stage)
	}
	return names
}

// unionSortedNames returns the sorted, de-duplicated union of two listings.
func unionSortedNames(listed, extra []string) []string {
	seen := make(map[string]struct{}, len(listed)+len(extra))
	union := make([]string, 0, len(listed)+len(extra))
	for _, group := range [][]string{listed, extra} {
		for _, name := range group {
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			union = append(union, name)
		}
	}
	sort.Strings(union)
	return union
}

// deleteStageVerified reports whether this process already normalized stage
// to project 0, fsynced it, and still sees the same directory.
func (x *xfsVolumeManager) deleteStageVerified(stage xfsDeleteStageName, info os.FileInfo) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	verified, ok := x.verifiedDeleteStages[stage]
	return ok && os.SameFile(verified, info)
}

func (x *xfsVolumeManager) rememberVerifiedDeleteStage(stage xfsDeleteStageName, info os.FileInfo) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.verifiedDeleteStages == nil {
		x.verifiedDeleteStages = make(map[xfsDeleteStageName]os.FileInfo)
	}
	x.verifiedDeleteStages[stage] = info
}

// promoteRecoveredResidualHolds sizes the holds Start registered for recovered
// stages whose final path is already gone, so that the admission pool counts
// their projects before the Backend serves. It does no removal and no waiting:
// one Lstat per stage, one parent sync that makes the observed absences
// durable, and one report-row read per stage. A stage it cannot size stays in
// the removal phase, and the hold executor sizes it on its first attempt.
func (x *xfsVolumeManager) promoteRecoveredResidualHolds(ctx context.Context) {
	x.mu.Lock()
	var candidates []xfsDeleteStageName
	for _, hold := range x.deleteHolds {
		if hold.reason == holdReasonRecovered && !hold.phase.residual {
			candidates = append(candidates, hold.stage)
		}
	}
	x.mu.Unlock()
	if len(candidates) == 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].value() < candidates[j].value() })
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		x.logger.Warn("cannot size recovered volume delete holds at startup", "error", err)
		return
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()
	var absent []xfsDeleteStageName
	for _, stage := range candidates {
		if observeFinalPathAtRoot(root, stage.volumeID).absent() {
			absent = append(absent, stage)
		}
	}
	if len(absent) == 0 {
		return
	}
	if err := parent.Sync(); err != nil {
		x.logger.Warn("cannot make recovered volume deletions durable at startup; holds stay in the removal phase",
			"error", err)
		return
	}
	for _, stage := range absent {
		if ctx.Err() != nil {
			return
		}
		readCtx, cancel := context.WithTimeout(ctx, xfsResidualRowReadTimeout)
		row, err := x.readProjectQuotaRow(readCtx, stage.projID, "b")
		cancel()
		if err != nil {
			x.logger.Warn("cannot size recovered volume delete hold at startup; it stays in the removal phase",
				"volume_id", stage.volumeID.value(), "delete_stage", stage.value(), "project_id", stage.projID,
				"error", err)
			continue
		}
		footprint, ok := residualFootprintFromRow(row)
		if !ok {
			continue
		}
		sized := xfsDeleteAttempt{absenceDurable: true, footprint: footprint, footprintKnown: true}
		x.recordXFSDeleteStageOutcome(stage, sized,
			holdable(holdReasonRecovered, errDeleteStageRecovered), false)
	}
}
