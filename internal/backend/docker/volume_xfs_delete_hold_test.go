package docker

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fstree"
)

// The settle function is total, and its default is the latch: only an error
// marked holdable at an allowlisted site becomes a hold, and never when its
// chain also carries an ambiguous outcome or a recovery-pending contradiction.
func TestClassifyXFSDeleteStageCleanupIsTotalWithLatchDefault(t *testing.T) {
	t.Parallel()

	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	cause := errors.New("cause")
	for _, tc := range []struct {
		name   string
		err    error
		kind   deleteStageOutcomeKind
		reason volumeDeleteHoldReason
	}{
		{name: "nil completes", err: nil, kind: deleteStageCompleted},
		{name: "unknown error latches", err: errors.New("never seen before"), kind: deleteStageLatched},
		{name: "plain errno latches", err: fmt.Errorf("x: %w", unix.EIO), kind: deleteStageLatched},
		{name: "holdable holds", err: holdable(holdReasonUndeletable, cause), kind: deleteStageHeld,
			reason: holdReasonUndeletable},
		{name: "wrapped holdable holds", err: fmt.Errorf("outer: %w", holdable(holdReasonUsageNonzero, cause)),
			kind: deleteStageHeld, reason: holdReasonUsageNonzero},
		{name: "holdable over an ambiguous outcome latches",
			err:  holdable(holdReasonFinalRemovalFailed, backendidentity.ErrMutationOutcomeAmbiguous),
			kind: deleteStageLatched},
		{name: "holdable joined with an ambiguous re-read latches",
			err:  errors.Join(holdable(holdReasonStageRemovalFailed, cause), backendidentity.ErrMutationOutcomeAmbiguous),
			kind: deleteStageLatched},
		{name: "holdable over recovery pending latches",
			err:  holdable(holdReasonRemovalFailed, ErrVolumeMutationRecoveryPending),
			kind: deleteStageLatched},
		{name: "an error that merely says held latches", err: ErrVolumeDeleteHeld, kind: deleteStageLatched},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			outcome := classifyXFSDeleteStageCleanup(stage, xfsDeleteAttempt{}, nil, tc.err)
			assert.Equal(t, tc.kind, outcome.kind)
			result := outcome.result(finalPathNotObserved)
			switch tc.kind {
			case deleteStageCompleted:
				require.NoError(t, result)
			case deleteStageHeld:
				assert.Equal(t, tc.reason, outcome.reason)
				require.ErrorIs(t, result, ErrVolumeDeleteHeld)
				require.ErrorIs(t, result, cause, "a held error always wraps the original chain")
				require.NotErrorIs(t, result, ErrVolumeMutationRecoveryPending)
			case deleteStageLatched:
				require.ErrorIs(t, result, ErrVolumeMutationRecoveryPending)
				if tc.err != nil {
					require.ErrorIs(t, result, tc.err, "a latch preserves the original chain")
				}
			}
		})
	}
}

// Residual needs a durable absence and a footprint parsed from the project's
// row; a residual hold that loses neither stays residual, and only a removal
// attempt that sees the final directory leaves it in the removal phase.
func TestHoldPhaseAfterRequiresAParsedFootprintForResidual(t *testing.T) {
	t.Parallel()

	row, err := parseXfsReportRow("#4242 10 0 2048 0\n", xfsDeleteTestProjectID)
	require.NoError(t, err)
	footprint, ok := residualFootprintFromRow(row)
	require.True(t, ok)
	residualPhase, ok := residualHoldPhase(footprint)
	require.True(t, ok)
	residual := &xfsDeleteHold{phase: residualPhase}
	unsized := &xfsDeleteHold{phase: unsizedHoldPhase()}
	removal := &xfsDeleteHold{phase: removalHoldPhase()}
	for _, tc := range []struct {
		name     string
		attempt  xfsDeleteAttempt
		previous *xfsDeleteHold
		want     xfsDeleteHoldPhase
	}{
		{"nothing observed", xfsDeleteAttempt{}, nil, removalHoldPhase()},
		{"nothing observed keeps residual", xfsDeleteAttempt{}, residual, residual.phase},
		{"nothing observed keeps unsized", xfsDeleteAttempt{}, unsized, unsizedHoldPhase()},
		{"final seen", xfsDeleteAttempt{finalSeen: true}, nil, removalHoldPhase()},
		{"durable absence without footprint", xfsDeleteAttempt{absenceDurable: true}, nil, removalHoldPhase()},
		{"durable absence without footprint keeps a pending caller pending",
			xfsDeleteAttempt{absenceDurable: true}, removal, removalHoldPhase()},
		{"durable absence keeps a known footprint", xfsDeleteAttempt{absenceDurable: true}, residual, residual.phase},
		{"durable absence never turns unsized into removal", xfsDeleteAttempt{absenceDurable: true}, unsized,
			unsizedHoldPhase()},
		{"durable absence with footprint", xfsDeleteAttempt{absenceDurable: true, footprint: footprint}, nil,
			residualPhase},
		{"durable absence sizes an unsized hold", xfsDeleteAttempt{absenceDurable: true, footprint: footprint},
			unsized, residualPhase},
		{"an unsized footprint is never residual", xfsDeleteAttempt{absenceDurable: true,
			footprint: residualFootprintMB{}}, nil, removalHoldPhase()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, holdPhaseAfter(tc.attempt, tc.previous))
		})
	}
}

// Start classifies a recovered stage, whose history is unknown, from what it
// observes: a final path still present cannot have settled a caller, while a
// gone one may have, so it is never left in the removal phase.
func TestRecoveredHoldPhaseNeverLeavesAGoneVolumeUncounted(t *testing.T) {
	t.Parallel()

	row, err := parseXfsReportRow("#4242 0 0 4096 0\n", xfsDeleteTestProjectID)
	require.NoError(t, err)
	footprint, ok := residualFootprintFromRow(row)
	require.True(t, ok)
	residualPhase, ok := residualHoldPhase(footprint)
	require.True(t, ok)
	for _, tc := range []struct {
		name      string
		final     finalPathAtStart
		footprint residualFootprintMB
		want      xfsDeleteHoldPhase
	}{
		{"present", finalPresentAtStart, footprint, removalHoldPhase()},
		{"gone, durable and sized", finalAbsentDurableAtStart, footprint, residualPhase},
		{"gone and durable, unsized", finalAbsentDurableAtStart, residualFootprintMB{}, unsizedHoldPhase()},
		{"gone, not durable", finalAbsentAtStart, footprint, unsizedHoldPhase()},
		{"unknown observation", 0, footprint, unsizedHoldPhase()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, recoveredHoldPhase(tc.final, tc.footprint))
		})
	}
}

func TestResidualFootprintIsBuiltOnlyFromAParsedRow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		out  string
		mb   int64
		ok   bool
	}{
		{"no dquot", "", 0, true},
		{"hard limit bounds the footprint", "#4242 7 0 20480 0\n", 20, true},
		{"usage above an absent limit", "#4242 3000 0 0 0\n", 3, true},
		{"partial MiB rounds up", "#4242 0 0 1025 0\n", 2, true},
		{"row without limit columns", "#4242 7\n", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			row, err := parseXfsReportRow(tc.out, xfsDeleteTestProjectID)
			require.NoError(t, err)
			footprint, ok := residualFootprintFromRow(row)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.ok, footprint.sized())
			if ok {
				assert.Equal(t, tc.mb, footprint.mb)
			}
		})
	}
	_, err := parseXfsReportRow("#4242 7 0 not-a-limit 0\n", xfsDeleteTestProjectID)
	require.ErrorContains(t, err, "hard limit", "a malformed limit must never be guessed")

	// The zero row that travels beside every read error is not a parsed
	// report: it must never read as an absent dquot (0 MiB).
	footprint, ok := residualFootprintFromRow(xfsProjectQuotaRow{})
	assert.False(t, ok)
	assert.False(t, footprint.sized())
	_, ok = residualHoldPhase(footprint)
	assert.False(t, ok, "there is no residual phase without a sized footprint")
	_, readErr := (&xfsVolumeManager{}).readProjectQuotaRow(t.Context(), xfsDeleteTestProjectID, "x")
	require.Error(t, readErr)
}

func TestVolumeDeleteHoldBackoffDoublesToItsCap(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30*time.Second, volumeDeleteHoldBackoff(1))
	assert.Equal(t, time.Minute, volumeDeleteHoldBackoff(2))
	assert.Equal(t, 2*time.Minute, volumeDeleteHoldBackoff(3))
	assert.Equal(t, 30*time.Minute, volumeDeleteHoldBackoff(7))
	assert.Equal(t, 30*time.Minute, volumeDeleteHoldBackoff(1000))
	for _, reason := range []volumeDeleteHoldReason{holdReasonRecovered, holdReasonDeadline, holdReasonStopped} {
		assert.True(t, reason.keepsProgressing(), "%s must stay due", reason)
	}
	for _, reason := range []volumeDeleteHoldReason{
		holdReasonRemovalFailed, holdReasonTreeChanged, holdReasonCrossDevice, holdReasonUndeletable,
		holdReasonCutRefused, holdReasonWriterActive, holdReasonFinalRemovalFailed, holdReasonUsageUnprovable,
		holdReasonUsageNonzero, holdReasonQuotaClearFailed, holdReasonStageRemovalFailed,
	} {
		assert.False(t, reason.keepsProgressing(), "%s must back off", reason)
		assert.NotEqual(t, "unknown", reason.String())
	}
}

func TestVolumeDeleteHoldSnapshotDueOrderRotates(t *testing.T) {
	t.Parallel()

	now := time.Now()
	name := func(i int) managedVolumeName {
		parsed, err := parseManagedVolumeName(canonicalVolumeName("550e8400-e29b-41d4-a716-446655440000", "app", i))
		require.NoError(t, err)
		return parsed
	}
	snapshot := volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{
		name(0).value(): {volume: name(0), lastAttempt: now.Add(-time.Minute), nextAttempt: now.Add(-time.Second)},
		name(1).value(): {volume: name(1), nextAttempt: now},
		name(2).value(): {volume: name(2), lastAttempt: now.Add(-time.Hour), nextAttempt: now.Add(-time.Second)},
		name(3).value(): {volume: name(3), lastAttempt: now.Add(-time.Hour), nextAttempt: now.Add(time.Minute)},
		name(4).value(): {volume: name(4), phase: holdPhaseResidual, footprintMB: 7, nextAttempt: now.Add(time.Hour)},
		name(5).value(): {volume: name(5), phase: holdPhaseUnsized, lastAttempt: now, nextAttempt: now},
	}}
	due := snapshot.dueInOrder(now)
	var got []string
	for _, hold := range due {
		got = append(got, hold.volume.value())
	}
	assert.Equal(t, []string{name(5).value(), name(1).value(), name(2).value(), name(0).value()}, got,
		"unsized first, then never-attempted, then least recently attempted; not-yet-due holds wait")
	assert.Equal(t, map[string]int{
		volumeDeleteHoldPhaseRemoval: 4, volumeDeleteHoldPhaseUnsized: 1, volumeDeleteHoldPhaseResidual: 1,
	}, snapshot.phaseCounts())
	assert.Equal(t, heldDeletionAccount{residualMB: 7, unsized: 1}, snapshot.admissionAccount())
	assert.True(t, snapshot.callerHeld(name(0).value()))
	assert.True(t, snapshot.callerHeld(name(5).value()), "an unsized hold keeps its caller pending")
	assert.False(t, snapshot.callerHeld(name(4).value()))
	assert.True(t, snapshot.residualHeld(name(4).value()))
	assert.Equal(t, map[string]int{
		volumeDeleteHoldPhaseRemoval: 0, volumeDeleteHoldPhaseUnsized: 0, volumeDeleteHoldPhaseResidual: 0,
	}, volumeDeleteHoldSnapshot{}.phaseCounts(), "every phase is reported, so a gauge drops to zero")
}

// xfsSiteCase drives one return site of the real delete-stage cleanup.
type xfsSiteCase struct {
	name string
	// setup prepares the volume and returns the seams for the attempt.
	setup func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
		ctx context.Context, removeContent xfsRemoveTree, removeFinal, removeStage xfsRemove)
	quota    string
	held     bool
	reason   volumeDeleteHoldReason
	residual bool
}

func withVolume(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName, marker bool) string {
	t.Helper()
	volumePath := stage.volumeID.hostPath(mgr.dataPath)
	require.NoError(t, os.MkdirAll(filepath.Join(volumePath, writablePathSubdir), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, writablePathSubdir, "data"), []byte("x"), 0o600))
	if marker {
		require.NoError(t, writeProjectIDFile(volumePath, stage.projID))
	}
	return volumePath
}

func failingRemoval(err error) xfsRemoveTree {
	return func(context.Context, condemnedXFSVolume, fstree.Name) error { return err }
}

// TestXFSDeleteCleanupClassifiesEveryFailureSite drives every allowlisted
// return site of the cleanup, and representative latch sites including both
// ambiguous re-read arms, through the real attempt and settle.
func TestXFSDeleteCleanupClassifiesEveryFailureSite(t *testing.T) {
	zeroUsage := ""
	stageFixture := func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
		context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
		withVolume(t, mgr, stage, true)
		return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
	}
	removalError := func(err error) func(*testing.T, *xfsVolumeManager, xfsDeleteStageName) (
		context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
		return func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
			context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
			withVolume(t, mgr, stage, true)
			return t.Context(), failingRemoval(err), removeFromXFSRoot, removeFromXFSRoot
		}
	}
	// denyLookups makes the data root unsearchable inside a removal callback,
	// so the re-read that follows fails with EACCES: an ambiguous outcome.
	denyLookups := func(t *testing.T, mgr *xfsVolumeManager, remove xfsRemove) xfsRemove {
		return func(root *os.Root, name string) error {
			err := remove(root, name)
			require.NoError(t, os.Chmod(mgr.dataPath, 0o600))
			t.Cleanup(func() { _ = os.Chmod(mgr.dataPath, 0o700) })
			return err
		}
	}

	cases := []xfsSiteCase{
		{name: "removal: unclassified errno", setup: removalError(fmt.Errorf("x: %w", unix.EMFILE)),
			held: true, reason: holdReasonRemovalFailed},
		{name: "removal: undeletable", setup: removalError(fmt.Errorf("x: %w", fstree.ErrUndeletable)),
			held: true, reason: holdReasonUndeletable},
		{name: "removal: cut refused", setup: removalError(fmt.Errorf("x: %w", fstree.ErrCutRefused)),
			held: true, reason: holdReasonCutRefused},
		{name: "removal: cross device", setup: removalError(fmt.Errorf("x: %w", fstree.ErrCrossDevice)),
			held: true, reason: holdReasonCrossDevice},
		{name: "removal: tree changed", setup: removalError(fmt.Errorf("x: %w", fstree.ErrTreeChanged)),
			held: true, reason: holdReasonTreeChanged},
		{name: "removal: walk deadline", setup: removalError(fmt.Errorf("x: %w", context.DeadlineExceeded)),
			held: true, reason: holdReasonDeadline},
		{name: "removal: walk canceled", setup: removalError(fmt.Errorf("x: %w", context.Canceled)),
			held: true, reason: holdReasonStopped},
		{
			name: "removal: writer active",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				volumePath := withVolume(t, mgr, stage, true)
				return t.Context(), func(ctx context.Context, volume condemnedXFSVolume, name fstree.Name) error {
					if err := removeCondemnedXFSEntry(ctx, volume, name); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(volumePath, "late-writer"), []byte("x"), 0o600)
				}, removeFromXFSRoot, removeFromXFSRoot
			},
			held: true, reason: holdReasonWriterActive,
		},
		{
			name: "removal: emptied volume rmdir fails and it remains",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				return t.Context(), removeCondemnedXFSEntry,
					func(*os.Root, string) error { return unix.EBUSY }, removeFromXFSRoot
			},
			held: true, reason: holdReasonFinalRemovalFailed,
		},
		{
			name: "removal: emptied volume rmdir reports success but it remains",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				return t.Context(), removeCondemnedXFSEntry,
					func(*os.Root, string) error { return nil }, removeFromXFSRoot
			},
			held: true, reason: holdReasonFinalRemovalFailed,
		},
		{
			name: "removal: ambiguous re-read after the volume rmdir latches",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				return t.Context(), removeCondemnedXFSEntry, denyLookups(t, mgr, removeFromXFSRoot), removeFromXFSRoot
			},
		},
		{
			name: "removal: stop point after normalization",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx, removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
			},
			held: true, reason: holdReasonStopped,
		},
		{
			name:  "residual: usage nonzero",
			setup: stageFixture,
			quota: fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 7 0 2048 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, xfsDeleteTestProjectID, xfsDeleteTestProjectID),
			held: true, reason: holdReasonUsageNonzero, residual: true,
		},
		{
			name:  "residual: usage unprovable",
			setup: stageFixture,
			quota: `case "$*" in
  *"report -p -b -n -N"*) exit 23 ;;
esac`,
			held: true, reason: holdReasonUsageUnprovable,
		},
		{
			name:  "residual: quota clear fails",
			setup: stageFixture,
			quota: `case "$*" in
  *"bhard=0 bsoft=0 ihard=0 isoft=0"*) exit 19 ;;
esac`,
			held: true, reason: holdReasonQuotaClearFailed, residual: true,
		},
		{
			name: "residual: stage rmdir fails and it remains",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot,
					func(*os.Root, string) error { return unix.EBUSY }
			},
			quota: zeroUsage, held: true, reason: holdReasonStageRemovalFailed, residual: true,
		},
		{
			name: "residual: ambiguous re-read after the stage rmdir latches",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, denyLookups(t, mgr, removeFromXFSRoot)
			},
			quota: zeroUsage,
		},
		{
			name: "latch: project-ID authority conflict",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				mgr.activeIDs[stage.projID] = "fred-550e8400-e29b-41d4-a716-446655440000-other-0"
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
			},
		},
		{
			name: "latch: stage holds foreign content",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				require.NoError(t, os.WriteFile(filepath.Join(stage.hostPath(mgr.dataPath), "foreign"), []byte("x"), 0o600))
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
			},
		},
		{
			name: "latch: volume marker names another project",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				volumePath := withVolume(t, mgr, stage, false)
				require.NoError(t, writeProjectIDFile(volumePath, stage.projID+1))
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
			},
		},
		{
			name: "latch: stage normalization fails",
			setup: func(t *testing.T, mgr *xfsVolumeManager, stage xfsDeleteStageName) (
				context.Context, xfsRemoveTree, xfsRemove, xfsRemove) {
				withVolume(t, mgr, stage, true)
				mgr.projectAttributes = fixedXFSProjectAttributeReader{setErr: errors.New("injected FSSETXATTR EIO")}
				return t.Context(), removeCondemnedXFSEntry, removeFromXFSRoot, removeFromXFSRoot
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getuid() == 0 && strings.Contains(tc.name, "ambiguous re-read") {
				t.Skip("root bypasses directory search permission, so the re-read cannot be made to fail")
			}
			dataPath := t.TempDir()
			mgr := newXfsManagerForTest(dataPath)
			stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
			prepareDeleteStageForTest(t, mgr, stage)
			installXFSQuotaFixture(t, tc.quota)
			ctx, removeContent, removeFinal, removeStage := tc.setup(t, mgr, stage)
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			outcome := mgr.cleanupXFSDeleteStageWith(ctx, stage, removeContent, removeFinal, removeStage)
			err := outcome.result(finalPathNotObserved)
			if !tc.held {
				assert.Equal(t, deleteStageLatched, outcome.kind)
				require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
				require.NotErrorIs(t, err, ErrVolumeDeleteHeld)
				return
			}
			require.Equal(t, deleteStageHeld, outcome.kind, "%v", outcome.err)
			require.ErrorIs(t, err, ErrVolumeDeleteHeld)
			require.NotErrorIs(t, err, ErrVolumeMutationRecoveryPending)
			hold := heldForTest(t, mgr, stage.volumeID.value())
			assert.Equal(t, tc.reason, hold.reason)
			assert.Equal(t, tc.residual, hold.phase == holdPhaseResidual)
			assert.DirExists(t, stage.hostPath(dataPath), "a hold always keeps its stage")
			assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()], "a hold keeps the project ID")
		})
	}
}

// A held deletion is answered from the hold without touching the filesystem;
// only the hold executor's retry runs held work. A residual hold settles a
// Destroy caller after an Lstat proves the final path absent, and latches if
// the final path is there again.
func TestXFSDestroyAnswersAHoldWithoutWork(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	volumePath := withVolume(t, mgr, stage, true)
	installXFSQuotaFixture(t, "")
	injected := fmt.Errorf("x: %w", fstree.ErrUndeletable)
	err := mgr.destroyWith(t.Context(), stage.volumeID.value(), failingRemoval(injected))
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)

	mustNotRun := func(context.Context, condemnedXFSVolume, fstree.Name) error {
		t.Fatal("a held deletion must not run its cleanup outside the hold executor")
		return nil
	}
	err = mgr.destroyWith(t.Context(), stage.volumeID.value(), mustNotRun)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld, "a removal-phase hold answers held")
	assert.Equal(t, 1, heldForTest(t, mgr, stage.volumeID.value()).attempts, "the answer counts no attempt")
	assert.DirExists(t, volumePath)

	require.NoError(t, mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value()))
	assert.NoDirExists(t, volumePath)
	assert.NoDirExists(t, stage.hostPath(dataPath))
	require.NoError(t, mgr.destroyWith(t.Context(), stage.volumeID.value(), mustNotRun),
		"a completed deletion leaves nothing to destroy")
}

func TestXFSResidualHoldLatchesWhenTheFinalPathExistsAgain(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 7 0 2048 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, stage.projID, stage.projID))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, mgr.Destroy(ctx, stage.volumeID.value()))
	require.True(t, heldForTest(t, mgr, stage.volumeID.value()).phase == holdPhaseResidual)

	require.NoError(t, os.Mkdir(stage.volumeID.hostPath(dataPath), 0o700))
	err := mgr.Destroy(t.Context(), stage.volumeID.value())
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending,
		"a final path that exists again under a residual hold is an authority contradiction")
	err = mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value())
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending, "the executor must not remove it either")
	assert.DirExists(t, stage.volumeID.hostPath(dataPath))
}

// Before the hold executor runs (during Start), a first-time Destroy mints its
// stage and is held at once, removing nothing, so Start never waits on a tree.
func TestXFSDestroyBeforeTheExecutorRunsIsHeldWithoutAnAttempt(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	mgr.inlineDeletes.Store(false)
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	volumePath := filepath.Join(dataPath, name)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, writeProjectIDFile(volumePath, xfsDeleteTestProjectID))
	logPath := installLoggingXFSQuota(t)

	err := mgr.destroyWith(t.Context(), name, func(context.Context, condemnedXFSVolume, fstree.Name) error {
		t.Fatal("Start must never remove inline")
		return nil
	})
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	hold := heldForTest(t, mgr, name)
	assert.Equal(t, holdReasonDeadline, hold.reason)
	assert.Zero(t, hold.attempts)
	assert.False(t, time.Now().Before(hold.nextAttempt), "a deferred deletion is due at once")
	assert.FileExists(t, filepath.Join(volumePath, projectIDFile))
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, name)
	assert.DirExists(t, stage.hostPath(dataPath), "the stage is minted and durable")
	assert.NoFileExists(t, logPath)
}

// A retry of a held deletion reads the stage's attributes instead of
// rewriting and re-syncing a stage this process already verified.
func TestXFSRetrySkipsNormalizingAVerifiedStage(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	var sets, reads int
	mgr.projectAttributes = xfsProjectAttributeFuncs{
		read: func(*os.Root) (linuxFSXAttr, error) {
			reads++
			return linuxFSXAttr{XFlags: linuxFSXFlagProjInherit}, nil
		},
		set: func(*os.Root, uint32) error {
			sets++
			return nil
		},
	}
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	volumePath := filepath.Join(dataPath, name)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, writeProjectIDFile(volumePath, xfsDeleteTestProjectID))
	installXFSQuotaFixture(t, "")
	require.ErrorIs(t, mgr.destroyWith(t.Context(), name, failingRemoval(fmt.Errorf("x: %w", unix.EIO))),
		ErrVolumeDeleteHeld)
	require.Equal(t, 1, sets, "prepare normalizes the new stage once")

	readsBefore := reads
	require.NoError(t, mgr.RetryHeldVolumeDelete(t.Context(), name))
	assert.Equal(t, 1, sets, "the retry must not rewrite a stage this process verified")
	assert.Greater(t, reads, readsBefore, "the retry still reads the attributes")
}

// ListForProof keeps every in-flight or removal-phase deletion visible, once,
// even when its final directory is gone, and the inventory proof accepts it,
// across both scans of one process (New's probe, then Start's Validate). A
// residual deletion is settled and leaves the listing.
func TestXFSListForProofKeepsRemovalPhaseDeletesVisibleAcrossBothScans(t *testing.T) {
	dataPath := t.TempDir()
	removing := mustXFSDeleteStage(t, xfsDeleteTestProjectID,
		"fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	residual := mustXFSDeleteStage(t, xfsDeleteTestProjectID+1,
		"fred-550e8400-e29b-41d4-a716-446655440000-app-1")
	live, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-2")
	require.NoError(t, err)
	// (a) a removal-phase stage whose final directory exists without its marker;
	// (b) a stage whose final directory is gone; and a live volume.
	require.NoError(t, os.Mkdir(removing.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(removing.volumeID.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(residual.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(live.hostPath(dataPath), 0o700))
	require.NoError(t, writeProjectIDFile(live.hostPath(dataPath), xfsDeleteTestProjectID+2))
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 0 0 4096 0\n' ;;
esac`, xfsDeleteTestProjectID+1))

	mgr := newXfsManagerForTest(dataPath)
	for scan := range 2 {
		require.NoError(t, mgr.loadProjectIDs(), "scan %d", scan)
		names, err := mgr.ListForProof(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{removing.volumeID.value(), residual.volumeID.value(), live.value()}, names,
			"scan %d: sorted, de-duplicated, and every unfinished deletion visible", scan)
		inventory, err := attestManagedVolumeInventory(t.Context(), mgr)
		require.NoError(t, err, "scan %d", scan)
		assert.Len(t, inventory, 3)
		raw, err := mgr.List()
		require.NoError(t, err)
		assert.NotContains(t, raw, residual.volumeID.value(), "List stays the raw on-disk listing")
	}

	require.NoError(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))
	require.True(t, heldForTest(t, mgr, residual.volumeID.value()).phase == holdPhaseResidual,
		"Start sizes a recovered deletion whose final path is gone")
	assert.Equal(t, int64(4), mgr.VolumeDeleteHolds().admissionAccount().residualMB)
	names, err := mgr.ListForProof(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{removing.volumeID.value(), live.value()}, names,
		"a residual deletion is settled and accounted in admission instead")
	require.ErrorContains(t, mgr.AttestManagedVolume(t.Context(), residual.volumeID), "does not exist")
	require.NoError(t, mgr.RequireNoUnheldVolumeMutations(t.Context()))
	require.Error(t, mgr.RequireNoInterruptedVolumeMutations(t.Context()))
}

// A recovered deletion whose final path is gone may already have settled its
// caller in a previous process. When Start cannot size it, it is unsized:
// never the removal phase, where its footprint would be counted by nothing.
// It keeps its caller (if any) pending and stays listed, withholds disk
// admission, and is retried on every executor pass, without backoff, until a
// read sizes it.
func TestXFSRecoveredGoneDeletionStaysUnsizedUntilARowSizesIt(t *testing.T) {
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700)) // the final path is already gone
	sizable := filepath.Join(t.TempDir(), "sizable")
	t.Setenv("FRED_TEST_XFS_SIZABLE", sizable)
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) [ -e "$FRED_TEST_XFS_SIZABLE" ] || exit 23; printf '#%d 5 0 2048 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, stage.projID, stage.projID))
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.PinIdentityRoot())
	require.NoError(t, mgr.loadProjectIDs())
	require.NoError(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))

	hold := heldForTest(t, mgr, stage.volumeID.value())
	assert.Equal(t, holdPhaseUnsized, hold.phase, "an unsized footprint is never counted as zero")
	assert.Equal(t, heldDeletionAccount{unsized: 1}, mgr.VolumeDeleteHolds().admissionAccount())
	require.ErrorIs(t, mgr.Destroy(t.Context(), stage.volumeID.value()), ErrVolumeDeleteHeld,
		"a caller that is still pending stays pending")
	verdict, err := mgr.PrecheckDestroy(stage.volumeID)
	assert.Equal(t, destroyPrecheckHeld, verdict)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	names, err := mgr.ListForProof(t.Context())
	require.NoError(t, err)
	assert.Contains(t, names, stage.volumeID.value(), "no consumer may read its absence as completion")

	// The executor's retry cannot size it either: it stays unsized and due.
	err = mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value())
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	hold = heldForTest(t, mgr, stage.volumeID.value())
	assert.Equal(t, holdPhaseUnsized, hold.phase)
	assert.Equal(t, holdReasonUsageUnprovable, hold.reason)
	assert.False(t, time.Now().Before(hold.nextAttempt), "an unsized hold is retried on the next pass")

	// Once a row can be read, the retry sizes it: residual, counted at its
	// block hard limit (2048 KiB = 2 MiB), and no longer withholding disk.
	require.NoError(t, os.WriteFile(sizable, []byte("x"), 0o600))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.ErrorIs(t, mgr.RetryHeldVolumeDelete(ctx, stage.volumeID.value()), ErrVolumeDeleteHeld)
	hold = heldForTest(t, mgr, stage.volumeID.value())
	assert.Equal(t, holdPhaseResidual, hold.phase)
	assert.Equal(t, heldDeletionAccount{residualMB: 2}, mgr.VolumeDeleteHolds().admissionAccount())
	require.NoError(t, mgr.Destroy(t.Context(), stage.volumeID.value()), "a residual hold settles its caller")
}

// A recovered deletion whose final path is still there cannot have settled
// any caller: Start leaves it in the removal phase without reading its quota.
// One whose final path reappears after Start observed it gone is an authority
// contradiction, never tenant data to remove.
func TestXFSRecoveredDeletionsAreClassifiedByWhatStartObserves(t *testing.T) {
	dataPath := t.TempDir()
	present := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	gone := mustXFSDeleteStage(t, xfsDeleteTestProjectID+1, "fred-550e8400-e29b-41d4-a716-446655440001-app-0")
	require.NoError(t, os.Mkdir(present.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(present.volumeID.hostPath(dataPath), 0o700)) // marker already removed
	require.NoError(t, os.Mkdir(gone.hostPath(dataPath), 0o700))
	logPath := installXFSQuotaFixture(t, `case "$*" in
  *"report -p -b -n -N"*) exit 23 ;;
esac`)
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	require.NoError(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))

	assert.Equal(t, holdPhaseRemoval, heldForTest(t, mgr, present.volumeID.value()).phase)
	assert.Equal(t, holdPhaseUnsized, heldForTest(t, mgr, gone.volumeID.value()).phase)
	assert.NotContains(t, readQuotaLog(t, logPath), fmt.Sprintf("-L %d", present.projID),
		"a present volume's quota is not read at Start")

	require.NoError(t, os.Mkdir(gone.volumeID.hostPath(dataPath), 0o700))
	err := mgr.RetryHeldVolumeDelete(t.Context(), gone.volumeID.value())
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending,
		"a final path seen gone at Start and there again is a contradiction")
	assert.DirExists(t, gone.volumeID.hostPath(dataPath), "it is never emptied as condemned data")
}

// A delete stage that disappears from disk releases its project only when
// this process's own attempt reached stage removal (after the zero-usage proof
// and the limit clear) and the final path is positively absent. A stage
// removed from outside at any earlier point is an authority contradiction: it
// latches and the project ID stays reserved, never a recorded completion.
func TestXFSVanishedDeleteStageCompletesOnlyAfterStageRemovalWithTheVolumeGone(t *testing.T) {
	t.Run("removed from outside during the removal phase", func(t *testing.T) {
		dataPath := t.TempDir()
		mgr := newXfsManagerForTest(dataPath)
		stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
		prepareDeleteStageForTest(t, mgr, stage)
		volumePath := withVolume(t, mgr, stage, true)
		installXFSQuotaFixture(t, "")
		require.ErrorIs(t, mgr.destroyWith(t.Context(), stage.volumeID.value(),
			failingRemoval(fmt.Errorf("x: %w", fstree.ErrUndeletable))), ErrVolumeDeleteHeld)
		require.NoError(t, os.Remove(stage.hostPath(dataPath)))

		err := mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value())
		require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending, "a vanished stage is a contradiction")
		assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()], "the project ID stays reserved")
		assert.Equal(t, stage.volumeID.value(), mgr.activeIDs[stage.projID])
		heldForTest(t, mgr, stage.volumeID.value())
		assert.DirExists(t, volumePath, "nothing is removed")
	})
	stageRemovalFailed := func(t *testing.T) (*xfsVolumeManager, xfsDeleteStageName) {
		dataPath := t.TempDir()
		mgr := newXfsManagerForTest(dataPath)
		stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
		prepareDeleteStageForTest(t, mgr, stage)
		withVolume(t, mgr, stage, true)
		installXFSQuotaFixture(t, "")
		outcome := mgr.cleanupXFSDeleteStageWith(t.Context(), stage, removeCondemnedXFSEntry, removeFromXFSRoot,
			func(*os.Root, string) error { return unix.EBUSY })
		require.Equal(t, deleteStageHeld, outcome.kind)
		require.Equal(t, holdReasonStageRemovalFailed, heldForTest(t, mgr, stage.volumeID.value()).reason)
		require.NoError(t, os.Remove(stage.hostPath(dataPath)))
		return mgr, stage
	}
	t.Run("gone after stage removal with the volume gone", func(t *testing.T) {
		mgr, stage := stageRemovalFailed(t)
		require.NoError(t, mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value()))
		assert.Empty(t, mgr.VolumeDeleteHolds().holds)
		assert.NotContains(t, mgr.volumeToID, stage.volumeID.value())
		assert.NotContains(t, mgr.activeIDs, stage.projID)
	})
	t.Run("gone after stage removal but the volume is back", func(t *testing.T) {
		mgr, stage := stageRemovalFailed(t)
		require.NoError(t, os.Mkdir(stage.volumeID.hostPath(mgr.dataPath), 0o700))
		err := mgr.RetryHeldVolumeDelete(t.Context(), stage.volumeID.value())
		require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
		assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()], "the project ID stays reserved")
	})
}

// Start's report-row reads share one aggregate budget: a hung xfs_quota delays
// Start by that budget once, not once per recovered stage, and every stage it
// could not size is unsized.
func TestXFSRecoveredSizingIsBoundedInAggregate(t *testing.T) {
	dataPath := t.TempDir()
	var stages []xfsDeleteStageName
	for i := range 4 {
		stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID+uint32(i),
			canonicalVolumeName("550e8400-e29b-41d4-a716-446655440000", "app", i))
		require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
		stages = append(stages, stage)
	}
	// The fixture's PATH holds only xfs_quota itself: name sleep absolutely so
	// the read really hangs instead of failing fast.
	sleepPath, err := exec.LookPath("sleep")
	require.NoError(t, err)
	logPath := installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) exec %s 5 ;;
esac`, sleepPath))
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())

	const budget = 300 * time.Millisecond
	started := time.Now()
	require.NoError(t, mgr.classifyRecoveredDeleteHoldsWithin(t.Context(), budget))
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, budget, "the first read really hung until the budget ended")
	assert.Less(t, elapsed, 3*time.Second, "four hung reads share one budget")
	assert.Equal(t, 1, strings.Count(readQuotaLog(t, logPath), "report -p -b"),
		"no read starts once the budget is spent")
	for _, stage := range stages {
		assert.Equal(t, holdPhaseUnsized, heldForTest(t, mgr, stage.volumeID.value()).phase)
	}
	assert.Equal(t, heldDeletionAccount{unsized: 4}, mgr.VolumeDeleteHolds().admissionAccount())
}

// A final path Start cannot observe is a fault of the shared root, as it is
// for the startup scan: Start fails rather than guess.
func TestXFSRecoveredClassificationFailsStartOnAnUnreadableRoot(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses directory search permission")
	}
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	require.NoError(t, os.Chmod(dataPath, 0o600))
	t.Cleanup(func() { _ = os.Chmod(dataPath, 0o700) })
	require.Error(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))
}

// Start's gate accepts held deletions and refuses everything else, while the
// strict gate of the exclusive commands still refuses held deletions.
func TestXFSStartGateAcceptsOnlyHeldDeletes(t *testing.T) {
	dataPath := t.TempDir()
	deleting := mustXFSDeleteStage(t, xfsDeleteTestProjectID, "fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, os.Mkdir(deleting.hostPath(dataPath), 0o700))
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	require.NoError(t, mgr.RequireNoUnheldVolumeMutations(t.Context()))
	require.ErrorContains(t, mgr.RequireNoInterruptedVolumeMutations(t.Context()), deleting.value())

	createVolume, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-1")
	require.NoError(t, err)
	createStage, err := newXFSStageName(xfsDeleteTestProjectID+1, createVolume)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(createStage.hostPath(dataPath), 0o700))
	require.NoError(t, mgr.loadProjectIDs())
	require.ErrorContains(t, mgr.RequireNoUnheldVolumeMutations(t.Context()), createStage.value(),
		"an interrupted create still refuses Start")
	assert.NotContains(t, mgr.RequireNoUnheldVolumeMutations(t.Context()).Error(), deleting.value())

	mgr.mu.Lock()
	delete(mgr.deleteHolds, deleting.volumeID.value())
	mgr.mu.Unlock()
	require.ErrorContains(t, mgr.RequireNoUnheldVolumeMutations(t.Context()), deleting.value(),
		"a delete stage the manager does not hold refuses Start")
}

// EnsureQuota skips a name whose deletion is pending: the delete authority
// owns its limits, and its marker is usually already gone.
func TestXFSEnsureQuotaSkipsADeletePendingName(t *testing.T) {
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(stage.volumeID.hostPath(dataPath), 0o700)) // marker-less
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	logPath := installLoggingXFSQuota(t)
	outcome, err := mgr.EnsureQuota(t.Context(), stage.volumeID.value(), 20)
	require.NoError(t, err)
	assert.Equal(t, volumeQuotaDeletePending, outcome, "a skipped name is never reported as applied")
	assert.NoFileExists(t, logPath, "no limit is touched for a held deletion")
	assert.True(t, mgr.VolumeDeleteHolds().deletePending(stage.volumeID.value()))
}

// The pre-lock query answers held and gone only from state it can vouch for:
// the hold table, and an Lstat bound to the pinned data root.
func TestXFSPrecheckDestroy(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	absent, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-5")
	require.NoError(t, err)

	verdict, err := mgr.PrecheckDestroy(absent)
	require.NoError(t, err)
	assert.Equal(t, destroyPrecheckNeedsLock, verdict, "an unpinned root cannot vouch for an absence")

	require.NoError(t, mgr.PinIdentityRoot())
	verdict, err = mgr.PrecheckDestroy(absent)
	require.NoError(t, err)
	assert.Equal(t, destroyPrecheckGone, verdict)

	mgr.mu.Lock()
	mgr.volumeToID[absent.value()] = 777
	mgr.activeIDs[777] = absent.value()
	mgr.mu.Unlock()
	verdict, err = mgr.PrecheckDestroy(absent)
	require.NoError(t, err)
	assert.Equal(t, destroyPrecheckNeedsLock, verdict, "a project mapping needs the locked Destroy")

	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	withVolume(t, mgr, stage, true)
	installXFSQuotaFixture(t, "")
	require.ErrorIs(t, mgr.destroyWith(t.Context(), stage.volumeID.value(),
		failingRemoval(fmt.Errorf("x: %w", fstree.ErrUndeletable))), ErrVolumeDeleteHeld)
	verdict, err = mgr.PrecheckDestroy(stage.volumeID)
	assert.Equal(t, destroyPrecheckHeld, verdict)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)

	present, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-6")
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(present.hostPath(dataPath), 0o700))
	verdict, err = mgr.PrecheckDestroy(present)
	require.NoError(t, err)
	assert.Equal(t, destroyPrecheckNeedsLock, verdict, "a present volume needs the locked Destroy")
}

// The constructors are the only producers of their types: a cause only through
// holdable, an outcome only through the classifier, a hold record only from a
// held outcome. A literal anywhere else would bypass the allowlist.
func TestDeleteHoldTypesHaveSingleConstructors(t *testing.T) {
	t.Parallel()

	owners := map[string]string{
		"xfsDeleteHoldCause": "holdable",
		"deleteStageOutcome": "classifyXFSDeleteStageCleanup",
		"xfsDeleteHold":      "newXFSDeleteHold",
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	seen := map[string]int{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				ident, ok := literal.Type.(*ast.Ident)
				if !ok {
					return true
				}
				owner, tracked := owners[ident.Name]
				if !tracked {
					return true
				}
				seen[ident.Name]++
				if fn.Name.Name != owner {
					// Its zero value carries no reason, phase or authority.
					if len(literal.Elts) == 0 {
						return true
					}
					t.Errorf("%s: %s literal outside its constructor %s (in %s)",
						fset.Position(literal.Pos()), ident.Name, owner, fn.Name.Name)
				}
				return true
			})
		}
	}
	for name := range owners {
		assert.Positive(t, seen[name], "%s: the constructor guard matched nothing", name)
	}
}

// holdable never returns a nil cause, so a site can never return a nil error
// that reads as success.
func TestHoldableNeverReturnsNil(t *testing.T) {
	t.Parallel()

	cause := holdable(holdReasonDeadline, nil)
	require.NotNil(t, cause)
	require.Error(t, cause)
	assert.Contains(t, cause.Error(), "deadline")
}

// The deep-chain removal of a condemned volume works below the descriptor
// limit too, now through the manager's own Destroy.
func TestXFSDestroyRemovesAChainDeeperThanTheFDLimit(t *testing.T) {
	if os.Getenv(deepWritablePathChildEnv+"_XFS") == "1" {
		destroyDeepChainUnderLowFDLimit(t)
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	output, err := runChildTest(t, executable, "TestXFSDestroyRemovesAChainDeeperThanTheFDLimit",
		deepWritablePathChildEnv+"_XFS=1")
	require.NoError(t, err, "%s", output)
	require.Contains(t, output, "--- PASS: TestXFSDestroyRemovesAChainDeeperThanTheFDLimit (")
}

func destroyDeepChainUnderLowFDLimit(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	volumePath := filepath.Join(dataPath, name)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, writeProjectIDFile(volumePath, xfsDeleteTestProjectID))
	buildDirectoryChain(t, volumePath, writablePathSubdir, 4096)
	installXFSQuotaFixture(t, "")

	var limit unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &limit))
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 128, Max: limit.Max}))
	t.Cleanup(func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &limit) })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, mgr.Destroy(ctx, name))
	assert.NoDirExists(t, volumePath)
	assert.Empty(t, mgr.VolumeDeleteHolds().holds)
}

func runChildTest(t *testing.T, executable, test, env string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^"+test+"$", "-test.v")
	command.Env = append(os.Environ(), env)
	output, err := command.CombinedOutput()
	return string(output), err
}
