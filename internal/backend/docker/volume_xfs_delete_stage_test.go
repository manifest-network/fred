package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

const xfsDeleteTestProjectID = uint32(4242)

func mustXFSDeleteStage(t *testing.T, projectID uint32, volume string) xfsDeleteStageName {
	t.Helper()
	managed, err := parseManagedVolumeName(volume)
	require.NoError(t, err)
	stage, err := newXFSDeleteStageName(projectID, managed)
	require.NoError(t, err)
	return stage
}

func installXFSQuotaFixture(t *testing.T, body string) string {
	t.Helper()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "xfs-quota.log")
	toolPath := filepath.Join(binDir, "xfs_quota")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> \"$FRED_TEST_XFS_LOG\"\n" + body + "\n"
	require.NoError(t, os.WriteFile(toolPath, []byte(script), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_XFS_LOG", logPath)
	return logPath
}

func prepareDeleteStageForTest(
	t *testing.T,
	mgr *xfsVolumeManager,
	stage xfsDeleteStageName,
) {
	t.Helper()
	require.NoError(t, os.Mkdir(stage.hostPath(mgr.dataPath), 0o700))
	mgr.activeIDs[stage.projID] = stage.volumeID.value()
	mgr.volumeToID[stage.volumeID.value()] = stage.projID
	require.NoError(t, mgr.rememberDurableDeleteStage(stage))
}

func TestValidateXFSDefaultProject(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uintptr(28), unsafe.Sizeof(linuxFSXAttr{}),
		"FS_IOC_FSGETXATTR encodes the stable Linux UAPI structure size")

	tests := []struct {
		name    string
		attr    linuxFSXAttr
		wantErr bool
	}{
		{
			name: "default project with inheritance",
			attr: linuxFSXAttr{XFlags: linuxFSXFlagProjInherit},
		},
		{name: "missing inheritance", wantErr: true},
		{name: "non-default project", attr: linuxFSXAttr{ProjectID: 42}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateXFSDefaultProject(tc.attr)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestWaitForZeroProjectQuotaUsageTriggersAndPollsExactID(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	readyPath := filepath.Join(t.TempDir(), "inodegc-ready")
	t.Setenv("FRED_TEST_XFS_READY", readyPath)
	logPath := installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"%s"*) ;;
  *"report -p -b -n -N -L %d -U %d"*)
    if [ -e "$FRED_TEST_XFS_READY" ]; then
      printf '#%d 0 0 0 0\n'
    else
      printf '#%d 7 0 0 0\n'
    fi
    ;;
  *"report -p -i -n -N -L %d -U %d"*)
    if [ -e "$FRED_TEST_XFS_READY" ]; then
      printf '#%d 0 0 0 0\n'
    else
      printf '#%d 1 0 0 0\n'
      printf ready > "$FRED_TEST_XFS_READY"
    fi
    ;;
esac`,
		xfsInodeGCTriggerCmd(),
		xfsDeleteTestProjectID, xfsDeleteTestProjectID,
		xfsDeleteTestProjectID, xfsDeleteTestProjectID,
		xfsDeleteTestProjectID, xfsDeleteTestProjectID,
		xfsDeleteTestProjectID, xfsDeleteTestProjectID,
	))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	blocks, inodes, err := mgr.waitForZeroProjectQuotaUsage(ctx, xfsDeleteTestProjectID)
	require.NoError(t, err)
	assert.Zero(t, blocks)
	assert.Zero(t, inodes)
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	commandText := string(commands)
	assert.Equal(t, 1, strings.Count(commandText, xfsInodeGCTriggerCmd()),
		"cleanup must trigger pending inode inactivation once before polling the exact retiring ID")
	triggerAt := strings.Index(commandText, xfsInodeGCTriggerCmd())
	blockProofAt := strings.Index(commandText, xfsProjectReportCmd("b", xfsDeleteTestProjectID))
	require.GreaterOrEqual(t, triggerAt, 0)
	assert.Greater(t, blockProofAt, triggerAt)
}

func TestReadProjectQuotaUsageRejectsSuccessfulDiagnosticStderr(t *testing.T) {
	mgr := newXfsManagerForTest(t.TempDir())
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"%s"*)
    printf '#%d 0 0 0 0\n'
    printf 'cannot setup path\n' >&2
    ;;
esac`, xfsProjectReportCmd("b", xfsDeleteTestProjectID), xfsDeleteTestProjectID))

	_, err := mgr.readProjectQuotaUsage(t.Context(), xfsDeleteTestProjectID, "b")
	require.ErrorContains(t, err, "diagnostic stderr")
}

func TestReadProjectQuotaUsageReportsNonzeroExitStderrSeparately(t *testing.T) {
	mgr := newXfsManagerForTest(t.TempDir())
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"%s"*)
    printf 'quota device unavailable\n' >&2
    exit 23
    ;;
esac`, xfsProjectReportCmd("b", xfsDeleteTestProjectID)))

	_, err := mgr.readProjectQuotaUsage(t.Context(), xfsDeleteTestProjectID, "b")
	require.ErrorContains(t, err, "stderr: quota device unavailable")
}

func TestParseXFSDeleteStageNameRequiresCanonicalTypedAuthority(t *testing.T) {
	t.Parallel()

	valid := xfsDeleteStagePrefix + "42-" + xfsStageTestVolume
	stage, err := parseXFSDeleteStageName(valid)
	require.NoError(t, err)
	assert.Equal(t, valid, stage.value())
	assert.Equal(t, uint32(42), stage.projID)
	assert.Equal(t, xfsStageTestVolume, stage.volumeID.value())

	for _, value := range []string{
		"fred-xfs-delete-42-" + xfsStageTestVolume,
		xfsDeleteStagePrefix + "0-" + xfsStageTestVolume,
		xfsDeleteStagePrefix + "00-" + xfsStageTestVolume,
		xfsDeleteStagePrefix + "+42-" + xfsStageTestVolume,
		xfsDeleteStagePrefix + "4294967296-" + xfsStageTestVolume,
		xfsDeleteStagePrefix + "42-fred-not-a-uuid-app-0",
		xfsDeleteStagePrefix + "42-../" + xfsStageTestVolume,
	} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, err := parseXFSDeleteStageName(value)
			require.Error(t, err)
		})
	}
}

func TestXFSPrepareDeleteStageRollbackClassifiesDurability(t *testing.T) {
	syncFailure := errors.New("injected first parent sync failure")
	rollbackFailure := errors.New("injected rollback failure")
	for _, tc := range []struct {
		name            string
		removeFails     bool
		secondSyncFails bool
		wantAmbiguous   bool
		wantStage       bool
	}{
		{name: "durable rollback is ordinary"},
		{name: "remove failure is ambiguous", removeFails: true, wantAmbiguous: true, wantStage: true},
		{name: "rollback sync failure is ambiguous", secondSyncFails: true, wantAmbiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			mgr := newXfsManagerForTest(dataPath)
			stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
			installXFSQuotaFixture(t, "")
			root, parent, err := openXFSRootCapabilities(dataPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
			syncCalls := 0
			syncParent := func() error {
				syncCalls++
				if syncCalls == 1 {
					return syncFailure
				}
				if tc.secondSyncFails {
					return rollbackFailure
				}
				return nil
			}
			removeStage := root.Remove
			if tc.removeFails {
				removeStage = func(string) error { return rollbackFailure }
			}

			err = mgr.prepareXFSDeleteStageWith(
				t.Context(), root, parent, stage, syncParent, removeStage,
			)
			require.ErrorIs(t, err, syncFailure)
			assert.Equal(t, tc.wantAmbiguous, errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous))
			if tc.wantStage {
				assert.DirExists(t, stage.hostPath(dataPath))
			} else {
				assert.NoDirExists(t, stage.hostPath(dataPath))
			}
			assert.Empty(t, mgr.durableDeleteStages)
		})
	}
}

func TestXFSPrepareDeleteStageRememberConflictIsAmbiguous(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	conflicting := mustXFSDeleteStage(t, xfsDeleteTestProjectID+1, xfsStageTestVolume)
	mgr.durableDeleteStages = map[string]xfsDeleteStageName{stage.volumeID.value(): conflicting}
	installXFSQuotaFixture(t, "")
	root, parent, err := openXFSRootCapabilities(dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

	err = mgr.prepareXFSDeleteStage(t.Context(), root, parent, stage)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.DirExists(t, stage.hostPath(dataPath), "the parent-durable exact evidence must not be guessed away")
}

func TestXFSPrepareDeleteStageRejectsUntrackedExistingAuthorityAsRecoveryPending(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	installXFSQuotaFixture(t, "")
	root, parent, err := openXFSRootCapabilities(dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

	err = mgr.prepareXFSDeleteStage(t.Context(), root, parent, stage)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.DirExists(t, stage.hostPath(dataPath),
		"an untracked typed name must be consumed only by a fresh strict startup scan")
}

func TestXFSPrepareDeleteStageResetsAndAttestsDefaultProjectBeforePublication(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	logPath := installXFSQuotaFixture(t, "")
	root, parent, err := openXFSRootCapabilities(dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

	require.NoError(t, mgr.prepareXFSDeleteStage(t.Context(), root, parent, stage))
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(commands)), "\n")
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], xfsProjectResetToDefaultCmd(stage.hostPath(dataPath)))
	assert.DirExists(t, stage.hostPath(dataPath))
	assert.Equal(t, stage, mgr.durableDeleteStages[stage.volumeID.value()])
}

func TestXFSPrepareDeleteStageRollsBackFailedDefaultProjectReset(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	installXFSQuotaFixture(t, `case "$*" in
  *"project -s -d 0"*) exit 24 ;;
esac`)
	root, parent, err := openXFSRootCapabilities(dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

	err = mgr.prepareXFSDeleteStage(t.Context(), root, parent, stage)
	require.ErrorContains(t, err, "reset xfs delete-stage")
	assert.NoDirExists(t, stage.hostPath(dataPath), "failed reset must durably roll back the unpublished sibling")
	assert.Empty(t, mgr.durableDeleteStages)
}

func TestXFSPrepareDeleteStageRollsBackFailedProjectAttributeAttestation(t *testing.T) {
	readFailure := errors.New("injected project-attribute read failure")
	for _, tc := range []struct {
		name   string
		reader fixedXFSProjectAttributeReader
		want   string
	}{
		{
			name:   "read failure",
			reader: fixedXFSProjectAttributeReader{err: readFailure},
			want:   readFailure.Error(),
		},
		{
			name: "attribute mismatch",
			reader: fixedXFSProjectAttributeReader{attr: linuxFSXAttr{
				ProjectID: xfsDeleteTestProjectID,
				XFlags:    linuxFSXFlagProjInherit,
				Padding:   [8]byte{},
			}},
			want: "remains assigned to project",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			mgr := newXfsManagerForTest(dataPath)
			mgr.projectAttributes = tc.reader
			stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
			installXFSQuotaFixture(t, "")
			root, parent, err := openXFSRootCapabilities(dataPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

			err = mgr.prepareXFSDeleteStage(t.Context(), root, parent, stage)
			require.ErrorContains(t, err, tc.want)
			assert.NoDirExists(t, stage.hostPath(dataPath),
				"failed kernel attestation must durably roll back the unpublished sibling")
			assert.Empty(t, mgr.durableDeleteStages)
		})
	}
}

func TestXFSDestroyMarkerFirstPartialDeleteRecoversAfterRestart(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	volumePath := stage.volumeID.hostPath(dataPath)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, writeProjectIDFile(volumePath, stage.projID))
	survivor := filepath.Join(volumePath, "tenant-data")
	require.NoError(t, os.WriteFile(survivor, []byte("keep until retry"), 0o600))
	injected := errors.New("injected recursive removal failure")
	installXFSQuotaFixture(t, "")

	err := mgr.destroyWith(t.Context(), stage.volumeID.value(), func(root *os.Root, name string) error {
		if name == projectIDFile {
			return root.RemoveAll(name)
		}
		return injected
	})
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.FileExists(t, survivor)
	assert.NoFileExists(t, filepath.Join(volumePath, projectIDFile),
		"the fault must reproduce marker-first partial recursive deletion")
	assert.DirExists(t, stage.hostPath(dataPath), "typed sibling must survive the partial delete")
	assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()])

	// Simulate a process crash: all volatile maps disappear. Validate's scan and
	// the exact inventory proof used by existing-identity construction must both
	// accept the marker-missing final only because its typed sibling is present.
	restarted := newXfsManagerForTest(dataPath)
	require.NoError(t, restarted.loadProjectIDs())
	assert.Equal(t, stage.projID, restarted.volumeToID[stage.volumeID.value()])
	_, recovered := restarted.recoveredDeleteStages[stage.volumeID.value()]
	require.True(t, recovered)
	_, err = attestManagedVolumeInventory(t.Context(), restarted)
	require.NoError(t, err, "existing-identity construction must reach explicit startup recovery")
	require.ErrorContains(t, restarted.RequireNoInterruptedVolumeMutations(t.Context()), stage.value(),
		"new/adopt/preflight must still reject cleanup-owned evidence")

	logPath := installXFSQuotaFixture(t, "")
	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(t.Context()))
	assert.NoDirExists(t, volumePath)
	assert.NoDirExists(t, stage.hostPath(dataPath))
	assert.Empty(t, restarted.volumeToID)
	assert.Empty(t, restarted.activeIDs)
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Contains(t, string(commands), "report -p -b -n -N")
	assert.Contains(t, string(commands), "report -p -i -n -N")
	assert.Contains(t, string(commands), xfsLimitClearCmd(stage.projID))
}

func TestXFSDeleteInodeGCTriggerFailureRetainsTombstone(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	logPath := installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"%s"*) exit 23 ;;
esac`, xfsInodeGCTriggerCmd()))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := mgr.cleanupXFSDeleteStage(ctx, stage)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorContains(t, err, fmt.Sprintf("before project %d usage proof", stage.projID))
	assert.DirExists(t, stage.hostPath(dataPath),
		"a failed inode-GC trigger must retain the typed cleanup tombstone")
	assert.Equal(t, stage, mgr.durableDeleteStages[stage.volumeID.value()])
	assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()])
	assert.Equal(t, stage.volumeID.value(), mgr.activeIDs[stage.projID])

	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	commandText := string(commands)
	assert.Equal(t, 1, strings.Count(commandText, xfsInodeGCTriggerCmd()))
	assert.NotContains(t, commandText, "report -p",
		"an unproven trigger must stop before exact retiring-ID reads")
	assert.NotContains(t, commandText, xfsLimitClearCmd(stage.projID),
		"an unproven trigger must stop before clearing quota authority")
}

func TestXFSDeleteUsageProofFailureRetainsTombstone(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "nonzero blocks",
			body: fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 7 0 0 0\\n' ;;
  *"report -p -i -n -N"*) printf '#%d 0 0 0 0\\n' ;;
esac`, xfsDeleteTestProjectID, xfsDeleteTestProjectID),
			want: "still uses 7 blocks and 0 inodes",
		},
		{
			name: "open zero-length inode",
			body: fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 0 0 0 0\\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\\n' ;;
esac`, xfsDeleteTestProjectID, xfsDeleteTestProjectID),
			want: "still uses 0 blocks and 1 inodes",
		},
		{
			name: "malformed report",
			body: fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d not-a-number 0 0 0\\n' ;;
esac`, xfsDeleteTestProjectID),
			want: "parse project 4242 used value",
		},
		{
			name: "named project despite numeric flag",
			body: `case "$*" in
  *"report -p -b -n -N"*) printf 'tenant-project 0 0 0 0\n' ;;
esac`,
			want: "nonnumeric project ID",
		},
		{
			name: "report command fails",
			body: `case "$*" in
  *"report -p -b -n -N"*) exit 23 ;;
esac`,
			want: "xfs_quota report -p -b -n -N",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			mgr := newXfsManagerForTest(dataPath)
			stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
			prepareDeleteStageForTest(t, mgr, stage)
			installXFSQuotaFixture(t, tc.body)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			err := mgr.cleanupXFSDeleteStage(ctx, stage)
			require.ErrorContains(t, err, tc.want)
			require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
			assert.DirExists(t, stage.hostPath(dataPath))
			assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()])
			assert.Equal(t, stage.volumeID.value(), mgr.activeIDs[stage.projID])
		})
	}
}

func TestXFSDestroyAbsentMappedVolumePersistsAuthorityBeforeUsageProof(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	// Models a live process whose managed namespace was removed while an open,
	// unlinked project inode remains. The collision-checked map is still exact
	// authority, but the inner marker is necessarily gone with the directory.
	mgr.activeIDs[stage.projID] = stage.volumeID.value()
	mgr.volumeToID[stage.volumeID.value()] = stage.projID
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 9 0 0 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, stage.projID, stage.projID))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := mgr.Destroy(ctx, stage.volumeID.value())
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorContains(t, err, "still uses 9 blocks and 1 inodes")
	assert.DirExists(t, stage.hostPath(dataPath),
		"usage must be proven under durable typed authority even when the final name was already absent")
	assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()])
	assert.Equal(t, stage.volumeID.value(), mgr.activeIDs[stage.projID])
}

func TestXFSDeleteCrashAfterQuotaClearBeforeTombstoneRemovalRecovers(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	logPath := installXFSQuotaFixture(t, "")
	injected := errors.New("injected tombstone unlink failure after clear")

	err := mgr.cleanupXFSDeleteStageWith(
		t.Context(), stage, removeAllFromXFSRoot, removeFromXFSRoot,
		func(*os.Root, string) error { return injected },
	)
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.DirExists(t, stage.hostPath(dataPath), "post-clear failure must retain restart authority")
	assert.Equal(t, stage.projID, mgr.volumeToID[stage.volumeID.value()])
	beforeRestart, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Contains(t, string(beforeRestart), xfsLimitClearCmd(stage.projID),
		"the injected failure must occur after dquot clear")

	restarted := newXfsManagerForTest(dataPath)
	require.NoError(t, restarted.loadProjectIDs())
	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(t.Context()),
		"repeating the zero proof and dquot clear is idempotent")
	assert.NoDirExists(t, stage.hostPath(dataPath))
	assert.Empty(t, restarted.volumeToID)
	afterRestart, err := os.ReadFile(logPath)
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(string(afterRestart), xfsLimitClearCmd(stage.projID)))
}

func TestXFSRecoveredPreResetDeleteStageIsNormalizedBeforeCleanup(t *testing.T) {
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	// Models power loss after mkdir reached the journal but before the first
	// project-0 reset/fsync. The startup scanner is read-only and mints only a
	// cleanup capability; recovery must reapply normalization before progressing.
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	restarted := newXfsManagerForTest(dataPath)
	require.NoError(t, restarted.loadProjectIDs())
	logPath := installXFSQuotaFixture(t, "")

	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(t.Context()))
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	logText := string(commands)
	resetAt := strings.Index(logText, xfsProjectResetToDefaultCmd(stage.hostPath(dataPath)))
	blockProofAt := strings.Index(logText, "report -p -b -n -N")
	require.GreaterOrEqual(t, resetAt, 0)
	assert.Greater(t, blockProofAt, resetAt, "usage proof must follow recovered-stage normalization")
	assert.NoDirExists(t, stage.hostPath(dataPath))
}

func TestXFSDeleteRecoveryHonorsEarlierParentDeadline(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	logPath := installXFSQuotaFixture(t, "")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := mgr.cleanupXFSDeleteStage(ctx, stage)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.DirExists(t, stage.hostPath(dataPath), "expired startup budget must retain exact recovery evidence")
	_, statErr := os.Stat(logPath)
	assert.ErrorIs(t, statErr, os.ErrNotExist, "an expired parent budget must not start a fresh quota subprocess")
}

func TestXFSDeleteRecoveryDeadlineStopsBetweenEntriesBeforeQuotaClear(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	prepareDeleteStageForTest(t, mgr, stage)
	volumePath := stage.volumeID.hostPath(dataPath)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, writeProjectIDFile(volumePath, stage.projID))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, "a"), []byte("a"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, "b"), []byte("b"), 0o600))
	logPath := installXFSQuotaFixture(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	removeCalls := 0

	err := mgr.cleanupXFSDeleteStageWith(
		ctx,
		stage,
		func(*os.Root, string) error {
			removeCalls++
			<-ctx.Done()
			return nil
		},
		removeFromXFSRoot,
		removeFromXFSRoot,
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.Equal(t, 1, removeCalls, "expired aggregate budget must stop before the next recursive entry syscall")
	assert.DirExists(t, stage.hostPath(dataPath))
	assert.DirExists(t, volumePath)
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.NotContains(t, string(commands), "report -p -b", "deadline expiry must stop before quota proof")
	assert.NotContains(t, string(commands), xfsLimitClearCmd(stage.projID))
}

func TestXFSLoadProjectIDsRejectsNonemptyDeleteAuthorityAtomically(t *testing.T) {
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(stage.hostPath(dataPath), "foreign"), []byte("keep"), 0o600))
	mgr := newXfsManagerForTest(dataPath)
	mgr.activeIDs[9001] = "preexisting"
	mgr.volumeToID["preexisting"] = 9001

	err := mgr.loadProjectIDs()
	require.ErrorContains(t, err, "is not empty")
	assert.Equal(t, map[uint32]string{9001: "preexisting"}, mgr.activeIDs)
	assert.Equal(t, map[string]uint32{"preexisting": 9001}, mgr.volumeToID)
	assert.Empty(t, mgr.recoveredDeleteStages)
}

func TestXFSRenameRejectsPendingMutationForEitherName(t *testing.T) {
	const (
		oldName = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		newName = "fred-retained-550e8400-e29b-41d4-a716-446655440000-app-0"
	)

	for _, tc := range []struct {
		name        string
		forOld      bool
		deleteStage bool
	}{
		{name: "old create", forOld: true},
		{name: "new create"},
		{name: "old delete", forOld: true, deleteStage: true},
		{name: "new delete", deleteStage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataPath := t.TempDir()
			mgr := newXfsManagerForTest(dataPath)
			mgr.durableStages = make(map[string]xfsStageName)
			mgr.durableDeleteStages = make(map[string]xfsDeleteStageName)
			oldVolume, err := parseManagedVolumeName(oldName)
			require.NoError(t, err)
			newVolume, err := parseManagedVolumeName(newName)
			require.NoError(t, err)
			require.NoError(t, os.Mkdir(oldVolume.hostPath(dataPath), 0o700))
			require.NoError(t, writeProjectIDFile(oldVolume.hostPath(dataPath), 42))
			mgr.activeIDs[42] = oldVolume.value()
			mgr.volumeToID[oldVolume.value()] = 42

			target := newVolume
			if tc.forOld {
				target = oldVolume
			}
			const pendingID = uint32(43)
			if tc.deleteStage {
				stage, stageErr := newXFSDeleteStageName(pendingID, target)
				require.NoError(t, stageErr)
				mgr.durableDeleteStages[target.value()] = stage
			} else {
				stage, stageErr := newXFSStageName(pendingID, target)
				require.NoError(t, stageErr)
				mgr.durableStages[target.value()] = stage
			}

			err = mgr.RenameVolume(t.Context(), oldName, newName)
			require.ErrorContains(t, err, "volume mutation")
			assert.DirExists(t, oldVolume.hostPath(dataPath))
			assert.NoDirExists(t, newVolume.hostPath(dataPath))
		})
	}
}
