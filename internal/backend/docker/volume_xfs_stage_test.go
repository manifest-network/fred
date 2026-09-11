package docker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

const xfsStageTestVolume = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"

func mustXFSStage(t *testing.T, projectID uint32, volume string) xfsStageName {
	t.Helper()
	managed, err := parseManagedVolumeName(volume)
	require.NoError(t, err)
	stage, err := newXFSStageName(projectID, managed)
	require.NoError(t, err)
	return stage
}

func makeXFSStage(t *testing.T, rootPath string, stage xfsStageName, marker bool) {
	t.Helper()
	stagePath := stage.hostPath(rootPath)
	require.NoError(t, os.Mkdir(stagePath, 0o700))
	if marker {
		require.NoError(t, writeProjectIDFile(stagePath, stage.projID))
	}
}

func TestParseXFSStageNameRequiresCanonicalTypedAuthority(t *testing.T) {
	t.Parallel()

	valid := xfsStagePrefix + "42-" + xfsStageTestVolume
	stage, err := parseXFSStageName(valid)
	require.NoError(t, err)
	assert.Equal(t, valid, stage.value())
	assert.Equal(t, uint32(42), stage.projID)
	assert.Equal(t, xfsStageTestVolume, stage.volumeID.value())

	for _, value := range []string{
		"fred-xfs-stage-42-" + xfsStageTestVolume,
		xfsStagePrefix + "0-" + xfsStageTestVolume,
		xfsStagePrefix + "00-" + xfsStageTestVolume,
		xfsStagePrefix + "+42-" + xfsStageTestVolume,
		xfsStagePrefix + "4294967296-" + xfsStageTestVolume,
		xfsStagePrefix + "42-fred-550E8400-e29b-41d4-a716-446655440000-app-0",
		xfsStagePrefix + "42-../" + xfsStageTestVolume,
		xfsStagePrefix + "42-fred-not-a-uuid-app-0",
	} {
		value := value
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, err := parseXFSStageName(value)
			require.Error(t, err)
		})
	}
}

func TestInspectXFSStageAcceptsOnlyCrashValidShapes(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, false)
		root, err := os.OpenRoot(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close() })

		marker, err := inspectXFSStage(root, stage)
		require.NoError(t, err)
		assert.False(t, marker)
	})

	t.Run("matching marker", func(t *testing.T) {
		t.Parallel()
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		root, err := os.OpenRoot(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close() })

		marker, err := inspectXFSStage(root, stage)
		require.NoError(t, err)
		assert.True(t, marker)
	})

	for _, tc := range []struct {
		name  string
		build func(*testing.T, string, xfsStageName)
		want  string
	}{
		{
			name: "mismatched marker",
			build: func(t *testing.T, rootPath string, stage xfsStageName) {
				makeXFSStage(t, rootPath, stage, false)
				require.NoError(t, writeProjectIDFile(stage.hostPath(rootPath), stage.projID+1))
			},
			want: "marker names",
		},
		{
			name: "tenant data",
			build: func(t *testing.T, rootPath string, stage xfsStageName) {
				makeXFSStage(t, rootPath, stage, true)
				require.NoError(t, os.WriteFile(filepath.Join(stage.hostPath(rootPath), "tenant-data"), []byte("keep"), 0o600))
			},
			want: "outside its project marker",
		},
		{
			name: "regular file",
			build: func(t *testing.T, rootPath string, stage xfsStageName) {
				require.NoError(t, os.WriteFile(stage.hostPath(rootPath), []byte("foreign"), 0o600))
			},
			want: "not a real directory",
		},
		{
			name: "symlink",
			build: func(t *testing.T, rootPath string, stage xfsStageName) {
				target := filepath.Join(rootPath, "foreign")
				require.NoError(t, os.Mkdir(target, 0o700))
				require.NoError(t, os.Symlink(target, stage.hostPath(rootPath)))
			},
			want: "not a real directory",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rootPath := t.TempDir()
			stage := mustXFSStage(t, 42, xfsStageTestVolume)
			tc.build(t, rootPath, stage)
			root, err := os.OpenRoot(rootPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = root.Close() })

			_, err = inspectXFSStage(root, stage)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestInspectXFSStageForCleanupAcceptsBoundedTornMarkers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		markerData *string
	}{
		{name: "mkdir only"},
		{name: "marker created before write", markerData: new("")},
		{name: "prefix write", markerData: new("4")},
		{name: "complete write", markerData: new("42")},
		{name: "different digits after torn write", markerData: new("43")},
		{name: "zero-filled torn write", markerData: new("\x00\x00")},
		{name: "bounded arbitrary torn write", markerData: new("garbage")},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rootPath := t.TempDir()
			stage := mustXFSStage(t, 42, xfsStageTestVolume)
			makeXFSStage(t, rootPath, stage, false)
			if tc.markerData != nil {
				require.NoError(t, os.WriteFile(
					filepath.Join(stage.hostPath(rootPath), projectIDFile),
					[]byte(*tc.markerData),
					0o600,
				))
			}
			root, err := os.OpenRoot(rootPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = root.Close() })

			proof, err := inspectXFSStageForCleanup(root, stage)
			require.NoError(t, err)
			assert.Equal(t, tc.markerData != nil, proof.markerPresent)
			if tc.markerData != nil {
				assert.Equal(t, *tc.markerData, proof.markerData)
			}
		})
	}

	t.Run("reject content larger than writer", func(t *testing.T) {
		t.Parallel()
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, false)
		require.NoError(t, os.WriteFile(
			filepath.Join(stage.hostPath(rootPath), projectIDFile),
			[]byte(strings.Repeat("x", 11)),
			0o600,
		))
		root, err := os.OpenRoot(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close() })

		_, err = inspectXFSStageForCleanup(root, stage)
		require.ErrorContains(t, err, "emits at most 10")
	})
}

func TestXFSLoadProjectIDsRebuildsInterruptedCreateAuthorityAtomically(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	stage := mustXFSStage(t, 42, xfsStageTestVolume)
	makeXFSStage(t, rootPath, stage, true)
	mgr := newXfsManagerForTest(rootPath)

	require.NoError(t, mgr.loadProjectIDs())
	assert.Equal(t, map[uint32]string{42: xfsStageTestVolume}, mgr.activeIDs)
	assert.Equal(t, map[string]uint32{xfsStageTestVolume: 42}, mgr.volumeToID)
	recovered, ok := mgr.recoveredStage(stage.volumeID)
	require.True(t, ok)
	assert.Equal(t, stage, recovered.stage)
	require.ErrorContains(t, mgr.RequireNoInterruptedVolumeMutations(t.Context()), stage.value())
}

func TestXFSLoadProjectIDsRejectsMalformedAndConflictingStagesWithoutPartialCommit(t *testing.T) {
	t.Parallel()

	const otherVolume = "fred-650e8400-e29b-41d4-a716-446655440000-app-0"
	for _, tc := range []struct {
		name  string
		build func(*testing.T, string)
		want  string
	}{
		{
			name: "malformed reserved name",
			build: func(t *testing.T, rootPath string) {
				require.NoError(t, os.Mkdir(filepath.Join(rootPath, xfsStagePrefix+"00-"+xfsStageTestVolume), 0o700))
			},
			want: "malformed entry",
		},
		{
			name: "two stages target one volume",
			build: func(t *testing.T, rootPath string) {
				makeXFSStage(t, rootPath, mustXFSStage(t, 42, xfsStageTestVolume), true)
				makeXFSStage(t, rootPath, mustXFSStage(t, 43, xfsStageTestVolume), true)
			},
			want: "multiple xfs stages target volume",
		},
		{
			name: "project ID collision",
			build: func(t *testing.T, rootPath string) {
				makeXFSStage(t, rootPath, mustXFSStage(t, 42, xfsStageTestVolume), true)
				makeXFSStage(t, rootPath, mustXFSStage(t, 42, otherVolume), true)
			},
			want: "duplicate project ID",
		},
		{
			name: "stage and final coexist",
			build: func(t *testing.T, rootPath string) {
				makeXFSStage(t, rootPath, mustXFSStage(t, 42, xfsStageTestVolume), true)
				finalPath := filepath.Join(rootPath, xfsStageTestVolume)
				require.NoError(t, os.Mkdir(finalPath, 0o700))
				require.NoError(t, writeProjectIDFile(finalPath, 42))
			},
			want: "coexist",
		},
		{
			name: "stage marker exceeds writer bound",
			build: func(t *testing.T, rootPath string) {
				stage := mustXFSStage(t, 42, xfsStageTestVolume)
				makeXFSStage(t, rootPath, stage, false)
				require.NoError(t, os.WriteFile(
					filepath.Join(stage.hostPath(rootPath), projectIDFile),
					[]byte(strings.Repeat("x", 11)),
					0o600,
				))
			},
			want: "emits at most 10",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rootPath := t.TempDir()
			tc.build(t, rootPath)
			mgr := newXfsManagerForTest(rootPath)
			mgr.activeIDs[9001] = "preexisting"
			mgr.volumeToID["preexisting"] = 9001

			err := mgr.loadProjectIDs()
			require.ErrorContains(t, err, tc.want)
			assert.Equal(t, map[uint32]string{9001: "preexisting"}, mgr.activeIDs)
			assert.Equal(t, map[string]uint32{"preexisting": 9001}, mgr.volumeToID)
			assert.Empty(t, mgr.recoveredStages)
		})
	}
}

func TestXFSRecoverInterruptedVolumeMutationsCleansOnlyExactStageAndNeverPublishes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		markerData *string
	}{
		{name: "empty directory"},
		{name: "empty torn marker", markerData: new("")},
		{name: "partial torn marker", markerData: new("4")},
		{name: "zero-filled torn marker", markerData: new("\x00\x00")},
		{name: "complete marker", markerData: new("42")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rootPath := t.TempDir()
			stage := mustXFSStage(t, 42, xfsStageTestVolume)
			makeXFSStage(t, rootPath, stage, false)
			if tc.markerData != nil {
				require.NoError(t, os.WriteFile(
					filepath.Join(stage.hostPath(rootPath), projectIDFile),
					[]byte(*tc.markerData),
					0o600,
				))
			}
			mgr := newXfsManagerForTest(rootPath)
			require.NoError(t, mgr.loadProjectIDs())
			logPath := installLoggingXFSQuota(t)

			require.NoError(t, mgr.RecoverInterruptedVolumeMutations(t.Context()))
			assert.NoDirExists(t, stage.hostPath(rootPath))
			assert.NoDirExists(t, filepath.Join(rootPath, xfsStageTestVolume), "startup recovery must never publish an unknown quota")
			assert.Empty(t, mgr.activeIDs)
			assert.Empty(t, mgr.volumeToID)
			assert.Empty(t, mgr.recoveredStages)
			require.NoError(t, mgr.RequireNoInterruptedVolumeMutations(t.Context()))
			commands, err := os.ReadFile(logPath)
			require.NoError(t, err)
			assert.Contains(t, string(commands), xfsLimitClearCmd(stage.projID))
		})
	}
}

func TestXFSPublishStageIsNoReplaceAndRereadsAmbiguousResult(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
		before, err := root.Lstat(stage.value())
		require.NoError(t, err)

		require.NoError(t, newXfsManagerForTest(rootPath).publishXFSStage(root, parent, stage))
		assert.NoDirExists(t, stage.hostPath(rootPath))
		after, err := root.Lstat(stage.volumeID.value())
		require.NoError(t, err)
		assert.True(t, os.SameFile(before, after))
	})

	t.Run("destination conflict", func(t *testing.T) {
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		finalPath := filepath.Join(rootPath, xfsStageTestVolume)
		require.NoError(t, os.Mkdir(finalPath, 0o700))
		sentinel := filepath.Join(finalPath, "foreign")
		require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })

		err = newXfsManagerForTest(rootPath).publishXFSStage(root, parent, stage)
		require.ErrorContains(t, err, "ambiguous xfs stage publication")
		assert.DirExists(t, stage.hostPath(rootPath))
		assert.FileExists(t, sentinel)
	})

	t.Run("rename returned error after committing", func(t *testing.T) {
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
		injected := errors.New("ambiguous syscall result")

		err = newXfsManagerForTest(rootPath).publishXFSStageWith(root, parent, stage, func(oldName, newName string) error {
			require.NoError(t, os.Rename(filepath.Join(rootPath, oldName), filepath.Join(rootPath, newName)))
			return injected
		})
		require.NoError(t, err, "the inode reread proves that the publication committed")
		assert.NoDirExists(t, stage.hostPath(rootPath))
		assert.DirExists(t, filepath.Join(rootPath, xfsStageTestVolume))
	})

	t.Run("rename returned error without committing", func(t *testing.T) {
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
		injected := errors.New("definite refusal")

		err = newXfsManagerForTest(rootPath).publishXFSStageWith(root, parent, stage, func(string, string) error {
			return injected
		})
		require.ErrorIs(t, err, injected)
		assert.DirExists(t, stage.hostPath(rootPath))
		assert.NoDirExists(t, filepath.Join(rootPath, xfsStageTestVolume))
	})

	t.Run("publication survives parent sync error for exact retry", func(t *testing.T) {
		rootPath := t.TempDir()
		stage := mustXFSStage(t, 42, xfsStageTestVolume)
		makeXFSStage(t, rootPath, stage, true)
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
		require.NoError(t, parent.Close(), "force only the post-publication directory sync to fail")

		err = newXfsManagerForTest(rootPath).publishXFSStageWith(root, parent, stage, func(oldName, newName string) error {
			return os.Rename(filepath.Join(rootPath, oldName), filepath.Join(rootPath, newName))
		})
		require.ErrorContains(t, err, "parent sync failed")
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		assert.NoDirExists(t, stage.hostPath(rootPath))
		assert.DirExists(t, filepath.Join(rootPath, xfsStageTestVolume),
			"a sync error is an ambiguous acknowledgement, not proof that rename rolled back")
	})
}

func TestCommitXFSVolumeRenameClassifiesPostSyscallOutcomes(t *testing.T) {
	const (
		oldName = "fred-550e8400-e29b-41d4-a716-446655440000-0"
		newName = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		projID  = uint32(42)
	)

	setup := func(t *testing.T) (
		string,
		*os.Root,
		*fsidentity.Directory,
		managedVolumeName,
		managedVolumeName,
		os.FileInfo,
	) {
		t.Helper()
		rootPath := t.TempDir()
		oldVolume, err := parseManagedVolumeName(oldName)
		require.NoError(t, err)
		newVolume, err := parseManagedVolumeName(newName)
		require.NoError(t, err)
		require.NoError(t, os.Mkdir(oldVolume.hostPath(rootPath), 0o700))
		require.NoError(t, writeProjectIDFile(oldVolume.hostPath(rootPath), projID))
		root, parent, err := openXFSRootCapabilities(rootPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.Close(); _ = parent.Close() })
		sourceInfo, err := root.Lstat(oldVolume.value())
		require.NoError(t, err)
		return rootPath, root, parent, oldVolume, newVolume, sourceInfo
	}

	t.Run("error acknowledgement after committed rename converges", func(t *testing.T) {
		rootPath, root, parent, oldVolume, newVolume, sourceInfo := setup(t)
		injected := errors.New("simulated syscall acknowledgement error")

		err := commitXFSVolumeRename(
			root, oldVolume, newVolume, projID, sourceInfo,
			func(oldName, newName string) error {
				require.NoError(t, parent.RenameNoReplace(oldName, newName))
				return injected
			},
			parent.Sync,
		)
		require.NoError(t, err, "the exact inode reread plus parent sync proves the rename durable")
		assert.NoDirExists(t, oldVolume.hostPath(rootPath))
		assert.DirExists(t, newVolume.hostPath(rootPath))
	})

	t.Run("definitive refusal remains non-ambiguous", func(t *testing.T) {
		rootPath, root, _, oldVolume, newVolume, sourceInfo := setup(t)
		injected := errors.New("simulated definite refusal")

		err := commitXFSVolumeRename(
			root, oldVolume, newVolume, projID, sourceInfo,
			func(string, string) error { return injected },
			func() error {
				t.Fatal("an uncommitted rename must not sync the parent")
				return nil
			},
		)
		require.ErrorIs(t, err, injected)
		assert.NotErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		assert.DirExists(t, oldVolume.hostPath(rootPath))
		assert.NoDirExists(t, newVolume.hostPath(rootPath))
	})

	t.Run("post-rename reread failure is typed ambiguous", func(t *testing.T) {
		_, root, parent, oldVolume, newVolume, sourceInfo := setup(t)

		err := commitXFSVolumeRename(
			root, oldVolume, newVolume, projID, sourceInfo,
			func(oldName, newName string) error {
				require.NoError(t, parent.RenameNoReplace(oldName, newName))
				require.NoError(t, root.Close())
				return nil
			},
			func() error {
				t.Fatal("an unverified rename must not sync the parent")
				return nil
			},
		)
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		require.ErrorContains(t, err, "re-read old xfs volume")
	})

	t.Run("post-rename marker failure is typed ambiguous", func(t *testing.T) {
		rootPath, root, parent, oldVolume, newVolume, sourceInfo := setup(t)

		err := commitXFSVolumeRename(
			root, oldVolume, newVolume, projID, sourceInfo,
			func(oldName, newName string) error {
				require.NoError(t, parent.RenameNoReplace(oldName, newName))
				require.NoError(t, os.Remove(filepath.Join(rootPath, newName, projectIDFile)))
				return nil
			},
			func() error {
				t.Fatal("an unattested rename must not sync the parent")
				return nil
			},
		)
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		require.ErrorContains(t, err, "attest renamed xfs volume")
	})

	t.Run("post-rename parent sync failure is typed ambiguous", func(t *testing.T) {
		_, root, parent, oldVolume, newVolume, sourceInfo := setup(t)
		injected := errors.New("simulated parent sync failure")

		err := commitXFSVolumeRename(
			root, oldVolume, newVolume, projID, sourceInfo,
			parent.RenameNoReplace,
			func() error { return injected },
		)
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		require.ErrorIs(t, err, injected)
	})
}

func TestXFSCreateStagesQuotaBeforeAtomicPublication(t *testing.T) {
	rootPath := t.TempDir()
	mgr := newXfsManagerForTest(rootPath)
	logPath := installLoggingXFSQuota(t)

	hostPath, created, err := mgr.Create(t.Context(), xfsStageTestVolume, 100)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, filepath.Join(rootPath, xfsStageTestVolume), hostPath)
	assert.DirExists(t, hostPath)
	entries, err := os.ReadDir(rootPath)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, strings.HasPrefix(entry.Name(), xfsStagePrefix), "successful create must consume its stage")
	}
	marker, err := readProjectIDFile(hostPath)
	require.NoError(t, err)
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	logText := string(commands)
	stage := mustXFSStage(t, marker, xfsStageTestVolume)
	setupAt := strings.Index(logText, xfsProjectSetupCmd(stage.hostPath(rootPath), marker))
	limitAt := strings.Index(logText, xfsLimitCmd(marker, "100m", inodeHardLimit(100, 0)))
	require.GreaterOrEqual(t, setupAt, 0)
	assert.Greater(t, limitAt, setupAt, "the dquot limit must be established before publication")
}

func TestXFSCreateStageRollbackClassifiesUncertainAbsence(t *testing.T) {
	removeFailure := errors.New("injected rollback remove failure")
	syncFailure := errors.New("injected rollback parent sync failure")
	for _, tc := range []struct {
		name          string
		remove        func(*os.Root, string) error
		sync          func() error
		wantPending   bool
		wantStageLive bool
		wantCause     error
	}{
		{
			name: "remove failure leaves recovery evidence",
			remove: func(*os.Root, string) error {
				return removeFailure
			},
			sync:          func() error { return nil },
			wantPending:   true,
			wantStageLive: true,
			wantCause:     removeFailure,
		},
		{
			name: "parent sync failure leaves crash outcome unknown",
			remove: func(root *os.Root, name string) error {
				return root.Remove(name)
			},
			sync:          func() error { return syncFailure },
			wantStageLive: false,
			wantCause:     syncFailure,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rootPath := t.TempDir()
			mgr := newXfsManagerForTest(rootPath)
			volumeID, err := parseManagedVolumeName(xfsStageTestVolume)
			require.NoError(t, err)
			projID, reservation, err := mgr.reserveProjectID(volumeID.value())
			require.NoError(t, err)
			require.NotNil(t, reservation)
			stage, err := newXFSStageName(projID, volumeID)
			require.NoError(t, err)
			root, err := os.OpenRoot(rootPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = root.Close() })
			require.NoError(t, root.Mkdir(stage.value(), 0o700))
			created := &createdXFSStageDirectory{stage: stage}

			err = mgr.rollbackUndurableStageWith(
				created,
				reservation,
				"parent_sync",
				func(name string) error { return tc.remove(root, name) },
				tc.sync,
			)
			require.ErrorIs(t, err, tc.wantCause)
			require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
			assert.Equal(t, tc.wantPending, errors.Is(err, ErrVolumeMutationRecoveryPending))
			if tc.wantStageLive {
				assert.DirExists(t, stage.hostPath(rootPath))
			} else {
				assert.NoDirExists(t, stage.hostPath(rootPath))
			}
			assert.Equal(t, projID, mgr.volumeToID[volumeID.value()],
				"uncertain rollback must retain the reverse project-ID reservation")
			assert.Equal(t, volumeID.value(), mgr.activeIDs[projID],
				"uncertain rollback must retain the forward project-ID reservation")
		})
	}
}

func TestXFSCreateCompensatesDurableStageAfterQuotaFailure(t *testing.T) {
	for _, failCommand := range []string{"project -s", "limit -p bhard=100m"} {
		t.Run(failCommand, func(t *testing.T) {
			rootPath := t.TempDir()
			mgr := newXfsManagerForTest(rootPath)
			binDir := t.TempDir()
			logPath := filepath.Join(t.TempDir(), "xfs-quota.log")
			fakeQuota := filepath.Join(binDir, "xfs_quota")
			require.NoError(t, os.WriteFile(fakeQuota, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_XFS_LOG"
case "$*" in
  *"$FRED_TEST_XFS_FAIL"*) exit 19 ;;
esac
`), 0o700))
			t.Setenv("PATH", binDir)
			t.Setenv("FRED_TEST_XFS_LOG", logPath)
			t.Setenv("FRED_TEST_XFS_FAIL", failCommand)

			_, created, err := mgr.Create(t.Context(), xfsStageTestVolume, 100)
			require.Error(t, err)
			assert.False(t, created)
			assert.NoDirExists(t, filepath.Join(rootPath, xfsStageTestVolume))
			entries, readErr := os.ReadDir(rootPath)
			require.NoError(t, readErr)
			for _, entry := range entries {
				assert.False(t, strings.HasPrefix(entry.Name(), xfsStagePrefix), "compensation must consume the private stage")
			}
			assert.Empty(t, mgr.activeIDs)
			assert.Empty(t, mgr.volumeToID)
			assert.Empty(t, mgr.durableStages)
			commands, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			assert.Contains(t, string(commands), "bhard=0 bsoft=0 ihard=0 isoft=0")
		})
	}
}

func TestXFSCreateFailedDurableStageCompensationRequiresRecovery(t *testing.T) {
	rootPath := t.TempDir()
	mgr := newXfsManagerForTest(rootPath)
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "xfs-quota.log")
	fakeQuota := filepath.Join(binDir, "xfs_quota")
	require.NoError(t, os.WriteFile(fakeQuota, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_XFS_LOG"
case "$*" in
  *"project -s -p"*) exit 19 ;;
  *"bhard=0 bsoft=0 ihard=0 isoft=0"*) exit 23 ;;
esac
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_XFS_LOG", logPath)

	_, created, err := mgr.Create(t.Context(), xfsStageTestVolume, 100)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.False(t, created)
	assert.NoDirExists(t, filepath.Join(rootPath, xfsStageTestVolume))
	require.Len(t, mgr.durableStages, 1)
	stage := mgr.durableStages[xfsStageTestVolume]
	assert.DirExists(t, stage.hostPath(rootPath),
		"failed compensation must retain the parent-durable typed recovery capability")
	assert.Equal(t, stage.projID, mgr.volumeToID[xfsStageTestVolume])
	assert.Equal(t, xfsStageTestVolume, mgr.activeIDs[stage.projID])
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(commands), "project -s -p")
	assert.Contains(t, string(commands), xfsLimitClearCmd(stage.projID))
}

func TestXFSCreateStageRememberConflictRequiresRecovery(t *testing.T) {
	rootPath := t.TempDir()
	mgr := newXfsManagerForTest(rootPath)
	volumeID, err := parseManagedVolumeName(xfsStageTestVolume)
	require.NoError(t, err)
	projID, err := mgr.assignProjectID(volumeID.value())
	require.NoError(t, err)
	stage, err := newXFSStageName(projID, volumeID)
	require.NoError(t, err)
	conflictingID := projID + 1
	if conflictingID == 0 {
		conflictingID = 1
	}
	conflicting, err := newXFSStageName(conflictingID, volumeID)
	require.NoError(t, err)
	mgr.durableStages = make(map[string]xfsStageName)
	mgr.durableStages[volumeID.value()] = conflicting

	_, created, err := mgr.Create(t.Context(), volumeID.value(), 100)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.False(t, created)
	assert.DirExists(t, stage.hostPath(rootPath),
		"the parent-durable on-disk stage must survive a volatile registration conflict")
	assert.Equal(t, projID, mgr.volumeToID[volumeID.value()])
	assert.Equal(t, volumeID.value(), mgr.activeIDs[projID])
}

func TestXFSCreateRefusesUntrackedStageAndReleasesFreshReservation(t *testing.T) {
	rootPath := t.TempDir()
	mgr := newXfsManagerForTest(rootPath)
	volumeID, err := parseManagedVolumeName(xfsStageTestVolume)
	require.NoError(t, err)
	projectID, reservation, err := mgr.reserveProjectID(xfsStageTestVolume)
	require.NoError(t, err)
	require.NotNil(t, reservation)
	stage, err := newXFSStageName(projectID, volumeID)
	require.NoError(t, err)
	mgr.releaseCreatedProjectID(reservation)
	makeXFSStage(t, rootPath, stage, false)
	logPath := installLoggingXFSQuota(t)

	_, _, err = mgr.Create(t.Context(), xfsStageTestVolume, 100)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorContains(t, err, "refuse untracked preexisting xfs stage")
	assert.Empty(t, mgr.activeIDs, "a refused foreign stage must not leak allocation authority")
	assert.Empty(t, mgr.volumeToID)
	assert.NoFileExists(t, logPath, "refusal must occur before quota mutation")
	assert.DirExists(t, stage.hostPath(rootPath))
}
