package placement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareReplacementRepairDB(t *testing.T, dbPath string) ([]byte, string, string) {
	t.Helper()
	contents, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	replacementPath := filepath.Join(filepath.Dir(dbPath), "replacement-placements.db")
	displacedPath := filepath.Join(filepath.Dir(dbPath), "opened-placements.db")
	require.NoError(t, os.WriteFile(replacementPath, contents, 0o600))
	return contents, replacementPath, displacedPath
}

func replaceOpenRepairDB(dbPath, replacementPath, displacedPath string) error {
	if err := os.Rename(dbPath, displacedPath); err != nil {
		return err
	}
	return os.Rename(replacementPath, dbPath)
}

func repairCachedPlacement(repair *AttemptRepair, leaseUUID string) (Placement, bool) {
	repair.store.mu.RLock()
	defer repair.store.mu.RUnlock()
	placement, exists := repair.store.cache[leaseUUID]
	return placement, exists
}

func TestAttemptRepairRejectsRenameOverOpenBeforeRefusalMutation(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	before, replacementPath, displacedPath := prepareReplacementRepairDB(t, dbPath)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, originalProbe := requireAttemptRepairAuthorities(t, repair, candidate)

	probe := func(ctx context.Context) (RepairInventorySnapshot, error) {
		inventory, probeErr := originalProbe(ctx)
		if probeErr != nil {
			return RepairInventorySnapshot{}, probeErr
		}
		if err := replaceOpenRepairDB(dbPath, replacementPath, displacedPath); err != nil {
			return RepairInventorySnapshot{}, err
		}
		return inventory, nil
	}
	_, err = repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.ErrorContains(t, err, "no longer identifies the opened repair database")
	assert.NotErrorIs(t, err, ErrRepairMutationCommitted,
		"pathname loss before the transaction must remain categorically uncommitted")
	cached, exists := repairCachedPlacement(repair, repairLease)
	require.True(t, exists)
	assert.Equal(t, operationID, cached.attemptOperationID)

	configured, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, configured)
	opened, readErr := os.ReadFile(displacedPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, opened)
}

func TestAttemptRepairRejectsRenameOverOpenBeforeConflictMutation(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	before, replacementPath, displacedPath := prepareReplacementRepairDB(t, dbPath)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, plan, drain, originalProbe := requireConflictRepairAuthorities(
		t, repair, candidate, true, nil,
	)

	probe := func(ctx context.Context) (RepairInventorySnapshot, error) {
		inventory, probeErr := originalProbe(ctx)
		if probeErr != nil {
			return RepairInventorySnapshot{}, probeErr
		}
		if err := replaceOpenRepairDB(dbPath, replacementPath, displacedPath); err != nil {
			return RepairInventorySnapshot{}, err
		}
		return inventory, nil
	}
	_, err = repair.ResolveConflictContext(ctx, plan, drain, probe)
	require.ErrorContains(t, err, "no longer identifies the opened repair database")
	assert.NotErrorIs(t, err, ErrRepairMutationCommitted,
		"pathname loss before the transaction must remain categorically uncommitted")
	cached, exists := repairCachedPlacement(repair, repairLease)
	require.True(t, exists)
	assert.True(t, cached.Conflict)
	assert.Equal(t, []string{"backend-a", "backend-b"}, cached.ConflictBackends)

	configured, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, configured)
	opened, readErr := os.ReadFile(displacedPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, opened)
}

func TestAttemptRepairPathLossAfterCommitIsClassifiedCommitted(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	_, replacementPath, displacedPath := prepareReplacementRepairDB(t, dbPath)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, _, _ := requireAttemptRepairAuthorities(t, repair, candidate)

	repair.store.mu.Lock()
	applied, mutationErr := repair.refuseAttemptContextLocked(ctx, candidate, evidence)
	replaceErr := replaceOpenRepairDB(dbPath, replacementPath, displacedPath)
	postconditionErr := repair.verifySourcePathAfterMutation("refuse exact placement operation")
	repair.store.mu.Unlock()

	require.NoError(t, mutationErr)
	require.True(t, applied)
	require.NoError(t, replaceErr)
	require.ErrorIs(t, postconditionErr, ErrRepairMutationCommitted)
	require.ErrorContains(t, postconditionErr, "no longer identifies the opened repair database")
	_, exists := repairCachedPlacement(repair, repairLease)
	assert.False(t, exists,
		"the open inode and in-memory cache contain the committed refusal")
}

func TestRepairInspectorRejectsSemanticallyValidRefusalOnReplacementInode(t *testing.T) {
	dbPath, operationID := createRepairFixture(t, false)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchAttempt(repairLease, "backend-a", operationID)
	require.NoError(t, err)
	ctx, evidence, drain, probe := requireAttemptRepairAuthorities(t, repair, candidate)
	result, err := repair.RefuseContext(ctx, candidate, evidence, drain, probe)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	_, replacementPath, displacedPath := prepareReplacementRepairDB(t, dbPath)
	require.NoError(t, replaceOpenRepairDB(dbPath, replacementPath, displacedPath))
	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inspector.Close() })
	require.NoError(t, verifyRefusedStore(inspector.store, candidate),
		"the replacement deliberately has the exact successful semantic result")
	err = inspector.VerifyRefusalPostcondition(candidate, result)
	require.ErrorContains(t, err, "not the inode mutated by the repair session")
}

func TestRepairInspectorRejectsSemanticallyValidConflictResultOnReplacementInode(t *testing.T) {
	dbPath := createConflictRepairFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	candidate, err := repair.MatchConflict(repairLease, "backend-a")
	require.NoError(t, err)
	ctx, plan, drain, probe := requireConflictRepairAuthorities(
		t, repair, candidate, true, nil,
	)
	result, err := repair.ResolveConflictContext(ctx, plan, drain, probe)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	_, replacementPath, displacedPath := prepareReplacementRepairDB(t, dbPath)
	require.NoError(t, replaceOpenRepairDB(dbPath, replacementPath, displacedPath))
	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inspector.Close() })
	inspector.store.mu.RLock()
	semanticErr := verifyResolvedConflictLocked(
		inspector.store, candidate.leaseUUID, result.expected, result.lifecycle,
	)
	inspector.store.mu.RUnlock()
	require.NoError(t, semanticErr,
		"the replacement deliberately has the exact successful semantic result")
	err = inspector.VerifyConflictResolutionPostcondition(candidate, result)
	require.ErrorContains(t, err, "not the inode mutated by the repair session")
}

// Every offline command opens through bindOfflinePlacementAuthority.  These
// checks make the shared boundary reject the same unsafe final entry forms as
// the running daemon before bbolt can inspect or lock a different object.
func TestOfflinePlacementAuthorityRejectsUnsafeFinalEntries(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(t *testing.T, dbPath string)
		want  string
	}{
		{
			name: "mode",
			setup: func(t *testing.T, dbPath string) {
				t.Helper()
				require.NoError(t, os.Chmod(dbPath, 0o640))
			},
			want: "exact mode 0600",
		},
		{
			name: "hard link",
			setup: func(t *testing.T, dbPath string) {
				t.Helper()
				require.NoError(t, os.Link(dbPath, dbPath+".extra"))
			},
			want: "exactly one hard link",
		},
		{
			name: "final symlink",
			setup: func(t *testing.T, dbPath string) {
				t.Helper()
				target := dbPath + ".target"
				require.NoError(t, os.Rename(dbPath, target))
				require.NoError(t, os.Symlink(target, dbPath))
			},
			want: "not a regular file",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath, _ := createRepairFixture(t, false)
			test.setup(t, dbPath)

			_, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestOfflinePlacementAuthorityDetectsParentReplacement(t *testing.T) {
	dbPath, _ := createRepairFixture(t, false)
	authority, err := bindOfflinePlacementAuthority(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = authority.close() })

	parent := filepath.Dir(dbPath)
	displaced := parent + "-displaced"
	require.NoError(t, os.Rename(parent, displaced))
	require.NoError(t, os.Mkdir(parent, 0o700))
	require.ErrorContains(t, authority.verify(), "directory identity changed")
}

func TestRepairSourcePathCheckRejectsMissingSourceIdentity(t *testing.T) {
	err := (&AttemptRepair{}).verifySourcePath()
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrRepairMutationCommitted))
}
