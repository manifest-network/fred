package placement

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openExactBackupSourceForTest(t *testing.T) (*bolt.DB, os.FileInfo) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket([]byte("placements"))
		return createErr
	}))
	info, err := os.Stat(path)
	require.NoError(t, err)
	return db, info
}

// An exact backup never overwrites. A destination that appears after the
// target was bound is refused at write time, and nothing is published.
func TestWriteExactBackupRefusesADestinationThatAppearedAfterBinding(t *testing.T) {
	db, info := openExactBackupSourceForTest(t)
	backupPath := filepath.Join(t.TempDir(), "placements.bak")
	target, err := BindExactBackupTarget(backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	require.NoError(t, os.WriteFile(backupPath, []byte("someone else's file"), 0o600))

	err = writeExactBackup(db, info, target)
	require.ErrorContains(t, err, "backup destination already exists")
	require.NotErrorIs(t, err, ErrExactBackupPublished)
	content, readErr := os.ReadFile(backupPath)
	require.NoError(t, readErr)
	require.Equal(t, "someone else's file", string(content))
	require.ErrorContains(t, target.VerifyPublished(), "has not been durably published")
}

// A bound target proves nothing until a backup was published through it.
func TestExactBackupTargetIsNotPublishedUntilWritten(t *testing.T) {
	db, info := openExactBackupSourceForTest(t)
	target, err := BindExactBackupTarget(filepath.Join(t.TempDir(), "placements.bak"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	require.NoError(t, target.Verify())
	require.ErrorContains(t, target.VerifyPublished(), "has not been durably published")

	require.NoError(t, writeExactBackup(db, info, target))
	require.NoError(t, target.VerifyPublished())
}
