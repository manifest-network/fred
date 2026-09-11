package placement

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func verifyBoltFilePhysicalConsistency(path string) (resultErr error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
	})
	if err != nil {
		return fmt.Errorf("open placement database for physical validation: %w", err)
	}
	defer func() {
		if err := db.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close physically validated placement database: %w", err)
		}
	}()
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat physically validated placement database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("physically validated placement database is not a regular file")
	}
	return nil
}

type fakeBoltWriteTransaction struct {
	commitErr     error
	rollbackErr   error
	commitCalls   int
	rollbackCalls int
}

func (tx *fakeBoltWriteTransaction) Commit() error {
	tx.commitCalls++
	return tx.commitErr
}

func (tx *fakeBoltWriteTransaction) Rollback() error {
	tx.rollbackCalls++
	return tx.rollbackErr
}

func TestFinishBoltWriteTransaction_ClassifiesOnlyCommitErrorsAsOutcomeUnknown(t *testing.T) {
	assemblyErr := errors.New("synthetic mutation assembly failure")
	assemblyTx := &fakeBoltWriteTransaction{}
	err := finishBoltWriteTransaction(assemblyTx, func() error { return assemblyErr })
	require.ErrorIs(t, err, assemblyErr)
	assert.NotErrorIs(t, err, errBoltCommitOutcomeUnknown)
	assert.Zero(t, assemblyTx.commitCalls)
	assert.Equal(t, 1, assemblyTx.rollbackCalls)

	commitErr := errors.New("synthetic bbolt commit failure")
	commitTx := &fakeBoltWriteTransaction{commitErr: commitErr}
	err = finishBoltWriteTransaction(commitTx, func() error { return nil })
	require.ErrorIs(t, err, commitErr)
	require.ErrorIs(t, err, errBoltCommitOutcomeUnknown)
	assert.Equal(t, 1, commitTx.commitCalls)
	assert.Equal(t, 1, commitTx.rollbackCalls)

	successTx := &fakeBoltWriteTransaction{}
	require.NoError(t, finishBoltWriteTransaction(successTx, func() error { return nil }))
	assert.Equal(t, 1, successTx.commitCalls)
	assert.Zero(t, successTx.rollbackCalls)
}

func TestVerifyBoltPhysicalConsistency_AcceptsHealthyDatabaseAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("placements"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("lease"), []byte("backend"))
	}))
	require.NoError(t, verifyBoltPhysicalConsistency(db))
	require.NoError(t, db.Sync())
	require.NoError(t, db.Close())
	require.NoError(t, verifyBoltFilePhysicalConsistency(path))
}

func TestVerifyBoltFilePhysicalConsistencyRejectsControlledPageCorruption(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("placements"))
		if createErr != nil {
			return createErr
		}
		// Multiple values make the data bucket unambiguously materialize at least
		// one leaf page separate from bbolt's two checksummed meta pages.
		for index := range 32 {
			if err := bucket.Put(
				[]byte(fmt.Sprintf("lease-%02d", index)),
				[]byte(fmt.Sprintf("backend-%02d", index)),
			); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Sync())
	pageSize := db.Info().PageSize
	require.NoError(t, db.Close())

	image, err := os.ReadFile(path)
	require.NoError(t, err)
	// Each meta page begins with a 16-byte page header. Within its meta body,
	// root.root is at +16 and txid at +48. Select the newer valid meta page and
	// corrupt the active root page rather than an older page that may be free.
	metaOffset := 0
	if txID0, txID1 := binary.LittleEndian.Uint64(image[64:72]),
		binary.LittleEndian.Uint64(image[pageSize+64:pageSize+72]); txID1 > txID0 {
		metaOffset = pageSize
	}
	rootPageID := binary.LittleEndian.Uint64(image[metaOffset+32 : metaOffset+40])
	require.GreaterOrEqual(t, rootPageID, uint64(2))
	rootOffset := int(rootPageID) * pageSize
	require.LessOrEqual(t, rootOffset+16, len(image))
	flags := binary.LittleEndian.Uint16(image[rootOffset+8 : rootOffset+10])
	require.NotZero(t, flags&0x03, "active root must be a branch or leaf page")
	// Keep the page otherwise parseable but make its self-ID disagree with its
	// physical location. Tx.Check reports the now out-of-bounds reachable page.
	binary.LittleEndian.PutUint64(
		image[rootOffset:rootOffset+8], rootPageID+1000,
	)
	require.NoError(t, os.WriteFile(path, image, 0o600))

	err = verifyBoltFilePhysicalConsistency(path)
	require.ErrorIs(t, err, ErrPhysicalConsistency)
	require.ErrorContains(t, err, "errors")
}

func TestClassifyExactBackupResult_PreservesPublishedArtifactBoundary(t *testing.T) {
	cause := errors.New("synthetic deferred cleanup failure")
	assert.ErrorIs(t, classifyExactBackupResult(false, cause), cause)
	assert.NotErrorIs(t, classifyExactBackupResult(false, cause), ErrExactBackupPublished)

	err := classifyExactBackupResult(true, cause)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, ErrExactBackupPublished)
	assert.Same(t, err, classifyExactBackupResult(true, err),
		"classification must be idempotent across nested cleanup defers")
}

func TestToolMutationClassifiers_ExposeOperationSpecificOutcomeUnknownSentinels(t *testing.T) {
	cause := errors.New("synthetic commit failure")
	commitErr := fmt.Errorf("%w: %w", errBoltCommitOutcomeUnknown, cause)

	legacyErr := classifyLegacyPreparationTransactionError(commitErr)
	require.ErrorIs(t, legacyErr, ErrLegacyPreparationOutcomeUnknown)
	require.ErrorIs(t, legacyErr, cause)

	repairErr := classifyRepairTransactionError("refuse test operation", commitErr)
	require.ErrorIs(t, repairErr, ErrRepairMutationOutcomeUnknown)
	require.ErrorIs(t, repairErr, cause)

	ordinary := errors.New("synthetic pre-commit failure")
	assert.Same(t, ordinary, classifyLegacyPreparationTransactionError(ordinary))
	assert.Same(t, ordinary, classifyRepairTransactionError("test", ordinary))
}
