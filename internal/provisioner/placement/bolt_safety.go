package placement

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/fsidentity"

	bolt "go.etcd.io/bbolt"
)

const maxPhysicalConsistencyErrors = 16

var (
	// ErrPhysicalConsistency means bbolt's physical page/freelist consistency
	// check found corruption. Offline tooling must refuse to copy or mutate the
	// database because logical bucket reads alone cannot establish a safe image.
	ErrPhysicalConsistency = errors.New("placement database failed physical consistency validation")

	// errBoltCommitOutcomeUnknown is deliberately package-private. A bbolt
	// Commit error can be returned after database pages or a meta page reached
	// the filesystem, so callers must not report it as a definitely rolled-back
	// mutation. Public tool APIs translate it into operation-specific sentinels.
	errBoltCommitOutcomeUnknown = errors.New("bbolt commit outcome is unknown")
)

// verifyBoltPhysicalConsistency runs bbolt's complete page/freelist checker in
// one read transaction. It drains the checker channel even after the reporting
// limit so its producer goroutine cannot be stranded on a corrupt database.
func verifyBoltPhysicalConsistency(db *bolt.DB) error {
	if db == nil {
		return errors.New("placement database is not open")
	}
	return db.View(func(tx *bolt.Tx) error {
		var (
			messages []string
			count    int
		)
		for checkErr := range tx.Check() {
			count++
			if len(messages) < maxPhysicalConsistencyErrors {
				messages = append(messages, checkErr.Error())
			}
		}
		if count == 0 {
			return nil
		}
		if omitted := count - len(messages); omitted > 0 {
			messages = append(messages, fmt.Sprintf("... and %d more", omitted))
		}
		return fmt.Errorf(
			"%w (%d errors): %s",
			ErrPhysicalConsistency,
			count,
			strings.Join(messages, "; "),
		)
	})
}

func verifyBoltEntryPhysicalConsistency(
	entry fsidentity.Entry,
	expected os.FileInfo,
) (resultErr error) {
	if !entry.Valid() || expected == nil {
		return errors.New("bound placement database entry and expected inode are required")
	}
	db, err := bolt.Open(entry.DisplayPath(), 0o600, &bolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
		OpenFile: func(_ string, flag int, mode os.FileMode) (*os.File, error) {
			file, openErr := entry.OpenFile(flag&^os.O_CREATE, mode)
			if openErr != nil {
				return nil, openErr
			}
			info, statErr := file.Stat()
			if statErr != nil || !info.Mode().IsRegular() || !os.SameFile(expected, info) {
				_ = file.Close()
				if statErr != nil {
					return nil, fmt.Errorf("stat bound placement database: %w", statErr)
				}
				return nil, errors.New("bound placement database entry changed before open")
			}
			return file, nil
		},
	})
	if err != nil {
		return fmt.Errorf("open bound placement database for physical validation: %w", err)
	}
	defer func() {
		if err := db.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close physically validated bound placement database: %w", err)
		}
	}()
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		return err
	}
	return verifyPublishedEntryIdentity(entry, expected)
}

func verifyPublishedEntryIdentity(entry fsidentity.Entry, expected os.FileInfo) error {
	if !entry.Valid() || expected == nil {
		return errors.New("published bound entry and source identity are required")
	}
	actual, err := entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat published bound file: %w", err)
	}
	if !actual.Mode().IsRegular() || !os.SameFile(expected, actual) {
		return errors.New("published bound entry no longer names the no-overwrite source inode")
	}
	return nil
}

// verifyExactAuthorityBucketSet is used only by offline creation/migration
// postconditions. Runtime schema checks deliberately remain forward-compatible,
// while a tool claiming to have created one exact authority must reject any
// unexpected top-level state that was raced in or copied from another store.
func verifyExactAuthorityBucketSet(tx *bolt.Tx) error {
	if tx == nil {
		return errors.New("placement database transaction is required")
	}
	expected := [][]byte{bucketName, lifecycleCapabilityBucketName, metadataBucketName}
	seen := make(map[string]struct{}, len(expected))
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		for _, allowed := range expected {
			if bytes.Equal(name, allowed) {
				seen[string(name)] = struct{}{}
				return nil
			}
		}
		return fmt.Errorf("unexpected top-level placement authority bucket %q", name)
	}); err != nil {
		return err
	}
	for _, required := range expected {
		if _, ok := seen[string(required)]; !ok {
			return fmt.Errorf("required placement authority bucket %q is missing", required)
		}
	}
	return nil
}

type boltWriteTransaction interface {
	Commit() error
	Rollback() error
}

// finishBoltWriteTransaction makes the commit boundary explicit. Mutation
// assembly failures never call Commit and are therefore definitely
// uncommitted; every Commit error is outcome-unknown, regardless of the stage
// at which bbolt surfaced it.
func finishBoltWriteTransaction(
	tx boltWriteTransaction,
	mutate func() error,
) (resultErr error) {
	if tx == nil {
		return errors.New("bbolt write transaction is required")
	}
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback()
		}
	}()

	if err := mutate(); err != nil {
		rollbackErr := tx.Rollback()
		finished = true
		if rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("roll back rejected placement mutation: %w", rollbackErr))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		// Some bbolt Commit errors close/roll back the transaction while others can
		// leave it open. A defensive Rollback is harmless when already closed and
		// releases the writer lock otherwise; it cannot undo bytes already written.
		_ = tx.Rollback()
		finished = true
		return fmt.Errorf("%w: %w", errBoltCommitOutcomeUnknown, err)
	}
	finished = true
	return nil
}

func updateBoltWithExplicitOutcome(
	db *bolt.DB,
	mutate func(*bolt.Tx) error,
) error {
	if db == nil {
		return errors.New("placement database is not open")
	}
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	return finishBoltWriteTransaction(tx, func() error { return mutate(tx) })
}
