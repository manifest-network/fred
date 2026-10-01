package payload

import (
	"errors"
	"fmt"
	"io"

	bolt "go.etcd.io/bbolt"
)

// SnapshotRead is a read-only view of the payload database at one instant, for
// copying it while the store stays live. Only BeginSnapshotRead mints one; its
// owner must call WriteTo and Rollback from the same goroutine, because bbolt
// panics when a transaction is rolled back during its own WriteTo. The zero
// value is invalid.
type SnapshotRead struct {
	tx *bolt.Tx
}

// BeginSnapshotRead begins the read transaction of an online snapshot. It only
// begins the transaction: placement calls it while holding the placement write
// gate, so it must not take this store's gate or probe its authority. Check
// authority with CheckSnapshotAuthority before beginning and after the copy.
func (s *Store) BeginSnapshotRead() (SnapshotRead, error) {
	if s == nil || s.db == nil {
		return SnapshotRead{}, ErrStoreAuthorityUnavailable
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return SnapshotRead{}, fmt.Errorf("begin payload snapshot read: %w", err)
	}
	return SnapshotRead{tx: tx}, nil
}

// CheckSnapshotAuthority re-attests the store's file identity and authority.
// Call it before beginning a snapshot read and after its copy, never while a
// snapshot read is open: a payload writer that must grow the database waits for
// open reads while holding this store's gate.
func (s *Store) CheckSnapshotAuthority() error {
	if s == nil {
		return ErrStoreAuthorityUnavailable
	}
	return s.reattestAuthority()
}

// Size is the database size in bytes as of the snapshot instant; WriteTo
// writes exactly this many bytes. Call it before WriteTo or Rollback.
func (read SnapshotRead) Size() int64 {
	if read.tx == nil {
		return 0
	}
	return read.tx.Size()
}

// WriteTo writes the database as of the snapshot instant to w.
func (read SnapshotRead) WriteTo(w io.Writer) (int64, error) {
	if read.tx == nil {
		return 0, errors.New("payload snapshot read is invalid")
	}
	return read.tx.WriteTo(w)
}

// Rollback ends the read transaction.
func (read SnapshotRead) Rollback() error {
	if read.tx == nil {
		return errors.New("payload snapshot read is invalid")
	}
	return read.tx.Rollback()
}
