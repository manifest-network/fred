package imagefetch

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

const debitFileName = "image-import-debit-v1"
const debitRecordSize = 8 + 8 + sha256.Size
const debitMagic = "FREDIMG2"
const legacyDebitMagic = "FREDIMG1"

// debitLedger belongs to the staging directory's exclusive owner. It is a
// shared pointer so copying a Loader cannot duplicate its update mutex. Every
// operation reads the durable record; reopening never assumes zero debt.
type debitLedger struct {
	mu   sync.Mutex
	root string
}

// PendingBytes reports import allocations whose completion has not been
// established. The caller must charge these bytes before any new admission.
// An unreadable record is an admission refusal, never evidence of zero debt.
func (l *Loader) PendingBytes() (int64, error) {
	if l == nil || l.ledger == nil {
		return 0, errors.New("image import ledger is unavailable")
	}
	l.ledger.mu.Lock()
	defer l.ledger.mu.Unlock()
	root, err := os.OpenRoot(l.ledger.root)
	if err != nil {
		return 0, err
	}
	defer func() { _ = root.Close() }()
	return readDebit(root)
}

func (l *Loader) changeDebit(change int64) error {
	l.ledger.mu.Lock()
	defer l.ledger.mu.Unlock()
	root, err := os.OpenRoot(l.ledger.root)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	current, err := readDebit(root)
	if err != nil {
		return err
	}
	if (change > 0 && current > math.MaxInt64-change) || (change < 0 && current < -change) {
		return errors.New("image import debit is outside its accounting range")
	}
	return writeDebit(root, current+change)
}

func readDebit(root *os.Root) (int64, error) {
	f, err := root.Open(debitFileName)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read outstanding image import debit: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() != debitRecordSize {
		return 0, errors.New("invalid image import debit record")
	}
	var record [debitRecordSize]byte
	if _, err := io.ReadFull(f, record[:]); err != nil {
		return 0, err
	}
	checksum := sha256.Sum256(record[:16])
	magic := string(record[:8])
	if (magic != debitMagic && magic != legacyDebitMagic) || subtle.ConstantTimeCompare(checksum[:], record[16:]) != 1 {
		return 0, errors.New("corrupt image import debit record")
	}
	amount := binary.BigEndian.Uint64(record[8:16])
	if amount > math.MaxInt64 {
		return 0, errors.New("invalid image import debit amount")
	}
	if magic == legacyDebitMagic && amount != 0 {
		return 0, errors.New("legacy image import allocation lacks complete metadata accounting; external runtime drain and offline debit recovery are required")
	}
	return int64(amount), nil
}

func writeDebit(root *os.Root, amount int64) error {
	var record [debitRecordSize]byte
	copy(record[:8], debitMagic)
	binary.BigEndian.PutUint64(record[8:16], uint64(amount))
	checksum := sha256.Sum256(record[:16])
	copy(record[16:], checksum[:])
	name := ".fred-image-debit-" + rand.Text()
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = root.Remove(name) }()
	if _, err = f.Write(record[:]); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := root.Rename(name, debitFileName); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
