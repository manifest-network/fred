package payload

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/fsidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/provisioner/storeauthority"
	"github.com/manifest-network/fred/internal/util"
)

// Write batching configuration for reducing bbolt lock contention.
const (
	// DefaultBatchSize is the maximum number of operations to batch together.
	DefaultBatchSize = 50

	// DefaultFlushInterval is how often to flush pending writes if batch isn't full.
	DefaultFlushInterval = 50 * time.Millisecond

	// writeChannelSize is the buffer size for the write operation channel.
	// When the buffer is full, write operations (Store, Pop, Delete) will block
	// until space is available, providing backpressure under extreme load.
	// With DefaultBatchSize=50 and DefaultFlushInterval=50ms, the theoretical
	// throughput is ~1000 ops/sec, so this buffer handles short bursts well.
	writeChannelSize = 1000
)

var (
	// payloadBucketName is the bbolt bucket for storing payloads.
	payloadBucketName = []byte("payloads")

	// payloadMetaBucketName is the bbolt bucket for storing payload metadata (timestamps).
	payloadMetaBucketName = []byte("payload_meta")

	// payloadHashBucketName is the bbolt bucket for storing each payload's own
	// SHA-256 (ENG-619). It is deliberately a separate bucket rather than a
	// field inside the payload record: a checksum stored outside the data it
	// describes is what makes it useful on a single-node store, and it keeps
	// the payload bucket byte-identical to what pre-ENG-619 builds wrote.
	//
	// Entries are written by Store and Put and removed by Pop and Delete, all
	// inside the same bbolt transaction as the payload itself, so a payload can
	// never outlive its hash or vice versa. A payload written by an older build
	// simply has no entry here; readers fall back to the on-chain MetaHash.
	payloadHashBucketName = []byte("payload_hashes")

	// ErrStoreAuthorityUnavailable is sticky for one Store lifetime. It means the
	// configured pathname can no longer be proved to name the private, single-link
	// regular file opened by this process.
	ErrStoreAuthorityUnavailable = errors.New("payload store authority is unavailable")

	// ErrStoreAuthorityPathChanged identifies permission, link-count, pathname,
	// or physical-parent drift at the payload database.
	ErrStoreAuthorityPathChanged = errors.New("payload store pathname changed")

	// ErrStoreMutationOutcomeUnknown means bbolt returned an error from Commit.
	// Database pages or a meta page may already be durable, so the process must
	// withdraw this Store's authority instead of retrying as though the batch
	// definitely rolled back.
	ErrStoreMutationOutcomeUnknown = errors.New("payload store mutation outcome is unknown")
)

// writeOpType represents the type of write operation.
type writeOpType int

const (
	opStore writeOpType = iota
	opDelete
	opPop
	// opPut overwrites unconditionally, where opStore conflicts on an existing
	// key. It exists for the tenant /update path (ENG-619), which by definition
	// replaces a payload that is already there.
	opPut
)

// writeOp represents a write operation to be batched.
type writeOp struct {
	opType   writeOpType
	key      string
	payload  []byte
	hash     []byte    // For store/put operations: SHA-256 of payload, computed off the writer goroutine
	time     time.Time // For store operations
	resultCh chan writeResult
}

// writeResult is returned to callers after their operation completes.
type writeResult struct {
	stored  bool   // For Store: whether the payload was stored (false if already existed)
	payload []byte // For Pop: the retrieved payload
	existed bool   // For Delete/Put: whether the key existed beforehand
	err     error
}

// Store stores pending payloads for leases awaiting provisioning.
// Payloads are persisted to bbolt to survive restarts.
// The chain's MetaHash remains the source of truth for validation.
//
// Write operations are batched through a dedicated writer goroutine to reduce
// bbolt lock contention under high concurrency.
type Store struct {
	db *bolt.DB

	authorityDirectory *fsidentity.Directory
	authorityEntry     fsidentity.Entry
	authorityInfo      os.FileInfo
	authorityGate      *storeauthority.Gate

	// Write batching
	writeCh       chan writeOp
	batchSize     int
	flushInterval time.Duration

	// For graceful shutdown
	ctx       context.Context // store-level context; Done channel used by write methods to avoid blocking after Close
	cancel    context.CancelFunc
	wg        *sync.WaitGroup // Pointer to avoid copy-by-value issues
	closeOnce *sync.Once      // Pointer to avoid copy-by-value issues
	closeErr  error
}

// StoreConfig configures the payload store.
type StoreConfig struct {
	DBPath        string        // Path to bbolt database file
	BatchSize     int           // Max operations per batch (default: 50)
	FlushInterval time.Duration // Max wait before flushing batch (default: 50ms)
}

// NewStore creates a new payload store with bbolt persistence.
func NewStore(cfg StoreConfig) (*Store, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	// For batchSize and flushInterval, use max() to convert negative values to 0
	batchSize := cmp.Or(max(cfg.BatchSize, 0), DefaultBatchSize)
	flushInterval := cmp.Or(max(cfg.FlushInterval, 0), DefaultFlushInterval)

	canonicalPath, directory, entry, existingInfo, exists, err := bindPayloadStorePath(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	closeDirectory := true
	defer func() {
		if closeDirectory {
			_ = directory.Close()
		}
	}()

	var openedInfo os.FileInfo
	db, err := bolt.Open(canonicalPath, 0o600, &bolt.Options{
		Timeout: 5 * time.Second,
		OpenFile: func(requested string, flag int, mode os.FileMode) (*os.File, error) {
			if requested != canonicalPath {
				return nil, errors.New("bbolt requested an unexpected payload database path")
			}
			if exists {
				flag &^= os.O_CREATE
			} else {
				flag |= os.O_CREATE | os.O_EXCL
			}
			file, openErr := entry.OpenFile(flag, mode)
			if openErr != nil {
				return nil, openErr
			}
			info, statErr := file.Stat()
			if statErr != nil {
				_ = file.Close()
				return nil, fmt.Errorf("stat opened payload database: %w", statErr)
			}
			if validationErr := validatePayloadStoreFile(canonicalPath, info); validationErr != nil {
				_ = file.Close()
				return nil, validationErr
			}
			if existingInfo != nil && !os.SameFile(existingInfo, info) {
				_ = file.Close()
				return nil, errors.New("payload database changed between validation and open")
			}
			if openedInfo != nil && !os.SameFile(openedInfo, info) {
				_ = file.Close()
				return nil, errors.New("bbolt reopened a different payload database inode")
			}
			openedInfo = info
			return file, nil
		},
	})
	if err != nil {
		return nil, err
	}
	if openedInfo == nil {
		_ = db.Close()
		return nil, errors.New("bbolt did not open the payload database")
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(payloadBucketName); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(payloadMetaBucketName); err != nil {
			return err
		}
		// Created here rather than migrated: a database written by a pre-ENG-619
		// build simply gains an empty bucket on first open, and its existing
		// payloads keep verifying against the on-chain MetaHash.
		if _, err := tx.CreateBucketIfNotExists(payloadHashBucketName); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		_ = db.Close() // Best effort cleanup on init failure
		return nil, err
	}
	if err := verifyPayloadStoreBinding(directory, entry, openedInfo); err != nil {
		_ = db.Close()
		return nil, err
	}
	// bbolt synchronizes file pages, not the directory entry which names the
	// inode. Sync on every construction, including a retry after an earlier
	// constructor failed, so NewStore never publishes an entry whose durability
	// depends on whether this invocation happened to create it.
	if err := directory.Sync(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sync payload database parent: %w", err)
	}
	if err := verifyPayloadStoreBinding(directory, entry, openedInfo); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("re-attest payload database after parent sync: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	authorityGate, err := storeauthority.New(
		func(err error) bool {
			return errors.Is(err, ErrStoreAuthorityUnavailable) ||
				errors.Is(err, ErrStoreMutationOutcomeUnknown)
		},
		func(error) { cancel() },
	)
	if err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("construct payload store authority gate: %w", err)
	}
	s := &Store{
		db:                 db,
		authorityDirectory: directory,
		authorityEntry:     entry,
		authorityInfo:      openedInfo,
		authorityGate:      authorityGate,
		writeCh:            make(chan writeOp, writeChannelSize),
		batchSize:          batchSize,
		flushInterval:      flushInterval,
		ctx:                ctx,
		cancel:             cancel,
		wg:                 &sync.WaitGroup{},
		closeOnce:          &sync.Once{},
	}
	initialCount, err := s.count()
	if err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("count initialized payloads: %w", err)
	}
	closeDirectory = false

	// Start the batching writer goroutine (using WaitGroup.Go for Go 1.25+).
	// Wrap with recover so a panic in writerLoop (e.g., bbolt returning
	// a nil bucket after corruption) doesn't crash fred. On panic, cancel
	// the store's ctx so Store/Pop/Delete callers fail fast with
	// ctx.Err() instead of hanging forever on a now-dead writeCh reader.
	s.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("payload writer panic — closing store to keep fred alive",
					"panic", r,
					"stack", string(debug.Stack()),
				)
				background.GoroutinePanicsTotal.WithLabelValues("payload_writer").Inc()
				// Cancel the store's ctx; subsequent Store/Pop/Delete
				// calls observe ctx.Done() and return errors rather
				// than blocking on the now-defunct writeCh.
				s.cancel()
			}
		}()
		s.writerLoop(ctx)
	})

	// Initialize the stored count metric based on the pre-publication snapshot.
	metrics.PayloadStoredCount.Set(float64(initialCount))

	slog.Info("payload store initialized",
		"db_path", cfg.DBPath,
		"batch_size", batchSize,
		"flush_interval", flushInterval,
		"initial_count", initialCount,
	)

	return s, nil
}

func bindPayloadStorePath(
	path string,
) (
	canonicalPath string,
	directory *fsidentity.Directory,
	entry fsidentity.Entry,
	existingInfo os.FileInfo,
	exists bool,
	resultErr error,
) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			fmt.Errorf("resolve payload database path: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			fmt.Errorf("resolve payload database parent: %w", err)
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			fmt.Errorf("resolve absolute payload database parent: %w", err)
	}
	parent = filepath.Clean(parent)
	directory, err = fsidentity.OpenDirectory(parent)
	if err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			fmt.Errorf("bind payload database parent: %w", err)
	}
	closeOnError := func(cause error) error {
		if closeErr := directory.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("close payload database parent: %w", closeErr))
		}
		return cause
	}
	entry, err = directory.Entry(filepath.Base(absolute))
	if err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			closeOnError(fmt.Errorf("bind payload database entry: %w", err))
	}
	canonicalPath = entry.DisplayPath()
	existingInfo, err = entry.Lstat()
	switch {
	case err == nil:
		if err := validatePayloadStoreFile(canonicalPath, existingInfo); err != nil {
			return "", nil, fsidentity.Entry{}, nil, false, closeOnError(err)
		}
		exists = true
	case errors.Is(err, os.ErrNotExist):
		existingInfo = nil
		exists = false
	case err != nil:
		return "", nil, fsidentity.Entry{}, nil, false,
			closeOnError(fmt.Errorf("stat payload database: %w", err))
	}
	if err := directory.VerifyPath(); err != nil {
		return "", nil, fsidentity.Entry{}, nil, false,
			closeOnError(fmt.Errorf("verify payload database parent: %w", err))
	}
	return canonicalPath, directory, entry, existingInfo, exists, nil
}

func validatePayloadStoreFile(path string, info os.FileInfo) error {
	if info == nil || !info.Mode().IsRegular() {
		return fmt.Errorf("payload database %q is not a regular file", path)
	}
	if info.Mode() != 0o600 {
		return fmt.Errorf("payload database %q must have exact mode 0600", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("payload database %q must have exactly one hard link", path)
	}
	return nil
}

func verifyPayloadStoreBinding(
	directory *fsidentity.Directory,
	entry fsidentity.Entry,
	expected os.FileInfo,
) error {
	if directory == nil || !entry.Valid() || expected == nil {
		return errors.New("payload database authority is not bound")
	}
	if err := directory.VerifyPath(); err != nil {
		return fmt.Errorf("payload database parent changed: %w", err)
	}
	current, err := entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat payload database authority: %w", err)
	}
	if err := validatePayloadStoreFile(entry.DisplayPath(), current); err != nil {
		return err
	}
	if !os.SameFile(expected, current) {
		return errors.New("payload database path no longer names the opened inode")
	}
	return directory.VerifyPath()
}

func (s *Store) authorityFailure() error {
	if s == nil || s.authorityGate == nil || !s.authorityGate.Valid() {
		return ErrStoreAuthorityUnavailable
	}
	return s.authorityGate.Error()
}

func (s *Store) latchAuthorityFailure(cause error) error {
	if cause == nil {
		cause = errors.New("unknown payload database authority failure")
	}
	if s == nil || s.authorityGate == nil || !s.authorityGate.Valid() {
		return fmt.Errorf("%w: %w", ErrStoreAuthorityUnavailable, cause)
	}
	return s.authorityGate.Withdraw(payloadAuthorityPathFailure(cause))
}

func payloadAuthorityPathFailure(cause error) error {
	return fmt.Errorf("%w: %w: %w",
		ErrStoreAuthorityUnavailable, ErrStoreAuthorityPathChanged, cause)
}

func payloadCommitOutcomeUnknown(cause error) error {
	return fmt.Errorf("%w: %w: %w",
		ErrStoreAuthorityUnavailable, ErrStoreMutationOutcomeUnknown, cause)
}

func (s *Store) probeAuthority() error {
	if s == nil {
		return errors.New("payload store is nil")
	}
	return verifyPayloadStoreBinding(
		s.authorityDirectory,
		s.authorityEntry,
		s.authorityInfo,
	)
}

func (s *Store) reattestAuthority() error {
	if err := s.authorityFailure(); err != nil {
		return err
	}
	if err := s.probeAuthority(); err != nil {
		return s.latchAuthorityFailure(err)
	}
	return s.authorityFailure()
}

// terminalError preserves a sticky authority-withdrawal classification when
// the same failure also canceled the writer context. Ordinary shutdown reports
// the context error; a recovered mutation-boundary panic is latched first.
func (s *Store) terminalError() error {
	if err := s.authorityFailure(); err != nil {
		return err
	}
	return s.ctx.Err()
}

func (s *Store) view(fn func(*bolt.Tx) error) error {
	if s == nil || fn == nil {
		return errors.New("payload store and read transaction are required")
	}
	if err := s.reattestAuthority(); err != nil {
		return err
	}
	viewErr := s.db.View(fn)
	authorityErr := s.reattestAuthority()
	return errors.Join(viewErr, authorityErr)
}

func (s *Store) update(fn func(*bolt.Tx) error) error {
	if s == nil || fn == nil {
		return errors.New("payload store and write transaction are required")
	}
	if s.authorityGate == nil || !s.authorityGate.Valid() {
		return ErrStoreAuthorityUnavailable
	}
	return s.authorityGate.Run(func() error {
		if err := s.probeAuthority(); err != nil {
			return payloadAuthorityPathFailure(err)
		}
		updateErr := updatePayloadWithExplicitOutcome(s.db, fn)
		if errors.Is(updateErr, ErrStoreMutationOutcomeUnknown) {
			updateErr = payloadCommitOutcomeUnknown(updateErr)
		}
		postcheckErr := s.probeAuthority()
		if postcheckErr != nil {
			postcheckErr = payloadAuthorityPathFailure(postcheckErr)
		}
		return errors.Join(updateErr, postcheckErr)
	})
}

type payloadWriteTransaction interface {
	Commit() error
	Rollback() error
}

// finishPayloadWriteTransaction distinguishes a definitely uncommitted
// mutation-function rejection from an outcome-unknown bbolt Commit error.
func finishPayloadWriteTransaction(
	tx payloadWriteTransaction,
	mutate func() error,
) error {
	if tx == nil {
		return errors.New("payload write transaction is required")
	}
	if mutate == nil {
		return errors.New("payload mutation is required")
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
			return errors.Join(err, fmt.Errorf("roll back rejected payload mutation: %w", rollbackErr))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		// Commit may have made pages durable before returning its error. Rollback is
		// only a defensive writer-lock release; it cannot make the outcome definite.
		_ = tx.Rollback()
		finished = true
		return fmt.Errorf("%w: %w", ErrStoreMutationOutcomeUnknown, err)
	}
	finished = true
	return nil
}

func updatePayloadWithExplicitOutcome(
	db *bolt.DB,
	mutate func(*bolt.Tx) error,
) error {
	if db == nil {
		return errors.New("payload database is not open")
	}
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	return finishPayloadWriteTransaction(tx, func() error { return mutate(tx) })
}

// Store stores a payload for a lease.
// Returns false if a payload already exists for this lease (conflict).
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *Store) Store(leaseUUID string, payload []byte) bool {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opStore,
		key:      leaseUUID,
		payload:  payload,
		hash:     ComputeHash(payload),
		time:     time.Now(),
		resultCh: resultCh,
	}

	select {
	case s.writeCh <- op:
	case <-s.ctx.Done():
		slog.Warn("payload store closed, cannot store", "lease_uuid", leaseUUID)
		return false
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			slog.Error("failed to store payload", "lease_uuid", leaseUUID, "error", result.err)
			return false
		}
		if result.stored {
			metrics.PayloadStoredCount.Inc()
		}
		return result.stored
	case <-s.ctx.Done():
		slog.Warn("payload store closed during store", "lease_uuid", leaseUUID)
		return false
	}
}

// Put stores a payload for a lease, overwriting any payload already there, and
// records the payload's own SHA-256 in the same transaction.
//
// This is the tenant-update path (ENG-619). Store refuses to overwrite because
// the create path must not clobber a payload it did not write; an update is the
// opposite — replacing the stored manifest is the entire point, and failing to
// replace it is what made reprovisions silently revert tenants to their
// as-created deployment.
//
// Unlike Store, which reports a conflict through its bool, Put returns an error:
// its caller has already applied the update to the backend, so a failure here is
// a durability failure worth surfacing rather than a benign "someone got there
// first".
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *Store) Put(leaseUUID string, payload []byte) error {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opPut,
		key:      leaseUUID,
		payload:  payload,
		hash:     ComputeHash(payload),
		time:     time.Now(),
		resultCh: resultCh,
	}

	select {
	case s.writeCh <- op:
	case <-s.ctx.Done():
		return fmt.Errorf("payload store closed, cannot put payload for %s: %w", leaseUUID, s.terminalError())
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			return fmt.Errorf("failed to put payload for %s: %w", leaseUUID, result.err)
		}
		// Only a genuinely new key changes the stored count; an overwrite
		// replaces one payload with another.
		if !result.existed {
			metrics.PayloadStoredCount.Inc()
		}
		return nil
	case <-s.ctx.Done():
		return fmt.Errorf("payload store closed during put for %s: %w", leaseUUID, s.terminalError())
	}
}

// GetWithHash returns a lease's payload together with the SHA-256 recorded
// alongside it, read from a SINGLE bbolt snapshot.
//
// The pairing is the entire point, and it is why there is no exported
// hash-only reader. Reading the payload and its hash in two transactions lets an
// /update's Put commit between them, handing the caller the OLD payload and the
// NEW hash. That pair does not verify, and the caller's response to a
// verification failure is to delete the payload as corrupt — destroying the
// update that had just been persisted, and then closing the ACTIVE lease
// on-chain on the following sweep. A bbolt read transaction pins the meta page
// at Begin, so every bucket it reads comes from one committed state and the
// payload/hash pair is always self-consistent.
//
// Returns:
//   - (nil, nil, nil) when the lease has no stored payload.
//   - (payload, nil, nil) when a payload exists but no hash was recorded — a
//     payload written before the hash bucket existed. Callers fall back to the
//     on-chain MetaHash for these, and must never read the absent hash as a
//     mismatch: that would delete a legitimate payload and close a live lease.
//   - a non-nil error when the read fails OR the recorded hash is present but is
//     not a SHA-256. A corrupt checksum is a failed read, not an absence —
//     reporting it as absence would route the caller into the MetaHash fallback,
//     which an updated payload legitimately fails.
func (s *Store) GetWithHash(leaseUUID string) ([]byte, []byte, error) {
	key := []byte(leaseUUID)
	var payload, hash []byte

	err := s.view(func(tx *bolt.Tx) error {
		if data := tx.Bucket(payloadBucketName).Get(key); data != nil {
			// Make a copy since bbolt data is only valid within the transaction
			payload = make([]byte, len(data))
			copy(payload, data)
		}

		data := tx.Bucket(payloadHashBucketName).Get(key)
		if data == nil {
			// No hash recorded: the legitimate pre-ENG-619 case.
			return nil
		}
		if len(data) != HashSize {
			return fmt.Errorf("recorded hash is %d bytes, want %d", len(data), HashSize)
		}
		hash = make([]byte, len(data))
		copy(hash, data)
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to get payload and hash for %s: %w", leaseUUID, err)
	}

	return payload, hash, nil
}

// Get retrieves a payload for a lease without removing it.
// Returns (nil, nil) if no payload exists.
// Returns a non-nil error if the database read fails — callers must not treat
// errors the same as "not found" to avoid closing active leases on transient
// disk failures.
func (s *Store) Get(leaseUUID string) ([]byte, error) {
	key := []byte(leaseUUID)
	var payload []byte

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		data := b.Get(key)
		if data != nil {
			// Make a copy since bbolt data is only valid within the transaction
			payload = make([]byte, len(data))
			copy(payload, data)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get payload for %s: %w", leaseUUID, err)
	}

	return payload, nil
}

// Pop retrieves and removes a payload for a lease.
// Returns nil if no payload exists.
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *Store) Pop(leaseUUID string) []byte {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opPop,
		key:      leaseUUID,
		resultCh: resultCh,
	}

	select {
	case s.writeCh <- op:
	case <-s.ctx.Done():
		slog.Warn("payload store closed, cannot pop", "lease_uuid", leaseUUID)
		return nil
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			slog.Error("failed to pop payload", "lease_uuid", leaseUUID, "error", result.err)
			return nil
		}
		if result.payload != nil {
			metrics.PayloadStoredCount.Dec()
		}
		return result.payload
	case <-s.ctx.Done():
		slog.Warn("payload store closed during pop", "lease_uuid", leaseUUID)
		return nil
	}
}

// Has checks if a payload exists for a lease.
// Returns an error if the database read fails — callers must not treat
// errors the same as "not found" to avoid incorrect provisioning decisions.
func (s *Store) Has(leaseUUID string) (bool, error) {
	key := []byte(leaseUUID)
	var exists bool

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		exists = b.Get(key) != nil
		return nil
	})

	if err != nil {
		return false, fmt.Errorf("failed to check payload for %s: %w", leaseUUID, err)
	}

	return exists, nil
}

// Delete removes a payload for a lease.
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *Store) Delete(leaseUUID string) {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opDelete,
		key:      leaseUUID,
		resultCh: resultCh,
	}

	select {
	case s.writeCh <- op:
	case <-s.ctx.Done():
		slog.Warn("payload store closed, cannot delete", "lease_uuid", leaseUUID)
		return
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			slog.Error("failed to delete payload", "lease_uuid", leaseUUID, "error", result.err)
			return
		}
		if result.existed {
			metrics.PayloadStoredCount.Dec()
		}
	case <-s.ctx.Done():
		slog.Warn("payload store closed during delete", "lease_uuid", leaseUUID)
	}
}

// Count returns the number of stored payloads.
func (s *Store) Count() int {
	count, err := s.count()
	if err != nil {
		slog.Error("failed to count payloads", "error", err)
		return 0
	}
	return count
}

// count preserves a read or authority failure for constructor callers, which
// must not publish a Store whose initial database state cannot be proved safe.
// Count is the compatibility wrapper for callers that cannot return an error.
func (s *Store) count() (int, error) {
	var count int

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		count = b.Stats().KeyN
		return nil
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

// List returns all lease UUIDs that have stored payloads.
// This is used by the reconciler to check for orphaned payloads.
func (s *Store) List() []string {
	var leaseUUIDs []string

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			leaseUUIDs = append(leaseUUIDs, string(k))
		}
		return nil
	})

	if err != nil {
		slog.Error("failed to list payloads", "error", err)
		return nil
	}

	return leaseUUIDs
}

// Healthy checks if the bbolt database is accessible and both buckets exist.
func (s *Store) Healthy() error {
	return s.view(func(tx *bolt.Tx) error {
		if tx.Bucket(payloadBucketName) == nil {
			return errors.New("payload bucket missing")
		}
		if tx.Bucket(payloadMetaBucketName) == nil {
			return errors.New("payload metadata bucket missing")
		}
		if tx.Bucket(payloadHashBucketName) == nil {
			return errors.New("payload hash bucket missing")
		}
		return nil
	})
}

// Close shuts down the payload store gracefully.
// It waits for all pending writes to complete before closing the database.
// Close is idempotent and safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		// Signal shutdown - this will cause writerLoop to exit
		s.cancel()

		// Wait for all goroutines to finish (writer will flush pending ops)
		s.wg.Wait()

		dbErr := s.db.Close()
		var directoryErr error
		if s.authorityDirectory != nil {
			directoryErr = s.authorityDirectory.Close()
		}
		s.closeErr = errors.Join(dbErr, directoryErr)
	})
	return s.closeErr
}

// writerLoop is the dedicated writer goroutine that batches write operations.
// All write operations (Store, Pop, Delete) are serialized through this goroutine
// to eliminate bbolt lock contention.
// Note: WaitGroup.Done is handled by the caller via wg.Go() (Go 1.25+).
func (s *Store) writerLoop(ctx context.Context) {

	batch := make([]writeOp, 0, s.batchSize)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Collect results to send after transaction commits
		results := make([]writeResult, len(batch))

		// Process all operations in a single transaction.
		// Individual operation errors are recorded per-result but don't cause
		// transaction rollback - each lease's payload is independent, so one
		// failed operation shouldn't block unrelated operations in the batch.
		err := s.update(func(tx *bolt.Tx) error {
			payloadBucket := tx.Bucket(payloadBucketName)
			metaBucket := tx.Bucket(payloadMetaBucketName)
			hashBucket := tx.Bucket(payloadHashBucketName)

			for i := range batch {
				op := &batch[i]
				key := []byte(op.key)

				switch op.opType {
				case opStore:
					// Check if payload already exists
					if payloadBucket.Get(key) != nil {
						results[i] = writeResult{stored: false}
						continue
					}
					// Store payload, metadata and the payload's own hash
					if err := payloadBucket.Put(key, op.payload); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Put(key, util.TimeToBytes(op.time)); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := hashBucket.Put(key, op.hash); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					results[i] = writeResult{stored: true}

				case opPut:
					// Unconditional overwrite. Payload, metadata and hash move
					// together in this one transaction, so a reader can never
					// see a new payload against an old hash.
					existed := payloadBucket.Get(key) != nil
					if err := payloadBucket.Put(key, op.payload); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Put(key, util.TimeToBytes(op.time)); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := hashBucket.Put(key, op.hash); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					results[i] = writeResult{stored: true, existed: existed}

				case opPop:
					data := payloadBucket.Get(key)
					if data == nil {
						results[i] = writeResult{payload: nil}
						continue
					}
					// Make a copy before deleting
					payload := make([]byte, len(data))
					copy(payload, data)
					// Delete from all three buckets
					if err := payloadBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := hashBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					results[i] = writeResult{payload: payload}

				case opDelete:
					existed := payloadBucket.Get(key) != nil
					if err := payloadBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := hashBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					results[i] = writeResult{existed: existed}
				}
			}
			return nil
		})

		// Send results after transaction commits (or fails)
		if err != nil {
			slog.Error("batch write failed", "error", err, "batch_size", len(batch))
			// Transaction failed - send error to all waiters
			for i := range batch {
				batch[i].resultCh <- writeResult{err: err}
			}
		} else {
			// Transaction succeeded - send collected results
			for i := range batch {
				batch[i].resultCh <- results[i]
			}
		}

		// Clear the batch
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain all remaining operations from channel before exiting.
			// This prevents callers from blocking forever on resultCh.
		drain:
			for {
				select {
				case op := <-s.writeCh:
					batch = append(batch, op)
				default:
					break drain
				}
			}
			// Flush all collected operations (batch + drained from channel)
			flush()
			return

		case op := <-s.writeCh:
			batch = append(batch, op)
			if len(batch) >= s.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}
