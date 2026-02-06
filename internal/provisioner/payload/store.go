package payload

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
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
)

// writeOpType represents the type of write operation.
type writeOpType int

const (
	opStore writeOpType = iota
	opDelete
	opPop
)

// writeOp represents a write operation to be batched.
type writeOp struct {
	opType   writeOpType
	key      string
	payload  []byte
	time     time.Time // For store operations
	resultCh chan writeResult
}

// writeResult is returned to callers after their operation completes.
type writeResult struct {
	stored  bool   // For Store: whether the payload was stored (false if already existed)
	payload []byte // For Pop: the retrieved payload
	existed bool   // For Delete: whether the key existed
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

	// Write batching
	writeCh       chan writeOp
	batchSize     int
	flushInterval time.Duration

	// For graceful shutdown
	ctx       context.Context // store-level context; Done channel used by write methods to avoid blocking after Close
	cancel    context.CancelFunc
	wg        *sync.WaitGroup // Pointer to avoid copy-by-value issues
	closeOnce *sync.Once      // Pointer to avoid copy-by-value issues
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

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// Create buckets if they don't exist
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(payloadBucketName); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(payloadMetaBucketName); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		_ = db.Close() // Best effort cleanup on init failure
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{
		db:            db,
		writeCh:       make(chan writeOp, writeChannelSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		ctx:           ctx,
		cancel:        cancel,
		wg:            &sync.WaitGroup{},
		closeOnce:     &sync.Once{},
	}

	// Start the batching writer goroutine (using WaitGroup.Go for Go 1.25+)
	s.wg.Go(func() { s.writerLoop(ctx) })

	// Initialize the stored count metric based on current database state
	metrics.PayloadStoredCount.Set(float64(s.Count()))

	slog.Info("payload store initialized",
		"db_path", cfg.DBPath,
		"batch_size", batchSize,
		"flush_interval", flushInterval,
		"initial_count", s.Count(),
	)

	return s, nil
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

// Get retrieves a payload for a lease without removing it.
// Returns (nil, nil) if no payload exists.
// Returns a non-nil error if the database read fails — callers must not treat
// errors the same as "not found" to avoid closing active leases on transient
// disk failures.
func (s *Store) Get(leaseUUID string) ([]byte, error) {
	key := []byte(leaseUUID)
	var payload []byte

	err := s.db.View(func(tx *bolt.Tx) error {
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

	err := s.db.View(func(tx *bolt.Tx) error {
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
	var count int

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		count = b.Stats().KeyN
		return nil
	})

	if err != nil {
		slog.Error("failed to count payloads", "error", err)
		return 0
	}

	return count
}

// List returns all lease UUIDs that have stored payloads.
// This is used by the reconciler to check for orphaned payloads.
func (s *Store) List() []string {
	var leaseUUIDs []string

	err := s.db.View(func(tx *bolt.Tx) error {
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
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(payloadBucketName) == nil {
			return errors.New("payload bucket missing")
		}
		if tx.Bucket(payloadMetaBucketName) == nil {
			return errors.New("payload metadata bucket missing")
		}
		return nil
	})
}

// Close shuts down the payload store gracefully.
// It waits for all pending writes to complete before closing the database.
// Close is idempotent and safe to call multiple times.
func (s *Store) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		// Signal shutdown - this will cause writerLoop to exit
		s.cancel()

		// Wait for all goroutines to finish (writer will flush pending ops)
		s.wg.Wait()

		closeErr = s.db.Close()
	})
	return closeErr
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
		err := s.db.Update(func(tx *bolt.Tx) error {
			payloadBucket := tx.Bucket(payloadBucketName)
			metaBucket := tx.Bucket(payloadMetaBucketName)

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
					// Store payload and metadata
					if err := payloadBucket.Put(key, op.payload); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Put(key, util.TimeToBytes(op.time)); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					results[i] = writeResult{stored: true}

				case opPop:
					data := payloadBucket.Get(key)
					if data == nil {
						results[i] = writeResult{payload: nil}
						continue
					}
					// Make a copy before deleting
					payload := make([]byte, len(data))
					copy(payload, data)
					// Delete from both buckets
					if err := payloadBucket.Delete(key); err != nil {
						results[i] = writeResult{err: err}
						continue
					}
					if err := metaBucket.Delete(key); err != nil {
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
