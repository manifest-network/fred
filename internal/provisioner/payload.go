package provisioner

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
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

const (
	// DefaultPayloadTTL is the default time-to-live for stored payloads.
	// Payloads older than this are considered stale and will be cleaned up.
	DefaultPayloadTTL = 1 * time.Hour

	// DefaultPayloadCleanupInterval is how often to clean up stale payloads.
	DefaultPayloadCleanupInterval = 10 * time.Minute
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
	opCleanup
)

// writeOp represents a write operation to be batched.
type writeOp struct {
	opType   writeOpType
	key      string
	payload  []byte
	time     time.Time    // For store operations
	resultCh chan writeResult
}

// writeResult is returned to callers after their operation completes.
type writeResult struct {
	stored  bool   // For Store: whether the payload was stored (false if already existed)
	payload []byte // For Pop: the retrieved payload
	existed bool   // For Delete: whether the key existed
	err     error
}

// PayloadStore stores pending payloads for leases awaiting provisioning.
// Payloads are persisted to bbolt to survive restarts.
// The chain's MetaHash remains the source of truth for validation.
//
// Write operations are batched through a dedicated writer goroutine to reduce
// bbolt lock contention under high concurrency.
type PayloadStore struct {
	db              *bolt.DB
	ttl             time.Duration
	cleanupInterval time.Duration

	// Write batching
	writeCh       chan writeOp
	batchSize     int
	flushInterval time.Duration

	// For graceful shutdown
	cancel context.CancelFunc
	wg     *sync.WaitGroup // Pointer to avoid copy-by-value issues
}

// PayloadStoreConfig configures the payload store.
type PayloadStoreConfig struct {
	DBPath          string        // Path to bbolt database file
	TTL             time.Duration // How long to keep payloads before cleanup (default: 1 hour)
	CleanupInterval time.Duration // How often to clean up stale payloads (default: 10 minutes)
	BatchSize       int           // Max operations per batch (default: 50)
	FlushInterval   time.Duration // Max wait before flushing batch (default: 50ms)
}

// NewPayloadStore creates a new payload store with bbolt persistence.
func NewPayloadStore(cfg PayloadStoreConfig) (*PayloadStore, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}

	// Apply defaults
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultPayloadTTL
	}
	cleanupInterval := cfg.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = DefaultPayloadCleanupInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = DefaultFlushInterval
	}

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
		db.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &PayloadStore{
		db:              db,
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		writeCh:         make(chan writeOp, writeChannelSize),
		batchSize:       batchSize,
		flushInterval:   flushInterval,
		cancel:          cancel,
		wg:              &sync.WaitGroup{},
	}

	// Start the batching writer goroutine
	s.wg.Add(1)
	go s.writerLoop(ctx)

	// Start background cleanup
	s.wg.Add(1)
	go s.cleanupLoop(ctx)

	// Run initial cleanup to remove any stale entries from previous run
	if err := s.cleanupDirect(); err != nil {
		slog.Warn("initial payload cleanup failed", "error", err)
	}

	// Initialize the stored count metric based on current database state
	metrics.PayloadStoredCount.Set(float64(s.Count()))

	slog.Info("payload store initialized",
		"db_path", cfg.DBPath,
		"ttl", ttl,
		"cleanup_interval", cleanupInterval,
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
func (s *PayloadStore) Store(leaseUUID string, payload []byte) bool {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opStore,
		key:      leaseUUID,
		payload:  payload,
		time:     time.Now(),
		resultCh: resultCh,
	}

	s.writeCh <- op
	result := <-resultCh

	if result.err != nil {
		slog.Error("failed to store payload", "lease_uuid", leaseUUID, "error", result.err)
		return false
	}

	if result.stored {
		metrics.PayloadStoredCount.Inc()
	}

	return result.stored
}

// Get retrieves a payload for a lease without removing it.
// Returns nil if no payload exists.
func (s *PayloadStore) Get(leaseUUID string) []byte {
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
		slog.Error("failed to get payload", "lease_uuid", leaseUUID, "error", err)
		return nil
	}

	return payload
}

// Pop retrieves and removes a payload for a lease.
// Returns nil if no payload exists.
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *PayloadStore) Pop(leaseUUID string) []byte {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opPop,
		key:      leaseUUID,
		resultCh: resultCh,
	}

	s.writeCh <- op
	result := <-resultCh

	if result.err != nil {
		slog.Error("failed to pop payload", "lease_uuid", leaseUUID, "error", result.err)
		return nil
	}

	if result.payload != nil {
		metrics.PayloadStoredCount.Dec()
	}

	return result.payload
}

// Has checks if a payload exists for a lease.
func (s *PayloadStore) Has(leaseUUID string) bool {
	key := []byte(leaseUUID)
	var exists bool

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(payloadBucketName)
		exists = b.Get(key) != nil
		return nil
	})

	if err != nil {
		slog.Error("failed to check payload", "lease_uuid", leaseUUID, "error", err)
		return false
	}

	return exists
}

// Delete removes a payload for a lease.
//
// Note: This method blocks until the write completes. If the internal write queue
// is full (>1000 pending operations), it will block until space is available.
// This provides backpressure under extreme load. Callers should not hold locks
// when calling this method.
func (s *PayloadStore) Delete(leaseUUID string) {
	resultCh := make(chan writeResult, 1)

	op := writeOp{
		opType:   opDelete,
		key:      leaseUUID,
		resultCh: resultCh,
	}

	s.writeCh <- op
	result := <-resultCh

	if result.err != nil {
		slog.Error("failed to delete payload", "lease_uuid", leaseUUID, "error", result.err)
		return
	}

	if result.existed {
		metrics.PayloadStoredCount.Dec()
	}
}

// Count returns the number of stored payloads.
func (s *PayloadStore) Count() int {
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
func (s *PayloadStore) List() []string {
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

// Close shuts down the payload store gracefully.
// It waits for all pending writes to complete before closing the database.
func (s *PayloadStore) Close() error {
	// Signal shutdown - this will cause writerLoop and cleanupLoop to exit
	s.cancel()

	// Wait for all goroutines to finish (writer will flush pending ops)
	s.wg.Wait()

	return s.db.Close()
}

// writerLoop is the dedicated writer goroutine that batches write operations.
// All write operations (Store, Pop, Delete) are serialized through this goroutine
// to eliminate bbolt lock contention.
func (s *PayloadStore) writerLoop(ctx context.Context) {
	defer s.wg.Done()

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
			// Flush any pending operations before exiting
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

// cleanupLoop periodically removes stale payloads.
func (s *PayloadStore) cleanupLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.cleanupDirect(); err != nil {
				slog.Error("payload cleanup failed", "error", err)
			}
		}
	}
}

// cleanupDirect removes stale payloads from the database.
// This is called directly (not through the batching writer) since cleanup
// is already serialized through cleanupLoop and runs infrequently.
func (s *PayloadStore) cleanupDirect() error {
	now := time.Now()
	cutoff := now.Add(-s.ttl)
	var staleCount int

	err := s.db.Update(func(tx *bolt.Tx) error {
		payloadBucket := tx.Bucket(payloadBucketName)
		metaBucket := tx.Bucket(payloadMetaBucketName)
		c := metaBucket.Cursor()

		// Collect keys to delete
		var toDelete [][]byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			storedAt := util.BytesToTime(v)
			if storedAt.Before(cutoff) {
				// Make a copy of the key since cursor reuses the slice
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				toDelete = append(toDelete, keyCopy)
			}
		}

		// Delete stale entries from both buckets
		for _, k := range toDelete {
			if err := payloadBucket.Delete(k); err != nil {
				return err
			}
			if err := metaBucket.Delete(k); err != nil {
				return err
			}
		}

		staleCount = len(toDelete)
		return nil
	})

	if err == nil && staleCount > 0 {
		metrics.PayloadStoredCount.Sub(float64(staleCount))
		slog.Info("cleaned up stale payloads", "count", staleCount)
	}

	return err
}

// PayloadEvent represents a payload upload event for Watermill.
type PayloadEvent struct {
	LeaseUUID   string `json:"lease_uuid"`
	Tenant      string `json:"tenant"`
	MetaHashHex string `json:"meta_hash_hex"` // Hex-encoded SHA-256
}

// VerifyPayloadHash computes the SHA-256 hash of payload and compares it against expectedHash
// using constant-time comparison to prevent timing attacks.
// Returns nil if the hash matches, otherwise returns an error with the mismatched hashes.
func VerifyPayloadHash(payload, expectedHash []byte) error {
	if len(expectedHash) == 0 {
		return errors.New("expected hash is empty")
	}

	actualHash := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(actualHash[:], expectedHash) != 1 {
		return &HashMismatchError{
			Expected: expectedHash,
			Actual:   actualHash[:],
		}
	}
	return nil
}

// VerifyPayloadHashHex computes the SHA-256 hash of payload and compares it against expectedHashHex
// (a hex-encoded hash string) using constant-time comparison.
// Returns nil if the hash matches, otherwise returns an error.
func VerifyPayloadHashHex(payload []byte, expectedHashHex string) error {
	expectedHash, err := hex.DecodeString(expectedHashHex)
	if err != nil {
		return fmt.Errorf("invalid expected hash hex: %w", err)
	}
	return VerifyPayloadHash(payload, expectedHash)
}

// HashMismatchError is returned when a payload hash doesn't match the expected hash.
type HashMismatchError struct {
	Expected []byte
	Actual   []byte
}

func (e *HashMismatchError) Error() string {
	return fmt.Sprintf("payload hash mismatch: expected %s, got %s",
		hex.EncodeToString(e.Expected), hex.EncodeToString(e.Actual))
}
