package provisioner

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
)

// Retry configuration for bbolt operations under contention.
const (
	bboltMaxRetries     = 3
	bboltInitialBackoff = 10 * time.Millisecond
	bboltMaxBackoff     = 100 * time.Millisecond
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

// PayloadStore stores pending payloads for leases awaiting provisioning.
// Payloads are persisted to bbolt to survive restarts.
// The chain's MetaHash remains the source of truth for validation.
type PayloadStore struct {
	db              *bolt.DB
	ttl             time.Duration
	cleanupInterval time.Duration

	// For graceful shutdown
	cancel context.CancelFunc
	wg     *sync.WaitGroup // Pointer to avoid copy-by-value issues
}

// PayloadStoreConfig configures the payload store.
type PayloadStoreConfig struct {
	DBPath          string        // Path to bbolt database file
	TTL             time.Duration // How long to keep payloads before cleanup (default: 1 hour)
	CleanupInterval time.Duration // How often to clean up stale payloads (default: 10 minutes)
}

// NewPayloadStore creates a new payload store with bbolt persistence.
func NewPayloadStore(cfg PayloadStoreConfig) (*PayloadStore, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("DBPath is required")
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
		cancel:          cancel,
		wg:              &sync.WaitGroup{},
	}

	// Start background cleanup
	s.wg.Add(1)
	go s.cleanupLoop(ctx)

	// Run initial cleanup to remove any stale entries from previous run
	if err := s.cleanup(); err != nil {
		slog.Warn("initial payload cleanup failed", "error", err)
	}

	// Initialize the stored count metric based on current database state
	metrics.PayloadStoredCount.Set(float64(s.Count()))

	slog.Info("payload store initialized",
		"db_path", cfg.DBPath,
		"ttl", ttl,
		"cleanup_interval", cleanupInterval,
		"initial_count", s.Count(),
	)

	return s, nil
}

// retryBboltUpdate retries a bbolt Update operation with exponential backoff
// on timeout errors (database contention). Returns the error from the last attempt.
func retryBboltUpdate(db *bolt.DB, fn func(tx *bolt.Tx) error) error {
	var err error
	backoff := bboltInitialBackoff

	for attempt := 1; attempt <= bboltMaxRetries; attempt++ {
		err = db.Update(fn)

		if err == nil {
			return nil
		}

		// Only retry on timeout errors (database locked)
		if !errors.Is(err, bolt.ErrTimeout) && !errors.Is(err, bolt.ErrDatabaseNotOpen) {
			return err
		}

		if attempt < bboltMaxRetries {
			slog.Debug("bbolt timeout, retrying",
				"attempt", attempt,
				"backoff", backoff,
				"error", err,
			)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > bboltMaxBackoff {
				backoff = bboltMaxBackoff
			}
		}
	}

	return err
}

// Store stores a payload for a lease.
// Returns false if a payload already exists for this lease (conflict).
func (s *PayloadStore) Store(leaseUUID string, payload []byte) bool {
	key := []byte(leaseUUID)
	now := time.Now()

	var stored bool
	err := retryBboltUpdate(s.db, func(tx *bolt.Tx) error {
		payloadBucket := tx.Bucket(payloadBucketName)
		metaBucket := tx.Bucket(payloadMetaBucketName)

		// Check if payload already exists
		if payloadBucket.Get(key) != nil {
			stored = false
			return nil
		}

		// Store payload
		if err := payloadBucket.Put(key, payload); err != nil {
			return err
		}

		// Store timestamp for TTL tracking
		if err := metaBucket.Put(key, timeToBytes(now)); err != nil {
			return err
		}

		stored = true
		return nil
	})

	if err != nil {
		slog.Error("failed to store payload", "lease_uuid", leaseUUID, "error", err)
		return false
	}

	if stored {
		metrics.PayloadStoredCount.Inc()
	}

	return stored
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
func (s *PayloadStore) Pop(leaseUUID string) []byte {
	key := []byte(leaseUUID)
	var payload []byte

	err := retryBboltUpdate(s.db, func(tx *bolt.Tx) error {
		payloadBucket := tx.Bucket(payloadBucketName)
		metaBucket := tx.Bucket(payloadMetaBucketName)

		data := payloadBucket.Get(key)
		if data == nil {
			return nil
		}

		// Make a copy before deleting
		payload = make([]byte, len(data))
		copy(payload, data)

		// Delete from both buckets
		if err := payloadBucket.Delete(key); err != nil {
			return err
		}
		if err := metaBucket.Delete(key); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		slog.Error("failed to pop payload", "lease_uuid", leaseUUID, "error", err)
		return nil
	}

	if payload != nil {
		metrics.PayloadStoredCount.Dec()
	}

	return payload
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
func (s *PayloadStore) Delete(leaseUUID string) {
	key := []byte(leaseUUID)
	var existed bool

	err := retryBboltUpdate(s.db, func(tx *bolt.Tx) error {
		payloadBucket := tx.Bucket(payloadBucketName)
		metaBucket := tx.Bucket(payloadMetaBucketName)

		// Check if it exists before deleting (for metric tracking)
		existed = payloadBucket.Get(key) != nil

		if err := payloadBucket.Delete(key); err != nil {
			return err
		}
		if err := metaBucket.Delete(key); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		slog.Error("failed to delete payload", "lease_uuid", leaseUUID, "error", err)
		return
	}

	if existed {
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

// Close shuts down the payload store gracefully.
func (s *PayloadStore) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.db.Close()
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
			if err := s.cleanup(); err != nil {
				slog.Error("payload cleanup failed", "error", err)
			}
		}
	}
}

// cleanup removes stale payloads from the database.
func (s *PayloadStore) cleanup() error {
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
			storedAt := bytesToTime(v)
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

// timeToBytes converts a time.Time to bytes for storage.
func timeToBytes(t time.Time) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UnixNano()))
	return b
}

// bytesToTime converts bytes back to time.Time.
func bytesToTime(b []byte) time.Time {
	if len(b) != 8 {
		return time.Time{}
	}
	nano := int64(binary.BigEndian.Uint64(b))
	return time.Unix(0, nano)
}

// PayloadEvent represents a payload upload event for Watermill.
type PayloadEvent struct {
	LeaseUUID   string `json:"lease_uuid"`
	Tenant      string `json:"tenant"`
	MetaHashHex string `json:"meta_hash_hex"` // Hex-encoded SHA-256
}
