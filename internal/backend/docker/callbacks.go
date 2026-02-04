package docker

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var callbackBucketName = []byte("pending_callbacks")

// CallbackEntry represents a pending callback to be delivered.
type CallbackEntry struct {
	LeaseUUID   string    `json:"lease_uuid"`
	CallbackURL string    `json:"callback_url"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	db *bolt.DB
}

// NewCallbackStore opens or creates a bbolt database for callback persistence.
func NewCallbackStore(dbPath string) (*CallbackStore, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("callback db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback db: %w", err)
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(callbackBucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create callback bucket: %w", err)
	}

	return &CallbackStore{db: db}, nil
}

// Store persists a callback entry before attempting delivery.
func (s *CallbackStore) Store(entry CallbackEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal callback entry: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		return b.Put([]byte(entry.LeaseUUID), data)
	})
}

// Remove deletes a callback entry after successful delivery.
func (s *CallbackStore) Remove(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// ListPending returns all pending callback entries for replay on startup.
func (s *CallbackStore) ListPending() ([]CallbackEntry, error) {
	var entries []CallbackEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry CallbackEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				continue // Skip malformed entries
			}
			entries = append(entries, entry)
		}
		return nil
	})

	return entries, err
}

// RemoveOlderThan deletes callback entries older than maxAge and returns
// the number of entries removed.
func (s *CallbackStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry CallbackEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				// Remove malformed entries
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
				continue
			}
			if entry.CreatedAt.Before(cutoff) {
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
			}
		}
		return nil
	})

	return removed, err
}

// Close closes the underlying bbolt database.
func (s *CallbackStore) Close() error {
	return s.db.Close()
}
