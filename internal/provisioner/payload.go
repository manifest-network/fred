package provisioner

import (
	"slices"
	"sync"
)

// PayloadStore stores pending payloads for leases awaiting provisioning.
// Payloads are stored in memory and recovered via reconciliation if lost.
// This is intentionally ephemeral - the chain's MetaHash is the source of truth.
type PayloadStore struct {
	mu       sync.RWMutex
	payloads map[string][]byte // leaseUUID -> payload
}

// NewPayloadStore creates a new payload store.
func NewPayloadStore() *PayloadStore {
	return &PayloadStore{
		payloads: make(map[string][]byte),
	}
}

// Store stores a payload for a lease.
// Returns false if a payload already exists for this lease (conflict).
func (s *PayloadStore) Store(leaseUUID string, payload []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.payloads[leaseUUID]; exists {
		return false
	}

	// Clone to prevent external modification
	s.payloads[leaseUUID] = slices.Clone(payload)
	return true
}

// Get retrieves a payload for a lease without removing it.
// Returns nil if no payload exists.
func (s *PayloadStore) Get(leaseUUID string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	payload, exists := s.payloads[leaseUUID]
	if !exists {
		return nil
	}

	// Return a clone to prevent external modification
	return slices.Clone(payload)
}

// Pop retrieves and removes a payload for a lease.
// Returns nil if no payload exists.
func (s *PayloadStore) Pop(leaseUUID string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload, exists := s.payloads[leaseUUID]
	if !exists {
		return nil
	}

	delete(s.payloads, leaseUUID)
	// No need to clone - we're transferring ownership
	return payload
}

// Has checks if a payload exists for a lease.
func (s *PayloadStore) Has(leaseUUID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.payloads[leaseUUID]
	return exists
}

// Delete removes a payload for a lease.
func (s *PayloadStore) Delete(leaseUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.payloads, leaseUUID)
}

// Count returns the number of stored payloads.
func (s *PayloadStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.payloads)
}

// PayloadEvent represents a payload upload event for Watermill.
type PayloadEvent struct {
	LeaseUUID   string `json:"lease_uuid"`
	Tenant      string `json:"tenant"`
	MetaHashHex string `json:"meta_hash_hex"` // Hex-encoded SHA-256
}
