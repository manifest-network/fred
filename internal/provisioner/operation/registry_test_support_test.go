package operation

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/google/uuid"
)

var errOperationIDSequenceExhausted = errors.New("operation ID sequence exhausted")

func newRegistry(operationSeed, claimSeed uint64) *Registry {
	return newRegistryWithObserver(operationSeed, claimSeed, nil)
}

func newRegistryWithObserver(
	operationSeed, claimSeed uint64,
	observer func(int),
) *Registry {
	registry := newRegistryBase(claimSeed, observer)
	// Deterministic allocation exists only for package tests. Production
	// constructors install randomOperationID above so an ID observed by one
	// backend reveals nothing about another backend's current or future ID.
	registry.operationIDSource = func() (OperationID, error) {
		if operationSeed == math.MaxUint64 {
			return OperationID{}, errOperationIDSequenceExhausted
		}
		operationSeed++
		return deterministicOperationID(operationSeed), nil
	}
	return registry
}

// deterministicOperationID is used only by package-private deterministic test
// construction. Production constructors always install randomOperationID.
func deterministicOperationID(sequence uint64) OperationID {
	var input [8]byte
	binary.BigEndian.PutUint64(input[:], sequence)
	digest := sha256.Sum256(input[:])
	var value uuid.UUID
	copy(value[:], digest[:len(value)])
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return newOperationID(value)
}
