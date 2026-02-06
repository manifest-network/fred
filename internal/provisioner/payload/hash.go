package payload

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
)

// VerifyHash computes the SHA-256 hash of payload and compares it against expectedHash
// using constant-time comparison to prevent timing attacks.
// Returns nil if the hash matches, otherwise returns an error with the mismatched hashes.
func VerifyHash(payload, expectedHash []byte) error {
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

// VerifyHashHex computes the SHA-256 hash of payload and compares it against expectedHashHex
// (a hex-encoded hash string) using constant-time comparison.
// Returns nil if the hash matches, otherwise returns an error.
func VerifyHashHex(payload []byte, expectedHashHex string) error {
	expectedHash, err := hex.DecodeString(expectedHashHex)
	if err != nil {
		return fmt.Errorf("invalid expected hash hex: %w", err)
	}
	return VerifyHash(payload, expectedHash)
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
