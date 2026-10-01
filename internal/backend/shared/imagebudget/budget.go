// Package imagebudget keeps immutable image verification work and physical
// daemon allocation in separate dimensions across preparation and durable pins.
package imagebudget

import (
	"errors"
	"math"
)

// VerificationBudget bounds compressed staging and decoded verification work.
// It is deliberately distinct from a daemon's physical allocation allowance.
type VerificationBudget struct{ bytes int64 }

// NewVerificationBudget admits a configured or independently capacity-derived
// verification ceiling. Saved evidence is reconstructed by Decode.
func NewVerificationBudget(bytes int64) (VerificationBudget, error) {
	if bytes <= 0 || bytes > math.MaxInt64/8 {
		return VerificationBudget{}, errors.New("image verification requires a bounded positive byte limit")
	}
	return VerificationBudget{bytes: bytes}, nil
}

func (b VerificationBudget) Bytes() int64 { return b.bytes }
func (b VerificationBudget) Valid() bool  { return b.bytes > 0 && b.bytes <= math.MaxInt64/8 }

// ImportAllocation bounds daemon archive, extraction and retained metadata.
type ImportAllocation struct{ allocatedBytes int64 }

func (a ImportAllocation) Bytes() int64 { return a.allocatedBytes }

// Budget keeps both dimensions together. A legacy allocation without a
// verification bound cannot authorize recovery decoding.
type Budget struct {
	verification VerificationBudget
	allocation   ImportAllocation
}

func (b Budget) Verification() VerificationBudget { return b.verification }
func (b Budget) Allocation() ImportAllocation     { return b.allocation }

// Verified constructs the envelope after complete verification of the exact
// image. The preparation issuer must supply a ceiling covering both staged and
// decoded bytes, including its two-allowance physical admission ceiling.
func Verified(verification VerificationBudget, allocationBytes int64) (Budget, error) {
	if !verification.Valid() || allocationBytes <= 0 || allocationBytes > 2*verification.Bytes() {
		return Budget{}, errors.New("invalid verified image byte allowances")
	}
	return Budget{verification: verification, allocation: ImportAllocation{allocatedBytes: allocationBytes}}, nil
}

// Stored is a durable codec DTO, never a preparation or import capability.
// The pin codec binds its fields to the same immutable image and platform.
type Stored struct {
	VerificationBytes int64
	ImportBytes       int64
}

// Decode reconstructs a durable envelope. An absent verification field grants
// no decoding authority. Independently merged bounds need not retain a ratio:
// legacy allocation can be larger without supplying any verification evidence.
func Decode(stored Stored) (Budget, error) {
	if stored.VerificationBytes < 0 || stored.VerificationBytes > math.MaxInt64/8 || stored.ImportBytes < 0 ||
		(stored.VerificationBytes > 0 && stored.ImportBytes == 0) {
		return Budget{}, errors.New("invalid saved image byte allowances")
	}
	return Budget{verification: VerificationBudget{bytes: stored.VerificationBytes}, allocation: ImportAllocation{allocatedBytes: stored.ImportBytes}}, nil
}

// Merge preserves independently established bounds for the same image. Its
// caller must first prove identical immutable content and platform. Legacy
// physical bytes never enlarge the verification dimension.
func (b Budget) Merge(other Budget) Budget {
	return Budget{
		verification: VerificationBudget{bytes: max(b.verification.bytes, other.verification.bytes)},
		allocation:   ImportAllocation{allocatedBytes: max(b.allocation.allocatedBytes, other.allocation.allocatedBytes)},
	}
}
