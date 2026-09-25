package imagebudget

import (
	"math"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

func TestMergePreservesIndependentDimensions(t *testing.T) {
	property := func(legacyBytes uint32, verifiedBytes uint32) bool {
		legacy, err := Decode(Stored{ImportBytes: int64(legacyBytes)})
		if err != nil {
			return false
		}
		bound, err := NewVerificationBudget(int64(verifiedBytes) + 1)
		if err != nil {
			return false
		}
		fresh, err := Verified(bound, 2*bound.Bytes())
		if err != nil {
			return false
		}
		for _, merged := range []Budget{legacy.Merge(fresh), fresh.Merge(legacy)} {
			if merged.Verification() != fresh.Verification() || merged.Allocation().Bytes() != max(legacy.Allocation().Bytes(), fresh.Allocation().Bytes()) {
				return false
			}
			saved, err := Decode(Stored{VerificationBytes: merged.Verification().Bytes(), ImportBytes: merged.Allocation().Bytes()})
			if err != nil || saved != merged {
				return false
			}
		}
		return !legacy.Merge(Budget{}).Verification().Valid()
	}
	require.NoError(t, quick.Check(property, &quick.Config{MaxCount: 1000}))
}

func TestVerificationAndAllocationRejectInvalidConstruction(t *testing.T) {
	for _, bytes := range []int64{-1, 0, math.MaxInt64/8 + 1, math.MaxInt64} {
		_, err := NewVerificationBudget(bytes)
		require.Error(t, err)
	}
	bound, err := NewVerificationBudget(100)
	require.NoError(t, err)
	for _, bytes := range []int64{-1, 0, 201, math.MaxInt64} {
		_, err := Verified(bound, bytes)
		require.Error(t, err)
	}
	_, err = Verified(VerificationBudget{}, 1)
	require.Error(t, err)
	for _, record := range []Stored{{VerificationBytes: -1}, {ImportBytes: -1}, {VerificationBytes: math.MaxInt64}, {VerificationBytes: 1}} {
		_, err := Decode(record)
		require.Error(t, err)
	}
	legacy, err := Decode(Stored{ImportBytes: math.MaxInt64})
	require.NoError(t, err)
	require.False(t, legacy.Verification().Valid())
	fresh, err := Verified(bound, 1)
	require.NoError(t, err)
	require.Equal(t, int64(100), legacy.Merge(fresh).Verification().Bytes())
}
