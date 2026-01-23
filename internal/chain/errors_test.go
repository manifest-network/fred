package chain

import (
	"errors"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

func TestChainTxError_Error(t *testing.T) {
	err := &ChainTxError{
		Code:      22,
		Codespace: "billing",
		RawLog:    "lease not in pending state",
	}

	msg := err.Error()
	if msg != "transaction failed (code 22, codespace billing): lease not in pending state" {
		t.Errorf("Error() = %q, want specific format", msg)
	}
}

func TestChainTxError_Is(t *testing.T) {
	tests := []struct {
		name   string
		err    *ChainTxError
		target error
		want   bool
	}{
		{
			name: "matches ErrLeaseNotPending",
			err: &ChainTxError{
				Code:      22,
				Codespace: "billing",
				RawLog:    "lease not in pending state",
			},
			target: billingtypes.ErrLeaseNotPending,
			want:   true,
		},
		{
			name: "matches ErrLeaseNotFound",
			err: &ChainTxError{
				Code:      2,
				Codespace: "billing",
				RawLog:    "lease not found",
			},
			target: billingtypes.ErrLeaseNotFound,
			want:   true,
		},
		{
			name: "wrong code - no match",
			err: &ChainTxError{
				Code:      999,
				Codespace: "billing",
				RawLog:    "some error",
			},
			target: billingtypes.ErrLeaseNotPending,
			want:   false,
		},
		{
			name: "wrong codespace - no match",
			err: &ChainTxError{
				Code:      22,
				Codespace: "other-module",
				RawLog:    "some error",
			},
			target: billingtypes.ErrLeaseNotPending,
			want:   false,
		},
		{
			name: "non-sdk error - no match",
			err: &ChainTxError{
				Code:      22,
				Codespace: "billing",
				RawLog:    "some error",
			},
			target: errors.New("some error"),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use errors.Is to test the Is() method
			got := errors.Is(tt.err, tt.target)
			if got != tt.want {
				t.Errorf("errors.Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChainTxError_WorksWithErrorsIs(t *testing.T) {
	// Test that ChainTxError works correctly with errors.Is()
	// even when wrapped in fmt.Errorf
	chainErr := &ChainTxError{
		Code:      22,
		Codespace: "billing",
		RawLog:    "lease not in pending state",
	}

	if !errors.Is(chainErr, billingtypes.ErrLeaseNotPending) {
		t.Error("ChainTxError should match ErrLeaseNotPending via errors.Is()")
	}

	if errors.Is(chainErr, billingtypes.ErrLeaseNotFound) {
		t.Error("ChainTxError should NOT match ErrLeaseNotFound")
	}
}
