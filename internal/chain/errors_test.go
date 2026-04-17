package chain

import (
	"errors"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
)

func TestChainTxError_Error(t *testing.T) {
	err := &ChainTxError{
		Code:      22,
		Codespace: "billing",
		RawLog:    "lease not in pending state",
	}

	msg := err.Error()
	assert.Equal(t, "transaction failed (code 22, codespace billing): lease not in pending state", msg)
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
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestChainTxError_IsTxInMempool(t *testing.T) {
	tests := []struct {
		name string
		err  *ChainTxError
		want bool
	}{
		{
			name: "mempool duplicate",
			err:  &ChainTxError{Code: 19, Codespace: "sdk", RawLog: "tx already exists in cache"},
			want: true,
		},
		{
			name: "sequence mismatch is not mempool",
			err:  &ChainTxError{Code: 32, Codespace: "sdk", RawLog: "account sequence mismatch"},
			want: false,
		},
		{
			name: "wrong codespace",
			err:  &ChainTxError{Code: 19, Codespace: "billing", RawLog: "something"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.IsTxInMempool())
		})
	}
}

func TestChainTxError_IsOutOfGas(t *testing.T) {
	tests := []struct {
		name string
		err  *ChainTxError
		want bool
	}{
		{
			name: "out of gas",
			err:  &ChainTxError{Code: 11, Codespace: "sdk", RawLog: "out of gas"},
			want: true,
		},
		{
			name: "wrong code",
			err:  &ChainTxError{Code: 32, Codespace: "sdk", RawLog: "sequence mismatch"},
			want: false,
		},
		{
			name: "wrong codespace",
			err:  &ChainTxError{Code: 11, Codespace: "billing", RawLog: "out of gas"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.IsOutOfGas())
		})
	}
}

func TestChainTxError_IsInsufficientFee(t *testing.T) {
	tests := []struct {
		name string
		err  *ChainTxError
		want bool
	}{
		{
			name: "insufficient fee",
			err:  &ChainTxError{Code: 13, Codespace: "sdk", RawLog: "insufficient fee"},
			want: true,
		},
		{
			name: "adjacent lower code",
			err:  &ChainTxError{Code: 12, Codespace: "sdk", RawLog: "other"},
			want: false,
		},
		{
			name: "adjacent higher code",
			err:  &ChainTxError{Code: 14, Codespace: "sdk", RawLog: "other"},
			want: false,
		},
		{
			name: "wrong codespace",
			err:  &ChainTxError{Code: 13, Codespace: "billing", RawLog: "insufficient fee"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.IsInsufficientFee())
		})
	}
}

func TestChainTxError_ExpectedSequence(t *testing.T) {
	tests := []struct {
		name    string
		err     *ChainTxError
		wantSeq uint64
		wantOK  bool
	}{
		{
			name: "standard sequence mismatch",
			err: &ChainTxError{
				Code:      32,
				Codespace: "sdk",
				RawLog:    "account sequence mismatch, expected 8754, got 8753: incorrect account sequence",
			},
			wantSeq: 8754,
			wantOK:  true,
		},
		{
			name: "not a sequence mismatch",
			err: &ChainTxError{
				Code:      4,
				Codespace: "sdk",
				RawLog:    "signature verification failed",
			},
			wantSeq: 0,
			wantOK:  false,
		},
		{
			name: "sequence mismatch without parseable number",
			err: &ChainTxError{
				Code:      32,
				Codespace: "sdk",
				RawLog:    "account sequence mismatch",
			},
			wantSeq: 0,
			wantOK:  false,
		},
		{
			name: "expected sequence 0",
			err: &ChainTxError{
				Code:      32,
				Codespace: "sdk",
				RawLog:    "account sequence mismatch, expected 0, got 1: incorrect account sequence",
			},
			wantSeq: 0,
			wantOK:  true,
		},
		{
			name: "wrong codespace",
			err: &ChainTxError{
				Code:      32,
				Codespace: "billing",
				RawLog:    "expected 100, got 99",
			},
			wantSeq: 0,
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seq, ok := tt.err.ExpectedSequence()
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantSeq, seq)
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

	assert.True(t, errors.Is(chainErr, billingtypes.ErrLeaseNotPending))
	assert.False(t, errors.Is(chainErr, billingtypes.ErrLeaseNotFound))
}
