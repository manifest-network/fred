package chain

import (
	"fmt"
	"regexp"
	"strconv"

	sdkerrors "cosmossdk.io/errors"
)

// ChainTxError represents an error from a failed chain transaction.
// It captures the ABCI error code and codespace, allowing callers to
// check against specific module errors using errors.Is().
type ChainTxError struct {
	Code      uint32 // ABCI error code
	Codespace string // Module that generated the error
	RawLog    string // Human-readable error message
}

// Error implements the error interface.
func (e *ChainTxError) Error() string {
	return fmt.Sprintf("transaction failed (code %d, codespace %s): %s", e.Code, e.Codespace, e.RawLog)
}

// Is implements errors.Is() interface to check against cosmossdk.io/errors registered errors.
// This allows checking: errors.Is(err, billingtypes.ErrLeaseNotPending)
func (e *ChainTxError) Is(target error) bool {
	// Check if target is a registered cosmos SDK error
	sdkErr, ok := target.(*sdkerrors.Error)
	if !ok {
		return false
	}

	// Match by codespace and code
	return e.Codespace == sdkErr.Codespace() && e.Code == sdkErr.ABCICode()
}

// IsSequenceMismatch returns true if this error is a sequence mismatch (SDK code 32).
func (e *ChainTxError) IsSequenceMismatch() bool {
	return e.Codespace == "sdk" && e.Code == 32
}

// IsTxInMempool returns true if this error indicates the transaction is already
// in the mempool cache (SDK code 19). This happens when waitForTx times out on
// a previous attempt and the retry re-signs with the same sequence, producing
// identical tx bytes whose hash is still cached in the CometBFT mempool.
func (e *ChainTxError) IsTxInMempool() bool {
	return e.Codespace == "sdk" && e.Code == 19
}

// reExpectedSeq extracts "expected N, got M" from Cosmos SDK sequence mismatch errors.
// False positives are prevented by the IsSequenceMismatch() guard in ExpectedSequence(),
// which requires codespace "sdk" and code 32 before the regex is applied.
var reExpectedSeq = regexp.MustCompile(`expected (\d+), got \d+`)

// ExpectedSequence extracts the expected sequence number from a sequence
// mismatch error. Returns 0, false if the error is not a sequence mismatch
// or the expected sequence cannot be parsed.
func (e *ChainTxError) ExpectedSequence() (uint64, bool) {
	if !e.IsSequenceMismatch() {
		return 0, false
	}
	m := reExpectedSeq.FindStringSubmatch(e.RawLog)
	if len(m) < 2 {
		return 0, false
	}
	seq, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}
