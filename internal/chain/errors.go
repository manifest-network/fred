package chain

import (
	"fmt"

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
