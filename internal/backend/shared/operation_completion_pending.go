package shared

import (
	"errors"
	"fmt"
)

// operationCompletionPendingError is minted only after the journal has read a
// valid earlier operation completion that still owns this lease's callback FIFO.
// It describes ordinary contention, not corruption or operation settlement.
type operationCompletionPendingError struct{ leaseUUID string }

func (e *operationCompletionPendingError) Error() string {
	return fmt.Sprintf("%s for lease %q: an earlier operation completion is pending", ErrOperationIntentConflict, e.leaseUUID)
}

func (*operationCompletionPendingError) Unwrap() error { return ErrOperationIntentConflict }

// IsOperationCompletionPending identifies the journal-authored FIFO diagnostic.
// It grants no refusal, replay, or lifecycle authority; the requested operation
// remains unresolved until exact callback settlement or reconciliation.
func IsOperationCompletionPending(err error) bool {
	var pending *operationCompletionPendingError
	return errors.As(err, &pending)
}
