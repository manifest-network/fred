package shared

import (
	"errors"
	"fmt"
)

// maintenanceContention is issued only by the journal after validating a live
// competing mutation head. Generic conflicts and failed authority checks do not
// grant this availability diagnostic.
type maintenanceContention struct{ leaseUUID string }

func (e *maintenanceContention) Error() string {
	return fmt.Sprintf("%s for lease %q: admitted lifecycle work remains pending", ErrMaintenanceIntentConflict, e.leaseUUID)
}

func (*maintenanceContention) Unwrap() error { return ErrMaintenanceIntentConflict }

// IsLifecyclePending reports an exact journal/physical observation, never a
// refusal, terminal result, or permission to replace the outstanding request.
func IsLifecyclePending(err error) bool {
	var contention *maintenanceContention
	if errors.As(err, &contention) {
		return contention != nil && contention.leaseUUID != ""
	}
	var closePending CloseExecutionPending
	return errors.As(err, &closePending) && closePending.observable && closePending.Valid()
}
