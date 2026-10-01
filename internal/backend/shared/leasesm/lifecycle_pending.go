package leasesm

import (
	"errors"
	"fmt"
)

// workerDrainPending is an observation made by the serial actor after it has
// canceled its own worker and found the worker barrier still occupied. It does
// not grant exclusive teardown, nor prove that any daemon request was refused.
// Its private constructor prevents unrelated errors from acquiring that class.
type workerDrainPending struct{ leaseUUID string }

func (pending workerDrainPending) Error() string {
	return fmt.Sprintf("lease %q still has an actor-owned worker draining", pending.leaseUUID)
}

// IsLifecyclePending recognizes only an actor-issued worker observation. The
// observation remains true of the original request after the worker drains;
// the next deprovision request must independently obtain teardown authority.
func IsLifecyclePending(err error) bool {
	var pending workerDrainPending
	return errors.As(err, &pending) && pending.leaseUUID != ""
}
