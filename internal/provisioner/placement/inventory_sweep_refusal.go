package placement

import "fmt"

// inventorySweepRefusal is the Store's refusal of a lease side effect whose
// only inventory barriers belong to the sweep still collecting. That sweep's
// projection is the next event that can remove them, so the refusal is safe to
// report as retryable (ENG-2161). Nothing was written before it was returned.
//
// unprojectedPositiveErrorLocked is its only minter. A barrier left by an
// earlier projection, or an interrupted-sweep recovery, waits on something
// other than the open sweep and gets the plain ErrUnprojectedInventoryPositive
// instead.
type inventorySweepRefusal struct {
	leaseUUID string
	sweepID   uint64
}

func (refusal inventorySweepRefusal) Error() string {
	return fmt.Sprintf("%s: lease %q awaits the projection of inventory sweep %d",
		ErrUnprojectedInventoryPositive, refusal.leaseUUID, refusal.sweepID)
}

// Unwrap keeps every errors.Is(err, ErrUnprojectedInventoryPositive) reader
// unchanged: the refusal is a narrower statement of the same barrier.
func (inventorySweepRefusal) Unwrap() error { return ErrUnprojectedInventoryPositive }

// inventorySweepPending reports whether err is exactly a refusal the Store
// authored for an open sweep's barrier. A wrapped or joined error is not. As
// with maintenanceReservationRefusal, only the unaltered source refusal proves
// that nothing else on the path failed, so only it may be reported as
// retryable. This is the only place the refusal is recognized.
func inventorySweepPending(err error) bool {
	_, pending := err.(inventorySweepRefusal) //nolint:errorlint // Only the exact source refusal is retryable evidence.
	return pending
}
