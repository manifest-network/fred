package placement

import billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

// mayHoldRetainedData is the shared source-state policy for restore admission
// and durable attempt recovery. Its receiver is an exact chain observation:
// missing, failed, or unauthorized reads cannot reach this decision. The zero
// observation and unknown future states are ineligible.
//
// A restore can commit after its destination is rejected or expires. Closing
// that destination retains the data under its own lease, so all three terminal
// states must remain eligible. This grants no retention or dispatch authority:
// placement still binds the owning backend, which checks the actual retained
// data, tenant, retention lifetime, and restore constraints.
func (observed observedExactLease) mayHoldRetainedData() bool {
	switch observed.lease.State {
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_EXPIRED, billingtypes.LEASE_STATE_REJECTED:
		return true
	default:
		return false
	}
}
