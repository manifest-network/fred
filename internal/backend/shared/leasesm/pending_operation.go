package leasesm

import "github.com/manifest-network/fred/internal/backend/shared"

// PendingOperation names the one provision or restore whose outcome an
// actor-owned projection is waiting for (ENG-1125). The state machine stamps
// it when a provision enters Provisioning and when a restore enters
// Restarting, keeps it through Failed, and clears it when the lease becomes
// Ready or a maintenance replacement starts.
//
// Live operation recovery matches a settled failure to its projection by this
// exact operation, never by the projection's callback pair: a re-provision
// whose predecessor teardown failed still carries the predecessor's pair while
// it waits, and matching by pair left that lease Provisioning forever.
//
// The value is opaque. Only AwaitOperation can stamp it, and only from a
// store-issued operation claim for the same lease, so a substrate cannot make a
// projection appear to await an operation the journal never admitted. The zero
// value names no operation.
type PendingOperation struct{ id shared.OperationID }

// Names reports whether the projection awaits exactly operationID. An invalid
// or zero ID is never named.
func (p PendingOperation) Names(operationID shared.OperationID) bool {
	return operationID.Valid() && p.id == operationID
}

// AwaitOperation stamps p as waiting for claim's exact operation. A claim that
// is invalid or belongs to another lease leaves p unchanged and returns false.
// The state machine calls it on Provisioning (and restore) entry; a substrate
// may call it only to rebuild a projection for a pending operation it has just
// re-read from the durable journal, such as a cold-start recovery overlay.
func (p *ProvisionState) AwaitOperation(claim shared.OperationIntentClaim) bool {
	if !claim.Valid() || !claim.OperationID().Valid() || claim.LeaseUUID() != p.LeaseUUID {
		return false
	}
	p.PendingOperation = PendingOperation{id: claim.OperationID()}
	return true
}

// awaitNoOperation records that no provision or restore is in flight: a
// maintenance replacement started, or the lease reached Ready.
func (p *ProvisionState) awaitNoOperation() {
	p.PendingOperation = PendingOperation{}
}
