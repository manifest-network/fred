package shared

import (
	"context"
	"errors"
)

// journalPair is the single private construction and per-lease transition
// boundary shared by the phase-specific operation and maintenance services.
// Keeping it private prevents callers from confusing their distinct proof
// types while ensuring both services enforce identical journal lineage.
type journalPair struct {
	callbacks *CallbackStore
	releases  *ReleaseStore
}

func newJournalPair(callbacks *CallbackStore, releases *ReleaseStore) (journalPair, error) {
	if callbacks == nil || releases == nil ||
		!boltStoreIsOpen(callbacks.boltStore) || !boltStoreIsOpen(releases.boltStore) ||
		callbacks.binding == nil || releases.binding == nil ||
		callbacks.backendAuthorityGate == nil ||
		callbacks.backendAuthorityGate != releases.backendAuthorityGate ||
		callbacks.binding.backendName != releases.binding.backendName ||
		callbacks.binding.storageID != releases.binding.storageID {
		return journalPair{}, errors.New(
			"settlement requires exact identity-bound callback and release journals",
		)
	}
	return journalPair{callbacks: callbacks, releases: releases}, nil
}

func (pair journalPair) valid() bool {
	return pair.callbacks != nil && pair.releases != nil &&
		boltStoreIsOpen(pair.callbacks.boltStore) && boltStoreIsOpen(pair.releases.boltStore)
}

// boltStoreIsOpen is the shared lifetime predicate for capabilities which bind
// exact store instances. Pointer identity alone is insufficient after Close:
// a value minted by that instance must become invalid before another method
// reaches bbolt's closed database handle.
func boltStoreIsOpen(store *boltStore) bool {
	return store != nil && store.db != nil && store.ctx != nil && store.ctx.Err() == nil
}

// retentionStoreIsOpen is the shared lifetime predicate for authorities which
// add the independently transacted retention journal to a callback/release
// pair. Requiring its identity binding and backend gate here keeps constructors
// and later consumers from treating a closed or unbound store as live
// authority.
func retentionStoreIsOpen(store *RetentionStore) bool {
	return store != nil && boltStoreIsOpen(store.boltStore) && store.binding != nil &&
		store.backendAuthorityGate != nil
}

func (pair journalPair) lockLease(leaseUUID string) func() {
	return pair.callbacks.lockDeliveryLease(leaseUUID)
}

func (pair journalPair) lockLeaseContext(ctx context.Context, leaseUUID string) (func(), error) {
	return pair.callbacks.lockDeliveryLeaseContext(ctx, leaseUUID)
}

func (pair journalPair) tryLockLease(leaseUUID string) (func(), bool) {
	return pair.callbacks.tryLockDeliveryLease(leaseUUID)
}
