package shared

import (
	"context"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// ResolveOperationIntent is a test-only compatibility seam for historical
// callback-journal tests that intentionally exercise the aggregate transition
// in isolation from the release journal. Production does not compile this raw
// status-selected method; cross-journal behavior is covered by the typed
// operation_handoff tests.
func (s *CallbackStore) ResolveOperationIntent(
	claim OperationIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	durable, err := validateOperationIntentClaim(claim)
	if err != nil {
		return CallbackEntry{}, err
	}
	var entry CallbackEntry
	failurePredecessor := operationFailurePredecessorRecord{}
	switch status {
	case backend.CallbackStatusSuccess:
		entry = operationSuccessCallbackEntry(*durable.entry)
	case backend.CallbackStatusFailed:
		entry = operationFailureCallbackEntry(*durable.entry, errMsg)
		// This test-only aggregate seam has no ReleaseStore to inspect. Make
		// absence explicit instead of manufacturing or omitting predecessor
		// authority; production failure settlement always captures the pair.
		failurePredecessor = operationFailurePredecessorRecord{
			Kind: operationFailurePredecessorAbsent,
		}
	default:
		return CallbackEntry{}, fmt.Errorf("invalid callback status %q", status)
	}
	unlock := s.lockDeliveryLease(durable.LeaseUUID())
	defer unlock()
	return s.resolveOperationIntentLocked(durable, entry, failurePredecessor)
}

// settleOperationCallbackLocked is the test-only raw aggregate seam used by
// close-journal isolation tests. Production settlement always crosses the
// paired OperationSettlement proof boundary.
func (s *CallbackStore) settleOperationCallbackLocked(entry CallbackEntry) error {
	var claim OperationIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, entry.LeaseUUID)
		if err != nil {
			return err
		}
		operation, ok := head.(operationLeaseMutationHead)
		if !present || !ok {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, entry.LeaseUUID)
		}
		claim = operation.claim
		return nil
	})
	if err != nil {
		return err
	}
	if claim.entry.State != operationIntentPending {
		return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, entry.LeaseUUID)
	}
	failurePredecessor := operationFailurePredecessorRecord{}
	if entry.Status == backend.CallbackStatusFailed {
		failurePredecessor.Kind = operationFailurePredecessorAbsent
	}
	_, err = s.resolveOperationIntentLocked(claim, entry, failurePredecessor)
	return err
}

// These sender methods exist only in the shared package's tests. Durable
// success without a committed proof remains rejected; ephemeral delivery tests
// can keep exercising wire behavior without manufacturing durable authority.
func (s *CallbackSender) sendOperationCallbackForTest(
	leaseUUID, callbackURL, backendName string,
	status backend.CallbackStatus,
	errMsg string,
) {
	s.sendLegacyTestCallback(
		context.Background(), leaseUUID, callbackURL, backendName, status, errMsg,
		false, CallbackDeliveryKindOperation,
	)
}

func (s *CallbackSender) sendOperationCallbackContextForTest(
	ctx context.Context,
	leaseUUID, callbackURL, backendName string,
	status backend.CallbackStatus,
	errMsg string,
) {
	s.sendLegacyTestCallback(
		ctx, leaseUUID, callbackURL, backendName, status, errMsg,
		false, CallbackDeliveryKindOperation,
	)
}

func (s *CallbackSender) sendLifecycleCallbackForTest(
	leaseUUID, callbackURL, backendName string,
	status backend.CallbackStatus,
	errMsg string,
	retained bool,
) {
	s.sendLegacyTestCallback(
		context.Background(), leaseUUID, callbackURL, backendName, status, errMsg,
		retained, CallbackDeliveryKindLifecycle,
	)
}

// sendLegacyTestCallback preserves the old raw callback fixture strictly in
// this package's tests. Production code can only publish through the typed
// CallbackPublisher boundary.
func (s *CallbackSender) sendLegacyTestCallback(
	ownerCtx context.Context,
	leaseUUID, callbackURL, backendName string,
	status backend.CallbackStatus,
	errMsg string,
	retained bool,
	kind CallbackDeliveryKind,
) {
	if callbackURL == "" {
		return
	}
	if kind == CallbackDeliveryKindOperation {
		if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
			return
		}
		if err := backend.ValidateOperationCallbackURL(callbackURL); err != nil {
			return
		}
	} else {
		if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed &&
			status != backend.CallbackStatusDeprovisioned {
			return
		}
		if retained && status != backend.CallbackStatusDeprovisioned {
			return
		}
		if err := backend.ValidateLifecycleCallbackURL(callbackURL); err != nil {
			return
		}
	}
	entry := CallbackEntry{
		LeaseUUID: leaseUUID, CallbackURL: callbackURL, DeliveryKind: kind,
		Status:  status,
		Backend: backendName, Error: errMsg, Retained: retained, CreatedAt: time.Now(),
	}
	if s.storageIdentity.Valid() {
		entry.BackendStorageID = s.storageIdentity.String()
	}
	if ownerCtx != nil && ownerCtx.Err() != nil {
		return
	}
	if err := s.attestor.verify(ownerCtx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return
		}
	}
	unlock := s.lockLease(leaseUUID)
	defer unlock()
	if ownerCtx != nil && ownerCtx.Err() != nil {
		return
	}
	identityErr := s.attestor.verify(ownerCtx)
	identityVerified := identityErr == nil
	if identityErr != nil {
		if isTerminalStorageAuthorityError(identityErr) {
			return
		}
	}
	if ownerCtx != nil && ownerCtx.Err() != nil {
		return
	}
	if s.store == nil {
		if !identityVerified {
			return
		}
		body, err := callbackEntryPayload(entry, s.storageIdentity)
		if err == nil {
			s.deliverCallback(leaseUUID, callbackURL, body)
		}
		return
	}
	var err error
	if kind == CallbackDeliveryKindOperation {
		err = s.store.settleOperationCallbackLocked(entry)
	} else {
		_, err = s.store.storeEntryLocked(entry)
	}
	if err != nil && !errors.Is(err, errTerminalLifecyclePending) {
		s.reportStoreError()
		return
	}
	s.NotifyPendingCallbacks()
}
