package docker

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// acquireCloseIntent returns the immutable cleanup authority already owned by
// this lease, or publishes it before the first destructive side effect. The
// callback store transaction also preempts any provision/restore intent, so a
// close can never erase the operation completion it won against.
func (b *Backend) acquireCloseIntent(
	_ context.Context,
	leaseUUID string,
	projectionExists bool,
) (shared.CloseIntentClaim, bool, error) {
	if b.callbackStore == nil {
		return shared.CloseIntentClaim{}, false, nil
	}
	if b.closeSettlement == nil {
		return shared.CloseIntentClaim{}, false, errors.New(
			"durable close requires a bound close settlement",
		)
	}
	b.recoverySnapshotMu.RLock()
	defer b.recoverySnapshotMu.RUnlock()
	if existing, found, err := b.closeSettlement.GetCloseIntent(leaseUUID); err != nil {
		return shared.CloseIntentClaim{}, false, fmt.Errorf("read durable close intent: %w", err)
	} else if found {
		if existing.Backend() != b.Name() || existing.BackendStorageID() != b.storageIdentity {
			return shared.CloseIntentClaim{}, false, fmt.Errorf(
				"durable close intent belongs to backend %q storage %s",
				existing.Backend(), existing.BackendStorageID(),
			)
		}
		return existing, true, nil
	}

	cleanupOnly := !projectionExists
	var admission shared.CloseIntentAdmission
	var err error
	if cleanupOnly {
		request, requestErr := b.closeSettlement.NewCleanupCloseRequest(leaseUUID)
		if requestErr != nil {
			return shared.CloseIntentClaim{}, false, fmt.Errorf("construct cleanup close request: %w", requestErr)
		}
		admission, err = b.closeSettlement.BeginCleanupClose(request)
		if errors.Is(err, shared.ErrCloseAuthorityMissing) {
			return shared.CloseIntentClaim{}, false, nil
		}
	} else {
		request, requestErr := b.closeSettlement.NewCloseRequest(leaseUUID, b.cfg.RetainOnClose)
		if requestErr != nil {
			return shared.CloseIntentClaim{}, false, fmt.Errorf("construct close request: %w", requestErr)
		}
		admission, err = b.closeSettlement.BeginClose(request)
	}
	if err != nil {
		return shared.CloseIntentClaim{}, false, fmt.Errorf("publish durable close intent: %w", err)
	}
	return admission.Claim(), true, nil
}

// settleCommittedOperationBeforeClose preserves causal callback ordering at the
// actor drain boundary. A worker writes its exact active Release before queuing
// terminal Success; if that terminal callback write failed (or Deprovision was
// already queued behind it), close admission must not classify the committed
// operation as preempted Failed. A genuinely uncommitted intent is deliberately
// left for BeginClose's atomic failure-preemption transaction.
func (b *Backend) settleCommittedOperationBeforeClose(leaseUUID string) error {
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	pending, err := b.pendingOperationIntentForLease(leaseUUID)
	if err != nil {
		return fmt.Errorf("list operation intents before close admission: %w", err)
	}
	if !pending.Valid() {
		return nil
	}
	if pending.Backend() != b.Name() || pending.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"operation intent before close belongs to backend %q storage %s",
			pending.Backend(), pending.BackendStorageID(),
		)
	}
	committed, err := b.operationIntentHasCommittedRelease(pending)
	if err != nil {
		return fmt.Errorf("validate committed operation before close: %w", err)
	}
	if !committed {
		return nil
	}
	proof, err := b.operationSettlement.ProveCommittedOperation(pending)
	if err != nil {
		return fmt.Errorf("prove committed operation before close admission: %w", err)
	}
	if b.callbackPublisher == nil {
		return errors.New("callback publisher is required")
	}
	if err := b.callbackPublisher.PublishOperationSuccessContext(b.stopCtx, proof); err != nil {
		return fmt.Errorf("settle committed operation before close admission: %w", err)
	}
	return nil
}

// settleCommittedOperationBeforeMaintenance closes the operation-callback
// write-ahead window before a restart, update, or autonomous custom-domain
// replacement can append a new release in the same provision/restore lineage.
func (b *Backend) settleCommittedOperationBeforeMaintenance(leaseUUID string) error {
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	pending, err := b.pendingOperationIntentForLease(leaseUUID)
	if err != nil {
		return fmt.Errorf("list operation intents before maintenance admission: %w", err)
	}
	if !pending.Valid() {
		return nil
	}
	if pending.Backend() != b.Name() || pending.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"operation intent before maintenance belongs to backend %q storage %s",
			pending.Backend(), pending.BackendStorageID(),
		)
	}
	committed, err := b.operationIntentHasCommittedRelease(pending)
	if err != nil {
		return fmt.Errorf("validate committed operation before maintenance: %w", err)
	}
	if !committed {
		return fmt.Errorf(
			"%w: lease %q has an unresolved provision or restore operation",
			backend.ErrInvalidState, leaseUUID,
		)
	}
	proof, err := b.operationSettlement.ProveCommittedOperation(pending)
	if err != nil {
		return fmt.Errorf("prove committed operation before maintenance admission: %w", err)
	}
	if b.callbackPublisher == nil {
		return errors.New("callback publisher is required")
	}
	if err := b.callbackPublisher.PublishOperationSuccessContext(b.stopCtx, proof); err != nil {
		return fmt.Errorf("settle committed operation before maintenance admission: %w", err)
	}
	return nil
}

func (b *Backend) pendingOperationIntentForLease(
	leaseUUID string,
) (shared.OperationIntentClaim, error) {
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return shared.OperationIntentClaim{}, err
	}
	var pending shared.OperationIntentClaim
	for i := range claims {
		if claims[i].LeaseUUID() != leaseUUID {
			continue
		}
		if pending.Valid() {
			return shared.OperationIntentClaim{}, fmt.Errorf("multiple operation intents exist for lease %q", leaseUUID)
		}
		pending = claims[i]
	}
	return pending, nil
}
