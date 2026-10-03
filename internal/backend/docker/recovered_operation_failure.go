package docker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// settleRecoveredOperationFailure is the one place operation recovery turns a
// cleanup's exact-absence evidence into a terminal provision or restore
// failure (ENG-1125). Its order is the actor's own: prove the operation's
// Release is still uncommitted, publish the Failed projection, then settle the
// journal. If settlement then fails, the next live pass finds a Failed
// projection that awaits this exact operation and completes it through the
// proven-failure path. Publishing after settlement would instead leave the
// lease Provisioning whenever the publisher's result is ambiguous: recoverState
// keeps an in-flight projection whole, providerd plans nothing for an ACTIVE
// lease that is not Failed, and nothing else would move it.
func (b *Backend) settleRecoveredOperationFailure(ctx context.Context, decision recoveredIntentDecision) error {
	uncommitted, err := b.operationSettlement.CommitOperationFailure(decision.failureOutcome)
	if err != nil {
		return err
	}
	if b.callbackPublisher == nil {
		return errors.New("callback publisher is required")
	}
	switch decision.projection {
	case recoveredFailureRebuildsProjection, recoveredFailureKeepsProjection:
		return b.callbackPublisher.PublishOperationFailureContext(ctx, uncommitted, decision.errMsg)
	case recoveredFailurePublishesProjection:
		surface, err := b.prepareRecoveredOperationFailure(ctx, uncommitted, decision.errMsg)
		if err != nil {
			return err
		}
		if err := b.publishRecoveredOperationFailure(decision.claim, surface); err != nil {
			return err
		}
		return b.callbackPublisher.PublishOperationFailureContext(ctx, uncommitted, surface.message)
	default:
		return fmt.Errorf("recovered %s failure has no projection disposition", decision.claim.Kind())
	}
}

// prepareRecoveredOperationFailure makes the attempt's diagnostic capture
// durable and reads back the curated surface it authored at the failure
// source. The callback publisher is the diagnostic publisher in production;
// without one, the surface is the recovery observation itself.
func (b *Backend) prepareRecoveredOperationFailure(
	ctx context.Context,
	proof shared.OperationReleaseUncommitted,
	message string,
) (operationFailureSurface, error) {
	if preparer, ok := b.callbackPublisher.(operationFailurePreparer); ok {
		return preparer.prepareOperationFailure(ctx, proof, message)
	}
	return operationFailureSurface{reason: backend.ReasonInternal, message: message, lastError: message}, nil
}

// publishRecoveredOperationFailure publishes Failed on the projection that
// awaits claim's exact operation. Recovery holds the lease's command fence and
// has retired and detached its quiescent actor, so it is the projection's only
// writer. The actor registry is still checked, in the actor-creation lock
// order, so an actor that appeared defers this pass instead of losing state.
// A projection that awaits another operation, or none, is left untouched: it
// has nothing of this operation to fail.
func (b *Backend) publishRecoveredOperationFailure(
	claim shared.OperationIntentClaim,
	surface operationFailureSurface,
) error {
	var predecessor *shared.Release
	if claim.Kind() == shared.OperationIntentProvision {
		if b.releaseStore == nil {
			return errors.New("release store is required to publish a recovered provision failure")
		}
		active, err := b.releaseStore.LatestActive(claim.LeaseUUID())
		if err != nil {
			return fmt.Errorf("read failed provision predecessor release: %w", err)
		}
		predecessor = active
	}
	runtime, err := leasesm.NewDurableRuntime(claim, predecessor)
	if err != nil {
		return err
	}
	now := time.Now()

	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.actors[claim.LeaseUUID()] != nil {
		return errors.New("a lease actor appeared before the recovered failure was published; retry")
	}
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()
	current := b.provisions[claim.LeaseUUID()]
	if current == nil || !current.PendingOperation.Names(claim.OperationID()) {
		return nil
	}
	switch current.Status {
	case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting:
	default:
		return nil
	}
	failed := recoveredFromProvision(current)
	applyFailedOperationProjection(&failed.ProvisionState, runtime, surface, now)
	// A fresh pointer, as for an awaited success: recoverState detects the
	// replacement and preserves it while the intent is still pending.
	b.provisions[claim.LeaseUUID()] = failed.materialize()
	return nil
}

// applyFailedOperationProjection reduces p, a projection that awaited the
// failed operation, to the lease's durable runtime (leasesm.DurableRuntime)
// and marks it Failed. Cleanup proved the operation's exact absence before
// settlement, so no container of the attempt survives and none is published.
//
// The failure is never counted against the terminal budget: SetStatus only
// applies the Ready boundary. FailCount stays a lifetime diagnostic.
func applyFailedOperationProjection(
	p *leasesm.ProvisionState,
	runtime leasesm.DurableRuntime,
	surface operationFailureSurface,
	now time.Time,
) {
	runtime.Apply(p)
	p.FailCount++
	p.SetStatus(backend.ProvisionStatusFailed, now)
	p.LastError = surface.lastError
	p.Reason = surface.reason
	p.Message = surface.message
}
