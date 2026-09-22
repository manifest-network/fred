package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// terminalProvisionPayload is a private cleanup capability. Only the owner of
// the exact live lease claim can mint it, from a positive terminal observation
// or a confirmed rejection of that exact lease. Detached handler results carry
// no chain mutation or cleanup authority.
type terminalProvisionPayload struct {
	issuer *ProvisionCoordinator
	claim  operation.LeaseClaim
	lease  billingtypes.Lease
}

func (authority *ProvisionCoordinator) terminalPayload(
	lease billingtypes.Lease, claim operation.LeaseClaim,
) (terminalProvisionPayload, error) {
	if !authority.coordinator.operations.HoldsLeaseClaim(claim, lease.Uuid) {
		return terminalProvisionPayload{}, errors.New("exact live provision lease claim is required")
	}
	switch lease.State {
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
	default:
		return terminalProvisionPayload{}, errors.New("positive terminal provision state is required")
	}
	if err := authority.coordinator.store.leaseSideEffectError(lease.Uuid); err != nil {
		return terminalProvisionPayload{}, err
	}
	return terminalProvisionPayload{issuer: authority, claim: claim, lease: lease}, nil
}

func (cleanup terminalProvisionPayload) commit() error {
	if cleanup.issuer == nil || !cleanup.issuer.Valid() ||
		!cleanup.issuer.coordinator.operations.HoldsLeaseClaim(cleanup.claim, cleanup.lease.Uuid) {
		return errors.New("terminal payload cleanup capability is no longer live")
	}
	if cleanup.issuer.payloads == nil {
		return nil
	}
	return cleanup.issuer.payloads.DeleteDurable(cleanup.lease.Uuid)
}

func (authority *ProvisionCoordinator) rejectInvalidProvision(
	ctx context.Context, invalid ProvisionEventResult, claim operation.LeaseClaim,
) ProvisionEventResult {
	lease := invalid.lease
	uncertain := func(err error) ProvisionEventResult {
		return newProvisionEventResult(ProvisionEventUncertain, &lease, err)
	}
	if !invalid.hasLease || !authority.coordinator.operations.HoldsLeaseClaim(claim, lease.Uuid) {
		return uncertain(errors.New("claimed provision validation result is required"))
	}
	observation := authority.readLease(ctx, lease.Uuid, lease.Tenant)
	exact, ok := observation.(observedExactLease)
	if !ok {
		return uncertain(fmt.Errorf("revalidate provision rejection: %w", exactLeaseObservationError(observation)))
	}
	if exact.lease.State != billingtypes.LEASE_STATE_PENDING {
		return authority.finishTerminalProvision(exact.lease, claim)
	}
	if err := authority.coordinator.store.leaseSideEffectError(lease.Uuid); err != nil {
		return uncertain(err)
	}
	reason := provisionRejectionReason(invalid)
	slog.WarnContext(ctx, "provision validation failed, rejecting lease",
		"lease_uuid", lease.Uuid, "tenant", lease.Tenant,
		"reason", reason, "error", invalid.Err(),
	)
	rejected, _, err := authority.controlPlane.rejectLease(ctx, lease.Uuid, reason)
	if err != nil || rejected != 1 {
		// An RPC error can follow a committed rejection. Only a fresh exact
		// terminal observation permits cleanup; Pending, Active and unknown
		// responses all preserve the original bytes for a later event.
		if current, ok := authority.readLease(ctx, lease.Uuid, lease.Tenant).(observedExactLease); ok {
			switch current.lease.State {
			case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
				return authority.finishTerminalProvision(current.lease, claim)
			}
		}
		if err == nil {
			err = fmt.Errorf("rejection acknowledged %d leases, expected one", rejected)
		}
		return uncertain(fmt.Errorf("reject lease %s after validation: %w", lease.Uuid, err))
	}
	// RejectLeases acknowledged the sole submitted lease. The exact live claim
	// remains held through the durable payload removal.
	lease.State = billingtypes.LEASE_STATE_REJECTED
	lease.RejectionReason = reason
	completion := authority.finishTerminalProvision(lease, claim)
	// The chain has acknowledged this rejection even when local cleanup
	// fails. Report the committed observation and the retryable cleanup error
	// independently, so failure notification does not depend on deleting bytes.
	return newProvisionEventResult(ProvisionEventRejected, &lease, completion.Err())
}

func (authority *ProvisionCoordinator) finishTerminalProvision(
	lease billingtypes.Lease, claim operation.LeaseClaim,
) ProvisionEventResult {
	cleanup, err := authority.terminalPayload(lease, claim)
	if err != nil {
		return newProvisionEventResult(ProvisionEventUncertain, &lease, err)
	}
	err = cleanup.commit()
	if err != nil {
		return newProvisionEventResult(ProvisionEventUncertain, &lease,
			fmt.Errorf("clean terminal lease payload: %w", err))
	}
	return newProvisionEventResult(ProvisionEventLeaseTerminal, &lease, nil)
}

// Only stable, curated codes reach the chain; backend diagnostics remain local.
func provisionRejectionReason(result ProvisionEventResult) string {
	if result.disposition == ProvisionEventPayloadInvalid {
		return "payload corrupted"
	}
	return ValidationRejectionReason(result.err)
}

// ValidationRejectionReason projects a validation error to a fixed public
// category. It never includes dynamic diagnostics and grants no permission to
// reject a lease; callers must already own an authoritative validation refusal.
// Event-driven provisioning and reconciliation use this same vocabulary.
func ValidationRejectionReason(err error) string {
	switch {
	case errors.Is(err, backend.ErrUnknownSKU):
		return "invalid SKU"
	case errors.Is(err, backend.ErrInvalidManifest):
		return "invalid manifest"
	case errors.Is(err, backend.ErrImageNotAllowed):
		return "image not allowed"
	default:
		return "validation error"
	}
}
