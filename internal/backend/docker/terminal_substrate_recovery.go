package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

type terminalReceiptKind string

const (
	terminalReceiptClosed terminalReceiptKind = "closed"
	terminalReceiptFailed terminalReceiptKind = "failed_operation"
)

// observeClosedSubstrate publishes exclusion and accounting together. A
// compact terminal receipt cannot size a late container: its release may have
// been deleted or superseded. Withhold capacity instead of reconstructing a
// fictitious resource allocation from mutable SKU configuration. Each receipt
// family owns a separate pool-issued hold, so one empty observation cannot
// release another family's pending footprint. recoverMu serializes observers.
func (b *Backend) observeClosedSubstrate(count int) {
	owner := &b.closedSubstrateCapacityHold
	if count > 0 && *owner == nil {
		hold := b.pool.HoldUnaccountedFootprint()
		*owner = &hold
	} else if count == 0 && *owner != nil {
		(*owner).Release()
		*owner = nil
	}
	terminalSubstratePendingContainers.WithLabelValues(string(terminalReceiptClosed)).Set(float64(count))
}

// failedSubstrateAccountingHold retains only observation scope, never cleanup
// authority. A successful close replaces its failed-operation receipts with a
// stronger permanent closed receipt; that journal transition cannot prove the
// physical footprint behind an existing capacity hold disappeared. Either
// callback identity keeps the hold until a strict inventory proves absence,
// including a partial/contradictory match. Only current durable receipts may
// authorize mutation in recoverFailedOperationSubstrate.
type failedSubstrateAccountingHold struct {
	capacity           shared.ResourceAccountingHold
	callbackIdentities map[string]struct{}
}

func (h *failedSubstrateAccountingHold) pendingContainers(containers []ContainerInfo) int {
	if h == nil {
		return 0
	}
	count := 0
	for _, container := range containers {
		_, operationMatch := h.callbackIdentities[container.CallbackURL]
		_, lifecycleMatch := h.callbackIdentities[container.LifecycleCallbackURL]
		if operationMatch || lifecycleMatch {
			count++
		}
	}
	return count
}

// retainFailedSubstrate may add observation scope from either ordinary or
// strict inventory, but never releases a hold. Scopes remain owned by the hold
// even after successful close retires the corresponding operation history.
func (b *Backend) retainFailedSubstrate(containers []ContainerInfo, targets []failedOperationReceiptTarget) {
	if len(targets) == 0 {
		return
	}
	if b.failedSubstrateCapacityHold == nil {
		b.failedSubstrateCapacityHold = &failedSubstrateAccountingHold{
			capacity:           b.pool.HoldUnaccountedFootprint(),
			callbackIdentities: make(map[string]struct{}),
		}
	}
	for _, target := range targets {
		b.failedSubstrateCapacityHold.callbackIdentities[target.receipt.CallbackURL()] = struct{}{}
		b.failedSubstrateCapacityHold.callbackIdentities[target.receipt.LifecycleCallbackURL()] = struct{}{}
	}
	terminalSubstratePendingContainers.WithLabelValues(string(terminalReceiptFailed)).Set(
		float64(b.failedSubstrateCapacityHold.pendingContainers(containers)),
	)
}

// releaseAbsentFailedSubstrate consumes only a fresh strict inventory. Journal
// receipt absence, callback delivery, and ordinary inventory are not sufficient
// to release the hold's independent physical observation scope.
func (b *Backend) releaseAbsentFailedSubstrate(containers []ContainerInfo) {
	count := b.failedSubstrateCapacityHold.pendingContainers(containers)
	terminalSubstratePendingContainers.WithLabelValues(string(terminalReceiptFailed)).Set(float64(count))
	if hold := b.failedSubstrateCapacityHold; hold != nil && count == 0 {
		hold.capacity.Release()
		b.failedSubstrateCapacityHold = nil
	}
}

// deferTerminalSubstrateCleanup retains the permanent receipt and its pool
// exclusion for the next sweep. Ordinary idempotent removal failure cannot
// invalidate durable authority. The mutation bracket independently latches
// actual storage-identity loss, and that stronger failure still propagates.
func (b *Backend) deferTerminalSubstrateCleanup(kind terminalReceiptKind, cause error) error {
	if err := b.terminalStorageAuthorityError(); err != nil {
		return err
	}
	terminalSubstrateCleanupRetriesTotal.WithLabelValues(string(kind)).Inc()
	b.logger.Warn("terminal substrate cleanup remains pending",
		"receipt", kind, "error", cause)
	return nil
}

// recoverClosedLeaseSubstrate enforces permanent UUID retirement at the
// Docker boundary. Close success is not a one-shot absence assertion: a
// daemon-side Create accepted before close may surface later. The sealed
// aggregate receipt remains forever, every recovery sweep batch-checks each
// observed managed UUID, and this pass removes any matching container that
// subsequently appears. Work is O(live inventory), not O(all historical closes).
func (b *Backend) recoverClosedLeaseSubstrateUsing(
	ctx context.Context,
	removeContainer backgroundContainerRemove,
) (map[string]struct{}, error) {
	closedLeases := make(map[string]struct{})
	if b.callbackStore == nil {
		return closedLeases, nil
	}
	if removeContainer == nil {
		return nil, errBackgroundMaintenanceUnavailable
	}

	for observation := range 3 {
		containers, inventoryErr := b.strictIdentityBoundOperationInventory(ctx)
		if inventoryErr != nil {
			return nil, fmt.Errorf("inspect late substrate for closed leases: %w", inventoryErr)
		}
		byLease, receiptErr := b.closedLeaseRecoveryReceipts(containers)
		if receiptErr != nil {
			return nil, receiptErr
		}
		for leaseUUID := range byLease {
			closedLeases[leaseUUID] = struct{}{}
		}
		targets, targetErr := closedLeaseRecoveryTargets(byLease, containers)
		if targetErr != nil {
			return nil, targetErr
		}
		b.observeClosedSubstrate(len(targets))
		if len(targets) == 0 {
			return closedLeases, nil
		}
		if observation == 2 {
			return closedLeases, b.deferTerminalSubstrateCleanup(terminalReceiptClosed,
				fmt.Errorf("closed-lease substrate remained after two cleanup passes"))
		}
		for _, target := range targets {
			removeCtx, cancelRemove := context.WithTimeout(ctx, 10*time.Second)
			removeErr := removeContainer(removeCtx, target.ContainerID)
			cancelRemove()
			if removeErr != nil {
				return closedLeases, b.deferTerminalSubstrateCleanup(terminalReceiptClosed,
					fmt.Errorf("remove late substrate for closed lease %q: %w", target.LeaseUUID, removeErr))
			}
			b.logger.Warn("removed late container for permanently closed lease",
				"lease_uuid", target.LeaseUUID,
				"container_id", target.ContainerID,
			)
		}
	}
	return closedLeases, nil
}

func (b *Backend) closedLeaseRecoveryReceipts(
	containers []ContainerInfo,
) (map[string]shared.ClosedLeaseReceipt, error) {
	byLease := make(map[string]shared.ClosedLeaseReceipt)
	if b.callbackStore == nil || len(containers) == 0 {
		return byLease, nil
	}
	leaseUUIDs := make([]string, 0, len(containers))
	seen := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		// Production's strict inventory has already rejected this shape. Keeping
		// invalid mock/legacy values out of the batch lookup lets ordinary recovery
		// report their own compatibility error instead of treating them as a
		// possible canonical closed-lease authority.
		if !backend.IsCanonicalLeaseUUID(container.LeaseUUID) {
			continue
		}
		if _, duplicate := seen[container.LeaseUUID]; duplicate {
			continue
		}
		seen[container.LeaseUUID] = struct{}{}
		leaseUUIDs = append(leaseUUIDs, container.LeaseUUID)
	}
	receipts, err := b.callbackStore.LookupClosedLeaseReceipts(leaseUUIDs)
	if err != nil {
		return nil, fmt.Errorf("lookup closed lease receipts: %w", err)
	}
	for _, receipt := range receipts {
		if receipt.Backend() != b.cfg.Name || receipt.BackendStorageID() != b.storageIdentity {
			return nil, fmt.Errorf(
				"closed lease receipt for %q belongs to backend %q storage %q",
				receipt.LeaseUUID(), receipt.Backend(), receipt.BackendStorageID().String(),
			)
		}
		switch receipt.AuthorityKind() {
		case shared.ClosedLeaseAuthorityPrincipal:
			if receipt.Tenant() == "" || receipt.ProviderUUID() == "" {
				return nil, fmt.Errorf("closed lease receipt for %q has partial principal authority", receipt.LeaseUUID())
			}
		case shared.ClosedLeaseAuthorityOrphan:
			if !receipt.CleanupOnly() || receipt.Tenant() != "" || receipt.ProviderUUID() != "" {
				return nil, fmt.Errorf("closed lease receipt for %q has invalid orphan authority", receipt.LeaseUUID())
			}
		default:
			return nil, fmt.Errorf(
				"closed lease receipt for %q has unknown authority kind %q",
				receipt.LeaseUUID(), receipt.AuthorityKind(),
			)
		}
		if existing := byLease[receipt.LeaseUUID()]; existing.Valid() {
			return nil, fmt.Errorf("closed lease %q has duplicate retirement receipts", receipt.LeaseUUID())
		}
		byLease[receipt.LeaseUUID()] = receipt
	}
	return byLease, nil
}

func closedLeaseRecoveryTargets(
	byLease map[string]shared.ClosedLeaseReceipt,
	containers []ContainerInfo,
) ([]ContainerInfo, error) {
	var targets []ContainerInfo
	for _, container := range containers {
		receipt := byLease[container.LeaseUUID]
		if !receipt.Valid() {
			continue
		}
		// A principal-bound receipt must match both immutable identities. The
		// deliberately weaker true-orphan variant exists only when no principal
		// witness survived admission; its trust boundary is the sealed journal,
		// provider-authenticated deprovision command, reserved fred.* Docker labels,
		// exact lease UUID, and the attested backend/storage bracket.
		if receipt.AuthorityKind() == shared.ClosedLeaseAuthorityPrincipal &&
			(container.Tenant != receipt.Tenant() || container.ProviderUUID != receipt.ProviderUUID()) {
			return nil, fmt.Errorf(
				"container %q for closed lease %q has divergent principal identity",
				container.ContainerID, receipt.LeaseUUID(),
			)
		}
		targets = append(targets, container)
	}
	return targets, nil
}
