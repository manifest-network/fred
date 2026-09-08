package docker

import (
	"context"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// observeUnaccountedManagedVolumes reports namespace entries missing from the
// live, admitted-operation, and retention read projections. It grants no cleanup
// or admission authority. A concurrent create/rename may briefly lead its read
// projection; the next successful observation replaces the gauge. Read failure
// preserves the last value instead of reporting a falsely empty footprint.
func (b *Backend) observeUnaccountedManagedVolumes(ctx context.Context) {
	if b.cfg.VolumeDataPath == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, b.cfg.storageAttestationBudget())
	defer cancel()
	count, err := b.unaccountedManagedVolumeCount(ctx)
	if err != nil {
		unaccountedManagedVolumeObservationFailuresTotal.Inc()
		b.logger.Warn("cannot observe unaccounted managed volumes", "error", err)
		return
	}
	unaccountedManagedVolumes.Set(float64(count))
}

func (b *Backend) unaccountedManagedVolumeCount(ctx context.Context) (int, error) {
	managed, err := attestManagedVolumeInventory(ctx, b.volumes)
	if err != nil {
		return 0, err
	}
	// Observe substrate before the journals/projection: an operation that
	// admitted a volume before this inventory remains covered by either its
	// pending intent or the later provision/retention publication.
	intents, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return 0, fmt.Errorf("read admitted volume footprints: %w", err)
	}
	retentions, err := b.retentionStore.List()
	if err != nil {
		return 0, fmt.Errorf("read retained volume footprints: %w", err)
	}
	canonical := make(map[string][]managedVolumeEvidenceAuthority)
	addCanonical := func(leaseUUID string, items []backend.LeaseItem) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		footprint, err := managedVolumeEvidenceAuthorityForLease(leaseUUID, items)
		if err != nil {
			return fmt.Errorf("interpret projected volume footprint: %w", err)
		}
		canonical[leaseUUID] = append(canonical[leaseUUID], footprint)
		return nil
	}
	for _, intent := range intents {
		if err := addCanonical(intent.LeaseUUID(), intent.EffectiveItems()); err != nil {
			return 0, err
		}
	}
	retained := make(map[string]struct{})
	for _, retention := range retentions {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		for _, name := range retention.RetainedVolumeNames {
			retained[name] = struct{}{}
			// Active and Reaping finalizers also cover interrupted close renames.
			retained[canonicalFromRetained(name)] = struct{}{}
			if retention.Status == shared.RetentionStatusRestoring {
				retained[retainedToNewCanonical(name, retention.OriginalLeaseUUID, retention.NewLeaseUUID)] = struct{}{}
			}
		}
		if retention.Status == shared.RetentionStatusRestoring {
			if err := addCanonical(retention.NewLeaseUUID, retention.DestinationItems); err != nil {
				return 0, err
			}
		}
	}
	b.provisionsMu.RLock()
	for leaseUUID, provision := range b.provisions {
		if err := addCanonical(leaseUUID, provision.Items); err != nil {
			b.provisionsMu.RUnlock()
			return 0, err
		}
	}
	b.provisionsMu.RUnlock()

	unaccounted := 0
	for name, parsed := range managed {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if _, present := retained[name]; present {
			continue
		}
		identity := managedVolumeEvidenceIdentityFromName(parsed)
		accounted := false
		if !identity.retained {
			for _, footprint := range canonical[identity.leaseUUID] {
				if footprint.containsIdentity(identity) {
					accounted = true
					break
				}
			}
		}
		if !accounted {
			unaccounted++
		}
	}
	return unaccounted, ctx.Err()
}
