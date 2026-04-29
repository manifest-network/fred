package docker

import (
	"context"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend"
)

// ReconcileCustomDomain reapplies the per-LeaseItem custom_domain values from
// chain onto the running provision. When at least one item's CustomDomain
// differs from the in-memory state, the backend snapshots the current values,
// applies the new ones, and triggers a Restart() to re-emit Traefik labels.
// On Restart failure the in-memory state is restored from the snapshot so the
// next reconciler tick can retry cleanly.
//
// Reconciliation only runs when the provision is in ProvisionStatusReady.
// Any other state (Provisioning, Restarting, Updating, Failing, Failed,
// Deprovisioning, Unknown) is treated as "not the right time": skip without
// error and let the periodic reconciler call back when the provision settles.
func (b *Backend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	b.provisionsMu.Lock()
	prov, ok := b.provisions[leaseUUID]
	if !ok {
		b.provisionsMu.Unlock()
		return nil
	}
	if prov.Status != backend.ProvisionStatusReady {
		b.provisionsMu.Unlock()
		return nil
	}

	// Compute per-position diff: for each incoming item, find the matching
	// provision item by ServiceName (legacy uses "" on both sides), validate
	// the new domain, and stage the change. Validation failures are skipped
	// per item — bad inputs never block the rest of the reconcile.
	type pendingChange struct {
		index    int
		oldValue string
		newValue string
	}
	var pending []pendingChange

	logger := slog.With("lease_uuid", leaseUUID)

	for _, chainItem := range items {
		idx := -1
		for i := range prov.Items {
			if prov.Items[i].ServiceName == chainItem.ServiceName {
				idx = i
				break
			}
		}
		if idx == -1 {
			// Item present on chain but not in provision: skip silently.
			// Lease items are immutable post-creation on chain, so a
			// missing-from-prov item can only mean the provision was
			// recovered partially or pre-dates this code; nothing for us to
			// do here.
			continue
		}
		if prov.Items[idx].CustomDomain == chainItem.CustomDomain {
			continue
		}
		// Defense-in-depth: chain validates on MsgSetLeaseItemCustomDomain,
		// but we re-run the same check here so a malformed value (e.g. set
		// before this validation existed, or via a future chain bug) cannot
		// leak into Traefik labels. Empty domain (clear) bypasses the FQDN
		// check.
		if chainItem.CustomDomain != "" {
			if err := validateCustomDomain(chainItem.CustomDomain, b.cfg.Ingress.WildcardDomain); err != nil {
				logger.Error("skipping custom-domain reconcile (validation failed)",
					"service_name", chainItem.ServiceName,
					"custom_domain", chainItem.CustomDomain,
					"error", err)
				continue
			}
		}
		pending = append(pending, pendingChange{
			index:    idx,
			oldValue: prov.Items[idx].CustomDomain,
			newValue: chainItem.CustomDomain,
		})
	}

	if len(pending) == 0 {
		b.provisionsMu.Unlock()
		return nil
	}

	// Apply the new values in memory before triggering Restart so the
	// rebuild reads the updated CustomDomain via prov.Items.
	for _, ch := range pending {
		prov.Items[ch.index].CustomDomain = ch.newValue
	}
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()

	logger.Info("custom_domain drift detected; redeploying",
		"changes", len(pending))

	// Re-emit labels via the existing Restart machinery. Restart uses the
	// stored manifest unchanged and rebuilds containers from prov.Items —
	// the new CustomDomain values get baked into the secondary-router
	// labels naturally.
	err := b.Restart(ctx, backend.RestartRequest{
		LeaseUUID:   leaseUUID,
		CallbackURL: callbackURL,
	})
	if err != nil {
		// Roll back the in-memory change so the next reconciler tick sees
		// the same drift and tries again. The provision is still running
		// the OLD containers (Restart's rollback restores them), so the
		// in-memory state must match.
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[leaseUUID]; ok {
			for _, ch := range pending {
				if ch.index < len(prov.Items) {
					prov.Items[ch.index].CustomDomain = ch.oldValue
				}
			}
		}
		b.provisionsMu.Unlock()
		return err
	}
	return nil
}
