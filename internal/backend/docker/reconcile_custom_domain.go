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
//
// No-op when this backend is configured with ingress disabled. Without
// ingress, applyIngressLabels emits no Traefik labels (primary or secondary)
// at all, so a Restart triggered for custom-domain drift would recreate the
// containers with no LabelCustomDomain — and the next recoverState tick
// would rebuild prov.Items[].CustomDomain back to "" from the unlabeled
// containers, putting the reconciler into a permanent restart loop against
// the chain's non-empty value. Returning early here avoids the loop.
func (b *Backend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	if !b.cfg.Ingress.Enabled {
		return nil
	}
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
	//
	// Rollback identifies items by ServiceName (NOT by index) plus a
	// value-CAS check. Between this section's unlock and the rollback path's
	// re-lock, recoverState (running on the docker-backend's own
	// reconcileLoop) can swap b.provisions, replacing the provision struct
	// with a freshly-rebuilt one whose Items may be in a different order.
	// A stored index would silently target the wrong item; a stored
	// ServiceName + value-CAS is robust against any reordering and against
	// any concurrent mutation that already supplied the rolled-back value.
	type pendingChange struct {
		serviceName string
		oldValue    string
		newValue    string
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
		// Defense-in-depth: chain validates on MsgSetItemCustomDomain,
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
		// Apply now while we still hold the lock; the snapshot for rollback
		// keys on ServiceName so we can safely re-find the item if the
		// provision struct is replaced before rollback runs.
		pending = append(pending, pendingChange{
			serviceName: chainItem.ServiceName,
			oldValue:    prov.Items[idx].CustomDomain,
			newValue:    chainItem.CustomDomain,
		})
		prov.Items[idx].CustomDomain = chainItem.CustomDomain
	}

	if len(pending) == 0 {
		b.provisionsMu.Unlock()
		return nil
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
		//
		// Look up by ServiceName (not stored index) and gate the write on
		// the current value still being newValue. If recoverState rebuilt
		// b.provisions with a reordered Items slice, ServiceName lookup
		// targets the correct item; the CAS makes the rollback a no-op
		// when something else (e.g., recoverState reading from container
		// labels) has already restored the previous value.
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[leaseUUID]; ok {
			for _, ch := range pending {
				for i := range prov.Items {
					if prov.Items[i].ServiceName != ch.serviceName {
						continue
					}
					if prov.Items[i].CustomDomain == ch.newValue {
						prov.Items[i].CustomDomain = ch.oldValue
					}
					break
				}
			}
		}
		b.provisionsMu.Unlock()
		return err
	}
	return nil
}
