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

	// Precompute DNS readiness for each non-empty incoming custom domain BEFORE
	// taking provisionsMu — the lookups do network I/O (a quorum of public
	// resolvers) and must not block the lock. Keyed by domain; a lease has 0-1
	// custom domains in practice. (ENG-266)
	dnsReady := make(map[string]bool)
	for i := range items {
		d := items[i].CustomDomain
		if d == "" {
			continue
		}
		if _, seen := dnsReady[d]; seen {
			continue
		}
		// Validate before any DNS I/O: a malformed/forbidden value is rejected
		// by the diff loop below anyway, so resolving it is wasted network work
		// — and would needlessly send a bad value to the public resolvers.
		// Record it as not-ready so a (rare) duplicate isn't re-validated.
		if err := validateCustomDomain(d, b.cfg.Ingress.WildcardDomain); err != nil {
			dnsReady[d] = false
			continue
		}
		dnsReady[d] = b.dnsGateAllows(ctx, d)
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
	// provision item by normalized service name (see normalizedServiceKeys),
	// validate the new domain, and stage the change. Validation failures are
	// skipped per item — bad inputs never block the rest of the reconcile.
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

	// Match chain items to provision items by their NORMALIZED service name.
	// The provision path runs NormalizeProvisionRequest, which tags a lone
	// unnamed item with DefaultServiceName ("app"); that normalized name is
	// what lands in the container label and, via recoverState, in
	// prov.Items[].ServiceName. The chain items handed to us carry the RAW
	// on-chain service_name (ExtractLeaseItems does no normalization), which is
	// "" for a single-image deploy created without -service-name. Normalizing
	// both sides keeps them in lock-step; matching the raw strings would miss
	// ("app" != "") and silently drop a custom_domain set after deploy
	// (ENG-264).
	chainKeys := normalizedServiceKeys(items)
	provKeys := normalizedServiceKeys(prov.Items)

	for ci := range items {
		chainItem := items[ci]
		idx := -1
		for i := range prov.Items {
			if provKeys[i] == chainKeys[ci] {
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
		emitted := prov.Items[idx].CustomDomain
		desired := chainItem.CustomDomain
		// Defense-in-depth: chain validates on MsgSetItemCustomDomain, but we
		// re-run the cheap checks here so a malformed value (e.g. set before
		// this validation existed, or via a future chain bug) cannot leak into
		// Traefik labels. Empty domain (clear) bypasses the FQDN check.
		if desired != "" {
			if err := validateCustomDomain(desired, b.cfg.Ingress.WildcardDomain); err != nil {
				logger.Error("skipping custom-domain reconcile (validation failed)",
					"service_name", prov.Items[idx].ServiceName,
					"custom_domain", desired,
					"error", err)
				continue
			}
		}
		// Asymmetric DNS-readiness gate (ENG-266): only gate emitting a domain
		// that is not already the emitted one. Never tear down an
		// already-emitted domain on a transient DNS mismatch; clearing ("") is
		// never gated. dnsReady was precomputed before the lock.
		if desired != "" && desired != emitted && !dnsReady[desired] {
			logger.Info("custom_domain set but DNS not yet pointing at this host; deferring cert issuance",
				"service_name", prov.Items[idx].ServiceName,
				"custom_domain", desired)
			desired = emitted
		}
		if emitted == desired {
			continue
		}
		// Apply now while we still hold the lock. The rollback snapshot keys on
		// the provision item's ACTUAL ServiceName (not the normalized key) so it
		// re-finds the item even if the provision struct is replaced before
		// rollback runs.
		pending = append(pending, pendingChange{
			serviceName: prov.Items[idx].ServiceName,
			oldValue:    emitted,
			newValue:    desired,
		})
		prov.Items[idx].CustomDomain = desired
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

// normalizedServiceKeys returns, for each item, the service name it resolves
// to after the same boundary normalization the provision path applies — a lone
// unnamed item (service_name == "") becomes DefaultServiceName ("app"). This
// lets the reconcile match chain items (whose ServiceName is the RAW on-chain
// value — "" for a single-image deploy created without -service-name) against
// prov.Items, whose ServiceName came from the normalized container labels
// (NormalizeProvisionRequest runs at provision, so the container is labeled
// "app"). Matching the raw strings would miss ("app" != "") and silently drop
// a custom_domain set after deploy (ENG-264).
//
// Best-effort: NormalizeProvisionRequest only rewrites the lone-unnamed case
// and otherwise leaves names untouched, erroring (without mutating) only on
// structurally-invalid sets — mixed or multiple-unnamed — which chain
// validation prevents for a real lease. On error we fall back to the raw
// names, which is no worse than the pre-fix exact-string match.
func normalizedServiceKeys(items []backend.LeaseItem) []string {
	cp := append([]backend.LeaseItem(nil), items...)
	_ = backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: cp})
	keys := make([]string, len(cp))
	for i := range cp {
		keys[i] = cp[i].ServiceName
	}
	return keys
}
