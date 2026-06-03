package docker

import (
	"context"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend"
)

// ReconcileCustomDomain reapplies the per-LeaseItem custom_domain values from
// chain onto the running provision. When at least one item's CustomDomain
// differs from the in-memory state, the backend computes the diff read-only,
// then routes the redeploy through the lease actor via routeReplaceRestart.
// The actor commits prov.Items on success — no off-actor mutation, no CAS
// rollback. A failed redeploy leaves prov.Items untouched so the next
// reconciler tick retries (ENG-278).
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
	// taking provisionsMu — the lookups do network I/O and must not block the
	// lock. Keyed by domain. (ENG-266)
	dnsReady := make(map[string]bool)
	for i := range items {
		d := items[i].CustomDomain
		if d == "" {
			continue
		}
		if _, seen := dnsReady[d]; seen {
			continue
		}
		if err := validateCustomDomain(d, b.cfg.Ingress.WildcardDomain); err != nil {
			dnsReady[d] = false
			continue
		}
		dnsReady[d] = b.dnsGateAllows(ctx, d)
	}

	// Compute the diff READ-ONLY under the lock; stage nothing in prov.Items.
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
	overrides := b.computeCustomDomainOverrides(prov, items, dnsReady)
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()

	if len(overrides) == 0 {
		return nil
	}

	slog.With("lease_uuid", leaseUUID).Info("custom_domain drift detected; redeploying",
		"changes", len(overrides))

	// Route the redeploy through the lease actor. The worker renders the new
	// domain from the override-applied item snapshot; the actor commits the
	// values into prov.Items on success (ENG-231). No off-actor mutation, no
	// CAS rollback — a failed redeploy leaves prov.Items untouched so the next
	// tick retries (ENG-278).
	return b.routeReplaceRestart(ctx, leaseUUID, callbackURL, overrides)
}

// matchedDomain pairs a chain item's desired custom_domain with the matched
// provision item's currently-emitted value and ServiceName. Produced by
// matchCustomDomainItems for the matched chain items only.
type matchedDomain struct {
	desired     string
	emitted     string
	serviceName string
}

// matchCustomDomainItems matches each incoming chain item to a provision item by
// normalized ServiceName (ENG-264) and returns one matchedDomain per MATCHED
// chain item, in chain order, excluding chain items with no provision match. It
// is pure and read-only: no validation, no DNS gate (those stay in the callers).
// The caller must hold provisionsMu (read or write). Shared by the candidate
// pre-pass and computeCustomDomainOverrides so the resolved set and the applied
// diff cannot diverge.
func matchCustomDomainItems(prov *provision, items []backend.LeaseItem) []matchedDomain {
	chainKeys := normalizedServiceKeys(items)
	provKeys := normalizedServiceKeys(prov.Items)
	matched := make([]matchedDomain, 0, len(items))
	for ci := range items {
		idx := -1
		for i := range prov.Items {
			if provKeys[i] == chainKeys[ci] {
				idx = i
				break
			}
		}
		if idx == -1 {
			continue
		}
		matched = append(matched, matchedDomain{
			desired:     items[ci].CustomDomain,
			emitted:     prov.Items[idx].CustomDomain,
			serviceName: prov.Items[idx].ServiceName,
		})
	}
	return matched
}

// computeCustomDomainOverrides returns the per-ServiceName custom_domain changes
// to apply, computed READ-ONLY from the current provision and the incoming chain
// items. The caller must hold provisionsMu. It performs the same
// ServiceName-normalized match (ENG-264), defense-in-depth validation, and
// asymmetric DNS-readiness gate (ENG-266) as the previous in-place mutation, but
// stages nothing in prov.Items — routeReplaceRestart applies the returned
// overrides to the worker snapshot and the actor commits them on success.
func (b *Backend) computeCustomDomainOverrides(prov *provision, items []backend.LeaseItem, dnsReady map[string]bool) map[string]string {
	logger := slog.With("lease_uuid", prov.LeaseUUID)
	overrides := make(map[string]string)
	for _, m := range matchCustomDomainItems(prov, items) {
		emitted := m.emitted
		desired := m.desired
		// Defense-in-depth validation (empty clear bypasses the FQDN check).
		if desired != "" {
			if err := validateCustomDomain(desired, b.cfg.Ingress.WildcardDomain); err != nil {
				logger.Error("skipping custom-domain reconcile (validation failed)",
					"service_name", m.serviceName,
					"custom_domain", desired,
					"error", err)
				continue
			}
		}
		// Asymmetric DNS-readiness gate (ENG-266): only gate emitting a domain
		// that is not already the emitted one. Never tear down an already-emitted
		// domain on a transient DNS mismatch; clearing ("") is never gated.
		if desired != "" && desired != emitted && !dnsReady[desired] {
			logger.Debug("custom_domain set but DNS not yet pointing at this host; deferring cert issuance",
				"service_name", m.serviceName,
				"custom_domain", desired)
			desired = emitted
		}
		if emitted == desired {
			continue
		}
		overrides[m.serviceName] = desired
	}
	return overrides
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
