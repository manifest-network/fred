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
// The reconcile uses a two-RLock-pass candidate-only shape (ENG-277): a
// candidate pre-pass (RLock) finds which incoming domains actually differ from
// what's emitted and need DNS resolution; only those are resolved off-lock via
// b.dnsGateAllows; a main pass (RLock) re-reads fresh prov and runs
// computeCustomDomainOverrides read-only. A steady-state lease performs zero
// DNS lookups; a not-Ready lease short-circuits in the pre-pass with zero DNS.
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

	// Candidate pre-pass: under RLock, find which incoming domains actually
	// differ from what's emitted and need resolving. Steady-state domains are
	// not candidates, so the resolver below is never called for them. A
	// not-provisioned / not-Ready lease short-circuits here with ZERO DNS.
	// (ENG-277)
	b.provisionsMu.RLock()
	prov, ok := b.provisions[leaseUUID]
	ready := ok && prov.Status == backend.ProvisionStatusReady
	var candidates []string
	if ready {
		candidates = b.customDomainDNSCandidates(prov, items)
	}
	b.provisionsMu.RUnlock()
	if !ready {
		return nil
	}

	// Resolve ONLY the candidates, off-lock — network I/O must not block the
	// lock. dnsReady is empty in steady state. Use the nil-safe dnsGateAllows
	// wrapper (default-allow when customDomainDNSReady is unset). (ENG-266)
	dnsReady := make(map[string]bool, len(candidates))
	for _, d := range candidates {
		dnsReady[d] = b.dnsGateAllows(ctx, d)
	}

	// Main pass: re-read FRESH prov under RLock (recoverState may have swapped
	// b.provisions during the resolve window) and compute the diff read-only.
	// dnsReady is keyed by the chain-immutable domain string, so it stays valid
	// across the window; a domain that became a candidate in the window but was
	// not resolved is simply deferred one tick by the asymmetric gate.
	b.provisionsMu.RLock()
	prov, ok = b.provisions[leaseUUID]
	if !ok || prov.Status != backend.ProvisionStatusReady {
		b.provisionsMu.RUnlock()
		return nil
	}
	overrides := b.computeCustomDomainOverrides(prov, items, dnsReady)
	callbackURL := prov.CallbackURL
	b.provisionsMu.RUnlock()

	if len(overrides) == 0 {
		return nil
	}

	slog.With("lease_uuid", leaseUUID).Info("custom_domain drift detected; redeploying",
		"changes", len(overrides))
	// Route the redeploy through the lease actor (ENG-231): the worker renders
	// the new domain from the override-applied snapshot; the actor commits
	// prov.Items on success.
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

// customDomainDNSCandidates returns the deduped set of incoming custom domains
// that need DNS resolution this tick: non-empty, different from the matched
// item's currently-emitted value, and passing validation. Already-emitted or
// invalid domains and clears are excluded, so a steady-state lease yields an
// empty set. Read-only; the caller must hold provisionsMu. Candidacy is
// evaluated per matched item; the resolve REQUEST is deduped by exact domain.
func (b *Backend) customDomainDNSCandidates(prov *provision, items []backend.LeaseItem) []string {
	seen := make(map[string]bool)
	var candidates []string
	for _, m := range matchCustomDomainItems(prov, items) {
		d := m.desired
		if d == "" || d == m.emitted || seen[d] {
			continue
		}
		if err := validateCustomDomain(d, b.cfg.Ingress.WildcardDomain); err != nil {
			continue
		}
		seen[d] = true
		candidates = append(candidates, d)
	}
	return candidates
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
			logger.Debug("custom_domain set but DNS does not resolve yet; deferring cert issuance",
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
