# ENG-277 — Skip steady-state custom-domain DNS lookups — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `ReconcileCustomDomain` from resolving every custom domain on every tick — resolve only the domains that actually differ from what's emitted (candidates), so a steady-state lease does zero DNS work.

**Architecture:** A two-`RLock`-pass shape (the idiomatic Go "I/O-dependent decision over locked state": read under lock → release → do I/O → re-acquire + re-validate). Pass 1 (`RLock`) computes the DNS *candidates* (incoming domains that differ from the matched item's emitted value and validate); only those are resolved off-lock via `b.dnsGateAllows`; pass 2 (`RLock`) re-reads fresh `prov` and runs the unchanged `computeCustomDomainOverrides` gate. A shared `matchCustomDomainItems` helper keeps the candidate set and the override diff from diverging. Both passes are read-only post-#103, so both use `RLock`.

**Tech Stack:** Go; `internal/backend/docker` (`reconcile_custom_domain.go`); testify; the existing injectable `b.customDomainDNSReady` DNS seam (reached via `b.dnsGateAllows`).

**Spec:** `docs/superpowers/specs/2026-06-03-eng-277-skip-steady-state-dns-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `internal/backend/docker/reconcile_custom_domain.go` | Add `matchedDomain` + `matchCustomDomainItems` (shared read-only match); refactor `computeCustomDomainOverrides` to use it; add `customDomainDNSCandidates`; rewrite `ReconcileCustomDomain` into the two-`RLock`-pass candidate-only shape. |
| `internal/backend/docker/reconcile_custom_domain_test.go` | Add DNS-count tests (steady-state, candidate-only, not-Ready, invalid-domain) and the deterministic deferral/self-heal white-box test. Existing ENG-264/ENG-266 tests stay. |

No new files — the change is localized to `reconcile_custom_domain.go`.

---

## Task 1: Extract the shared match helper (behavior-preserving)

Extract the chain↔prov normalized match into `matchCustomDomainItems` and refactor `computeCustomDomainOverrides` to use it. No behavior change — verified entirely by the existing reconcile suite.

**Files:**
- Modify: `internal/backend/docker/reconcile_custom_domain.go`

- [ ] **Step 1: Add `matchedDomain` + `matchCustomDomainItems`** (place above `computeCustomDomainOverrides` in `reconcile_custom_domain.go`)

```go
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
```

- [ ] **Step 2: Refactor `computeCustomDomainOverrides` to use the helper**

Replace the current `computeCustomDomainOverrides` body (its `chainKeys`/`provKeys` setup + the `for ci := range items` match loop) with a loop over `matchCustomDomainItems`. The validation + asymmetric gate are unchanged:

```go
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
```

- [ ] **Step 3: Run the existing reconcile suite — verify behavior-preserving**

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain -count=1`
Expected: PASS (no behavior change; `computeCustomDomainOverrides` produces identical overrides).

- [ ] **Step 4: Build + vet**

Run: `go build ./... && go vet ./internal/backend/docker/`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain.go
git commit -m "refactor(docker-backend): extract matchCustomDomainItems shared by the reconcile diff (ENG-277)"
```

---

## Task 2: Candidate-only DNS resolution (the optimization)

Add `customDomainDNSCandidates` and rewrite `ReconcileCustomDomain` into the two-`RLock`-pass shape. Write the DNS-count tests first (they fail on the current all-domains code), then implement.

**Files:**
- Modify: `internal/backend/docker/reconcile_custom_domain.go`
- Test: `internal/backend/docker/reconcile_custom_domain_test.go`

- [ ] **Step 1: Write the failing DNS-count tests** (append to `reconcile_custom_domain_test.go`)

```go
// recordingDNS wraps customDomainDNSReady to record which domains are resolved
// and return a per-domain readiness verdict. Uses the existing injectable seam
// (no test-only production code).
func recordingDNS(b *Backend, ready func(d string) bool) *[]string {
	var mu sync.Mutex
	var resolved []string
	b.customDomainDNSReady = func(_ context.Context, d string) bool {
		mu.Lock()
		resolved = append(resolved, d)
		mu.Unlock()
		return ready(d)
	}
	return &resolved
}

func TestReconcileCustomDomain_SteadyState_NoDNS(t *testing.T) {
	// A Ready lease whose custom domain already equals the chain value must
	// perform ZERO DNS lookups (ENG-277 primary acceptance).
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "live.example.com"}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "live.example.com"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "an already-emitted unchanged domain must not be resolved")
}

func TestReconcileCustomDomain_CandidateOnly_ResolvesOnlyChanged(t *testing.T) {
	// Two services: one already-emitted (unchanged), one changed. Only the
	// changed domain is a candidate, so only it is resolved.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "frontend", CustomDomain: "live.example.com"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "api", CustomDomain: ""},
		}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	// The changed (api) domain stages an override → routeReplaceRestart fails on
	// the nil StackManifest (ErrInvalidState); that only happens if api was
	// resolved+gated, confirming the candidate path ran end-to-end.
	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "frontend", CustomDomain: "live.example.com"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "api", CustomDomain: "new.example.com"},
	})
	require.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Equal(t, []string{"new.example.com"}, *resolved,
		"only the changed domain is a candidate; the already-emitted one must not be resolved")
}

func TestReconcileCustomDomain_NotReady_NoDNS(t *testing.T) {
	// A not-Ready lease must perform ZERO DNS lookups (the pre-pass returns
	// before resolving). Pre-ENG-277 the all-domains precompute ran before the
	// Ready gate and would resolve here.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusProvisioning,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: ""}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "a not-Ready lease must not resolve any custom domain")
}

func TestReconcileCustomDomain_InvalidChangedDomain_NoDNS(t *testing.T) {
	// A changed-but-invalid domain (subdomain of the wildcard) is rejected by
	// validation, so it is NOT a candidate and is never resolved (validate
	// before resolve). No override is staged.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: ""}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "evil.barney0.manifest0.net"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "an invalid changed domain must not be resolved")
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain)
}
```

- [ ] **Step 2: Run the new tests — verify they FAIL on the current code**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileCustomDomain_SteadyState_NoDNS|TestReconcileCustomDomain_CandidateOnly_ResolvesOnlyChanged|TestReconcileCustomDomain_NotReady_NoDNS' -count=1`
Expected: FAIL — the current all-domains precompute resolves `live.example.com` (steady-state), resolves the unchanged `frontend` domain too (candidate-only), and resolves before the Ready gate (not-Ready). `*resolved` is non-empty. (The invalid-domain test passes already — both old and new code validate before resolving; it is a regression guard.)

- [ ] **Step 3: Add `customDomainDNSCandidates`** (in `reconcile_custom_domain.go`, after `matchCustomDomainItems`)

```go
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
```

- [ ] **Step 4: Rewrite `ReconcileCustomDomain` into the two-`RLock`-pass shape**

Replace the body of `ReconcileCustomDomain` (the `dnsReady` precompute loop + the single `Lock` diff section) with:

```go
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
```

Also update `ReconcileCustomDomain`'s doc comment: the precompute is now candidate-only and the diff is two read-only passes (the existing "Precompute DNS readiness for each non-empty incoming custom domain" wording in the body comment is replaced by the above).

- [ ] **Step 5: Run the new tests — verify they now PASS**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileCustomDomain_SteadyState_NoDNS|TestReconcileCustomDomain_CandidateOnly_ResolvesOnlyChanged|TestReconcileCustomDomain_NotReady_NoDNS|TestReconcileCustomDomain_InvalidChangedDomain_NoDNS' -count=1`
Expected: PASS (zero/only-candidate resolution).

- [ ] **Step 6: Run the FULL reconcile suite — verify ENG-264/ENG-266 behavior is unchanged**

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain -count=1`
Expected: PASS — every existing gate test (`_NotDNSReady_DoesNotEmit`, `_DNSReady_Emits`, `_AlreadyEmitted_NotTornDownOnDNSBlip`, `_Clear_NotGated`, `_ChangedDomain_NewNotReady`, `_ChangedDomain_NewReady`, `_MultiService_MixedReadiness`, `_DeferredThenReady`, `_RecoverStateSwap_RedeploysNewDomain`, `_ConcurrentRecoverState_NoRace`, …) still passes: changed domains are candidates → resolved → gated identically; unchanged domains are skipped with no observable difference.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain.go internal/backend/docker/reconcile_custom_domain_test.go
git commit -m "perf(docker-backend): resolve only candidate custom domains in the reconcile (ENG-277)"
```

---

## Task 3: Deterministic deferral/self-heal test (window safety)

Pin the one-tick-deferral + next-tick self-heal — the most subtle new behavior — deterministically at the helper level (white-box, no test seam). A `recoverState` swap in the resolve window is represented by two `prov` states; a `require.Eventually` hammer would essentially never land a swap inside the narrow `RUnlock→RLock` window.

**Files:**
- Test: `internal/backend/docker/reconcile_custom_domain_test.go`

- [ ] **Step 1: Write the test**

```go
func TestReconcileCustomDomain_WindowDeferralThenSelfHeal(t *testing.T) {
	// White-box (ENG-277 window safety). The candidate pre-pass and the main
	// diff read prov twice across the resolve window. If a recoverState swap
	// changes the emitted value in that window, a domain that was NOT a candidate
	// (so dnsReady lacks it) becomes a change the main pass sees → the asymmetric
	// gate DEFERS it that tick (no tear-down), and the NEXT tick's pre-pass
	// resolves it → emits. The swap is represented by two prov states.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}

	chain := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"}}
	// Pre-pass state: domain already emitted == desired → NOT a candidate.
	provPrepass := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "new.example.com"}}}}
	// Window state after a recoverState swap: emitted reverted to old.
	provSwapped := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "old.example.com"}}}}

	b.provisionsMu.RLock()
	cands := b.customDomainDNSCandidates(provPrepass, chain)
	b.provisionsMu.RUnlock()
	require.Empty(t, cands, "an already-emitted domain is not a DNS candidate")

	// Tick 1 main pass on the swapped state with the (empty) dnsReady: the now-
	// changed domain is unresolved → DEFERRED (emitted preserved, no override).
	b.provisionsMu.RLock()
	deferred := b.computeCustomDomainOverrides(provSwapped, chain, map[string]bool{})
	b.provisionsMu.RUnlock()
	assert.Empty(t, deferred, "an unresolved changed domain must be deferred, not applied")

	// Tick 2 pre-pass on the swapped state: now a candidate → resolved.
	b.provisionsMu.RLock()
	cands2 := b.customDomainDNSCandidates(provSwapped, chain)
	b.provisionsMu.RUnlock()
	require.Equal(t, []string{"new.example.com"}, cands2, "the changed domain is now a candidate")

	// Tick 2 main pass with the resolved-ready domain: SELF-HEAL → emits.
	b.provisionsMu.RLock()
	healed := b.computeCustomDomainOverrides(provSwapped, chain, map[string]bool{"new.example.com": true})
	b.provisionsMu.RUnlock()
	assert.Equal(t, map[string]string{"app": "new.example.com"}, healed, "next tick emits the new domain")
}
```

- [ ] **Step 2: Run it**

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain_WindowDeferralThenSelfHeal -count=1`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain_test.go
git commit -m "test(docker-backend): deterministic deferral/self-heal for the DNS-candidate window (ENG-277)"
```

---

## Task 4: Final verification (lock-discipline guard + full suite)

The new RLock pre-pass is exercised under `-race` by the existing `TestReconcileCustomDomain_ConcurrentRecoverState_NoRace` (it drives the new `ReconcileCustomDomain` concurrently with `recoverState`), so no new concurrency test is needed — confirm it stays green under `-race`.

**Files:** none (verification only).

- [ ] **Step 1: Race the reconcile + concurrency tests**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileCustomDomain' -race -count=5`
Expected: PASS across all 5 — no race report. This confirms the added `RLock` pre-pass introduces no new race (the concurrency test reads/writes `b.provisions` from a `recoverState` goroutine while reconcile runs the new pre-pass).

- [ ] **Step 2: Full package under race + vet**

Run: `go build ./... && go vet ./internal/backend/docker/ && go test ./internal/backend/docker/ -race -count=1`
Expected: clean build, no vet issues, all tests pass under `-race`.

- [ ] **Step 3 (if any prior step required a fix): Commit**

```bash
git add -A && git commit -m "test(docker-backend): verify ENG-277 candidate-only reconcile under -race"
```
(If steps 1–2 pass with no changes, skip — nothing to commit.)

---

## Self-Review (against the spec)

**Spec coverage:**
- Candidate pre-pass + candidate-only resolution + both-RLock → Task 2 Steps 3–4. ✔
- Shared `matchCustomDomainItems` (anti-divergence) with the pinned contract → Task 1. ✔
- `dnsGateAllows` (nil-safe) for resolution → Task 2 Step 4. ✔
- `computeCustomDomainOverrides` gate unchanged → Task 1 Step 2 (same validation/gate). ✔
- Test 1 steady-state zero DNS → `_SteadyState_NoDNS`. Test 2 candidate-only → `_CandidateOnly_ResolvesOnlyChanged`. Test 3 invalid → `_InvalidChangedDomain_NoDNS`. Test 4 not-Ready zero DNS → `_NotReady_NoDNS`. Test 5 gate-unchanged → existing ENG-266 suite (Task 2 Step 6). Test 6 deferral/self-heal → Task 3. Test 7 `-race` guard → Task 4 (existing concurrency test). ✔
- Out-of-scope (double-validate kept, no CPU micro-tuning, focused) — honored: validation stays in both `customDomainDNSCandidates` and `computeCustomDomainOverrides`; no other changes. ✔

**Type consistency:** `matchedDomain{desired, emitted, serviceName}` and `matchCustomDomainItems(prov, items) []matchedDomain` are defined in Task 1 and consumed by `customDomainDNSCandidates` and `computeCustomDomainOverrides` in Task 2. `customDomainDNSCandidates(prov, items) []string` and `dnsReady map[string]bool` are consistent across `ReconcileCustomDomain` and `computeCustomDomainOverrides`.

**Placeholder scan:** none — every code step is complete; every run step has an exact command + expected result, including the red (Task 2 Step 2) and green (Step 5/6) states.
