# ENG-277 — design: skip steady-state custom-domain DNS lookups in the reconcile

**Ticket:** ENG-277 (follow-up to ENG-266; reconcile-specific). Priority: Low. Efficiency, not correctness.
**Status:** design approved (approach + scope locked 2026-06-03; proof/tests tightened after an adversarial idiomatic review). Next: writing-plans → TDD.
**Worktree:** `.claude/worktrees/eng-277`, off `main` (`33a430b`, post-#103 / ENG-278/231 merged).

## Problem

`ReconcileCustomDomain` (`internal/backend/docker/reconcile_custom_domain.go`) is driven per-lease, per-tick from the provisioner reconcile loop (`internal/provisioner/reconciler.go:930`). Its **DNS-readiness precompute** (lines 38–52) resolves **every** non-empty incoming custom domain on **every** tick — including domains that already match the emitted value and will no-op. The precompute runs *before* `provisionsMu` (it does network I/O — a quorum of public-resolver lookups — which must not block the lock), so it can't yet tell which incoming domains differ from what's emitted, and resolves all of them.

In steady state this is wasted network I/O (one resolver-quorum round-trip per custom-domain lease per tick) plus per-tick latency. The gate is correct today; it just does more DNS work than necessary.

## Design — candidate-only resolution (ENG-277 option 1, adapted to post-#103)

The idiomatic Go shape for "an I/O-dependent decision over lock-protected state" is: read under a lock, **release**, do the I/O outside the lock, then **re-acquire and re-validate against fresh state** (Go's `RWMutex` deliberately forbids upgrading `RLock`→`Lock`, so you release-and-reacquire with a double-check). This change is exactly that two-pass shape. The ticket's original option-1 rationale cited the *ServiceName-keyed rollback/CAS* for window tolerance — **that CAS was deleted in #103 (ENG-231)** — so window safety now rests on the re-validation below.

```
ReconcileCustomDomain:
  ├─ ingress guard (unchanged)
  ├─ pre-pass under provisionsMu.RLock():
  │     prov, ok := b.provisions[uuid]; ready := ok && prov.Status == Ready
  │     if ready: candidates := b.customDomainDNSCandidates(prov, items)   // []string, deduped domains to resolve
  │     RUnlock()
  │     if !ready: return nil          // not provisioned / not Ready → no-op, and now ZERO DNS (see note)
  ├─ resolve ONLY candidates off-lock: for each d → dnsReady[d] = b.dnsGateAllows(ctx, d)
  │     (dnsReady is empty in steady state)
  └─ main pass under provisionsMu.RLock()  (read-only post-#103):
        prov re-fetched fresh; Ready-gate; overrides := computeCustomDomainOverrides(prov, items, dnsReady)
        callbackURL := prov.CallbackURL; RUnlock(); if no overrides return nil; else routeReplaceRestart(...)
```

**Candidate definition (per matched item, resolve deduped by domain):** for each chain item matched to a prov item, the item is a *resolve candidate* iff `desired != "" && desired != that item's emitted && validateCustomDomain(desired) == nil`. The resulting domains are deduped by exact string into the resolve set. Two items that share a domain string but have divergent `emitted` values are each evaluated independently; the domain is resolved if *any* matched item needs it. Clearing (`desired == ""`) is never a candidate (clears are never DNS-gated) but is still applied by the main pass.

**Why steady-state lookups vanish:** for an already-emitted-unchanged domain, `desired == emitted`, so it is not a candidate → never resolved. In the main pass, `computeCustomDomainOverrides`'s gate (`desired != "" && desired != emitted && !dnsReady[desired]`, line 125) is short-circuited by `desired != emitted` being false, so `dnsReady[d]` is never read. Identical result, zero DNS.

**Resolution seam:** the off-lock loop calls the existing nil-safe wrapper `b.dnsGateAllows(ctx, d)` (which returns `true` when `b.customDomainDNSReady` is nil — the default-allow used by tests and the cert-disabled path), **not** `customDomainDNSReady` directly (calling that directly would panic on nil and bypass the default).

### Shared chain↔prov matching (the anti-divergence helper)

The pre-pass and `computeCustomDomainOverrides` must agree on which prov item each chain item maps to (the ENG-264 normalized-`ServiceName` match), or a domain could be resolved-but-not-applied (or vice versa). Extract the match into **one** pure read-only helper used by both:

- `matchCustomDomainItems(prov *provision, items []backend.LeaseItem) []matchedDomain` where `matchedDomain` carries exactly `{desired, emitted, serviceName string}`.
- Returns a **slice in chain-item order** (not a map — `ServiceName` is non-unique across providers and a lone item may be `""`), **excluding** unmatched chain items (mirrors the `idx == -1` skip at `computeCustomDomainOverrides:106`).
- It performs the normalized match only — **no validation, no gate** (validation stays in *both* callers as defense-in-depth; the gate stays in `computeCustomDomainOverrides`).
- **Lock precondition:** caller holds `provisionsMu` (read or write), matching the existing convention on `computeCustomDomainOverrides`.

`customDomainDNSCandidates` iterates `matchedDomain`s and collects the deduped resolve set; `computeCustomDomainOverrides` iterates them to validate + gate + build the override map. Its validation/gate logic is otherwise **unchanged** — only its inline match loop is replaced by the shared helper.

## Window safety (the re-validation proof)

Between the pre-pass `RUnlock` and the main `RLock`, `recoverState` can swap `b.provisions[uuid]` (a whole-map swap with fresh pointers, `recover.go:341`). The main pass re-reads fresh `prov` and recomputes the diff; the **only** value carried across the window is `dnsReady`, keyed by the **chain-immutable domain string** (from the `items` argument), not a prov pointer/index or the mutable `ServiceName`.

**Load-bearing invariant:** `dnsReady[desired] == true` means *that exact domain string* resolved ready in the pre-pass, independent of which prov item it later matches; an absent/`false` entry can only collapse `desired → emitted` (a no-op via the `emitted == desired` `continue` at line 131) — it can **never** tear down an emitted domain and **never** apply an unresolved one.

This discharges every interleaving:
- **Domain aliasing** (two matched items share a domain string, one already-emitted and one changed): `dnsReady` dedups by the exact string, so a `true` entry always corresponds to that resolved domain; a missing entry only defers.
- **Match/idx divergence under a swap** (the swap changes which prov item a chain item matches, or reorders): `emitted` is re-read fresh under the main lock and `desired` comes from the immutable `items` argument, so a re-matched item still cannot emit a domain that wasn't resolved.
- **Becomes-a-candidate-in-the-window** (a domain that was already-emitted in the pre-pass has `emitted` changed by the swap, making it a candidate the main pass wants but `dnsReady` lacks): `dnsReady[d]` absent → gate defers it this tick; the **next** tick's pre-pass sees it as a candidate, resolves it, and emits. At worst one extra tick of cert-issuance latency — self-healing, no tear-down.
- **Lease leaves Ready in the window:** the main-pass Ready-gate re-checks under the lock and returns no overrides.

**Out of scope (pre-existing, unchanged):** the *stale-`true` forward* direction — a domain resolved `ready` in the pre-pass that stops pointing here before the main lock is still emitted (the gate doesn't fire) — is identical to today's all-domains-precompute behavior and is not introduced or worsened by this change. The asymmetric gate's guarantee is specifically about never *tearing down* on stale-absent readiness, which holds.

## Why not option 2 (per-domain TTL cache)

A `(domain → ready, at)` memo with a TTL ≥ the reconcile interval only *partially* reduces lookups (miss/expiry still resolves), adds cache state + eviction + TTL-vs-interval coupling, and a stale "ready" entry could drive an emit on regressed DNS. Option 1 eliminates steady-state lookups entirely with **zero** persistent state — the simpler, more idiomatic choice (prefer eliminating the work over caching it).

## Out of scope (deliberately)

Per the focused-PR + premature-optimization + defense-in-depth principles, this PR is **DNS-only**:
- The double `validateCustomDomain` (candidate filter + `computeCustomDomainOverrides`) stays — intentional defense-in-depth right before a value becomes a Traefik label.
- No `normalizedServiceKeys` CPU micro-tuning beyond the shared-match extraction; per-tick CPU on a 5-minute interval is not the measured cost.
- *Within* this change, the main pass is downgraded `Lock`→`RLock` because it is read-only post-#103 (the mutation moved to the actor) — this is choosing the correct lock for the section being restructured and matches the package's read-only `RLock` convention, not an unrelated cleanup.

## Files to change

- **`internal/backend/docker/reconcile_custom_domain.go`** — replace the all-domains precompute with the RLock candidate pre-pass + candidate-only `dnsGateAllows` resolution; add `matchCustomDomainItems` and `customDomainDNSCandidates`; refactor `computeCustomDomainOverrides`'s inline match to use `matchCustomDomainItems` (validation/gate unchanged); both passes use `RLock`.
- **`internal/backend/docker/reconcile_custom_domain_test.go`** — add the tests below. Existing ENG-264/ENG-266 tests must still pass.

## Test strategy

The production DNS seam `b.customDomainDNSReady func(ctx, domain) bool` is already injectable (`backend.go:69`, reached via `dnsGateAllows`) — tests inject a counting/recording func to assert **which** domains are resolved (no test-only production code; reuses the existing DI seam).

1. **Steady state → zero DNS** *(primary acceptance)*. Ready lease whose `prov.Items[].CustomDomain` already equals the chain value. Assert the resolver is called **0 times** and no redeploy.
2. **Candidate-only resolution (multi-domain)**. Lease with one already-emitted domain + one changed domain. Assert the resolver is called for **only** the changed domain — by identity, not just count.
3. **Invalid changed domain is not a candidate**. A *changed* but syntactically-invalid domain → assert resolver called **0 times** (validation short-circuits candidacy) and no override (pins the validate-before-resolve branch).
4. **Not-Ready / not-provisioned → zero DNS** *(behavior change)*. A non-Ready lease: assert resolver called **0 times** (today the all-domains precompute runs before the Ready gate and would resolve; the pre-pass returns first).
5. **Gate unchanged for new/changed domains**. Changed domain DNS-ready → resolved → emitted; changed domain not-ready → resolved → deferred (emitted preserved). Mirrors the existing ENG-266 gate tests via the candidate path.
6. **Deterministic one-tick-deferral + self-heal** *(white-box, no seam — the #103 pattern)*. Modeled on `TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain`: call `customDomainDNSCandidates` while the domain is already-emitted (not a candidate → empty resolve set); run a real `recoverState` swap that changes `emitted` so the domain becomes a candidate; call `computeCustomDomainOverrides` with the (empty) `dnsReady` → assert **deferred** (emitted preserved, no override). Then a second pass: `customDomainDNSCandidates` now returns the domain → resolve → `computeCustomDomainOverrides` → assert it **emits** (self-heal). This pins the one-tick-deferral edge — the most subtle new behavior — deterministically; a `require.Eventually` hammer would essentially never land a swap inside the narrow `RUnlock→RLock` window.
7. **`-race` lock-discipline guard**. Concurrent `ReconcileCustomDomain` + `recoverState` against the same lease (the existing `_ConcurrentRecoverState_NoRace` interleaving), re-asserted to prove the **added RLock pre-pass** introduces no new race, and bounding the resolver-call-count across the run. This is the lock-discipline guard (not the bug discriminator — test 6 owns the window correctness).

## Acceptance criteria

- [x] A reconcile tick for a lease whose custom domain is already emitted and unchanged performs **no** DNS lookups → tests 1, 4.
- [x] A newly-set / changed domain is still resolved and gated exactly as today → tests 2, 3, 5, 6.
- [x] No new races (`-race`); asymmetric gate semantics unchanged (never tear down emitted, never gate a clear) → tests 6, 7 + unchanged `computeCustomDomainOverrides` gate.

## Risks

- **Match divergence** between the pre-pass and the main diff — mitigated by the single shared `matchCustomDomainItems` helper (both passes use it; pinned contract above).
- **Extra RLock pre-pass** per tick — read-only, brief, no I/O; negligible vs the DNS round-trip removed, and `RLock` permits concurrent readers.
- **One-tick deferral** under a `recoverState` swap in the window — acceptable and self-healing (proven above); the gate never tears down an emitted domain.
