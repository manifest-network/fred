# ENG-333 — Restore backend affinity (design)

**Status:** approved approach, pending spec review
**Issue:** ENG-333 (Plan-2 hand-off from ENG-325). Depends on ENG-318 (merged, `RouteForProvision`).
**Branch / worktree:** `worktree-eng-333-restore-backend-affinity`

## Problem

On a multi-backend pool, `RestoreLease` (`internal/api/handlers.go:562`) resolves the backend for the **new (target)** lease via `authenticateAndResolve` → `resolveBackend(newLeaseUUID, sku)`. The new lease has no placement record, so resolution falls back to SKU routing and lands on an arbitrary backend. But a lease's retained volumes + retention record live **only on the backend that originally provisioned the source lease**. So restore succeeds only when the target happens to route to the source's backend (~1/N); otherwise it returns `ErrNotRetained` (404).

Confirmed root facts in current source (`origin/main` @ `8a7b895`, post ENG-325 + ENG-318):

- `orchestrator.Deprovision` deletes placement on close (`orchestrator.go:219`, fallback `:257`) — **but only after a *successful* backend deprovision; a failed deprovision already keeps placement.**
- Restore is a **direct synchronous** `backendClient.Restore(...)` from the API handler. It is **not** in-flight-tracked and has **no** provisioner-side callback.
- `StartProvisioning` / `doStartProvisioning` call `RouteForProvision` and **ignore existing placement**, so a re-provision/reconcile of a restored lease can drift to a different backend and lose its data.
- The reconciler already **derives placement from backend ground truth** every sweep for active provisions (`reconciler.go:206`, `SetBatch`), but **additive-only**: it never prunes, with an in-code rationale — *"a concurrent StartProvisioning may have just Set a placement that backends haven't reported yet."* It covers active provisions only, not retentions.
- **Default reconcile interval is 5 minutes** (`reconciler.go:104`, `cmp.Or(cfg.Interval, 5*time.Minute)`). This window size is why edge optimizations below are retained.
- `placementStore.Count()`/`List()` have **no production consumers** (only an unrelated payload store uses `.Count()`), so a lingering placement for a closed lease is harmless.
- Grace-reap is **backend-autonomous** (`retention.go` `ReapIfExpired`/`ListExpired`, no fred notification).
- Backends expose `GET /provisions` (active) but **no** retention-enumeration endpoint. Retention/restore is **docker-only**; k3s has no `/restore`.

## Global architecture & the governing invariant

| Layer | Role |
|---|---|
| Chain | desired-state authority (which leases exist; ACTIVE/CLOSED) — etcd/spec analog |
| Backends (docker/k3s) | data-plane actual state, **incl. the authoritative location of pinned data**: active provisions + docker retention records — kubelet/node analog |
| fred placement store (bbolt, single-writer) | a **derived index** lease→backend — controller local-cache analog |
| fred reconciler | level-triggered loop that re-derives placement from backend ground truth each sweep — controller reconcile loop |

**Governing invariant (idiomatic, already fred's model for active leases):** the placement index is **derived and reconciled from data-plane ground truth**; the **reconciler is the single authority** (sole writer of record + sole pruner). Edge writes (restore-success bookkeeping; not-deleting-on-close) are **latency optimizations** that close the up-to-5-minute reconcile window — never a parallel source of truth. This is the Kubernetes "edge as optimization, level as truth" pattern, and the authoritative-placement (vs scatter-gather discovery) choice mirrors local-PV `nodeAffinity`.

**Rejected alternatives:**
- **Changing `Backend.Deprovision` to return `(retained bool, err error)`** — an invasive change to a core, 4-implementer interface to carry a flag (un-idiomatic per Go convention + interface-segregation); unnecessary because placement survives close simply by *not deleting it* (see #1).
- **Async `CallbackStatusRetained`** — adds a delete-then-reset race for synchronously-knowable state.
- **Retention discovery / fan-out on restore** — less idiomatic given an existing index; scatter-gather for no gain.

## Design

### #1 Placement survives close (no interface change)
- `orchestrator.Deprovision`: **remove the `placementStore.Delete` calls** (`:219`, `:257`). A closed lease's placement persists until the reconciler prunes it (#5). This merely extends Deprovision's *existing* behavior on a failed deprovision (which already keeps placement) to the success path.
- No `Backend` interface change, no actor/wire threading. `placement[source]` is available immediately for a restore right after close — no reconcile-window gap.

### #2 Route restore via the source's placement
- In `RestoreLease`: authenticate the **new** lease, read `from_lease_uuid`, then resolve via `placementLookup.Get(from_lease_uuid)` → `backendRouter.GetBackendByName(name)`.
  - No placement, or named backend missing ⇒ `ErrNotRetained` (404). Never silently round-robin to a wrong backend (that reintroduces the bug).
- Refactor `authenticateAndResolve` into an auth-only helper for restore (so it does not resolve a backend by the *new* lease).

### #3 Honor placement before routing on (re)provision
- `orchestrator.StartProvisioning` and `reconciler.doStartProvisioning`: before `RouteForProvision`, check `placementStore.Get(lease.Uuid)`; route there if present and the backend exists, else `RouteForProvision`.
- Prevents a restored (or any already-placed) lease drifting off its data. In the reconcile loop this is consistent because placement is synced from backend state **before** provisioning decisions (`reconciler.go:206` already runs first).

### #4 Restore-success bookkeeping (optimistic, owned by the orchestrator)
- On successful `Restore`, optimistically record `placement[new_lease] = backend` and delete `placement[source]`.
- **Justified by the 5-minute reconcile interval:** without it, a freshly-restored lease would mis-route reads/restarts for up to one sweep. **Owned by the orchestrator** (a small method the restore handler calls), not the API handler — the state owner does optimistic updates; the API layer stays a stateless router. Typed-nil safe.
- The reconciler (#5) converges to the same result regardless, so this is purely a window-closer.

### #5 Reconciler = single authority (derive from provisions ∪ retentions; sole pruner)
- Add `GET /retentions` to backends (docker: from `RetentionStore.List()` → lease UUIDs; k3s: empty). New `Backend` method + `HTTPClient` + `MockBackend`.
- Extend the reconciler's placement sync to read **active provisions ∪ retentions** per backend → makes retained-lease placement self-healing (missed edge write, providerd restart, grace-reap GC — uniformly).
- **Introduce pruning, gated to answer the documented additive-only hazard.** Prune `placement[L]` only when **all** hold:
  - (a) **every** backend was queried **successfully** this sweep (no prune on partial failure);
  - (b) L is absent from every backend's `provisions ∪ retentions`;
  - (c) L is **terminal on chain** (not PENDING/ACTIVE) — this protects the concurrent-`StartProvisioning` Set the existing comment warns about, since an in-flight lease is non-terminal;
  - (d) L is not currently in-flight.
- Pruning is idempotent and commutative across sweeps (reconciliation correctness).

## Control flow (happy path, multi-backend)
1. Source provisioned on **B2** → `placement[source]=B2`.
2. Close with retain → `B2.Deprovision` retains; orchestrator **does not delete** `placement[source]` (survives immediately; reconciler would also re-derive it from B2's `/retentions`).
3. New PENDING lease **L_new**; `POST /v1/leases/L_new/restore {from_lease_uuid: source}`.
4. Handler authenticates L_new, resolves `placement[source]=B2`, calls `B2.Restore(...)`.
5. Success: orchestrator records `placement[L_new]=B2`, deletes `placement[source]`. Reconciler converges identically from B2's `/provisions` (L_new active) and `/retentions` (source consumed).
6. Later re-provision/reconcile of L_new honors `placement[L_new]=B2`.
7. Source never restored → grace-reaped on B2 → drops from `/retentions` → reconciler prunes `placement[source]` (terminal on chain + absent from all backends, full successful sweep).

## Error handling
- Source has no retained placement (never retained / already adopted / reaped) ⇒ `ErrNotRetained` (404).
- Named placement backend missing from the router ⇒ unavailable ⇒ 404 (no silent round-robin).
- Backend `/retentions` query failure ⇒ **add-only reconcile (no prune)** that sweep.
- A non-retained closed lease's placement lingers until the reconciler prunes it (harmless; no `Count()`/`List()` consumers; reads validate-on-access and fall back).

## Testing strategy
- **Affinity must not depend on fallback order** (ENG-318 least-loaded / RR non-determinism): pin `placement[source]` to a backend and assert restore routes there even when `RouteForProvision` would pick a *different* one (mock load stats so least-loaded ≠ source).
- Unit: `orchestrator.Deprovision` no longer deletes placement; `StartProvisioning`/`doStartProvisioning` honor existing placement; `RestoreLease` routes by source placement and 404s when absent; orchestrator restore-bookkeeping sets/clears placement.
- Reconciler: placement derived from `provisions ∪ retentions`; **prune gates** — no prune on partial backend-query failure; no prune of PENDING/ACTIVE leases; no prune of in-flight leases; prune only chain-terminal leases absent everywhere after a full successful sweep; grace-reaped retention ⇒ placement dropped; missed edge write ⇒ placement re-derived (soundness regression test).
- Backend: `GET /retentions` lists retained leases (docker real, k3s empty); HTTPClient round-trips it; Mock conforms.
- Integration (docker): retain-close → restore lands on the source backend on a multi-backend router.
- Out of repo: manifest-loadtest restore scenarios 14 (functional gate) then 15/16 on the dev pool — external validation.

## Acceptance criteria mapping
- Restore routes to the source's backend, independent of new-lease routing → #2 (index from #1/#5).
- Source placement survives retain-close; cleaned up on adopt-success → #1, #4; grace-reap → #5 (reconciler prune).
- Source has no retained placement ⇒ `ErrNotRetained` (404) → #2.
- Restart/reconcile of a restored lease stays on the same backend → #3.
- loadtest scenarios 14/15/16 green → external validation.

## Out of scope / follow-ups
- Cross-backend data migration (restore stays same-backend; source backend gone ⇒ 404).
- The least-loaded router itself (ENG-318, merged).
- Making the reconcile interval / restore-window configurable (only if the 5-min default proves too coarse in the loadtest).
