# ENG-333 — Restore backend affinity (design)

**Status:** approved approach, pending spec review
**Issue:** ENG-333 (Plan-2 hand-off from ENG-325). Depends on ENG-318 (merged, `RouteForProvision`).
**Branch / worktree:** `worktree-eng-333-restore-backend-affinity`

## Problem

On a multi-backend pool, `RestoreLease` (`internal/api/handlers.go:562`) resolves the backend for the **new (target)** lease via `authenticateAndResolve` → `resolveBackend(newLeaseUUID, sku)`. The new lease has no placement record, so resolution falls back to SKU routing and lands on an arbitrary backend. But a lease's retained volumes + retention record live **only on the backend that originally provisioned the source lease**. So restore succeeds only when the target happens to route to the source's backend (~1/N); otherwise it returns `ErrNotRetained` (404).

Confirmed root facts in current source (`origin/main` @ `8a7b895`, post ENG-325 + ENG-318):

- `Backend.Deprovision(ctx, leaseUUID) error` returns **only an error** — fred gets no signal whether the backend retained. Retention is created entirely backend-side (`internal/backend/docker/deprovision.go` `PutActiveMerged` branch). There is no `CallbackStatusRetained`; only a best-effort `ProvisionStatusRetained` *event* to the tenant at close (`handler_set.go:189`).
- `orchestrator.Deprovision` deletes placement **unconditionally** on close (`orchestrator.go:219`, fallback `:257`).
- Restore is a **direct synchronous** `backendClient.Restore(...)` from the API handler. It is **not** in-flight-tracked and has **no** provisioner-side callback.
- `StartProvisioning` / `doStartProvisioning` call `RouteForProvision` and **ignore existing placement**, so a re-provision/reconcile of a restored lease can drift to a different backend and lose its data.
- The reconciler already **derives placement from backend ground truth** every sweep for active provisions (`reconciler.go:206`, `SetBatch`) — but only additively (it never prunes), and only for active provisions (not retentions).
- Grace-reap is **backend-autonomous** (`retention.go` `ReapIfExpired`/`ListExpired`, no fred notification).
- Backends expose `GET /provisions` (active) but **no** retention-enumeration endpoint. Retention/restore is **docker-only**; k3s has no `/restore`.

## Global architecture & the governing invariant

| Layer | Role |
|---|---|
| Chain | desired-state authority (which leases exist; ACTIVE/CLOSED) — etcd/spec analog |
| Backends (docker/k3s) | data-plane actual state, **incl. the authoritative location of pinned data**: active provisions + docker retention records — kubelet/node analog |
| fred placement store (bbolt, single-writer) | a **derived index** lease→backend — controller local-cache analog |
| fred reconciler | level-triggered loop that re-derives placement from backend ground truth each sweep — controller reconcile loop |

**Governing invariant (idiomatic, and already fred's model for active leases):** the placement index is **derived and reconciled from data-plane ground truth by reading all needed state each loop**; edge signals (close-time, restore-success) are **latency optimizations, never the source of truth**. This is the Kubernetes "edge-triggered as optimization, level-triggered as truth" pattern (controller-runtime FAQ; K8s garbage collection).

This design extends that invariant uniformly to **retained** leases — it does not introduce a parallel, edge-only mechanism for them. The authoritative-placement (vs scatter-gather discovery) choice mirrors Kubernetes local-PV `nodeAffinity`: location is durable metadata on the data, consulted at routing time.

Rejected alternatives: async `CallbackStatusRetained` (adds a delete-then-reset race for synchronously-available info); retention discovery / fan-out on restore (less idiomatic given an existing index; needs scatter-gather for no gain).

## Design

### #1 Retain signal → preserve placement at close (edge optimization)
- Extend the `Backend` interface: `Deprovision(ctx, leaseUUID) (retained bool, err error)`.
  - **docker:** thread the retain boolean out of `doDeprovision` (known at the `PutActiveMerged` branch) through the lease-actor reply (`DeprovisionMsg`) → `/deprovision` response JSON (`retained`).
  - **HTTPClient:** parse `retained` from the deprovision response.
  - **k3s:** always `false`. **MockBackend:** configurable setter.
- `orchestrator.Deprovision`: when `retained`, **skip** `placementStore.Delete` (`:219` and fallback `:257`). `retained` must default to `false` on any error/uncertain path (fail toward cleanup — the reconciler re-derives the truth next sweep anyway).
- This gives an immediate, correct index right after close; it is **not** the sole authority (see #5).

### #2 Route restore via the source's placement
- In `RestoreLease`: authenticate the **new** lease, read `from_lease_uuid`, then resolve via `placementLookup.Get(from_lease_uuid)` → `backendRouter.GetBackendByName(name)`.
  - No placement, or named backend missing ⇒ `ErrNotRetained` (404). Never silently round-robin to a wrong backend (that reintroduces the bug).
- Refactor `authenticateAndResolve` into an auth-only helper for restore (so it does not resolve a backend by the *new* lease).

### #3 Honor placement before routing on (re)provision
- `orchestrator.StartProvisioning` and `reconciler.doStartProvisioning`: before `RouteForProvision`, check `placementStore.Get(lease.Uuid)`; route there if present and the backend exists, else `RouteForProvision`.
- Prevents a restored (or any already-placed) lease drifting off its data. In the reconcile loop this is consistent because placement is synced from backend state **before** provisioning decisions (`reconciler.go:206` already runs first).

### #4 Restore-success placement bookkeeping (optimistic, owned by the state owner)
- On successful `Restore`, optimistically record `placement[new_lease] = backend` and delete `placement[source]`.
- **Ownership:** the write is owned by the **orchestrator** (a small method the restore handler calls), not the API handler — keep the API layer a stateless router; the state owner does optimistic updates. Typed-nil safe.
- This only closes the post-restore latency window; the reconciler (#5) is the authority that converges regardless.

### #5 Reconciler derives placement from backend ground truth — incl. retentions (the authority)
- Add `GET /retentions` to backends (docker: from `RetentionStore.List()` → lease UUIDs; k3s: empty). New `Backend` method + `HTTPClient` + `MockBackend`.
- Extend the reconciler's existing placement sync to read **active provisions ∪ retentions** per backend and reconcile placement to that union — making retained-lease placement self-healing (covers a missed close-signal, a providerd crash, and grace-reap orphan GC uniformly).
- **Pruning safety (K8s-GC conservatism):** only prune a placement when the reconciler has **successfully** queried every backend this sweep and the lease appears in none. If any backend query fails, **skip pruning** this sweep (add-only), so a transient backend outage cannot wipe valid placements.
- Lazy on-access cleanup (delete `placement[source]` when a restore hits `ErrNotRetained`) is an optional cheap complement, not required for soundness.

## Control flow (happy path, multi-backend)
1. Source provisioned on **B2** → `placement[source]=B2`.
2. Close with retain → `B2.Deprovision` retains, returns `retained=true` → orchestrator keeps `placement[source]=B2` (and the reconciler would re-derive it from B2's `/retentions` regardless).
3. New PENDING lease **L_new**; `POST /v1/leases/L_new/restore {from_lease_uuid: source}`.
4. Handler authenticates L_new, resolves `placement[source]=B2`, calls `B2.Restore(...)`.
5. Success: orchestrator records `placement[L_new]=B2`, deletes `placement[source]`. Reconciler converges to the same from B2's `/provisions` (L_new active) and `/retentions` (source consumed).
6. Later re-provision/reconcile of L_new honors `placement[L_new]=B2`.

## Error handling
- Source has no retained placement (never retained / already adopted / reaped) ⇒ `ErrNotRetained` (404).
- Named placement backend missing from the router ⇒ unavailable ⇒ 404 (no silent round-robin).
- `Deprovision` retain-signal defaults to `false` on error; the reconciler corrects any resulting under/over-cleanup next sweep.
- Backend `/retentions` query failure ⇒ add-only reconcile (no prune) that sweep.

## Testing strategy
- **Affinity must not depend on fallback order** (ENG-318 least-loaded / RR non-determinism): pin `placement[source]` to a backend and assert restore routes there even when `RouteForProvision` would pick a *different* one (mock load stats so least-loaded ≠ source).
- Unit: `orchestrator.Deprovision` keeps placement iff `retained`; `StartProvisioning`/`doStartProvisioning` honor existing placement; `RestoreLease` routes by source placement and 404s when absent; orchestrator restore-bookkeeping sets/clears placement.
- Reconciler: placement derived from `provisions ∪ retentions`; **prune skipped when a backend query fails** (self-heal + safety); grace-reaped retention ⇒ placement dropped after a full successful sweep; missed close-signal ⇒ placement re-derived (soundness regression test).
- Backend: docker `Deprovision` returns `retained=true` on a retaining close else `false`; `GET /retentions` lists retained leases; HTTPClient round-trips both; k3s/Mock conform.
- Integration (docker): retain-close → restore lands on the source backend on a multi-backend router.
- Out of repo: manifest-loadtest restore scenarios 14 (functional gate) then 15/16 on the dev pool — external validation.

## Acceptance criteria mapping
- Restore routes to the source's backend, independent of new-lease routing → #2 (index from #1/#5).
- Source placement survives retain-close; cleaned up on adopt-success → #1, #4; grace-reap → #5 (reconciler).
- Source has no retained placement ⇒ `ErrNotRetained` (404) → #2.
- Restart/reconcile of a restored lease stays on the same backend → #3.
- loadtest scenarios 14/15/16 green → external validation.

## Out of scope / follow-ups
- Cross-backend data migration (restore stays same-backend; source backend gone ⇒ 404).
- The least-loaded router itself (ENG-318, merged).
