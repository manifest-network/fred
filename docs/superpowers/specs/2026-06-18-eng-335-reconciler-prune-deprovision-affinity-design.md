# ENG-335 — Reconciler prunes in-flight placements → close deprovisions the wrong backend

- **Issue:** ENG-335 (High, Bug/Infrastructure)
- **Branch:** `worktree-felix+eng-335`
- **Related:** ENG-333 (restore backend affinity, done), ENG-325 (soft-delete + restore), ENG-329, ENG-331
- **Date:** 2026-06-18

## Problem

Under sustained restore load, two coupled defects strand retained volumes on the wrong backend and inflate restore latency:

1. **Reconciler prunes live placements (trigger).** `cleanupOrphanedPlacements` decides a placement is orphaned using data snapshotted at the *start* of a long reconcile sweep (the sweep is slow because it enumerates ~3,800 cross-backend retentions). A lease that provisions *entirely within one sweep* is absent from that stale snapshot (chain + provisions), and its only current-time guard — the in-flight check — misses because the provision has already completed and been popped from the in-flight tracker. The placement is deleted even though the lease is live.

2. **Close deprovisions the wrong backend (harm).** `ProvisionOrchestrator.Deprovision` resolves the backend as: Case 0 placement → Case 1 in-flight → **Case 2 SKU-route → `defaultBackend`** → Case 3 all-backends. With the placement pruned (1) and the provision no longer in-flight, Case 2 routes to `defaultBackend` (docker-1). In a least-loaded multi-backend pool a SKU is not pinned to one backend, so this is a **phantom no-op** reported as success: the real container/volume on docker-2/3 is untouched and stranded until the next ~2-min reconcile sweep finds it as an orphan and tears it down. The retention record is therefore created ~110 s late, so restore 404s ("retention not ready") for that window, blowing the scenario-16 `dropped_iterations` threshold.

This is the same wrong-backend-fallback class ENG-333 fixed on the **restore** path; here it manifests on the **close/deprovision** path, triggered by the prune.

## Goals / non-goals

- **Goal:** Close resolves the lease's actual backend (or fails safe across all backends) — never a phantom no-op against `defaultBackend`.
- **Goal:** The reconciler does not prune a placement for a lease that was placed during/just-before the current sweep.
- **Non-goal:** The cross-backend retention inventory cost (~3,800 volumes) that makes sweeps slow and widens the race window — noted as a **separate perf follow-up** (file against the dev provider retention inventory). No code change here.

## Design

### Part A — Deprovision fail-safe (`internal/provisioner/orchestrator.go`, `Deprovision`)

Only deprovision a **single** backend when it is **positively resolved**:

- **Case 0 (placement)** — `placementStore.Get(leaseUUID)` resolves to a configured backend, or
- **Case 1 (in-flight)** — `PopInFlight` returns a tracked backend.

**Remove Case 2** (the `o.router.Route(skuHint)` → `defaultBackend` fallback). When neither Case 0 nor Case 1 positively resolves, fall through to **Case 3 (deprovision from all backends)**. This is idempotent — deprovisioning a lease that does not exist on a backend succeeds as a no-op — so the real backend is soft-deleted (creating the retention record immediately, restore settles in ~6 s) while the others are harmless no-ops. Single-backend deployments are unaffected (all-backends == the one backend).

`skuHint` is no longer consumed by `Deprovision`; remove it from the signature and its caller (`handler_set.go processLeaseClose`) if it becomes unused, or leave the extraction in place if other call paths need it (decide during implementation — favor full removal if it goes vestigial).

**Improved all-backends logging (in scope):** in the Case 3 loop, distinguish the backend that *actually held* the lease (real soft-delete) from the no-op backends, so a future stranding is diagnosable from logs alone. Emit a single summary line naming the holding backend (or "none found").

### Part B — Prune grace window (`internal/provisioner/placement/store.go` + `reconciler.go`)

**Placement store schema:** add a per-record `SetAt time.Time` (first-seen time).

- bbolt value changes from a raw backend-name string to a small encoded record (e.g. JSON `{"backend":...,"set_at":...}`). On load, a value that does not decode as the new format is treated as **legacy**: `backend = rawString`, `SetAt = zero`. Legacy entries predate this change and are settled, so a zero `SetAt` correctly makes them immediately prunable.
- `Get(leaseUUID) string` is **unchanged** (returns the backend name) so existing readers don't move.
- Add `SetAt(leaseUUID) (time.Time, bool)` accessor.
- Add a `now func() time.Time` clock seam to the store, defaulting to `time.Now` — a real DI seam for deterministic tests, not a test-only hook.
- `Set` stamps `SetAt = now()`. `SetBatch` (reconciler additive sync) **preserves** an existing record's `SetAt` and only stamps `now()` for entries it creates — `SetAt` means "when we first learned this placement", and must not be reset every sweep.
- Extend the `PlacementStore` interface(s) consumed by the reconciler (and orchestrator, if it shares one) with the `SetAt` accessor.

**Pruner gate:** in `cleanupOrphanedPlacements`, before `Delete`, skip any placement whose age `now() − SetAt < 2 × r.interval` (`r.interval` defaults to 5 min). A grace comfortably larger than one sweep eliminates the stale-snapshot race; because lingering placement entries strand nothing (`processOrphan` independently GCs the real backend resource, and a closed lease is never restored), erring generous is safe. The reconciler passes its clock into the pruner so the grace check is deterministically testable.

## Why grace window over live re-verify

The placement is a *derived local index*, not the source of truth. "Minimum age before treating as orphaned" is a standard reconciler idiom (k8s `node-monitor-grace-period`, pod-eviction-timeout, `minimum-container-ttl-duration`, TTL-after-finished). The alternative idiom — the k8s GC controller's live re-verify before delete (`absentOwnerCache`, born from stale-cache TOCTOU #88097) — adds per-placement backend/chain queries to an already-slow sweep and still races a just-provisioned lease the backend hasn't surfaced. Grace window is simpler and has no downside here.

## Testing (TDD, test-first)

- **Orchestrator:** placement-missing + not-in-flight close → asserts `Deprovision` attempts **all** backends (reproduces the docker-1 phantom), and the holding backend is soft-deleted; positively-resolved cases still deprovision exactly one backend.
- **Reconciler:** placement `SetAt = now`, lease chain-terminal + absent from the (stale) snapshot, not in-flight → asserts **NOT pruned** within grace; advance the injected clock past `2 × interval` → asserts **pruned**.
- **Placement store:** `SetAt` round-trips; legacy string value loads with zero `SetAt`; `SetBatch` preserves existing `SetAt`, stamps new entries.
- `go test ./... -race`.

## Touchpoints

- `internal/provisioner/orchestrator.go` — `Deprovision` (drop Case 2, all-backends fallback, holding-backend logging).
- `internal/provisioner/handler_set.go` — `processLeaseClose` (skuHint plumbing if it goes unused).
- `internal/provisioner/placement/store.go` — `SetAt`, clock seam, record encoding, legacy load, `SetBatch` preserve-semantics.
- `internal/provisioner/reconciler.go` — `cleanupOrphanedPlacements` grace gate using `r.interval`; `PlacementStore` interface extension.
- Tests alongside each.
