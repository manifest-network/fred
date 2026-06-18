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

**Remove Case 2** (the `o.router.Route(skuHint)` → `defaultBackend` fallback). Case 2 is the anti-pattern: it **guesses** an unknown location and acts on the guess. When neither Case 0 nor Case 1 positively resolves, fall through to **Case 3 (deprovision from all backends)**. This is idempotent — deprovisioning a lease that does not exist on a backend succeeds as a no-op — so the real backend is soft-deleted (creating the retention record immediately, restore settles in ~6 s) while the others are harmless no-ops. This is the idiomatic reconciler stance: never act on a guessed state; use idempotent operations that converge regardless of where the resource actually lives. Single-backend deployments are unaffected (all-backends == the one backend).

`skuHint` is no longer consumed by `Deprovision`; remove it from the signature and its caller (`handler_set.go processLeaseClose`) if it becomes unused, or leave the extraction in place if other call paths need it (decide during implementation — favor full removal if it goes vestigial).

**Improved all-backends logging (in scope):** in the Case 3 loop, distinguish the backend that *actually held* the lease (real soft-delete) from the no-op backends, so a future stranding is diagnosable from logs alone. Emit a single summary line naming the holding backend (or "none found").

### Part B — Prune grace window (`internal/provisioner/placement/store.go` + `reconciler.go`)

**Placement store schema:** add a per-record `SetAt time.Time` (first-seen time).

- In-memory cache type changes from `map[string]string` to `map[string]record` where `record = struct{ backend string; setAt time.Time }`.
- bbolt value changes from a raw backend-name string to JSON `{"backend":"docker-2","set_at":"2026-06-18T17:11:15Z"}` (`time.Time` marshals to RFC3339 — human-readable in the DB for debugging). On load, the format is discriminated by the **first byte**: `'{'` ⇒ new JSON record; anything else ⇒ **legacy** raw backend string, loaded as `backend = rawString, SetAt = zero`. Backend names never start with `'{'`, so the discriminator is unambiguous (more robust than try-unmarshal-and-catch). Legacy entries predate this change and are settled, so a zero `SetAt` correctly makes them immediately prunable.
- `Get(leaseUUID) string` is **unchanged** (returns the backend name) so existing readers don't move.
- Add `SetAt(leaseUUID) (time.Time, bool)` accessor.
- Clock seam: follow the repo's existing convention (`internal/api/callback_auth.go` uses `nowFunc func() time.Time` defaulted to `time.Now`). Add `now func() time.Time` to `Store`, **always set to `time.Now` in `NewStore`** (never nil in prod — a real DI dependency, not a nil-in-prod test hook), overridable in tests via a functional option `WithClock(func() time.Time)` on `NewStore`.
- `Set` stamps `SetAt = s.now().UTC()` (UTC canonicalizes storage **and** strips the monotonic clock reading, so the in-memory value matches the JSON-reloaded value — comparisons must use `time.Time.Equal`, never `==`). `SetBatch` (reconciler additive sync) **preserves** an existing record's `SetAt` and only stamps `s.now().UTC()` for entries it creates — `SetAt` means "when we first learned this placement", and must not be reset every sweep (analogous to an immutable `creationTimestamp`). A corrupt `{`-prefixed value is dropped (empty record), not misread as a backend name.
- Extend the `PlacementStore` interface(s) consumed by the reconciler (and orchestrator, if it shares one) with the `SetAt` accessor.

**Pruner gate:** `ReconcileAll` already captures `startTime := time.Now()` at sweep start (`reconciler.go:152`). Thread that sweep-reference time into `cleanupOrphanedPlacements(..., now time.Time)` and, before `Delete`, skip any placement whose age `now − SetAt < 2 × r.interval` (`r.interval` defaults to 5 min). Using the sweep-start time (rather than an inline `time.Now()`) keeps the grace check deterministic in tests and is the precise reference — it is the moment the stale snapshot was taken. A grace comfortably larger than one sweep eliminates the stale-snapshot race; because lingering placement entries strand nothing (`processOrphan` independently GCs the real backend resource, and a closed lease is never restored), erring generous is safe.

## Why grace window over live re-verify

The placement is a *derived local index*, not the source of truth. "Minimum age before treating as orphaned" is a standard reconciler idiom (k8s `node-monitor-grace-period`, pod-eviction-timeout, `minimum-container-ttl-duration`, TTL-after-finished). The alternative idiom — the k8s GC controller's live re-verify before delete (`absentOwnerCache`, born from stale-cache TOCTOU #88097) — adds per-placement backend/chain queries to an already-slow sweep and still races a just-provisioned lease the backend hasn't surfaced. Grace window is simpler and has no downside here.

## Industry best-practices validation (researched 2026-06-18)

Each decision was checked against established practice:

- **Idempotent, no-guess deprovision (Part A).** Reconciler guidance is explicit: "implement reconcile to be declarative and idempotent … avoid relying on guessing unknown state." Removing the `defaultBackend` guess and converging via idempotent all-backends deletes is exactly this — safe to retry, self-healing. (k8s reconciliation principle; controller-runtime good practices.)
- **Minimum-age GC grace (Part B pruner).** Retaining derived/garbage state for a grace period before final removal is pervasive: Git keeps dangling objects < 2 weeks, Cassandra's `gc_grace_seconds` retains tombstones (default 10 days) precisely to avoid races, generational GC favors keeping young objects. `2 × reconcile interval` is the same idiom scaled to sweep cadence.
- **Clock dependency injection (Part B testability).** The canonical pattern is to accept a clock seam rather than call `time.Now()` inline (`k8s.io/utils/clock`, `jonboulle/clockwork` — both already indirect deps). We deliberately use the repo's lighter, already-established `func() time.Time` convention (`callback_auth.go`) for consistency, since we only need `Now()`, not timers.
- **Additive schema evolution (Part B store).** Adding an optional field with a default and a clean legacy discriminator is the recommended backward-compatible change; JSON is self-describing and forward-compatible (future fields ignored by old readers). Legacy raw-string values decode deterministically via the first-byte check. JSON is the idiomatic bbolt value encoding for a small struct ("dead simple", human-readable in the DB); protobuf/gob are for size- or hot-path-critical data, which a per-provision placement record is not.
- **`time.Time` persistence (Part B store).** Go strips the monotonic clock reading on JSON (de)serialization, so we stamp `SetAt` in UTC (which also strips it) for consistency across the persist boundary and compare with `time.Time.Equal` in tests, per the standard guidance.

Sources: [k8s reconciliation principle](https://www.chainguard.dev/unchained/the-principle-of-reconciliation), [controller-runtime good practices](https://book.kubebuilder.io/reference/good-practices), [k8s GC docs](https://kubernetes.io/docs/concepts/architecture/garbage-collection/), [Cassandra gc_grace_seconds](https://murukeshm.github.io/cassandra/3.9/operating/compaction.html), [k8s.io/utils/clock](https://pkg.go.dev/k8s.io/utils/clock), [schema-evolution backward compat](https://www.dataexpert.io/blog/backward-compatibility-schema-evolution-guide), [BoltDB serialization (B. Johnson)](https://github.com/benbjohnson/application-development-using-boltdb), [Go time wall vs monotonic](https://pkg.go.dev/time#hdr-Monotonic_Clocks), [functional options (golang.design)](https://golang.design/research/generic-option/).

## Testing (TDD, test-first)

- **Orchestrator:** placement-missing + not-in-flight close → asserts `Deprovision` attempts **all** backends (reproduces the docker-1 phantom), and the holding backend is soft-deleted; positively-resolved cases still deprovision exactly one backend.
- **Reconciler:** place a record at `T0` (store clock), lease chain-terminal + absent from the (stale) snapshot, not in-flight → calling the pruner with `now = T0 + interval` asserts **NOT pruned** (within grace); calling with `now = T0 + 2×interval + ε` asserts **pruned**.
- **Placement store:** `SetAt` round-trips; legacy string value loads with zero `SetAt`; `SetBatch` preserves existing `SetAt`, stamps new entries.
- `go test ./... -race`.

## Touchpoints

- `internal/provisioner/orchestrator.go` — `Deprovision` (drop Case 2, all-backends fallback, holding-backend logging).
- `internal/provisioner/handler_set.go` — `processLeaseClose` (skuHint plumbing if it goes unused).
- `internal/provisioner/placement/store.go` — `SetAt`, clock seam, record encoding, legacy load, `SetBatch` preserve-semantics.
- `internal/provisioner/reconciler.go` — `cleanupOrphanedPlacements` grace gate using `r.interval`; `PlacementStore` interface extension.
- Tests alongside each.
