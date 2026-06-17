# ENG-329 — A tenant whose lease auto-closed can't learn their data is retained

**Status:** spec for review (not yet implemented) — **Rev 3** (pull-side gaps resolved after a second best-practices review; design converged)
**Date:** 2026-06-17
**Parent:** ENG-325 (soft-delete + restore), PR #114
**Worktree/branch:** `.claude/worktrees/eng-329-retained-notice` / `eng-329-retained-notice` (off `main` @ 74e87f8)

---

## Problem

ENG-325 soft-deletes a lease's data on close/expire/auto-close (`RenameVolume` into the
`fred-retained-` namespace, 90-day grace, restorable on the same node). The tenant is
supposed to be told so they can restore before the grace reaper deletes it.

Two defects:

1. **Auto-close never notifies.** `Manager.PublishLeaseEvent` (`manager.go:331-343`) drops
   `LeaseAutoClosed` via `default: return nil`, so credit-exhausted leases never reach
   `processLeaseClose` (`handler_set.go:189`) — the only notice emitter. Their teardown
   falls to the reconciler orphan sweep (`reconciler.go:985`), which retains data silently.
2. **The notice is fired on intent, not outcome.** `handler_set.go:189` emits *before*
   deprovision runs, "regardless of whether the backend actually retained" (the `:187`
   comment admits providerd can't know the outcome at close time).

**The deeper finding (best-practices review + code trace) — the real gap is the last hop.**
The tenant-facing notice is delivered **only** over a live WebSocket via the in-memory
`EventBroker` (`broker.go:122-140`): no persistence, no replay, **silently dropped when no
socket is subscribed** for that lease. The credit-exhausted cohort is **offline by
construction** (the lease lapsed for non-payment). And there is **no pull endpoint** to
rediscover retention: `GET /status` only fetches provision status for `ACTIVE` leases
(`handlers.go:378`); `GET /provision` returns 404/`Failed` after deprovision; the only
retention-aware route is `POST /restore`, which requires *creating a fresh paid lease
first*. So an offline tenant can **never** learn their data is retained, however reliable
we make the internal hops.

Per the Kubernetes principle — **machine-readable, decision-driving facts belong in a
queryable, level-readable `.status`; Events are explicitly best-effort and a missed Event
must never cause permanent divergence** — the fix is a *queryable retention status*, with
the push demoted to an optimization. (k8s api-conventions; event-driven.io outbox/inbox;
microservices.io idempotent-consumer. A prior draft of this spec tried to make the *push*
reliable via a callback `Notified` marker + reaper re-drive; the review correctly flagged
that as (a) a premature-ack bug — the marker would flip on providerd's fire-and-forget 2xx
while the tenant saw nothing — and (b) hardening the wrong hop. That approach is dropped.)

## Design — pull is the source of truth; push is a best-effort optimization

### Part A — Queryable retention status (pull) — REQUIRED, the source of truth

The retained fact is **already durable** in the backend's `RetentionEntry`. Expose it so an
offline tenant can self-serve on reconnect, indefinitely within the grace window. Reuses the
existing `GetProvision` wire path — **no new `Backend` interface method**. This matches the
AIP-164 rule that a single-resource Get SHOULD return a soft-deleted resource (with a state +
deadline) rather than 404, and the AWS/GCP/Azure soft-delete model.

1. `internal/backend/client.go`: add to `ProvisionInfo` — `RetainedUntil time.Time
   \`json:"retained_until,omitempty"\``, and `Tenant string \`json:"tenant,omitempty"\``. NOTE:
   `ProvisionInfo` IS the backend→providerd wire format (`HTTPClient.GetProvision` unmarshals it),
   so `Tenant` must be `json:"tenant"` (NOT `json:"-"`) to reach providerd for the #5 authz
   fallback — this rides the trusted, HMAC-signed internal hop exactly as `RestoreRequest.Tenant`
   (`json:"tenant"`) already does. It is kept OFF the tenant-facing responses by simply not copying
   it into `LeaseStatusResponse`/`LeaseProvisionResponse` (separate types). The
   `ProvisionStatusRetained` const already exists (client.go:349).
2. `internal/backend/docker/info.go` — `GetProvision` (131-164): when the lease is not in the
   in-memory map, **check `b.retentionStore.Get(leaseUUID)` before the diagnostics fallback**;
   if a record exists return
   `ProvisionInfo{Status: ProvisionStatusRetained, CreatedAt: rec.CreatedAt,
   RetainedUntil: rec.CreatedAt + b.cfg.RetentionMaxAge, Items: rec.Items, Tenant: rec.Tenant,
   BackendName: cfg.Name}` — for BOTH `active` and `restoring` records (a tenant polling during
   their own restore must still see `retained`, not a 404). **Invariant (tested):** the retention
   lookup precedes the diagnostics fallback, so a retained lease never regresses to
   `Status=failed`. Non-retaining backends (mock/k3s) have no retention store → unchanged → never
   return `Retained`.
3. `internal/api/handlers.go` — make the representation **actionable** (not just a flag + date):
   - `LeaseStatusResponse` (329) and `LeaseProvisionResponse` (402): add
     `RetainedUntil string \`json:"retained_until,omitempty"\`` (RFC3339) **and** the restore
     shape — `Items` (service name + SKU + quantity) — so the tenant can build the matching fresh
     PENDING lease `itemsShapeMatch` requires (restore.go:454). The representation is keyed on the
     **original lease UUID** (the path param; `fred-retained-` renames don't collide because
     lookup is by UUID, not name).
   - `GetLeaseStatus` (344): **lift the `State == ACTIVE` gate at line 378** so the provision
     status is fetched for **non-`ACTIVE`** leases too; surface `provision_status=retained` +
     `retained_until` + `items` + a short restore hint (`restore via POST .../restore with a fresh
     PENDING lease of matching shape`) when the backend returns `retained`.
   - `GetLeaseProvision` (420) → `GET /provision` already calls `GetProvision`; it now returns
     `retained` (not 404) within the grace window.
4. **Routing to the holding node — RESOLVED: bounded fan-out.** Confirmed by the code trace: the
   lease→backend **placement is deleted on close** (`orchestrator.go:218-220,256-258`;
   `reconciler.go:546,566`), and `SetBatch` can't rebuild it (`ListProvisions` skips retained
   leases), so placement-based routing would silently 404 the exact retained leases this ticket
   surfaces. Instead, the pull **fans out**: call `GetProvision(leaseUUID)` across
   `backendRouter.RouteAll(sku)` (SKU from the closed chain lease) — or `Backends()` if the SKU is
   unknown — and take the first `retained` answer; all non-holders return `ErrNotProvisioned` and
   are skipped. Bounded (O(matching backends); single-backend dev = 1 call), needs **no
   placement-lifecycle change and no new durable state**. (A retained-placement scheme was
   considered and rejected: providerd can't know at close time whether the backend actually
   retained, so it would have to retain placement for every close and GC it via reconcile — more
   moving parts than a fan-out over an infrequent, tenant-initiated query.)
5. **Authz for a closed lease — chain-primary with a backend fallback.** The pull authenticates
   via `AuthenticateLeaseRequest(..., requireActive=false)` → `ChainClient.GetLease`
   (handlers.go:348,1099). This works **iff** the chain still returns the closed/expired lease for
   the full grace window. **Do not assume it:** if `GetLease` returns nil, fall back to authorizing
   against the retained record — the fan-out `GetProvision` returns `Tenant` (#1), and providerd
   checks the ADR-036-signed caller's tenant `==` `ProvisionInfo.Tenant` (the same tenant-isolation
   boundary as restore's cross-tenant guard, restore.go:438). This keeps the pull working for the
   auto-closed cohort even if the chain prunes closed leases. **Verify** the chain's closed-lease
   retention window during impl and pick: chain-only if ≥ grace, else wire the fallback.

### Part B — Best-effort ground-truth push (optimization, fixes the intent-emit bug)

For tenants who *are* connected, keep a low-latency hint — but explicitly best-effort (a
k8s-Event analogue), because Part A is authoritative.

5. `internal/backend/docker/deprovision.go`: declare `var retained bool` in `doDeprovision`;
   set `retained = true` in the `default:` arm when `len(volumeErrs) == 0` (line ~226); carry it
   to the terminal success callback at line ~350.
6. `internal/backend/client.go`: add `Retained bool \`json:"retained,omitempty"\`` to
   `CallbackPayload`.
7. `internal/backend/shared/callback_sender.go`: thread `retained` → `CallbackPayload.Retained`;
   persist it on `CallbackEntry` and set it in `ReplayPendingCallbacks` so a restart-replayed
   callback keeps the flag. (Docker `sendCallback`/`sendCallbackWithURL` shims gain the param,
   default `false` at non-deprovision call sites.)
8. `internal/provisioner/handler_set.go`: in `HandleBackendCallback`'s `CallbackStatusDeprovisioned`
   arm, if `payload.Retained`, `publishLeaseEvent(leaseUUID, ProvisionStatusRetained, <message>)`.
   **Delete the optimistic emit at `:189`** (notify on observed outcome, once, from the single
   chokepoint all teardown paths — close/expire/auto-close/max-reprovision — funnel through).
   This emit stays best-effort/fire-and-forget; **no `Notified` marker, no reaper re-drive** —
   the pull endpoint (Part A) is the durable backstop, so there is no premature-ack to commit.

## What changed from Rev 1

- **Dropped** the entire Layer-2 push-reliability machinery: `RetentionEntry.Notified`, the
  mark-on-2xx ack, reaper re-drive, idempotency key, dead-letter/backoff. The review showed it
  hardened the wrong hop and contained a premature-ack bug. **Added** the queryable status (Part A).
- Net result is **simpler on the push side and correct for the offline cohort** — the
  level-triggered convergence the idiom wants is the tenant *pulling* current state, not a push
  reaper. The grace reaper (grace deletion) is unchanged.

## Decisions locked

- **Source of truth = the backend `RetentionEntry`, surfaced via `GetProvision`** (`Status=retained`
  + `RetainedUntil`). No new `Backend` interface method; reuse the existing query path.
- **Push is best-effort**, fired on ground truth from the deprovision callback; the `:189`
  intent-emit is removed. No `Notified` marker / reaper / idempotency key (the pull is authoritative).
- **Discoverability:** extend the existing `GET /status` and `GET /provision` rather than a new
  route, so a returning tenant finds it naturally.
- **`RetainedUntil`** is computed backend-side (`CreatedAt + RetentionMaxAge`) — the backend is the
  only component that knows both.

## Test plan (`-race` where concurrent; integration runs in CI via ENG-330)

- **docker unit:** `GetProvision` returns `Retained` + correct `RetainedUntil` + `Items` when an
  active `RetentionEntry` exists and the provision is gone; returns `ErrNotProvisioned` when neither;
  retention precedence over a stale `Failed` diagnostics entry. `retained` flag ground-truth on the
  callback (true on retain-success; false on destroy / partial-failure paths).
- **shared unit:** `CallbackPayload`/`CallbackEntry` round-trip `Retained`; `ReplayPendingCallbacks`
  preserves it.
- **api unit:** `GetLeaseStatus` surfaces `provision_status=retained` + `retained_until` + `items`
  for a **non-ACTIVE** lease with retained data; `GET /provision` returns `retained` (not 404) within
  the grace window; auth allows the closed-lease owner via chain, **and** via the
  `ProvisionInfo.Tenant` fallback when `GetLease` returns nil (cross-tenant caller rejected).
- **routing (multi-backend):** with ≥2 backends, only the holder returns `retained`; the fan-out over
  `RouteAll(sku)`/`Backends()` finds it and others return `ErrNotProvisioned`. (The bug this prevents:
  a single statically-routed pick 404-ing a genuinely retained lease.)
- **provisioner unit:** `HandleBackendCallback` emits `ProvisionStatusRetained` iff `payload.Retained`;
  the `:189` intent-emit is gone (close/expire no longer emit pre-deprovision).
- **integration (`//go:build integration`, CI via ENG-330):** credit-exhaustion auto-close →
  `GET /status` for the lapsed lease returns `retained` + a sane `retained_until` + `items` (the
  offline-tenant reconnect path, end-to-end through the reconciler orphan sweep). Include an
  **EXPIRED** lease (not just freshly-closed) to exercise the authz path. And the connected-tenant
  best-effort push delivers `ProvisionStatusRetained`.

## Risks / caveats (and accepted tradeoffs)

- **Chain closed-lease retention window** (Part A #5) is the one external unknown to verify: if the
  chain prunes closed/expired leases before the grace window, the chain-only authz path 404s and the
  `ProvisionInfo.Tenant` fallback must be wired. Resolve during impl.
- **Pull availability is coupled to the holding backend being up** (accepted by design): the source of
  truth is the node-local `RetentionEntry`; during a node outage the pull can't report `retained` —
  but the data itself is equally unavailable then, so this is the right tradeoff, not a gap.
- **No enumeration ("list my retained leases")** in v1 (accepted): restore already requires a known
  `from_lease_uuid`, and there is no providerd-side cross-backend tenant index (only per-backend
  `RetentionStore.ListByTenant`). A tenant-scoped, default-excluded `show_retained` list is the
  idiomatic future add (AIP-164) but is out of scope here.
- **`no-test-only-code-in-prod`:** mock backend's behavior must be real (it simply has no retention
  store → never returns `Retained`), not a prod-nil hack.
- **Grace clock visibility:** `RetainedUntil` makes the deadline discoverable by pull at any time —
  closing the "offline tenant can't tell how many of the 90 days remain" gap.

## Scope note (for the owner)

ENG-329 was filed as a *notification gap* ("auto-close doesn't emit the retained notice"). The
best-practices reviews established that the idiomatic fix for an **offline-by-construction** cohort is
a **queryable retention status** (Part A), with the push demoted to a best-effort hint (Part B). Part A
is therefore a small new *feature* (a retention-aware `GetProvision` + actionable status fields + fan-out
routing), larger than the literal ticket. Two ways to land it:
- **(a) Together (this spec):** Part A + Part B in ENG-329 — one coherent "tenant learns their data is
  retained" change.
- **(b) Split:** Part B (best-effort push, ~small, closes the literal notification gap) in ENG-329 now;
  Part A (queryable retention-status API) as its own issue/PR. Cleaner review boundaries; the status API
  has standalone value and its own test surface.
Recommendation: **(a)** — they're tightly coupled (both are "how the tenant discovers retention") and Part
A is what actually fixes the offline cohort; splitting risks shipping only the best-effort half. Owner's call.

## Out of scope

- Reliable/durable *push* delivery (the pull endpoint is the reliable channel; the push is an
  explicit best-effort optimization). A durable per-tenant notification inbox is a possible future
  enhancement, not needed here.
- Auto-close *teardown mechanism* stays the reconciler orphan sweep; we add discoverability, not a
  new deprovision path.
- SKU/billing mismatch (ENG-331) and the unit-coverage backlog (ENG-332) are separate.
