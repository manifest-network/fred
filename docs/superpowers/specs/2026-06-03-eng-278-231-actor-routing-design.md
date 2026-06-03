# ENG-278 / ENG-231 — design: route the ReconcileCustomDomain apply + redeploy through the lease actor

**Tickets:** ENG-231 (structural fix, child of ENG-229 actor-only write model) — folds in ENG-278 (the TOCTOU bug). Both close together.
**Status:** design approved (approach + commit point + test seam locked 2026-06-03). Next: writing-plans → TDD.
**Worktree:** `.claude/worktrees/felix+eng-278-231-reconcile-actor-routing`, off `main` (`b9f3a03`, ENG-266 merged).

## Problem

`internal/backend/docker/reconcile_custom_domain.go` `ReconcileCustomDomain` runs on the reconciler goroutine. It mutates `prov.Items[idx].CustomDomain` under `provisionsMu` (`:162`), **releases the lock** (`:171`), then calls `b.Restart`, whose prelude **re-acquires** the lock and **re-snapshots** `prov.Items` (`restart_update.go:82`) into the worker's `work` closure.

Between the unlock and the re-snapshot, the periodic `recoverState` can run. The lease is still `Ready` in this window (status only flips to `Restarting` *inside* the actor, after the unlock), and `recover.go`'s merge preserves the in-memory struct only for `Provisioning`/`Restarting`/`Updating` (`recover.go:301`) — **not `Ready`**. So `recoverState` rebuilds the provision from container labels (the **old** domain, `recover.go:183-188`) and swaps the whole `b.provisions` map (`recover.go:341`). `Restart`'s prelude then snapshots the swapped struct → the worker recreates containers with the **old** domain and the staged change is dropped for that tick.

Self-healing (the next tick re-detects drift), but the symptom is a redundant container restart (brief tenant downtime) with the wrong domain and delayed cert issuance.

The mutate→unlock→`Restart` pattern is pre-existing (base `3e54bd7`); ENG-266 only changed the computed value (the DNS gate), and ENG-264 made the reconcile→Restart path fire for single-image leases, so the latent window is now reachable.

This is the same single-writer-violation class ENG-230 closed for the Restart/Update `Status` seam, on a different field (`prov.Items[].CustomDomain`).

## Idiomatic basis (why this approach)

The bug is a read-modify-write split across a lock boundary, racing a concurrent writer. The idiomatic Go fix is *"share memory by communicating" / the stateful-goroutine (actor) pattern*: route the mutation **and its dependent side-effect** through the single owner so they're serialized — which is exactly what the `leasesm` actor is.

Two finer idioms shape the specific design:

1. **Level-triggered, idempotent reconcile.** The reconciler derives desired state read-only and applies it idempotently. The value written (`domain = the chain's value`) comes from the chain, *not* from `prov`'s current value, so separating the read (diff) from the write (apply) cannot lose an update. The read-only diff stays in the reconciler; only the write moves into the actor.
2. **fred's model is sole-*writer*, not sole-*reader*** (ENG-229). Lock-guarded reads stay off-actor (recover.go, info endpoints, the replace worker — category C); only the *write* of live `*provision` belongs to the actor. The design keeps the reconciler's lock-guarded read and moves only the write.

## Design (Approach A — reuse the restart path)

Data flow:

```
ReconcileCustomDomain (reconciler goroutine)
  ├─ ingress guard + DNS precompute  (unchanged, off-lock — ENG-266)
  ├─ provisionsMu.Lock()
  │    ├─ Ready-gate (unchanged)
  │    ├─ diff: normalizedServiceKeys match + validateCustomDomain + asymmetric DNS gate  (unchanged logic)
  │    │     → produce `overrides` map[serviceName]desiredDomain  (READ-ONLY; no prov.Items mutation)
  │    └─ if no overrides → unlock, return nil
  ├─ provisionsMu.Unlock()
  └─ route the redeploy through the actor's restart path, carrying `overrides`
        ├─ worker renders containers from an item snapshot with `overrides` applied  → NEW-domain Traefik labels
        └─ on SUCCESS, the actor's terminal entry action commits `overrides` into prov.Items
```

Key properties:

- **The worker's item snapshot carries the desired domains.** The item slice handed to the replace worker is a value copy with `overrides` applied by `ServiceName`. Because it is a value captured under the reconciler's lock (or re-derived in the restart prelude and re-applied by `ServiceName`), a concurrent `recoverState` swap cannot clobber it. This is the core of the fix: the redeploy no longer depends on a post-unlock re-read of `b.provisions`.
- **The in-memory `prov.Items` commit moves to the actor, on success.** `OnSuccess` is set on the success `ReplaceResult`; it runs inside `onEnterReadyFromReplaceCompleted` (`lease_sm.go:526`, `:551-552`) — a **terminal SM entry action on the serial actor goroutine**, under one `UpdateFn` critical section, atomic with `Status→Ready`. This satisfies ENG-231's "no write to `prov.Items` outside the actor goroutine" *strictly*, and reuses the exact hook the Update flow uses for `StackManifest`.
- **`overrides` is keyed by the actual `prov.Items[].ServiceName`** (stable across `recoverState` rebuilds, since it comes from container labels) — robust against reordering, exactly like the existing rollback's ServiceName keying.

### Why the TOCTOU is closed

- The redeploy renders from the override-applied snapshot, not a post-unlock re-read — a `recoverState` swap between the diff and the redeploy is harmless (the override re-applies the desired domain by `ServiceName`).
- The `prov.Items` commit happens inside the actor while `Status` transitions `Restarting→Ready` under `UpdateFn`. Throughout the in-flight window `Status` is `Restarting`, so `recover.go:301` **preserves** the struct (no swap). There is no `Ready`-with-staged-change window for `recoverState` to clobber.
- `recoverState` needs **no change** — its existing `Restarting`-preserve semantics are precisely what the fix leans on.

### Failure semantics (CAS rollback deleted)

On a failed redeploy, `onEnterReadyFromReplaceCompleted` does **not** run, so `OnSuccess` never commits — `prov.Items` is left untouched (old domain), consistent with the rolled-back old containers. The next reconcile tick re-detects the same drift and retries. This is the idiomatic level-triggered retry and **cleanly replaces** the ServiceName+value-CAS rollback (`reconcile_custom_domain.go:196-211`), which is deleted. No transient `prov.Items` inconsistency is externally observable (it is never written on failure).

The in-flight Ready-gate (`reconcile_custom_domain.go:64`) already prevents re-entry while a redeploy is running (`Status` is `Restarting`), so no redundant restart is triggered during the window; the `OnSuccess` commit prevents drift re-detection after success.

### Preserved as-is

- **ENG-266 DNS gate.** The network-I/O precompute stays off-lock in the reconciler; the asymmetric apply (`desired != "" && desired != emitted && !dnsReady[desired]` ⇒ defer; clearing never gated; emitted never torn down) stays in the read-only diff. `validateCustomDomain` stays both in the precompute and as defense-in-depth in the diff.
- **ENG-264 ServiceName normalization.** `normalizedServiceKeys` matching (chain raw name vs normalized `prov.Items` name) is unchanged; it is part of the read-only diff.
- The ingress-disabled early return and its loop-avoidance rationale (`reconcile_custom_domain.go:22-32`).

## Concrete changes per file

- **`internal/backend/docker/reconcile_custom_domain.go`** — the diff loop computes `overrides map[string]string` (serviceName→desiredDomain) instead of mutating `prov.Items[idx]` and recording `pendingChange`. Delete the `prov.Items[idx].CustomDomain = desired` write (`:162`), the post-`Restart` CAS rollback block (`:196-211`), and the `pendingChange` struct. If `len(overrides)==0`, return nil. Otherwise route the redeploy with `overrides`. `normalizedServiceKeys` stays.
- **`internal/backend/docker/restart_update.go`** — factor the restart routing so it can carry custom-domain overrides:
  - The shared routing applies `overrides` (by `ServiceName`) to the worker's `items` snapshot, and, when `overrides != nil`, sets an `OnSuccess` closure on the replace op that re-applies `overrides` to `prov.Items` by `ServiceName`.
  - Public `Restart(ctx, RestartRequest)` calls the shared routing with `overrides == nil` (behavior-identical).
  - `doRestart` gains an `onSuccess func(*leasesm.ProvisionState)` parameter threaded onto `replaceContainersOp.OnSuccess` (currently unset for the restart op).
  - *Factoring note for the plan:* prefer a single internal entry (e.g. `routeReplaceRestart(ctx, leaseUUID, callbackURL, overrides)`). Computing the diff in the reconciler under one lock and snapshotting in the restart prelude under a second lock are both correct because `overrides` is idempotent and `ServiceName`-keyed; whether to collapse to one lock acquisition is an implementation choice, not a correctness one.
- **`internal/backend/docker/recover.go`** — **no change** (the `Restarting`-preserve branch at `:301` + the map swap at `:341` already provide the property the fix relies on). Listed only to flag it as deliberately untouched.
- **`internal/backend/shared/leasesm/`** — **no change.** `OnSuccess` already exists on `ReplaceSuccessResult` (`lease_sm.go:821-824`) and is invoked on the actor goroutine in `onEnterReadyFromReplaceCompleted` (`:551-552`). No new message type or handler.
- **`internal/backend/docker/` (Backend struct)** — add an unexported test-only hook field (nil in prod), e.g. `reconcileApplyHook func()`, invoked in `ReconcileCustomDomain` immediately after the reconciler releases `provisionsMu` following the diff (i.e. in the bug window, before the redeploy is routed). Documented as test-only.

## Test strategy (deterministic regression test)

A green `go test -race` over the existing *synchronous* reconcile tests proves nothing about this race (the corpus notes "ReconcileCustomDomain runs synchronously"). The regression test forces the real interleaving deterministically:

1. Provision a lease to `Ready` with the **old** domain in both `prov.Items` and the fake container labels; wire a compose mock that captures the built project.
2. Set `reconcileApplyHook` to run `recoverState` (which rebuilds from the old-labeled fake containers and swaps the map) — so the swap lands exactly in the bug window.
3. Drive `ReconcileCustomDomain` with the chain wanting the **new** domain (`dnsReady[new]=true`).
4. Assert: (a) the compose project the worker built carries the **new** `LabelCustomDomain`; (b) `b.provisions[uuid].Items[idx].CustomDomain == new` after the cycle; (c) `Status` went `Ready→Restarting→Ready`.

- **Pre-fix (TDD red):** the hook's swap reverts `prov.Items` to old; `Restart`'s prelude re-snapshots old → worker renders old → assertion (a)/(b) fail.
- **Post-fix (green):** the override is already captured/re-applied by `ServiceName` → worker renders new → passes.

The hook + a real `recoverState` call make the interleaving deterministic; the old/new domain is the discriminator, not the race detector. Existing tests that assert the CAS-rollback behavior are removed/rewritten (the CAS is deleted); the other synchronous reconcile tests stay.

## Acceptance criteria

ENG-231:
- [x] No write to `prov.Items` outside the actor goroutine — the only write is `OnSuccess` inside `onEnterReadyFromReplaceCompleted` (serial actor goroutine).
- [x] Custom-domain reconcile still re-renders Traefik labels (worker renders from the override-applied snapshot) and still rolls back cleanly on failure — now via the actor's success/failure terminal path (no commit on failure → next-tick retry), replacing the CAS.

ENG-278:
- [x] A custom_domain change is not lost if `recoverState` runs concurrently (no redundant restart-with-old-domain).
- [x] A concurrency test interleaves `ReconcileCustomDomain` with `recoverState` and asserts the recreate uses the new domain.

## Out of scope

- ENG-277 (steady-state-lookup optimization) — done next, separately (both restructure this function's lock dance; sequential to avoid conflicts).
- k3s-backend (provisioner still a stub).
- The broader ENG-229 category-B/C seams (bootstrap writes, worker pre-publish) — separate children.

## Risks / notes

- **The override must be applied to a *post-diff* item snapshot.** If the redeploy ever rendered from a pre-override snapshot, the fix would silently no-op — the new-domain assertion in the regression test is the guard.
- **Two lock acquisitions (reconciler diff, then restart prelude)** are safe only because the override is idempotent and `ServiceName`-keyed; do not introduce a value that depends on `prov`'s prior state across that gap.
- A concurrent `Restart`/`Update` that wins the actor race makes the redeploy fail-fast with `ErrInvalidState` (409-class), returned to the reconciler, which retries next tick — unchanged from today (the routing stays blocking; no fire-and-forget change).
