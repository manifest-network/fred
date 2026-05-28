# ENG-230 — Close the Restart/Update off-actor Status seam (DESIGN RECORD)

**Status:** Shipped in PR #93 — commits `3665df9` (actor-only status writes) and `31b1179`
(gauge + preflight rekey off actor state; drop `PrevStatus`). Part of epic ENG-229
(docker-backend actor-only write model). This is the as-built design record, not a
deliberation log.

---

## 1. Problem

`Backend.Restart` / `Backend.Update` ran a synchronous prelude on the HTTP goroutine that,
under `provisionsMu`, **wrote `prov.Status = Restarting/Updating` (+ `CallbackURL`) before
routing to the lease actor**, then rolled those writes back on any routing/ack failure. This
off-actor write was the last place `prov.Status` was mutated outside the lease actor, and it
forced a web of compensating machinery: a `restartRollback` helper at 6+ sites, an
`onEnterFailing` Status recheck (+ a `lease_failing_race_skipped_total` metric) to absorb the
race between a container-death transition and the off-actor flip, and `Restarting/Updating`
SM `Ignore` guards to swallow the self-event a freshly-created actor saw when the prelude had
pre-initialized its SM in the target state.

Goal: make the lease actor the **sole writer** of `prov.Status`/`CallbackURL` for restart and
update, and delete all of the above.

---

## 2. Final design (as shipped)

### 2.1 Status/CallbackURL writes — sole writer is the SM entry action, before the ack
`onEnterRestarting` / `onEnterUpdating` (both via `applyReplaceEntry`, `lease_sm.go`) are the
**only** writers of `prov.Status` (Restarting/Updating) and `prov.CallbackURL` for these
paths. They run inside `fireAndVerify` on the actor goroutine, **before**
`handleRestartRequested`/`handleUpdateRequested` send `msg.Ack`. This preserves the contract
`api/handlers.go` relies on: "after `Restart()`/`Update()` returns, the lease is already
Restarting/Updating" (the handler publishes a hardcoded status event right after the call
returns). `CallbackURL` rides on `RestartRequestedMsg`/`UpdateRequestedMsg` (field
`CallbackURL`) and is applied inside the entry action via `replaceEntryArgs`.

### 2.2 Prelude — read-only, no prov mutation, no rollback
The prelude now does ONLY: read-only fast-fail validation (`ErrNotProvisioned`,
`ErrInvalidState` on status, manifest/normalize/image/SKU validation), a pure manifest
marshal, and `releaseStore.Append(..., Status:"deploying")`. None of these mutate
`prov.Status`/`CallbackURL`, so there is nothing to roll back. `restartRollback` and all its
call sites are **deleted**. (`releaseStore.Append` writes the release-history store, not
`prov` — see delta §4.)

### 2.3 Busy-state rejection → typed 409 (`classifyReplaceReject`)
Removing the off-actor flip opened a small TOCTOU: two concurrent same-lease restarts can
both pass the read-only prelude (Status still `Ready`) and route. The actor processes serially,
so exactly **one** worker ever runs; the loser's `fireAndVerify` fails (unhandled trigger from
a busy state). `handleRestartRequested`/`handleUpdateRequested` map that via
`classifyReplaceReject(err)`: if `sm.State() ∉ {Ready, Failed}` → wrap as
`backend.ErrInvalidState` (handlers map → **409 Conflict**), else forward the raw error. The
`a.terminated` early-return still returns `errActorTerminated` (caller retries against a fresh
actor) and must stay distinct from a 409. `a.workCancel` is set **after** a successful
`fireAndVerify` (not before), so a rejected duplicate cannot clobber the in-flight worker's
cancel func.

### 2.4 Defenses removed (race structurally eliminated)
Because every `prov.Status` write for restart/update is now an SM entry action on the actor
goroutine, all Status writes are actor-serial and the following are gone:
- the `onEnterFailing` Status recheck (the `raceSkipped` branch);
- the `lease_failing_race_skipped_total` metric **and** the `SMMetrics.FailingRaceSkipped`
  interface method;
- `Restarting.Ignore(evRestartRequested)` / `Updating.Ignore(evUpdateRequested)`.

### 2.5 `activeProvisions` gauge — keyed on actor-observed `replaceWasActive`
The gauge counts `Status==Ready` leases (authority: `recover.go` `activeProvisions.Set(readyCount)`).
It is maintained incrementally and keyed on `LeaseActor.replaceWasActive`, captured in
`applyReplaceEntry` by reading `p.Status` **before** overwriting it (true = SM source was
`Ready`). The replace-outcome entry actions then apply:
- `onEnterReadyFromReplaceCompleted` / `onEnterReadyFromReplaceRecovered`: `Inc` iff
  `!replaceWasActive`;
- `onEnterFailedFromReplace`: `Dec` iff `replaceWasActive`.

This replaces the previous gauge key, the prelude-captured `PrevStatus` snapshot, which was
**stale** under the death-then-queued-restart ordering (prelude saw `Ready`, but an
intervening `Ready→Failing` had already `Dec`'d).

### 2.6 `doRestart` preflight recovered-vs-failed — additive `RecoveredIfSourceActive` flag
A preflight failure (e.g. SKU-profile lookup fails before any container is touched) must
report "recovered to Ready" iff the lease's containers were running at replace-start. That
decision also used the stale prelude `prevStatus`. It is relocated to the actor via an
**additive** field `ReplaceResult.RecoveredIfSourceActive`, set by **exactly one site**
(`doRestart`'s preflight branch). `spawnReplaceWorker` computes
`recovered := result.Restored; if result.RecoveredIfSourceActive { recovered = wasActive }`
(`wasActive` captured on the actor goroutine before the worker spawns). The `Work` closure
signature is **unchanged** (`func() ReplaceResult`) — important, it is the exported
substrate-agnostic worker contract. Provably scoped: `doUpdate` preflight (intentionally
`Restored:false` — an update that never pulled its image did not reach the desired state) and
the post-replace rollback path (`Restored` = `rollbackViaCompose` result) never set the flag,
so they are unchanged.

### 2.7 `PrevStatus` fully removed
With the gauge (§2.5) and the preflight decision (§2.6) rekeyed off actor state, `PrevStatus`
had no readers. It is deleted end-to-end: the `prevStatus` prelude captures, the
`doRestart`/`doUpdate` params, `replaceContainersOp.PrevStatus`, and
`ReplaceSuccessResult.PrevStatus` / `ReplaceFailureInfo.PrevStatus`. Invariant:
`grep -rin prevstatus internal/` → **zero**.

---

## 3. Safety rationale (the accurate basis)

- **Actor-serial sole-writer is the safety basis.** The actor processes its inbox serially
  and `Fire` runs OnExit/OnEntry synchronously on the actor goroutine; every `prov.Status`
  write for restart/update is such an entry action. Nothing can interleave between a guard's
  status read and an entry action's write, so the death-vs-restart race is structurally gone.
- **`recover.go` PRESERVES recovered Restarting/Updating leases** (only `Failing` is
  normalized to `Failed`); pinned by `TestRecoverState_RestartingPreserved`. A recovered lease
  can therefore rest in Restarting/Updating.
- **The route-time prelude fast-fail is a precondition, NOT the safety basis.** It rejects a
  restart/update on any lease whose `Status ∉ {Ready, Failed}`, but it does **not** constrain
  processing-time SM state (the concurrent-duplicate TOCTOU passes it with `Status==Ready`).
  Do not document or assume otherwise.
- **Why dropping the `Ignore` guards is safe:** they only absorbed the self-event a fresh
  actor saw when the prelude pre-wrote `Status=Restarting` (so `readProvisionStatus`
  initialized the SM there). With the pre-write gone, `readProvisionStatus` reads the true
  status and a normal restart is a real `Permit` from `Ready`/`Failed`. The only remaining way
  `evRestartRequested` meets an already-`Restarting` SM is the concurrent duplicate, which is
  correctly rejected (§2.3) — keeping `Ignore` would instead no-op it and spawn a second worker.

---

## 4. Behavior deltas

- **Gauge correction #1 (metric-only, newly exposed by ENG-230):** removing the
  `onEnterFailing` recheck made it `Dec` unconditionally; in death-then-restart-success the
  lease was left under-counted. Fixed by §2.5.
- **Gauge correction #2 (metric-only):** `onEnterReadyFromReplaceRecovered` previously did no
  gauge op, so a recovered-from-non-active lease under-counted. Now `Inc`s correctly (§2.5).
- **`doRestart` preflight wrong-Ready (functional; PRE-EXISTING edge, fixed opportunistically):**
  in the death-then-restart + preflight-failure race the lease was marked `Ready` with dead
  containers. This is pre-existing — the stale `prevStatus` drove `:143` in both eras; the
  removed recheck governed only the gauge, not this decision. Now correctly ends `Failed`
  (§2.6). It is rare, self-heals on the next reconcile, and emitted **no** wrong on-chain
  callback (the callback is `CallbackStatusFailed` on both the recovered and failed paths).
- **Concurrent same-lease double-restart → 409:** preserved. It was a prelude `ErrInvalidState`
  reject before; now the actor classifier produces the same 409 (§2.3).
- **Cosmetic:** a routing/ack failure now leaves a `"deploying"` release record (no
  release-only rollback). Harmless: recovery uses `LatestActive` (skips non-active),
  deprovision deletes the lease's releases, and the next op appends a fresh record.

---

## 5. Tests

- Contract / no speculative write: `TestRestartRequested_WritesStatusBeforeAck`,
  `TestUpdateRequested_WritesStatusBeforeAck` (status written before ack);
  `TestRestart_RoutingFailureLeavesStatusUnchanged`,
  `TestUpdate_RoutingFailureLeavesStatusUnchanged`.
- Fresh-actor-from-Failed: `TestRestartFromFailed_Succeeds`, `TestUpdateFromFailed_Succeeds`.
- Busy-state 409: `TestSecondConcurrentRestartRejected` / `…Update…` (one worker, loser
  `errors.Is(ErrInvalidState)`, no `workCancel` clobber); `TestRestartLosesToDeprovision` /
  `…Update…` (409, no worker); existing `TestRestart/Update_InvalidState_*` (prelude 409).
- Death-vs-restart ordering: `TestContainerDiedThenRestart_Succeeds` (Failing→Restarting via
  Permit, one worker, gauge net-zero).
- Preflight recovered relocation: `TestRestartPreflight_FromReady_Recovers`,
  `TestRestartPreflight_FromFailed_StaysFailed`, `TestUpdatePreflight_StaysFailed` (update
  regression guard), `TestContainerDiedThenRestartPreflight_EndsFailed` (the pre-existing
  wrong-Ready edge, now Failed), `TestDoRestartPreflight_SetsRecoveredIfSourceActive`
  (producer half — `doRestart` actually sets the flag).
- Recovery: `TestRecoverState_RestartingPreserved` (pins §3's preserve fact).
- Deleted: `TestOnEnterFailing_RaceWithConcurrentStatusFlip` (pinned the now-removed recheck +
  metric).

---

## 6. Key symbols / files

- `internal/backend/shared/leasesm/lease_sm.go`: `onEnterRestarting`/`onEnterUpdating`
  (`applyReplaceEntry`), `replaceEntryArgs`, gauge entry actions, `ReplaceResult.RecoveredIfSourceActive`.
- `internal/backend/shared/leasesm/lease_actor.go`: `RestartRequestedMsg`/`UpdateRequestedMsg`
  (`CallbackURL` field, `Work func() ReplaceResult`), `handleRestartRequested`/`handleUpdateRequested`,
  `classifyReplaceReject`, `LeaseActor.replaceWasActive`, `spawnReplaceWorker` switch.
- `internal/backend/docker/restart_update.go`: `Restart`/`Update` preludes, `doRestart`
  preflight (sets the flag), `doUpdate` preflight (`Restored:false`), `doReplaceContainers`.
- `internal/api/handlers.go`: `RestartLease`/`UpdateLease` (post-call status publish; maps
  `ErrInvalidState`→409).
- `internal/backend/docker/recover.go`: preserves Restarting/Updating; `activeProvisions.Set(readyCount)`.
