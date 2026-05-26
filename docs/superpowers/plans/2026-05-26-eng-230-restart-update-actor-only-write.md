# ENG-230 — Close the Restart/Update off-actor Status seam

Part of epic ENG-229 (docker-backend actor-only write model).

**Goal:** make the lease actor the sole writer of `prov.Status` / `prov.CallbackURL`
for the Restart and Update paths; eliminate every off-actor write of those two
fields; delete the speculative-write + rollback machinery.

Baseline: HEAD `1f38742`, branch `worktree-eng-230-restart-update-seam`. `go build`,
`go vet`, leasesm + api unit tests green.

---

## 0. Grounding — what the code actually does today (verified, not from the brief)

- `restart_update.go`
  - `restartRollback` is at **lines 16–39** (doc comment 16–22, func 23–39).
  - `Restart` (73–159): prelude takes `provisionsMu`, read-only fast-fail
    (`ErrNotProvisioned` 79–82, `ErrInvalidState` on status 83–87, `ErrInvalidState`
    on `StackManifest == nil` 88–97), then **writes** `prevStatus`/`prevCallbackURL`
    (98–99), `prov.Status = Restarting` (100), `prov.CallbackURL` (101–103), snapshots
    fields, unlocks. Marshals manifest + `releaseStore.Append("deploying")` (116–137)
    with inline prov rollback on marshal-fail (119–123) and append-fail (131–136).
    Routes `RestartRequestedMsg{Cancel, Work, Ack}` (147); `restartRollback` on
    routing-fail (149) and ack-fail (155).
  - `Update` (510–631): same shape — validation (status 520–524, NormalizeProvisionRequest
    533–536, ParsePayload 540–544, ValidateStackAgainstItems 554–557, ValidateImage
    559–564, GetSKUProfile 566–577), then **writes** `prevStatus`/`prevCallbackURL`
    (585–586), `prov.Status = Updating` (587), `prov.CallbackURL` (588–590).
    `releaseStore.Append(req.Payload, "deploying")` (597–610) with inline rollback
    (604–608). Routes `UpdateRequestedMsg` (619); `restartRollback` on routing-fail (621)
    and ack-fail (627).
  - `prevStatus` is still consumed by the **worker** (`doRestart`/`doUpdate` →
    `ReplaceResult.Failure.PrevStatus`) to drive the activeProvisions gauge in
    `onEnterReadyFromReplaceRecovered` / `onEnterFailedFromReplace`. **Keep capturing it.**
    `prevCallbackURL` is used **only** by `restartRollback` → delete it.

- `lease_actor.go`
  - `RestartRequestedMsg` (150–154) / `UpdateRequestedMsg` (165–169): `{Cancel, Work, Ack}`.
  - `fireAndVerify` (557–565): `sm.Fire(stopCtx, ev)` with **no args**, then verifies
    `State() == wantState`.
  - `handleRestartRequested` (689–701) / `handleUpdateRequested` (703–715): set
    `a.workCancel = msg.Cancel` **before** `fireAndVerify`, then `fireAndVerify` →
    `msg.Ack <- nil` → `spawnReplaceWorker`. **Ack is sent AFTER the SM transition fires.**

- `lease_sm.go`
  - `Restarting` config (186–199): replace-completion Permits + `Permit(evDeprovisionRequested)`
    + `OnExit(onExitProvisioning)` + `Ignore(evContainerDied)` + **`Ignore(evRestartRequested)`** (192–199).
  - `Updating` config (200–207): same + **`Ignore(evUpdateRequested)`** (207).
  - `Ready` (121–125) Permits `evRestartRequested→Restarting`, `evUpdateRequested→Updating`
    (no guard). `Failed` (238–240) Permits the same. `Failing` (147–149) Permits
    `evRestartRequested→Restarting`, `evUpdateRequested→Updating` (no guard).
  - `onEnterFailing` (319–387): re-reads Status under the store lock and **bails** if
    `p.Status != Ready` (the `raceSkipped` branch 348–364) → `Metrics.FailingRaceSkipped()`.
    Big comment 330–346 documents the off-actor flip as the reason.
  - `readProvisionStatus` (795–801): SM initial state = `prov.Status` at construction.

- `recover.go` **preserves** a recovered lease's `Restarting`/`Updating` status as-is
  (301–307 with-containers branch, 317–320 no-containers branch); only `Failing→Failed`
  is normalized (332). So a recovered lease CAN rest in `Restarting`/`Updating`, and a
  later actor CAN initialize its SM there via `readProvisionStatus`. **This does not
  endanger removing the `Ignore` guards** — see §3 for the owner-approved rationale: the
  safety basis is that the actor inbox is the only Status writer (serialized), and the
  one case where `evRestartRequested` meets an already-`Restarting` SM (the §4 TOCTOU
  duplicate) is correctly rejected → `ErrInvalidState`→409. **The prelude fast-fail is a
  route-time precondition only and is NOT the safety basis** (a recovered-Restarting lease
  happens to be blocked at route-time because its `prov.Status` is `Restarting`, but the
  fast-fail does not constrain processing-time state in general — the TOCTOU duplicate
  passes it). No code comment may claim the prelude guarantees processing-time state.

- `provision.go` seeds `Status: Provisioning` at **record creation** for a brand-new
  lease (84). That is why Provision keeps `Provisioning.Ignore(evProvisionRequested)` and
  why **Provision is out of ENG-230 scope** — the asymmetry is intentional. Do not touch
  the provision path.

- `handlers.go` `RestartLease` (514–556) / `UpdateLease` (560–622): on success publish a
  hardcoded `Status: Restarting`/`Updating` event (542–548 / 606–612), relying on
  "after the call returns the lease is already in that status."

- The named integration tests `TestRestartLease_BackendIntegration` (3010) /
  `TestUpdateLease_BackendIntegration` (3235) run against a **mock HTTP backend server**,
  not the docker backend. They exercise handler↔HTTP-client plumbing only and are
  **unaffected** by these changes (still 503/404/409/202 + publish). Keep them green by
  not changing the HTTP-layer contract.

---

## 1. Key correctness argument (why this is safe)

**The actor processes messages serially; `Fire` (with its OnExit/OnEntry actions) runs
synchronously on the actor goroutine.** After this change every write to
`prov.Status`/`prov.CallbackURL` for restart/update happens inside an SM entry action on
the actor goroutine. `onEnterFailing` also writes Status on the actor goroutine. No
worker goroutine writes Status (provision/replace pre-publish writes only
`ContainerIDs`/`ServiceContainers`; the diag goroutine is pure I/O). Therefore **all
Status writes are actor-serial** and the guard-read → entry-write interleaving that the
current `onEnterFailing` recheck defends against is **structurally impossible**.

Concretely, for a `ContainerDied` racing a `Restart` on the same lease:
- **Died dequeued first:** `Ready→Failing` (guard passes; `onEnterFailing` reads Status,
  which is still `Ready` because nothing off-actor can flip it, writes `Failing`). Then
  the queued `RestartRequestedMsg` is dequeued: `Failing→Restarting` via
  `Failing.Permit(evRestartRequested)`; `onEnterRestarting` writes `Restarting`; worker
  spawns. Lease ends Restarting — matches the handler's published event.
- **Restart dequeued first:** `Ready→Restarting`; worker spawns. The later
  `ContainerDied` hits `Restarting.Ignore(evContainerDied)` (still present) and is
  dropped — correct, the replace recreates all containers anyway.

The "Restart() returns ⇒ Status == Restarting" contract is preserved because the actor
runs `onEnterRestarting` (Status write) **before** `msg.Ack <- nil`, and `Restart()`
returns only after `ackOrAbort` observes that ack.

---

## 2. Item-#4 decision — release-store `Append` stays in the prelude (no rollback)

**Decision: keep `releaseStore.Append` in the prelude; do NOT move it into the actor.**

Rationale:
- `releaseStore.Append` writes the **release-history bbolt store**, never
  `prov.Status`/`prov.CallbackURL`. So leaving it in the prelude already satisfies the
  ticket's literal goal — **zero speculative `prov` writes** — because after stripping,
  the prelude performs *no* `prov` mutation at all. Marshal is pure; Append is a separate
  store. If marshal or Append fails, there is **nothing on `prov` to undo** → `restartRollback`
  is genuinely unnecessary, not merely relocated.
- Ordering constraint (from the existing comment 113–115): the "deploying" release MUST
  exist before the worker runs, because the worker's success path calls
  `ActivateLatest` and its failure path calls `UpdateLatestStatus` on the latest release.
  Prelude order is therefore: validate → marshal → Append → route → ack. The worker
  (spawned by the actor after ack) sees the appended release. Unchanged from today.
- Moving Append into the actor would require a **new `LeaseActorConfig` seam**
  (`AppendReleaseFn`) plus plumbing marshaled bytes onto the message, expanding the
  substrate-agnostic interface — and it still cannot fully avoid an orphaned release
  (a fire-failure after a successful Append would orphan one anyway). Not worth the
  added surface.

**Known behavioral delta (decision A — ACCEPTED, §8):** today `restartRollback`
marks the "deploying" release `failed` on routing/ack failure. After this change a
routing-fail or ack-reject leaves the release in `"deploying"`. This is **cosmetic**:
`recover.go` uses `LatestActive` (skips non-active, recover.go:150 / migrate.go:151),
`ActivateLatest` only touches the latest, `deprovision` `Delete`s the lease's releases, and
the next restart/update `Append`s a fresh record. Routing/ack failures only occur on
client-disconnect, backend-shutdown, or actor-terminated/SM-reject — all rare and
non-corrupting. No release-only rollback is added.

---

## 3. Item-#6 decision — remove the now-vestigial defenses

**Owner-approved rationale (the safety basis):** deleting the off-actor prelude Status
write makes the **actor inbox the ONLY path that mutates `prov.Status`**, so every Status
write is serialized on the actor goroutine and nothing interleaves between a read and a
write. That serialization — NOT the prelude fast-fail — is the safety basis. The prelude
fast-fail is a pre-existing **route-time precondition** that does **not** constrain
**processing-time** state (the §4 TOCTOU duplicate passes the route-time check with
`Status==Ready` yet meets a `Restarting` SM at processing time). No code comment may claim
the prelude guarantees processing-time state.

Given that basis:

1. **`onEnterFailing` Status recheck (`raceSkipped` branch) — REMOVE.** It defended against
   the old prelude flip landing mid-`InspectContainer`, between the SM guard's status-read
   and the entry-write. With every Status write serialized on the actor goroutine, nothing
   interleaves → the race is gone. `onEnterFailing` now runs only on a genuine
   `Ready→Failing` where Status is provably `Ready`. Keep the `exists` guard (cheap
   nil-guard; `UpdateFn` returns false if the lease was removed) and the
   `ActiveProvisionsDec()` (now unconditional on the applied path).

2. **`Restarting.Ignore(evRestartRequested)` / `Updating.Ignore(evUpdateRequested)` — REMOVE.**
   They existed only because the prelude pre-wrote `Status=Restarting`, so
   `readProvisionStatus` initialized a freshly-created SM in `Restarting` and the normal
   restart event had to be `Ignore`d as a no-op. With the pre-write gone,
   `readProvisionStatus` reads the lease's **true** status (`Ready`/`Failed`) → a normal
   restart `Permit`s a real transition. The **only** remaining case where
   `evRestartRequested` meets an already-`Restarting` SM is a **concurrent duplicate**
   (the §4 decision-B TOCTOU, previously blocked by the prelude's atomic flip we are
   deleting); that case is now **correctly rejected as an invalid transition →
   `ErrInvalidState`→409**. **Keeping the `Ignore` would be actively harmful**: it would
   `fireAndVerify`-*succeed* and spawn a **second** replace worker against an
   already-restarting lease.

3. **`lease_failing_race_skipped_total` metric + `SMMetrics.FailingRaceSkipped()` — REMOVE
   fully.** The counter's own help text says a sustained rate is the trigger to "move the
   synchronous Status flip into the actor" — i.e. it is the diagnostic for the exact seam
   we are closing. Post-change it can only ever read 0; a permanently-zero counter is
   misleading dead observability, and a `SMMetrics` method with no caller pollutes the
   clean seam. Remove the interface method and all four implementations.

---

## 4. New, smaller race introduced — and its fix (must-do, not gold-plating)

Removing the prelude's atomic Status flip opens a **TOCTOU window** for two concurrent
restart/update calls on the *same* lease: both preludes can read `Status == Ready` (the
actor hasn't yet run `onEnterRestarting`) and both route. Msg#2 then reaches
`handleRestartRequested` while the SM is already in `Restarting`.

- With `Ignore` removed (§3.2), msg#2's `fireAndVerify` returns an unhandled-trigger error.
  `classifyReplaceReject` (§5.2) sees the SM in a busy state (`Restarting`) and wraps it as
  `backend.ErrInvalidState` → ack error → `Restart#2` returns it → handlers.go maps it to a
  clean **409 Conflict** (decision B, owner-overridden). The lease stays correct: **exactly
  one** worker runs.
- **But** `handleRestartRequested`/`handleUpdateRequested` currently set
  `a.workCancel = msg.Cancel` **before** `fireAndVerify`. On the rejected msg#2 this
  **clobbers** msg#1's `workCancel`. A subsequent Deprovision-preempt would then call
  msg#2's (no-op) cancel instead of cancelling msg#1's in-flight worker → orphan-container
  regression (the very thing `onExitProvisioning` exists to prevent).

**Fix (required companion change):** move `a.workCancel = msg.Cancel` to **after** the
successful `fireAndVerify`, before `msg.Ack <- nil`, in `handleRestartRequested` and
`handleUpdateRequested`. This is safe because `workCancel` is consumed only by
`onExitProvisioning`, which can only run after the state was entered (i.e. after a
successful fire). Leave `handleProvisionRequested` unchanged (provision is out of scope
and its double-route is still prevented by the provision prelude + `Provisioning.Ignore`).

Outcome: a concurrent double-restart returns the same clean **409** as today (now enforced
by the actor's busy-state rejection instead of the prelude's atomic flip), with exactly one
worker and no `workCancel` clobber. See decision B (§8) and the classification correctness
note in §5.2.

---

## 5. Per-file change list

### 5.1 `internal/backend/shared/leasesm/lease_sm.go`
- **Add** an args type near the other arg structs:
  ```go
  // replaceEntryArgs carries the new CallbackURL from a Restart/Update
  // request message into the onEnterRestarting / onEnterUpdating entry
  // actions via Fire args, so the actor (not the HTTP prelude) is the
  // sole writer of prov.CallbackURL for these paths.
  type replaceEntryArgs struct{ CallbackURL string }
  ```
- **Add** entry actions (place them with the other `onEnter*` funcs, e.g. after
  `onEnterFailing`):
  ```go
  func (lsm *leaseSM) onEnterRestarting(ctx context.Context, args ...any) error {
      return lsm.applyReplaceEntry(args, backend.ProvisionStatusRestarting)
  }
  func (lsm *leaseSM) onEnterUpdating(ctx context.Context, args ...any) error {
      return lsm.applyReplaceEntry(args, backend.ProvisionStatusUpdating)
  }
  func (lsm *leaseSM) applyReplaceEntry(args []any, status backend.ProvisionStatus) error {
      var callbackURL string
      if len(args) > 0 {
          if a, ok := args[0].(replaceEntryArgs); ok {
              callbackURL = a.CallbackURL
          }
      }
      lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
          p.Status = status
          if callbackURL != "" {
              p.CallbackURL = callbackURL
          }
      })
      return nil
  }
  ```
  (No metric/log side effect inside `UpdateFn`, per the closure idempotence contract.)
- **Register** on the Restarting config (186–199): add
  `.OnEntryFrom(evRestartRequested, lsm.onEnterRestarting)` and **delete**
  `.Ignore(evRestartRequested)` (192–199, including its comment).
- **Register** on the Updating config (200–207): add
  `.OnEntryFrom(evUpdateRequested, lsm.onEnterUpdating)` and **delete**
  `.Ignore(evUpdateRequested)` (207).
- ⚠️ **Delete ONLY the two `Ignore` lines above.** Do NOT touch the source-state
  `Permit`s that drive the actual transitions the new entry actions hang off:
  `Ready.Permit(evRestartRequested→Restarting)` / `Permit(evUpdateRequested→Updating)`
  (lease_sm.go:124–125), `Failed.Permit(...)` (238–240), and `Failing.Permit(...)`
  (148–149). Removing any of those would break the normal restart/update transition (and
  the `Failing→Restarting` success case 6.3(c)). The `WritesStatusBeforeAck` red driver
  would catch it, but leave them intact regardless.
- **Simplify `onEnterFailing`** (319–387): drop the `raceSkipped` recheck (348–364 logic)
  and the `Metrics.FailingRaceSkipped()` call; keep the `exists` early-return and emit
  `ActiveProvisionsDec()` on the applied path. New body of the UpdateFn:
  ```go
  exists := lsm.actor.cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
      p.Status = backend.ProvisionStatusFailing
      p.FailCount++
      p.LastError = errMsgContainerExited
  })
  if !exists {
      return nil
  }
  lsm.actor.cfg.Metrics.ActiveProvisionsDec()
  ```
  Rewrite the 330–346 comment to state: off-actor Status writes for restart/update are
  eliminated (ENG-230); Status is actor-serial, so a `Ready→Failing` entry always sees
  `Ready` — no recheck needed.

### 5.2 `internal/backend/shared/leasesm/lease_actor.go`
- **`RestartRequestedMsg`** (150–154) and **`UpdateRequestedMsg`** (165–169): add field
  `CallbackURL string` (doc: "applied to prov by onEnterRestarting/onEnterUpdating before ack").
- **`fireAndVerify`** (557–565): make variadic and forward:
  ```go
  func (a *LeaseActor) fireAndVerify(ev leaseEvent, wantState backend.ProvisionStatus, args ...any) error {
      if err := a.sm.Fire(a.cfg.StopCtx, ev, args...); err != nil {
          return err
      }
      if state := a.sm.State(); state != wantState {
          return fmt.Errorf("%v not accepted by SM from state %v", ev, state)
      }
      return nil
  }
  ```
  (`handleProvisionRequested`'s existing no-arg call still compiles.)
- **`handleRestartRequested`** (689–701): pass args, reorder `workCancel`, and classify
  the rejection so the loser of the §4 TOCTOU duplicate gets a clean **409** (typed
  `backend.ErrInvalidState`) instead of a raw 500:
  ```go
  func (a *LeaseActor) handleRestartRequested(msg RestartRequestedMsg) {
      if a.terminated {
          msg.Ack <- errActorTerminated // UNCHANGED — must NOT become 409 (retry → fresh actor)
          return
      }
      if err := a.fireAndVerify(evRestartRequested, backend.ProvisionStatusRestarting,
          replaceEntryArgs{CallbackURL: msg.CallbackURL}); err != nil {
          msg.Ack <- a.classifyReplaceReject(err)
          return
      }
      a.workCancel = msg.Cancel
      msg.Ack <- nil
      a.spawnReplaceWorker(msg.Work)
  }
  ```
- **`handleUpdateRequested`** (703–715): identical shape with `evUpdateRequested` /
  `ProvisionStatusUpdating`, same `classifyReplaceReject(err)` on the failure path.
- **Add `classifyReplaceReject`** (new helper, near the two handlers):
  ```go
  // classifyReplaceReject maps a fireAndVerify failure for a restart/update
  // request to the caller-facing error. A busy-state rejection — the SM is
  // mid-Restart/Update/Deprovision/Provision when a duplicate or racing
  // request arrives (the §4 TOCTOU window opened by ENG-230 removing the
  // prelude's atomic Status flip) — is reported as backend.ErrInvalidState so
  // the HTTP handler returns 409 Conflict, preserving the pre-ENG-230 contract
  // where Backend.Restart/Update's prelude fast-fail rejected a non-Ready/Failed
  // lease under the lock. Any OTHER Fire error is forwarded verbatim so a
  // shutdown/internal error is never mislabeled as a client 409.
  func (a *LeaseActor) classifyReplaceReject(err error) error {
      switch a.sm.State() {
      case backend.ProvisionStatusReady, backend.ProvisionStatusFailed:
          // SM still in a restartable state ⇒ the failure was NOT a busy-state
          // rejection (those leave the SM in a busy state). Forward raw.
          return err
      default:
          return fmt.Errorf("%w: lease not in a restartable state (%s)",
              backend.ErrInvalidState, a.sm.State())
      }
  }
  ```
  **Classification correctness (verified, not assumed):** for `evRestartRequested` /
  `evUpdateRequested`, `sm.Fire` returns a non-nil error ONLY via stateless's
  `unhandledTriggerAction` — i.e. only when the current state has no handler for the
  trigger, which (after removing the `Ignore`s) is exactly the busy states
  Restarting/Updating/Deprovisioning/Provisioning. The `Permit`s from Ready/Failed/**Failing**
  are guardless and our entry/exit actions return nil, so Fire never errors from a
  restartable state. **`stateless@v1.8.0` does NOT convert ctx (StopCtx) cancellation into
  a Fire error** — `internalFireOne` only forwards ctx to the in-memory state accessor
  (which ignores it), guards, and entry/exit actions; none here observe it. So the
  `default` (409) branch is what fires in practice; the `Ready/Failed` "forward raw" branch
  is defense-in-depth that keeps a future ctx-aware or guarded action from being
  mislabeled 409. (`leasesm` already imports `backend`; no import cycle. `ackOrAbort`
  lease_actor_routing.go:124-127 returns the ack error verbatim → `Backend.Restart/Update`
  returns it → handlers.go maps `backend.ErrInvalidState`→409 at restart 529-531 /
  update 589-591.)
  **Keying equivalence (closes the owner's keying concern):** because the only Fire-error
  path here is the busy-state unhandled trigger (above), keying the classification on
  `State() ∉ {Ready, Failed}` is **equivalent** to "Fire failed due to an invalid
  transition" — and is deliberately **more robust** than matching stateless's *untyped*
  unhandled-trigger error string (which has no exported sentinel and could change across
  library versions). We classify on the SM's own post-Fire state, not on error-string
  parsing, so the mapping stays correct even if stateless rewords its error.
- **CHANGELOG + code comment (owner requirement):** the `classifyReplaceReject` doc comment
  above satisfies the "code comment near the handler classification" requirement. Also add
  a CHANGELOG.md entry under the appropriate heading, e.g.: *"fred(docker): the 409 Conflict
  response for a restart/update on a busy lease is now enforced by the lease actor's
  busy-state rejection rather than the pre-routing status flip (ENG-230); the
  externally-observed behavior is unchanged."* QA must verify both the comment and the
  CHANGELOG entry exist.

### 5.3 `internal/backend/shared/leasesm/leasesm.go`
- Remove `FailingRaceSkipped()` from the `SMMetrics` interface (260).

### 5.4 `internal/backend/docker/leasesm_metrics.go`
- Remove `dockerSMMetrics.FailingRaceSkipped` method + its comment (29–33).

### 5.5 `internal/backend/docker/metrics.go`
- Remove the `leaseFailingRaceSkippedTotal` counter block + comment (232–243).

### 5.6 `internal/backend/docker/restart_update.go`
- **Delete `restartRollback`** entirely (16–39).
- **`Restart`** (73–159):
  - Rewrite the 41–72 "ARCHITECTURAL SEAM — intentional" doc comment to "SEAM CLOSED
    (ENG-230)": prelude is read-only validation + pure marshal + release Append; the
    actor's `onEnterRestarting` is the sole writer of `prov.Status`/`CallbackURL`. **The
    comment MUST NOT claim the prelude fast-fail guarantees processing-time state** — it is
    a route-time precondition only; the actor's serial inbox is what guarantees Status
    consistency, and the concurrent-duplicate TOCTOU is handled by the actor's
    `ErrInvalidState`→409 rejection (§3, §5.2).
  - Prelude: keep the three fast-fail guards; keep `prevStatus := prov.Status` (98); keep
    field snapshots (104–110); **delete** `prevCallbackURL` (99), `prov.Status = Restarting`
    (100), and the `req.CallbackURL` write (101–103).
  - Marshal-fail branch (118–124): `return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)`
    (drop the 119–122 prov rollback).
  - Append-fail branch (130–136): `return fmt.Errorf("failed to record release: %w", relErr)`
    (drop the 131–134 prov rollback).
  - Routing-fail (147–151): `opCancel(); return routeErr` (drop `restartRollback` 149).
  - Ack-fail (153–157): `opCancel(); return err` (drop `restartRollback` 155).
  - Add `CallbackURL: req.CallbackURL` to the `RestartRequestedMsg` literal (147).
- **`Update`** (510–631):
  - Rewrite the 503–509 doc comment the same way.
  - Prelude: keep all validation; keep `prevStatus := prov.Status` (585); **delete**
    `prevCallbackURL` (586), `prov.Status = Updating` (587), `req.CallbackURL` write
    (588–590).
  - Append-fail branch (603–609): `return fmt.Errorf("failed to record release: %w", relErr)`
    (drop the 604–607 prov rollback).
  - Routing-fail (619–623): `opCancel(); return routeErr` (drop `restartRollback` 621).
  - Ack-fail (625–629): `opCancel(); return err` (drop `restartRollback` 627).
  - Add `CallbackURL: req.CallbackURL` to the `UpdateRequestedMsg` literal (619).
- Imports `encoding/json`, `time`, `shared` remain used (marshal + Append). No import churn.

### 5.7 `internal/api/handlers.go` (doc-only, low priority)
- Update the comments at 538–541 and 602–605: replace "transitions to Restarting/Updating
  synchronously" with "the actor writes prov.Status before acking the request, so after
  the call returns the lease is already Restarting/Updating." Behavior unchanged.

---

## 6. Test strategy (TDD — failing test first)

### 6.1 Delete / edit existing tests
- **Delete** `internal/backend/docker/lease_actor_test.go::TestOnEnterFailing_RaceWithConcurrentStatusFlip`
  (778–878). It simulates the off-actor write (`b.provisions["lease-1"].Status =
  Restarting` at 839–841) — a scenario that can no longer occur — and asserts on the
  removed `leaseFailingRaceSkippedTotal`. Replaced by 6.3's structural test.
- **`internal/backend/shared/leasesm/lease_actor_test.go`**: remove the
  `countingMetrics.failingRaceSkipped` field (885) and its `FailingRaceSkipped()` method (895).
- **`internal/backend/shared/leasesm/mocks_test.go`**: remove `mockSMMetrics.FailingRaceSkipped` (90).
- Confirm `TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine` (leasesm 334) stays
  green — it seeds Status=Restarting as a fixture and fires only `evDeprovisionRequested`
  (`Restarting.Permit(evDeprovisionRequested)` is untouched); it never routes
  `evRestartRequested`, so removing the `Ignore` does not affect it.

### 6.2 Regression guard — routing failure leaves Status unchanged (the brief's required test)
**NOTE: this test is GREEN at HEAD, not failing-first.** At HEAD `restartRollback` restores
Status to `Ready` on routing failure, so the assertion "Status==Ready after routing-fail"
passes both before and after the change. It is a valuable **regression guard** — it stays
green and proves the stripped prelude never leaves a speculative write — **not** a red
driver. The genuine failing-first driver is §6.3 `TestRestartRequested_WritesStatusBeforeAck`.

In the docker package (e.g. `lease_actor_test.go` or a new `restart_update_test.go`):
`TestRestart_RoutingFailureLeavesStatusUnchanged` —
- seed a provision `Status: Ready` with a non-nil `StackManifest`;
- `b.stopCancel()` so `routeToLeaseBlocking` returns "backend shutting down" before any
  actor work;
- call `b.Restart(ctx, …)`, assert it returns an error;
- assert `b.provisions[uuid].Status == Ready` (no speculative write) and `CallbackURL`
  unchanged.
Add the mirror `TestUpdate_RoutingFailureLeavesStatusUnchanged`.

### 6.3 New tests — contract + busy-state rejection matrix
**Contract test (the genuine red driver).** In the leasesm package (`lease_actor_test.go`),
using `newTestActor` + `newMockProvisionStore`:
- `TestRestartRequested_WritesStatusBeforeAck`: seed `Status: Ready`; route a
  `RestartRequestedMsg{CallbackURL: "cb", Work: <no-op returning a benign ReplaceResult>,
  Ack: ack}`; on `<-ack == nil`, assert `store.Get(uuid).Status == Restarting` and
  `CallbackURL == "cb"`. Mirror for Update. (Pins the handler-publish contract + item #2.)
- `TestRestartFromFailed_Succeeds` / `TestUpdateFromFailed_Succeeds` (**REQUIRED**, leasesm):
  seed `Status: Failed` on a **fresh** actor and route a `RestartRequestedMsg` /
  `UpdateRequestedMsg`; assert success (`ack == nil`, ends `Restarting`/`Updating`, Status
  written before ack, exactly one worker). **Why this is newly meaningful:** ENG-230 unmasks
  the fresh-actor-init-in-`Failed` path — previously the prelude pre-wrote `Status=Restarting`
  so `newLeaseSM` initialized the SM in `Restarting` and `Restarting.Ignore` absorbed the
  event; now a fresh actor for a `Failed` lease inits in `Failed`, exercising the existing
  `Failed.Permit(evRestartRequested→Restarting)` / `Permit(evUpdateRequested→Updating)`
  (lease_sm.go:236–240) on the fresh-actor path for the first time. **Test-only coverage —
  these Permits already exist; do NOT add a duplicate Permit.**

**Busy-state rejection / ordering matrix (all REQUIRED).** These three cases are the
empirical proof that removing the `Ignore` guards (§3) is correct — the SM correctly
rejects evRestart/UpdateRequested from busy states and correctly permits it from Failing:

- **(a) Concurrent same-lease duplicate → SM `Restarting` → 409, one worker, no clobber.**
  `TestSecondConcurrentRestartRejected` (leasesm): seed `Status: Ready`; route restart#1
  with a `Work` closure that increments an atomic `workerCount` and blocks on a release
  channel (msg#1's worker in-flight, SM in `Restarting`); after `<-ack1 == nil`, route
  restart#2. Assert: `workerCount == 1` (no second worker); `errors.Is(<-ack2,
  backend.ErrInvalidState)` (→409); **no workCancel clobber** — give msg#1.Cancel /
  msg#2.Cancel distinct atomic markers, then drive a real `DeprovisionMsg`
  (`evDeprovisionRequested`: `Restarting→Deprovisioning`; `onExitProvisioning` calls
  `a.workCancel`) and assert **msg#1's** marker fired (funcs aren't comparable — don't
  compare pointers; routing through the inbox keeps it race-free). Release the blocked
  worker at the end so the actor quiesces. **+ Update mirror** `TestSecondConcurrentUpdateRejected`.
- **(b) Restart races a Deprovision and loses → SM `Deprovisioning` → 409, NO worker.**
  `TestRestartLosesToDeprovision` (leasesm): seed `Status: Ready`; route a `DeprovisionMsg`
  (or drive `evDeprovisionRequested`) so the SM is in `Deprovisioning`; then route a
  `RestartRequestedMsg`. Assert `errors.Is(<-ack, backend.ErrInvalidState)` (→409) and that
  **no replace worker was spawned** (`workerCount == 0`). Proves the general classifier
  maps Deprovisioning→409, not just the exact duplicate. **+ Update mirror.**
- **(c) Restart races ContainerDied, death lands first → SM `Failing` → restart SUCCEEDS,
  one worker.** `TestContainerDiedThenRestart_Succeeds` (docker package, where the guard's
  `InspectContainer` mock lives): drive a real `ContainerDiedMsg` (container reported
  exited) so the lease goes `Ready→Failing` and `onEnterFailing` runs to completion
  (Status=Failing, FailCount=1); then route a real `RestartRequestedMsg` and assert it
  **SUCCEEDS** (ack == nil, lease ends `Restarting`, exactly ONE worker) via
  `Failing.Permit(evRestartRequested)→Restarting` (lease_sm.go:148, comment 142–147). This
  asserts **success, NOT 409** — `Failing` intentionally permits restart/update retries —
  and proves the death/restart race is resolved by serial ordering, not a recheck.

### 6.4 Keep green
- `internal/api/handlers_test.go::TestRestartLease_BackendIntegration` (3010) and
  `TestUpdateLease_BackendIntegration` (3235) — mock-HTTP-backend tests, unaffected.
- Full gates: `go build ./...`, `go vet ./...`,
  `go test ./internal/backend/shared/leasesm/... ./internal/backend/docker/... ./internal/api/...`,
  and `go test -race` on the leasesm + docker packages (this change is concurrency-sensitive).

---

## 7. Ordered task list for the engineer

1. Add the tests. The genuine **(red)** driver is 6.3
   `TestRestartRequested_WritesStatusBeforeAck` (+Update mirror): it won't compile at HEAD
   (no `CallbackURL` field), and once compiling its `Status==Restarting`/`CallbackURL`
   assertion fails because routing the message directly to the actor writes no Status at
   HEAD. Confirm it fails for the right reason before writing code. The 6.2
   `TestRestart_/Update_RoutingFailureLeavesStatusUnchanged` tests are **GREEN at HEAD**
   (restartRollback already restores Status) — add them as regression guards, not red
   drivers. What actually proves the seam is gone: a `grep` for off-actor `prov.Status =`
   writes in restart_update.go returning clean + `WritesStatusBeforeAck` passing +
   `restartRollback` deleted. Also add the §6.3 `TestRestartFromFailed_Succeeds` /
   `TestUpdateFromFailed_Succeeds` happy-path tests (test-only coverage of the now-exercised
   fresh-actor-init-in-`Failed` path; the `Failed.Permit`s already exist — no SM change).
2. lease_sm.go: add `replaceEntryArgs`, `onEnterRestarting`, `onEnterUpdating`,
   `applyReplaceEntry`; register `OnEntryFrom` on Restarting/Updating (§5.1).
3. lease_actor.go: add `CallbackURL` to both msgs; make `fireAndVerify` variadic; update
   `handleRestartRequested`/`handleUpdateRequested` to pass args **and move `workCancel`
   after `fireAndVerify`**; add `classifyReplaceReject` and use it on the failure path so
   the §4 TOCTOU loser gets a clean 409 via `backend.ErrInvalidState` (`a.terminated` keeps
   sending `errActorTerminated` unchanged) (§5.2, §4).
4. restart_update.go: strip the two preludes to read-only validation + marshal + Append;
   add `CallbackURL` to both message literals; delete the 7 rollback sites; delete
   `restartRollback`; rewrite the two SEAM doc comments (§5.6).
   → 6.2/6.3 tests now pass.
5. Remove the `Ignore(evRestartRequested)`/`Ignore(evUpdateRequested)` lines (§5.1) and
   add the REQUIRED §6.3 busy-state rejection matrix — (a) `TestSecondConcurrentRestartRejected`,
   (b) `TestRestartLosesToDeprovision`, (c) `TestContainerDiedThenRestart_Succeeds` — plus
   the Update mirrors for (a)/(b). (a)/(b) assert 409 via `errors.Is(backend.ErrInvalidState)`;
   (c) asserts SUCCESS via `Failing.Permit`. Confirm green.
6. Simplify `onEnterFailing` (remove recheck + metric call) (§5.1). Delete
   `TestOnEnterFailing_RaceWithConcurrentStatusFlip` (§6.1).
7. Remove the metric end-to-end: `SMMetrics.FailingRaceSkipped` (leasesm.go),
   `dockerSMMetrics.FailingRaceSkipped` (leasesm_metrics.go), `leaseFailingRaceSkippedTotal`
   (metrics.go), and the test impls in `mocks_test.go` + `countingMetrics` (§5.3–5.5, §6.1).
8. handlers.go doc comment touch-up (§5.7).
9. **CHANGELOG.md**: add the decision-B entry (409 now actor-enforced; externally-observed
   behavior unchanged) — §5.2. QA verifies this entry + the `classifyReplaceReject` comment.
10. Run all gates incl. `-race` (§6.4). Then `go vet ./...` to catch any now-unused symbol.

---

## 8. Decisions (RESOLVED by owner/lead — 2026-05-26)

- **A. ACCEPTED.** Leave the cosmetic "deploying" release on routing/ack failure; **no**
  release-only rollback. Verified functionally safe: `LatestActive` skips non-active
  releases (recover.go:150, migrate.go:151) and `ActivateLatest` only touches the latest.
- **B. OVERRIDDEN → restore clean 409.** A (rare) concurrent same-lease double-restart must
  return **409**, not 500. The actor classifies a busy-state `fireAndVerify` rejection as
  `backend.ErrInvalidState` on the ack (§5.2 `classifyReplaceReject`) — not a new sentinel,
  just wrapping the existing `chan error`; `Backend.Restart/Update` returns it verbatim and
  handlers.go already maps `ErrInvalidState`→409. The serial-actor / one-worker outcome and
  the §4 `workCancel` reorder remain REQUIRED. Behavior change documented in CHANGELOG +
  the `classifyReplaceReject` comment (QA-checked).
- **C. CONFIRMED → remove.** Full removal of `lease_failing_race_skipped_total` + the
  `SMMetrics.FailingRaceSkipped` interface method; no out-of-Go references to the metric.
