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

---

## 9. PR #93 review-fix addendum (Copilot findings 1–3) — design for the engineer

Three valid Copilot comments on PR #93 (commit 3665df9). Architect designs (this section);
engineer implements; QA re-verifies full gates + the red-first proof. Do NOT commit; owner
integrates onto PR #93.

### 9.1 Finding #1 — false rationale comment (lease_sm.go 193–198)
The implemented comment says "*(and recover.go never resting a lease in
Restarting/Updating)*". That is **factually wrong** (it echoes the architect's original §0
claim, later corrected): recover.go **preserves** Restarting/Updating (recover.go:301–307,
317–320) and only normalizes `Failing→Failed` (332). **Replace the comment block** with:
```go
// No Ignore(evRestartRequested/evUpdateRequested) guard is needed any more.
// Those guards existed only because the off-actor prelude pre-wrote
// prov.Status=Restarting/Updating, so newLeaseSM initialized a freshly-
// created actor's SM directly in Restarting/Updating and the incoming
// request event had to be Ignored as a self-event. With the prelude flip
// gone, readProvisionStatus reads the lease's TRUE status (Ready/Failed)
// and the event is a real Permit transition. recover.go can still leave a
// lease at rest in Restarting/Updating (it PRESERVES those statuses; only
// Failing is normalized to Failed) — but such a lease is unreachable by a
// restart/update event: the prelude fast-fail is a ROUTE-TIME precondition
// that refuses to route on a non-Ready/Failed lease. The one case where
// evRestartRequested meets an already-Restarting SM is a concurrent
// duplicate (handleRestartRequested TOCTOU), correctly rejected as an
// invalid transition → ErrInvalidState (409); keeping Ignore would instead
// no-op it and spawn a second worker. NOTE: this comment must NOT claim the
// prelude guarantees processing-time state — it is only a route-time
// precondition; the actor's serial inbox is what guarantees Status
// consistency.
```

### 9.2 Finding #2 — activeProvisions gauge drift (under-count) — THE FIX
**Bug:** in the container-death-before-queued-restart ordering, the gauge under-counts.
Walk:
1. `Ready→Failing` (`onEnterFailing`) → `ActiveProvisionsDec()` (−1).
2. `Failing→Restarting` (`onEnterRestarting`) → no gauge change.
3. `Restarting→Ready` (`onEnterReadyFromReplaceCompleted`) → re-Inc is gated on
   `result.PrevStatus == Failed` (lease_sm.go:522). But `PrevStatus` was captured by the
   **prelude** (route-time) as **Ready** in this ordering, so **no Inc** → the lease ends
   `Ready` but the gauge stayed −1 → **drift**.

**Root cause:** `PrevStatus` reflects the prelude's route-time view and is **stale** with
respect to the intervening, actor-serial `Ready→Failing` Dec. The same staleness also means
`onEnterReadyFromReplaceRecovered` (which today does **no** gauge op at all) under-counts a
recovered-from-Failed/Failing lease.

**Fix — make gauge accounting actor-authoritative, keyed on the TRUE pre-replace state:**

(a) **Add an actor field** (lease_actor.go, near `workCancel`):
```go
// replaceWasActive records whether the lease was counted in activeProvisions
// (i.e. Status==Ready) at the instant the replace transition began — captured
// by onEnterRestarting/onEnterUpdating reading prov.Status BEFORE overwriting
// it. The replace-outcome entry actions key the gauge on THIS (actor-observed,
// serial) value instead of the prelude-captured PrevStatus, which is stale when
// an intervening Ready→Failing already Dec'd the gauge (the death-before-queued-
// restart ordering). Single-field handoff is safe: the SM allows at most one
// in-flight replace at a time and the actor is serial, mirroring pendingDeathInfo.
replaceWasActive bool
```

(b) **Capture it in `applyReplaceEntry`** (lease_sm.go ~397) — read `p.Status` BEFORE the
overwrite, assign the actor field AFTER the closure (keep the closure side-effect-free):
```go
func (lsm *leaseSM) applyReplaceEntry(args []any, status backend.ProvisionStatus) error {
    var callbackURL string
    if len(args) > 0 {
        if a, ok := args[0].(replaceEntryArgs); ok {
            callbackURL = a.CallbackURL
        }
    }
    var wasActive bool
    lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
        wasActive = p.Status == backend.ProvisionStatusReady // capture BEFORE overwrite
        p.Status = status
        if callbackURL != "" {
            p.CallbackURL = callbackURL
        }
    })
    lsm.actor.replaceWasActive = wasActive
    return nil
}
```

(c) **Key the three replace-outcome entry actions on `replaceWasActive`**, replacing the
`PrevStatus` comparisons:
- `onEnterReadyFromReplaceCompleted` (522): `incActive := !lsm.actor.replaceWasActive`
  (computed OUTSIDE the UpdateFn closure — it's an actor field, not ProvisionState); drop
  the `result.PrevStatus == Failed` check.
- `onEnterReadyFromReplaceRecovered` (544–580): **add** `if !lsm.actor.replaceWasActive {
  cfg.Metrics.ActiveProvisionsInc() }` after the UpdateFn (it currently does no gauge op).
- `onEnterFailedFromReplace` (604): `decActive := lsm.actor.replaceWasActive`; drop the
  `info.PrevStatus == Ready` check.

Resulting symmetric invariant: **Inc on entering Ready iff the lease was NOT active at
replace-start; Dec on entering Failed iff it WAS active.** Verified across ALL orderings the
lead enumerated (gauge contribution of this lease, where the gauge counts **Status==Ready
only** — see below):
| ordering | path | net |
|---|---|---|
| restart-from-Ready, success | Ready(+1)→Restarting(wasActive=T, no-op)→Ready(!T⇒no Inc) | +1 ✓ |
| restart-from-Ready, fail→Failed | Ready(+1)→Restarting(T)→Failed(T⇒Dec) | 0 ✓ |
| restart-from-Failed, success | Failed(0)→Restarting(F)→Ready(!F⇒Inc) | +1 ✓ |
| restart-from-Failed, fail→Failed | Failed(0)→Restarting(F)→Failed(F⇒no Dec) | 0 ✓ |
| death-then-restart, success | Ready(+1)→Failing(Dec,0)→Restarting(F)→Ready(Inc) | +1 ✓ (was −1 drift) |
| death-then-restart, fail→Failed | Ready(+1)→Failing(Dec,0)→Restarting(F)→Failed(no Dec) | 0 ✓ (no double-Dec) |
| death-then-restart, recovered→Ready | Ready(+1)→Failing(Dec,0)→Restarting(F)→Ready-recovered(Inc) | +1 ✓ |
| restart-then-death | Ready(+1)→Restarting(T); ContainerDied Ignored in Restarting | +1 ✓ |

**Gauge definition + why the corrective Inc lands on →Ready:** the reconcile authority
(`recover.go:362–380`) computes `activeProvisions.Set(readyCount)` counting **only
`Status==Ready`** leases. So the gauge tracks "# Ready leases," and the corrective re-Inc for
a death-then-restart fires on the `Restarting→Ready` transition (`onEnterReadyFromReplace*`),
not on entry to Restarting/Updating. This matches `replaceWasActive` (captured = source was
Ready). The periodic `Set` is also the **self-heal**: the drift is transient (corrected on
the next reconcile tick), so this is a correctness fix, **not** corruption recovery.

**Out of scope (do NOT add):** in the normal restart-from-Ready case the lease stays counted
during `Restarting` (no Dec on `Ready→Restarting`), which transiently disagrees with the
reconcile `Set` (which excludes `Restarting`). This is **pre-existing** behavior, unchanged
by this fix, and self-heals via `Set`. Do **not** "fix" it by adding a Dec to
`onEnterRestarting/Updating` — that is the big-rewrite the lead warned against and is not what
Copilot flagged.

**Scope note on `PrevStatus` (RESOLVED — owner ruled (a) FULL REMOVAL, this PR):** after (c),
`PrevStatus`'s only two readers (lease_sm.go:522, :604) are gone, so the field is **write-only
dead** (NOT "retained for diagnostics" — there is no diagnostic reader). The owner chose full
removal this pass. **Remove `PrevStatus` end-to-end** via a compiler-guided cascade:
  - the `PrevStatus` field on `ReplaceSuccessResult` and `ReplaceFailureInfo` (lease_sm.go);
  - the `PrevStatus` field on `replaceContainersOp` and every populating site
    (restart_update.go:142/160/232/251/613/631);
  - the prelude `prevStatus := prov.Status` captures and the `doRestart`/`doUpdate` signature
    params that thread it;
  - the related doc comments (restart_update.go:73/174/552; lease_sm.go:777/787/790);
  - any test sites that set `PrevStatus:` (incl. the §9.3 test — drop the field there; the
    test no longer needs it since the gauge keys on `replaceWasActive`).
Gauge correctness (`replaceWasActive`) is independent of this removal. Let the compiler drive
the cascade (build will flag every site).

### 9.3 Finding #3 — red-first test (restart_update_test.go ~537)
The existing `ContainerDiedThenRestart_Succeeds` test's Work closure returns
`ReplaceSuccessResult{PrevStatus: backend.ProvisionStatusFailed}` — which **masks** the
drift, because the old `PrevStatus==Failed` gate happens to Inc and balance the Dec.

**Sequencing matters because `PrevStatus` is being removed this pass (owner ruling):**
1. **Red step (against current code, field still present):** **remove** the masking
   `PrevStatus: backend.ProvisionStatusFailed` from the Work result — do **NOT** replace it
   with `PrevStatus: Ready` (the field is going away). With it absent, `result.PrevStatus`
   is the zero value, the old gauge gate (`== Failed`) is false → no Inc → drift. Add:
   capture `activeBefore := testutil.ToFloat64(activeProvisions)` at the top; after the
   restart completes (release the blocked worker, await ack/terminal), assert
   `testutil.ToFloat64(activeProvisions) == activeBefore` (net-zero: lease started Ready,
   ends Ready). Optionally assert the intermediate value is `activeBefore-1` right after the
   death. → **RED** (ends `activeBefore-1`). (Either omitting `PrevStatus` or setting it to
   `Ready` is red here; omit it so the test already matches the post-removal struct shape.)
2. **Green step:** after the §9.2 rekey + the PrevStatus full removal, the `PrevStatus` field
   no longer exists on `ReplaceSuccessResult`, the Work result simply omits it, and the gauge
   (now keyed on `replaceWasActive`) re-Incs on `Restarting→Ready` → assertion passes.
Keep the existing success assertions (ack==nil, ends Restarting, exactly one worker via the
atomic counter). The test must never reference `PrevStatus` in its final (green) form.

### 9.4 Ordered tasks (engineer)
1. **(red)** Apply the 9.3 test edit; confirm it FAILS on the gauge assertion for the right
   reason (off-by-one under-count), success assertions still pass.
2. lease_actor.go: add the `replaceWasActive` field (9.2a).
3. lease_sm.go: capture in `applyReplaceEntry` (9.2b); rekey the three outcome actions
   (9.2c); update `PrevStatus` doc comments (9.2 scope note); fix the 9.1 comment.
   → 9.3 test now green.
4. Re-run full gates: `go build ./...`, `go vet ./...`, `golangci-lint run ./...` (v2.8.0),
   `go test ./internal/backend/shared/leasesm/... ./internal/backend/docker/... ./internal/api/...`,
   `go test -race` on leasesm + docker. Watch for any existing test that over-asserted "no
   Inc on recovered" (9.2c adds a correct Inc on recovered-from-non-active).

### 9.5 Finding #1 secondary investigation — can a restart/update reach a RECOVERED Restarting/Updating lease? (RESULT: NO)
**Question (lead):** if any path re-routes a restart/update to a lease that recovery left in
`Restarting`/`Updating`, dropping `Ignore` turns an old silent no-op into a 409 — behavior
change worth handling?

**Investigated the actual code; answer: it cannot happen, and there is zero behavior change.**
- `RestartRequestedMsg` / `UpdateRequestedMsg` are constructed in **exactly one place each** —
  `Backend.Restart` (restart_update.go:113) and `Backend.Update` (restart_update.go:581) —
  both **after** the prelude read-only fast-fail (`Status ∈ {Ready, Failed}`, else
  `ErrInvalidState`). No other production code builds or routes these messages (grep-verified).
- The **only** non-HTTP caller of restart is `ReconcileCustomDomain`
  (reconcile_custom_domain.go:125), and it calls the **public `b.Restart`** — i.e. it goes
  through the same prelude fast-fail. It does not bypass it.
- The `reconcileLoop` / `containerEventLoop` (recover.go:471, 620) route **only**
  `ContainerDiedMsg` — never restart/update.
- Therefore a recovered lease at rest in `Restarting`/`Updating` (Status ∉ {Ready, Failed})
  is **refused at the prelude** (`ErrInvalidState`→409) and `evRestartRequested`/`evUpdateRequested`
  is **never fired into its SM**. It never reaches `classifyReplaceReject` and never reaches
  the (now-deleted) `Ignore`.

**Behavior-change assessment: NONE.** The prelude fast-fail **predates ENG-230** (it was
always the first thing `Backend.Restart/Update` did). So a restart on a recovered-Restarting
lease was a prelude 409 *before* this work and remains a prelude 409 after. The `Ignore`
guards never covered this path — they only absorbed the *fresh-actor-init-in-Restarting*
self-event caused by the prelude's **own** pre-write (which ENG-230 deletes). Dropping
`Ignore` is therefore safe with no observable change for the recovered-rest case; the only
path that now produces a 409 *via the actor* (`classifyReplaceReject`) is the concurrent
TOCTOU duplicate, where the prelude legitimately saw `Ready`. This confirms the §9.1 comment
and needs no extra handling.

### 9.6 THIRD `prevStatus` reader (assessment) — `doRestart` preflight `Restored` (same staleness as #2)
The engineer found a third reader the §9.2 cascade missed (grep case-sensitivity: lowercase
`prevStatus`): **restart_update.go `doRestart`:143 → `Restored: prevStatus == ProvisionStatusReady`**.
It drives the **restart preflight-failure** recovered-vs-failed choice (SKU-profile lookup
fails *before any container is touched*): `Restored`→`evReplaceRecovered` (Ready) vs
`!Restored`→`evReplaceFailed` (Failed). (The post-container-replace path at `:233` uses the
real rollback result `restored`, NOT `prevStatus` — fine. `doUpdate` preflight is
`Restored:false` unconditionally — see asymmetry below.)

**(1) Is `Restored = replaceWasActive` correct, and does it fix the race? — CONFIRMED, for
the restart-preflight branch only.** A preflight failure touches no containers, so the lease
is left in exactly its replace-start state; "recovered to Ready" is correct iff its
containers were running at replace-start, i.e. iff the SM source was `Ready` — which is
exactly `replaceWasActive`:
| source | containers at start | correct outcome | replaceWasActive | ✓ |
|---|---|---|---|---|
| Ready | running | recovered→Ready | true | ✓ |
| Failed | dead | failed→Failed | false | ✓ |
| Failing (death-then-restart) | just died | failed→Failed | false | ✓ **(the fix)** |
Today, the death-then-restart+preflight-fail case captures `prevStatus=Ready` at the prelude
(before the death) → `Restored=true` → lease wrongly ends **Ready with dead containers**.
`replaceWasActive` (actor-observed source = `Failing`) → `false` → correctly **Failed**.
**Scope caveat:** this equivalence holds for the **restart-preflight** branch. It does NOT
generalize: the **post-replace** path must keep `result.Restored` (the real `rollbackViaCompose`
result, not stale), and **`doUpdate` preflight must stay `Restored:false` unconditionally** —
that is an intentional policy (an update that never pulled the new image did not achieve the
desired state ⇒ Failed even from a Ready source), NOT a staleness bug. Do not "unify" update
onto `replaceWasActive`.

**(2) Severity — reachable but rare; transient/self-healing; bounded.** Reachable only with
BOTH (a) a SKU profile removed/renamed in config between provision and restart (config drift)
AND (b) the death-then-queued-restart ordering (death detected → `Failing` before the restart
dequeues). Self-heal: confirmed — `reconcileLoop`→`recoverState` (every `ReconcileInterval`)
reconciles in-memory Status against actual container state (Ready→actual when containers are
dead, recover.go:223–226) and re-`Set`s the gauge (380); so the wrongly-`Ready` lease is
corrected within one reconcile tick — same transient class as the #2 gauge drift. Note the
**on-chain callback is `CallbackStatusFailed` in BOTH the recovered and failed paths**
(lease_sm.go onEnterReadyFromReplaceRecovered / onEnterFailedFromReplace), so there is **no
wrong on-chain success**; the symptom is a wrongly-`Ready` API status (+ gauge over-count) for
≤ one `ReconcileInterval`. So: real bug, same root cause as #2, low severity, self-healing.

**(3) Option-(a) protocol sketch (enables full `prevStatus` removal AND fixes the staleness).**
Recommended minimal shape — thread the actor-observed flag as a `work` argument (no
worker→actor-field read, no terminal-message protocol change, existing recovered/failed
switch untouched):
- `RestartRequestedMsg.Work` / `UpdateRequestedMsg.Work`: `func() ReplaceResult` →
  `func(wasActive bool) ReplaceResult`.
- `spawnReplaceWorker`: read `wasActive := a.replaceWasActive` **on the actor goroutine**
  (before `a.cfg.WG.Go`), then call `work(wasActive)` inside the worker. (Actor owns the read;
  worker receives a plain arg.)
- `doRestart` preflight branch: `Restored: wasActive` (replaces `prevStatus == Ready`).
- `doUpdate` preflight branch: keep `Restored: false` (ignores `wasActive` — preserve the
  intentional update asymmetry).
- `doReplaceContainers` post-replace path: unchanged (`Restored` = `rollbackViaCompose` result).
- The actor's `spawnReplaceWorker` switch (`case result.Restored:` → recovered) is **unchanged** —
  `result.Restored` now carries the correct value in every branch.
- This removes the LAST `prevStatus` reader, so `prevStatus`/`PrevStatus` can be deleted
  end-to-end (completing the owner's (a) full-removal) AND the death-then-restart+preflight
  race is fixed in the same pass.
- (Smaller-diff alternative if the owner prefers no signature change: have the switch in
  `spawnReplaceWorker` compute the preflight branch as `result.Failure.Operation == "restart"
  && a.replaceWasActive` when `!result.Failure.OldStopped`. Race-free via the write-before-spawn
  happens-before, but reads an actor field from the worker — slightly less clean. Either works.)

**Recommendation:** option **(a)** via the `work(wasActive)` arg. It is the *same* root cause
as #2 (stale prelude `prevStatus`), the marginal cost is small (we already add
`replaceWasActive`), and it both completes the clean full-removal and removes a latent
wrong-`Ready` race. Severity is low/self-healing, so it is not urgent on its own — but since
the full removal the owner chose **cannot complete without relocating this decision** anyway
(it is the third reader), folding the fix in here is the coherent move. Option (b)
partial-removal (keep `doRestart`'s local `prevStatus` for this one decision, honest comment)
is viable if the owner wants to defer the protocol tweak — but it leaves the (self-healing)
race in place and keeps one `prevStatus` reader.

### 9.7 LOCKED DESIGN — owner ruled (a): relocate `doRestart`-preflight `Restored` to the actor + full `prevStatus` removal
Owner approved option (a). This is the authoritative spec (supersedes the §9.6 sketch).
Engineer implements via this spec + echo-back; do not deviate without re-gating.

**Mechanism — a dedicated flag, NOT `OldStopped`.** `OldStopped=false` is true for BOTH
restart-preflight (want `Restored = replaceWasActive`) AND update-preflight (must stay
`Restored=false`), so it cannot drive the decision. Add a flag set by **exactly one site**
(`doRestart`'s preflight branch); every other path leaves it false and is byte-for-byte
unchanged.

**Per-site changes (relocation):**
1. `leasesm/lease_sm.go` `ReplaceResult` (currently `{CallbackErr, Err, Restored, Success,
   Failure}`): **add** field
   ```go
   // RecoveredIfSourceActive, when true, tells the actor to derive the
   // recovered-vs-failed outcome from LeaseActor.replaceWasActive (the
   // actor-observed SM source) INSTEAD of Restored. Set ONLY by doRestart's
   // preflight-failure branch (no container was touched, so "recovered to
   // Ready" is correct iff the lease was Ready/running at replace-start —
   // which the prelude's route-time prevStatus snapshot got wrong under the
   // death-before-queued-restart ordering, ENG-230 PR#93 finding #6). Every
   // other failure leaves this false: update-preflight stays Restored=false
   // (intentional), post-replace keeps Restored=rollback result.
   RecoveredIfSourceActive bool
   ```
2. `leasesm/lease_actor.go` `spawnReplaceWorker`: capture the actor field **on the actor
   goroutine, before `a.cfg.WG.Go`**, and apply it in the existing switch (do NOT read
   `a.replaceWasActive` inside the worker closure):
   ```go
   func (a *LeaseActor) spawnReplaceWorker(work func() ReplaceResult) {
       wasActive := a.replaceWasActive // actor goroutine; happens-before the worker
       a.workers.Add()
       a.cfg.WG.Go(func() {
           ...
           result := work()
           ... (success pre-publish unchanged) ...
           recovered := result.Restored
           if result.RecoveredIfSourceActive {
               recovered = wasActive
           }
           switch {
           case result.Err == nil:
               terminalMsg = replaceCompletedMsg{result: result.Success}; event = "replace_completed"
           case recovered:
               terminalMsg = replaceRecoveredMsg{info: result.Failure}; event = "replace_recovered"
           default:
               terminalMsg = replaceFailedMsg{info: result.Failure}; event = "replace_failed"
           }
       })
   }
   ```
   (The work-closure signature is **unchanged** — `func() ReplaceResult`. No message-protocol
   change. The post-replace and update-preflight branches never set the flag, so `recovered`
   == their existing `result.Restored` → provably unaffected.)
3. `docker/restart_update.go` `doRestart` preflight branch (:140–150): drop
   `Restored: prevStatus == backend.ProvisionStatusReady` (:143) and `PrevStatus: prevStatus`
   (:145); **set `RecoveredIfSourceActive: true`** on the returned `ReplaceResult`. (`doRestart`
   no longer needs `prevStatus` at all.)

**Full `prevStatus`/`PrevStatus` removal map (owner-confirmed; do after the relocation so the
compiler guides the cascade) — current PR#93 line refs:**
- `docker/restart_update.go`: comment :72–73 and :554–555; captures `prevStatus := prov.Status`
  at :79 (Restart) and :559 (Update); param passes at :113 and :582; `doRestart` (:130) and
  `doUpdate` (:598) signature params; write-only stores at :145 (already gone via step 3),
  :163, :235, :254, :617, :635; `replaceContainersOp.PrevStatus` field :177.
- `leasesm/lease_sm.go`: `ReplaceSuccessResult.PrevStatus` (:816 + comment :811–815) and
  `ReplaceFailureInfo.PrevStatus` (:832 + comment :830–831).
- Remove every "route-time view / diagnostics / NOT the gauge key" comment that referenced
  `PrevStatus`.
- **Acceptance:** case-insensitive `grep -rin prevstatus internal/` → **ZERO**. (`replaceWasActive`
  is the only remaining source-activeness signal; `RecoveredIfSourceActive` is the only flag.)
- Note `doUpdate` preflight (:615 area) stays `Restored: false` (no flag, no `prevStatus`) —
  unchanged behavior, just loses its write-only `PrevStatus` store.

**Red-first test matrix (`leasesm` and/or `docker`; all 4 committed in final-protocol shape).**
Set the SM source by seeding status (+ driving a real `ContainerDied` for the Failing case),
route the request directly to the actor with a `Work` closure returning a **preflight**
`ReplaceResult`, assert the terminal Status:
| # | source | request | Work returns | assert | role |
|---|---|---|---|---|---|
| 1 | **Failing** (drive ContainerDied first) | restart | `{Err, RecoveredIfSourceActive:true, Failure:{Operation:"restart"}}` | lease **Failed** (not Ready) | **the latent bug** |
| 2 | Ready | restart | `{Err, RecoveredIfSourceActive:true, Failure:{Operation:"restart"}}` | lease **Ready** (recovered) | guard |
| 3 | Failed | restart | `{Err, RecoveredIfSourceActive:true, Failure:{Operation:"restart"}}` | lease **Failed** | guard |
| 4 | Ready | update | `{Err, Restored:false, Failure:{Operation:"update"}}` (NO flag) | lease **Failed** | regression guard: update-preflight unaffected |

- After relocation: actor maps `RecoveredIfSourceActive`→`wasActive` (= source was Ready):
  case1 Failing→false→Failed ✓; case2 Ready→true→Ready ✓; case3 Failed→false→Failed ✓;
  case4 no flag→`result.Restored`=false→Failed ✓.
- **RED demonstration (case 1):** before wiring, reproduce today's behavior by having case-1's
  Work return the CURRENT-protocol output `{Err, Restored:true, Failure:{Operation:"restart"}}`
  (faithful to doRestart-preflight when the prelude captured `Ready` before the death) → the
  current actor takes `case result.Restored:` → `evReplaceRecovered` → lease **Ready** → the
  `assert Failed` **FAILS**. Then implement steps 1–3 and switch case-1's Work to the
  final-protocol `RecoveredIfSourceActive:true` shape → **PASSES**. Engineer shows red→green.
- Optional gauge tie-in on case 1: assert final `activeProvisions == activeBefore-1` (death
  Dec'd; ends Failed so no re-Inc) — links #2 and #6.

**Provably-unaffected check (state in the echo-back):** update-preflight (no flag → `recovered
= result.Restored = false`) and post-replace-rollback (no flag → `recovered = result.Restored
= rollbackViaCompose result`) both bypass the new branch entirely; only `doRestart` preflight
sets the flag.

**PRODUCER-side test (REQUIRED — lead's fidelity note).** Cases 1–4 use test-supplied `Work`
closures, so they verify only the ACTOR's *consumption* of the flag — a regression where
`doRestart` stops *setting* it would still pass them. Add a fifth, producer-side test:
- `TestDoRestartPreflight_SetsRecoveredIfSourceActive` (docker pkg): invoke the **real**
  `doRestart` (or `b.Restart` driving it) with a SKU profile that `GetSKUProfile` cannot
  resolve (config drift) so the preflight branch fires; assert the returned `ReplaceResult`
  has `RecoveredIfSourceActive == true`, `Err != nil`, and does NOT derive `Restored` from any
  status. This pins the producer half so the two halves (doRestart SETS the flag ↔ actor MAPS
  it to `wasActive`) are independently regression-guarded.

### 9.8 DECIDING FACTOR for descope — is `doRestart:143` staleness NEW (ENG-230 regression) or PRE-EXISTING?
**DETERMINATION: PRE-EXISTING. Confirms the lead's analysis. The pre-ENG-230 recheck-bail
NEVER prevented the wrong-Ready-on-preflight-fail outcome — answer to the pointed question is
NO.** Therefore :143 + full removal is a clean, defer-able follow-up; only the #2 gauge
under-count is a genuine ENG-230 regression (already fixed in core).

The two are independent axes:
- The `onEnterFailing` recheck governed the **gauge/FailCount** axis only: it `raceSkipped`ed
  (no `Dec`, no `FailCount++`) when a concurrent off-actor Restart had flipped `Status` off
  `Ready`. Removing it made `onEnterFailing` `Dec` unconditionally → the #2 under-count. **#2
  is genuinely new.** ✓ (lead's point 1).
- `doRestart:143` `Restored = prevStatus==Ready` reads the **prelude-captured `prevStatus`
  snapshot**, computed in `doRestart` entirely **independent** of the recheck. The recheck
  lives in the death path (`onEnterFailing`); it never gated the restart message, which is a
  separate inbox message that proceeds to `doRestart` regardless.

**Both-eras trace of the triggering window** (death races a Ready-lease restart; preflight
then fails). The window that triggers :143 is: *the restart prelude reads `Status==Ready`
(so `prevStatus=Ready`) before `onEnterFailing` flips it.* (If the death fully completes to
`Failing` first, the prelude fast-fails `ErrInvalidState` — Failing ∉ {Ready,Failed} — and
`doRestart` never runs; that bound is identical in both eras and is not the bug.)

- **PRE-ENG-230:** prelude reads `Ready` → `prevStatus=Ready`, writes `Status=Restarting`
  off-actor, routes the restart (Work carries `prevStatus=Ready`). The container dies; the
  death's `onEnterFailing`, when it runs, reads `Status==Restarting` (the prelude wrote it) →
  **recheck BAILS** (no `Dec`/`FailCount++`). The restart proceeds anyway → `doRestart`
  preflight fails → `Restored = prevStatus(Ready) == Ready = true` → `evReplaceRecovered` →
  **lease ends Ready with dead containers.** The bail prevented the gauge double-count, **not**
  the wrong-Ready.
- **POST-ENG-230 (pre-#6-fix):** prelude reads `Ready` → `prevStatus=Ready`, routes (no
  off-actor write). Death dequeued first → `Ready→Failing` (`onEnterFailing` `Dec`s, no
  recheck) → restart dequeued → `Failing→Restarting` → `doRestart` preflight fails →
  `Restored = prevStatus(Ready) == Ready = true` → `evReplaceRecovered` → **lease ends Ready
  with dead containers.**

**Identical :143 result in both eras.** ENG-230 changed the *intermediate SM path*
(bail-and-stay-`Restarting` vs `Failing`-then-`Restarting`) and the *gauge/FailCount* bookkeeping
(the new #2 axis) — but the `prevStatus`-snapshot fed to `:143`, and the resulting
recovered→wrong-Ready outcome, are unchanged. The recheck-bail operated on a different field
(`Status`/`FailCount`/gauge) and at a different site (`onEnterFailing`); it had no causal path
to the `doRestart` `Restored` decision. So it never prevented the wrong-Ready.

**Recommendation for the owner:** DESCOPE-SAFE. Shipping the green core now (with #1 doc + #2
gauge fix + their tests) and deferring `:143` relocation + full `prevStatus` removal (§9.7) as
a clean follow-up does **not** ship a new ENG-230 regression — `:143` is a pre-existing,
rare, self-healing (reconcile re-detects within one `ReconcileInterval`), no-wrong-on-chain-
success bug (callback is `Failed` in both recovered/failed paths, §9.6). Caveat to record in
the follow-up: until §9.7 lands, the now-write-only `prevStatus` field remains (the §9.6
"write-only dead" comment must say so honestly — NOT "diagnostics"), and `grep prevstatus`
will be non-zero by design until the follow-up.
