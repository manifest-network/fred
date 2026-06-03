# ENG-232 — design: tighten remaining provision-write discipline

**Ticket:** ENG-232 (final cleanup under the ENG-229 "actor is the sole writer of live ProvisionState" epic). Priority: Low. Structural hardening, not a bug fix.
**Status:** design approved (Part 2 decision + scope locked 2026-06-03; tightened after an online-grounded idiomatic review). Next: writing-plans → TDD. The acceptance boxes below are **decisions, not yet verified-done**.
**Worktree:** `.claude/worktrees/eng-232`, off `main` (`39eb07b`, post-#103/#104).

## Background

ENG-229 makes the lease actor the sole writer of live `*ProvisionState`. Category A (hot-path seams) is done (ENG-230, ENG-231). ENG-232 is the final cleanup: two spots where live-state writes don't go through the `LeaseProvisionStore.UpdateFn` seam. Neither is a live bug.

## Part 1 — route `doDeprovision`'s direct writes through the store seam

`doDeprovision` (`internal/backend/docker/deprovision.go`) runs **on** the actor goroutine (via `handleDeprovision`, `lease_actor.go:1092`) but reaches into `b.provisions[...]` directly instead of `UpdateFn`. Goal: every **`ProvisionState`** write goes through the seam, so it can later swap to `atomic.Pointer` (ENG-154) uniformly.

### New seam method

`LeaseProvisionStore` (`leasesm.go:215`) has only `Get` + `UpdateFn` — no delete. Add a plain method (idiomatic store-seam shape: simple op as a direct method alongside the closure updater):

```go
// Delete removes the lease's live state from the store. Returns true if an entry
// was present. Takes the same provisionsMu as Get/UpdateFn.
Delete(leaseUUID string) bool
```

Implement in `backendProvisionStore` (`leasesm_adapters.go`) as `Lock(); _, ok := b.provisions[uuid]; delete(b.provisions, uuid); Unlock(); return ok`. Because it shares **the same `provisionsMu`** as `Get`/`UpdateFn`, `handleDeprovision`'s post-call `Get` probe (`lease_actor.go:1110`, sets `a.terminated`) still observes the deletion — the probe relies on **mutex-sharing**, not on `Delete`'s return value (the success-path call site discards the bool; the bool is kept only for symmetry with `UpdateFn`/a clear documented contract).

**Compile-time note:** adding `Delete` to the interface breaks every test double until it gains the method. `mockProvisionStore` (`mocks_test.go`) currently has only `Get`/`UpdateFn` + an unexported `remove()` helper — it must gain an exported `Delete` wired to `remove()`. So "existing suite passes unchanged" is a **runtime** claim, true only after the mock is updated.

### Write-site migrations

The substrate-agnostic seam takes `func(*ProvisionState)`, so only `ProvisionState` fields route through it:

- **Initial-mark** (`Status=Deprovisioning`): → `UpdateFn`. Read `wasReady` (`p.Status == Ready`) **inside the closure before** writing `Status`; capture `ContainerIDs`/`Items`/`Tenant`/`CallbackURL` copies for the teardown; use `UpdateFn`'s `bool` return as the idempotent-exit (**false ⇒ already gone ⇒ return nil**). `activeProvisions.Dec()` stays **outside** the closure, gated on captured `wasReady` (the `UpdateFn` no-side-effect/idempotence contract).
- **Partial-container-failure** (`Status=Failed`, `ContainerIDs=failedIDs`, `LastError`): → one `UpdateFn` closure; capture `diagSnap = leasesm.DiagnosticSnapshot(p)` inside (it takes `*ProvisionState`, `lease_sm.go:1033`). **Asymmetric false-semantics (correctness trap):** unlike initial-mark, this block must **NOT** early-return on `UpdateFn==false`. It still calls `persistDiagnostics(diagSnap, failedIDs)` (outside the closure — disk I/O) and returns the "deprovision partially failed" error. When the entry is gone, `diagSnap` is zero-value and `persistDiagnostics` no-ops via its `entry.LeaseUUID==""` guard. An implementer copying the initial-mark pattern here would silently swallow the error and drop the persist call.
- **Success-delete** (the separate block after volume cleanup fully succeeds): → `store.Delete(uuid)`.
- **Volume-retry block** (the residual — see below).

### The volume-retry block — kept as one atomic documented span

`VolumeCleanupAttempts` lives on the **docker-private `provision` wrapper** (`backend.go:183`), **not** on `ProvisionState`, so it is unreachable through `UpdateFn(func(*ProvisionState))`. It is also incremented and read **atomically with** `ContainerIDs=nil`, the limit test, and (under limit) `Status=Failed`/`LastError`.

**Decision: keep the volume-retry block as a single, documented direct `provisionsMu` span** (its `ProvisionState` writes and its give-up-branch inline `delete` stay direct within that span; snapshot `diagSnap` before the inline delete). The **airtight reasons** (the earlier "avoids a *new* recoverState-interpose window" framing was inaccurate — see below):

1. `VolumeCleanupAttempts` is docker-private and **cannot** route through the substrate seam regardless.
2. Seaming the `ProvisionState` fields would **split one currently-atomic read-modify-write critical section into two** lock acquisitions — exactly the lost-update/atomicity hazard the closure-under-lock seam exists to prevent. Keeping it atomic is behavior-preserving.

**Accuracy note (matters for ENG-285):** this is *not* a "new window" argument — `recoverState` (reconcile-loop goroutine, `backend.go:565`) **already** can drop an in-flight `Deprovisioning` entry today: its merge switch (`recover.go:317-339`) has cases for Provisioning/Restarting/Updating/Failing/Failed but **no `Deprovisioning` case**, and it wholesale-swaps the map (`recover.go:341`). During `doDeprovision`'s lockless compose-down span the entry's containers are already removed, so it can't match `recovered` and is silently dropped. `doDeprovision` already runs four separate re-probing lock spans, so interpose windows already pervade the function. The keep-atomic *decision* stands on reasons (1)+(2); only the prior justification was wrong.

So Part 1's invariant is precisely *"the actor is the sole writer of live **ProvisionState**, through `UpdateFn`/`Delete`"* — with the volume-retry block the one **docker-private-coupled, atomic, actor-goroutine** exception. Fully seaming it (and the recoverState `Deprovisioning` gap it depends on) is tracked as **ENG-285**.

## Part 2 — worker pre-publish: **keep as a documented lock-guarded exception (option a)**

`spawnProvisionWorker`/`spawnReplaceWorker` (`lease_actor.go`) pre-publish `ContainerIDs`/`ServiceContainers` via `UpdateFn` from the **worker** goroutine. **Keep it.** Document the invariant + its safe-publication rationale in the `leasesm` package doc and ENG-229 category C.

**Why (a), not (b) "route through an actor publish message"** (idiomatic-review + code confirmed):

- **The visibility need is real:** a preempting `Deprovision` reads `prov.ContainerIDs` to tear the new containers down; without the pre-publish they orphan.
- **Correctness rests on the workers-barrier happens-before, not the lock alone:** `onExitProvisioning` does `workCancel()` then `waitForWorkers()`; the pre-publish `UpdateFn` runs in the worker's normal flow **before** `workers.Done()` (the **outermost** defer, LIFO, `lease_actor.go:671`). Chain: *pre-publish commit (mutex = release) → `Done()` closes `zeroCh` (barrier) → `waitForWorkers` receives (acquire) → `doDeprovision` reads (mutex)*. Textbook safe-publication per the Go memory model.
- **(b) self-deadlocks:** the actor goroutine is *blocked inside* `waitForWorkers()` and cannot dequeue a "publish" message until the barrier hits zero — but the worker only releases the barrier after sending. The canonical actor self-wait anti-pattern. A non-blocking variant adds a message type + handler for **zero** extra guarantee over the current direct `UpdateFn`.
- **Not even a true race:** the duplicate write (worker + the actor's success entry action `onEnterReadyFromProvision`/`onEnterReadyFromReplaceCompleted`) writes the *same* value, and the late terminal event is dropped by `Deprovisioning.Ignore(evProvisionCompleted/evReplaceCompleted)` on preemption.

**Idiomatic basis:** a mutex for shared *state* is idiomatic Go ("share memory by communicating" is for coordination, not state protection); forcing this publish through a message is both non-idiomatic and the actor self-deadlock anti-pattern. The epic goal is best served by *single-writer-through-the-seam by default, with one narrow barrier-guarded shared-memory exception where message-passing would deadlock.*

## Documentation deliverable

- A package-doc block in `leasesm.go` stating the invariant: **all live `ProvisionState` mutations occur on the actor goroutine via `UpdateFn`/`Delete`, with exactly two documented exceptions** — (1) the worker success-path pre-publish of `ContainerIDs`/`ServiceContainers` (barrier-ordered safe publication; routing through the actor is prohibited because it deadlocks `waitForWorkers`), and (2) the deprovision volume-retry block, kept atomic because it is coupled to the docker-private `VolumeCleanupAttempts`. Document each exception's **failure mode** symmetrically: exception #1's bounded escape is the layered timeout budget (`diagnosticsGatherTimeout` 30s < `workExitWaitTimeout` 75s; a wedged worker degrades to a `recoverState`-reconciled zombie, never state corruption); exception #2 runs inline on the actor goroutine, so a hung `volume.Destroy` blocks the actor *for that lease* until its ctx/timeout (a distinct failure mode worth stating).
- Update **ENG-229's category C** to record the (a) decision and this invariant.

## Testing

1. **The deterministic preemption-ordering guard already exists — verify it stays green under `-race` after the migration.** `TestProvision_DeprovisionWaitsForInFlightGoroutine` (`lease_actor_test.go:794`) and `TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine` (`lease_actor_test.go:334`) already pre-publish `ContainerIDs` under `UpdateFn` from a real concurrent worker, hold it mid-flight with an anti-vacuity negative assertion ("`doDeprovision` must NOT have run yet"), and assert the new IDs are torn down — using `actor.workers.Add()`/`actor.workCancel` directly, **no production or test-only seam**. So the Part 2 happens-before is already guarded; ENG-232 just confirms it survives the Part 1 migration. Optionally add a small `backendProvisionStore.Delete` unit test (present/absent ⇒ true/false; post-`Delete` `Get` probe returns `!ok`).
2. **Write the missing branch tests for the migrated risky paths — they do NOT exist today.** All three `DestroyFn` mocks return nil (`provision_test.go:688`, `stack_test.go:858`/`1028`), so the volume-retry block (give-up + under-limit) and the partial-container-failure block **never execute under test** — "existing coverage passes" is vacuous there, and a dropped `Dec()`, a `diagSnap`-after-delete reorder, or a swallowed callback would pass CI silently. Land the two comment-only stubs (`provision_test.go:844` `TestDeprovision_VolumeExhaustionSendsFailedCallback`, `:849` `TestDeprovision_RetryAfterPartialFailureFiresOneCallback`) **with real bodies**, plus a partial-failure test:
   - **Volume-exhaustion give-up** (`DestroyFn` errors `maxVolumeCleanupAttempts` times): provision deleted, diagnostics persisted, `releaseStore.Delete` called, `CallbackStatusFailed` fired, `deprovisionsTotal` incremented.
   - **Under-limit volume-retry** (`DestroyFn` fails once): `Status=Failed`, entry retained, no callback, `VolumeCleanupAttempts==1`.
   - **Partial-container-failure** (compose Down fails AND `RemoveContainer` fails): `Status=Failed`, `ContainerIDs==failedIDs`, no callback, diagnostics persisted.
   These should land **before/with** migrating each branch (write the test, see the current behavior, migrate, see it stays green).
3. **Migration regression:** the rest of the deprovision coverage (the success path, the gauge, the actor `terminated` probe) stays green; update `mockProvisionStore` with `Delete` so the package compiles.

## Acceptance criteria (decisions — to be verified during TDD)

- [ ] Deprovision `ProvisionState` writes go through `UpdateFn`; map-deletes go through the new `store.Delete` (the volume-retry block's docker-private-coupled writes are the one documented, behavior-preserving exception).
- [ ] `LeaseProvisionStore.Delete` added; `mockProvisionStore` updated; `handleDeprovision`'s `Get` probe still works.
- [ ] Worker pre-publish decision (a) recorded in the `leasesm` package doc **and** ENG-229 category C; no behavior change.
- [ ] The volume-retry give-up / under-limit / partial-container-failure branches gain real tests (the two named stubs landed with bodies); the existing preemption tests stay green under `-race`.

## Out of scope

- **ENG-193** (Category B bootstrap writes: `provision.go` initial insert + `recover.go` map build → `recoveredProvision`/`installRecovered`) — the next epic child.
- **ENG-154** (lock-free `atomic.Pointer`) — the worker pre-publish must stay mutex-backed + barrier-ordered; ENG-154 must preserve that.
- `removeProvision` (`backend.go:687`) and the initial insert (ENG-193 territory).
- Moving `VolumeCleanupAttempts` onto `ProvisionState` (rejected — leaks a docker-volume-specific field into the substrate-agnostic interface).
- **Fully seaming the volume-retry block** and **adding a `Deprovisioning` preserve-case to `recoverState`'s merge** (the prerequisite that makes the split provably safe) — tracked as **ENG-285** per the owner's "keep the block for now, log a ticket" decision.

## Risks

- **Don't split the volume-retry RMW:** keeping `VolumeCleanupAttempts++` + the `ProvisionState` writes in one lock acquisition is required; splitting them is a lost-update/atomicity hazard (and the broader `recoverState`-drops-`Deprovisioning` gap is ENG-285's to fix, not this ticket's).
- **Re-entrancy:** never call **any** lock-taking store method (`Delete`/`UpdateFn`/`Get`) while already holding `provisionsMu` — whether inside an `UpdateFn` closure OR inside the volume-retry raw span (`sync.RWMutex` is non-reentrant → self-deadlock). The give-up branch's inline `delete(b.provisions, uuid)` is correctly kept as a raw-map delete, not `store.Delete`.
- **Idempotent-exit asymmetry:** initial-mark returns nil on `UpdateFn==false`; partial-failure must **not** (it keeps `persistDiagnostics` + the error). `wasReady` must be read before `Status` is overwritten (else the `activeProvisions` gauge drifts — cf. ENG-235).
- **Under-tested branches:** the migrated volume-retry/partial-failure paths have no current coverage; the migration is only safe if the branch tests in Testing item 2 are written first.
