> Historical report from the November 2026 modeling bundle. See [README.md](README.md) for implementation status, reproducible simulations, and verification limits. The original author's Apalache claims below have not been independently rerun here.

# Fred × Quint: first pass

These are eight small Quint models of Fred and manifest-ledger (`manifest-network/fred` @ `a802bca`, Nov 6 2026). Each one asks a single question about a slice of the system. Where a bug was found, the model has two variants: `current` follows the code as written, and `fixed` applies the proposed change.

| Model | Question | `current` | `fixed` |
|---|---|---|---|
| `lease_death_routing.qnt` | Can a lease sit in **Ready** with a dead container that nothing will ever tell the actor about? | **Violated**: 6-step trace via restore (restart and update traces too) | Holds |
| `ack_lane_flush.qnt` | Can an ack caller get an **error for a lease that stayed PENDING** (one that could have been acked)? | **Violated**: one lease leaving PENDING mid-flush makes healthy leases fail | Holds |
| `restore_retention.qnt` | Can a tenant's retained data end up where no restore can reach it again? | **Violated**: 4-step trace (destination rejected, restore commits, data retained under REJECTED D) | Holds |
| `billing_ledger.qnt` (manifest-ledger) | Is every token accounted for, are reservations and counters consistent, and is the provider paid for every whole second? | Conservation, reservations and counters hold. **Pay-per-second violated** (7 steps) | Holds |
| `maintenance_commands.qnt` | Do restart/update commands settle exactly once, in agreement with the backend, without losing an applied update or leaving a command pending forever? | Holds (all four properties) | n/a |
| `lease_settlement.qnt` | (a) Can Fred **reject a lease after authenticating its Success callback**? (b) Once nothing can act any more, is the lease **consistent** (no workload on an ended lease, no billing without a Ready workload, no PENDING lease stuck with a Ready workload)? | (a) **Violated**: 6-step trace. (b) Holds | (a) Holds. (b) Holds |
| `custom_domain_handoff.qnt` | When a custom domain moves from one lease to another on the same backend, is it ever routed to **two leases at once**, or kept by the **former owner**? | **Violated**: 3-step traces for both | Holds |
| `billing_fairness.qnt` (manifest-ledger) | When a tenant's credit runs out across two providers, is each lease paid for its **first `min_lease_duration`**, as the README promises? | **Violated**: one provider can be paid nothing for a fully reserved lease | Holds (shrinking per-lease reservation, see Finding 7) |

Sanity check: `lease_death_routing.qnt` run with `--step=provisionOnlyStep` holds on `current`. In other words, the model reproduces the provision-path fix ENG-1125 already shipped.

## Running

```bash
npm i -g @informalsystems/quint        # 0.33.x; `verify` also needs Java 17+

# random simulation (fast)
quint run lease_death_routing.qnt --backend=typescript --main=current \
  --invariant=deadReadyContainerIsInFlight --max-steps=20
quint run ack_lane_flush.qnt --backend=typescript --main=current \
  --invariant=noSpuriousAckError --max-steps=20

# exhaustive bounded model checking (Apalache, downloaded on first use)
quint verify lease_death_routing.qnt --main=fixed \
  --invariant=deadReadyContainerIsInFlight --max-steps=16
quint verify lease_settlement.qnt --main=current \
  --invariant=noRejectAfterAuthenticatedSuccess --max-steps=8
quint verify lease_settlement.qnt --main=current \
  --invariant=quiescentIsConsistent --max-steps=14
quint verify custom_domain_handoff.qnt --main=current \
  --invariant=exclusiveRoute --max-steps=10        # also: stripOnly, exclusiveOnly, fixed, verified
quint verify billing_fairness.qnt --main=current \
  --invariant=guaranteeHonored --max-steps=10      # fixed: ~5 min per invariant
```

What was checked:

- **Death routing:** Apalache explored every execution up to 16 steps. `current` fails, while `fixed` and `current`+`provisionOnlyStep` hold. Random simulation agrees (30k traces each).
- **Ack lane:** simulation on the 8-lease instance: `current` fails within seconds, and `fixed` held over 20k traces of up to 40 steps. Apalache on the 5-lease/3-block instances: `currentSmall` fails within 10 steps, and `fixedSmall` holds for every execution up to 12 steps (~7 min).

---

## Finding 1: a container that dies right after a restart, update or restore is missed

**What happens.** ENG-1125 handled a container death that races the Provisioning → Ready transition. Every death is written to `liveDeathLedger` first. Then, at the Ready entry, `UpdateFn` calls `redispatchStartupDeaths` for the new cohort. That hook only fires when the old status was Provisioning:

```go
// internal/backend/docker/leasesm_adapters.go, backendProvisionStore.UpdateFn
if wasProvisioning && isReady {
    s.backend.redispatchStartupDeaths(p.ContainerIDs)
}
```

Restart, update **and restore** all go through `spawnReplaceWorker` and pass through Restarting/Updating → Ready, so they never reach that hook. A new-cohort container that dies after the worker's last startup inspection but before the actor's Ready transition is dropped in one of two ways:

- **before pre-publish:** the container isn't in any projection yet, so `dispatchLiveContainerDeath` returns and relies on a ledger redispatch that never comes;
- **after pre-publish:** the status is Restarting/Updating, so `observation.Current` is false, the routing is refused and the death is counted in `dieEventDroppedTotal`.

The lease then reports **Ready**, and Fred sends the success callback with a dead workload. Only the backend's reconcile sweep (5 min by default) finds the death, and it arrives *unattributed*, so it never counts toward the ENG-799 failure budget. The window covers the worker's durable commits (`CommitOperationSuccess`, plus restore-source finalization for a restore). It's the same window ENG-1125 decided was worth closing.

**Minimal trace (Apalache, restore path):**

```
Reserved --requestRestore--> Restarting   (new container 0)
verify                                    (worker's last inspection: 0 running)
die(0)                                    (ledger += {0})
dispatch(0) -> not in projection, ledger only
prePublish                                (projection = {0}, still Restarting)
complete                                  -> Ready, no redispatch   ✗ dead container, nobody told
```

**Fix (small):** fire the hook on every transition that installs a new cohort:

```go
wasStarting := p.Status == backend.ProvisionStatusProvisioning ||
    p.Status == backend.ProvisionStatusRestarting ||
    p.Status == backend.ProvisionStatusUpdating
...
if wasStarting && isReady {
    s.backend.redispatchStartupDeaths(p.ContainerIDs)
}
```

Optionally, `dispatchLiveContainerDeath` could also return silently for Restarting/Updating, the same way it does for Provisioning. Those deaths are then left to the ledger instead of being counted as drops. A redispatched death for a container that is actually running is harmless, because `guardContainerActuallyDied` re-inspects the container and re-checks its generation. That covers ReplaceRecovered (rollback to the old cohort) and any ID reuse.

## Finding 2: one lease leaving PENDING fails most of an ack batch

**What happens.** `MsgAcknowledgeLease` is all-or-nothing on chain (`validatePendingLeaseBatch` fails fast). If any lease in a lane's batch leaves PENDING between the lane's pending query and the broadcast, the whole batch fails. Two things can cause that: a tenant `MsgCancelLease`, or the chain's `pending_timeout` expiry in EndBlock. The second is likely for slow provisions under load. After the failure, `flush` falls back to `acknowledgeIndividually`, which sends **one tx per lease, one after another, with a wait for each block**, all inside the single flush deadline (`AckFlushTimeout` = `tx_timeout` = 30s, with ~5s blocks ≈ 6 txs). In a full batch of 50, about 6 leases get acked. **The other ~43 callers get context-deadline errors for leases that were perfectly ackable.** Their callbacks then fail and retry through the backend outbox, and the reconciler retries on its next sweep. Meanwhile the lane is blocked for the whole 30s.

**Trace (8 leases, budget 6):**

```
query: l1..l8 PENDING -> batch
l7 leaves PENDING (cancel / pending_timeout)
broadcast: ErrLeaseNotPending -> individual fallback
fallback: l3 l5 l2 l6 l1 l4 acked (6 blocks)
fallback: l8 -> ctx deadline -> Err   ✗ l8 is still PENDING
```

**Fix (small):** when the batch fails with something other than a fee/gas error, re-observe the batch's leases, drop the ones that aren't PENDING, and retry the filtered set as a *batch*. Loop while the budget remains. Fall back to individual txs only if the filtered retry fails *again* for an unrelated reason. The `fixed` variant models exactly this. An alternative is to read the offending UUID out of `ErrLeaseNotPending` and drop just that lease.

## Finding 3: a healthy lease is rejected if its ack fails near the callback timeout

**Scope of the model.** `lease_settlement.qnt` covers one lease and one provision operation across chain × placement attempt × operation Registry × backend × the backend's durable callback outbox. The actors are the dispatcher, the backend worker, callback application, the timeout checker, the reconciler (acting on a stale inventory snapshot), the tenant/chain ending the lease, and providerd crash/restart.

**What happens.** The backend reports Success, and Fred authenticates it and claims the operation. Then the acknowledgment fails. That can be a chain/RPC hiccup, the flush deadline, or the batch failure from Finding 2. The settle path rereads the lease, sees it still PENDING, **releases both claims**, and returns a retryable error. Nothing remembers that a Success was authenticated, and the Registry operation is still `PhaseActive`. Once it is older than `callback_timeout` (10 min, checked every minute), `TimeoutChecker` claims it and **rejects the lease**. The backend's retry then arrives through the recovery path, finds the lease terminal, and settles quietly. The orphan sweep tears down the healthy workload.

Provisions that take close to 10 minutes are realistic for large AI images, and Finding 2 makes ack failures cluster under load, so the two compound.

**Minimal trace (Apalache):**

```
dispatch (backend received)
backend: Ready, queue Success callback
time: op exceeds callback_timeout
callback(Success) claimed
callback(Success): ack failed, claims released, retryable error
timeout: rejected lease, op untracked          ✗ rejected after authenticated success
```

**Fix (small):** record on the Registry operation that its Success was authenticated, and have the timeout path leave such an operation to the callback's retries. The reconciler already defers PENDING+Ready while the operation is in flight. If the chain stays unreachable, the lease still ends through the chain's own `pending_timeout`. An alternative is for `TimeoutCoordinator.Settle` to try `Acknowledge` instead of `Reject` for an operation with an authenticated Success. The `fixed` variant models the first option.

**Results:** (a) `current` fails, with the 6-step trace above found exhaustively. `fixed` holds for every execution up to 18 steps. (b) The quiescent-consistency property holds on `current` for every execution up to 14 steps and over 20k random 25-step traces. That's a meaningful positive result for the design: across crashes, stale inventory, lost dispatches, late callbacks and lease ends, the model never strands a workload, never bills without a Ready workload, and never leaves a PENDING lease stuck with a Ready workload.

**Model limits:** one backend, one operation; no ACTIVE re-provision, conflict quarantine, placement pruning, restore or maintenance.

## Finding 4: a restore whose destination gets rejected strands the tenant's data

**Found by:** the sibling-path audit (reading) first, then confirmed with `restore_retention.qnt`.

**The asymmetry.** Three places decide which chain states a restore *source* may be in:

| Place | Accepts |
|---|---|
| Restore admission (`placement/restore_coordinator.go` `ExecuteApplication`) | CLOSED, EXPIRED |
| Restore attempt recovery (`placement/operation_coordinator.go` `validateRestoreSource`) | CLOSED only |
| What the backend actually retains on close (`retain_on_close`, independent of chain state) | any terminal lease, REJECTED included |

**Consequence A (data stranded).** Suppose Fred rejects the *destination* D of a restore. That happens through the callback timeout (ENG-709), or after an ack failure (ENG-2129). If the backend still commits the restore, the orphan sweep deprovisions D. Closing a committed restore destination consumes the source finalizer and retains D's volumes **under D**. D is REJECTED, so admission refuses it as a source. The tenant's data, which was safely restorable under S before the restore, can now never be restored. It sits counted against retention quota until the age reaper destroys it (90 days by default).

**Consequence B (restore not recoverable).** A restore from an EXPIRED source passes admission. But if its dispatch is interrupted (provider restart, ambiguous call), attempt recovery refuses to redeliver it ("not a closed retained lease") on every pass. The destination waits for the chain's `pending_timeout`, and the restore silently fails.

**Minimal trace (Apalache, current):**

```
restore admitted (S active -> restoring)
Fred: callback timeout rejects D
backend: volumes adopted, destination Release committed
Fred: deprovision terminal D; backend retains D's volumes under D   ✗ retained under REJECTED D
```

**Fix (small):** one shared predicate for "a source that may hold retained data" (CLOSED, EXPIRED, REJECTED), used by both admission and recovery. The backend already decides whether restorable data exists. With that predicate, the model holds for every execution up to 15 steps. If excluding REJECTED was deliberate (a policy refusal), the alternative is to never reject a restore destination on timeout once the restore may have committed.

## Design check: does ENG-709's direction ("timeout never rejects") fix 2129 and 2130?

Both models now take the policy as a parameter (`TIMEOUT_REJECTS`), and the restore model also includes the reconciler's positive-failure path: planLease rejects PENDING + Failed, with no in-flight check.

| Variant | 2129 property (`lease_settlement`) | Settlement consistency | 2130 property (`restore_retention`) |
|---|---|---|---|
| Code as written | Violated | Holds | Violated |
| ENG-709 only (timeout never rejects) | **Holds** (14 steps) | **Holds** (14 steps) | **Violated**, 6 steps |
| ENG-2130 predicate only (accept REJECTED sources) | (n/a) | (n/a) | Holds (15 steps) |
| Both | Holds | Holds | Holds (15 steps) |

**Verdict.** ENG-709's direction removes ENG-2129 completely and opens no leak or stuck state in the settlement model. It does **not** fix ENG-2130: a restored workload that fails after commit, while D is still PENDING (for example because its ack failed), is rejected by the reconciler on a *positive* Failed fact. That lands the data in the same stranded place:

```
restore admitted -> commit -> restored workload fails -> reconciler rejects PENDING D (Failed)
-> deprovision D, retained under REJECTED D   ✗
```

So ENG-2130's shared predicate is needed regardless, and it also covers the EXPIRED-recovery half. ENG-709 (or the targeted ENG-2129 marker) is the separate fix for wrongful rejections.

## Finding 5 (manifest-ledger x/billing): every settlement drops the fractional second

**The model.** `billing_ledger.qnt` covers one tenant credit account and two leases with one provider: funding, create (reserve), acknowledge, reject/cancel/expire, Withdraw, CloseLease and auto-close on exhaustion. Block time is in milliseconds, because CometBFT block times carry sub-second precision.

| Property | `current` | `fixed` |
|---|---|---|
| Conservation (balance + provider = funded, no negative balance) | Holds | Holds |
| Reservations equal the sum over PENDING/ACTIVE leases | Holds | Holds |
| Pending/active counters match lease states | Holds | Holds |
| **Provider is paid for every whole second a lease ran** (unless the tenant ran out of credit) | **Violated** (7-step trace) | Holds |

**What happens.** `CalculateAccruedAmount` charges whole seconds (`int64(duration / time.Second)`). But every settlement path then sets `LastSettledAt = settleTime`, the full block time including its fractional second: `settleLease`, both Withdraw paths, CloseLease and AutoCloseLease. The fraction that wasn't charged is discarded each time. The provider is underpaid by up to one second of accrual on **every** settlement of **every** lease. Tenants keep those tokens, so the cost falls entirely on providers.

```
ack at t=0; Withdraw at t=2.5s -> pays 2s, LastSettledAt=2.5s
Withdraw at t=5.0s -> pays 2s (2.5s elapsed), LastSettledAt=5.0s
-> 5s of service, 4s paid   ✗
```

**Size.** About 0.5 s lost per settlement on average. At Fred's default `withdraw_interval: 1h` that's roughly 0.014% of revenue, small. It grows with withdrawal frequency: about 0.8% at once a minute, and about 9% at once per ~5.5 s block. Any provider that settles often pays it.

**Fix (small):** advance `LastSettledAt` by exactly the whole seconds charged, `LastSettledAt.Add(time.Duration(seconds) * time.Second)`, so the remainder carries into the next settlement. On close, losing the last fraction once is harmless. With the fix, the property holds for every execution up to 8 steps and over 20k random 25-step traces.

## Finding 6: a custom domain can be routed to two leases, or kept by its former owner

**The model.** `custom_domain_handoff.qnt` covers one domain *d* and two leases on the same docker-backend host (one Traefik, one public IP, one ACME store). Lease A starts out owning *d* and serving it. The chain can clear *d* (tenant, authority or `allowed_list`), close a lease (tenant, credit auto-close, authority) and let an ACTIVE lease claim *d* once it's free. On the Fred side:

- There is no handler for `lease_custom_domain_set`/`cleared`. Only the 5-minute reconcile sweep calls `ReconcileCustomDomain`, lease by lease, in no fixed order (ENG-94 tracks the missing events).
- `ReconcileCustomDomain` acts only on a **Ready** provision; otherwise it skips until a later sweep.
- A set passes the DNS gate as soon as *d* resolves to any address (deliberate, ENG-618). After a handoff *d* already points here, so the gate always passes. A clear is never gated.
- A failed custom-domain redeploy is compensated: the captured source containers are recreated with their exact labels (`snapshotCompensationContainer` clones `Config.Labels`), and "a verified source returns the lease to Ready". Custom-domain outcomes are `maintenance` failures, so they never count toward the failure budget.
- Router names are per lease, so two leases emitting `Host(d)` are two routers with the same rule and the same default priority. Which one Traefik picks is undefined (the same consequence ENG-609 describes for hash collisions).

| Property | `current` | strip on compensate only | exclusivity only | **both** | both + ownership proof |
|---|---|---|---|---|---|
| I1: *d* is routed to at most one lease | **Violated** (3 steps) | Violated | Holds | Holds | Holds |
| I2: after a clear redeploy finishes, the lease no longer routes *d* | **Violated** (3 steps) | Holds | Violated | Holds | Holds |
| I3: only the tenant that controls *d*'s DNS is served on *d* (ENG-62) | Violated (4 steps) | Violated | Violated | Violated | Holds |
| Witness: a legitimate handoff to B can still complete | yes | yes | yes | yes | n/a |

All results are from Apalache for every execution up to 10 steps, confirmed by random simulation.

**Traces.**

```
I1  A clears d on chain            routes {A}
    B claims d                     routes {A}      (A's clear waits for the next sweep)
    B is provisioned with d        routes {A, B}   ✗ two Host(d) routers

I2  A clears d on chain            routes {A}
    sweep starts A's clear redeploy
    the new container fails startup → captured source recreated, labels included
                                   routes {A}      ✗ A is Ready and serving d again

I3  A's lease closes (e.g. credit auto-close)     d is free on chain
    B claims d; A is deprovisioned; B provisions  routes {B}
                                   ✗ B serves A's domain; HTTP-01 is answered here,
                                     so B also gets a valid certificate
```

**How long.** In the honest case, the overlap lasts until the sweep reaches A and A's redeploy finishes: up to about 5 minutes, and another 5 each time A isn't Ready when the sweep comes by. With a failing redeploy it repeats every sweep. A tenant whose app fails on a fresh start but runs on the compensating start (a toggle file on its volume is enough) keeps *d* routed to itself for as long as it likes, alongside the new owner.

**Who's affected.** Cross-tenant only when *d* moves between tenants on the same provider: the authority or `allowed_list` reassigns it (which is the ENG-62 "unstick" remedy), or one account hands it to another. When a tenant moves *d* between its own leases (old version → new version), traffic splits between versions for the same window.

**Fix (both parts needed).**

1. **Exclusivity at emit time.** The docker-backend refuses to emit *d* (provision or custom-domain redeploy) while another provision on the backend emits it or has an admitted operation that will. The reconciler retries on the next sweep. This must be a reservation, not a check of live labels: with only the live-label check, two in-flight emits race and I1 fails again (mutant, 10 steps).
2. **A failed clear must not bring the route back.** Compensation strips custom-domain router labels that the lease no longer owns.

Exclusivity alone stops the split, but a former owner whose clear keeps failing holds *d*, and the new owner waits forever. Stripping alone fixes the stale route but not the ordering window. ENG-94 (event-driven set/clear) shortens the honest window, but it fixes neither the Ready gate nor compensation.

**I3 and ENG-62.** ENG-62 rests on the premise that a squatter "doesn't gain anything — HTTP-01 fails because they don't control DNS". That premise fails once *d* already points at a provider: after the owner's lease closes (credit auto-close included), anyone with an ACTIVE lease on the same provider can claim *d* and is served on it with a valid certificate. The 2026-09-08 review (ENG-897, R2) rated traffic misrouting as "conditional on provider behavior". Fred's code shows that this provider does route it. Nothing short of an ownership proof makes I3 hold.

## Finding 7 (manifest-ledger x/billing): reservations don't protect a lease when credit runs out

This confirms **ENG-858** (already filed; the contract decision is open). The model adds concrete traces and a candidate contract checked against them.

**The model.** `billing_fairness.qnt` covers one tenant credit balance and two leases at two different providers. Each provider settles on its own schedule (any party may settle any lease at any time), with lazy settlement and auto-close on touch. Time is in whole seconds, `min_lease_duration` = 3. To keep it small, accrual starts at creation.

| Property | `current` | shrinking reservation |
|---|---|---|
| Conservation | Holds | Holds |
| **A lease is paid for its first `min_lease_duration`** (the README's promise) | **Violated** (9 steps) | Holds |
| **No provider serves a full `min_lease_duration` for nothing** | **Violated** (11 steps) | Holds |
| The balance covers every open lease's unpaid guaranteed window | Violated | Holds |

**Traces.**

```
1) balance 6; L1 and L2 created at t=0, each reserves 3   (fully collateralized)
   t=4  tenant closes L1        -> L1 paid 4 (1 s past its guarantee)
   t=4  L2 settles              -> L2 paid 2 for 4 s       ✗ guaranteed 3
        (the tenant's close order decides who is short)

2) t=0  balance 3; L2 created (reserves 3)
   t=2  tenant adds 3; L1 created (available 6 - 3 = 3 ✓)
   t=6  P2 withdraws L2: accrued 6 >= balance 6 -> takes all 6, auto-close
   t=6  P1 settles L1           -> 0 for 4 s of service    ✗ including its whole reserved window
```

**Why.** `CreateLease` admits against `balance − ReservedAmounts`. Settlement (`performSettlementCore`) then transfers `min(accrued, balance)` from the shared credit address, first come first served. So a reservation gates creation but protects nothing. With Fred's defaults (`withdraw_interval` 1h, `min_lease_duration` 1h), whichever provider's withdrawal lands first after the balance stops covering everyone is paid in full. The slower one can lose up to a full interval of that tenant's revenue. Fred's depletion estimator and cross-provider withdraw trigger turn this into a race.

**A contract that holds (ENG-858 option 1, "strict guarantee").**

- A lease's reservation is the unpaid part of its guaranteed window: `rate × (start + min_lease_duration − LastSettledAt)`, floored at 0. It shrinks as that window is paid and is released on close.
- A settlement never takes another open lease's reservation. A lease whose accrual can't be paid from the rest is exhausted and closes.
- The creation check keeps its form: `balance − reserved ≥ rate × min_lease_duration`.
- Solvency invariant: `balance ≥ Σ reservations`. It is maintained incrementally, with no per-block scan, and it is the executable invariant ENG-858 asks for.

With this contract, every property in the table holds for every execution up to 10 steps (Apalache) and over 30k random 20-step traces.

**Pitfall.** Protecting the *existing* constant reservation (`rate × min_lease_duration` until close) is not enough. An older lease keeps its full reservation after its window was already paid, so a newer lease is still shorted (a 10-step trace). The reservation has to shrink.

**Not covered:** multiple denoms, a gap between creation and acknowledgement (the window should start at ack, while the reservation is held from creation), how past-window shortfall is split (still first come first served; pro-rata is a separate policy choice), and ENG-860's query-side allocation.

## Sibling-path audit (ENG-2126 pattern)

I listed every condition that names some but not all of a sibling set: the starting statuses (Provisioning/Restarting/Updating), terminal chain states (CLOSED/REJECTED/EXPIRED), and operation kinds (provision/restore). Then I checked each one in context. Almost all are deliberate and documented: maintenance-only branches, provision-only startup-failure findings, and failure-budget counting. The restore source states (Finding 4) are the real one. ENG-2126 itself showed up again as expected.

## Maintenance commands (restart/update): held up

`maintenance_commands.qnt` covers one lease and two commands in sequence across the provider journal (one pending head, phases, payload store) and the backend (exact-replay admission, one maintenance at a time, durable completion FIFO). It includes lost requests and responses, provider crashes (including between the payload write and the receipt), completions that arrive before the HTTP response, and the lease ending mid-command.

| Property | Result |
|---|---|
| The payload store only ever holds an update the backend actually applied | Holds |
| Every terminal receipt agrees with what the backend did (Accepted update ⇒ backend succeeded, ExecutionFailed ⇒ backend failed, InvalidState ⇒ backend never admitted it) | Holds |
| No lost update: once nothing can act, every applied update is in the payload store | Holds |
| No immortal pending command | Holds |

Every property holds for every execution up to 12 steps (Apalache) and over 20k random 30-step traces. As a sanity check, two deliberately broken variants each fail at once: settling an update on HTTP acceptance, and not cancelling when the lease ends. So the properties do catch real mistakes.

**Not modeled:** the backend's internal maintenance journal, release append and recovery classification (folded into "the outcome becomes durable"), receipt-window eviction and admission-stamp expiry, and custom-domain redeploys.

## Also reviewed without a model

- **Signer pool / broadcast** (`internal/chain`): sound. Two optional speed-ups: retry immediately after a sequence mismatch whose expected sequence was parsed (today it waits 6s), and let `waitForTx` stop early once the account sequence has passed a tx that was never found.
- **Failure budget (ENG-799):** the close path is fenced against a concurrent tenant restart/update by the Registry's mutation revision. Policy question: health-check-never-passes, image-pull failures and missed deaths never count, so an ACTIVE lease that can never start is re-provisioned every 5 min and billed until its credit runs out.

## Nits found while reading

- `lease_sm.go`: the comment on `Configure(Restarting)` says restore "enters Restarting from Provisioning". The code only permits `evRestoreRequested` from `Reserved`.
- docker-backend README, config table: the DNS gate is described as confirming the custom domain resolves "to this host" (and the quorum as seeing it "at this host"). The code deliberately checks only that it resolves to some address (ENG-618).
- `evContainersRemoved` is declared and has a `String()` case, but it's never configured on any state and never fired. It's dead code.

## Not covered (yet)

- Multi-backend placement (conflict quarantine, routing, retirement), ACTIVE re-provision and the failure budget, deferred close, restore, and maintenance recovery.
- Tying the specs to the code: the next step would be exporting ITF traces (`--out-itf`) and replaying them against the Go code with fakes, model-based-testing style.
