# Quint models

These models preserve the November 2026 formal-modeling findings and subsequent
regressions. They are executable specifications of selected behaviors, not a Go
test harness. A passing simulation does not establish that the implementation
refines the model or prove correctness for every execution.

## Run

Use Node.js 22 or later, npm, and Python 3.10 or later. Quint is pinned to 0.33.0
with its dependency lockfile; the runner rejects other versions.

```sh
npm ci --prefix models/quint --ignore-scripts
make test-models

# Deeper run, or one model (includes its positive and negative controls):
python3 models/quint/run.py --samples 20000
python3 models/quint/run.py --model ack_lane_flush --samples 20000
python3 models/quint/run.py --model ack_retry_budget --samples 20000
```

The default suite typechecks each selected source and samples 2,000 traces per
case, at most 30 steps each, using seed 1517 and Quint's TypeScript backend.
`cases.json` records the invariant, entry module, step action, and expected
outcome for every case. A negative control passes only when Quint explicitly
reports an invariant violation; a syntax error, crash, or timeout fails the suite.
The settlement witness wrappers select a known failing schedule because ordinary
random sampling can miss that race at this budget.

Generated logs, ITF traces, commands, source hashes, and a JSON result summary go
to ignored `results/`. Use `--out /path`, `--seed`, `--steps`, `--samples`, or
`--jobs` to change a run. A trace count is a sampling limit, not a proof bound.
Apalache bounded checking requires a separate installation; this runner does not
execute it. Historical Apalache claims in FINDINGS.md have not been independently
reproduced by this suite.

## Exhaustive check

Sampling can miss a violation. A case marked `"exhaustive": true` in
`cases.json` is also checked in every reachable state:

```sh
make test-models-exhaustive
python3 models/quint/verify.py --model deletion_settlement
```

This runs `quint verify --backend=tlc`, one case at a time. It needs a Java
runtime on `PATH` (Java 21 was used), and on its first run Quint downloads the
checkers into `~/.quint`. `make test-models` needs neither and does not run it.

A case that should hold passes only when TLC reports a completed search with no
error and, where the case records `distinct_states`, exactly that many distinct
states. A changed count means the model's state space changed: check the change,
then update the number. A negative control passes only on a reported violation.
Mark a case only if its model is finite: TLC enumerates every state and cannot
pick from an unbounded set.

Each case is checked in a copy of its model that keeps only that case's variant.
With every variant in one file, the compiled form of `deletion_settlement.qnt`
is just above the 20 MB input limit of the Apalache release Quint uses to
produce TLA+, and the command fails with `error: error`. A model that imports
another file is checked whole. Copies, logs and a JSON summary go to ignored
`results/exhaustive/`.

A completed search covers the model as bounded in its source (for example two
leases), not larger instances, and says nothing about the implementation.

## Model map and implementation status

`current` means the historical baseline modeled in the bundle, **not today's
main branch**. `fixed` means the proposed model variant; it does not imply that
its corresponding product change has shipped.

| Model | Finding / scope | Implementation status |
| --- | --- | --- |
| `lease_death_routing.qnt` | ENG-2126: death races replacement Ready | Fixed in PR #295 |
| `ack_lane_flush.qnt` | ENG-2127: one non-PENDING member poisons a batch | Filtered retry implemented alongside this suite |
| `ack_retry_budget.qnt` | ENG-2127: deadlines, exact evidence, bounded retries | Follow-up model of the implemented retry |
| `lease_settlement.qnt` | ENG-2129: timeout after authenticated Success | Fixed in PR #296; witness wrappers exercise the original race |
| [`inline_callback_completion.qnt`](../../internal/provisioner/operation/testdata/inline_callback_completion.qnt) | ENG-2129: synchronous completion after an inline callback | Fixed in PR #296; remains beside operation regression tests, included by this runner |
| `restore_retention.qnt` | ENG-2130: terminal destination strands retained data | Shared terminal-source eligibility implemented alongside the recovery model |
| `restore_source_recovery.qnt` | ENG-2130: admitted EXPIRED/REJECTED sources must remain recoverable after restart | Shared eligibility used by admission and recovery |
| `close_recovery_routing.qnt` | ENG-2154: recovery resumes a close that a retiring actor never ran | Typed close answer and re-admitting `routeClose` implemented alongside the model |
| `cohort_divergence_close.qnt` | ENG-2155: a close admitted before recovery's cohort-divergence report aborts the pass | Typed cohort-divergence answer implemented alongside the model |
| `billing_ledger.qnt` | ENG-2131: fractional seconds lost at withdrawal | Proposed manifest-ledger change; requires a chain upgrade |
| `custom_domain_handoff.qnt` | ENG-2132: overlapping and restored stale routes | Deferred architectural change; partial-fix mutants are included |
| `billing_fairness.qnt` | ENG-858: reservation contract | Candidate contract, awaiting a product decision |
| `maintenance_commands.qnt` | Maintenance payload, receipt, and recovery consistency | Baseline audit invariants; no new fix claimed |
| `deletion_settlement.qnt` | Deletion by rename: when a caller or a listing reader may settle, with crashes, failed and independent publications, and a recycled project ID | Proposed plan, revision 6; not implemented |
| `deletion_quota.qnt` | Deletion by rename: tombstone size, limit and tombstone lifetime against a leftover writer; project-ID allocation with a kernel check | Proposed plan, revision 6; not implemented |

ENG-2128 was a stale comment and unused event cleanup, included in PR #295; it
has no separate model.

## Deletion by rename: correspondence and limits

`deletion_settlement.qnt` and `deletion_quota.qnt` model a proposed design, not
code. Their `plan` variant is revision 6 of the deletion-by-rename plan; every
other variant turns off one rule of that plan. Read from the fred `main` of
2026-11-27 (`b1f8a39d8`): the reservation is durable, settlement readers read
the volume listing without the lease's lock, a publication snapshots first and
acknowledges after the pool update, the pool serves only after the first
successful full refresh of a process, and a destroy that runs before that
refresh (startup's own reap) answers without publishing.

Two variants are expected to hold although they drop a rule. `restartCounted`
treats tombstones found at startup as already counted; it holds because the
pool does not serve before the first publication. `tombstoneRuleOnly` drops the
allocator's kernel check; it holds because a tombstone stays until usage is
zero. Both rules are a second layer in these models, not the only guard.

`unlockedStartupPublish` keeps the counterexample behind one rule the plan had
not stated: a destroy's publication takes the lock of every refresh, also
before the first full refresh. `limitOnlyBound` starts the volume above its
limit, where a size equal to the limit alone is too small.

`ackByProjectID` needs the `recycledKeyWitness` schedule: random sampling at
the default budget does not find it. `neverBothDone` and `neverReclaimed` are
reachability controls: a violation shows that the good end state can be reached.

Both models are small enough to check completely, and all their cases but two
are marked exhaustive: the `plan` variants have 19,076 distinct states
(`deletion_settlement.qnt`) and 2,310 (`deletion_quota.qnt`), and hold their
safety invariants in all of them. The two `plan` cases under
`recycledKeyWitness` are left to sampling; the full `step` covers them.

Neither model covers the cleanup failure classes, executor fairness, storage
identity checks, a new volume created under a name whose old tombstone still
exists, block rounding, or a crash between two steps of one cleanup.

## ENG-2127 correspondence and limits

The original `ack_lane_flush.qnt` models failed simulation and queries as costing
zero blocks. Its `noSpuriousAckError` invariant holds only under that abstraction;
real RPC delay, failed transaction inclusion, or repeated cancellations can
exhaust the flush deadline even after this fix. The model also uses sets of lease
IDs, so duplicate caller delivery is verified in Go tests instead.

The implementation retries only the typed `ErrLeaseNotPending` race, after exact
lease/provider observations. `observedAckBatch` binds the remaining PENDING
requests to their observing lane. ACTIVE observations settle without claiming a
transaction hash; terminal, missing, foreign, and failed observations return
errors to their callers. Every batch retry removes at least one member. A stale
reader that makes no progress uses the existing individual fallback, and fee/gas
errors are surfaced without individualizing.

`ack_retry_budget.qnt` adds finite time, state changes between observation and
broadcast, stale reads, fee/gas failures, and unrelated RPC failures. It checks
that retries require observed evidence, batch retries are bounded by the initial
membership, successes have ACTIVE evidence, and no write starts after the budget
is exhausted. The `missingDeadlineCheck` mutant must violate the deadline
invariant. Its observation step is atomic; Go tests cover a deadline expiring
partway through sequential exact reads. These models do not prove network
liveness, model chain reorganization, or replace the actual acknowledgment tests
in `internal/provisioner/ack_batch_retry_test.go`.

## ENG-2130 correspondence and limits

`restore_retention.qnt` preserves the original stranded-data counterexample.
`restore_source_recovery.qnt` adds admission followed by interrupted dispatch and
restart recovery, which the original model did not cover. The historical and
admission-only variants must violate recovery parity. The shared-policy variant
preserves parity and refuses dispatch after source authority is lost. Negative
controls also detect an ownership-check bypass and an added retention gate that
would block exact retries after the same operation consumed the source.

`Redelivered` means Fred authorized a retry with the same durable operation ID,
not backend acceptance or successful restoration. Recovery does not query retained
data and deliberately permits an absent source placement. The model distinguishes
retained data, data consumed by the same operation, and reaped data; the backend's
idempotent replay and missing-data responses are outside this model. Its ownership
flag abstracts exact UUID, tenant, provider, and compatible source placement/backend
checks. Go regressions separately exercise identity mismatches; the recovery-source
gate tests cover confirmed matching/foreign backends, pending source operations,
ambiguous placement, and absent placement. The model assumes the other recovery
gates (including the PENDING target and exact durable generation) are satisfied.

The source-state observation is constant in this model. It tests admission/recovery
policy parity, but cannot detect a missing terminal-state recheck after a lagging or
reset RPC node reports a different state. The terminal half of the dispatch invariant
therefore follows from admission; Go recovery regressions separately exercise changed
nonterminal and unknown observations. In `fixed`, the premise of
`consumedSourceCanRetry` is unreachable because eligible consumed sources are
redelivered, never preserved. The `retentionGatedRetry` negative control makes that
premise reachable and demonstrates that the property detects an erroneous gate.

The fleet regression runs Fred's actual timeout, orphan cleanup, inventory, restore
application service, and Fred-to-backend HMAC HTTP paths. It covers provider timeout
rejection and the chain outcome of tenant cancellation; it does not execute the
ledger's `CancelLease` transaction or the tenant API handler. Its backend fixture
represents the committed-destination retention outcome and does not execute Docker
volume operations. Docker's
`TestDeprovision_ReadyRestoreDestinationFinalizesSourceBeforeClose` separately checks
the source-finalizer handoff and destination retained-volume rename.

## ENG-2154 correspondence and limits

`close_recovery_routing.qnt` models one lease whose close intent docker-backend's
periodic recovery pass resumes. Either a live close is still tearing down (the
reported race), or an earlier close failed and waits for recovery. The lease
actor answers a close it handled with that close's outcome, or says it handled
nothing because it was retiring. `current` reports the second answer as a failed
close and must violate `noSpuriousCloseError`. `fixed` admits the close again
under the command fence, re-reading the intent, and routes it only while it is
still pending. Two mutants show that the re-admission is what makes it safe:
`rerouteUnread` routes again without re-reading and tears down a later admission
of the same lease (`noStaleTeardown`), and `dropNotRun` treats the answer as done
while the intent is still pending (`noDroppedPendingClose`).

The model has one live close and one recovery worker. It abstracts a later
admission of the lease as a single environment action allowed only once the close
has settled and the fence is free. It does not model the blocking wait itself:
recovery's close still waits behind a live close's teardown. Go regressions in
`internal/backend/docker/close_recovery_routing_test.go` run the real recovery
pass and both recovery entry points; `internal/testutil/close_routing_guard_test.go`
pins close-command minting to `routeClose`.

## ENG-2155 correspondence and limits

`cohort_divergence_close.qnt` models one Ready lease whose cohort diverged.
Recovery publishes it and later routes the divergence report, while a close may
be admitted, tear down, and succeed or fail. Routing delivers only while the
projection reads Ready, which it still does while a close is admitted, so the
report can queue behind the close. `current` reports the superseded answers as
errors and must violate `noAbortOnSupersededReport`. `fixed` skips a superseded
report and holds that invariant, `handledFailureAborts` and
`noDivergenceUnderClose`. Two mutants check the other invariants: `skipAllErrors`
hides a handled failure, and `applyDespiteClose` applies the report although a
failed close still owns the lease.

The model has one lease and one report, and abstracts the handler's failure as a
nondeterministic outcome when no close owns the actor. It does not model a stale
generation separately; routing and the actor treat it as superseded, like a
projection that left Ready. The Go regression
`TestRecoverState_CloseAdmittedBeforeCohortDivergenceDoesNotAbortThePass` holds
a real close in admission while the real recovery pass routes the report.

## Provenance

The eight original `.qnt` files and `ack_current_cex.itf.json` are copied verbatim
from `fred-quint.zip`; `SOURCE.sha256` identifies that archive. They modeled Fred
at `a802bca` and manifest-ledger billing at `43e2e1a`. [FINDINGS.md](FINDINGS.md)
preserves the original report with a provenance note. The settlement witness
wrappers, inline-callback model, budget and restore-recovery models, and runner were added during review
and remediation. No private session transcript or agent execution prompt is
required to run the suite.
