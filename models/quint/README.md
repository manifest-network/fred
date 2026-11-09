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
| `billing_ledger.qnt` | ENG-2131: fractional seconds lost at withdrawal | Proposed manifest-ledger change; requires a chain upgrade |
| `custom_domain_handoff.qnt` | ENG-2132: overlapping and restored stale routes | Deferred architectural change; partial-fix mutants are included |
| `billing_fairness.qnt` | ENG-858: reservation contract | Candidate contract, awaiting a product decision |
| `maintenance_commands.qnt` | Maintenance payload, receipt, and recovery consistency | Baseline audit invariants; no new fix claimed |

ENG-2128 was a stale comment and unused event cleanup, included in PR #295; it
has no separate model.

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

## Provenance

The eight original `.qnt` files and `ack_current_cex.itf.json` are copied verbatim
from `fred-quint.zip`; `SOURCE.sha256` identifies that archive. They modeled Fred
at `a802bca` and manifest-ledger billing at `43e2e1a`. [FINDINGS.md](FINDINGS.md)
preserves the original report with a provenance note. The settlement witness
wrappers, inline-callback model, budget and restore-recovery models, and runner were added during review
and remediation. No private session transcript or agent execution prompt is
required to run the suite.
