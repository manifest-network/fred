# Operations Runbook

This document covers day-to-day operation of a Fred deployment: health checks, alert interpretation, common failure modes, recovery procedures, and tuning.

For deployment and initial setup see [DEPLOYMENT.md](DEPLOYMENT.md). For metric definitions see [ARCHITECTURE.md](ARCHITECTURE.md#metrics-prometheus). Sample Grafana dashboards live in the `manifest-deploy` repository.

---

## Health checks

Both `providerd` and `docker-backend` expose `GET /health`. **`providerd /health` is a liveness contract: no dependency verdict makes it 503, and no dependency can make it slow enough for a prober to give up either.** Point load-balancer health checks at it. `providerd` additionally exposes `GET /readyz` — the same body, but the verdict reaches the status code. **Do not point a load balancer at `/readyz`.**

| Endpoint | Probes | 503 when |
|---|---|---|
| `providerd /health` | Chain gRPC, every backend, mandatory placement-store readability and placement-inventory bootstrap, plus token-tracker and payload-store DB readability when those optional stores are configured. The whole sweep is bounded (3s) and backends are probed concurrently, so no dependency can make this endpoint slow enough for a prober to give up on it | Never |
| `providerd /readyz` | Same probes, same budget | A configured bbolt store is unreadable, or no durable inventory baseline matches the configured backend topology (verdict `unhealthy`) |
| `docker-backend /health` | Docker daemon reachable, plus the callback, diagnostics, release, and retention bbolt stores open and carry their expected buckets. Callback health validates delivery rows, durable operation and maintenance intents, non-expiring close intents, and that no lease simultaneously owns incompatible intent classes | Any of them is unhealthy |

All bbolt probes are read-only opens: they prove the database is present and structurally intact, **not** that a write would succeed. A full or read-only filesystem passes them.

Both `providerd` endpoints return the same JSON body, whose `status` is one of three verdicts:

| `status` | Meaning | `/health` | `/readyz` |
|---|---|---|---|
| `healthy` | Every configured probe passed | 200 | 200 |
| `degraded` | A **remote, shared** dependency is impaired — the chain, or one or more backends. Existing workloads keep serving. After inventory bootstrap, exact callbacks and safely evidenced reconciliation continue, and the reconciler may place genuinely new recordless `PENDING` work only on nodes that answered both inventories. Work pinned to a silent owner, recordless `ACTIVE` work, attempts, and conflicts remain deferred. A chain outage halts reconciliation entirely and fails every lease-resolving tenant call. Either way providerd still accepts backend callbacks, which is why it must stay in rotation | 200 | 200 |
| `unhealthy` | A **local, process-owned** bbolt store is unreadable, or no durable inventory baseline matches the configured backend topology. Restart a broken store; let an incomplete bootstrap inventory retry | 200 | 503 |

A check absent from `checks` means an optional dependency is **not configured**,
not that it passed. Every `providerd`, including development instances, reports
the mandatory `placement_store` and `placement_inventory` checks. The
`token_tracker` and `payload_store` checks may be absent when their optional
stores are not configured.

`checks.placement_inventory` is a durable, topology-bound bootstrap latch. A
complete `/provisions` plus `/retentions` projection establishes it, and the
matching provider-bound placement database restores it across process restarts.
Transient incomplete sweeps do not revoke it while topology is unchanged;
changing membership requires the complete proposed fleet and invalidates the
baseline until a new complete projection commits.

`fred_reconciler_sweep_complete` is 0 while a sweep is in progress or after an
incomplete/error sweep, and becomes 1 only after the most recently completed
full-fleet inventory was durably projected. A 0 narrows the reconciler's
recordless `PENDING` admission to a typed scope of nodes that answered both
inventories; it does not globally revoke the durable baseline. Owner-affine work
stays pinned, and recordless `ACTIVE` work plus unresolved attempts/conflicts
remain deferred.

Tenant event dispatch has no per-sweep witness. It requires the durable baseline,
live-routes by backend stats inside the configured topology, and durably records
the exact attempted backend before dispatch. A later ambiguous result stays
pinned even if inventory omits the lease.

### Why `/health` never 503s

It used to, on any failing probe, and that is the direct cause of two outages. `providerd` sits behind a single-server load-balancer pool with no fail-open floor, so an unhealthy verdict sheds no load onto a peer — there is no peer — it removes the provider entirely. And the backends' completion callbacks arrive on that same listener and the same vhost as the tenant API, so a backend-triggered 503 severs the channel a *recovering* backend needs to report what it finished.

- **mainnet-morpheus, 2026-07-13** (ENG-522): a ~1-minute upstream chain-RPC blip failed a single `Ping`. `/health` flipped 200→503 for one 30s probe interval, the load balancer dropped the only server, and a backend's provision-success callback got `no available server` three times in 6s and was dropped. The workload ran; providerd never learned; the lease was rejected 10 minutes later with `reason="callback timeout"`.
- **dev, 2026-08-17**: one docker backend of three was stopped. The tenant API returned 503 for 15 minutes (38 × 503 vs 9 × 200), four tenant provisions failed, and one workload was orphaned — while providerd itself was serving fine when reached directly.

The dependency signal did not disappear, it moved: the per-check map is still in the body, and every probe now has a metric (`fred_health_check_healthy` and `fred_backend_healthy`). **Alert on those, not on the status code.** A broken bbolt store is fixed by restarting the process, which a supervisor can do and a load balancer cannot — that is why even `unhealthy` keeps `/health` at 200.

**Slowness is bounded too, and that is a separate guarantee from the verdict.** A backend that *refuses* connections fails in milliseconds; one that accepts the connection and never answers would otherwise burn its full client timeout (30s by default), and three independent deadlines each turn that into the same outage: Traefik gives up at 5s and marks the server DOWN, `http_write_timeout` severs the response at 15s, and the request-timeout middleware answers 503 at 30s from `http.TimeoutHandler` — with no verdict involved at all. So the whole sweep is capped at 3s, under the smallest of the three, and backends are probed concurrently rather than serially so that cap does not starve the backends probed last. If you shorten Traefik's `healthCheck.timeout` below 3s, shorten the budget with it.

---

## Common alerts and what they mean

| Signal | Likely cause | First step |
|---|---|---|
| `fred_backend_circuit_breaker_state{backend="X"} == 2` (open) | Backend X has been unhealthy long enough to trip the breaker | `curl backendX/health`, check backend logs |
| `fred_backend_healthy{backend="X"} == 0` for >1 min | Backend health probe failing | Same as above. Note this no longer affects the tenant API's availability — the provider reports `degraded` and keeps serving |
| Backend X reports `callback store unhealthy` | `callbacks.db` is missing a delivery/intent bucket, contains malformed durable evidence, or gives one lease simultaneous operation, maintenance, or close authority. Current deliveries live below a lease-identifying nested bucket. Operation identity/snapshot fields are immutable; maintenance advances through typed pre-append and append-started phases and then binds one exact target fence; close preserves its immutable cleanup snapshot while durably advancing `cleanup_attempts`. Every change uses an exact digest-bearing claim. Replay/TTL never silently deletes poison data, and causal intents, close intents, and exact completions never age out | Stop that backend, take a copy of `callbacks.db` with the matching release store, storage markers, containers, and volumes. Inspect or restore the named lease offline (or the complete file when a root bucket is missing). Prefer exact repair/restore over deleting the database; wholesale deletion can lose accepted work, replacement identity, destructive-cleanup authority, and pending completions. Keep the node out of new placement until `/health` is clean |
| Backend latches after `post-mutation storage verification`, refuses startup with `recover interrupted operations`, or `fred_*_backend_callback_store_errors_total` increases | A raw mutation returned without a usable postcheck, callback persistence/store access failed on an instrumented path, operation-intent startup recovery failed, or another authoritative journal/substrate proof reached a terminal identity or outcome-unknown failure. The first cause is sticky for the backend lifetime: callback, release, and retention journals (where present), substrate mutation admission, and callback delivery all refuse through the same latch. A running docker-backend publishes that first cause to its main loop, closes the listener, drains workers, and exits status 1 so the supervisor must launch a fresh `Start`; a persistent fault therefore crash-loops closed instead of serving. A valid but semantically indeterminate maintenance row is different: it need not make `/health` fail or increment this counter; use the Docker reconciliation signal below. A close intent already owns destruction, so recovery resumes it from its immutable snapshot before ordinary exact-cohort validation and reports retry errors in the lease-scoped close log below | Fence mutation ingress and preserve `callbacks.db`, `releases.db`, `retention.db` where present, the storage-identity marker pair, and the substrate as one evidence set. Do not treat one still-readable sibling journal or a queued callback as permission to continue; the shared latch intentionally withdrew the entire lineage. Let the supervised restart retry only after repairing the Docker/retention/SKU/store inconsistency or restoring the matching stopped-process snapshot. Restart only against that same set. Never delete an intent, finalizer, release fence, retained data, or callback evidence merely to make readiness green |
| `fred_docker_backend_oldest_close_intent_age_seconds` remains above the normal close window, `fred_docker_backend_pending_close_intents` remains non-zero, or `durable close recovery remains pending` repeats for one lease | Docker admitted deprovision before teardown, then a transient container/volume/release/accounting/outbox failure prevented finalization. The aggregate gauges deliberately omit lease labels; the log's lease UUID and durable `cleanup_attempts` identify the row and survive restart. A full close keeps a conservative projection and capacity reservation; a cleanup-only close may have no tenant-visible projection but remains the sole non-expiring retry owner | Correlate the recovery log's lease UUID with nearby teardown, retention, release-store, and callback-store errors. Restore the failed dependency and let the next docker-backend recovery tick retry. If offline inspection is required, stop the backend and inspect the copied `pending_callback_close_intents` row together with the exact `releases.db` history and substrate; callback URLs contain causal identifiers, so do not paste raw row contents into tickets. Never delete the row merely because Docker reports zero containers |
| `increase(fred_docker_backend_reconciliation_total{outcome="error"}[15m]) > 0` or `fred_docker_backend_reconciliation_last_success_timestamp_seconds` is stale beyond the expected Docker `reconcile_interval` | Docker's periodic `recoverState` pass failed closed. A pending maintenance WAL with indeterminate readiness, divergent authority, exact-cleanup ambiguity, or a busy callback FIFO is one important cause. Structurally valid semantic recovery evidence can leave backend `/health` green and `fred_docker_backend_callback_store_errors_total` unchanged. During cold start the equivalent failure exits before the periodic metrics loop starts, with `failed to recover state` and a nested `recover maintenance for lease` error | Inspect the backend's `reconciliation failed` log and its wrapped lease/error. A busy FIFO or still-starting workload should converge on a later backend tick. For a persistent mismatch, stop the backend and preserve the exact `callbacks.db`, `releases.db`, marker pair, Docker metadata, and volumes before following [A pending or corrupt Docker maintenance intent](#a-pending-or-corrupt-docker-maintenance-intent). Do not delete the WAL or use `/health` success as permission to bypass it |
| `increase(fred_docker_backend_restore_finalizer_pending_total[15m]) > 0` | A restore reached Ready, but the backend could not durably commit or verify its exact active Release, so it deliberately did not delete the source `restoring` finalizer. The source remains non-restorable and all new Provision/Restore/Restart/Update work for the destination is fenced. Reconcile retries do not increment this counter again | Correlate the WARN by destination and original lease UUID, repair the release-store dependency, and let the periodic retention sweep retry. Confirm the source row and destination Release together; do not delete the source row, because it is protecting adopted data and exact destination accounting |
| `fred_health_check_healthy{check="chain"} == 0` | providerd cannot reach the chain gRPC endpoint (or it answered slower than the health probe's budget). Every tenant endpoint that resolves a lease fails, **and reconciliation stops entirely** — a sweep reads the complete paginated `PENDING` and `ACTIVE` inventories concurrently with independent 30s contexts, then returns an error if either failed because everything downstream treats "absent from chain" as ground truth. The bound also prevents startup reconciliation from indefinitely delaying the subscriber and schedulers. Callback HTTP ingress remains reachable, but exact application that needs the chain returns 503; the originating backend keeps that lease's FIFO head durable and periodically retries without blocking other leases | Check the node and `grpc_endpoint`. This is the ENG-522 trigger, and it is now a metric rather than a liveness 503 — providerd deliberately stays in rotation, because dropping out would sever even the retryable callback path without restoring anything |
| `fred_health_check_healthy{check=~"token_tracker\|payload_store"} == 0` | That bbolt store could not be opened, or is missing the buckets it should have. The payload check additionally fails permanently for that process when its retained parent/path/inode, exact `0600` mode, or single-link proof drifts. `/readyz` is 503 and `/health` reports `unhealthy` while still answering 200 | Check the store's `*_db_path` — that it exists, is the intended unsymlinked file, and is readable. For payload authority drift, fence payload-dependent mutation, preserve the file and any unexpected alias, stop `providerd`, restore exact `0600` mode and one link from understood evidence, then restart. A load balancer cannot fix this. The token probe is read-only, so a full or read-only filesystem can pass it and still break writes: check disk space too even when this gauge reads 1 |
| `fred_health_check_healthy{check="placement_store"} == 0`, or log `placement runtime authority withdrawn` | The provider can no longer prove that the configured placement pathname names the exact single-link regular-file inode it opened with mode `0600`, or a bbolt `Commit` returned an outcome-unknown error. The failure is process-sticky: all later placement authority reads and writes fail closed even if the pathname or permissions are restored | Fence tenant and chain-event mutation ingress immediately. If the pathname or file metadata changed, **do not restart or overwrite either file before preserving both the still-open inode and the current pathname/aliases**; the running process may hold the best surviving copy. Follow [Placement runtime authority was withdrawn](#placement-runtime-authority-was-withdrawn), then stop, classify the preserved authority offline, and restart only with the exact private provider-bound file chosen from that evidence |
| `fred_health_check_healthy{check="placement_inventory"} == 0` | The placement database has no complete inventory baseline for the configured backend identities. `/readyz` is 503, but `/health` and the callback route remain available | Restore every configured backend's `/provisions` and `/retentions` responses and let reconciliation commit the bootstrap projection. Do not admit new tenant lifecycle traffic until the check becomes 1 |
| `increase(fred_placement_write_failures_total[5m]) > 0` | providerd could not durably record or verify a placement-store synchronization point. A write-ahead failure blocks a new provision, re-provision, or restore before the backend is contacted; a later confirm/cleanup failure preserves conservative authority rather than silently claiming no backend was contacted. An error before bbolt `Commit` is definitely uncommitted and may be retried; a `Commit` error is outcome-unknown and permanently withdraws this process's placement authority | Check the `failed to ... placement`, `placement runtime authority withdrawn`, or placement-sync verification log, then check free space, filesystem permissions and I/O errors at `placement_store_db_path`. If authority was **not** withdrawn, restore access and let reconciliation retry safe work. If it was withdrawn, do not rely on a retry or restart: preserve and classify the exact file as described below. Page on every increase — do not alert on the raw counter being non-zero, because counters remain non-zero after recovery |
| `fred_backend_health_probe_panics_total > 0` | Bug — a backend health probe panicked. The probe is an HTTP call that should return an error, not panic. It is recovered (the probe runs on its own goroutine, where net/http's recovery does not reach it) and counts as unhealthy, so nothing crashes | Check logs for `backend health probe panicked` and the stack trace, then file an issue |
| `fred_health_check_healthy` series absent, or frozen while nothing changes | Nothing is polling `/health`. Both this gauge and `fred_backend_healthy` are written **only** from inside the health handler, so with no prober they latch at their last value instead of going absent | Confirm the load balancer's health check on `/health` still exists and its interval (30s in the reference deployment). A latched 1 masks a real outage |
| `fred_backend_insufficient_resources_total{backend="X",verdict="coded_refusal"}` rising | Backend X is returning contract-conforming capacity refusals; the matching attempt is normally clearable | Reduce SKU sizes, add backend hosts, or check `docker-backend /stats` |
| `fred_backend_insufficient_resources_total{backend="X",verdict="ambiguous"}` rising | Backend X or an intermediary is returning legacy/code-less/unknown-code capacity 503s; Fred retains the write-ahead attempt | Fix the responder to emit the declared coded envelope, then settle the retained attempt from its exact callback or an upgraded inventory report carrying the same paired typed generation; otherwise perform explicit operator repair. Malformed envelopes appear in `fred_backend_malformed_error_body_total` instead |
| `fred_backend_malformed_error_body_total` rising on a backend | That backend answers 4xx with a body that is not the declared `{"error": ...}` envelope, so its tenants get a generic message instead of a diagnostic | Find the raw body in the `backend returned a malformed error body` log line and fix the backend to emit the envelope (BACKEND_GUIDE.md). If the backend looks correct, suspect an intermediary answering on its behalf |
| `fred_provisioner_callback_timeouts_total` rising | Backend accepted provision but never called back | Backend logs; verify `callback_base_url` is reachable from backend; check HMAC secret match |
| `increase(fred_provisioner_callback_settlement_claim_wait_timeouts_total[5m]) > 0` | A callback waited 30 seconds while another callback or the timeout checker retained the same operation ID's terminal-settlement claim. The actor may be blocked on a slow chain call, or a bug may have leaked its claim | Find the `callback settlement claim is contended` and timeout logs for the lease/operation ID; correlate concurrent callback, timeout, acknowledge/reject, and downstream chain-latency logs. A deprovision-owned claim returns immediately and cannot increment this counter. If no actor completes and the counter repeats, restart providerd to clear the process-local claim, then file an issue with the logs |
| `increase(fred_provisioner_callback_placement_semantic_conflicts_total[5m]) > 0` | A backend reported successful provisioning, but its authenticated callback contradicted the durable backend, attempt, or conflict record. Fred preserved that record for repair and continued toward chain acknowledgement | Find `failed to confirm placement from authenticated success callback; continuing chain acknowledgement` with `permanent_semantic_verdict=true`, then reconcile the logged `lease_uuid`, `backend`, `operation_id`, and `error` against backend inventory and the placement store before changing or deleting the record. Page on every increase. A later acknowledgement failure can retry and increment this attempt counter again, so correlate by lease and operation ID rather than treating the value as unique leases |
| `increase(fred_provisioner_callback_deprovision_owned_success_total[5m]) > 0` | A backend completed provisioning while close/deprovision owned the same operation ID. Fred consumed the success without acknowledging the closing lease and continued teardown | Correlate `ignoring success callback emitted while deprovision owns the operation` with close/deprovision logs for the same lease, backend, and operation ID. A one-off race is safe; sustained increases suggest slow provisioning or unusually fast lease closure |
| `fred_provisioner_lifecycle_callback_outcomes_total` | Every authenticated lifecycle callback receives exactly one terminal `outcome`: `applied`, `dropped`, or `retryable`. `verdict` is bounded to `authorized`, `legacy`, `teardown_only`, `retired`, `invalid`, `missing`, `stale`, `unusable`, `unavailable`, or defensive `unknown`; `status` is exactly one of the closed callback protocol values `success`, `failed`, or `deprovisioned` (anything else is rejected with 400 before application). Summing across `outcome` is the lifecycle-specific received count. The older `fred_api_non_in_flight_callbacks_total` deliberately remains a received-at-ingress compatibility counter and increments even for a later drop | `verdict="legacy"` is expected for v0.13 placements during one-upgrade adoption. `teardown_only` means its matching confirmed placement authority is gone: runtime observations are dropped and only the exact terminal deprovision observation can consume the residual authority. Occasional `outcome="dropped",verdict=~"stale|retired"` is expected after a lost 2xx or lifecycle rotation. Sustained `missing`/`unusable` drops mean the backend is presenting an authority Fred cannot use; correlate the authorization log with placement inventory. Any `outcome="retryable"` means Fred returned non-2xx and the backend must retain its FIFO head; check placement-store health and callback application errors |
| `fred_provisioner_lifecycle_event_sink_panics_total{event=~"provision_starting|restore_restarting|restore_refused|callback"} > 0` | Fred recovered a panic from a best-effort pre-dispatch, restore-refusal, or post-settlement callback event sink. Recovery deliberately lets backend dispatch or callback settlement continue; `event="callback"` means the durable callback is still acknowledged so it cannot wedge that lease's FIFO | Correlate the provision, restore, or callback log by lease and operation ID, use `event` to identify the affected sink, and file a bug with the panic stack; this should never occur |
| `fred_provisioner_ack_batch_fee_gas_errors_total` rising | Out-of-gas on lease acknowledgment txs | See [Out-of-gas tuning](#out-of-gas-tuning) |
| `fred_chain_signer_oog_retries_total{result="exhausted"}` rising | Same; the broadcast retry loop hit `max_gas_limit` | Same as above |
| `fred_docker_backend_die_event_dropped_total` sustained non-zero | Lease actor inbox is wedged | See [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_stuck_seconds > 900` | Some actor's `handle()` has been running for >15 min | See [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_panics_total > 0` | Bug — actor handler panicked | Check logs for stack trace, file an issue |
| `fred_docker_backend_lease_terminal_event_dropped_total` rising under clean shutdown | Real data loss pattern | The release store / provision struct may be out of sync with Docker — reconciler will re-detect on next cycle, but root-cause the wedged actor |
| `fred_provisioner_reconciler_panics_total > 0` | Bug — reconciler goroutine panicked | Reconciler keeps running for other leases, but file an issue with the stack trace |
| `fred_background_cleanup_panics_total > 0` | Bug in a cleanup loop. **Emitted by every fred binary**, so read `job` alongside `component`: `token` is providerd; `callback`, `diagnostics` and `releases` are a backend, and `retention` is the docker backend specifically | Same — keep running, file issue. Go to the journal of the host the `job`/`server` labels name, not to providerd by default |
| `fred_background_goroutine_panics_total{component="callback_replay"} > 0` | A bundled backend recovered a panic while replaying one lease's durable callback FIFO. That lease remains queued for a later pass; the bounded worker pool continues with unrelated leases | Check the backend log for `panic while replaying callback outbox` and its lease UUID/stack, then file an issue. Do not delete `callbacks.db`; replay is the recovery owner |
| `fred_api_rate_limit_rejections_total{limiter="tenant"}` spike | Specific tenant exceeded their bucket | Expected if a tenant is bursting; sustained spikes indicate a misbehaving client |
| `fred_payload_leases_awaiting > 0` for >5 min | Tenant created lease with `meta_hash` but never uploaded payload | Tenant-side issue; the lease will eventually expire |
| `fred_payload_persist_failures_total > 0` | A tenant `/update` reached the backend but could not be written to `payloads.db`. That lease is now running a manifest fred has no durable record of, and the next reprovision will revert it to its as-created deployment | Check disk space and permissions on `payload_store_db_path`, then confirm the store is healthy. The tenant received a `500` and can retry — a retry re-applies **and** re-persists. Nothing in fred retries on their behalf, so a lease left in this state stays exposed until the tenant acts |
| `fred_reconciler_last_success_timestamp_seconds` stalled | Reconciler is stuck, panicking, running with incomplete inventory, or failing an external read/durable projection — only a complete successful projection advances this | Check `fred_reconciler_sweep_complete` first: 0 means a sweep is in progress or the latest sweep did not complete a durable full-fleet projection, not that the durable topology baseline was revoked. Then inspect `fred_reconciler_backend_fetch_total{outcome!="ok"}`, chain health, placement-write logs, and `fred_reconciler_runs_total{outcome="error"}` |
| `fred_reconciler_backend_fetch_total{outcome!="ok"}` sustained for one backend across ≥3 sweeps (~6 min at a 2m interval) | That backend is unreachable from providerd. Its owner-affine leases are deferred and inventory silence changes no attempt or conflict. With an established baseline, safe callbacks/status/cleanup can continue and the reconciler may use nodes that answered both inventories for genuinely new recordless `PENDING` work | [Backend unreachable during reconciliation](#backend-unreachable-during-reconciliation) |
| `fred_reconciler_sweep_complete == 0` sustained | The last fleet observation was incomplete. The gauge becomes 0 before every sweep and remains there while it is in progress or after any chain read, provision/retention inventory, or durable projection failure. It is observability, not a fleet-wide authority switch: a matching durable baseline may remain healthy, while the reconciler narrows recordless `PENDING` admission to the exact answering-node scope and defers lease-specific unsafe work | Inspect backend fetch outcomes, chain health, reconciliation errors, placement-write failures, and deferred lease logs. Do not infer that all mutations are blocked or that absence on a silent node is evidence |
| `fred_provisioner_reconciler_deferred_leases_total` rising while `fred_reconciler_sweep_complete == 1` | Every backend answered, but one or more leases still lacked a safe, current lifecycle decision: ownership was ambiguous, placement was unusable or unresolved, or an operation/placement change crossed the inventory boundary. A low rate during provisioning, restore, or other lease churn is expected | Correlate the lease-level `reconcile: deferring lease` logs with operation Registry and placement changes. Investigate a sustained rate or the same lease repeating without concurrent work; it can indicate a stuck unresolved record or unusually slow sweeps |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown"}` rising | Fred is declining to clean up state for a lease **the chain has no record of**, and will decline again every sweep — this one does not self-heal. Either providerd is pointed at the wrong or a reset chain (check the `pass` label spread: fleet-wide means config, one lease means a phantom), or a provision exists that no lease ever created | Confirm the chain endpoint and provider UUID first. If the chain is right, the resource is genuinely unowned: deprovision it by hand once you have confirmed the tenant is gone |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown_state"}` rising | The chain reports a lease state this providerd build cannot classify — either the zero `UNSPECIFIED`, or a state added to the ledger after this binary shipped. Cleanup is withheld, which is data-safe but permanent for those leases | **Upgrade fred** to a build whose `manifest-ledger` pin knows the new state. Unlike `chain_unknown` the chain is fine and providerd is behind it, so do not go looking for a phantom provision |
| `fred_reconciler_cleanup_skips_total{reason="chain_error"}` sustained | The per-candidate chain re-check is failing, so cleanup is paused (data-safe). Usually the same cause as any other chain-query failure, or a lookup that blew its 10s budget — that budget exists so a stalled query cannot wedge the sweep, and it reports as an error rather than as evidence | Check `fred_chain_query_duration_seconds{query="get_lease"}` and the node's health; self-heals |
| `fred_reconciler_cleanup_skips_total{reason="chain_live"}` rising steadily | The sweep's lease snapshot is often stale by the time cleanup runs — expected at a low rate, but a high one means sweeps are slow relative to lease churn | Compare `fred_reconciler_duration_seconds` against the reconcile interval; no action if the rate is low |
| `fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}` steady for an unreachable backend | Expected: that backend's placement records are never pruned from silence. Removing its name while records refer to it is rejected at startup | [Removing, renaming or pausing a backend](#removing-renaming-or-pausing-a-backend) |
| `fred_reconciler_cleanup_skips_total{pass="placement",reason="attempt_pending"}` sustained for the same lease | A write-ahead backend effect is still causally unresolved, so Fred preserves its placement evidence and refuses destructive cleanup. A low rate during ordinary provision/restore is expected; each live sweep redelivers the exact typed operation, persisted callback pair, immutable tenant/provider/item snapshot, and payload fingerprint or restore source only to its pinned backend. Accepted/idempotent responses promote it, contract-conforming refusals clear it, and ambiguity retains it. A terminal chain lease uses exact deprovision instead; every distinct attempted/confirmed backend must succeed before conservative affinity is promoted | Correlate the attempted backend and operation ID with that backend's durable intent/callback queue and inventory. Restore an unavailable backend, callback path, or payload database so exact recovery can settle. Missing payload data is retriable and never downgrades the request or terminates the live lease. If the backend definitively created nothing and cannot return a conforming refusal, follow the explicit placement-repair procedure; never clear the row from inventory silence alone |
| `fred_watermill_poisoned_messages_total > 0` | A handler exhausted retries on a message | Logs around the topic in question; the poison log identifies the message |
| `fred_docker_backend_retention_refused_total` increasing / `fred_docker_backend_retained_volume_bytes` approaching `fred_docker_backend_disk_pool_bytes` | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |
| `fred_docker_backend_retention_reaping_bytes` > 0 sustained across several sweeps | A `fred-retained-*`/leaked volume the sweep can't destroy — its footprint **is** counted in the admission pool (no over-admit) but pins capacity and likely needs manual reclaim. A rising `..._retention_leaked_total` with `reaping_bytes` flat is instead the self-healing rollback store-error case (no action). | [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
| `sum without (outcome) (increase(fred_docker_backend_retention_sweep_total[3h])) == 0` (with retention enabled) | The periodic retention sweep has stopped iterating entirely — the loop goroutine is gone, the ticker is starved, or the process is wedged. The sum advances on **every** pass regardless of outcome, so a flat sum is absence, not failure. Nothing is being reaped, no interrupted restore is being reconciled, and no orphan record is being pruned | Check the docker-backend process and its logs for `retention cleanup panic`; `fred_background_cleanup_panics_total{component="retention"}` distinguishes a panicking sweep from a dead one |
| `increase(fred_docker_backend_retention_sweep_total{outcome="error"}[6h]) > 0` | At least one sweep stage failed. Two distinct causes land here, so **read the log line before acting**: an unenumerable `retention.db` (the common one — the reaper and orphan pruner reclaim nothing and **every lease close skips volume teardown entirely**, leaving closes `Failed` and retrying, so the provider degrades toward refusing new work), or an unreadable **volume root**, which the orphan stage reports through the same outcome with a perfectly healthy store | **Start with the sweep's log line, not the database.** It prefixes each failure with its stage — `reap expired:` / `retry reaping:` / `list restoring:` are store reads, `reconcile orphans:` can be either (pair it with `retention_orphan_skips_total`: `reason="store_error"` vs `reason="list_error"` separates them exactly). Then fix whichever dependency it names; the parked work resumes on its own. Shares a root cause with the `claims_unreadable` row below |
| `fred_docker_backend_retention_accounting_refresh_failed_total` rising | The retained-disk projection could not be recomputed, so the five retention gauges **and** the admission pool's retained input are frozen at their last values. That is the data-safe direction (a zeroed projection would over-admit), but it means those gauges are stale — do not read them as current while this is rising | Same root cause as the row above: fix the retention store. Until then, treat `retained_volume_bytes` / `retention_reaping_bytes` as last-known-good, not live |
| `fred_docker_backend_volume_quota_clear_failed_total` rising | An XFS quota-clear command failed during interrupted-create compensation or typed deletion. The preceding block/inode proof failures do not increment this metric. Typed authority is retained and the current backend instance fail-stops; a fresh `Start` recovers it before readiness. A historical already-absent volume without typed authority can still leave an unowned table entry | [XFS deletion recovery and legacy quota entries](#xfs-deletion-recovery-and-legacy-quota-entries) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claims_unreadable"}` > 0 | The retention store could not be read, so no path could establish who owns a volume and **nothing was destroyed** — data-safe, but every close and reap is parked. At startup the orphan sweep now returns this error and readiness never opens; during runtime closing leases stay `Failed` and retry. Same root cause as the `claim_unreadable` and `store_error` skips | Fix `retention.db` health first and restart if startup was refused; parked runtime work otherwise resumes on its own. See the `store_error` row in [Partition collapse triage](#partition-collapse-triage) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claimed"}` sustained | A destroy path keeps meeting a volume another lease owns — normally an in-flight restore that is not converging, since a healthy restore clears its own claim on commit or rollback. Never data loss: the refusal is the guard working | Read with `restore_finalizer_pending_total` and `retention_reaping_leases`; the WARN log names the volume and its owning lease. [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation=~"restore_reconcile\|restore_rollback\|deprovision"}` rising | A `compose down` failed AND the per-container fallback could not finish it, so containers — and the anonymous volumes attached to them — are pinned on this host. Not data loss: on these **blocking** operations fred keeps the lease tracked and its capacity reserved rather than advancing state over live containers, and retries. A sustained rate with `operation="restore_reconcile"` also means the affected tenant cannot retry its restore until the teardown succeeds | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation="provision_cleanup"}` rising | A failed provision could not prove its candidate containers were gone. Fred retains the exact operation intent, full pool reservation, and volume claims; suppresses the terminal callback; and fail-stops this backend instance. A fresh `Start` classifies and cleans the immutable candidate before settling, so capacity is never returned over possibly live substrate | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation="restore_prelude"}` rising | The canceled restore prelude's advisory teardown also failed. Nothing is held open and no prelude retry owns it; this is routinely just a canceled or timed-out restore request with nothing on the host at all | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_retention_partition_collapsed_total` increasing | Partition declarations collapsing to the default bucket — harmless (closes are never blocked, data is never destroyed), but check the `reason` label first: `invalid` / `divergent` / `over_limit` signal an integrator-side key bug, while `no_input` / `store_error` signal a backend hydration or store-health issue | [Partition collapse triage](#partition-collapse-triage) |
| `fred_docker_backend_retention_cap_check_failed_total` increasing | Retention cap checks are failing OPEN on store-read errors — quotas are silently unenforced (data-safe, but the gates are off) | Check `retention.db` health; see the `store_error` row in [Partition collapse triage](#partition-collapse-triage) |

---

## Out-of-gas tuning

Lease acknowledgments and withdrawals are submitted as Cosmos SDK transactions. Since ENG-431 the daemon **simulates gas per transaction**: the declared gas is `gas_adjustment × simulated GasUsed` (default `gas_adjustment` 1.2), and a simulated estimate exceeding `max_gas_limit` (default `0` = uncapped) is rejected before broadcast. `gas_limit` (default 1,500,000) is now only the **Simulate-failure fallback ceiling** — used when the Simulate RPC errors or the simulation circuit-breaker is open, not on the steady-state path. When the chain still rejects a broadcast with `out of gas`, the broadcast layer retries with `1.5×` more gas, compounding up to `max_gas_limit`.

**Diagnosing:**
- `fred_chain_gas_simulation_total{result}`: a rising `fallback` rate means Simulate is unavailable and the daemon is on the fixed `gas_limit` ceiling (so `gas_limit` tuning below becomes relevant); `refused` means a simulated estimate exceeded `max_gas_limit` and the tx was rejected before broadcast.
- `fred_chain_gas_simulated`: histogram of the declared-gas magnitude per broadcast — watch it to observe steady-state gas draw and to size `max_gas_limit`.
- Spikes in `fred_chain_signer_oog_retries_total{result="retried"}`: retries are working — the estimate was tight but eventually succeeded.
- Spikes in `fred_chain_signer_oog_retries_total{result="exhausted"}` or `fred_provisioner_ack_batch_fee_gas_errors_total`: the cap is too low or the underlying tx genuinely needs more gas (e.g. a large authz batch).

**Tuning:**
1. Steady-state gas self-tunes via per-tx simulation — you normally do **not** set `gas_limit`; it only affects the Simulate-failure fallback path.
2. If `fred_chain_gas_simulation_total{result="fallback"}` is non-trivial, set `gas_limit` to `1.2 × p99 gas_used` (from chain logs or `fred_chain_gas_simulated`) so the fallback still covers a real tx.
3. If using authz sub-signers (`sub_signer_count > 0`), each lane has its own gas budget; the total chain cost scales with `sub_signer_count`.
4. Set `max_gas_limit` to a safety cap (e.g. `4 × gas_limit`) so a runaway tx doesn't consume an entire fee budget on retries; note it also bounds the pre-broadcast reject (`result="refused"`).

---

## Wedged lease actor (docker-backend)

If `lease_actor_stuck_seconds` exceeds your alert threshold, one specific lease's actor goroutine has been mid-handler for too long.

**Investigation:**
1. The metric is unlabeled (gauge of the *oldest* in-flight actor across all leases), so identifying which lease is stuck requires a goroutine dump. Send SIGQUIT to the docker-backend process — the Go runtime dumps every goroutine's stack to stderr and exits. Capture stderr first:
   ```
   journalctl -u docker-backend -f &     # or `docker logs -f docker-backend`
   kill -SIGQUIT $(pgrep -f docker-backend)
   ```
   (Fred does not expose `net/http/pprof` by default. If you need non-fatal goroutine dumps, build with pprof enabled or use `delve` against the running process.)
2. In the dump, look for goroutines in `leaseActor.handle()` and `leaseActor.run()`. The actor's `leaseUUID` is on the receiver — visible as the `*leaseActor` argument in the stack frame.
3. Check what handler it's in — typically `provision.go`, `deprovision.go`, or `restart_update.go`.

**Common causes and remedies:**
- **Image pull stuck**: Docker Hub rate-limited or registry unreachable. Check `docker logs` for the daemon, then `docker pull <image>` manually. Reduce `image_pull_timeout` so the actor errors out sooner.
- **`docker stop` hanging**: a container is ignoring SIGTERM and the grace period is long. Lower `container_stop_timeout`.
- **Volume cleanup hanging on btrfs/zfs**: a quota or subvolume operation is blocked in the kernel. Inspect the filesystem state directly.
- **Genuine deadlock**: file an issue with the goroutine dump. The actor will not unblock; the reconciler will re-detect the lease on its next cycle and retry, but the wedged goroutine leaks until restart.

**Last resort:** restarting the docker-backend recovers cleanly. State is rebuilt from Docker labels and bbolt stores on startup.

---

## Stuck teardown (docker-backend)

**Symptom:** `fred_docker_backend_teardown_fallback_total{outcome="failed"}` is rising.

**What it means:** `compose down` failed for a lease, and the per-container fallback could not finish the job either.

**Read the `operation` label first — it decides what fred did next, and therefore what you must do.**

| `operation` | Kind | What fred did |
|---|---|---|
| `restore_reconcile`, `restore_rollback`, `deprovision` | **Blocking** | Refused to advance state over containers it cannot prove are gone: the lease stays tracked, its pool reservation stays held, and the teardown is retried (the retention sweep for a restore, the lease's own retry for a close) |
| `provision_cleanup` | **Fail-stop recovery** | Retained the exact provision operation intent, full pool reservation, and volume claims; suppressed its terminal callback; latched the backend; and left the immutable candidate for a fresh `Start` to classify and clean before settlement |
| `restore_prelude` | **Advisory** | Discarded the error and advanced anyway. Nothing is held open and **nothing will retry** |

On a **blocking** operation this is not data loss and not an over-admission — the cost is capacity and disk that stay reserved, plus one anonymous volume per surviving container, until the substrate recovers.

`provision_cleanup` is stricter than either category: once candidate substrate
may exist, a teardown failure retains its write-ahead intent, reservation, and
volume claims, emits no terminal callback, and fail-stops the backend. Restart
the same sealed lineage; cold recovery removes only the intent's immutable
candidate IDs, proves cleanup, and settles afterward. `restore_prelude` remains
advisory: its callers are entered *because* the restore request's context was
canceled, and they hand that same dead context to the teardown, so both the
`compose down` and the container listing fail with `context canceled` and there
is usually **nothing on the host at all** — a canceled or timed-out restore, not
a fault.

`outcome="recovered"` is the benign twin — `down` failed but the fallback removed everything it found — with one caveat: on an advisory operation it can be vacuous, recording success after finding zero containers.

**Triage:**

1. Find the lease and the operation from the `operation` label and the warning logs (`compose down failed, falling back to individual removal`, then `failed to remove container`).
2. Check the daemon: `docker ps -a --filter label=fred.lease_uuid=<uuid>`.
   - **Nothing returned** and `operation="restore_prelude"` → this was a canceled restore request, not a leak. Correlate with a `POST /restore` that timed out (providerd's backend client gives up at 30s) or a providerd restart. No action.
   - A container stuck in `Removal In Progress`, or one whose `docker rm -f` hangs, usually means a wedged storage driver or a mount the kernel still holds.
3. Fix the substrate (see [Wedged lease actor](#wedged-lease-actor-docker-backend) for the same class of causes). Ordinary blocking restore/close paths retry once the daemon can remove containers again. `provision_cleanup` requires a fresh backend start because the failure deliberately latched that instance. An advisory `restore_prelude` has no retry owner; classify anything step 2 found before manual removal.
4. Restarting the docker-backend resumes exact durable recovery. For `provision_cleanup`, preserve the operation-intent, release, retention, storage-marker, volume, and Docker lineage together; recovery uses that evidence rather than merely adopting a leaked candidate as ordinary live work.

**Restore-specific consequence.** With `operation="restore_reconcile"` — and with `restore_rollback`, whose worker arm blocks the same way — the tenant's retention record stays in `restoring` until the teardown succeeds, and a restore cannot be re-requested while it does (the API reports the lease as not restorable). The data itself is safe: a `restoring` record is never reaped and its volumes are excluded from the orphan sweep. But the retention window keeps ageing, so clear the daemon fault well before `retention_max_age` expires for that lease. `restore_prelude` carries none of this — the record is already back to `active` and the tenant can retry immediately.

To confirm nothing is stranded after the fault clears, the counter should stop rising and `docker volume ls -qf dangling=true` should stop growing.

---

## docker-backend refuses to start: quota capability or reconciliation failure

On an `xfs` or `btrfs` backend the docker-backend **fails fast at startup** if it
cannot set volume quotas, exiting with (message from `internal/backend/docker/capability.go`):

```
docker-backend cannot set xfs volume quotas: CAP_SYS_ADMIN is not available to the
exec'd quota tools — grant AmbientCapabilities=CAP_SYS_ADMIN for a non-root daemon,
or include CAP_SYS_ADMIN in CapabilityBoundingSet when running as root (a plain
`setcap cap_sys_admin+ep` on the binary does NOT propagate to the child) — refusing
to start so per-volume disk_mb limits are enforced, not silently skipped
```

This is deliberate: a missing capability would otherwise silently drop every
`disk_mb` cap. Grant `AmbientCapabilities=CAP_SYS_ADMIN CAP_FOWNER` on the
docker-backend systemd unit — `CAP_SYS_ADMIN` to set the block limit, and
`CAP_FOWNER` so the startup backfill can re-tag pre-existing tenant-owned volumes.
A plain `setcap …+ep` on the binary does **not** work — the grant must reach the
exec'd `xfs_quota`/`btrfs` children. Full setup is in the xfs section and the
systemd note of [DEPLOYMENT.md](DEPLOYMENT.md#xfs-good-for-large-fleets). The `zfs`
backend is exempt (it supports `zfs allow` delegation, so a properly-delegated
non-root host is not rejected) and the `noop` backend is unaffected (no privileged
ops).

**XFS project-ID ownership.** Project IDs and dquots are global to the containing
XFS filesystem. `volume_data_path` being a subdirectory does not create a quota
namespace, and Fred inventories project IDs only below its own root. Enforce one
of these deployment states:

1. Prefer a dedicated XFS filesystem/mount for Fred volumes; or
2. On the current manifest-managed shared `/data` layout, continuously require
   Fred to be the only project-ID allocator on that mount.

Do not run two independently configured Fred roots, or another project-quota
manager, on the same filesystem. This release cannot reserve a disjoint range.
Before adding a second allocator, move Fred to a dedicated mount or implement
and deploy explicit range coordination as follow-up work. If this invariant may
already have been violated, stop every allocator and snapshot the filesystem;
do not clear or reassign a project ID until every directory using that dquot has
been accounted for.

**Startup quota reconciliation.** After the preliminary guard passes, the
backend re-applies each expected present managed volume's immutable effective
quota (re-tag + limit) so existing leases are enforced without a re-provision.
It attempts the complete live and retained inventory and joins all failures, but
any inventory, durable-resource-authority, or enforcement error makes `Start`
fail before the command-line HTTP/metrics server is created. The process never
serves a known volume uncapped.

`fred_docker_backend_volume_quota_backfill_total{outcome}` (`outcome ∈
{applied, failed}`) counts the individual attempts, but do not depend on scraping
it from this failure mode: the normal binary has not bound its metrics endpoint.
Use the nested `reconcile startup volume quotas` startup error to identify every
affected name. On XFS, a common cause is missing `CAP_FOWNER` while recursively
re-tagging a tree containing tenant-owned inodes. A truly fresh XFS root does not
need that capability; an existing one may. Grant it or repair the reported
substrate/authority error, then restart. No tenant volume needs reprovisioning.

---

## Interrupted managed-volume mutation at startup

Normal sealed startup inspects manager-private mutation evidence only after it has
exclusively opened the matching identity-bound journals. It resolves only strict,
typed forms before operation-intent recovery:

- **XFS:** a `.fred-xfs-stage-<project-id>-<managed-volume>` directory records
  the exact nonzero project ID and intended final name, but not the original
  requested quota. Startup therefore treats it as cleanup-only: it clears that
  dquot and removes only an empty stage or one whose sole entry is a no-follow
  regular project marker of at most ten bytes. A crash before marker fsync can
  recover those bytes empty, partial, or zero-filled; the parent-synced typed
  stage name is the cleanup authority. It is deliberately insufficient for
  publication, which still requires a complete parsed marker equal to the ID in
  the stage name. Runtime errors after the stage becomes durable attempt exact
  compensation; the external quota clear uses a detached cleanup context capped
  at 30 seconds and by any earlier aggregate parent deadline. Any cleanup or
  outcome ambiguity preserves the stage and fail-stops the current backend
  instance; a fresh `Start` must recover it before serving. Startup never renames
  a recovered stage into a live tenant volume. An unexpected entry, conflicting
  project ID, extra contents, quota-clear error, or ambiguous removal preserves
  the evidence and fails startup.
- **XFS deletion:**
  `.fred-xfs-delete-<project-id>-<managed-volume>` is the empty,
  parent-synced authority for one admitted destructive operation. It is
  normalized to project ID zero so it cannot keep the retiring project in use.
  Startup re-normalizes and syncs it before resuming in-place removal of only
  the encoded final volume. The final absence is parent-synced, then numeric
  quota reports must prove both block and inode usage for the encoded project
  ID are zero before all four limits are cleared. An open-but-unlinked tenant
  file keeps usage nonzero and therefore keeps startup unready. The authority
  is removed and the parent synced only after the clear succeeds; failures
  preserve it, latch and stop the current backend instance, and refuse same-name
  creation. A fresh `Start` must complete recovery before readiness.
- **ZFS:** an exact managed child with the configured mountpoint but
  `mounted=no` is preserved interrupted-create evidence. Startup attempts to
  mount and re-attest that exact child; it never destroys it. A different
  mountpoint, collision, failed mount, or ambiguous result fails startup.
- **Btrfs:** there is no private stage. The create command publishes the
  subvolume before setting its qgroup limit, so operation-intent recovery, the
  fail-closed startup quota reconciliation, and orphan classification converge
  an interrupted result.

First fix the filesystem or quota-control-plane error and retry the same sealed
backend. Do not rename or delete a create/delete stage, synthesize a marker,
change a ZFS
mountpoint, or destroy a dataset merely to make readiness green. If retry still
fails, do not leave `Restart=on-failure` probing the lineage every few seconds:
run `systemctl stop fred-docker-backend`, verify the unit and process are fully
inactive, then snapshot the complete marker/journal/Docker/volume lineage and
inspect the exact error before any proof-bearing repair. A runtime terminal
latch closes the listener and drains before exiting 1; a recovery error found by
`Start` never binds the listener and exits 1 immediately. In either case only a
new process retries recovery.

The stopped `-preflight-storage-identity-adoption` and
`-initialize-storage-identity` commands are deliberately different: they refuse
XFS create/delete stages and unmounted ZFS children without normalizing them.
Neither XFS form can originate from v0.13.0, so finding one during an unsealed
v0.13 cutover
means an upgraded process already mutated the root or the wrong snapshot is in
use; preserve the evidence and restore the matching stopped snapshot. For an
unmounted ZFS child, verify the exact child and configured mountpoint, then
remount it or restore the matching pool snapshot before rerunning the same
one-shot mode. Never interpret a failed one-shot command as cleanup authority.

---

## XFS deletion recovery and legacy quota entries

`fred_docker_backend_volume_quota_clear_failed_total` increments only when the
four-limit XFS clear command fails during interrupted-create compensation or
typed deletion
(`xfs_quota -x -c 'limit -p bhard=0 bsoft=0 ihard=0 isoft=0 <projID>'`). It does
not count the preceding numeric block/inode usage proof. A retained dquot holds
no disk by itself, but it remains in the project-quota table and every
`xfs_quota` scan (`report -p`, used by `Usage` and `Validate`) has to walk it.

For a current-generation typed deletion, the matching
`.fred-xfs-delete-<project-id>-<managed-volume>` authority remains on disk.
Cleanup is automatic and strict: restore quota-control-plane availability,
close any process that still holds an unlinked file from that project, and
restart the same sealed backend. Do not remove or edit the authority and do not
clear its quota manually; it is the proof that makes retry safe. Startup remains
unready until final-path absence is durable, numeric block and inode usage are
both zero, the clear succeeds, and the authority itself is durably removed.
At runtime, any error after that authority becomes durable latches and stops the
current backend instance; there is intentionally no same-process retry.

Keep `volume_data_path` and its containing XFS mount fixed while docker-backend
runs. Stop the backend before unmounting, replacing, bind-mounting over, or
moving either path, and restart it afterward so the complete storage lineage is
re-attested. XFS command-line quota operations use the stable mountpoint/path;
Fred's surrounding descriptor proofs detect drift but cannot make an external
tool call atomic with a concurrent mount replacement.

**Legacy remediation.** An entry leaked by a pre-ENG-459 daemon has no typed
delete authority. There is no safe automatic sweep for those rows: a
filesystem-wide `report -p` cannot distinguish Fred's historical orphan from a
live foreign limit. In particular, path absence alone is never authority to
clear a dquot.

1. Stop and fence docker-backend and every other project-ID allocator on the
   containing XFS filesystem; verify no process remains and take a recoverable
   filesystem plus Fred-journal backup.
2. Prove the historical sole-allocator invariant for that filesystem, or prove
   the exact project ID belonged to Fred from matching stopped-process backups,
   journals, markers, and change records. If ownership is not positively
   attributable, leave the row unchanged and use a proof-bearing repair tool.
3. Query that exact numeric ID in both block and inode reports and require used
   blocks **and** used inodes to be zero. Nonzero inode use may be an
   open-but-unlinked file even when no directory is visible; find and close its
   holder instead of clearing or reusing the ID.
4. Only after those independent ownership and zero-use proofs, clear the exact
   ID with `xfs_quota -x -c 'limit -p bhard=0 bsoft=0 ihard=0 isoft=0 <projID>' <mountpoint>`,
   re-read both reports, then start one allocator and let startup re-attest the
   complete lineage.

Backends that ran a **pre-v0.7.0** build never cleared limits on `Destroy` and so
accumulated one leaked entry per provision — those need this one-time manual
cleanup. Later legacy builds attempted a best-effort clear after removing the
directory; a rising counter from such an already-absent/no-authority case still
needs the same classified manual cleanup. Current typed deletion instead keeps
restart-recovery authority and fail-stops the current instance; a fresh `Start`
must complete it before readiness. Since ENG-548,
`Destroy` also clears the inode limits
(`ihard`/`isoft`) alongside the block limits — a backend running a pre-ENG-548
build clears only `bhard`/`bsoft` and leaves `ihard` behind on downgrade; see
[Tenant hits its inode quota (`EDQUOT`)](#tenant-hits-its-inode-quota-edquot)
below.

---

## Tenant hits its inode quota (`EDQUOT`)

XFS project quotas enforce block (`bhard`) and inode (`ihard`) limits
independently, so a workload writing many small/zero-byte files can hit its
inode ceiling well before its disk-space cap (ENG-548). This is a per-tenant
limit, so it does not surface on any fred metric; the host-wide analogue is
the standard `FilesystemInodesLow` alert on the underlying filesystem's global
inode usage.

**Symptoms:**
- Tenant reports `EDQUOT` or "no space left on device" from its container
- `df -h` on the tenant's volume still shows free bytes

**Confirm:**
1. `df -i <volume-path>` reports **filesystem-wide** inode usage, not the per-tenant quota — for an `ihard` hit it still shows ample free inodes (`IUse%` well below 100%). That is the tell that it's the per-project inode cap, not global exhaustion (global exhaustion would instead fire `FilesystemInodesLow`).
2. `xfs_quota -x -c 'report -p -i' <mountpoint>` filtered to the tenant's project ID confirms inode count is pinned at the hard limit.

**Remediation:** there is no fred-side alert or auto-remediation for a single tenant's inode ceiling. Two levers:
1. Raise the SKU's `disk_mb` — `ihard` scales with it (`disk_mb × 1 MiB / min_avg_file_bytes`), so a bigger disk budget raises the inode ceiling too.
2. Lower `min_avg_file_bytes` provider-wide (denser ratio, more inodes per MB) — restart required; values below 512 are rejected at config validation.

---

## Backend at capacity

When SKU resource pools are full, bundled backends return HTTP 503 with the
declared error envelope and `code="insufficient_resources"`. That machine code
produces the typed `ErrCapacityRefused` contract verdict, so Fred clears only the
matching write-ahead attempt and can route a later retry to another eligible
backend. The response body is not HMAC-authenticated: the code establishes
protocol conformance under the configured transport trust boundary, not
cryptographic authorship. Use TLS or an equivalently trusted network if an
on-path response forger is in scope.

A legacy/code-less or unknown-code 503 remains `ErrInsufficientResources` but is
ambiguous; a malformed/non-envelope 503 is `ErrMalformedErrorBody` and is also
ambiguous. An intermediary could have emitted either after backend acceptance.
Fred therefore keeps that write-ahead attempt and does not substitute another
backend. On a later sweep it redelivers the identical operation—including its
durable tenant, provider, ordered items, callback pair, and payload identity—only to the
pinned backend: acceptance/idempotent recognition confirms ownership, a
contract-conforming coded refusal clears the attempt, and another ambiguous
result retains it. Inventory absence does not clear the ambiguity because the
original request could commit after the list response. An exact callback,
exact paired-generation inventory, or explicit operator proof and repair are
the other settlement paths.

**Symptoms:**
- `fred_backend_insufficient_resources_total{backend="X",verdict="coded_refusal"}` rising for declared capacity refusals
- `fred_backend_insufficient_resources_total{backend="X",verdict="ambiguous"}` rising for legacy/code-less/unknown-code 503s
- `fred_backend_malformed_error_body_total{backend="X",operation=~"provision|restore"}` rising for malformed/non-envelope 503s
- `docker-backend /stats` shows allocated == total or close to it
- Active leases stay in `provisioning` state

For Docker, `allocated_disk_mb` is physical admission accounting, not only the
sum of SKU `disk_mb`. Each live `disk_mb: 0` instance also holds the positive
`container_tmpfs_size_mb` value frozen as its scratch allowance when that
generation was admitted. The reservation is intentionally conservative and is
present even when image inspection finds no writable path and no host directory
is created. It counts against `total_disk_mb`, `tenant_quota.max_disk_mb`, and the
disk allocated ratio. `/tmp`, `/run`, image `VOLUME` tmpfs overrides, and
tenant-declared tmpfs remain memory-backed and are not additional disk charges.

**Options:**
1. **Add capacity**: spin up another docker-backend on a different host with the
   same `skus` and a new unique backend name. Fred routes each new provision to
   the least-loaded matching backend — the one reporting the lowest
   allocated-CPU ratio from its `/stats` endpoint — so a fresh, empty host
   preferentially absorbs new provisions. Adding an identity invalidates the old
   topology baseline; keep lifecycle ingress closed until a complete inventory
   establishes the new one. The mandatory `placement_store_db_path` records
   every resulting ownership decision.
2. **Tighten SKU profiles for future generations**: smaller CPU/memory/disk per
   SKU lets newly admitted leases fit. Existing active generations, maintenance
   operations, recovery, and closes use their pinned profiles. Never combine a
   v0.13 cutover with a profile resize or removal; keep the deployed mapping and
   numeric values—and Docker's `container_tmpfs_size_mb` scratch allowance—through
   the first successful upgraded backend startup.
3. **Tenant quotas**: if one tenant is hogging resources, set `tenant_quota` in
   `docker-backend.yaml` to cap them. Size `max_disk_mb` for durable SKU disk plus
   the pinned scratch allowance of every live diskless instance.
4. **Force reconciliation**: orphan provisions (lease closed but containers still running) consume budget. The reconciler removes them on its cycle. Restarting a backend does not trigger a provider sweep; wait for the next cycle or, during a deliberate maintenance window, restart `providerd` to run startup reconciliation immediately.

---

## Backend unreachable during reconciliation

A backend that is configured but not answering `GET /provisions` or
`GET /retentions` — down, wedged, partitioned, or with its circuit breaker open
— no longer aborts the whole reconciliation sweep. Fred marks the inventory
incomplete and retries on the next cycle. Existing workloads continue serving;
exact callbacks and safely evidenced status/cleanup work can continue. Once a
complete inventory has established the durable baseline for this topology, a
partial sweep can also admit genuinely new recordless `PENDING` work on the
typed set of nodes that answered **both** inventories. It never treats a silent
node as evidence about the leases it may hold.

**Symptoms**

- `fred_reconciler_backend_fetch_total{backend="X",outcome!="ok"}` rising, with
  `outcome` distinguishing `error` (contacted and failed), `circuit_open` (fred
  short-circuited without dialing), and `panic` (a bug — file an issue)
- `fred_reconciler_sweep_complete` at 0
- `fred_provisioner_reconciler_deferred_leases_total` rising
- `fred_reconciler_runs_total{outcome="degraded"}` incrementing each cycle
- `fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}`
  rising — that backend's placement records are being held, which is the intended
  behavior and not an additional fault
- `fred_reconciler_last_success_timestamp_seconds` frozen — expected while
  degraded, and the reason the staleness alert does not go quiet during an outage

**What is happening to the leases**

| Lease | Behavior |
|---|---|
| In the reconciler's answering-node scope | Positive observations, acknowledgements, and safely evidenced cleanup may progress. Genuinely new recordless `PENDING` reconciliation may route only within this scope |
| On the unreachable backend | Inventory-driven work is pinned and deferred rather than re-provisioned or deprovisioned elsewhere. An exact authenticated callback may still settle through the callback path |
| With no placement record | A `PENDING` lease may use the answering-node scope after bootstrap. A recordless `ACTIVE` lease is deferred during an incomplete sweep because recovery cannot safely infer its owner |
| With a confirmed owner | Pinned to that exact backend. If it did not answer, the lease is deferred rather than routed elsewhere |
| With an unresolved placement attempt | Redelivered with the same operation ID and request only to the attempted backend. Acceptance/idempotent recognition promotes it, a contract-conforming refusal clears it, and ambiguity retains it. A positive report confirms it only with the exact paired typed generation; silence can never prove rejection |
| With a placement conflict | Quarantined with every durable candidate. A positive report from another backend expands that union; another candidate going silent never resolves it |
| Positively reported by an inventory endpoint Fred rejected | The raw membership fact is persisted as an unusable `untrusted_positive` quarantine, even though its payload cannot establish ownership. This covers contradictory provision/retention membership, missing or inconsistent endpoint storage identities, and a storage identity that conflicts with the durable backend pin |
| Orphans on the backends that answered | Deprovisioned normally. A silent backend reports no provisions, so it contributes no orphan candidates of its own and cannot mask anyone else's |
| Orphaned payloads | Cleaned normally — that pass compares the payload store against the chain and reads no backend state at all |
| Placements of leases on the unreachable backend | Not pruned: only that backend's own report can turn "absent from the backend data" into evidence about its records |

Nothing migrates. Partial inventory permits only the narrow recordless
`PENDING` case above; owner-affine work, recordless `ACTIVE` recovery, attempts,
and conflicts remain pinned or deferred. Cleanup stays lease-local: a positively
reported terminal orphan or payload may still be removed under its own guards.
The operational cost is therefore concentrated on work whose safety evidence
depends on the unavailable backend, not every healthy node.

**What to do**

1. Identify the backend from the `backend` label and check it directly
   (`GET /health`, `GET /stats`, its own logs).
2. Bring it back. Recovery needs no action on fred's side — the next sweep can
   resume its pinned leases. Exact positive observations can confirm attempts;
   inventory absence never clears attempts or conflict quarantine. A sole
   `untrusted_positive` candidate self-resolves only when a later **complete**,
   identity-valid inventory positively reports that same lease on that same
   backend. A partial sweep, silence, a different reporter, multiple candidates,
   unknown ownership, or an ordinary placement conflict cannot self-resolve.
3. If it is gone for good, that is a **removal**, not an outage — see the next
   section. Do not leave it configured-but-absent indefinitely: PENDING leases on
   it are on a ~30-minute chain expiry clock the whole time.

> **Why cleanup no longer pauses fleet-wide.** Orphan detection, payload cleanup
> and placement pruning all delete durable state, and until ENG-654 all three were
> gated on a complete fleet view — so one silent machine stranded admission
> capacity on every healthy one, for as long as the outage lasted. Each pass now
> carries the guard its own hazard calls for. Orphan deprovision and payload
> cleanup re-read the lease from the chain per candidate and act only on a
> positively reported terminal state, which addresses the real hazard (the sweep's
> two lease queries are not atomic) on every sweep rather than only on degraded
> ones. Placement pruning asks whether *that record's* backend answered both
> inventories, which is a per-record question. Skipped cleanup still costs only a cycle of latency;
> mistaken cleanup still costs a tenant their workload.

---

## Removing, renaming or pausing a backend

**Fred never moves a lease between backends.** A backend `name` is a
case-sensitive, immutable storage identity persisted in the placement database.
Keep a paused or unreachable backend configured under the same name. Removing
or renaming a name while any confirmed owner, attempt, or conflict still refers
to it is rejected at `providerd` startup; Fred will not orphan those references
or silently reinterpret another machine as their owner.

| Operation | Behavior |
|---|---|
| Reconciler recovery for a new recordless `PENDING` lease | After bootstrap, may use another backend only when that node answered both inventories and belongs to the typed per-sweep admission scope |
| Live chain lease positively present in retention | Deferred lease-locally; ordinary provision cannot replace data that requires the restore path |
| Re-provision, read, or restore tied to the paused backend | Pinned and deferred/`503`, never routed to a peer |
| Close / deprovision | Every durably known owner, attempt, and conflict candidate is targeted. Reachable peers are still swept, but the close fails rather than acknowledging success while the recorded backend cannot be contacted |
| Recordless `ACTIVE`, unresolved attempt, or conflict | Deferred; inventory silence cannot clear or resolve it |
| Leases safely owned by other answering backends | Unaffected |

Two ERROR log lines cover the two paths, and the reconciler one is the one you
will actually see, since it fires unattended on every cycle:

```
reconcile: refusing to provision, lease is placed on a backend the router does not know   # reconciler
refusing to provision: lease is placed on a backend the router does not know              # event path
```

These log lines are defensive checks for legacy/corrupt composition; normal
startup now rejects a topology that omits a referenced name. The reconciler line
carries `lease_uuid`, `placement_backend`, and `placement_state`. The event-path
line carries `lease_uuid`, `sku`, and the recorded backend name.

This is deliberate (ENG-635). Substituting a healthy peer would provision a
brand-new **empty volume** while the tenant's real data sits intact on the
absent machine — unattended, on a timer, for every affected lease at once, and
reported to the caller as success. Refusing loses availability; substituting
loses data.

`503` rather than `404` matters for the same reason: a `404` tells a tenant
their deployment no longer exists and invites them to destroy and recreate it,
turning a recoverable outage into real data loss.

Recovery from an outage is to restore the same storage under its original
configured `name`. If the name temporarily left the topology, restoring its
entry reactivates that historical identity; it does not authorize replacement
storage under the old name. The membership change invalidates the prior
admission baseline until the next complete inventory commits. Restart
`providerd` after restoring the entry because the router has no configuration
reload signal.

Renaming does not migrate data. A removed, fully drained name remains historical
placement metadata and may later rejoin only for the same storage identity. A
replacement storage system must receive a new globally unique name and complete
a full inventory bootstrap for the changed topology before degraded admission
resumes.

Do not remove a merely silent name. Fred permits removal only when the latest
complete inventory for the still-current topology recorded that backend's raw
`/provisions` **and** `/retentions` responses as concretely empty, and neither
the placement nor lifecycle buckets retain a reference to it. This drain
evidence is collected before causal projection filters can hide in-flight work
and is bound to that topology generation. If it is missing, restore the backend,
let one complete sweep prove it empty, then change configuration. The proposed
topology must also be fully reachable for the identity probe; one healthy node
cannot authorize a membership change on behalf of another.

Its **placement records outlive an outage**, deliberately. The pruner deletes a
record only under its lease-local positive guards; a silent backend contributes
no proof, so its records stay in the index and are counted under
`fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}`.
They are the only surviving pointers to where that machine's data may be. A
lease ever reported by multiple backends retains the sorted union of every
candidate in conflict quarantine. Fred also preserves a positive fact from a
rejected inventory response as `untrusted_positive`, including when it has only
one candidate. That narrow one-candidate quarantine may self-resolve only from a
later complete, identity-valid matching positive from the same backend. A
partial sweep or silence never resolves it; multi-candidate, unknown-owner, and
ordinary conflicts require explicit operator proof and repair.

> **Ansible caution.** `roles/fred/templates/providerd.yaml.j2` renders backend
> names from each host's explicit `backend_index`; the role validates that every
> participating host has one and that the indices are unique. Treat those values
> as durable storage identity metadata: never renumber a surviving host during a
> membership change, and never assign a departed host's historical index/name to
> replacement storage.

---

## Reclaiming retained volumes under disk pressure

Retained (soft-deleted) volumes count against the disk admission pool until they
are reaped (`fred_docker_backend_retained_volume_bytes` shows the reserved
footprint; `fred_docker_backend_retained_leases` the count). When retained data
crowds out new provisioning, reclaim it least-destructive-first:

1. **Assess.** Compare `fred_docker_backend_retained_volume_bytes` against
   `fred_docker_backend_disk_pool_bytes` (and `..._retained_disk_cap_bytes` if a
   cap is set). A rising `fred_docker_backend_retention_refused_total` means
   closes are already being denied a grace window.
2. **Shorten the grace window.** Lower `retention_max_age` so the reaper sweeps
   sooner. Duration values use Go syntax (`h`/`m`/`s`) — `336h` = 14 days, not
   `14d`.
3. **Bound the tier.** Set/lower `max_retained_disk_mb` (per-provider) and/or
   `max_retained_leases_per_tenant` (per-tenant). New closes over the cap
   refuse-to-retain (destroy immediately); existing in-grace data is never
   evicted to admit another tenant.
4. **Force a sweep.** Restart the backend to trigger the boot-eager reaper.

`max_retained_disk_mb` directly trades retained-grace capacity against
live-provision capacity within the single `total_disk_mb` pool.

Scratch is not retention entitlement. A diskless writable-path-only volume is
reclaimed on close and does not consume the retained caps. It did consume the
live pool while the generation ran, however, and a conservative retained exact
name that could not yet be classified or destroyed remains in the physical
retained projection and has its pinned quota re-applied until the finalizer reaps
it. This is fail-closed physical accounting, not permission to retain future
diskless workloads. Correlate the retained/reaping gauges and destroy refusals
when an anomalous scratch name consumes admission headroom.

**Sizing `total_disk_mb`.** Fred WARNs at startup only when `total_disk_mb`
exceeds the filesystem's **gross** total (`statfs` f_blocks × block-size) — this
is a coarse upper-bound guard, not a usable-capacity check. Root-reserved blocks
and non-fred consumers (Docker image layers, logs, etc.) are **not** excluded from
f_blocks, so the WARN fires late. Operators must leave their own headroom below
the true usable capacity; a silent miss here means retained+live volumes can
exhaust physical disk (tenant ENOSPC).

**Per-tenant fairness.** `max_retained_leases_per_tenant` is a **count** cap: it
limits how many retained leases one tenant may hold, but not how much **disk** they
occupy. Each close-time eviction it forces (a tenant's own oldest retained lease
evicted from the active set (marked reaping) to make room) increments
`fred_docker_backend_retention_evicted_total`
— distinct from `..._retention_refused_total`, which is the global
`max_retained_disk_mb` refuse-to-retain path; a rising evicted counter is the signal
that tenants are silently losing restore grace to the count cap. Under
`max_retained_disk_mb`, one tenant using large-disk SKUs can fill the
entire retained pool, after which other tenants' `RetainOnClose` closes degrade to
refuse-to-retain (destroy, no grace window). This is an availability DoS on the
retention feature for those tenants, not a data-theft risk — destroy only touches
the closing lease's own volumes. True per-tenant disk fairness is available via
`max_retained_disk_mb_per_tenant` (and per-tenant `retention_tenant_budgets`).

**Which cap is biting (three-way triage).** With partition budgets deployed, the
retention counters resolve to three distinct signals — do not conflate them:

- `fred_docker_backend_retention_partition_evicted_total` rising is an
  aggregator's own **L2 per-partition** sub-cap working as intended (one of its
  end-customers hit its slice) — **NOT** a provider-capacity signal. The bare
  `..._retention_evicted_total` keeps its deployed **L1 per-tenant** meaning.
- `fred_docker_backend_retention_refused_by_scope_total{scope}` tells you which
  disk cap refused a close: `scope=global` (L0 `max_retained_disk_mb` — provider
  capacity, the real disk-pressure signal), `scope=tenant` (L1 per-tenant
  aggregate), or `scope=partition` (L2 per-partition, an aggregator's own slice).
  The bare `..._retention_refused_total` keeps its deployed L0-global-only meaning
  (and the alert keyed on it), so it is the `scope=global` subset.

So under disk pressure, a rising `scope=global` refusal (or the bare
`..._retention_refused_total`) is the provider-capacity signal that drives this
runbook; `partition`/`tenant`-scoped refusals and partition evictions are an
aggregator's own budget doing its job and do not mean the backend is full.

### Reclaiming leaked / stuck-reaping orphan volumes

A `fred-retained-*` (or leaked-canonical) volume whose destroy fails under a degraded
filesystem/store becomes a **reaping tombstone**: its footprint keeps counting in the admission
pool (so it never silently over-admits) and the retention sweep **auto-retries** the destroy
every interval. `cleanupOrphanedVolumes` deliberately never touches `fred-retained-*` names, so
the sweep is the only automatic reclaimer.

- **Signal.** `fred_docker_backend_retention_reaping_bytes` / `..._retention_reaping_leases` > 0
  is the stuck-volume signal (these footprints **are** counted in the admission pool, so there is
  no over-admit — they pin capacity until reclaimed). A transient EBUSY clears within one sweep;
  a value sustained across several sweeps is a stuck volume. `..._retention_leaked_total` is a
  broader event counter — it increments on a failed-destroy / give-up (which DO drive
  `reaping_bytes`) **and** on a rollback uncommitted-revert (which keeps its footprint counted as
  *live* and self-heals on the next `reconcileRestoring` sweep, NO stuck volume). So a rising
  `leaked_total` with `reaping_bytes`/`reaping_leases` flat needs no action; only sustained
  `reaping_bytes` is actionable here.
- **Diagnose.** Find the volume(s): `ls <volume_data_path> | grep -E 'fred-retained-|fred-'`.
  Check why destroy fails — a container still bind-mounting it (`docker ps`, then stop it), or a
  filesystem error (`dmesg`).
- **First check whether the reaper is holding the tombstone on purpose.** If
  `..._retention_reap_skips_total{reason="restore_claimed"}` is also incrementing, one of the
  tombstoned names is a volume an **in-flight restore adopted** — the destroy is refused
  deliberately, and the data belongs to the *restoring* lease, not the tombstoned one. **Do not
  remove it by hand:** the steps below would destroy another tenant's data, and the tombstone
  would not clear anyway (the skip is by name, not by existence). Find the restoring record
  (`GET /retentions`) and clear whatever is blocking its **rollback** — usually a re-quarantine
  rename that cannot complete (both names present on disk, or a filesystem error in `dmesg`).
  The tombstone clears on the next sweep once that rollback re-quarantines the volume. Do **not**
  wait for the restore to commit: a lease carrying a tombstone has already lost its provision, so
  the rollback arm is the only one `reconcileRestoring` can take. `reason="claim_unreadable"`
  instead means the retention store
  is unreadable, which silently stops the reaper (and blocks the close path too); fix the store
  first. See ENG-659.
- **A reaping record does not name the volumes it will destroy.** It records the abandoned
  footprint's *size* (its `Items`, which is what the admission projection sums); the finalizer
  re-derives the actual volume set on every sweep from the lease's namespace on disk
  (`fred-{lease}-*` and `fred-retained-{lease}-*`) intersected with the ownership table. So
  `GET /retentions` showing a reaping record with an empty volume list is **normal**, not a
  corrupt record — and the reclaim is driven by what is on disk now, not by a list captured
  when the record was written. A record whose lease has no volumes left on disk is dropped on
  the next sweep — but only when the volume root was readable: an absent or unreadable
  `volume_data_path` (an unmounted disk, most likely) is treated as uncertainty, so every
  reaping record is **kept** and `reap_skips_total{reason="claim_unreadable"}` rises. Remount
  the volume root and the next sweep proceeds. (ENG-676)
- **If the volume root is unmounted, fred refuses to act on the emptiness rather than
  believing it** (ENG-687). An unmount leaves the directory behind, so it enumerates as an
  empty node and nothing errors — which previously made every retention record look orphaned
  and led, over the following boots, to their data being destroyed as unclaimed. Now the
  daemon **fails to start** if `volume_data_path` is not on the filesystem `volume_filesystem`
  names (`is the volume mounted?` in the startup error), and a running daemon reports
  `volume data root ... is empty but now lives on a different filesystem` instead of reaping.
  Both mean the same thing: **check the mount first**, e.g. `findmnt /data`. Nothing is
  reclaimed and nothing is pruned until it is back.
- **A give-up under a degraded store still records the footprint.** It used to compute the
  record's volume list through the same ownership table it could not read, and on failure wrote
  no record at all — so the abandoned bytes were counted by nothing (no pool reservation, no
  retained record, no reaping record) and admission over-committed against real disk until an
  operator noticed. The record no longer carries a destroy plan, so there is nothing left for a
  degraded store to prevent it computing. **The bytes are still uncounted while the store
  stays broken** — the projection is recomputed by scanning the store — but the record is
  durable, so capacity accounting corrects itself on the first readable sweep with no
  operator action. Two residuals: if the store cannot be **written** either,
  `retention_leaked_total` plus the `MANUAL CLEANUP REQUIRED` log are the only record; and
  releasing the live reservation during that window is a general property of every
  live→retained hand-off, not of the give-up alone (an ordinary retaining close behaves the
  same way), tracked separately.
- **`reason="owner_claimed"` is a different hold, and there is nothing to unblock.** A
  tombstoned name belongs to a **live provision** (or another lease's retained record): the
  give-up deleted the provision while the lease was still ACTIVE on chain, the reconciler
  re-provisioned it, and a fresh volume now sits under the name the tombstone carries. The
  refusal is correct — that volume is a running tenant's data. Do **not** reclaim it, and do
  not go looking for a restore. The tombstone's other names still reap; the held one clears
  when that lease is next closed cleanly, so the record can legitimately sit `reaping` for as
  long as the lease lives. Expect `BackendRetentionVolumeStuckReaping` to fire on it; confirm
  the reason label before actioning. See ENG-658.
- **Second signal, and the one that names the volume:**
  `..._volume_destroy_refused_total{site="reaping",reason="claimed"}` counts the same refusals
  **per volume** rather than per sweep, and the accompanying WARN log carries `volume_id` plus
  the `owner_lease_uuid` that holds it — start there rather than diffing `/retentions` by hand.
  The same counter with `site="deprovision_destroy"` means a different path
  hit the same collision; the volume is safe in every case, and the owner named in the log is who
  must resolve it. A refusal whose owner is a **live provision** (rather than a restoring record)
  means a tombstone outlived its lease and the reconciler has since re-provisioned it — the
  tombstone's other names still reap, and the stale one clears when that lease is next closed
  cleanly. See ENG-658.
- **Reclaim (only after confirming no live/restoring lease references it).** Once the blocker is
  cleared the next sweep reclaims it automatically. To force it sooner, restart the backend
  (boot runs the reaping reconcile). If the volume is genuinely unrecoverable, remove it
  manually (`docker volume rm <name>` or `rm -rf <volume_data_path>/<name>`) — the next sweep
  then deletes the now-dangling tombstone (its destroy is an idempotent no-op).

---

## Partition collapse triage

`fred_docker_backend_retention_partition_collapsed_total{reason}` (counted per
close attempt; retries re-count) — a collapse **NEVER** blocks a close and
**NEVER** destroys data; it only files the record in the whole-tenant default
(`""`) bucket, exactly as if partitioning were off. It is only a signal that an
allowlisted (aggregator) tenant is emitting keys the backend can't use.

| `reason` | meaning | action |
|---|---|---|
| `invalid` | value fails the 1–64 char `[A-Za-z0-9._-]` rule (case-significant) | integrator-side key bug; share the charset rule |
| `divergent` | services in one manifest disagree on the value | integrator bug (mis-labeled sidecar); all services that carry the key must carry the SAME value |
| `no_input` | manifest unavailable at close (hydration failure) | cross-reference the `soft-delete: retained data will NOT be API-restorable` WARN; the stored partition is preserved on retries (the `PutActiveMerged` guard) |
| `over_limit` | tenant already at `max_partitions` distinct partition values | keys beyond the limit collapse; budgets are unaffected; raise `max_partitions` or expect default-bucket landing (a key rotation holds both generations until old records age out) |
| `store_error` | `retention.db` read failed during the partition bound | fail-open (safe); investigate store health; `..._retention_cap_check_failed_total{check="bound"}` fires alongside |

**Adoption / typo check.** Source configured and a tenant allowlisted, but
`fred_docker_backend_retention_partition_stamped_total` flat at 0 ⇒ the
configured key never matches what the integrator emits (a manifest typo or an
unpopulated label/env). No collapse fires in this case — the key is simply
absent, so verify the integrator is actually emitting the key the
`retention_partition_source` names.

---

## Budget lifecycle

Sizing, changing, and rolling back `retention_tenant_budgets` (the aggregator
allowlist) and the per-tenant caps (`max_retained_leases_per_tenant`,
`max_retained_disk_mb_per_tenant`). Every cap here is destructive at close time,
so measure before you set.

- **Measure before you budget.** Single-tenant (or provider-global) sizing: read
  the `fred_docker_backend_retained_leases` / `..._retained_volume_bytes` gauges.
  Per aggregator tenant: deploy a **generous** budget first, then read the
  startup `retention budget sanity` INFO log (emitted per budgeted tenant, with
  `active_count` / `active_mb` vs `budget_count` / `budget_mb`) and tighten from
  the observed holdings. An over-holdings budget instead logs `retention budget
  below tenant's current holdings` WARN with `over_count` / `over_disk` fields.
- **De-allowlist / shrink preflight.** Compare the tenant's current holdings
  (the sanity INFO/WARN, above) against the new budget **before** applying.
  Removing a `retention_tenant_budgets` entry drops the tenant to the default
  caps; if it holds more than the defaults allow, its next closes evict
  oldest-first (count, **batch-railed** at 32/close so it converges over several
  closes) or refuse-to-retain (disk). Neither blocks a close.
- **Rollback ordering.** A binary rolled back **below** the budgets release
  silently ignores the `retention_tenant_budgets` block (unknown config) and
  falls back to `max_retained_leases_per_tenant` / `max_retained_disk_mb_per_tenant`.
  If those are lower than a budgeted tenant's holdings, the old binary's next
  closes evict oldest-first (count cap) or refuse-to-retain the incoming close
  (disk cap). Raise `max_retained_leases_per_tenant` (and the
  per-tenant disk cap) to cover the **largest** budgeted tenant in the **same
  deploy** as any such binary rollback.
- **SKU additions re-trip the largest-SKU floor.** Every budget's
  `max_retained_disk_mb` (and the global/per-tenant disk caps) must be ≥ the
  largest stateful SKU's `disk_mb` — this is a startup `Validate` check. Adding a
  bigger stateful SKU raises that floor, so a now-undersized budget fails startup
  at the **next restart**; bump the budgets in the same change.
- **`max_partitions` shrink is non-retroactive.** Lowering it does not delete
  existing partitions; new distinct values beyond the limit collapse (`over_limit`)
  while existing partitions drain as their records age out. The
  `fred_docker_backend_retention_partitions` gauge may legitimately exceed the
  sum of budgeted `max_partitions` while draining — don't cry wolf.
- **Key rotation** holds both partition-value generations (old and new) until the old
  records age out, transiently consuming two partition slots — expect a brief
  `..._retention_partitions` bump and, if it crosses `max_partitions`, `over_limit`
  collapses on the new key until the old drains.
- **Restore consumes the grace slot.** Restoring a retained lease adopts its
  volume into the new lease and clears the retention record; a later re-close
  re-competes for the partition's disk sub-cap from scratch.

---

## Failed lease re-provisioning

When a container crashes or fails health checks, the lease moves to `failed`. The reconciler detects the chain ↔ provision mismatch and decides whether to re-provision based on `FailCount`.

**To investigate:**
1. `curl http://providerd/v1/leases/{uuid}/provision` (with auth) — returns full diagnostics including `last_error` (exit codes, OOM status, truncated logs).
2. `curl http://providerd/v1/leases/{uuid}/logs?tail=200` — full stdout/stderr.
3. Diagnostics persist for 7 days (configurable via `diagnostics_max_age`) even after the provision is gone.

**Common patterns:**
- `exit_code=137 oom_killed=true` → SKU memory too small, or app has a leak. Recommend a larger SKU or fix the app.
- `exit_code=1` early in startup → bad manifest configuration. Check the logs for stack traces.
- Health check failures → `health_check.start_period` may be too short. Update the manifest.

**On-chain callback messages are intentionally generic** (`container exited unexpectedly` / `internal error`) to prevent leaking secrets. Full diagnostics only flow through the authenticated API.

---

## bbolt database recovery

Fred uses bbolt (an embedded key-value store) for several persistent structures:

| Path | Purpose | Loss impact |
|---|---|---|
| `token_tracker_db_path` | Replay protection for tenant tokens | Brief replay window after restart; tokens are 30s anyway |
| `payload_store_db_path` | Tenant deployment payloads awaiting provisioning | Tenants must re-upload pending payloads |
| `placement_store_db_path` | Provider-bound durable confirmed and attempted lease→backend ownership, ordinary and rejected-positive (`untrusted_positive`) quarantine, immutable name→storage UUID history, and the topology-bound inventory baseline | Critical, non-derivable, and not hot-swappable. Normal startup refuses an absent, empty, unprepared, or differently provider-bound file and performs no creation or migration. Restore the exact database only while stopped; fresh initialization is only for a genuinely new provider with zero total chain lease history, never recovery after loss |
| `<docker>/callbacks.db` | Durable provision/restore intents with exact resource profiles (including Docker's pinned diskless scratch), exact restart/update/custom-domain maintenance intents, non-expiring Docker close intents, and the pending callback FIFO. Causal/close intents and exact operation/maintenance completions do not age out; legacy/lifecycle observations age out at `callback_max_age` | Accepted operation, replacement, and destructive-cleanup authority, immutable sizing, and queued callback evidence are not recreated. Loss can hide a substrate mutation, make a partial replacement or close indistinguishable from unexplained cohort loss, or strand a provider-side placement attempt; restore it with the matching release/retention stores and backend substrate |
| `<docker>/diagnostics.db` | Failure diagnostics (last_error, logs) | Older `failed` leases lose diagnostics; new failures still record after a stopped recreation. Open/create requires an unsymlinked, single-link regular file with exact mode `0600`, but diagnostics is not storage-identity authority and is not continuously re-attested |
| `<docker>/releases.db` | Per-lease immutable deployment topology/resource authority, tenant/provider identity, and current callback route: either typed operation lineage plus matching runtime authority, or a separately typed tokenless `LegacyRuntimeAuthority` frozen from a complete callback-bearing v0.13 cohort. An active callbackless pre-label cohort is rejected by stopped adoption because provider callback authority cannot be minted safely; only historical cleanup/close evidence remains readable, without zero-survivor or maintenance authority. The store also holds the exact generation checked when a present history is retired by close finalization. Encoded history is capped at 32 MiB per lease | Active release authority is not reconstructed from container survivors. Loss can erase the only identity and callback authority for a committed generation with zero survivors. A pending close remains resumable because its non-expiring callback-store row contains the complete cleanup snapshot and blocks newer operations; an absent release key is already retired. Treat the database and every backup as sensitive causal evidence |
| `<docker>/retention.db` | Retained-volume ownership, restore CAS generation, destination operation ID/callback pair/manifest/items, and immutable resource profiles | Losing or mismatching this file can orphan retained data or erase restore/finalizer lifecycle authority. Restore it with the matching callbacks/releases databases and substrate |

The database classes deliberately have different filesystem contracts. Backend
callback/release/retention journals, provider placement, and optional payload
storage require an unsymlinked, single-link regular file with exact mode `0600`;
the authority-bearing stores also re-attest their retained path/inode while
running. Diagnostics enforces the same shape and permissions only when opened or
created and remains recreatable. The token tracker is an ephemeral replay cache:
bbolt creates it with `0600`, but Fred does not identity-bind or continuously
re-attest an existing file. Stop the owning daemon before restoring or replacing
any class.

Release retention is both age- and capacity-bounded. `releases_max_age` defaults
to 90 days. Every write first preserves the index-latest row, the most recent
active row, and the newest legacy-migration cleanup row; it then removes expired
disposable audit rows before the oldest fresh disposable rows until the encoded
per-lease history fits 32 MiB. A capacity check runs before a provision,
restore, or legacy migration may mutate tenant substrate, and the write repeats
the same plan transactionally. If the protected authority alone cannot fit, the
operation is refused before mutation. Under extreme pressure a failed release
may omit its optional curated reason/message while retaining the terminal
`failed` state. `GET /releases/{lease_uuid}` has a separate 48 MiB response
budget because projection can add a default failure reason that was absent on
disk. Capacity compaction can therefore remove audit history before its age
expires; it never removes recovery or cleanup authority.

Every identity-bound backend store write has an explicit bbolt commit boundary.
An application rejection before `Commit` is rolled back and may be retried. Any
`Commit` error is outcome-unknown and permanently withdraws that process's store
authority. Identity drift or a terminal substrate-verification failure has the
same effect. The first cause is latched backend-wide before lifetime
cancellation; all sibling journal reads/writes and callback delivery return that
cause, so no independent bbolt file can advance after the lineage is only
partially trusted. A running docker-backend closes its listener, drains, and
exits status 1; its supervisor starts a new process whose `Start` must re-attest
and recover the retained evidence. A persistent fault crash-loops closed. Stop
mutation ingress, preserve the complete marker/store/substrate set, and reopen
and verify that exact set. Do not delete a pending intent, advance a callback
queue, or retry from an assumed rollback.

### A pending or corrupt Docker maintenance intent

`pending_callback_maintenance_intents` is the write-ahead owner for one exact
restart, update, or custom-domain replacement. Its store-assigned canonical
UUIDv4 `maintenance_id` must match the deploying/terminal Release and every
target container; the row also fences exact source and target release versions
and immutable digests. It is committed before the target Release or Docker
mutation and does not expire at `callback_max_age`.

The source and target carry one matching authority class. Current releases keep
their operation-scoped `ReleaseRuntimeAuthority`; upgraded v0.13 releases keep
their tokenless `LegacyRuntimeAuthority`. Mixed-class or principal-changing
targets are rejected. The UUIDv4 `maintenance_id` supplies exact causal identity
for either class. The first and every subsequent restart, update, or
custom-domain replacement of a v0.13 lineage stays legacy and tokenless;
`maintenance_id` is replacement WAL and cohort identity, not provider callback
authority. Only a later genuine provision or restore issued by providerd rotates
that lease to typed callback authority.

Before the target append, the row advances from a cancelable admission to an
append-started phase. Those phases use different opaque capabilities: capacity
refusal may cancel only the original admission, and every copy of that token is
stale once append-started commits. Seeing append-started with no target Release
after a crash is valid interrupted-operation evidence; recovery settles it as
failure rather than deleting or recreating the row by hand.

A pending row after a crash is expected recovery evidence, not an instruction to
rerun Compose. Startup and each docker-backend `reconcile_interval` tick classify
the exact target from `releases.db` and a fresh strict Docker inventory under the
lease command fence. `providerd` reconciliation does not trigger this pass:
the standard remote backend client's `RefreshState` is a no-op.

- an Active exact target settles maintenance success. If its runtime cohort is
  already definitively lost, the same callback-store transaction also appends a
  lifecycle Failed observation immediately after that Success;
- a complete Ready target cohort may activate the deploying target, then settle
  success;
- exact target absence records failed maintenance without changing the active
  source generation;
- a partial exact-ID cohort is removed only by its inspected immutable container
  IDs; any unreadable, divergent, or outcome-unknown evidence preserves the row
  and fails that recovery pass closed.

Container inspection is bounded and tri-state. A stopped/unhealthy container is
definitive unready evidence; an inspect transport error, timeout, or a workload
that has not completed its startup window is indeterminate and leaves the WAL
and target generation unsettled. Readiness classification itself is read-only.
Partial-target cleanup instead revalidates and removes exact immutable IDs one at
a time: a later sibling inspect/removal error can leave earlier confirmed IDs
already removed, but the WAL remains and the next pass resumes that idempotent
cleanup without following reusable names. Settlement normally replaces the
intent with one
non-coalescible maintenance delivery in `pending_callbacks_v2`. The committed-
but-runtime-lost case instead writes the ordered maintenance Success and
lifecycle Failed rows atomically, so a crash cannot expose only half the truth.
Both are exact maintenance-derived deliveries: after the Success head is
removed, the paired Failed row still fences a newer maintenance generation.
The maintenance callback travels over the lifecycle route but is neither a
replaceable lifecycle observation nor age-expirable. Recovery never waits
behind an in-flight HTTP drain for the lease: a busy callback FIFO preserves the
intent and defers the complete recovery sweep. If the Release is already
terminal and the intent remains, repair the store or storage-attestation failure
and let the next docker-backend recovery tick retry settlement. If the intent is
gone, inspect the per-lease FIFO before concluding the event was lost.
A queued exact maintenance completion also fences the next restart, update, or
custom-domain replacement for that lease. The command returns retryable
`409 Conflict` until callback delivery receives a synchronous 2xx and precisely
removes that row; unrelated leases remain available. This is intentional causal
backpressure: the lifecycle URL does not identify a maintenance generation, so
admitting a newer generation first could publish the older terminal result
after the newer start event. Do not delete the row to restore availability;
repair callback delivery and retry the command.
A close first settles an already-Active target as success; otherwise its own
admission transaction places failed maintenance ahead of the later deprovision
observation.

Do not delete the intent, change a MaintenanceID label, mark the newest Release
active by hand, or rerun the target Compose project. Stop the backend and
preserve `callbacks.db`, `releases.db`, both storage-identity markers, Docker
container metadata, and managed volumes as one snapshot before offline
inspection. A mismatched source/target digest, mixed MaintenanceID cohort, or
ambiguous removal needs proof-bearing repair or restoration of a matching
stopped snapshot; name similarity is not authority.

### A pending or corrupt Docker close intent

`pending_callback_close_intents` is a finalizer journal, not an ordinary callback
queue. Its row is committed before destructive work and intentionally survives
container absence, process restarts, `callback_max_age`, and transient cleanup
errors. Do not infer from zero containers that it is stale.

Recovery and live Deprovision share a backend-local recovery guard from the
container/intent snapshot through close resumption. This prevents a completed
close from racing a stale inventory publication without pausing unrelated
Provision or Restore commands.

For an ordinary full close, recovery reconstructs a conservative
`deprovisioning` projection and resource reservation from the row's immutable
per-SKU CPU/memory/durable-disk/scratch snapshot. A later SKU resize, removal,
or `container_tmpfs_size_mb` change therefore cannot
shrink the reservation for bytes or containers already owned by the close.
Retained/reaping rows created by this release carry the same snapshot. Older
retention rows remain readable and use the current SKU configuration; if an old
row references a removed SKU, startup fails closed before opening admission, so
restore that profile long enough to converge or repair the row offline from
authoritative deployment evidence. For a cleanup-only close, no tenant
projection is published: the fenced release still authorizes exact cleanup,
retention is forced off, and the row retries without an arbitrary give-up because
no safe tenant/reaping tombstone exists. In both cases the durable
`cleanup_attempts` field and the `durable close recovery remains pending` log
show progress.

A failed restore of a legacy retention row resolves the current source profile
once, proves actual usage fits, reapplies that exact physical quota, and persists
the same snapshot atomically with `restoring → active`. Any measurement, quota,
CAS, or accounting failure leaves the row `restoring` and its destination
allocation counted.

For a current restore row, the immutable destination items, manifest, profiles,
source generation, typed operation ID, and exact operation/lifecycle callback
pair are also
ownership and lifecycle authority. Provision and Restore
against that destination remain fenced until it converges. Before an
active destination Release exists, a failed restore can hand back only after
physical teardown/re-quarantine, exact source-quota proof, and failed-operation
settlement. An exact matching active Release is instead proof that restore
committed: keep the Release, settle a surviving intent as success, and delete
the source finalizer when a live Ready generation proves full handoff. With zero
survivors, recovery instead creates a conservative Failed destination, retains
its exact allocation, and keeps the source finalizer as durable tenant/provider
identity across restarts. After the exact restore intent settles, only a plain,
identity-preserving Restart is admitted; when it reaches Ready, reconciliation
consumes the row. Update and custom-domain redeploys remain fenced until then.
Close first persists a full close intent, then deletes the source finalizer
before teardown. Treat the missing cohort as post-commit runtime failure, not
permission to roll the data back.

Closing a restored destination with a lingering source finalizer first records
a complete close intent, then validates and CAS-deletes that source finalizer.
Validation or store failure returns before teardown; the close row is the sole
durable owner once handoff succeeds. Restore or repair `callbacks.db`,
`releases.db`, and `retention.db` as one evidence set, then retry the close.

If a row will not converge:

1. Stop the backend. Snapshot `callbacks.db`, `releases.db`, both storage-identity
   markers, the Docker data root, and `volume_data_path` as one evidence set.
2. With reviewed read-only bbolt tooling, inspect only that lease's JSON row and
   matching release history. Record the backend/storage identity, intent UUID,
   cleanup-attempt count, active-release version/digest, and immutable legacy
   rollback container IDs. Treat both callback URLs as sensitive causal evidence
   and keep them out of logs and tickets.
3. Reconcile the row with the exact Docker IDs, retention record, and volume
   ownership table. A missing release key is an idempotent retired state because
   the close row carries the cleanup snapshot and blocks newer operations. A
   different surviving release is a conflict, not permission to delete it.
4. Prefer restoring the matching stopped-process snapshot or fixing the
   substrate/store fault and restarting. Normal recovery resumes teardown before
   ordinary exact-cohort validation.

Do not hand-edit or delete the close row to make health/startup green. The
required completion order is release retirement under its exact fence, atomic
lifecycle-outbox enqueue plus close-row removal, then volatile projection
deletion. Skipping any step can either erase the only retry owner or lose the
terminal lifecycle observation.

If the close row and projection are gone but Fred has not observed the terminal
lifecycle event, teardown is already finalized; inspect the lease's
`pending_callbacks_v2` FIFO and `fred_docker_backend_callback_delivery_total`
instead of recreating a close. Resolution only sends a non-blocking wake to the
tracked replay loop, and the 30-second periodic scan is its fallback, so an HTTP
outage delays observation without reopening substrate cleanup.

### Placement runtime authority was withdrawn

`placement_store_db_path` is not hot-swappable. `providerd` retains a descriptor
for the exact regular-file inode it opened and re-attests the configured pathname
before and after authority-bearing operations. Any inability to prove that
identity and confidentiality—including an unlink, rename, symlink, additional
hard link, permission change away from exact `0600`, or replacement inode—emits
the exact log message `placement runtime authority withdrawn` and permanently
disables placement authority in that process. Restoring the pathname does not
clear the latch. A bbolt `Commit`
error has the same fail-stop result because the mutation may or may not be
visible; retrying against an unknown result can consume or duplicate authority.

Treat either case as an evidence-preservation incident:

1. Fence tenant and chain-event mutation ingress. Leave backend callback/outbox
   evidence intact and stop any automation that replaces or rotates the file.
2. On a pathname mismatch, keep `providerd` running only long enough to preserve
   **both** the inode it still has open and the file currently named by
   `placement_store_db_path`. Use storage/incident tooling that can copy the
   retained `/proc/<providerd-pid>/fd` file without altering either source, and
   record hashes and filesystem metadata. Stopping first can release and lose an
   unlinked inode that is the best surviving authority.
3. Stop `providerd`. Do not restart it merely because the configured path now
   exists. Run `placement-repair -config /etc/fred/config.yaml -classify` against
   each preserved candidate while it is offline; inspect affected lease rows as
   well when a write's commit outcome is unknown.
4. Select or reconstruct the exact provider-bound authority only from that
   evidence and a known-good stopped-process or atomic filesystem snapshot. Keep
   every rejected candidate. Restart once, against the chosen file at the
   configured path, then require clean `placement_store` and
   `placement_inventory` checks before reopening ingress.

Never copy, overwrite, unlink, rename, or restore the live pathname underneath
`providerd`. A backup taken while it runs must be an atomic filesystem snapshot;
restore is always a stopped-process operation. A stopped restore may naturally
publish a new inode: the next strict open validates and binds that file before
using it.

Mutating `placement-preflight` and `placement-repair` runs bind the physical
device/inode of the requested backup parent before collecting remote evidence.
They publish through that retained descriptor with
`renameat2(RENAME_NOREPLACE)`, then retain and re-attest the exact backup inode
and parent, SHA-256 bytes, length, exact `0600` mode, and single-link status
before/after mutation and before a success verdict. If the parent is renamed,
unmounted, or recreated—or the entry is replaced, modified in place, chmodded,
or hard-linked—stop and preserve both database paths. A
`BACKUP PUBLISHED` means the destination inode crossed the atomic no-overwrite
publication boundary and no mutation committed; a later backup verification
may have failed, so inspect it read-only before treating it as a rollback
image. `PREPARED:`/`COMMITTED:` means mutation committed before a later
verification failed; `OUTCOME UNKNOWN` means the commit result cannot be
inferred. Never retry any of those classifications with the same backup path.

### A logically corrupt placement row

If `providerd` startup reports `lease "<key>" has uninterpretable durable
placement`, the bbolt file opened successfully but that exact placement value
cannot prove which backend may own, retain, or still be attempting the lease.
The startup error quotes the exact bucket key and the decode reason; raw value
bytes are deliberately not logged. This is different from a structurally
unreadable bbolt file, and repeated restarts cannot repair it.

Do not bypass the topology check, silently discard the row, or immediately
replace the whole placement database. Any of those can erase the only evidence
of a delayed backend call or retained tenant data. Recover it as follows:

1. Stop `providerd` and take a byte-for-byte backup of the database before
   inspecting or changing it.
2. Record the quoted key and decode reason. Check that lease on chain and query
   `/provisions` and `/retentions` on every configured backend plus every
   historical backend that could have owned it. `placement-repair -inspect`
   always emits `untrusted_positive`: `true` means the candidate set came from
   positive membership in a rejected inventory response, not an authoritative
   owner. A sole such candidate can self-resolve only from a later complete,
   identity-valid matching positive; every other conflict remains operator-only.
3. Prefer restoring a known-good stopped-process backup. If no backup exists,
   preserve the row and escalate for operator repair unless the collected
   evidence explicitly proves that it represents no owner, retained data, or
   unresolved attempt.
4. Only with that proof, remove the one quoted key using reviewed offline bbolt
   tooling; never edit the live database. Restart with the unchanged backend
   identities and require a complete inventory projection before reopening
   tenant lifecycle ingress.

Moving the entire placement database aside is a last resort that also loses
attempts, conflict candidates, backend identity history, and the durable admission
baseline. Inventory may refresh positive observations only inside an existing
prepared authority; it cannot authorize reconstruction of a lost database or
its absence-invisible safety facts.

### A structurally unreadable bbolt file

**If a bbolt file is structurally corrupted** (file lock errors, bbolt panic on
open, or known bad magic):

1. **Stop the service**.
2. **Move the file aside** rather than deleting (`mv X.db X.db.broken`) so you can inspect it later if needed.
3. **Restore the file according to its authority class before restarting.** Some caches may be recreated, but release/retention/callback state should be restored whenever possible.
4. **For `placement_store_db_path`, restore the exact provider-bound database before starting providerd.** Normal startup never creates, initializes, or migrates a missing/unprepared file. Current chain/backend silence cannot recover a lost authority: if the provider has any chain lease history, including terminal history, restore the database. The explicit fresh initializer is only for a genuinely new provider with zero total lease history, and additionally requires an independently supplied exact fleet roster, complete identity-consistent empty provision and retention inventories from every configured backend, and continuous fencing of providerd plus tenant/chain mutation ingress. Each backend stays running so the tool can authenticate its inventories, but must be empty and drained with no in-flight mutation and an idle callback/outbox queue. Its print-time acknowledgement binds the target parent's physical device/inode; do not rename, unmount, or recreate that parent between print and initialize. Publication is descriptor-relative and no-overwrite. Follow [Initializing a genuinely fresh placement authority](DEPLOYMENT.md#initializing-a-genuinely-fresh-placement-authority) for that first-boot workflow. The payload store may start empty (tenants re-upload), and the token tracker may start empty (acceptable, see above); restore each backend callback store whenever any exact delivery could remain.

Never run two `providerd` or `docker-backend` instances against the same bbolt files — bbolt enforces single-writer with a file lock and the second process will fail to start. If it doesn't fail, you have data corruption coming.

---

## Restart and update operations

`POST /v1/leases/{uuid}/restart` and `POST /v1/leases/{uuid}/update` are tenant-initiated, asynchronous. Each follows a stop-rename → recreate → verify pattern with rollback on failure.

**On success**: a `success` callback is sent and the lease's status returns to `ready`.

**On failure**: rollback restores the previous containers. Two outcomes:
- **`ReplaceRecovered`**: rollback succeeded, lease back to `ready` (with a `failed` callback indicating the operation failed but service is restored).
- **`ReplaceFailed`**: both the operation and rollback failed, lease is `failed`. This is rare and indicates a deeper problem (Docker daemon issue, disk full).

**To diagnose a failed update:**
1. `GET /v1/leases/{uuid}/releases` — the failed release will have `status: "failed"` and an `error` field.
2. `GET /v1/leases/{uuid}/logs` — old containers' logs are gone after rollback; the diagnostics store retains the failure logs for 7 days.

### Restore operations

`POST /v1/leases/{lease_uuid}/restore` (on providerd; `POST /restore` on the docker-backend) re-deploys a lease onto its **retained** (soft-deleted) volumes — the v0.5.0 headline feature. It runs through the same replace machinery as restart/update, so the rollback semantics above apply, with one extra synchronous *adopt prelude* up front that renames the `fred-retained-*` volumes back to their canonical names before the worker spawns.

**Provider-side admission and recovery.** Before contacting a backend, providerd
acquires ordered lifecycle claims for both source and target, re-reads the target
as tenant/provider-owned `PENDING`, and atomically reserves the exact confirmed
source while writing an operation-scoped durable attempt for the absent target.
Concurrent lifecycle work sharing either lease returns 409 before dispatch. The
source reservation is process-local and lasts only through the synchronous call;
the target attempt survives restart.

Acceptance, idempotent recognition of an exact same-operation redelivery, or a matching exact-operation callback confirms the target. A
validated `already_provisioned` response proves only that some generation
exists, so the new target attempt remains until its exact callback or an
upgraded inventory report carrying that exact paired generation arrives. Each
later sweep reconstructs the durable source, operation ID, and immutable target
request snapshot and redelivers only to the attempted backend. A contract-conforming synchronous domain refusal
trusted under the configured backend transport clears it. A timeout, transport error, panic, generic
5xx, malformed error
envelope, or unvalidated 503 is ambiguous, so providerd releases the source
reservation but retains the target attempt. An immediate same-target retry then
normally returns 409. A positive report from the attempted backend confirms it
only with that exact paired typed generation;
a positive report from another backend expands durable conflict quarantine.
Inventory absence, complete or partial, never disproves or clears the attempt,
because the original restore may commit after the list response. Do not delete
the attempt merely to make the retry pass: let exact redelivery, its callback,
paired-generation inventory, or a contract-conforming refusal settle it; use
explicit operator proof and repair only when none can do so.

The tenant event stream publishes `restarting` immediately before backend
dispatch so an inline `ready`/`failed` callback cannot be followed by a stale
start event. If the synchronous result definitively refuses the restore
(including a coded capacity refusal), Fred follows that hint with `failed`.
The compensating event uses the neutral diagnostic `restore did not start`, since
the refusal class also includes local pre-dispatch conditions such as an open
circuit breaker.

Ambiguous outcomes intentionally keep `restarting`; REST and backend inventory
remain authoritative because WebSocket delivery is best-effort and lossy.

**Restoring onto a different SKU tier.** A restore's new lease may target a
different SKU than the source — only the item *shape* (service names + quantities)
must match; the disk (`disk_mb`) tier may differ. A **promote** (same-or-larger
`disk_mb`) is admitted only when its aggregate growth above the retained
footprint fits disk capacity, then applies the larger cap. A **demote** (smaller
`disk_mb`) is allowed only if the retained volume's *measured* data fits the new
tier — the backend runs `checkDemoteFit` before adopting. A demote that does not
fit is refused: the docker-backend returns HTTP 422 with body
`{"code":"demote_exceeds_tier"}`, which fred-api forwards to the tenant as a 422
with the message `retained data exceeds the requested smaller tier`. (This is
distinct from a *bare* 422 with no code — `ErrNotRetained`, no retained data —
which fred-api maps to 404.) `fred_docker_backend_restore_demote_refused_total{backend,reason}`
(`reason ∈ {measured_exceeds, unmeasurable_read_error, unmeasurable_backend,
ephemeral_tier}`) counts these refusals; like other synchronous-prelude failures
they are **not** counted by `restore_total`.

If a restore fails after changing quota, rollback tears down the destination,
re-quarantines the volumes, proves their usage fits the immutable source caps,
and reapplies those caps. Before actor acceptance, it then settles the exact
failed operation, pre-counts the retained footprint, commits the exact
source-generation CAS/backfill, and only afterward releases destination
allocations. After actor acceptance the worker deliberately parks at
`restoring`: the lease actor must first persist the Failed callback, and the
periodic sweep then performs that same make-before-break handback. A measurement,
quota-application, callback-store, CAS, or accounting uncertainty keeps the row
`restoring` and the live allocation counted. Investigate
`unable to restore source volume quotas` and the retention-sweep error, repair
the storage/store dependency, and let reconciliation retry; never delete the
source finalizer.

The source finalizer always excludes Provision and Restore of the destination.
Every maintenance path returns invalid-state before commit. After an exact active
destination Release proves commit and the exact restore intent settles, a plain
Restart may repair the destination; Update and custom-domain redeploys remain
fenced until that Restart reaches Ready and reconciliation consumes the
finalizer. The Release is a commit marker, not rollback debris: retain it and
settle any matching intent as success. At cold start,
exact Release plus zero survivors recovers a conservative Failed destination
with its allocation still held and preserves the source finalizer as
tenant/provider identity. Close instead persists a complete close intent before
deleting the row and starting teardown. Never delete the Release or return its
data to the source merely because the live cohort is gone.

**Success-rate signal.** `fred_docker_backend_restore_total{outcome}` (`outcome ∈ {success, failure}`) is the docker-backend's own restore success rate, mirroring `provisions_total`. Both outcome series are pre-initialized to 0, so a failure ratio reads 0 (not no-data) before the first restore. **Worker-scoped caveat:** a restore that fails in the synchronous adopt prelude (claim/rename/route/ack) before the worker spawns returns a synchronous error to the caller and is counted by **neither** outcome bucket — exactly as `provisions_total` omits synchronous provision failures. Such failures surface to the tenant as the restore HTTP status; providerd's `fred_provisioner_provisioning_total{operation="restore"}` counts the async callback/timeout outcomes, not these synchronous backend errors, so it does not backfill the gap.

**Latency.** `fred_docker_backend_restore_duration_seconds` measures the async re-deploy worker span (compose up + verify startup, success only) and **excludes** the synchronous adopt prelude. Its buckets mirror `provision_duration_seconds` so the two can be overlaid for the restore-vs-fresh-provision question — but the overlay is approximate: `provision_duration_seconds` is observed on both success and failure and carries no outcome label, so the comparison is robust at the median but tail-biased. Read it as indicative.

**Slow-phase diagnosis.** `fred_docker_backend_replace_phase_duration_seconds{operation="restore",phase}` breaks the re-deploy into per-phase timings (`adopt`, `image_setup`, `volume_setup`, `compose_up`, `verify_startup`). When a restore is slow, query this to see which phase dominates — `adopt` (volume rename), `compose_up`, or `verify_startup` are the usual suspects.

---

## Withdrawal and credit monitoring

Fees are pulled into the provider account by `WithdrawScheduler` on a paid-withdraw cadence of `withdraw_interval` (default 1h). The scheduler also wakes on a separate credit-check cadence, `credit_check_interval` (default `0s` = coupled to `withdraw_interval`); each wake estimates which tenants will deplete before the next paid withdrawal. When `credit_check_interval < withdraw_interval` a **withdraw-cadence guard** is active (`fred_withdraw_guard_active` = 1): credit checks run at the faster cadence, but the paid provider-wide withdrawal stays rate-limited to once per `withdraw_interval` since the last full drain, so faster credit polling no longer forces an extra paid withdrawal every tick (ENG-524). Closed leases settle in full regardless, so a deferred paid withdrawal is a cash-timing choice, not lost fees. `credit_check_interval` must be `≤ withdraw_interval` when set.

When a tenant's credit reads empty, the scheduler does **not** close its leases on that single read. Because closure is destructive (`MsgCloseLease` → volume soft-delete + 90-day grace), a transient stale read — e.g. fred's chain node briefly lagging a tenant top-up — would otherwise wrongfully soft-delete a paying tenant's data. Instead the empty balance must **persist** for `credit_check_zero_grace_period` (default `5m`, the equivalent of Kubernetes' `tolerationSeconds`): the first empty read starts the window and schedules an early re-check, any non-zero read clears it, and closure fires only if the balance is still empty when the window elapses. Deferrals are surfaced by `fred_withdraw_credit_check_zero_deferred_total` (an aggregate counter with no tenant label) — a sustained or rising rate points at a chronically lagging chain node or too-short a grace period; correlate with the lease-close transaction count (`fred_chain_transactions_total{type="close"}`) to see how many deferrals still closed. Lower the knob to reclaim unpaid leases faster; raise it to tolerate more chain-node lag before soft-deleting data (ENG-591).

**Symptoms of failure:**
- `fred_chain_transactions_total{type="withdraw",outcome="error"}` rising
- Provider balance not increasing despite active leases
- `fred_withdraw_incomplete_cycles_total` rising → the provider was not fully drained in a cycle because the cursor hit `max_withdraw_iterations` (default 100); raise it for the active-lease count (deferred to the next cycle, not lost)
- Cross-provider auto-close events not triggering withdrawals → check the watcher is running and seeded

**Common causes:**
- Insufficient gas (see [Out-of-gas tuning](#out-of-gas-tuning))
- Authz authorization expired or revoked when using `sub_signer_count > 0` — the primary key keeps working but sub-signers fail
- Sub-signer balance below `sub_signer_min_balance` and top-up failing — check `sub_signer_top_up_amount` and primary balance

> **Not a fault:** paid withdrawals appearing less frequent than credit checks is expected when the cadence guard is active — `fred_withdraw_guard_active` = 1 and `fred_withdraw_skipped_by_guard_total` incrementing is by-design rate-limiting, not an error.

### Signer pool demoted to single signer

`ProviderSignerPoolDemoted` fires on `fred_signer_pool_lane_count < sub_signer_count`. Since ENG-688 that has exactly two causes, and they need different responses:

1. **Sub-signer keys were missing from the keyring at boot.** Grep the journal for `sub-signer key not found` and `fewer sub-signer keys than requested`. This does **not** self-heal: restore the keyring entries (see the sub-signer runbook in `manifest-deploy`) and restart `providerd`.
2. **The authz grants were positively determined missing and could not be created.** Look for `authz grants are missing and could not be created, falling back to single signer`. This does **not** self-heal either: demotion empties the pool, and the sub-signer maintenance loop is gated on the pool having sub-signers, so it never starts. Fix the underlying cause — usually the provider account being too low on fees to broadcast `MsgGrant` — then restart `providerd`, which re-creates the grants on the next boot.

A *third* state is not this alert. If the grant **queries** failed, `providerd` deliberately keeps its sub-signers — a failed read says nothing about grants that are created without expiration, and the chain re-checks the authorization on every `MsgExec` anyway. Lane count stays put and this alert never fires; the signal is `fred_signer_grant_check_total{outcome="error"}` climbing, with `could not verify authz grants at startup` in the journal. That state is self-healing on the next `sub_signer_fund_check_interval` tick and needs no restart. Investigate only if the error outcome persists across several sweeps, which points at the chain endpoint rather than at fred.

While the pool is demoted, `fred_signer_balance{role="sub_signer"}` series stop existing, so `SubSignerLowBalance` and `SubSignerTopUpStalled` are blind — they compare `< threshold` over series that are absent. Do not read their silence as health while this alert is firing.

---

## Graceful shutdown

`providerd` and `docker-backend` both handle SIGINT and SIGTERM. The shutdown order is documented in [ARCHITECTURE.md](ARCHITECTURE.md#graceful-shutdown).

`providerd`'s `shutdown_timeout` (default 30s) bounds only the provider process's
admission, operation, HTTP, scheduler, and manager drain. It does not configure
docker-backend.

Docker-backend first gives its HTTP server 30s to stop accepting and drain
requests, then cancels backend work and waits a separate, fixed 90s for all
backend-owned goroutines. If a worker still has not returned, `Stop` leaves the
Docker client and bbolt stores open rather than closing dependencies underneath
an ambiguous mutation, logs `docker backend workers did not drain before
shutdown deadline`, and the binary exits non-zero. Let the service supervisor
restart it so startup recovery can re-attest Docker and durable state. There is
no production knob to lengthen this 90s fail-closed bound.

`fred_docker_backend_lease_terminal_event_dropped_total` should remain zero, but
it is a bug signal rather than a shutdown-tuning signal. Capture the shutdown
error and a goroutine dump if it rises or docker-backend exits non-zero during a
routine restart.

Callback timing is a separate reverse-direction budget. Bundled backends share
one 2m15s deadline across all three delivery attempts and their 0s/1s/5s
backoff; providerd gives each admitted callback up to 2m to apply. Operation,
maintenance, and lifecycle completion paths atomically queue their durable fact,
send a non-blocking coalescing wake, and return without HTTP in the lease actor,
API handler, or startup recovery. The tracked replay loop alone owns delivery.
A slow callback can therefore hold one replay worker and that lease's FIFO lock
for up to 2m15s, while actors and unrelated leases continue. `backends[].timeout`
applies to Fred-to-backend requests and does not control this callback deadline.
Backend shutdown cancels the shared callback context before starting its 90s
worker drain. A lost/already-pending wake is harmless because the 30s sweep reads
the same durable outbox row.

Upgrade the backend binaries one at a time when their wire protocol is backward-compatible,
then stop and replace the single `providerd` process. Do not run active-active or
overlapping rolling `providerd` instances for one provider/backend fleet: bbolt is
single-writer, and separate databases would split the process-local lifecycle-operation
registry without a cross-process coordinator. See [DEPLOYMENT.md](DEPLOYMENT.md#upgrades)
for the stop/start and rollback procedure.

---

## Capacity planning

Per the benchmarks in [PERFORMANCE.md](PERFORMANCE.md), Fred itself sustains 56,000+ events/sec, far above realistic chain event rates. The bottleneck is always the backend (Docker pull, container start, health check) and the chain (block time).

**Practical sizing:**
- Chain ack throughput is the typical limit. With `sub_signer_count = N`, you get up to `N × 50` acks per block (~5s blocks, chain-dependent ≈ 600 leases/min).
- Per docker-backend host, image pull and container start dominate provision latency (10s–60s for typical images).
- Budget memory: ~50MB baseline + ~1KB per active lease (operation Registry entries). bbolt stores grow with payload sizes and history retention.
- Budget Docker disk from effective profiles: durable `disk_mb` for stateful
  instances, or one pinned `container_tmpfs_size_mb` scratch allowance for every
  diskless instance. The latter is charged conservatively even when its image
  ultimately needs no managed writable-path volume.

---

## Logging

All logs are structured JSON via `slog`. Key fields:

| Field | Meaning |
|---|---|
| `lease_uuid` | Always set for lease-related operations |
| `tenant` | Set for tenant API calls and provisions |
| `backend` | Set for backend operations |
| `error` | Set on failures; full Go error chain |

State-machine transitions in the docker backend are surfaced via the `fred_docker_backend_lease_sm_transitions_total{from,to,event}` metric rather than as a log field; query the metric for transition history.

Set `log_level: debug` in `config.yaml` (or the docker-backend's own `log_level`) to see chain query traces, Watermill message routing, and per-actor inbox depth — but be aware debug-level under load can be very chatty.

---

## Getting help

- Logs first: `journalctl -u providerd -n 500` (or your equivalent) plus the docker-backend logs.
- Metrics second: the `/metrics` endpoints + Prometheus history.
- Reproduction: the `mock-backend` lets you reproduce provisioner-side issues without involving Docker.
- File an issue with the metric, log excerpt, and Fred version.
