# Operations Runbook

This document covers day-to-day operation of a Fred deployment: health checks, alert interpretation, common failure modes, recovery procedures, and tuning.

For deployment and initial setup see [DEPLOYMENT.md](DEPLOYMENT.md). For metric definitions see [ARCHITECTURE.md](ARCHITECTURE.md#metrics-prometheus). Sample Grafana dashboards live in the `manifest-deploy` repository.

The deployed and production-validated execution envelope is `docker-backend` on
XFS. K3s is a non-functional scaffold; Docker's Btrfs and ZFS implementations
are experimental and not deployed. Their recovery notes remain here to document
implemented behavior, not to grant production support.

---

## Health checks

Both `providerd` and `docker-backend` expose `GET /health`. **`providerd /health` is a liveness contract: no dependency verdict makes it 503, and no dependency can make it slow enough for a prober to give up either.** Point load-balancer health checks at it. `providerd` additionally exposes `GET /readyz` — the same body, but the verdict reaches the status code. **Do not point a load balancer at `/readyz`.**

| Endpoint | Probes | 503 when |
|---|---|---|
| `providerd /health` | Chain gRPC and all backends start concurrently under one three-second remote budget. Mandatory placement-store and inventory checks, plus optional token/payload-store validation, remain synchronous; their filesystem/lock waits can exceed that budget. Stage histograms distinguish these costs | Never from a health verdict; the outer request timeout can still fail a stalled request |
| `providerd /readyz` | Same probes, same budget | A configured bbolt store is unreadable, or the placement inventory does not admit fresh lease side effects: no durable inventory baseline matches the configured backend topology, an interrupted inventory sweep awaits its reporters, a fenced backend may hold a lease with no placement row, or placement authority was withdrawn (verdict `unhealthy`) |
| `docker-backend /health` | Storage identity verified (by the route's middleware, then again inside `Backend.Health`), Docker daemon reachable, no resource-accounting hold (late containers of a permanent closed/failed receipt), the callback, diagnostics, release, and retention bbolt stores open and carry their expected buckets, and the launch journal readable. Callback health validates delivery rows, durable operation and maintenance intents, non-expiring close intents, and that no lease simultaneously owns incompatible intent classes | Any of them is unhealthy |

All bbolt probes are read-only opens: they prove the database is present and structurally intact, **not** that a write would succeed. A full or read-only filesystem passes them.

Both `providerd` endpoints return the same JSON body, whose `status` is one of three verdicts:

| `status` | Meaning | `/health` | `/readyz` |
|---|---|---|---|
| `healthy` | Every configured probe passed | 200 | 200 |
| `degraded` | A **remote, shared** dependency is impaired — the chain, or one or more backends. Existing workloads keep serving. After inventory bootstrap, exact callbacks and safely evidenced reconciliation continue, and the reconciler may place genuinely new recordless `PENDING` work only on nodes that answered both inventories. Work pinned to a silent owner, recordless `ACTIVE` work, attempts, and conflicts remain deferred. A chain outage halts reconciliation entirely and fails every lease-resolving tenant call. Either way providerd still accepts backend callbacks, which is why it must stay in rotation | 200 | 200 |
| `unhealthy` | A **local, process-owned** bbolt store is unreadable, or the placement inventory does not admit fresh lease side effects (no durable baseline matches the configured backend topology, an interrupted sweep awaits its reporters, or a fenced backend may hold a lease with no placement row). Restart a broken store; let an incomplete bootstrap inventory retry | 200 | 503 |

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

To gate on readiness while a known backend is down, which
`fred_reconciler_sweep_complete` cannot do because it stays 0 fleet-wide while
any backend is silent, require `fred_reconciler_sweep_projection_committed == 1`
and `fred_reconciler_backend_inventory_answered{backend} == 1` for every backend
that must be up. The commit gauge is reset before each sweep's seal rewrites the
answered gauges and set only after its projection commits, so inside providerd a
1 means the answered gauges belong to that committed sweep. A scrape reads the
gauges one at a time and can straddle the next sweep's seal, so require the
condition on two consecutive scrapes.

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

**The remote probe budget and total response latency differ.** Chain Ping and
all backend probes start together under the same three-second deadline. A slow
chain therefore cannot consume the backends' entire opportunity to answer.
Local filesystem and bbolt validation is synchronous; that deadline cannot
interrupt a database lock or disk wait. The default five-second proxy health
timeout leaves room for local work, but is not a guarantee against stalled
storage. Diagnose the stage before changing a timeout or readiness threshold.

`fred_health_check_duration_seconds{check,backend}` separates `chain`, each
configured `backend`, `token_tracker`, `placement_store`, `placement_inventory`
and `payload_store`. The `backend` label is empty for local and chain checks.
On Docker, `fred_docker_backend_health_check_duration_seconds{check}` separates
`storage_identity`, `docker_ping`, `resource_accounting`, `callback_store`,
`diagnostics_store`, `release_store`, `retention_store` and `launch_journal`.
An early failure prevents later stages from running, so missing samples do not
mean those checks succeeded.

For request-level diagnosis, provider-to-backend health probes send
`X-Fred-Health-Probe`, a fresh canonical UUIDv4. Docker accepts exactly one
canonical value or generates a local one, and echoes it in the health response.
The value is diagnostic only and grants no identity or admission authority.
Docker logs every `health probe completed`: fast successful completions at INFO,
and failures or requests taking at least one second at WARN. The provider client
logs only slow or failed completions at WARN. A fast server success can therefore
be matched to a later client timeout. Join `probe_id` across the records; `side`,
`duration`, `status` and `outcome` describe the completed request. Docker's
`stages` include identity admission and only checks that actually ran. A request
can end canceled after earlier stages completed healthy; those stage outcomes
are preserved. A request still blocked inside synchronous I/O has no completion
log yet; a missing record alone cannot distinguish this from logging loss or a
request that never arrived. Probe IDs are never
metric labels. Cumulative histogram means cannot identify the stage responsible
for a particular timeout without this request-level evidence.

Callback-store health validation reads a fresh identity-bound transaction and
cooperatively stops between bounded records when the request is canceled. It
does not reuse a cached healthy verdict or leave a background traversal running.
Already validated closed-head payloads are reused within their strict decode,
preserving semantic, encoded-size and original-envelope digest checks. Database
transaction admission, filesystem waits and the current record's validation
remain synchronous; cancellation is not a hard I/O interruption. Later health
stages are not started after cancellation. The remote three-second budget and
readiness requirements are unchanged.

`fred_docker_backend_storage_identity_check_duration_seconds{check}` measures
production identity-verifier invocations, including HTTP admission and callback
delivery. Outer prevalidation or terminal rejection can finish before invoking
the verifier and contributes no sample. Its stages are `total`, `lock_wait`,
`substrate`, `daemon_info` and `stores`. These timings
overlap: `total` includes the other stages, `substrate` includes `daemon_info`,
and the health `storage_identity` check includes its own identity verification.
Do not sum them. Compare an idle baseline with the failing window and correlate
backend roundtrip time with these stages. A backend deadline alone does not
identify chain delay, transport latency, lock contention or retained-row cost.

Docker HTTP health intentionally performs two identity checks: middleware
qualifies the response's storage-identity header, and `Backend.Health` retains
its standalone integrity contract. Both contribute to the common identity
verifier histogram; only the latter is inside the health `storage_identity`
stage. Do not attribute their combined timing to that stage alone.

Callback receipt validation traverses each head and its histories once within
the immutable health read transaction. Compensation, effect-debt and queue
validation remain separate. Retention health uses the committed schema's strict decoder.
Both still scan all current authoritative rows on every probe; neither caches a
health verdict. Permanent callback history therefore still contributes to cost.
Use the callback and retention benchmarks for controlled before/after
measurements, then verify the unchanged readiness gates on the deployment pin.
`fred_chain_health_probe_panics_total` and
`fred_background_cleanup_panics_total{component=~"docker_reconciliation|docker_network_reclamation"}`
identify contained programming failures; any increase needs investigation even
when later probes or recovery passes succeed.

---

## Common alerts and what they mean

| Signal | Likely cause | First step |
|---|---|---|
| `fred_backend_circuit_breaker_state{backend="X"} == 2` (open) | Backend X has been unhealthy long enough to trip the breaker | `curl backendX/health`, check backend logs |
| `fred_backend_healthy{backend="X"} == 0 unless on(backend) fred_backend_fenced == 1` for >1 min | Backend health probe failing | Same as above. Note this no longer affects the tenant API's availability — the provider reports `degraded` and keeps serving. A fenced backend is unhealthy by configuration (`/health` names it `backend is fenced`); gate every per-backend availability alert on `fred_backend_fenced` |
| `fred_backend_fenced{backend="X"} == 1` | The operator fenced backend X (see SECURITY.md, "Containing a compromised backend"). Its leases wait, and a lost admission baseline cannot be rebuilt while it is fenced | Informational while the incident is handled. A fence is a holding state: end it by lifting it after replacing the key, or by retiring the backend |
| `fred_placement_unprojected_fenced_reporter{backend="X"} == 1` | Backend X reported a positive in an interrupted sweep whose recovery cleared while X was fenced, so a lease with no placement row may live on X. No lease without a placement row is admitted, so new leases wait; `/readyz` reports `placement inventory waits on a fenced backend` | Page. It clears when X answers both inventories again, after the fence is lifted. Otherwise retire X, which sets `recordless_unproven` and resumes admission after one sweep in which every surviving backend answers. Removing X from the topology is refused while it is 1. Before a later fence, run `placement-repair -classify` on the stopped database: `pending_inventory_sweep_id` marks an interrupted sweep, and `fence_restart_would_record` names the backends a restart with them fenced would record (SECURITY.md, "Containing a compromised backend") |
| `fred_docker_backend_volume_launches_pending > 0` beyond the expected launch window | Outstanding Docker launch receipts; a transient nonzero value is normal while launches run | Confirm recent successful backend health sampling, then correlate pending requests with backend logs. The gauge holds its last sample when health fails and does not count image-helper receipts. Follow [Unsettled Docker effects](#unsettled-docker-effects) for persistent unknown requests; never delete a receipt to clear the gauge |
| `increase(fred_docker_backend_image_preparation_refusals_total{reason="import_allocation"}[15m]) > 0` | A measured image footprint exceeded twice its verification allowance before Docker import; one tenant can encounter this without a fleet-wide failure rate | Open a sizing ticket on any increase and correlate the immutable source, `import_bytes` and `limit_bytes` in the WARN. Review temporary disk, parser limits and the existing image-size setting before raising it; updates fail before replacing workloads |
| `increase(fred_docker_backend_image_allocation_pressure_total[15m]) > 0` | A successful new-image preparation used over 80% of its configured import ceiling | Plan growth headroom using the measured footprint and source in the WARN. Exact saved-budget recovery is excluded because its content-derived allowance is deliberately close to usage. This is a capacity-planning signal, not a standalone page |
| `increase(fred_docker_backend_image_registry_requests_total{status="429"}[15m]) > 0` | A registry refused docker-backend's requests for quota. On Docker Hub this is the anonymous per-IP manifest-pull allowance, so new leases and updates that need a manifest GET (an image this backend has not verified since its start, or a moved tag) fail with `ImagePullFailed` until the registry's window resets. Every `GET`/`HEAD` series is initialized at zero, so the first refusal after a scrape is visible | Follow [Registry rate limits](#registry-rate-limits): check the remaining allowance with a non-metered HEAD from the backend host and compare `endpoint="manifest",method="GET"` with `fred_docker_backend_image_tag_resolutions_total`. Warning severity; page only if tenant rejections are sustained |
| `increase(fred_docker_backend_image_import_total{outcome="deadline"}[15m]) > 0` | An import owner reached its dispatch ceiling; Docker may still be unwinding | Correlate with pending import bytes and daemon logs. Before planned stops, fence new mutations, quiesce work and wait for pending bytes to reach zero. Owner cancellation is not completion proof |
| `fred_docker_backend_image_import_pending_bytes > 0` beyond the normal import window | The gauge includes both live owned imports and allocation whose completion is unknown. A persistent value after the backend becomes idle can be durable import debt; restarting does not clear it | Correlate imports, Docker response failures and shutdown logs. Alert with a site-specific `for` duration longer than a normal import; investigate sustained debt using [Recovering outstanding image import allocation](#recovering-outstanding-image-import-allocation). Never clear the debit while the runtime can still allocate |
| `increase(fred_docker_backend_image_gc_total{outcome="inhibited"}[15m]) > 0` together with sustained image-filesystem disk pressure | Incomplete pin authority or unresolved inspection evidence prevents safe deletion. This counter is diagnostic, not a standalone paging condition: pre-upgrade retained generations can legitimately lack pins for their remaining retention period | Check legacy pin-backfill warnings, retained rows and inspection receipts. Unpinned retained generations remain conservative until restored or safely reaped; the default grace is 90 days, plus the reaper interval, and unresolved reaping can extend it. Do not page on this expected upgrade condition while disk headroom is healthy. Docker inventory failures increment `outcome="error"`; ordinary live admissions increment `outcome="busy"`. Preserve authoritative evidence; import debt alone does not inhibit collection |
| `increase(fred_maintenance_admission_refusals_total{reason=~"count\|bytes"}[5m]) > 0` | New restart/update admission reached the provider pending-journal count/byte cap. Existing commands can still replay and settle | Correlate pending phase/oldest-age gauges with backend completion and callback health. Restore stalled completion rather than deleting pending rows. `reserved_count` and `reserved_bytes` are caller backpressure (`429`) while preserving room for a tenant without pending work; exclude those reasons from provider exhaustion alerts |
| Backend X reports `callback store unhealthy` | `callbacks.db` is missing a delivery/intent bucket, contains malformed durable evidence, or gives one lease simultaneous operation, maintenance, or close rows. A terminal Succeeded/Failed operation row is history rather than active mutation authority; authorized successor admission retires it atomically instead of leaving simultaneous rows. Current deliveries live below a lease-identifying nested bucket. Operation identity/snapshot fields are immutable; maintenance advances through typed pre-append and append-started phases and then binds one exact target fence; close preserves its immutable snapshot while durably advancing a monotonic execution generation immediately before physical work. Every change uses an exact digest-bearing claim. Replay/TTL never silently deletes poison data, terminal operation rows remain after delivery until an authorized successor, and causal intents, close intents, and exact completions never age out | Stop that backend, take a copy of `callbacks.db` with the matching release store, storage markers, containers, and volumes. Inspect or restore the named lease offline (or the complete file when a root bucket is missing). Prefer exact repair/restore over deleting the database; wholesale deletion can lose accepted work, terminal decisions, replacement identity, destructive-cleanup authority, and pending completions. Keep the node out of new placement until `/health` is clean |
| Backend latches after `post-mutation storage verification`, refuses startup with `recover interrupted operations`, or `fred_*_backend_callback_store_errors_total` increases | A raw mutation returned without a usable postcheck, callback persistence/store access failed on an instrumented path, operation-intent startup recovery failed, or another authoritative journal/substrate proof reached a terminal identity or outcome-unknown failure. The first cause is sticky for the backend lifetime: callback, release, and retention journals (where present), substrate mutation admission, and callback delivery all refuse through the same latch. A running docker-backend publishes that first cause to its main loop, closes the listener, drains workers, and exits status 1 so the supervisor must launch a fresh `Start`; a persistent fault therefore crash-loops closed instead of serving. An XFS volume deletion that cannot finish is not such a fault: it is held for that one volume and the backend keeps serving (see [Held volume deletions](#held-volume-deletions)). A valid but semantically indeterminate maintenance row is different: it need not make `/health` fail or increment this counter; use the Docker reconciliation signal below. A close intent already owns destruction, so recovery resumes it from its immutable snapshot before ordinary exact-cohort validation and reports retry errors in the lease-scoped close log below | Fence mutation ingress and preserve `callbacks.db`, `releases.db`, `retention.db` where present, the storage-identity marker pair, and the substrate as one evidence set. Do not treat one still-readable sibling journal or a queued callback as permission to continue; the shared latch intentionally withdrew the entire lineage. Let the supervised restart retry only after repairing the Docker/retention/SKU/store inconsistency or restoring the matching stopped-process snapshot. Restart only against that same set. Never delete an intent, finalizer, release fence, retained data, or callback evidence merely to make readiness green |
| `fred_docker_backend_oldest_unheld_close_intent_age_seconds` remains above the normal close window (it leaves out closes waiting only on held volume deletions, see [Held volume deletions](#held-volume-deletions)), `fred_docker_backend_pending_close_intents` remains non-zero, or `durable close recovery remains pending` repeats for one lease | Docker admitted deprovision before teardown, then a transient container/volume/release/accounting/outbox failure prevented finalization. The aggregate gauges deliberately omit lease labels; the log's lease UUID and durable `execution_generation` identify the exact attempted run and survive restart. A full close keeps a conservative projection and capacity reservation; a cleanup-only close may have no tenant-visible projection but remains the sole non-expiring retry owner. An unresolved launch receipt can also prevent terminal close even when current container and volume inventories are empty | Correlate the recovery log's lease UUID with nearby teardown, retention, release-store, and callback-store errors. Restore the failed dependency and let the next docker-backend recovery tick independently classify the Started generation before authorizing another run. If offline inspection is required, stop the backend and inspect that lease's close-tagged head in `callback_lease_mutation_heads` together with the exact `releases.db` history and substrate; callback URLs contain causal identifiers, so do not paste raw row contents into tickets. Never delete the row merely because Docker reports zero containers. If the exact lease retains an unknown launch request, follow [Unsettled Docker effects](#unsettled-docker-effects). A timeout, restart or empty inventory cannot retire that receipt; preserve the close intent and launch evidence until exact completion or stopped-backend operator fencing and repair establishes quiescence |
| `fred_docker_backend_lease_mutation_uuid_slots / clamp_min(fred_docker_backend_lease_mutation_uuid_slot_limit, 1) > 0.8` | This backend storage lineage has consumed more than 80% of its permanent lease-UUID budget. The numerator is monotonic by design: operation/maintenance settlement and close do not reclaim a UUID because an arbitrarily late substrate effect or retry must remain fenced | Follow [Permanent callback UUID capacity](#permanent-callback-uuid-capacity). Forecast the durable UUID burn rate and ship a reviewed limit increase well before exhaustion; new nodes can absorb never-before-seen leases meanwhile. Never delete slots, closed receipts, or `callbacks.db` to reduce the gauge |
| `fred_docker_backend_callback_receipt_reservations / clamp_min(fred_docker_backend_callback_receipt_reservation_limit, 1) > 0.8` | This backend has consumed more than 80% of its shared durable operation/maintenance receipt budget. Unlike UUID slots, successful close reclaims these reservations after installing the stronger closed-lease fence | Follow [Permanent callback UUID capacity](#permanent-callback-uuid-capacity). Forecast operation/maintenance churn and close convergence. Never delete history or `callbacks.db`; add capacity or ship a reviewed ceiling increase before admission reaches its definitive-refusal boundary |
| `increase(fred_docker_backend_reconciliation_total{outcome="error"}[15m]) > 0` or `fred_docker_backend_reconciliation_last_success_timestamp_seconds` is stale beyond the expected Docker `reconcile_interval` | Docker's periodic recovery pass failed at a global storage, journal, transport, or unclassified observation boundary, or a durable restore finalizer could not complete source handback. Explicit lease-local maintenance conflicts and expected readiness waits preserve their intent and reservation while siblings continue; they use `fred_docker_backend_maintenance_recovery_deferred_total` and `fred_docker_backend_maintenance_readiness_pending_total{branch}` instead. Semantic recovery errors can leave `/health` green and `fred_docker_backend_callback_store_errors_total` unchanged. A global failure during cold start exits before periodic metrics begin, and the timestamp reads 0 until the first periodic pass succeeds, one `reconcile_interval` after start | Inspect the backend's `reconciliation failed` log and its wrapped lease/error (a panicking pass logs `docker_reconciliation cleanup panic` instead and also counts as `error`). `reconcile restoring operations:` identifies restore-finalizer debt (including a lease-local quota failure). This intentionally increments the pass error and freezes last-success until handback succeeds; independent finalizers still run. It is not evidence that all recovery has stopped. For lease-local deferrals, correlate the separate maintenance warning and counter with the exact workload; a committed target may remain pending while its healthcheck stays `starting`. For a persistent mismatch, stop the backend and preserve the exact `callbacks.db`, `releases.db`, marker pair, Docker metadata, and volumes before following [A pending or corrupt Docker maintenance intent](#a-pending-or-corrupt-docker-maintenance-intent). Do not delete the WAL or use `/health` success as permission to bypass it |
| `fred_health_check_healthy{check="chain"} == 0` | providerd cannot reach the chain gRPC endpoint (or it answered slower than the health probe's budget). Every tenant endpoint that resolves a lease fails, **and reconciliation stops entirely** — a sweep reads the complete paginated `PENDING` and `ACTIVE` inventories concurrently with independent 30s contexts, then returns an error if either failed because everything downstream treats "absent from chain" as ground truth. The bound also prevents startup reconciliation from indefinitely delaying the subscriber and schedulers. Callback HTTP ingress remains reachable, but exact application that needs the chain returns 503; the originating backend keeps that lease's FIFO head durable and periodically retries without blocking other leases | Check the node and `grpc_endpoint`. This is the ENG-522 trigger, and it is now a metric rather than a liveness 503 — providerd deliberately stays in rotation, because dropping out would sever even the retryable callback path without restoring anything |
| `fred_health_check_healthy{check=~"token_tracker\|payload_store"} == 0` | That bbolt store could not be opened, or is missing the buckets it should have. The payload check additionally fails permanently for that process when its retained parent/path/inode, exact `0600` mode, or single-link proof drifts. `/readyz` is 503 and `/health` reports `unhealthy` while still answering 200 | Check the store's `*_db_path` — that it exists, is the intended unsymlinked file, and is readable. For payload authority drift, fence payload-dependent mutation, preserve the file and any unexpected alias, stop `providerd`, restore exact `0600` mode and one link from understood evidence, then restart. A load balancer cannot fix this. The token probe is read-only, so a full or read-only filesystem can pass it and still break writes: check disk space too even when this gauge reads 1 |
| `fred_health_check_healthy{check="placement_store"} == 0`, or log `placement runtime authority withdrawn` | The provider can no longer prove that the configured placement pathname names the exact single-link regular-file inode it opened with mode `0600`, or a bbolt `Commit` returned an outcome-unknown error. The failure is process-sticky: all later placement authority reads and writes fail closed even if the pathname or permissions are restored | Fence tenant and chain-event mutation ingress immediately. If the pathname or file metadata changed, **do not restart or overwrite either file before preserving both the still-open inode and the current pathname/aliases**; the running process may hold the best surviving copy. Follow [Placement runtime authority was withdrawn](#placement-runtime-authority-was-withdrawn), then stop, classify the preserved authority offline, and restart only with the exact private provider-bound file chosen from that evidence |
| `fred_health_check_healthy{check="placement_inventory"} == 0` | The placement inventory does not admit fresh lease side effects. The check's message names the cause: `placement inventory recovery pending` (an interrupted sweep waits for its reporters; `fred_placement_inventory_recovery_pending` is 1), `placement inventory waits on a fenced backend` (see the `fred_placement_unprojected_fenced_reporter` row), or `placement inventory not ready` (no complete inventory baseline for the configured backend identities, or placement authority was withdrawn). `/readyz` is 503, but `/health` and the callback route remain available | For a missing baseline or pending recovery, restore every configured backend's `/provisions` and `/retentions` responses and let reconciliation commit the projection. If `placement_store` is also 0, follow its row. Do not admit new tenant lifecycle traffic until the check becomes 1 |
| `increase(fred_placement_write_failures_total[5m]) > 0` | providerd could not durably record or verify a placement-store synchronization point. A write-ahead failure blocks a new provision, re-provision, or restore before the backend is contacted; a later confirm/cleanup failure preserves conservative authority rather than silently claiming no backend was contacted. An error before bbolt `Commit` is definitely uncommitted and may be retried; a `Commit` error is outcome-unknown and permanently withdraws this process's placement authority | Check the `failed to ... placement`, `placement runtime authority withdrawn`, or placement-sync verification log, then check free space, filesystem permissions and I/O errors at `placement_store_db_path`. If authority was **not** withdrawn, restore access and let reconciliation retry safe work. If it was withdrawn, do not rely on a retry or restart: preserve and classify the exact file as described below. Page on every increase — do not alert on the raw counter being non-zero, because counters remain non-zero after recovery |
| `fred_backend_health_probe_panics_total > 0` | Bug — a backend health probe panicked. The probe is an HTTP call that should return an error, not panic. It is recovered (the probe runs on its own goroutine, where net/http's recovery does not reach it) and counts as unhealthy, so nothing crashes | Check logs for `backend health probe panicked` and the stack trace, then file an issue |
| `fred_health_check_healthy` series absent, or frozen while nothing changes | Nothing is polling `/health`. Both this gauge and `fred_backend_healthy` are written **only** from inside the health handler, so with no prober they latch at their last value instead of going absent | Confirm the load balancer's health check on `/health` still exists and its interval (30s in the reference deployment). A latched 1 masks a real outage |
| `fred_backend_insufficient_resources_total{backend="X",verdict="coded_refusal"}` rising | Backend X is returning contract-conforming capacity refusals; the matching attempt is normally clearable | Reduce SKU sizes, add backend hosts, or check `docker-backend /stats` |
| `fred_backend_insufficient_resources_total{backend="X",verdict="ambiguous"}` rising | Backend X or an intermediary is returning legacy/code-less/unknown-code capacity 503s; Fred retains the write-ahead attempt | Fix the responder to emit the declared coded envelope, then settle the retained attempt from its exact callback or an upgraded inventory report carrying the same paired typed generation; otherwise perform explicit operator repair. Malformed envelopes appear in `fred_backend_malformed_error_body_total` instead |
| `fred_backend_malformed_error_body_total` rising on a backend | That backend answers a 4xx, or a 503 whose `code` fred reads (capacity, lifecycle-pending, read capacity), with a body that is not the declared `{"error": ...}` envelope, so its tenants get a generic message instead of a diagnostic | Find the raw body in the `backend returned a malformed error body` log line and fix the backend to emit the envelope (BACKEND_GUIDE.md). If the backend looks correct, suspect an intermediary answering on its behalf |
| `fred_provisioner_callback_timeouts_total` rising | Backend accepted provision but never called back | Backend logs; verify `callback_base_url` is reachable from backend; check HMAC secret match |
| `increase(fred_provisioner_callback_settlement_claim_wait_timeouts_total[5m]) > 0` | A callback waited 30 seconds while another callback or the timeout checker retained the same operation ID's terminal-settlement claim. The actor may be blocked on a slow chain call, or a bug may have leaked its claim | Find `failed to apply callback` logs whose error contains `timed out waiting for callback settlement claim`; the error names the lease. Correlate concurrent callback, timeout, acknowledge/reject, and downstream chain-latency logs for that lease. A deprovision-owned claim returns immediately and cannot increment this counter. If no actor completes and the counter repeats, restart providerd to clear the process-local claim, then file an issue with the logs |
| `increase(fred_provisioner_callback_deprovision_owned_success_total[5m]) > 0` | A backend completed provisioning while close/deprovision owned the same operation ID. Fred consumed the success without acknowledging the closing lease and continued teardown | This path logs nothing of its own: correlate the lease's `received provision callback` log with `status=success` against close/deprovision logs for the same lease. A one-off race is safe; sustained increases suggest slow provisioning or unusually fast lease closure |
| `fred_provisioner_lifecycle_callback_outcomes_total` | Every authenticated lifecycle callback receives exactly one terminal `outcome`: `applied`, `dropped`, or `retryable`. `verdict` is bounded to `authorized`, `legacy`, `teardown_only`, `retired`, `invalid`, `missing`, `stale`, `unusable`, or defensive `unknown`; `status` is exactly one of the closed callback protocol values `success`, `failed`, or `deprovisioned` (anything else is rejected with 400 before application). Summing across `outcome` is the lifecycle-specific received count. The older `fred_api_non_in_flight_callbacks_total` deliberately remains a received-at-ingress compatibility counter and increments even for a later drop | `verdict="legacy"` is expected for v0.13 placements during one-upgrade adoption. `teardown_only` means its matching confirmed placement authority is gone: runtime observations are dropped and only the exact terminal deprovision observation can consume the residual authority. Occasional `outcome="dropped",verdict=~"stale\|retired"` is expected after a lost 2xx or lifecycle rotation. Sustained `missing`/`unusable` drops mean the backend is presenting an authority Fred cannot use; correlate the authorization log with placement inventory. After restoring an older `placements.db`, `placement-repair -classify` counts repair candidates (`counts.unusable_adoption_candidates`) and `-list` marks each with `adoption_candidate`; see DEPLOYMENT.md, "Adopting a lifecycle generation after a restore". Any `outcome="retryable"` means Fred returned non-2xx and the backend must retain its FIFO head; check placement-store health and callback application errors |
| `fred_provisioner_lifecycle_event_sink_panics_total{event=~"provision_starting\|restore_restarting\|restore_refused\|callback"} > 0` | Fred recovered a panic from a best-effort pre-dispatch, restore-refusal, or post-settlement callback event sink. Recovery deliberately lets backend dispatch or callback settlement continue; `event="callback"` means the durable callback is still acknowledged so it cannot wedge that lease's FIFO | Correlate the provision, restore, or callback log by lease and operation fingerprint, use `event` to identify the affected sink, and file a bug with the panic stack; this should never occur |
| `fred_provisioner_backend_invocation_panics_total > 0` | A backend implementation panicked behind providerd's execution boundary. Mutation calls remain durably ambiguous because the side effect may have crossed the boundary before the panic | Use the bounded `operation` label and panic stack to identify the path; repair the backend and let callback/inventory recovery resolve the retained attempt rather than clearing placement state manually |
| `fred_provisioner_ack_batch_fee_gas_errors_total` rising | Out-of-gas on lease acknowledgment txs | See [Out-of-gas tuning](#out-of-gas-tuning) |
| `fred_chain_signer_oog_retries_total{result="exhausted"}` rising | Same; the broadcast retry loop hit `max_gas_limit` | Same as above |
| `increase(fred_provisioner_ack_batcher_lane_restarts_total[15m]) > 0` | Bug — an ack-batcher lane's flush panicked. The panic was recovered, the requests in that batch failed back to their callers, and the lane was respawned after one batch interval. A lane that keeps panicking stays crash-looping at that pace; `lane` names it | Check providerd logs for `ack batcher lane panic — recovering to keep fred alive` and its stack trace, and correlate `fred_background_goroutine_panics_total{component="ack_batcher"}`. File an issue with the stack |
| `increase(fred_docker_backend_die_event_dropped_total[15m]) > 0` sustained | Container-death observations are repeatedly refused because their exact provision generation became stale, restore recovery reserved the actor key, the backend is stopping, or the current actor cannot accept another message; for `source="event_loop"`, also deaths the event loop could not dispatch (its queue was full, or storage identity could not be re-verified). Deaths positively owned by an active actor close are excluded. A one-off stale-generation refusal is safe because reconciliation uses current state, but a death it re-detects is attributed `unknown` and never counts toward the terminal budget (ENG-799); repeated refusal for one unchanged generation can indicate a wedged actor | Correlate `source` with the lease-scoped dropped-event warning and recovery/replacement logs. If the same current generation repeats without recovery or replacement churn, see [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_stuck_seconds > 900` | Some actor's `handle()` has been running for >15 min | See [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_panics_total > 0` | Bug — actor handler panicked | Check logs for stack trace, file an issue |
| `fred_docker_backend_maintenance_readiness_pending_total` rising with long-lived pending maintenance | Exact maintenance remains pending while startup age or health readiness is uncertain; retries count again | Correlate the once-per-intent/branch warning with container health. A committed target is not rolled back solely because readiness stays uncertain |
| `fred_docker_backend_maintenance_expired_total` rising | A provider sent restarts or updates older than their lease's retained history, typically after a placement database was restored from an older copy and replayed its pending commands. If the provider's clock also stepped back, its new commands expire until the clock passes the newest stamp it issued before the restore | Each command was refused before mutation and settles for the tenant as `410 maintenance_expired`. Check for a recent placement database restore and the provider host's clock. Nothing needs repair |
| `fred_docker_backend_maintenance_receipts_unverifiable_total` rising | A failed maintenance receipt's target release row is gone or divergent, typically compacted away by the lease's later large updates. The receipt no longer authorizes cleanup, so a late container from that failed generation would be kept rather than removed | Inspect the `failed maintenance receipt target cannot be verified` log for the lease and maintenance ID. Nothing is blocked; if a stray container for that maintenance ID appears, remove it only after confirming it is not the lease's active cohort |
| `fred_docker_backend_maintenance_recovery_deferred_total` rising | A lease-local observation conflict retains its intent while sibling recovery proceeds | Inspect the lease-scoped recovery warning and substrate identity; do not delete the intent or release its reserved capacity manually |
| `fred_docker_backend_network_reclamation_total{outcome=~"error\|list_error\|budget_exhausted"}` sustained | The separate bounded network worker cannot drain its backlog in a pass | Check Docker errors, idle network count and address-pool headroom. Only `outcome="removed"` counts actual removals; active, connected or busy tenants are safe deferrals. This worker does not spend the operation-recovery budget |
| `fred_docker_backend_lease_terminal_event_dropped_total` rising under clean shutdown | Real data loss pattern | The release store / provision struct may be out of sync with Docker — reconciler will re-detect on next cycle, but root-cause the wedged actor |
| `fred_provisioner_reconciler_panics_total > 0` | Bug — a reconciler worker panicked. A `stage="placement_cleanup"` panic preserves that exact durable candidate while the bounded healthy lanes continue | File an issue with the stack trace. The next sweep retries preserved work; do not delete placement evidence |
| `fred_chain_health_probe_panics_total > 0` | The chain client panicked during a providerd health probe; that probe is reported unhealthy | Correlate `chain health probe panicked` in providerd logs, verify subsequent chain probes recover, and file an issue. A healthy later response does not erase the panic counter |
| `fred_background_cleanup_panics_total > 0` | Bug in a cleanup loop. **Emitted by every fred binary**, so read `job` alongside `component`: `token` is providerd; `callback`, `diagnostics` and `releases` are a backend; `retention`, `docker_reconciliation`, `docker_network_reclamation`, `docker_projid_audit`, `docker_seccomp_census` and `docker_volume_delete_hold` are docker-backend components | Inspect the recovered panic stack, verify the next iteration makes progress, and file an issue. Go to the journal of the host the `job`/`server` labels name, not to providerd by default |
| `fred_background_goroutine_panics_total{component="callback_replay"} > 0` | A bundled backend recovered a panic while replaying one lease's durable callback FIFO. That lease remains queued for a later pass; the bounded worker pool continues with unrelated leases | Check the backend log for `panic while replaying callback outbox` and its lease UUID/stack, then file an issue. Do not delete `callbacks.db`; replay is the recovery owner |
| `fred_background_goroutine_panics_total{component=~"timeout_checker_(sweep\|candidate)"} > 0` | The timeout checker recovered a bug at its periodic boundary. Exact operation/placement evidence remains durable; candidate isolation lets unrelated timeout settlements continue, and the next cadence retries preserved work | Check the providerd stack trace and file an issue. Do not repair the placement DB merely to clear the alert |
| `fred_api_rate_limit_rejections_total{limiter="tenant"}` spike | Specific tenant exceeded their bucket | Expected if a tenant is bursting; sustained spikes indicate a misbehaving client |
| `fred_payload_leases_awaiting > 0` for >5 min | Tenant created lease with `meta_hash` but never uploaded payload | Tenant-side issue; the lease will eventually expire |
| `fred_reconciler_last_success_timestamp_seconds` stalled | Reconciler is stuck, panicking, running with incomplete inventory, or failing an external read/durable projection — only a complete successful projection advances this. A fenced backend is never asked, so while any backend is fenced every sweep is incomplete and this never advances: gate the alert with `unless on() max(fred_backend_fenced) == 1` and watch `fred_reconciler_sweep_projection_committed` plus `fred_reconciler_backend_inventory_answered` for the unfenced backends instead | Check `fred_reconciler_sweep_complete` first: 0 means a sweep is in progress or the latest sweep did not complete a durable full-fleet projection, not that the durable topology baseline was revoked. Then inspect `fred_reconciler_backend_fetch_total{outcome!="ok"}`, chain health, placement-write logs, and `fred_reconciler_runs_total{outcome="error"}` |
| `fred_reconciler_backend_fetch_total{outcome!~"ok\|fenced"}` sustained for one backend across ≥3 sweeps (~6 min at a 2m interval) | That backend is unreachable from providerd. Its owner-affine leases are deferred and inventory silence changes no attempt or conflict. With an established baseline, safe callbacks/status/cleanup can continue and the reconciler may use nodes that answered both inventories for genuinely new recordless `PENDING` work | [Backend unreachable during reconciliation](#backend-unreachable-during-reconciliation) |
| `fred_reconciler_sweep_complete == 0` sustained (gate with `unless on() max(fred_backend_fenced) == 1`: a fence keeps it 0 for as long as it lasts) | The last fleet observation was incomplete. The gauge becomes 0 before every sweep and remains there while it is in progress or after any chain read, provision/retention inventory, or durable projection failure. It is observability, not a fleet-wide authority switch: a matching durable baseline may remain healthy, while the reconciler narrows recordless `PENDING` admission to the exact answering-node scope and defers lease-specific unsafe work | Inspect backend fetch outcomes, chain health, reconciliation errors, placement-write failures, and deferred lease logs. Do not infer that all mutations are blocked or that absence on a silent node is evidence |
| `fred_reconciler_backend_inventory_total{outcome!~"authoritative\|fenced"}` rising for one backend across ≥3 sweeps | That backend's inventory is not usable as authority: unreachable (`unanswered`), half-answered (`provisions_only`/`retentions_only`), an identity or refresh failure (`untrusted`), or some ambiguous leases (`partial`, which still counts as answered). `fenced` means the operator fenced it, so it was not asked | Triage as for `backend_fetch_total`. For `untrusted`, check the backend's storage identity and pin before anything else; never repoint its address to make it answer |
| `increase(fred_api_callback_auth_failures_total[15m]) > 0` or `increase(fred_docker_backend_request_auth_failures_total[15m]) > 0` | Signatures are being refused. `mismatch` usually means a misordered key rotation or a wrong key; `expired`/`future` mean clock skew between providerd and the backend; `unknown_storage` means a callback named a storage identity providerd has no key for; `fenced` is a fenced backend still calling back, which is expected until its host is stopped | During a rotation, compare `-print-hmac-key-ids` on both sides and restore the previous configuration. Otherwise check clocks (NTP) and that both sides hold the key pair from the same secret mapping |
| `sum(increase(fred_placement_snapshots_total{outcome="success"}[2h])) == 0` (use a window of at least twice `placement_snapshot_interval`) | No online snapshot of `placements.db` and `payloads.db` was published in two intervals. The series exist only when `placement_snapshot_dir` is set | Look at the other outcomes and the `placement snapshot` WARN logs. `insufficient_space`: free space on the snapshot filesystem, which needs twice the databases' size plus 256 MiB. Each attempt first removes staged files left by a crash (`.fred-snapshot-tmp-*`), so that space is already counted. `error`: the log names the step. A copy past its 30s deadline means a slow snapshot disk. A re-read that does not match the streamed digest means the snapshot disk returned different bytes. A copy that matches its digest but fails bbolt's consistency check means the live database itself is damaged: stop providerd and run `placement-repair -classify`. A capture failure means a live store refused the read: check both stores' authority (see [Placement runtime authority was withdrawn](#placement-runtime-authority-was-withdrawn)) |
| `increase(fred_placement_snapshot_prune_failures_total[1h]) > 0` | Pruning kept a snapshot file it could not prove safe to delete, or failed to delete one. `not_owned` or `live_database`: something other than providerd put a file under a snapshot name, such as a symlink or a hard link to a live database. `inspect`, `remove`, `sync`: an I/O or permission problem. `list`: the directory cannot be listed or holds at least 8192 entries | Kept files are never lost; the directory grows until the cause is fixed. Move the offending file out of the snapshot directory, or fix its permissions so it is a regular file owned by the providerd user. Keep the directory for snapshots only |
| `fred_api_callback_previous_key_configured == 1` for longer than a rotation takes | A rotation was not finished: the old key is still accepted on that backend's callbacks | Finish step 4 of the rolling rotation (DEPLOYMENT.md, "Rotating a backend's HMAC key") once `fred_api_callback_signature_key_total{slot="previous"}` has stopped rising |
| `fred_provisioner_reconciler_lost_leases_total{outcome="error"}` sustained | The reconciler cannot end a lease that was lost with a retired backend: the chain transaction or the lease re-read keeps failing | Check the `failed to end lost lease on chain` log and the signer and chain health. Each failure is a lease error, so the sweep reports `partial` and `fred_reconciler_last_success_timestamp_seconds` stops advancing until the close succeeds; each sweep retries. `closed`/`rejected` rise as each lost lease is ended after a retirement (see DEPLOYMENT.md, "Retiring a backend whose storage is lost") |
| `fred_provisioner_reconciler_deferred_leases_total` rising while `fred_reconciler_sweep_complete == 1` | Every backend answered, but one or more leases still lacked a safe, current lifecycle decision: ownership was ambiguous, placement was unusable or unresolved, or an operation/placement change crossed the inventory boundary. A low rate during provisioning, restore, or other lease churn is expected | Correlate the lease-level `reconcile: deferring lease` logs with operation Registry and placement changes. Investigate a sustained rate or the same lease repeating without concurrent work; it can indicate a stuck unresolved record or unusually slow sweeps |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown"}` rising | Fred is declining to clean up state for a lease **the chain has no record of**, and will decline again every sweep — this one does not self-heal. Either providerd is pointed at the wrong or a reset chain (check the `pass` label spread: fleet-wide means config, one lease means a phantom), or a provision exists that no lease ever created | Confirm the chain endpoint and provider UUID first. If the chain is right, the resource is genuinely unowned: deprovision it by hand once you have confirmed the tenant is gone |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown_state"}` rising | The chain reports a lease state this providerd build cannot classify — either the zero `UNSPECIFIED`, or a state added to the ledger after this binary shipped. Cleanup is withheld, which is data-safe but permanent for those leases | **Upgrade fred** to a build whose `manifest-ledger` pin knows the new state. Unlike `chain_unknown` the chain is fine and providerd is behind it, so do not go looking for a phantom provision |
| `fred_reconciler_cleanup_skips_total{reason="chain_error"}` sustained | The per-candidate chain re-check is failing, so cleanup is paused (data-safe). Usually the same cause as any other chain-query failure, or a lookup that blew its 10s budget — that budget exists so a stalled query cannot wedge the sweep, and it reports as an error rather than as evidence | Check `fred_chain_query_duration_seconds{query="get_lease"}` and the node's health; self-heals |
| `fred_reconciler_cleanup_skips_total{reason="chain_live"}` rising steadily | The sweep's lease snapshot is often stale by the time cleanup runs — expected at a low rate, but a high one means sweeps are slow relative to lease churn | Compare `fred_reconciler_duration_seconds` against the reconcile interval; no action if the rate is low |
| `fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}` steady for an unreachable backend | Expected: that backend's placement records are never pruned from silence. Removing its name while records refer to it is rejected at startup | [Removing, renaming or pausing a backend](#removing-renaming-or-pausing-a-backend) |
| `fred_reconciler_cleanup_skips_total{pass="placement",reason="attempt_pending"}` sustained for the same lease | A write-ahead backend effect is still causally unresolved, so Fred preserves its placement evidence and refuses destructive cleanup. A low rate during ordinary provision/restore is expected; each live sweep redelivers the exact typed operation, persisted callback pair, immutable tenant/provider/item snapshot, and payload fingerprint or restore source only to its pinned backend. Accepted/idempotent responses promote it, transport-minted contract refusals clear it, and ambiguity retains it. Public Go error sentinels are diagnostic only: a custom/legacy backend's non-nil return cannot manufacture refusal evidence. A terminal chain lease uses exact deprovision instead; every distinct attempted/confirmed backend must succeed before conservative affinity is promoted | Correlate the attempted backend and operation fingerprint with that backend's durable intent/callback queue and inventory. Restore an unavailable backend, callback path, or payload database so exact recovery can settle. Missing payload data is retriable and never downgrades the request or terminates the live lease. If the backend definitively created nothing and cannot return a conforming refusal, follow the explicit placement-repair procedure; never clear the row from inventory silence alone |
| `fred_watermill_poisoned_messages_total > 0` | A handler exhausted retries on a message. Known close inventory/lifecycle waits and locally proven circuit refusals normally transfer to the bounded deferred-close scheduler instead | Read the topic and reason in the poison log. Queue saturation or shutdown can still return a close event error; reconciliation remains the durable recovery path |
| `fred_provisioner_deferred_closes_pending` remains elevated | Queued or executing close retries await inventory projection, lifecycle ownership, or local backend circuit admission | Correlate `lease close deferred` with the bounded `reason` in `fred_provisioner_deferred_closes_total`. Restore the named dependency; never remove an inventory fence or durable attempt to accelerate a close |
| `fred_provisioner_deferred_closes_oldest_age_seconds` keeps increasing | The oldest queued or executing lease entry has not left the provider scheduler. Its first-enqueue age survives coalescing and retries, so repeated hints do not hide an extended wait | Correlate the oldest affected lease in deferred-close logs with inventory, lifecycle ownership or the named backend circuit. The gauge refreshes approximately once per second and on queue mutations and is zero when empty or stopped; it is not durable close-intent age. Restore the dependency instead of deleting attempts or relaxing fences |
| `fred_provisioner_deferred_closes_oldest_age_seconds > 2100` or `fred_provisioner_deferred_closes_total{outcome="overdue"}` increases | A retained close has waited beyond the 30-minute import ceiling plus five minutes. The scheduler emits an Error and increments `overdue` once per queued entry; retries and ownership continue | Inspect the named lease and backend dependency. Escalation never authorizes abandoning the close, releasing import allocation or deleting durable receipts |
| `increase(fred_provisioner_deferred_closes_total{outcome=~"failed\|full\|unavailable"}[5m]) > 0` | A retry encountered an actual failure, all 1,024 slots were occupied, or scheduler admission was closed | Inspect `deferred lease close failed` and event errors. `dispatched` means the call returned successfully, not physical completion; verify the exact deprovision callback or backend retention status. A newer hint can remain queued after the older attempt increments `dispatched` or `failed`. No tenant or lease identifiers appear in metric labels |
| `fred_docker_backend_retention_refused_total` increasing / `fred_docker_backend_retained_volume_bytes` approaching `fred_docker_backend_disk_pool_bytes` | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |
| `fred_docker_backend_retention_reaping_bytes` > 0 sustained across several sweeps | A volume owned by an exact retained-data tombstone cannot be destroyed — its footprint **is** counted in the admission pool (no over-admit) but pins capacity and likely needs manual repair. A rising `..._retention_leaked_total` with `reaping_bytes` flat is instead the self-healing rollback store-error case (no action). This is not unattributed-volume GC. | [Reclaiming retained-data / stuck-reaping volumes](#reclaiming-retained-data--stuck-reaping-volumes) |
| `sum without (outcome) (increase(fred_docker_backend_retention_sweep_total[3h])) == 0` (with retention enabled) | The periodic retention sweep is not completing passes — the loop goroutine is gone, the ticker is starved, the process is wedged, or every pass panics or fails storage-identity verification before its stages (such a pass records no outcome). Any other pass advances the sum whatever its outcome, so a flat sum is not a stage failure. Nothing is being reaped, no interrupted restore is being reconciled, and no orphan record is being pruned | Check the docker-backend process and its logs for `retention cleanup panic` and `retention cleanup failed` (an error starting `backend storage identity verification failed` means storage identity could not be verified); `fred_background_cleanup_panics_total{component="retention"}` distinguishes a panicking sweep from a dead one |
| `increase(fred_docker_backend_retention_sweep_total{outcome="error"}[6h]) > 0` | At least one sweep stage failed. Distinct causes land here, so **read the log line before acting**: an unenumerable `retention.db` (the common one — the reaper and orphan pruner reclaim nothing and **every lease close skips volume teardown entirely**, leaving closes `Failed` and retrying, so the provider degrades toward refusing new work), an unreadable **volume root**, which the orphan stage reports through the same outcome with a perfectly healthy store, or a restore finalizer that cannot finish | **Start with the sweep's `retention cleanup failed` log line, not the database.** It prefixes each failure with its stage — `reap expired:` / `retry reaping:` / `list restoring:` are store reads, `reconcile orphans:` can be either (pair it with `retention_orphan_skips_total`: `reason="store_error"` vs `reason="list_error"` separates them exactly), and `reconcile restoring source "…" destination "…":` is one restore finalizer. Then fix whichever dependency it names; the parked work resumes on its own. Shares a root cause with the `claims_unreadable` row below |
| `fred_docker_backend_retention_accounting_refresh_failed_total` rising | The retained-disk projection could not be recomputed, so the five retention gauges **and** the admission pool's retained input are frozen at their last values. That is the data-safe direction (a zeroed projection would over-admit), but it means those gauges are stale — do not read them as current while this is rising | Same root cause as the row above: fix the retention store. Until then, treat `retained_volume_bytes` / `retention_reaping_bytes` as last-known-good, not live |
| `fred_docker_backend_volume_quota_clear_failed_total` rising | An XFS quota-clear command failed during interrupted-create compensation or typed deletion. The preceding block/inode proof failures do not increment this metric. Typed authority is retained either way. During a deletion, that one volume's deletion is held (reason `quota_clear_failed`) and retried in the background while the backend keeps serving. During create compensation, the current backend instance fail-stops and a fresh `Start` recovers the stage before readiness. A historical already-absent volume without typed authority can still leave an unowned table entry | [Held volume deletions](#held-volume-deletions); [XFS deletion recovery and legacy quota entries](#xfs-deletion-recovery-and-legacy-quota-entries) |
| `fred_docker_backend_volume_delete_holds > 0` for 1h | At least one XFS volume deletion could not finish for a reason confined to that volume. Its delete stage and project ID are kept, the hold executor retries it, and the backend keeps serving; a hold in the `removal` or `unsized` phase keeps its close or operation pending, and an `unsized` hold also withholds disk admission. Ticket, do not page | [Held volume deletions](#held-volume-deletions) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claims_unreadable"}` > 0 | The retention store could not be read, so an exact close or retained-data finalizer could not establish who owns a volume and **nothing was destroyed** — data-safe, but those operations are parked. Runtime closing leases stay `Failed` and retry; retained-data reaping retries on its next sweep. Unattributed managed volumes are preserved without attempting destruction and therefore do not emit this series | Fix `retention.db` health first; parked work resumes when its exact authority is readable. See the `store_error` row in [Partition collapse triage](#partition-collapse-triage) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claimed"}` sustained | An exact destroy path keeps meeting a volume another lease owns — normally an in-flight restore that is not converging, since a healthy restore clears its own claim on commit or rollback. Never data loss: the refusal is the guard working | Read with `reconcile restoring operations:` errors (see the `fred_docker_backend_reconciliation_total` row) and `retention_reaping_leases`; the WARN log names the volume and its owning lease. [Reclaiming retained-data / stuck-reaping volumes](#reclaiming-retained-data--stuck-reaping-volumes) |
| `increase(fred_docker_backend_volume_bind_symlink_rejected_total[1h]) > 0` | A launch was refused because a path the image declares as a VOLUME is a symlink inside the lease's own managed volume (ENG-795). Only that tenant could have planted the link, so this is a tenant attempt, not a fred fault. It counts attempts: the volume is not wedged, and a repeated count is the tenant retrying | Informational; no operator cleanup is needed. An image that does not declare that path still launches on the volume and can remove the link |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation=~"restore_reconcile\|deprovision"}` rising | Container teardown could not prove absence; exact durable authority and accounting remain held for retry | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation="provision_cleanup"}` rising | Candidate cleanup remains incomplete. The exact operation intent, pool reservation, and volume claims remain. A live worker with an ambiguous side effect can still fail-stop; cold/live recovery retries ordinary cleanup failures without re-latching the process | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_operation_intent_recovery_timeout_exhaustions_total{reason="provision_timeout"}` rising | An exact interrupted provision or restore exceeded its durable admission horizon and entered failed-operation cleanup; both use the configured provision timeout, with no separate container-start recovery window | Correlate with lease-scoped warnings and cleanup retry counters. A pending cleanup retains authority and reservation for the next sweep |
| `fred_docker_backend_operation_intent_recovery_cleanup_retries_total` rising | Deferred exact operation cleanup (`provision`/`restore`); intent and reservation remain for periodic retry | Correlate lease-scoped recovery/observation warnings. Restore the failed substrate or journal dependency; never erase authority to clear the signal. Diagnostic volume counts may be transient during create/rename; investigate a sustained value |
| `fred_docker_backend_terminal_substrate_cleanup_retries_total` rising | Transient late-container cleanup retries; daemon stays alive and exact terminal receipts remain | Correlate lease-scoped recovery/observation warnings. Restore the failed substrate or journal dependency; never erase authority to clear the signal. Diagnostic volume counts may be transient during create/rename; investigate a sustained value |
| `fred_docker_backend_unaccounted_managed_volumes` > 0 | Attested managed volumes absent from current live, admitted-operation, and all retention projections; diagnostic only, never deletion or admission authority | Correlate lease-scoped recovery/observation warnings. Restore the failed substrate or journal dependency; never erase authority to clear the signal. Diagnostic volume counts may be transient during create/rename; investigate a sustained value |
| `fred_docker_backend_unaccounted_managed_volume_observation_failures_total` rising | Failed diagnostic inventory/footprint observations; last unaccounted-volume gauge is retained, not reset to zero | Correlate lease-scoped recovery/observation warnings. Restore the failed substrate or journal dependency; never erase authority to clear the signal. Diagnostic volume counts may be transient during create/rename; investigate a sustained value |
| `fred_docker_backend_terminal_substrate_pending_containers{receipt}` > 0 | Late containers remain for permanent `closed` or `failed_operation` receipts; the compact receipts cannot reconstruct their resource profiles, so pool capacity and readiness are withheld on this backend | Restore Docker/storage responsiveness. The daemon keeps retrying exact cleanup; confirm the gauge clears. Never delete the receipts to restore readiness |
| `fred_docker_backend_image_helpers_unsettled > 0` sustained | Image-inspection helpers survived recovery. `cleanup_pending` ones are retried every pass; `unknown_create` ones are kept until offline repair settles them, even after recovery finds and removes a late helper container. On the containerd image store, either blocks image ingestion and image GC on that backend | Restore Docker responsiveness and confirm `cleanup_pending` clears. If `unknown_create` persists, stop the backend and use `docker-backend -inspect-unsettled-docker-effects`, then `-repair-unsettled-docker-effects` |
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
   kill -SIGQUIT $(pgrep -x docker-backend)   # -x: exact name; -f would also match the journalctl
   ```
   (Fred does not include `net/http/pprof`. For a non-fatal goroutine dump, attach `delve` to the running process.)
2. In the dump, look for goroutines in `leasesm.(*LeaseActor).handle` and `leasesm.(*LeaseActor).run`. The actor's `leaseUUID` is on the receiver — visible as the `*LeaseActor` argument in the stack frame.
3. Check what handler it's in — typically `provision.go`, `deprovision.go`, or `restart_update.go`.

**Common causes and remedies:**
- **Image pull stuck or failing**: the registry is rate-limiting or unreachable. docker-backend makes these registry requests itself, so dockerd logs and a manual `docker pull` show neither the requests nor their errors. Search the docker-backend log for `resolve immutable image:` (a Docker Hub quota refusal contains `TOOMANYREQUESTS` or `429`), check `fred_docker_backend_image_registry_requests_total{status=~"429|5xx|error"}`, and follow [Registry rate limits](#registry-rate-limits). Reduce `image_pull_timeout` so the actor errors out sooner.
- **`docker stop` hanging**: a container is ignoring SIGTERM and the grace period is long. Lower `container_stop_timeout`.
- **Volume cleanup hanging on btrfs/zfs**: a quota or subvolume operation is blocked in the kernel. Inspect the filesystem state directly.
- **Genuine deadlock**: file an issue with the goroutine dump. The actor will not unblock; the reconciler will re-detect the lease on its next cycle and retry, but the wedged goroutine leaks until restart.

**Last resort:** restarting the docker-backend recovers cleanly. State is rebuilt from Docker labels and bbolt stores on startup.

---

## Stuck teardown (docker-backend)

**Symptom:** `fred_docker_backend_teardown_fallback_total{outcome="failed"}` is rising.

**What it means:** per-container teardown recovery could not prove absence. For
`restore_reconcile`, this is exact failed-attempt cleanup before source handback;
other paths use per-container fallback after `compose down` fails.

**Read the `operation` label first — it decides what fred did next, and therefore what you must do.**

| `operation` | Kind | What fred did |
|---|---|---|
| `restore_reconcile`, `deprovision` | **Blocking** | Keeps exact authority and reservation until teardown can be proved; recovery retries |
| `provision_cleanup` | **Authority retained** | Preserves the exact intent and reservation. An ambiguous live worker may fail-stop; ordinary failure during durable recovery stays pending for periodic retry |

On a **blocking** operation this is not data loss and not an over-admission — the cost is capacity and disk that stay reserved, plus one anonymous volume per surviving container, until the substrate recovers.

Failed cleanup is not, by itself, loss of storage authority. Recovery keeps
its exact durable intent or terminal receipt and retries. Unknown late-container
footprints withhold capacity through pool-owned accounting holds, so the daemon
can remain alive without advertising unsafe capacity. While held, `/stats`
returns `503` and in-process load reads refuse the incomplete ledger; routing
prefers healthy peers with usable statistics. Known allocations remain visible
in backend metrics. Verified identity drift still terminates the affected
backend. The obsolete `restore_prelude` and
`restore_rollback` paths/metric labels are no longer emitted.

These accounting holds apply to the backend's entire resource pool: fresh or
replacement resource admission is refused while a footprint remains unknown.
Other backend instances are not gated by this process-local hold. Successful close may
retire earlier failed-operation history without clearing an existing
`failed_operation` hold or gauge. Recovery retains only that cohort's callback
identities for observation until strict inventory proves absence; those saved
identities cannot authorize deletion. Closed and failed-family holds release
independently.

Cold and periodic recovery each take a bounded observation rather than waiting
for a transitional provision or restore to finish. Exact-empty cohorts, running
health checks, and `restarting`, `created`, or paused containers remain Pending until
the horizon derived from durable admission time and the current
`provision_timeout`; later sweeps re-observe them without blocking startup.
There is no separate `container_start_timeout` recovery window. An exact Ready
cohort settles successfully; a failed sibling or exhausted horizon enters exact
failed-operation cleanup. Cancellation, inventory uncertainty, or ordinary
cleanup failure preserves the intent and reservation for retry; verified
storage-authority loss remains fail-closed. Restore shares the recovery horizon,
but its rollback additionally requires exact source/destination authority and
an actor-quiescence capability; a committed destination Release cannot be rolled
back.

After a wall-clock rollback, an admission timestamp may appear to be in the
future. Provision, restore, and maintenance recovery then retain one monotonic
`provision_timeout` window per exact attempt for the life of the backend process;
periodic sweeps do not renew it. A process restart can conservatively grant
another bounded window while the timestamp remains in the future, so repeated
process restarts can delay recovery. Expiry still requires the normal cleanup
and settlement evidence before durable fences or reservations are released.
The first observation of a future admission logs a warning with the lease UUID,
attempt fingerprint, `admitted_at`, and `deadline`; repeated sweeps reuse the
window without repeating the warning.

`fred_docker_backend_operation_intent_recovery_timeout_exhaustions_total{reason="provision_timeout"}`
counts expired provision/restore classifications and may increase again on
later sweeps while cleanup remains pending.

Operation settlement does not delete its write-ahead row. The same bbolt
transaction changes the exact Pending row to Succeeded or Failed and enqueues
its callback; successful HTTP delivery removes only the FIFO delivery. The
terminal row remains the idempotency and crash-recovery decision until an
authorized successor atomically supersedes or retires it. Restore
handback in particular requires the exact Failed row. If a source finalizer
names an absent operation row before a destination Release committed, treat
`callbacks.db` as missing/corrupt authority
and preserve the source, destination, and substrate; absence is never shorthand
for failure. A Succeeded row plus the immutable finalizer reconstructs a missing
active Release; Failed plus an exact committed Release is contradictory authority
and fails closed.

`outcome="recovered"` is the benign twin: absence was proved. For `deprovision` and `provision_cleanup`, `down` failed but the fallback removed every container a fresh inventory found for the lease; for `restore_reconcile`, exact failed-attempt cleanup was followed by a strict inventory with no destination container.

**Triage:**

1. Find the lease and the operation from the `operation` label and the warning logs (`compose down failed, falling back to individual removal`, then `failed to remove container`).
2. Check the daemon: `docker ps -a --filter label=fred.lease_uuid=<uuid>`.
   - **Nothing returned**: a later strict recovery inventory can prove container absence; remaining volume/source finalization may still need retry.
   - A container stuck in `Removal In Progress`, or one whose `docker rm -f` hangs, usually means a wedged storage driver or a mount the kernel still holds.
3. Fix the substrate (see [Wedged lease actor](#wedged-lease-actor-docker-backend)). Durable recovery retries ordinary cleanup failures in-process. Restart is required only if logs identify a terminal storage-authority or live-operation ambiguity latch, not merely a failed removal.
4. Restarting the docker-backend resumes exact durable recovery. For `provision_cleanup`, preserve the operation-intent, release, retention, storage-marker, volume, and Docker lineage together; recovery uses that evidence rather than merely adopting a leaked candidate as ordinary live work.

**Restore-specific consequence.** With `operation="restore_reconcile"`, the retention record stays `restoring` until exact rollback/finalization succeeds. Its bytes are not eligible for expiry reaping while restoring. A committed destination Release cannot be rolled back. There is no inference-driven unattributed-volume collector; preserve the journals and substrate together while correcting the daemon fault.

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
`CAP_FOWNER` so startup can repair the root inode of a pre-existing tenant-owned volume.
A plain `setcap …+ep` on the binary is insufficient for CLI quota operations —
`CAP_SYS_ADMIN` must reach the exec'd `xfs_quota`/`btrfs` children. XFS root repair
uses descriptor-bound kernel ioctls in the backend process and never walks tenant
descendants. The warning `repaired xfs volume root project attributes; descendants
require offline verification` distinguishes a repaired root from ordinary limit
refresh. Preserve its path and previous project attributes, then verify historical
descendant tagging during stopped-writer maintenance; the warning does not attest
the existing tree. Full setup is in the xfs section and the
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
quota (root project/inheritance verification + limit refresh). Existing tenant
trees are not walked: Fred repairs the already-open root inode with XFS
`FSGETXATTR`/`FSSETXATTR`, preserving unrelated flags and verifying the result.
It does not use `xfs_quota project -s`, whose depth option still traverses trees.
It attempts the complete live and retained inventory and joins all failures, but
any inventory, durable-resource-authority, or enforcement error makes `Start`
fail before the command-line HTTP/metrics server is created. The process never
serves a known volume uncapped.

`fred_docker_backend_volume_quota_backfill_total{outcome}` (`outcome ∈
{applied, failed, delete_pending}`, where `delete_pending` is a volume whose
deletion is held and whose delete authority keeps its limits) counts the
individual attempts, but do not depend on scraping it from this failure mode:
the normal binary has not bound its metrics endpoint.
Use the nested `reconcile startup volume quotas` startup error to identify every
affected name. On XFS, repairing a tenant-owned root can require `CAP_FOWNER`.
Grant it or repair the reported substrate/authority error, then restart.
The 2026-09-23 fleet check recorded in ENG-1051 found no untagged descendants
across 1,043 volumes; no legacy recursive healing path is required.

---

## Image capacity and collection

Docker image storage is accounted separately from per-volume project quotas.
Shared image, journal and tenant-volume filesystems are supported with the
existing deployment layout. Containerd image storage requires `image_data_path`
naming its actual content directory; it may differ from Docker's data root.
Inspect free space there, in the Docker data root and beside the callback journal
when image admission is refused.
Ingestion supports classic `overlay2` and containerd's `overlayfs` snapshotter;
other drivers prevent backend startup. A driver that copies full parent
filesystems needs a different space model. Existing `overlay2` deployments keep
their configuration.
Classic Docker's default import staging under `DockerRootDir/tmp` is included in
the allowance. A `DOCKER_TMPDIR` override on another filesystem is not discoverable
through Docker's API. Supported accounting requires the default daemon staging
directory; explicit accounting for an external override is not implemented.

Registry access comes from `docker-backend` over HTTPS, with process proxy
settings and system CA trust. Docker daemon mirrors, insecure-registry settings
and `/etc/docker/certs.d` are not consulted. Configure the backend process
accordingly; a registry response must continue making progress within 30 seconds.

Fred stages registry content in `<callback_db_path>.image-staging`, checks
compressed-content digests, image configuration and expanded
layers, then imports those same verified bytes into Docker. Docker does not
perform a second registry fetch. The configured `image_max_size_mb` bounds
new staging and expanded content before import; the peak import allowance is
also capped at twice that budget. Verification bytes and physical import bytes
are separate saved bounds: compressed staging, decoded tar padding and metadata
cannot borrow authority from a physical disk estimate. Import accounting includes
classic Docker's retained tar-split metadata, including repeated layer occurrences.
Lowering the configured limit does not invalidate a local or
pinned historical image. Layer entry, path and metadata budgets also apply: at
most 128 layers, with a strict selected-platform match and the namespace
allowances below. Each normalized header is limited to 64 KiB of metadata;
its entire raw parser span, including hidden PAX/GNU extensions and framing,
is independently limited to 130 KiB. Repeated layer descriptors and global PAX headers are
supported. Sparse entries, duplicate
paths and hardlinks without an earlier regular-file target in the same layer
are refused; rebuild such images with supported layer contents. Verification
adds CPU and temporary disk use on first ingestion. Pinned images already
present locally require no registry access once their verified allowance is
recorded. A legacy containerd pin without a separate saved verification bound needs one verification
and import of its exact repository digest; Fred never falls back to its mutable
tag.
Tag selection verifies bounded manifest and config metadata, including their
platform and layer-count agreement, before a classic Docker cache hit can
persist its recovery reference. Every unpinned preparation re-resolves its tag
with one manifest HEAD, which Docker Hub does not meter as a pull, so a moved
tag is observed on the next preparation as it was with dockerd. The HEAD's
`Docker-Content-Digest` only selects manifest bytes this backend has already
hashed itself for the same registry repository, from a backend-owned LRU of at
most 1,024 manifests (an index image uses two) and 32 MiB; any manifest within
the 2-MiB metadata limit fits. Only an uncached digest costs a manifest GET,
made by that digest; a cached digest reference or index child needs no request.
Bytes enter the LRU only after the resolution that read them passes metadata,
platform and layer admission. Concurrent misses for one repository digest share
one read: the other preparations wait, each under its own `image_pull_timeout`,
until the first finishes its manifest and config reads and admission, so a
stalled registry exchange in that first preparation also delays them.
Config content is reused
from a backend-owned, digest-verified LRU (at most 128 entries / 32 MiB); a miss
requires a config fetch under the aggregate 2-MiB metadata allowance. Each
manifest still receives platform/layer/metadata admission, including on a cache
hit. Both caches are in memory, so eviction or restart can require those fetches
again. They do not download cached layers or
prove their future availability: missing local content still requires full
verification of the exact pinned registry content before import. Per-lease
registry cost is described under [Registry rate limits](#registry-rate-limits).

Before staging, admission checks the maximum staging allowance above the
free-space floor. Before Docker import, it checks the verified image's
conservative import footprint plus the floor again. Launches require the floor.
Staging, import and extraction owners account for each other without holding a
provider-wide lock during network or daemon I/O. A Started journal subject grants
an image preparation its immutable tenant identity. Only an actual staging miss
enters the four-slot pool; pinned and locally reusable images bypass this queue.
Concurrent preparations of the same resolved source digest, platform and
verification budget share one manager-owned download/import. Each member keeps
its own deadline and journal authority for pin publication; the first member
has no special cancellation authority. The last member leaving cancels
undispatched preparation. Dispatched imports retain their independent ownership.
The flight owns its staging slot and competes using its least-loaded live tenant;
if its accounting tenant leaves, a surviving tenant takes that charge. Cache/local reuse is checked again after queue admission.
Verification derives its namespace allowances from the same typed byte budget
that owns staging and decoding. Namespace memory is at least 128 MiB or 1/32 of
that budget, retained names at least 32 MiB or 1/128, and cumulative path-resolution
work at least 64 MiB or 1/64. At the default 10 GiB these limits are 320, 80 and
160 MiB. A separate construction ceiling limits each preparation to 1 GiB
model memory, 256 MiB names and 512 MiB resolution work, including legacy
recovery derived from host disk headroom. These maxima are reached at a 32-GiB
verification allowance; larger byte budgets do not raise parser resources.
Headers retain full paths; tree nodes charge their retained base component,
and symlinks separately charge their targets. Replacements and deletions never
refund usage. The namespace owner projects all three consumed dimensions into
the saved verification budget, so lowering new-image policy cannot remove an
already admitted image's recovery authority within the construction ceilings.
The fixed floors preserve old pins within those ceilings. An exceptionally
large historical image that exceeds them may be refused if its local content
is missing and must be verified again; free host disk cannot enlarge the parser.
Four staging slots bound concurrent verification, and each decoder remains
bounded to 64 MiB. Namespace allowances model retained allocations and work;
they are not a hard process-RSS limit. Raising `image_max_size_mb` also raises
these allowances up to their construction ceilings. Compressed/decoded bytes and physical import allocation remain
independent constraints: namespace headroom does not promise equal image-byte
growth within the default 20-GiB import ceiling.
Decoded padding after the tar terminator has a separate retained-metadata
allowance: compression streams can make Docker retain one JSON segment per
decoded byte. This charge is distinct from tar headers and file allocation.
Registry metadata GETs and manifest HEADs have at most three attempts for transient connection,
no-progress or availability failures. Each immutable blob owns one three-attempt
allowance shared across resumes, redirects and authentication renewal. Mixed
faults can exhaust it: token renewal plus two interrupted bodies receives no
fourth canonical request. This deliberate work bound is not three retries per
fault type; a lost response cannot prove how much content was sent. Wrapper
copies cannot reset it. Every blob attempt starts at its immutable registry URL,
using a fresh HTTP/1 connection so the native transport cannot silently replay
requests below that counter. This adds connection/TLS setup per exchange;
registries must support HTTP/1.1. Connections closed before response headers
may retry within the same allowance. Every retry starts at the registry origin,
refreshing redirects instead of reusing an expired CDN URL. Resumed downloads
can renew expired authentication within that allowance; token responses always
retain the separate metadata limit. Interrupted blobs
resume at the retained prefix when Range is supported; a rejected range falls
back to a full GET within the same attempt bound, replaying only that blob's
prefix. For HTTP 503, and for a blob's 429, Retry-After can delay the next
attempt by up to 30 seconds, subject to the existing caller deadline. A metadata
429 is a quota decision: it is retried only when Retry-After asks for at most
30 seconds, and otherwise fails at once. Each metadata redirect chain, including
its transient retries, owns at most ten actual HTTP exchanges; a tag's HEAD and
the manifest GET it selects share one such allowance. Each canonical
blob attempt has the same ten-exchange redirect bound. Chain copies share the
allowance. Redirects preserve HTTPS and exact-origin credentials, and private-IP
checks normalize IPv6 zones, legacy IPv4 spellings and trailing dots. The final
digest and descriptor size still bind all bytes. Completed
layers and dispatched Docker imports are never retried through this path.
Content, metadata and budget refusals remain terminal.
A sole tenant can use all four slots for distinct images. When capacity becomes
available, the waiting tenant with the fewest active stages goes first; ties and requests within
a tenant follow arrival order. Waiting requests consume no staging allowance,
and cancellation removes them from the queue. This scheduling does not preempt
occupied slots or guarantee isolation from a tenant using multiple addresses.
A shared flight may continue while successive eligible members sponsor ongoing
progress; there is no absolute flight lifetime or queue-wait guarantee.
A local
launch checks actual free space plus unknown import allocations; live owned
imports are not added
a second time to that launch floor. Before dispatch, Fred writes the allowance
to `<callback_db_path>.image-staging/image-import-debit-v1`. An admitted import
belongs to the loader's lifetime, independent of tenant cancellation or the
pull timeout once dispatch is admitted, with a 30-minute ceiling measured from
dispatch. Its staging files and capacity ownership remain held until the exchange
completes. Closing the lease cancels its workflow and immediately returns the
breaker-neutral `503 lifecycle_pending` while the owned worker is still draining;
providerd places the exact lease/client response on its bounded deferred-close
scheduler, without poisoning the close event. The actor retains close ownership
so a canceled worker settles as preempted by lease close. Teardown retries after
that worker exits. Shutdown closes import admission and
allows admitted imports to finish within the remaining shutdown budget, then
cancels their owner if that deadline expires. The command shares 75 seconds
between HTTP shutdown and backend drain, fitting the existing 90-second systemd
default; a direct Go `Backend.Stop` call retains its 90-second default.
Journals close after
the owned requests drain. Clean upload and terminal completion release its amount
even when
Docker reports a completed failure; the deployment still fails. Transport,
timeout or malformed-stream failures retain unproven allocation across restart,
without automatic expiry. Positive debit records written with the older
`FREDIMG1` accounting are preserved and refuse startup because their allowance
omits part of Docker's retained metadata. Follow the offline recovery procedure
below before clearing such a record. An empty old record upgrades automatically;
the new `FREDIMG2` record retains the same filename. A corrupt debit record or foreign staging content
prevents startup; preserve it for investigation instead of deleting it. The
`fred_docker_backend_image_import_pending_bytes` gauge reports outstanding
allocation, including unknown completion.
`fred_docker_backend_image_import_total{outcome}` counts each dispatched import
once as `success`, `failure`, `deadline`, or `shutdown`. Deadline and shutdown
are classified by the loader’s own lifetime. Deadline outcomes can be scraped
while Docker unwinds; shutdown outcomes happen after the metrics listener closes
and cannot be relied on for alerting. Shutdown instead logs a WARN with outstanding
admitted bytes (or an unreadable-allocation warning). Such an outcome does not
prove the daemon stopped allocating or authorize clearing its debit.
These checks sample available space; they do not physically reserve it against
concurrent tenant or unrelated host writes. Keep the tenant disk pool and other
host consumers within the filesystem's usable capacity with operational headroom.

Containerd admission also creates and removes a stopped, journal-owned probe
with an owned extraction allowance to force any deferred snapshot extraction. It
never starts, has no network, and covers image `VOLUME` declarations with tmpfs.
Content-inspection helpers use the same fixed configuration, so Docker does not
populate anonymous volumes or create the image's working directory. Archive
reads still expose the original image content and ownership.
The pin retains the verified extraction allowance for later admission. Pending
image-helper receipts block further containerd ingestion after restart; unknown
Create completion also fences the current storage authority. Recover those
receipts through [unsettled Docker effects](#unsettled-docker-effects).

The collector runs each minute and before image admission. The periodic pass
prunes obsolete manifest pins even while image work is active and below its disk
threshold. Image deletion still waits for active preparations to finish publishing
their pins. Collection removes unreferenced images
between the high and low thresholds. It uses Docker's non-force removal and
keeps every container-referenced or durably pinned image. Missing legacy pins
or incomplete journal/container inventories prevent destructive collection,
while unrelated admissions may continue if their own checks succeed. Startup
backfills missing active pins from an exact live cohort and immutable image
inspection after all fatal startup checks have succeeded. Failed/superseded
history retains pins only when needed by an exact
pending compensation. Pre-upgrade retained generations with missing pins, and
retained rows without a manifest, conservatively keep images until restored or
safely reaped. The default retention grace is 90 days plus the sweep interval;
parked reaping can extend it. This expected upgrade condition does not indicate
a new disk leak or require an alert while headroom is healthy.
`fred_docker_backend_image_unpinned_generations{kind="retained"}` reports
retained generations without complete pins, including historical rows without
a manifest. `kind="active"` reports active generations and required compensation
ancestry without complete pins. Counts are per generation, not per image; they
reflect the latest successful pin inventory. A failed inventory read preserves
the last observation. Use these gauges with inventory errors and disk pressure
to distinguish expected retention inhibition from incomplete active authority.
`fred_docker_backend_image_gc_total{outcome}` distinguishes `busy`
(ordinary live admission or helper work) from `inhibited` (incomplete pin authority or
unresolved inspection evidence). It also reports shared, below-threshold, removed, error and
panic decisions. Unknown import allocation alone does not inhibit collection of
unused, unpinned images; it still raises the headroom needed for admission.
Removal conflicts keep their images; resolve them during fenced maintenance,
without deleting pins or authoritative release history. Until real free space
recovers, capacity refusals remain possible. Lowering the new-image size cap
neither deletes nor invalidates content pinned by retained generations.

The image-pin journal admits at most 100,000 pins in total and 10,000 per
tenant. A tenant's share follows its verified durable lease/release identity;
a supplied label or image reference cannot choose a different owner. Exact
reuse and recovery of an existing pin remain available above these limits.
Fresh pins can be refused until obsolete pins are pruned by normal collection.
If an old row lacks positive durable tenant attribution, it counts against the
global cap and every tenant's fresh-pin share until authority proves its owner
or normal collection can safely prune it. Correlate pin-capacity refusals with
legacy backfill and collection warnings; preserve journal evidence rather than
deleting pins or assigning owners manually. The quota never automatically
forgives outstanding import allocation.

The Docker volume `fred-image-cache-owner-v1` records durable cache ownership.
Production uses an exclusive backend storage identity; development uses shared
mode with no image deletion. A conflicting mode or storage identity prevents
construction. Preserve this marker across restarts and backups of the daemon.
Never remove it to bypass an ownership error while any participating lineage
still has active or retained authority. A mode transition requires an offline
drain of every lineage. An unchanged manifest retains its pinned image even if
its tag moves; deploy a new image reference or digest to change the content.

### Registry rate limits

docker-backend pulls anonymously; dockerd's `registry-mirrors` and credentials
do not apply to it. Docker Hub counts manifest GETs against an anonymous
allowance per source IP address and reports it in the `ratelimit-limit` and
`ratelimit-remaining` response headers; a manifest HEAD is not counted. A
refused GET fails the preparation with `TOOMANYREQUESTS`, and the tenant sees
`ImagePullFailed`. Leases whose image pin is already published (restarts and
launch admission) resolve no tag and make no registry request while their
content is local.

| Unpinned preparation | Manifest requests | Metered manifest GETs |
|---|---|---|
| Tag unchanged, its manifests already verified by this backend process | ping, token, HEAD | 0 |
| First preparation of an image since backend start, or the tag moved | ping, token, HEAD, GET by digest | 1 (2 for an index) |
| Immutable reference or index child already verified | none | 0 |
| Registry answers the HEAD with 405/501, or a 2xx without a usable digest, content type or length | ping, token, HEAD, GET of the tag | 1 per preparation; index children stay cached |
| HEAD refused with any other status (such as 401, 403, 404 or 429), unavailable after its attempts, or unreachable | preparation fails without a GET | 0 |

The image config is read separately and reused from the 128-entry verified
config cache. A config missing from it, after a restart, an eviction or for a
new image, adds its own ping, token and blob GET, which Docker Hub does not
meter.

Budget one metered GET per distinct image (two for a multi-platform index) per
backend process while its manifests stay cached, plus one per tag move.
Concurrent preparations of one image share its GET and wait for the first
preparation to finish its reads. The verified-manifest cache is in memory, so
each restart pays those GETs again; avoid restart loops during a quota
incident. A metadata 429 is not retried unless its Retry-After asks for at most
30 seconds.

`fred_docker_backend_image_registry_requests_total{endpoint,method,status}`
counts every exchange, including retries and redirect hops;
`endpoint="manifest",method="GET"` approximates metered pulls and `status="429"`
counts quota refusals. `endpoint="ping",status="4xx"` includes ordinary bearer
challenges. `fred_docker_backend_image_tag_resolutions_total{source}` shows how
each tag resolution that passed its HEAD found its manifest: `cache` needed no
GET, `registry` fetched an uncached announced digest, and a sustained
`fallback_unsupported` or `fallback_incomplete` share identifies a registry that
costs one GET per preparation. In steady state this ratio stays near zero:

```promql
sum(rate(fred_docker_backend_image_registry_requests_total{endpoint="manifest",method="GET"}[1h]))
  / sum(rate(fred_docker_backend_image_tag_resolutions_total[1h]))
```

To check the remaining Docker Hub allowance without spending it, send a HEAD
from the backend host through the backend's proxy settings:

```bash
TOKEN=$(curl -fsS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:ratelimitpreview/test:pull" | jq -r .token)
curl -fsS --head -H "Authorization: Bearer $TOKEN" \
  https://registry-1.docker.io/v2/ratelimitpreview/test/manifests/latest | grep -i '^ratelimit'
```

Refusals stop when the registry's window resets; tenants must create new leases
to replace rejected ones. Registry credentials and pull-through mirrors are not
yet configurable for docker-backend. Shared credentials would let any tenant pull
whatever they can read, so any future credential must be a
public-repository-read-only token scoped to its registry host.

### Recovering outstanding image import allocation

The import debit represents work that may still allocate space. Freeing disk,
waiting, restarting Fred, or observing an image in Docker cannot settle it.
There is no automatic debit reset. For exceptional offline recovery:

1. Stop Fred and every client that can submit Docker work. Stop Docker and drain
   its container runtime; prevent automatic restart and establish that no old
   import or extraction can resume. Stopping Fred alone is insufficient.
2. Preserve a matching backup of `callbacks.db`, release/retention journals,
   storage and daemon ownership markers, image storage, and the staging debit
   record. Keep unresolved helper receipts; clearing the import debit does not
   repair them.
3. Check actual usage and free space on Docker's data root, any configured
   containerd image directory, staging and journal filesystems. Resolve storage
   faults and restore the required headroom while admission remains closed.
4. Only after that external drain and backup, explicitly clear the outstanding
   import amount offline by removing **only**
   `<callback_db_path>.image-staging/image-import-debit-v1`. Do not edit its
   checksummed bytes or delete the staging directory or callback journal.
   Restart the same Docker/storage lineage and then Fred; repeat normal health
   and capacity checks before reopening admission.

## Pending maintenance pressure

Signed completion wakes recovery. Each backend interleaves durably confirmed
completions with an ordinary rotating batch of at most 32 retries, and alternates
which class leads successive passes. Each class has its own progress cursor, so
slow or persistently failing completion settlement cannot continually consume
the entire backend budget before an ordinary retry gets an opportunity.
This fairness applies across returning recovery passes: a synchronous payload
write must return before another pass can start; the lane deadline does not abort
that write. Selection uses
compact committed lease/ID/backend/phase accounting; only the exact commands
actually processed are decoded. A confirmed
command therefore does not wait for the ordinary batch cursor to reach its lease. Live
dispatch ownership and the per-backend recovery timeout still apply; the
periodic tick retries failed persistence without a wake-driven retry loop. New
pending commands are bounded to 1,024 records and 64 MiB of encoded journal
content provider-wide. Each record reserves 512 bytes of phase-growth headroom.
There is no fixed per-tenant command or byte ceiling: aggregator tenants can
borrow the shared budget. A tenant that already has pending work must leave one
record and 2 MiB available for a tenant with no pending work. The latter may
consume that reserve; the finite pool cannot guarantee admission for unlimited
new tenant addresses. All admission checks run in the accepting transaction.
Replays and settlement of existing records remain available even when
a pre-upgrade journal exceeds these limits. The `fred_maintenance_pending`,
`fred_maintenance_pending_bytes` and `fred_maintenance_pending_oldest_age_seconds`
gauges expose each closed phase from transaction-maintained counts. Admission
refusals carry the bounded `reason` values `count`, `bytes`, `reserved_count` or
`reserved_bytes` in `fred_maintenance_admission_refusals_total`. Global exhaustion
returns `503`; an incumbent reaching the newcomer reserve receives `429` with
`reason: maintenance_capacity_reserved`, `Retry-After: 1`, and the message
`maintenance capacity is reserved for tenants without pending work;
retry after your pending work completes`. That refusal occurs before a new
command is recorded; retry once pending work has completed. `Retry-After` is a
minimum wait, not a polling cadence: use bounded exponential backoff with jitter
while work remains pending. It is not a provider health failure and belongs outside provider exhaustion alerts. No per-address
deployment override is required. An old completion
without
its maintenance ID is still insufficient authority to promote or discard a
payload; preserve that pending record for recovery.

## Custom-domain operation recovery

Ingress admission records only a domain that its typed route can emit. A lease
may request a domain while ingress is disabled, while its service has no
routable port, or while DNS is deferred; the workload still provisions without
a custom-domain label. Desired chain metadata remains separate.

A pre-fix ENG-1055 intent may already contain contradictory effective metadata.
Recovery keeps that exact operation pending, preserves its reservation and lease
fence, and logs `operation recovery retained unresolved lease authority`.
Healthy sibling recovery and backend startup can continue. It does not erase
the domain based on today's configuration or synthesize a success/timeout callback.
Preserve the journals and container evidence and keep that lease fenced while
engineering prepares a repair from the exact operation and physical cohort.
There is currently no shipped repair command for contradictory ENG-1055 ingress
metadata. The Docker effect-debt repair commands below do not repair it. Do not
delete the pending row or edit its domain to make recovery succeed; either can
orphan a running workload or invent a terminal decision.

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
  The encoded final volume is removed in place, its absence is parent-synced,
  then numeric quota reports must prove both block and inode usage for the
  encoded project ID are zero before all four limits are cleared. The authority
  is removed and the parent synced only after the clear succeeds. Startup does
  not run this work: it registers each stage it finds as a held deletion,
  refuses same-name creation, and serves; the hold executor finishes the
  deletion after `Start` returns. A failure confined to that volume, such as an
  open-but-unlinked tenant file keeping usage nonzero, keeps the deletion held
  and retried instead of stopping the backend; see
  [Held volume deletions](#held-volume-deletions). Only a contradiction of the
  stage's own authority, or an outcome that cannot be classified, still latches
  and stops the current backend instance.
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
run `systemctl stop docker-backend`, verify the unit and process are fully
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

## Unsettled Docker effects

A launch or image-helper request whose completion is unknown remains durable in
`callbacks.db`. A timeout, a restarted daemon, or empty container inventory
cannot prove that an earlier Docker or container-runtime request will never
execute. Launch records block reuse of their exact physical volumes and lease
namespace across restart. Ordinary work on unrelated leases remains independent.

The configured `docker_host` must reach a direct, trusted Docker endpoint. Fred
disables environment HTTP proxies for its Docker SDK transport so gateway errors
cannot masquerade as daemon completion. A recognized terminal Docker failure can
settle a finished request while the workflow still fails. Caller cancellation
alone does not settle or abandon an owned effect: a positive completion during
the completion owner's lifetime can still settle it. Lost responses or
unrecognized outcomes retain the unresolved record. Neither request
completion nor repair invents a Ready workload.

Image helpers use the same completion protocol as managed launches. Their durable
reservation follows dispatch admission and precedes Create; refusal before
dispatch admission does not leave an unknown helper record. Known completed helper failures settle
normally, while unknown effects remain subject to the fencing procedure below.

Even when Docker completion was observed, a journal commit failure (for example,
momentary `ENOSPC`) can leave the launch or helper record durably unresolved.
The in-memory completion receipt does not survive the failed workflow or a
restart; freeing disk space alone cannot prove completion to recovery. This
case still requires the offline operator fencing and repair procedure below.

Use the following exceptional offline procedure only when normal recovery cannot
settle an unknown request. The commands inspect or repair journal evidence; they
do not stop Docker, fence its runtime, or delete containers for you.

1. Stop the affected backend and **all old Fred clients and admission** that can
   reach it. Prevent supervisors, schedulers and old processes from resuming.
   Preserve the matching journals, storage markers and substrate. Do not delete
   launch/helper rows or recreate volume paths.
2. Externally fence and drain **both Docker and its container runtime** on that
   host, including earlier queued requests. One concrete option is a host reboot
   after disabling automatic restart of the backend and every old client, with
   admission still closed. Restarting only the Docker daemon, especially with
   runtime tasks surviving it, or observing zero containers is insufficient.
3. Restore access to the **same** daemon/storage lineage and mounted volumes;
   keep the backend and old clients stopped. Use the service's unchanged config
   and working directory when paths are relative. Do not initialize or adopt a
   replacement storage identity. Inspect without starting the backend:

   ```bash
   umask 077
   docker-backend -config docker-backend.yaml \
     -inspect-unsettled-docker-effects > docker-effects-inspection.json && \
     jq -e '.verdict == "DOCKER_EFFECTS_INSPECTED"' docker-effects-inspection.json && \
     jq . docker-effects-inspection.json
   ```

   Require exit status zero and `verdict: "DOCKER_EFFECTS_INSPECTED"`. Review the
   backend, storage ID, database path, snapshot SHA-256, launch/helper counts and
   affected leases. Read `acknowledgement` and verify every stated fencing fact
   before accepting it. It binds this exact snapshot; an old acknowledgement
   cannot authorize a changed journal. If both counts are zero, there is nothing
   for repair to settle; with the external fence maintained, proceed to step 6.
4. Choose a **new, absent backup path** in a trusted directory. Repair creates
   the mandatory backup with mode `0600`, verifies its exact bytes and refuses
   to overwrite an existing file. Only after the preceding review, run:

   ```bash
   docker_effects_ack=$(jq -r '.acknowledgement' docker-effects-inspection.json)
   docker-backend -config docker-backend.yaml \
     -repair-unsettled-docker-effects \
     -docker-effects-acknowledgement "$docker_effects_ack" \
     -docker-effects-backup /var/backups/fred/docker-effects-before-repair.db \
     > docker-effects-repair.json
   docker_effects_status=$?
   cat docker-effects-repair.json
   test "$docker_effects_status" -eq 0 && \
     jq -e '.verdict == "DOCKER_EFFECTS_FENCED"' docker-effects-repair.json
   ```

   Require **both exit status zero and `DOCKER_EFFECTS_FENCED`**. An error or
   `REPAIR_NOT_CONFIRMED`/`REPAIR_COMMITTED` means preserve any created backup and
   reconcile the outcome with a new read-only inspection before retrying. Even a
   complete-looking JSON response is insufficient if the command failed during
   output or final verification.
5. If repair returned an error, **keep the external fence and all clients
   stopped**. Retain the original inspection, repair report and backup. Inspect
   the same backend/storage/database again into a separate file:

   ```bash
   docker-backend -config docker-backend.yaml \
     -inspect-unsettled-docker-effects > docker-effects-after-repair.json && \
     jq -e --slurpfile original docker-effects-inspection.json '
       select(.verdict == "DOCKER_EFFECTS_INSPECTED"
         and .backend == $original[0].backend
         and .storage_id == $original[0].storage_id
         and .database == $original[0].database
         and .launches == 0 and .unknown_helpers == 0)
     ' docker-effects-after-repair.json
   ```

   A successful inspection with matching identity and **both durable counts
   zero** confirms that no unresolved Docker effects remain and permits normal
   recovery under the maintained external fence. This verifies the durable
   journal; it does not infer completion from empty container inventory. Do not
   rerun repair to obtain a success token: repair deliberately refuses when no
   work remains. Any identity mismatch, nonzero count or inspection error keeps
   the backend stopped. Remaining effects require a fresh acknowledgement and
   new backup path before another repair attempt.
6. After either confirmed repair or that successful zero-count inspection, start
   normal backend recovery against the same lineage, verify health and exact
   workload outcomes, then reopen admission.
   Recovery still requires exact container, release and volume ownership; repair
   does not bypass a foreign cohort, repair application data, or mark a lease
   Ready.

Inspection and repair are mutually exclusive with each other and with storage
initialization/adoption preflight. Both use
`-storage-identity-operation-timeout` (default `10m`) as a cooperative deadline.
Blocking filesystem calls may still require process supervision; elapsed time
never supplies the external fencing fact. Keep diagnostics on stderr and retain
the JSON reports and backup with the incident evidence.

---

## Held volume deletions

When docker-backend cannot finish deleting one XFS volume for a reason
confined to that volume, it holds that deletion instead of stopping. The
backend keeps serving every other lease, `/health` and readiness are unaffected,
and a restart is safe: `Start` registers every delete stage it finds as a held
deletion and serves. One background hold executor finishes held deletions. Its
first pass runs as soon as `Start` returns. A pass that moved a hold forward (completed
it, changed its phase, or removed volume content) is followed at once by the
next; otherwise the executor waits 30 seconds. A slice spent waiting on the
quota subsystem is not progress. A pass
lasts at most 60 seconds and runs up to two attempts at once, never two of the
same lease. Each attempt is limited to a 15-second slice and takes only that
lease's volume namespace, which it can keep for up to about 10 seconds longer
when the slice ends during the one quota-clear command.

**Signal.** `fred_docker_backend_volume_delete_holds{phase}` is the number of
held deletions by phase. A new or changed hold logs `volume delete held` with
`volume_id`, `delete_stage`, `project_id`, `phase`, `reason`, `attempts`,
`next_attempt` and `error`: at INFO while the deletion is merely still running
(`recovered`, `deadline`, `stopped`), at WARN when it was refused or is
unsized. `fred_docker_backend_volume_delete_outcomes_total{outcome}` counts
attempts (`completed`, `held_removal`, `held_unsized`, `held_residual`,
`latched`), and `fred_docker_backend_tree_removals_total{site,outcome}` counts
the removals of volume trees.

**Phases.**

- `removal`: tenant bytes may remain at the final path, or its absence is not
  yet durable. The caller keeps its authority: a close stays pending (Deprovision
  answers 503 `lifecycle_pending`, the lease reports reason
  `VolumeDeletionInProgress`, and `fred_docker_backend_close_intents_delete_held`
  counts closes waiting only on held deletions), a reaping retention record
  stays, and an operation intent stays. A close waits only on held deletions when
  every one of its volume slots is either held or done (destroyed, retained, or
  never had a volume, like a stateless service) and no container of the lease
  remains. A close with no provision record (cleanup-only) proves the latter by
  its own container teardown, so it runs once after every start before it
  counts as waiting. The executor resumes a waiting close as soon as its hold
  stops holding it.
- `unsized`: the volume directory was found gone at startup, so the deletion's
  caller may already have settled in an earlier process, but the project's
  footprint could not be read yet. A caller that is still pending stays pending,
  as in `removal`, and the footprint is never counted as zero: while any hold is
  unsized, docker-backend refuses every provision that needs disk as
  insufficient resources (WARN `disk admission withheld`; diskless work and
  `/health` are unaffected), and `/stats` reports `disk_withheld`, so
  providerd routes new provisions to a sibling backend serving the same SKU
  when there is one. The executor retries unsized holds on every pass, ahead
  of the other holds but alternating with them, so they never starve a removal
  whose close is pending, until a quota read sizes them.
- `residual`: the volume directory is durably gone; only the delete stage and
  the project ID remain while the zero-usage proof, the limit clear and the stage
  removal are retried. Until the hold completes, admission counts the
  project's block hard limit, or its block usage if larger
  (`fred_docker_backend_volume_delete_held_residual_mb`). The caller settles
  once that count has been published.

In every phase the name cannot be created again: a provision of the same lease
that needs that volume fails with reason `VolumeDeletePending` until the hold
completes. That is provider-side; the tenant cannot hasten it.

**What is kept.** The delete stage
`<volume_data_path>/.fred-xfs-delete-<project-id>-<managed-volume>`, the
project ID (never reused while held), and the project's limits. Two reasons are
the exception: after `stage_removal_failed` the limits are already cleared, and
after `quota_clear_failed` they may already be partly or fully cleared.

**Retry pacing.** `recovered`, `deadline` and `stopped` stay due on every pass:
they mean the attempt did not run, ran out of its time slice, or was stopped,
and the next attempt continues where it left off. An unsized hold stays due on
every pass too. Every other reason backs off from 30 seconds, doubling to at
most 30 minutes.

**Triage by reason.**

| `reason` | Meaning | Action |
|---|---|---|
| `recovered` | Found by `Start`; retried at once | None |
| `deadline` | A large tree ran out of its time slice, or the deletion was requested while `Start` ran | None. If it persists for hours, check whether something keeps writing under the volume path |
| `stopped` | The attempt's caller or the backend stopped | None |
| `removal_failed` | An entry could not be removed (I/O error) | Check the kernel log and filesystem health |
| `tree_changed` | The tree changed while it was being removed | Find and stop the process still writing under the volume path |
| `cross_device` | Another filesystem is mounted inside the volume | Unmount it. (A kernel older than Linux 5.8 would cause this on every volume; docker-backend refuses to start on one.) |
| `undeletable` | An entry refuses removal: an immutable or append-only attribute, or permissions | Inspect the entry named in `error` (`lsattr`); clear the attribute. The executor cannot finish this one alone |
| `cut_refused` | The deepest part of a tree could not be detached for removal | Confirm docker-backend runs with `CAP_FOWNER` as [DEPLOYMENT.md](DEPLOYMENT.md) requires |
| `writer_active` | Entries reappeared after removal | Find and stop the writer |
| `final_removal_failed` | The emptied volume directory could not be removed | Check it for attributes or a mount |
| `usage_unprovable` | The quota report could not prove the project's usage | Check `xfs_quota` and the `pquota` mount option. On an unsized hold this also keeps disk admission withheld |
| `usage_nonzero` | The project still uses blocks or inodes, usually an open but unlinked file | Find the process holding it (`lsof +L1` on the mount) and close it. If none holds one, inodes elsewhere may still be charged to this project: check `fred_docker_backend_volumes_with_projid_drift` and the warnings of the XFS project-ID audit (ENG-1118), which name the volume and project ID. Until the hold clears, admission keeps counting the project's hard limit (`fred_docker_backend_volume_delete_held_residual_mb`) |
| `quota_clear_failed` | The limit clear command failed | Check the quota control plane |
| `stage_removal_failed` | The delete stage itself could not be removed | Check that it is an empty directory without attributes |

**Never** delete or rename a delete stage; recreate or restore content at the
final path while its stage exists; clear the project's limits by hand while its
usage is nonzero; or set a project ID on any path (`chproj`) without first
confirming the ID is not another live volume's `.fred-project-id`. A delete
stage is the only record that makes the deletion safe to finish.

**A stage deleted by mistake.** Only for a deletion the backend reported as
held (a `volume delete held` line names it): a delete stage condemns its
volume. Stop docker-backend, recreate the stage with the project ID from that
line (`project_id`), which matches the volume's `.fred-project-id` if it still
has one, then start the backend. Create the stage only once the stop has
succeeded; the `&&` chain ends at the first failing step:

```bash
systemctl stop docker-backend &&
  mkdir -m 0700 <volume_data_path>/.fred-xfs-delete-<project-id>-<managed-volume> &&
  systemctl start docker-backend
```

`Start` registers it as a held deletion and the executor finishes it.

The stopped `-preflight-storage-identity-adoption` and
`-initialize-storage-identity` commands still refuse while any delete stage
exists.

**Upgrades and deploys.**

- The coordinated update's native drain proof runs against the stopped
  backend. It reports a pending close or operation whose remaining volume
  slots all carry a delete stage as `delete_held`, separately from `pending`
  (`docker.ClassifyStoppedDrain`, which reads the stopped volume root once,
  read-only). manifest-deploy's `update.py` must emit that count and treat it as
  non-blocking (ENG-1109): such work resumes by itself after the restart,
  because `Start` registers every stage as a held deletion. That is safe only
  when both the target and any rollback revision are hold-aware (this release
  or later); a pre-hold binary stops at every start on such a stage. All other
  pending work still blocks. The proof reads only the volume root: it does not
  check for remaining containers, and for a restore operation it sees only the
  destination lease's volumes, so a `delete_held` head may also owe other work
  that the restarted backend then retries.
- To check delete-held work before stopping a host:
  `fred_docker_backend_close_intents_delete_held` and
  `fred_docker_backend_volume_delete_holds{phase}` while it runs, and the
  `volume delete held` lines for the volume, stage and reason. With the backend
  stopped, each `<volume_data_path>/.fred-xfs-delete-<project-id>-<managed-volume>`
  directory is one held deletion. A deletion in progress when the backend
  stops is held, not abandoned, so it also shows up there.
- A host whose backend stops at every start because of a deletion failure on an
  earlier build: stop the backend, install this release, and start it. `Start`
  registers the stage as a held deletion instead of failing.
- The executor's first pass runs right after `Start`. Deployment automation that
  checks each host before moving on must wait at least about 90 seconds after a
  restart (manifest-deploy `docker_backend_settle_seconds`), so that a latch
  raised by that pass stops a rolling update on the first host.
- If the executor latches, the backend logs ERROR `volume delete cannot proceed;
  storage authority must be recovered by a fresh start` with `volume_id`,
  `delete_stage` and `project_id`, then exits 1. Follow
  [Interrupted managed-volume mutation at startup](#interrupted-managed-volume-mutation-at-startup)
  for that stage.

**Alerting.**

- Ticket on `sum(fred_docker_backend_volume_delete_holds) > 0` for one hour
  (`DockerBackendVolumeDeleteHeld`); do not page.
- A held close is expected to age while the executor finishes it. Page on the
  close-age gauge that leaves those closes out:
  `fred_docker_backend_oldest_unheld_close_intent_age_seconds > 900` for 5
  minutes (`DockerBackendCloseIntentAged`). It ages only the closes not waiting
  solely on held deletions and is 0 when there are none, so an old held close
  neither pages beside a young unheld one nor hides it. The deploy rule change
  from `oldest_close_intent_age_seconds` is part of ENG-1109.
- `fred_docker_backend_volume_delete_holds{phase="unsized"} > 0` means this host
  refuses disk-bearing provisions. Ticket if it lasts 15 minutes and check
  `xfs_quota` (`usage_unprovable`).

A hold does not join `/health` or readiness. A hold that keeps its caller
pending only keeps its own close or operation pending.

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
`.fred-xfs-delete-<project-id>-<managed-volume>` authority remains on disk and
the deletion is held: see [Held volume deletions](#held-volume-deletions).
Cleanup is automatic and strict. Restore quota-control-plane availability and
close any process that still holds an unlinked file from that project; the
hold executor retries without a restart. Do not remove or edit the authority
and do not clear its quota manually; it is the proof that makes retry safe. The
deletion completes only once final-path absence is durable, numeric block and
inode usage are both zero, the clear succeeds, and the authority itself is
durably removed.

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
its authority and holds the deletion for that volume, which the hold executor
retries while the backend serves. Since ENG-548,
`Destroy` also clears the inode limits
(`ihard`/`isoft`) alongside the block limits — a backend running a pre-ENG-548
build clears only `bhard`/`bsoft` and leaves `ihard` behind on downgrade; see
[Tenant hits its inode quota (`EDQUOT`)](#tenant-hits-its-inode-quota-edquot)
below.

---

## Tenant seccomp profile

docker-backend creates every tenant container under fred's own seccomp
profile, derived from Docker's default profile. Provision, restore, restart,
update, the redeploy that applies a custom domain, and the compensation that
rolls back a failed restart or update all use it. A container keeps the
profile it was created with, so containers created before an upgrade keep the
earlier one until they are recreated.

| Metric | Meaning | Action |
|---|---|---|
| `fred_docker_backend_tenant_containers_without_current_seccomp` | Running, restarting or paused tenant containers whose effective profile is not the current one, from the last completed census. The census runs after startup recovery and then every 10 minutes; it only reads. It logs a warning when the count changes and an info line while a nonzero count holds. | Restarting or updating a lease recreates its containers with the current profile. Expect a nonzero value after an upgrade until every lease has been recreated. |
| `fred_docker_backend_seccomp_census_total{outcome}` | Census passes. `error` leaves the gauge at its last completed value. | Investigate sustained errors; they usually mean the Docker API is unreachable or slow. |
| `fred_docker_backend_tenant_seccomp_profile_ready` | 1 when the profile was usable at its last request and the Docker daemon last reported seccomp support, 0 otherwise. | 0 means tenant launches fail: fred refuses them when it cannot build the profile, and a daemon without seccomp support refuses the profile (the backend then logs an error at startup). Startup and serving continue. Page. |
| `fred_docker_backend_seccomp_profile_refusals_total{sink}` | Launches refused because the profile could not be applied, by `provision`, `restore`, `restart`, `update`, `custom_domain`, `compensation` or `inspection`. A provision, restore, restart, update or custom-domain redeploy refused before it starts reports reason `Internal` to the tenant. | Any increase is a provider fault. Check the backend log; the profile is retried on every launch. |

Suggested alerts:

- `fred_docker_backend_tenant_seccomp_profile_ready == 0` for 5m: page.
- `increase(fred_docker_backend_seccomp_profile_refusals_total[15m]) > 0`: page.
- `fred_docker_backend_tenant_containers_without_current_seccomp > 0` for
  longer than the planned restart window after an upgrade: ticket.
- `increase(fred_docker_backend_seccomp_census_total{outcome="error"}[1h]) > 3`: ticket.

### Docker Engine upgrades and the profile

The profile is built from `github.com/moby/profiles/seccomp` v0.2.3, the
default profile of the supported Docker Engine, 29.7.2 (see DEPLOYMENT.md).
An Engine upgrade whose default profile differs needs a fred release that
regenerates the tenant profile from the matching version; until then tenant
containers keep the v0.2.3-derived profile.

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

## XFS project-ID audit

docker-backend audits managed XFS volumes once a day and reports inodes whose
project attributes differ from their volume's. The audit only reads: it never
changes a volume, and fred does not repair what it reports. It skips volumes
being deleted. It opens tenant directories and regular files read-only, at
most 10,000 per second; tenants can observe those opens.

| Metric | Meaning | Action |
|---|---|---|
| `fred_docker_backend_volume_projid_audit_total{outcome}` | Volumes audited, by `clean`, `drift`, `too_deep`, `incomplete`, `changed`, `error` or `skipped` (a volume being deleted). The first pass runs 10 minutes after startup, then daily, one volume at a time; a pass stops after 30 minutes and the next resumes after the last volume audited. Each volume gets 2 minutes. | `drift`: investigate the volume named in the warning log, which carries its project ID and the drifted directory and file counts. `too_deep`: part of the volume lies deeper than the audit walks and was not audited; investigate. `incomplete` (out of time, or a file another holder kept from opening) and `changed` (the tree changed during the walk) are audited again on later passes; a volume that is `incomplete` on every pass is never fully audited, so investigate it too. |
| `fred_docker_backend_volumes_with_projid_drift` | Volumes with recorded drift. A recorded drift stays until a later audit of the volume finishes undisturbed and finds it clean, or the volume is deleted; an audit that does not finish, or a skip, keeps it. The record is in memory, so after a restart the gauge counts again as the audit revisits each volume. | Investigate each volume. |

Suggested alerts:

- `fred_docker_backend_volumes_with_projid_drift > 0`: ticket.
- `increase(fred_docker_backend_volume_projid_audit_total{outcome="too_deep"}[1d]) > 0`: ticket.

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

## Load-test failure attribution

`operation_completion_pending` is a validated earlier callback completion in a
lease's FIFO. It preserves the current durable attempt and does not count as a
backend availability failure. Inspect callback delivery and the exact operation
before treating a repeated pending completion as corruption. Other intent
conflicts, foreign storage evidence and malformed responses remain failures.

Tenant log admission warnings include a bounded `reason`: `request_queue_full`,
`request_canceled`, `response_wait_expired`, or `backend_read_capacity`. The last
also names the backend and lease. Correlate these with timeout responses and
circuit transitions; an HTTP 503 alone does not identify which budget refused
the read. These warnings do not cover every timeout phase.

Global provision reservation refusals include incremental CPU, memory and disk
demand beside the headroom observed under the reservation lock. The same atomic
capacity check still owns admission. Sampled CPU utilization alone cannot
identify a refusal caused by memory, disk or concurrent reservations.

## Backend unreachable during reconciliation

A backend that is configured but not answering `GET /provisions` or
`GET /retentions` — down, wedged, or partitioned — no longer aborts the whole
reconciliation sweep. Fred marks the inventory
incomplete and retries on the next cycle. Existing workloads continue serving;
exact callbacks and safely evidenced status/cleanup work can continue. Once a
complete inventory has established the durable baseline for this topology, a
partial sweep can also admit genuinely new recordless `PENDING` work on the
typed set of nodes that answered **both** inventories. It never treats a silent
node as evidence about the leases it may hold.

Full inventory uses a separate bounded recovery lane: one complete provision or
retention walk at a time per HTTP client, with queueing and all pages covered by
the configured backend timeout. An open tenant circuit cannot suppress that
observation. Successful inventory does not reset the circuit or authorize tenant
calls; failed inventory does not trip it. An unavailable backend therefore still
makes the sweep incomplete, and identity, completeness and placement projection
checks remain mandatory. Filtered `/workloads` reads still use the tenant circuit,
but confirmed leases without an unresolved attempt query only their recorded
owner; an unrelated silent node
cannot add a warning to an otherwise complete owner-specific result.


That availability rule assumes the preceding inventory sweep ended cleanly. If
providerd restarts with an interrupted-sweep marker in `placements.db`, a lost
positive observation may not yet be represented by any placement row. Fred then
withholds fresh lease side effects—even owner-affine maintenance and
cleanup—until every backend that reported a positive during the interrupted
sweep chain answers both endpoints again, matching its storage pin, and the
projection durably accounts for every positive. Fred journals each reporter
before its positive can take effect, so a backend that was already down during
the interrupted sweep does not keep the whole provider fenced. Every configured
backend is still needed when the interrupted chain held evidence Fred could not
attribute to its reporter (a failed refresh, an identity that does not match
the pin, malformed rows, a lease in both endpoints of one backend, or a rejected
endpoint response), and for a marker written by an earlier revision. The WARN
log `placement inventory recovery pending` names the `reporters` (or every
configured backend) recovery is waiting for.
`/readyz` then reports `placement inventory recovery pending`, and
`fred_placement_inventory_recovery_pending` is 1. A fenced backend is the one
reporter recovery does not wait for: it is recorded instead, and while it is
recorded no lease without a placement row is admitted (`/readyz`:
`placement inventory waits on a fenced backend`). The coverage proof can retire
this inherited fence even when individual leases remain quarantined; it cannot
establish a new admission baseline or prove an empty backend.
Exact authenticated callback settlement and replay of already-durable attempts
or maintenance commands remain available. An increase in
`fred_placement_write_failures_total` accompanied by “inventory sweep marker
remains recovery-required” identifies a failed marker-clear write.
The marker is written after chain inventory succeeds and immediately before
backend inventory reads. Each successful endpoint response is held as an opaque
one-shot sweep receipt, and the sweep cannot seal until every receipt is paired
with its other endpoint or conservatively rejected as untrusted. This is why a
crash or local collection failure cannot silently reinterpret a buffered
positive as absence.
An ordinary in-flight operation whose trusted provision report exactly matches
its durable owner/attempt generation does not trigger this global recovery
state, so unrelated leases may still use healthy answering backends. Retention,
untrusted identity, novel reporters, and generation/principal contradictions
remain unresolved and fail closed.

Docker reads each retention page's records and continuation cursor in one
identity-bound database snapshot. A concurrent restore finalizer cannot remove
a selected row between key selection and decoding. Corrupt records and invalid
identities still fail the page; they are never skipped as if absent. Separate
pages and the provision/retention endpoints remain separate observations, so
the existing paired-inventory and lifecycle checks still apply.

**Symptoms**

- `fred_reconciler_backend_fetch_total{backend="X",outcome!="ok"}` rising.
  Bundled HTTP inventory reports `error` for failed or timed-out walks, including
  queue expiry. The compatibility `circuit_open` outcome can still describe
  custom clients; bundled full inventory no longer short-circuits on that state
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
| Positively reported by an inventory endpoint Fred rejected | The raw membership fact is persisted as an unusable `untrusted_positive` quarantine, even though its payload cannot establish ownership. A validated same-storage provision/retention overlap can preserve its existing confirmed sole owner when generation and principal already agree; it grants no new ownership or lifecycle authority. Other ambiguity rejects only that lease. Malformed responses, missing or inconsistent endpoint storage identities, and a storage identity conflicting with the durable backend pin reject the whole backend response |
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
   inventory absence alone never clears attempts or conflict quarantine. A sole
   `untrusted_positive` candidate can regain its owner when a later sealed
   observation covers that lease across **all configured backends**: paired,
   identity-valid responses, the same sole trusted reporter, and absence on each
   peer. An unrelated lease's overlap does not block that proof. A missing peer,
   continued ambiguity for this lease, silence, a different reporter, multiple
   candidates, unknown ownership, or an ordinary conflict cannot establish a
   single owner this way. An in-flight operation need not finish before that
   narrow quarantine can be removed: the Store must match its existing typed
   operation generation, backend and tenant/provider principal to the fresh
   provision observation. It preserves the complete attempt, callback route
   and lifecycle record; it does not confirm the attempt or settle the callback.
   The current sweep, record revision, maintenance, restore-source and active
   settlement-claim fences still apply. Normal authenticated settlement or
   independently authorized reconciliation must then complete the operation.
   Separately, terminal pruning can remove a known-owner
   quarantine after paired, identity-valid absence from every recorded candidate
   and an exact chain read proving CLOSED, REJECTED, or EXPIRED. It requires no
   unresolved attempt, maintenance, or restore claim and rechecks the exact
   placement revision under lease exclusion. Missing or unknown owners remain
   fenced; inventory absence alone is insufficient.
3. If it is gone for good, that is a **removal**, not an outage — see the next
   section. Do not leave it configured-but-absent indefinitely: PENDING leases on
   it are on a ~30-minute chain expiry clock the whole time.

A healthy endpoint or complete inventory is not proof that every lease has
settled. If `exact durable placement generation is unavailable for operation
settlement` repeats, correlate the callback's lease UUID with quarantine logs,
in-flight work and physical inventory after a later authoritative sweep. A
deferred close's `dispatched` record likewise does not prove teardown. Inventory
waits can last until the next sweep; retain the original close deadline failure
even if a later callback or retention record establishes eventual completion.

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
one candidate. That narrow one-candidate quarantine can regain its owner from a
later identity-valid matching positive from the same backend, with paired
responses from every configured backend and absence of that lease on every
peer. Ambiguity for another lease does not block this proof, but a missing peer
or silence cannot establish ownership. For an in-flight operation, exact typed
generation and principal matching can remove only the observation quarantine,
preserving its existing attempt and lifecycle until normal settlement. This
does not repair ordinary conflicts or grant cleanup authority. Separately, a fully known candidate set
can be pruned after paired, identity-valid absence from every candidate and an
exact chain read proving CLOSED, REJECTED, or EXPIRED, with no unresolved attempt,
maintenance, or restore claim. This includes a sole `untrusted_positive`
candidate. The proof is tied to the current placement revision and consumed
under lease exclusion. Unknown owners or incomplete evidence still require
operator investigation; absence alone never authorizes deletion.

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

### Reclaiming retained-data / stuck-reaping volumes

A volume whose exact retention-finalizer destroy fails under a degraded
filesystem/store remains represented by a **reaping tombstone**: its footprint
keeps counting in the admission pool (so it never silently over-admits) and the
retention sweep **auto-retries** that exact destroy every interval. There is no
global orphan-volume collector. A valid `fred-*` path with no exact operation,
close, release, or retention authority is preserved for operator attribution;
startup and periodic maintenance do not infer that it is disposable.

- **Signal.** `fred_docker_backend_retention_reaping_bytes` / `..._retention_reaping_leases` > 0
  is the stuck-volume signal (these footprints **are** counted in the admission pool, so there is
  no over-admit — they pin capacity until reclaimed). A transient EBUSY clears within one sweep;
  a value sustained across several sweeps is a stuck volume. `..._retention_leaked_total` is a
  broader event counter — it increments on a failed destroy (which drives
  `reaping_bytes`) **and** on a rollback uncommitted-revert (which keeps its footprint counted as
  *live* and self-heals on the next `reconcileRestoring` sweep, NO stuck volume). So a rising
  `leaked_total` with `reaping_bytes`/`reaping_leases` flat needs no action; only sustained
  `reaping_bytes` is actionable here.
- **Diagnose.** Find the volume(s): `ls <volume_data_path> | grep -E 'fred-retained-|fred-'`.
  Check why destroy fails — a container still bind-mounting it (`docker ps`, then stop it), or a
  filesystem error (`dmesg`).
- **Unattributed canonical volume.** If a `fred-*` path has no matching
  container and no exact durable operation, close, active release, or retention
  row, leave it untouched until its lineage is established from backups/audit
  data. Automatic recovery deliberately leaks this storage instead of risking
  tenant-data destruction. Remove it manually only after proving the encoded
  lease and volume generation are terminal; a name or an empty current
  inventory is not such proof.
- **First check whether the reaper can prove ownership.**
  `..._retention_reap_skips_total{reason="claim_unreadable"}` means the reaper could not prove
  what it may destroy, or that its destroys finished: the retention store or the volume root is
  unreadable, the backend's storage authority was withdrawn, the tombstone authority is
  unavailable, or a fresh read after the destroys failed or still found volumes. Its `reaping:`
  ERROR log names which; fix that first. An unreadable store also blocks the close path.
  `reason="owner_claimed"` is the deliberate live-lease hold described below. A restore is not a
  possible cause: the permanent close boundary that creates a retention row prevents operation
  admission from targeting that lease for restore (ENG-659).
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
- **Historical v0.13 give-up tombstones remain recoverable.** Older binaries
  could abandon a close into `reaping`, including with an empty stored name
  list. Current code never turns a retry count into that decision: it keeps the
  non-expiring close intent and conservative live reservation until exact
  physical evidence permits terminal settlement. During upgrade, a historical
  tombstone derives its destroy set from current disk namespaces and the
  ownership table, so its old stored list is not destructive authority.
- **`reason="owner_claimed"` is a deliberate hold, and there is nothing to unblock.** A
  tombstoned name belongs to a **live provision** (or another lease's retained record). This
  is primarily a historical-row shape: an older give-up deleted the provision while the
  lease was still ACTIVE on chain, the reconciler re-provisioned it, and a fresh volume now
  sits under the same namespace. The
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

When a container crashes or fails health checks, the lease moves to `failed`. The reconciler detects the chain ↔ provision mismatch and re-provisions the lease on its next pass. It closes the lease on-chain instead (reason `workload failed repeatedly`) only when the backend reports an exhausted consecutive-failure budget (`terminal_budget.verdict` = `exhausted`, ENG-799). `fail_count` is a lifetime diagnostic and never decides.

**What counts.** The docker-backend counts a failure only when the Docker event stream saw the tenant's own container exit while the lease was ready, or during the startup verification of a provision whose launch completed (ENG-1125): the same continuous subscription saw that run start and saw no API signal to it. Any exit status counts, including `137`, `143` and an OOM kill. A counted failure exhausts the budget only when it is the third or later in a row **and** lands at least 30 minutes after the streak's first counted failure, so a burst of deaths during an outage (the host, the network or a dependency flapping) re-provisions the lease but never closes it on its own; the streak is re-evaluated at its next counted failure. Every exit from ready (a failure, a restart, an update) resets the count, and the streak's start, once the lease has been ready for ten minutes; the recorded count and verdict shown on `/status` change only at such transitions, never with time alone. An accepted tenant restart or update also resets it; a custom-domain redeploy does not. These never count:

- `docker kill`, `docker stop`, or the daemon stopping a container (a `kill` event precedes the death): attribution `disruption`;
- a container that vanished or is `removing`/`dead`: `disruption`;
- a death found only by the periodic reconcile sweep, or of a run that started before the event stream (re)connected: `unknown`;
- a restart, update or restore outcome, whether it rolled back or not: `maintenance`;
- a refusal before any substrate effect (image admission or pull), an internal error, a cohort that diverged from its release, a container start Docker refused (`ContainerStartFailed`), a startup crash in a launch that `compose up` reported failed, a startup crash after a degraded launch: `platform`. A launch is degraded when the platform skipped part of its own preparation: writable-path seeding (a path it could not extract, a bind source that failed its confinement check, stale content it could not clear, or a writable-path volume it could not create), or the image's volume-owner or writable-path detection (the `failed to detect volume owner` / `failed to detect writable paths` warnings);
- a health check that never passed during startup: `unhealthy`;
- a provision failure settled only by the backend's periodic recovery: not recorded by the actor at all;
- a host reboot, or a backend restart (the budget lives in memory and resets; this can only delay a close).

Every failure the lease actor records increments `fred_docker_backend_lease_failures_total{attribution}` (a failed provision found at cold recovery, and a maintenance outcome converged without an actor, are not counted there). providerd counts each Failed ACTIVE lease's verdict per sweep in `fred_reconciler_terminal_verdicts_total{verdict}`. A tenant sees the same budget as `terminal_budget` on `/status` and `/provision`.

**The event stream.** Counting depends on the docker-backend's container event loop staying subscribed. Its reader only records `start`, `kill` and `die` and queues each death; a separate dispatcher re-verifies storage identity and routes it, so a slow death never stalls the subscription. The reader never logs, so a slow log consumer cannot stall it either: deaths dropped from a full queue are logged by a separate reporter as `container deaths dropped` with their count, the first at once and then at most one line every 10s. A stream error or a transient storage-verification failure makes the loop back off (1s, doubling to 30s) and reconnect with a fresh session; only shutdown or withdrawn storage authority stops it. `fred_docker_backend_container_event_stream_total{outcome}` counts `connected`, `reconnect` and `exited`. A death the loop cannot dispatch (queue full, identity unverifiable) is counted in `fred_docker_backend_die_event_dropped_total{source="event_loop"}` and later found by the sweep as `unknown`. `fred_docker_backend_container_death_queue_depth` is the number of deaths waiting for the dispatcher (capacity 4096): a death the sweep reaches first is also `unknown`, and no counter moves until the queue is full. Each re-verification is bounded at 10s, but that bound covers only the verification: the wait in the queue, and for the backend storage-verification lock while another verifier holds it, come on top.

**Residuals to know.**
- **Host OOM.** A host-wide OOM kill also sets Docker's `OOMKilled` (the cgroup's `oom_kill` counts kills by any OOM killer), so it counts like the workload exceeding its own limit. Correlate with the host OOM alert before blaming the tenant.
- **Kills that bypass the Docker API.** A host `kill(1)` of the container's process, a `ctr`/containerd task kill, stopping its systemd scope, or a shim crash emit no `kill` event, so the death is indistinguishable from the workload's own exit and counts. Stop or kill tenant containers only with `docker stop`/`docker kill`.
- **Dropped events.** dockerd gives each event subscriber a 1,024-event buffer and silently skips any event the subscriber has not taken within 100ms once that buffer is full; moby exposes no drop signal. If a `kill` is skipped and the following `die` is delivered, an operator stop counts as the tenant's failure. The reader never blocks on a death or on log output, so this needs a sustained event burst, and a close still needs three such misattributions in one streak spanning at least 30 minutes.
- **Stream gaps.** Deaths while the stream is down, and the first death of any run that started before it (re)connected, are `unknown` and never count. A backend whose stream keeps reconnecting can therefore re-provision a crash-looping lease indefinitely, billed; watch the `reconnect` rate below.
- **Startup crashes (ENG-1125).** A container that exits during startup verification fails the provision at once: the docker-backend removes the attempt's containers and the volumes it created and publishes `failed` with `ContainerExited`. An exit of a launch the platform completed counts like the death of a ready workload (an observed run with no signal), so a broken image that keeps crashing at startup is closed once its streak is three crashes long and 30 minutes old. When `compose up` itself fails after its requests completed (a `depends_on` dependency gated by `service_healthy` exits or turns unhealthy, or Docker refuses to start a container, for example an entrypoint missing from the image: `ContainerStartFailed`), the backend decides from what the containers show, never from Compose's error, but the launch was not completed, so its outcome never counts (attribution `platform`): an exit there, even an observed run, may follow from a service the platform never started. A health check that never passes (`HealthCheckFailed`, attribution `unhealthy`), a refused start, a crash in a launch `compose up` reported failed and a crash after a degraded launch (attribution `platform`) fail definitely but never count, so such a lease is re-provisioned every pass, billed, until the tenant acts; never alert on `unhealthy`.
  - *Health timing.* A health check that never passes is reported only at `provision_timeout` minus one minute, so with the deployed `provision_timeout` a PENDING lease is rejected first by providerd's 10-minute callback timeout (`callback timeout`), and an ACTIVE one stays `provisioning` until then on every attempt. A health-gated container that reported healthy once is from then on judged healthy while it runs, by the startup watch and by every later outcome check of the attempt (and by recovery in the same backend process), so a brief `unhealthy` flap while the rest of the stack starts no longer fails a provision. Neither rule covers a `depends_on` dependency gated by `service_healthy`: Compose judges that dependency's health itself during `compose up`, so a flap there still fails the deployment (`HealthCheckFailed` when it is still unhealthy as the backend looks, otherwise it is left to recovery), and a dependency that never becomes healthy ends `Internal` (`container creation failed`) at `provision_timeout`.
  - *Left to recovery.* When the backend cannot settle the attempt live (an earlier step of the attempt failed, including a failed image detection; leftover volume state of the lease; a failed inspection; a rejected launch whose containers show no failure), it removes nothing and the attempt stays `provisioning` until its periodic recovery settles it: within one `reconcile_interval` when one of its containers exited or its cohort is partial, otherwise at `provision_timeout`. A rejected launch whose containers are all running (and healthy where a health check gates them) is adopted `ready` by the next recovery pass, under the checks recovery applies to any interrupted provision. A rollback step that fails after the containers were removed (a volume it could not destroy, a failed re-list or re-read) leaves no container, so recovery cannot tell the attempt from one whose containers have not appeared yet: it settles `failed` only at `provision_timeout`, and a PENDING lease is rejected first with `callback timeout`. None of these count. A lease left `failed` this way, or by a definite startup failure, has no container until providerd re-provisions it; recovery logs its empty cohort (`recovered container cohort differs from durable release`) only at debug.
- **Restores.** A restore whose containers fail startup verification is settled by periodic recovery, never live, and never counts.
- **Validation refusals.** The immediate close of an ACTIVE lease whose re-provision the backend refuses with a validation error (`400`: unknown SKU, image not allowed, invalid manifest) is a separate path, unchanged by ENG-799 (ENG-800).
- **Older or third-party backends.** A backend that does not report `terminal_budget` never has a crash-looping lease closed: it is re-provisioned every pass and stays billed (`verdict="absent"`).

**Alerts.** Write them from these series; none of them should page on a tenant's own crashes or updates.
- `increase(fred_reconciler_terminal_verdicts_total{verdict=~"absent|unknown"}[30m]) > 0` for 1h (ticket): a backend reports no usable budget, so crash loops on it are never closed.
- `increase(fred_docker_backend_container_event_stream_total{outcome="reconnect"}[15m]) > 3` (ticket): the docker-backend's event stream is flapping; deaths in its gaps are `unknown` and never count. `outcome="exited"` increases only at shutdown or after storage authority is withdrawn, which the backend's own health alerts already cover.
- `increase(fred_docker_backend_lease_failures_total{attribution="platform"}[1h]) > 5` (ticket; tune the threshold to your fleet): the platform is failing tenant workloads, which never counts against them. A single lease whose start Docker refuses (for example, a tenant image without its entrypoint), or whose `compose up` keeps failing, adds one `platform` failure every reconcile pass (about 30 an hour at a 2-minute interval), as a never-healthy or degraded loop does to its own attribution; set the threshold above what one such lease produces, and look for it per lease before blaming the host.
- The deployed `ProvisioningFailureSustained` rule (manifest-deploy) assumes one tenant's broken image fails once; since ENG-1125 such loops fail definitely every pass and feed `fred_provisioner_provisioning_total{outcome="failed"}`, so the rule needs recalibration (ENG-1109).
- Do not alert on `unknown` or `disruption` alone: `unknown` follows every backend restart and stream reconnect (the first death of every run that predates the stream), and `disruption` follows every operator `docker stop` or `docker kill`. A sustained rise in `unknown` with no reconnects means deaths reach only the sweep; check `fred_docker_backend_container_death_queue_depth` (a dispatcher that falls behind turns live deaths into `unknown` without dropping any) and `die_event_dropped_total` (deaths the loop dropped).
- Never alert on `maintenance`, `tenant_workload` or `unhealthy`: failed tenant updates, the tenant's own crashes and health checks that never pass are ordinary work.
- The metrics carry no lease label. To find a lease that keeps failing without being closed, read `terminal_budget` from `GET /v1/leases/{uuid}/status`, or the docker-backend's `tenant workload failure counted` warnings and the reconciler's re-provision log lines for that lease.

**To investigate:**
1. `curl http://providerd/v1/leases/{uuid}/provision` (with auth) returns `status`, `reason`, `message`, `fail_count` and `terminal_budget`. It carries no `last_error`, exit code or logs: those are operator-side since ENG-508. Read them from the docker-backend's diagnostics store and from its WARN log `tenant workload failure counted` (`exit_code`, `oom_killed`, `provenance`, `consecutive_failures`, `streak_age`, `min_streak_span`, `budget_exhausted`). A line with `consecutive_failures` of 3 or more and `budget_exhausted=false` is a streak younger than `min_streak_span`.
2. `curl http://providerd/v1/leases/{uuid}/logs?tail=200` — full stdout/stderr.
3. Diagnostics persist for 7 days (configurable via `diagnostics_max_age`) even after the provision is gone.

**Common patterns** (from the operator log and diagnostics above):
- `exit_code=137 oom_killed=true` → SKU memory too small, or app has a leak. Recommend a larger SKU or fix the app.
- `exit_code=1` early in startup → bad manifest configuration. Check the logs for stack traces.
- Health check failures → `health_check.start_period` may be too short. Update the manifest.

**On-chain callback messages are intentionally generic** (`container exited unexpectedly` / `internal error`) to prevent leaking secrets. Full diagnostics only flow through the authenticated API.

---

## bbolt database recovery

Fred uses bbolt (an embedded key-value store) for several persistent structures:

| Path | Purpose | Loss impact |
|---|---|---|
| `token_tracker_db_path` | Replay protection for tenant tokens | Normal restart preserves consumed tokens. Cache loss may permit replay until signed expiry, up to 40s after initial acceptance when the permitted 10s future skew is used |
| `payload_store_db_path` | The manifest of every PENDING lease, the current manifest of every ACTIVE lease (used to re-provision it), and the exact bytes in-flight provision attempts re-send | Tenants can re-upload only a PENDING lease's original manifest. An ACTIVE lease that needs re-provisioning stays deferred (`payload not available`) and an in-flight attempt stays unresolved. Restore it with `placement_store_db_path`, from the same moment |
| `placement_store_db_path` | Provider-bound durable confirmed and attempted lease→backend ownership, ordinary and rejected-positive (`untrusted_positive`) quarantine, immutable name→storage UUID history, and the topology-bound inventory baseline | Critical, non-derivable, and not hot-swappable. Normal startup refuses an absent, empty, unprepared, or differently provider-bound file and performs no creation or migration. Restore the exact database only while stopped; fresh initialization is only for a genuinely new provider with zero total chain lease history, never recovery after loss |
| `<docker>/callbacks.db` | Durable provision/restore operation rows (Pending/Succeeded/Failed) with exact resource profiles (including Docker's pinned diskless scratch), exact restart/update/custom-domain maintenance intents, non-expiring Docker close intents, the pending callback FIFO, immutable maintenance source plans, unresolved physical-volume launches, and exact image-inspection cleanup obligations. A terminal operation row remains after callback delivery until an authorized successor atomically retires it. Causal/close rows and exact operation/maintenance completions do not age out; typed lifecycle observations age out at `callback_max_age`. Pre-identity v0.13 callback rows are a stopped-upgrade condition, never runtime queue entries | Accepted and terminal operation decisions, replacement and destructive-cleanup authority, immutable sizing, and queued callback evidence are not recreated. Loss can hide a substrate mutation, erase the exact outcome required to finish a restore handback, make a partial replacement or close indistinguishable from unexplained cohort loss, or strand a provider-side placement attempt; restore it with the matching release/retention stores and backend substrate |
| `<docker>/diagnostics.db` | Exact-attempt captures and published lease failure diagnostics (last_error, bounded logs) | Existing captures are lost after stopped recreation; new failures can record again. Capture write failures retain failed containers and pending failure publication for retry. Open/create requires an unsymlinked, single-link regular file with exact mode `0600`, but diagnostics is not storage-identity authority and is not continuously re-attested |
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
to 90 days. Every write first preserves the index-latest row and the most recent
active row; it then removes expired
disposable audit rows before the oldest fresh disposable rows until the encoded
per-lease history fits 32 MiB. A capacity check runs before a provision or
restore may mutate tenant substrate, and the write repeats
the same plan transactionally. If the protected authority alone cannot fit, the
operation is refused before mutation. Under extreme pressure a failed release
may omit its optional curated reason/message while retaining the terminal
`failed` state. `GET /releases/{lease_uuid}` has a separate 48 MiB response
budget because projection can add a default failure reason that was absent on
disk. Capacity compaction can therefore remove audit history before its age
expires; it never removes recovery or cleanup authority.

### Permanent callback UUID capacity

The callback journal reserves one permanent aggregate slot the first time it
admits an operation, maintenance command, or close for a lease UUID. That slot
is never TTL-pruned. A successful close replaces the aggregate head with a
permanent closed receipt, which is what prevents a delayed Docker Create or an
old request from resurrecting a retired UUID. Deleting the slot or receipt is
therefore not capacity recovery; it removes causal authority.

The fixed limit is 100,000 unique lease UUIDs per backend storage lineage. A
separate durable 100,000-entry budget is shared by operation and maintenance
receipt reservations: admission consumes one before substrate mutation,
settlement needs no fresh capacity, canceling an unstarted maintenance intent
returns its unused reservation, and successful close reclaims both receipt
classes behind the stronger closed-UUID fence. Neither counter is the stopped
inspector's logical-row count; nested history buckets, outbox deliveries, and
aggregate heads are additional rows. The common scale is a defensible fail-safe,
not a fleet-sizing promise: permanent UUID churn consumes the identity budget,
while long-lived completed-operation and maintenance churn consumes the receipt
budget. The running backend exports both used/limit pairs:
`fred_docker_backend_lease_mutation_uuid_slots` /
`fred_docker_backend_lease_mutation_uuid_slot_limit` and
`fred_docker_backend_callback_receipt_reservations` /
`fred_docker_backend_callback_receipt_reservation_limit`. Stopped read-only
inspection exposes the same counters as `LeaseMutationUUIDSlots`,
`LeaseMutationUUIDSlotLimit`, `CallbackReceiptReservations`, and
`CallbackReceiptReservationLimit` in `CallbackStoreInspection`.

At 80%, or earlier if the projected exhaustion date enters the deployment
horizon:

1. Confirm `docker-backend /health` is clean and graph the slot gauge's change
   over a representative lease-churn window. The gauge survives process restart
   because it is read from bbolt.
2. Preserve a stopped backup of the complete backend lineage before any journal
   change: `callbacks.db`, `releases.db`, `retention.db`, both identity markers,
   and substrate evidence.
3. Add backend capacity for new placement if needed, and prepare a reviewed Fred
   release with a higher fixed ceiling. Increasing the code ceiling needs no row
   rewrite; test health and stopped inspection at the new supported scale before
   rollout. Do not lower it below the durable used count.
4. Deploy before exhaustion. At the hard limit, admission of a never-before-seen
   UUID returns a coded capacity refusal before substrate side effects; work for
   already-reserved UUIDs, including close completion, remains admissible.

Never delete or TTL-prune UUID slots, closed receipts, or the callback database,
and never replace it with an empty file. If the limit is already reached, keep
the node available for its reserved UUIDs, route genuinely new leases to other
backends, and deploy the reviewed ceiling increase.

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

### An unsized legacy predecessor blocks Docker recovery

An `unsized pending provision predecessor` error, or a refusal stating that a
legacy predecessor has no durable runtime authority and no surviving cohort to
freeze, means recovery cannot attribute the old generation's complete resource
and cleanup authority. This is not ordinary young-operation or cleanup-retry
uncertainty. Startup fails on that backend; a running backend aborts that
recovery pass before the operation-recovery phase. Other configured backends
remain independent.

The supported stopped cutover freezes a complete v0.13 cohort before serving
new requests. Current provision admission also requires the predecessor's
principal and frozen sizing before it can issue an executable capacity token.
A Started operation alongside an unbackfilled predecessor therefore indicates
an unsupported intermediate writer or damaged/mixed-time state, not a normal
admission window. A candidate-only resource projection plus
`HoldUnaccountedFootprint` would block new allocations but would not supply
settlement, close, or quota authority; do not use it to force startup.

Recovery procedure:

1. Stop mutation ingress to the affected backend and preserve its current
   `callbacks.db`, `releases.db`, `retention.db`, both storage-identity markers,
   Docker metadata, and managed volumes as one coherent evidence set. Fence
   every other process that could write the same lineage. Never initialize a
   replacement database or replay Compose to make the error disappear.
2. If strict inventory or container inspection failed, repair that read or
   storage fault without changing durable identity. If the existing exact
   candidate naturally becomes fully Ready, retry normal startup: the strict
   classifier can supersede the unsized predecessor with the candidate's
   complete authority. Do not relabel, recreate, or resize containers to
   manufacture this evidence; an empty or partial inventory is insufficient.
3. Otherwise, restore only a verified coherent stopped snapshot containing the
   entire matching backend lineage and substrate, following the
   [backup and rollback requirements](DEPLOYMENT.md#upgrading-from-v0130).
   Check accepted callbacks, provider placement, and chain lifecycle changes
   since that snapshot before choosing it. In particular, restoring the
   pre-upgrade files after later accepted operations is not a safe general
   repair, and mixing journal times is never valid.
4. If neither exact-Ready evidence nor a causally safe matching snapshot exists,
   keep the backend fenced and preserve the data. This release has no tool that
   reconstructs the missing backend predecessor authority. A separately
   reviewed, proof-bearing forward-repair procedure is required; the provider's
   `placement-repair` tool cannot repair backend journals. Do not fill in Items,
   delete the pending intent, or issue cleanup-only close as a workaround.

### A pending or corrupt Docker maintenance intent

The maintenance-tagged row in `callback_lease_mutation_heads` is the write-ahead
owner for one exact restart, update, or custom-domain replacement. Its
store-assigned canonical
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

This compatibility applies to the existing workload, not to new tokenless
provision or restore requests. Those require a canonical UUIDv4 `operation_id`
in the completion URL and its matching resolved lifecycle route before durable
admission, even when restoring legacy retained data. A `400` for a missing
operation identity means the caller must use the upgraded protocol; do not edit
journals, relabel containers, or disable HMAC verification to work around it.
“Tokenless” refers to callback identity, not request authentication.

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

The close-tagged row in `callback_lease_mutation_heads` is a finalizer journal,
not an ordinary callback queue. It is committed before destructive work and intentionally survives
container absence, process restarts, `callback_max_age`, and transient cleanup
errors. Do not infer from zero containers that it is stale.

Recovery, live Deprovision admission/settlement, and Restore's intent-to-
`restoring` admission and rollback handback share a backend-local
recovery-snapshot guard. Recovery holds the exclusive side only through
inventory and matching provision/pool publication; live paths hold the shared
side only for authority capture and durable handoffs, not destructive substrate
work. This prevents a completed close or fully rolled-back restore from racing
stale inventory publication. Provision validation remains available during
recovery, but its short accepted-intent-to-projection handoff can wait for the
current publication; Restore admission can wait at its corresponding handoff.

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
no safe tenant/reaping tombstone exists. A principal-bound cleanup-only receipt
keeps the complete tenant/provider pair and refuses any late substrate whose
labels differ. The explicitly weaker orphan receipt exists only when no
principal witness survived; its cleanup trust boundary is the authenticated
provider close, reserved `fred.*` managed labels, exact retired UUID, and the
attested backend/storage identity. A half-present principal is corrupt. In both
cases the durable
`execution_generation` field and the `durable close recovery remains pending`
log identify progress. The wire field retains its historical JSON name for
upgrade compatibility; it is a generation, not a retry budget, and no value
causes give-up.

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
settlement. Settlement atomically records Failed on the exact operation row and
enqueues its callback; that terminal row remains after delivery and drives any
handback retry. Its absence is an authority error. An exact matching active
Release is instead proof that restore
committed: keep the Release, transition a matching Pending operation to
Succeeded, and delete
the source finalizer when a live Ready generation proves full handoff. With zero
survivors, recovery instead creates a conservative Failed destination, retains
its exact allocation, and keeps the source finalizer as durable tenant/provider
identity across restarts. After the exact restore operation is
Succeeded, or after an authorized successor atomically retires that history,
only a plain,
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
   execution generation, and active-release version/digest. Treat both callback
   URLs as sensitive causal evidence and keep them out of logs and tickets.
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
   exists. Classify each preserved candidate offline with `placement-repair
   -classify`, and inspect affected lease rows as well (`-list`, `-inspect
   -lease`) when a write's commit outcome is unknown. The tool has no
   database-path flag; it reads the file `placement_store_db_path` names. Point
   that key at the candidate in a copy of the stopped config, or override it:
   `PROVIDER_PLACEMENT_STORE_DB_PATH=<candidate> placement-repair -config
   /etc/fred/config.yaml -classify` (the override applies because the key is in
   the YAML). The candidate path must be absolute and clean, and the file a
   regular file with exact mode `0600` and a single hard link.
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
   owner. A sole such candidate can regain its owner from a later matching
   trusted reporter with paired, identity-valid coverage of every configured
   backend and absence on every peer. An unrelated lease's ambiguity does not
   block this proof. Separately, a fully known candidate set can be pruned by
   reconciliation after exact dual-endpoint absence from every candidate plus
   chain-terminal proof, with no unresolved attempt, maintenance, or restore
   claim and with the current revision protected by lease exclusion. Unknown
   owners, missing evidence, and chain absence do not authorize pruning.
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
4. **For `placement_store_db_path`, restore the exact provider-bound database before starting providerd.** Normal startup never creates, initializes, or migrates a missing/unprepared file. Current chain/backend silence cannot recover a lost authority: if the provider has any chain lease history, including terminal history, restore the database. The explicit fresh initializer is only for a genuinely new provider with zero total lease history, and additionally requires an independently supplied exact fleet roster, complete identity-consistent empty provision and retention inventories from every configured backend, and continuous fencing of providerd plus tenant/chain mutation ingress. Each backend stays running so the tool can authenticate its inventories, but must be empty and drained with no in-flight mutation and an idle callback/outbox queue. Its print-time acknowledgement binds the target parent's physical device/inode; do not rename, unmount, or recreate that parent between print and initialize. Publication is descriptor-relative and no-overwrite. Follow [Initializing a genuinely fresh placement authority](DEPLOYMENT.md#initializing-a-genuinely-fresh-placement-authority) for that first-boot workflow. A restored placement database is older than the one it replaces: run `placement-repair -attest-restored-backup` on it before starting providerd ([Restoring an older placement backup](DEPLOYMENT.md#restoring-an-older-placement-backup)), or a lease dispatched after the backup can be provisioned twice. Restore the payload store from the same moment as the placement database: without it, ACTIVE leases that need re-provisioning stay deferred, because tenants can re-upload only a PENDING lease's original manifest. The token tracker may start empty (acceptable, see above); restore each backend callback store whenever any exact delivery could remain.

Never run two `providerd` or `docker-backend` instances against the same bbolt files — bbolt enforces single-writer with a file lock and the second process will fail to start. If it doesn't fail, you have data corruption coming.

---

## Restart and update operations

`POST /v1/leases/{uuid}/restart` and `POST /v1/leases/{uuid}/update` are tenant-initiated, asynchronous. Each captures its source, creates a replacement under volume exclusion, and verifies startup before activating the new release.

Both endpoints require exactly one canonical UUIDv4 `Idempotency-Key`. Fred
writes the authenticated tenant, lease-scoped key, command kind, update payload
fingerprint and exact placement/storage/lifecycle route to the required
placement database before dispatch. Pending commands retain the same
process-local lease claim across recovery, so close, reconciliation, and a
different maintenance key cannot overtake them; an unavailable backend delays
only leases pinned to that backend. Startup rehydrates these claims without
network I/O, then bounded background passes re-authorize current chain and
routing facts before retrying the exact stored command. A chain read that
authoritatively proves the lease ended terminalizes the command and releases
the fence; read uncertainty and backend outage leave it pending.

The journal's tenant/provider/backend/storage/callback authority is store-minted,
not copied from the restart/update request. A current provision/restore writes
that runtime principal when its exact operation is promoted. Existing v0.13
owners receive it only from the first complete identity-bearing inventory whose
tenant, provider, backend storage identity, and legacy lifecycle class all
match the prepared placement authority. Until that projection succeeds,
restart/update for the old owner fails closed. Afterward the principal is
durable: a partial sweep or transient outage of another backend does not block
maintenance on an available owner.

Terminal provider receipts are scoped by lease and key. An exact replay
returns the stable result and a divergent kind or payload returns `409`; a
different key cannot pass a pending head. Provider and backend each keep a
rolling window of the lease's 1,024 most recent restart and update receipts,
and admitting a newer command evicts the oldest settled one, so there is no
per-lease command limit. The backend keeps a failed update whose late-container
cleanup is unconfirmed until cleanup attests absence at least an hour after the
failure, and keeps custom-domain reconciles in their own window of 64. The
transaction that removes the lease's final placement or lifecycle authority
reclaims its terminal receipts atomically; startup and periodic command
recovery never scan history. Pending never expires.

Forgetting old keys is safe because ordering does not depend on them: the
provider stamps each command with its admission time, strictly increasing per
lease, and the backend refuses a command it no longer has a receipt for that is
not newer than the newest one it accepted for the lease. Such a command settles
as `410 maintenance_expired` and never runs. A completed update also carries
store-assigned ordering, so an older recovered update can never rewrite the
provider payload store after a later generation. Generate a new UUIDv4 per
logical action, preserve it for retries, and never reuse it: a forgotten key
whose release generation the backend still retains is refused with `409`.

A backend refuses a command for capacity, as coded `503
insufficient_resources` before any replacement, only for an unstamped command
from an older provider at a full window, or when the window is full of failed
updates whose cleanup is unconfirmed; the latter clears as cleanup confirms.

**On success**: a `success` callback is sent and the lease's status returns to `ready`. For update, provider settlement occurs only after the accepted payload is durably persisted. Once backend acceptance is durably recorded, recovery performs local payload persistence without another backend call. Positive evidence that the exact lease is closed, rejected, or expired can instead retire the pending command and release its fence; missing, unreadable, foreign, or active chain observations cannot authorize that exit.

**On failure:** the result depends on durable execution evidence.

- A failure before replacement dispatch, such as an image pull failure, can
  preserve the intact source after verifying it is healthy. The lease remains
  `ready` and the maintenance callback reports `failed`.
- When replacement Docker calls completed and startup failed, Fred captures the
  failed attempt's logs before cleanup and can recreate source containers from
  the captured image and runtime configuration using the existing volumes.
  A verified source returns to `ready` with a `failed` maintenance callback.
- If source recreation also completes but cannot become ready, the lease becomes
  `failed`. A Failed lease with no source containers can still be restarted;
  there is no source to compensate in that case.
- A timed-out or response-lost Create/Start remains unresolved. Recovery never
  substitutes an empty inventory or elapsed timeout for completion evidence.
  `callbacks.db` retains the launch record; subsequent launches and volume
  namespace changes for that lease refuse with
  `physical volume has an unsettled Docker launch`. Preserve the databases and
  directories and investigate the original Docker request. Removing journal
  rows, recreating paths, or retrying with a different key cannot safely resolve
  that ambiguity. Use the [offline fencing and repair procedure](#unsettled-docker-effects)
  when normal recovery cannot obtain completion evidence.

Compensation uses the source's captured immutable image and effective settings,
including its resource limits and callback route. It does not pull a mutable tag
or undo application/database writes made by the failed replacement. An already
activated target release cannot be compensated.

**To diagnose a failed restart or update:**
1. `GET /v1/leases/{uuid}/releases` — the failed release has `status: "failed"` and curated `reason` and `message` fields.
2. `GET /v1/leases/{uuid}/logs` — the diagnostics store retains the failed attempt's captured logs for the configured retention (7 days by default), including under `failed/<service>/<instance>` keys alongside live logs when the restored source is Ready. An unavailable diagnostics store delays failed-target removal and terminal failure publication.
3. A restart or update that failed while the docker-backend ran it logs `maintenance failed (verbose detail retained operator-side)` at WARN, with the cause in `detail` and `lease_uuid`, `maintenance_id`, `operation` and `reason`. One interrupted by a backend restart and settled by recovery logs no such line. The attempt's diagnostic in `diagnostics.db` keeps the first cause observed for it in `error`. For a failure before any container is touched, such as capturing the source, that is the logged cause, and no logs were captured.

**Phase timing:** `fred_docker_backend_replace_phase_duration_seconds{operation,phase}`
now charges root materialization, reservation waits, writer drain/stop and bind
preparation/chown to `volume_setup`. `compose_up` covers protected create/start
and launch-receipt settlement. Source compensation is outside this histogram,
so its samples alone do not describe a failed replacement's complete duration.

### Restore operations

`POST /v1/leases/{lease_uuid}/restore` (on providerd; `POST /restore` on the docker-backend) re-deploys a lease onto its **retained** (soft-deleted) volumes — the v0.5.0 headline feature. Restore has its own durable operation and retention-finalizer protocol. Its adoption phase renames the exact `fred-retained-*` volumes to the destination namespace; failed restoration returns them only through the exact failed-operation handback described below.

**Provider-side admission and recovery.** Before contacting a backend, providerd
acquires ordered lifecycle claims for both source and target, then reserves the
exact confirmed source in the placement store before re-reading the target as
tenant/provider-owned `PENDING`. The reservation fences inventory that was already
captured. Admission transfers that same opaque reservation to the full restore
claim while atomically writing the absent target's durable operation attempt.
Failure before transfer releases only the early reservation; an older deferred
release cannot revoke a transferred or replacement claim.
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
tier — the backend runs `checkDemoteFitWithResourceProfiles` before adopting. A
demote that does not fit is refused: the docker-backend returns HTTP 422 with body
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
failed operation by atomically recording Failed and enqueueing its callback,
pre-counts the retained footprint, commits the exact
source-generation CAS/backfill, and only afterward releases destination
allocations. After actor acceptance the worker deliberately parks at
`restoring`: the lease actor must first persist the Failed operation outcome and
callback, and the
periodic sweep then performs that same make-before-break handback. A measurement,
quota-application, callback-store, CAS, or accounting uncertainty keeps the row
`restoring` and the live allocation counted. Investigate
`unable to restore source volume quotas` and the retention-sweep error, repair
the storage/store dependency, and let reconciliation retry; never delete the
source finalizer.

If the failed destination wrote more data than the immutable source quota can
hold, this is an intentional reservation hold, not an accounting leak. The
`restoring` row and destination allocation remain until a safe handback can be
proved. Repeated retries cannot shrink the data: preserve it, inspect the logged
usage/source cap and plan explicit operator recovery. Do not delete the finalizer,
free the allocation or edit the source quota to make reconciliation pass.

The periodic sweep takes the destination command fence and an exclusive typed
lease-actor quiescence claim before reading recovery inputs. That capability
spans queued and handling messages, workers, their terminal-message handoff, and
actor retirement/replacement. If it cannot be acquired, the sweep leaves this
lease `restoring` and retries; it never infers safety from separate inbox-depth
or worker-idleness samples. If source handback fails after operation settlement,
the next sweep follows the durable Failed row and retries rollback. A missing row
is an authority error and fails closed.

The source finalizer always excludes Provision and Restore of the destination.
Every maintenance path returns invalid-state before commit. After an exact active
destination Release proves commit and no Pending or contradictory Failed restore
operation remains (an authorized successor may already have retired Succeeded
history), a plain
Restart may repair the destination; Update and custom-domain redeploys remain
fenced until that Restart reaches Ready and reconciliation consumes the
finalizer. The Release is a commit marker, not rollback debris: retain it and
transition any matching Pending operation to Succeeded. At cold start,
exact Release plus zero survivors recovers a conservative Failed destination
with its allocation still held and preserves the source finalizer as
tenant/provider identity. Close instead persists a complete close intent before
deleting the row and starting teardown. Never delete the Release or return its
data to the source merely because the live cohort is gone.

**Success-rate signal.** `fred_docker_backend_restore_total{outcome}` (`outcome ∈ {success, failure}`) is the docker-backend's own restore success rate. Both outcome series are pre-initialized to 0, so a failure ratio reads 0 (not no-data) before the first restore. `failure` counts only a definite execution failure whose settlement committed; an ambiguous worker result (a post-effect error, a commit error or a panic) counts neither outcome, even after recovery settles it. **Worker-scoped caveat:** a restore refused in the synchronous `Restore()` prelude (validation, intent, reservation, source claim) returns a synchronous error to the caller and is counted by **neither** outcome bucket, as `provisions_total` omits synchronous provision failures. Such failures surface to the tenant as the restore HTTP status; providerd's `fred_provisioner_provisioning_total{operation="restore"}` counts the async callback/timeout outcomes, not these synchronous backend errors, so it does not backfill the gap.

**Latency.** `fred_docker_backend_restore_duration_seconds` measures the restore worker span on success only (volume adoption, compose up, verify startup, release commit and source finalization) and **excludes** the synchronous `Restore()` prelude. Its buckets mirror `provision_duration_seconds` so the two can be overlaid for the restore-vs-fresh-provision question — but the overlay is approximate: `provision_duration_seconds` is observed on both success and failure and carries no outcome label, so the comparison is robust at the median but tail-biased. Read it as indicative.

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
admission, operation, HTTP, scheduler, and manager drain. A reconciliation sweep
already reading backend inventories when the stop arrives may finish those reads
and commit its placement projection for up to half of `shutdown_timeout`; a
sweep still reading then is abandoned with its inventory marker pending. That
grace runs alongside the drain, except during the startup reconciliation, where
it comes first. It does not configure docker-backend.

Docker-backend shares one 75s process deadline across HTTP and backend drain.
HTTP requests receive at most 30s; backend-owned work receives the remaining
45–75s. If a worker still has not returned, shutdown leaves the Docker client
and bbolt stores open, logs `docker backend workers did not drain before
shutdown deadline`, and the binary exits non-zero. A fresh process re-attests
Docker and durable state; restart alone cannot discharge unknown import debt.
The command fits the existing 90s service stop allowance. Direct Go callers of
`Backend.Stop` retain a separate 90s default; it is not added to the command
budget. There is no production knob to extend either deadline.

For a planned upgrade, fence new mutations and let admitted lifecycle work
quiesce while docker-backend is still running. Require
`fred_docker_backend_image_import_pending_bytes == 0` before stopping it. A
75s shutdown can cancel a longer import and leave its allocation charged. If
the gauge remains nonzero after work has quiesced, follow
[the outstanding-import recovery procedure](#recovering-outstanding-image-import-allocation);
waiting or restarting alone cannot prove completion. Stopping Fred does not
fence Docker or its runtime.

`fred_docker_backend_lease_terminal_event_dropped_total` should remain zero, but
it is a bug signal rather than a shutdown-tuning signal. Capture the shutdown
error and a goroutine dump if it rises or docker-backend exits non-zero during a
routine restart.

Callback timing is a separate reverse-direction budget. Bundled backends share
one 2m15s deadline across all three delivery attempts and their 0s/1s/5s
backoff; providerd gives each admitted callback up to 2m to apply. Operation,
maintenance, and lifecycle completion paths atomically queue their durable fact,
send a non-blocking commit wake naming the exact affected lease, and return
without HTTP in the lease actor, API handler, or startup recovery. The mailbox
coalesces repeat wakes for one lease but retains every affected lease identity;
a stronger handoff transfers work when a canceled drainer releases ownership.
The tracked replay loop alone owns delivery.
A slow callback can therefore hold one replay worker and that lease's FIFO lock
for up to 2m15s, while actors and unrelated leases continue. `backends[].timeout`
applies to Fred-to-backend requests and does not control this callback deadline.
Backend shutdown cancels the shared callback context before draining workers
within the remaining process deadline. The 30s level-triggered sweep discovers
pre-start rows and retries dormant failed heads from the same durable outbox.

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
