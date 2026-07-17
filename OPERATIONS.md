# Operations Runbook

This document covers day-to-day operation of a Fred deployment: health checks, alert interpretation, common failure modes, recovery procedures, and tuning.

For deployment and initial setup see [DEPLOYMENT.md](DEPLOYMENT.md). For metric definitions see [ARCHITECTURE.md](ARCHITECTURE.md#metrics-prometheus). Sample Grafana dashboards live in the `manifest-deploy` repository.

---

## Health checks

Both `providerd` and `docker-backend` expose `GET /health` that returns 200 when the process can do useful work. Use these for load-balancer health checks and uptime monitoring.

| Endpoint | Checks |
|---|---|
| `providerd /health` | Chain gRPC reachable, all backends reachable, plus token-tracker and placement-store DB writability *if those stores are configured* (`token_tracker_db_path` and `placement_store_db_path`) |
| `docker-backend /health` | Docker daemon reachable, plus the callback, diagnostics, release, and retention bbolt stores are writable (503 if any is unhealthy) |

A 503 from `providerd /health` includes a JSON body with per-check status; the failing component is identified there. The `token_tracker` and `placement_store` keys only appear when the corresponding DB is configured — a dev-mode instance without them simply omits those checks rather than failing.

---

## Common alerts and what they mean

| Signal | Likely cause | First step |
|---|---|---|
| `fred_backend_circuit_breaker_state{backend="X"} == 2` (open) | Backend X has been unhealthy long enough to trip the breaker | `curl backendX/health`, check backend logs |
| `fred_backend_healthy{backend="X"} == 0` for >1 min | Backend health probe failing | Same as above |
| `fred_backend_insufficient_resources_total` rising on a backend | Backend at capacity | Reduce SKU sizes, add backend hosts, or check `docker-backend /stats` |
| `fred_provisioner_callback_timeouts_total` rising | Backend accepted provision but never called back | Backend logs; verify `callback_base_url` is reachable from backend; check HMAC secret match |
| `fred_provisioner_ack_batch_fee_gas_errors_total` rising | Out-of-gas on lease acknowledgment txs | See [Out-of-gas tuning](#out-of-gas-tuning) |
| `fred_chain_signer_oog_retries_total{result="exhausted"}` rising | Same; the broadcast retry loop hit `max_gas_limit` | Same as above |
| `fred_docker_backend_die_event_dropped_total` sustained non-zero | Lease actor inbox is wedged | See [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_stuck_seconds > 900` | Some actor's `handle()` has been running for >15 min | See [Wedged lease actor](#wedged-lease-actor-docker-backend) |
| `fred_docker_backend_lease_actor_panics_total > 0` | Bug — actor handler panicked | Check logs for stack trace, file an issue |
| `fred_docker_backend_lease_terminal_event_dropped_total` rising under clean shutdown | Real data loss pattern | The release store / provision struct may be out of sync with Docker — reconciler will re-detect on next cycle, but root-cause the wedged actor |
| `fred_provisioner_reconciler_panics_total > 0` | Bug — reconciler goroutine panicked | Reconciler keeps running for other leases, but file an issue with the stack trace |
| `fred_background_cleanup_panics_total > 0` | Bug in a cleanup loop | Same — keep running, file issue |
| `fred_api_rate_limit_rejections_total{limiter="tenant"}` spike | Specific tenant exceeded their bucket | Expected if a tenant is bursting; sustained spikes indicate a misbehaving client |
| `fred_payload_leases_awaiting > 0` for >5 min | Tenant created lease with `meta_hash` but never uploaded payload | Tenant-side issue; the lease will eventually expire |
| `fred_reconciler_last_success_timestamp_seconds` stalled | Reconciler is stuck or panicking | Logs + `fred_reconciler_runs_total{outcome="error"}` |
| `fred_watermill_poisoned_messages_total > 0` | A handler exhausted retries on a message | Logs around the topic in question; the poison log identifies the message |
| `fred_docker_backend_retention_refused_total` increasing / `fred_docker_backend_retained_volume_bytes` approaching `fred_docker_backend_disk_pool_bytes` | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |
| `fred_docker_backend_retention_reaping_bytes` > 0 sustained across several sweeps | A `fred-retained-*`/leaked volume the sweep can't destroy — its footprint **is** counted in the admission pool (no over-admit) but pins capacity and likely needs manual reclaim. A rising `..._retention_leaked_total` with `reaping_bytes` flat is instead the self-healing rollback store-error case (no action). | [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
| `fred_docker_backend_volume_quota_clear_failed_total` rising | An XFS volume `Destroy` failed to clear its project block limit — the project-quota table is regrowing (leaked zero-byte entries slow every `xfs_quota` scan) | [Leaked XFS project-quota entries](#leaked-xfs-project-quota-entries) |

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

## docker-backend refuses to start: `CAP_SYS_ADMIN` not available

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

**Startup quota backfill.** After the guard passes, the backend re-applies each
existing managed volume's quota (re-tag + limit) so leases provisioned before the
daemon held the capability get their `disk_mb` enforced without a re-provision.
`fred_docker_backend_volume_quota_backfill_total{outcome}` (`outcome ∈
{applied, failed}`) counts this per-volume work. Sustained `failed` means some
volumes stay unenforced — usually a missing `CAP_FOWNER` (the re-tag needs it);
grant it and restart to heal them.

---

## Leaked XFS project-quota entries

`fred_docker_backend_volume_quota_clear_failed_total` increments when a volume
`Destroy` fails to reset its XFS project block limit
(`xfs_quota -x -c 'limit -p bhard=0 bsoft=0 ihard=0 isoft=0 <projID>'`). Each leaked
entry holds no disk, but it lingers in the project-quota table and every `xfs_quota`
scan (`report -p`, used by `Usage` and `Validate`) has to walk it — so a steadily
rising counter surfaces as a slow cumulative provisioning-latency regression.

**Remediation.** There is no automatic sweep: a `report -p` is filesystem-wide and
cannot distinguish fred's orphaned entries from live foreign limits, so cleanup is a
manual operator task.

1. List quota entries: `xfs_quota -x -c 'report -p' <mountpoint>` and find project
   IDs with a non-zero hard limit but no live volume directory.
2. Clear each orphan: `xfs_quota -x -c 'limit -p bhard=0 bsoft=0 ihard=0 isoft=0 <projID>' <mountpoint>`.

Backends that ran a **pre-v0.7.0** build never cleared limits on `Destroy` and so
accumulated one leaked entry per provision — those need this one-time manual
cleanup. v0.7.0+ clears the limit on every successful `Destroy`; a rising counter
there instead points at a genuine `xfs_quota` failure (check the backend logs for
the `xfs_quota` stderr). Since ENG-548, `Destroy` also clears the inode limits
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
1. `df -i <volume-path>` — `IUse%` at 100% while `df -h` shows headroom points at the inode cap, not the block quota.
2. `xfs_quota -x -c 'report -p -i' <mountpoint>` filtered to the tenant's project ID confirms inode count is pinned at the hard limit.

**Remediation:** there is no fred-side alert or auto-remediation for a single tenant's inode ceiling. Two levers:
1. Raise the SKU's `disk_mb` — `ihard` scales with it (`disk_mb × 1 MiB / min_avg_file_bytes`), so a bigger disk budget raises the inode ceiling too.
2. Lower `min_avg_file_bytes` provider-wide (denser ratio, more inodes per MB) — restart required; values below 512 are rejected at config validation.

---

## Backend at capacity

When SKU resource pools are full, provision requests get HTTP 503 with `insufficient resources`. Fred logs the error and the reconciler retries on the next cycle.

**Symptoms:**
- `fred_backend_insufficient_resources_total{backend="X"}` rising
- `docker-backend /stats` shows allocated == total or close to it
- Active leases stay in `provisioning` state

**Options:**
1. **Add capacity**: spin up another docker-backend on a different host with the same `skus`. Fred routes each new provision to the least-loaded matching backend — the one reporting the lowest allocated-CPU ratio from its `/stats` endpoint — so a fresh, empty host preferentially absorbs new provisions (requires `placement_store_db_path`).
2. **Tighten SKU profiles**: smaller CPU/memory/disk per SKU lets more leases fit.
3. **Tenant quotas**: if one tenant is hogging resources, set `tenant_quota` in `docker-backend.yaml` to cap them.
4. **Force reconciliation**: orphan provisions (lease closed but containers still running) consume budget. The reconciler removes them on its cycle, but you can restart the backend to force an immediate cleanup.

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
the closing lease's own volumes. True per-tenant disk fairness would require a
per-tenant retained-disk quota (a possible follow-up).

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
- **Reclaim (only after confirming no live/restoring lease references it).** Once the blocker is
  cleared the next sweep reclaims it automatically. To force it sooner, restart the backend
  (boot runs the reaping reconcile). If the volume is genuinely unrecoverable, remove it
  manually (`docker volume rm <name>` or `rm -rf <volume_data_path>/<name>`) — the next sweep
  then deletes the now-dangling tombstone (its destroy is an idempotent no-op).

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
| `placement_store_db_path` | lease→backend mapping for multi-backend read routing | Read-path misroutes until reconciler rebuilds (next cycle) |
| `<docker>/callbacks.db` | Pending callbacks (delivery retry) | Some callbacks may not be redelivered after restart |
| `<docker>/diagnostics.db` | Failure diagnostics (last_error, logs) | Older `failed` leases lose diagnostics; new failures still recorded |
| `<docker>/releases.db` | Per-lease deployment history | Release history lost; provisioning still works |

**If a bbolt file is corrupted** (file lock errors, bbolt panic on open, or known bad magic):

1. **Stop the service**.
2. **Move the file aside** rather than deleting (`mv X.db X.db.broken`) so you can inspect it later if needed.
3. **Restart the service**. It will recreate an empty database.
4. **Run a manual reconciliation** (it runs automatically on startup) — the placement store rebuilds from backend `ListProvisions`, the payload store stays empty (tenants re-upload), the token tracker is empty (acceptable, see above), and the docker callback store starts fresh.

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

**Restoring onto a different SKU tier.** A restore's new lease may target a
different SKU than the source — only the item *shape* (service names + quantities)
must match; the disk (`disk_mb`) tier may differ. A **promote** (same-or-larger
`disk_mb`) is always allowed and applies the larger cap. A **demote** (smaller
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

**Success-rate signal.** `fred_docker_backend_restore_total{outcome}` (`outcome ∈ {success, failure}`) is the docker-backend's own restore success rate, mirroring `provisions_total`. Both outcome series are pre-initialized to 0, so a failure ratio reads 0 (not no-data) before the first restore. **Worker-scoped caveat:** a restore that fails in the synchronous adopt prelude (claim/rename/route/ack) before the worker spawns returns a synchronous error to the caller and is counted by **neither** outcome bucket — exactly as `provisions_total` omits synchronous provision failures. Such failures surface to the tenant as the restore HTTP status; providerd's `fred_provisioner_provisioning_total{operation="restore"}` counts the async callback/timeout outcomes, not these synchronous backend errors, so it does not backfill the gap.

**Latency.** `fred_docker_backend_restore_duration_seconds` measures the async re-deploy worker span (compose up + verify startup, success only) and **excludes** the synchronous adopt prelude. Its buckets mirror `provision_duration_seconds` so the two can be overlaid for the restore-vs-fresh-provision question — but the overlay is approximate: `provision_duration_seconds` is observed on both success and failure and carries no outcome label, so the comparison is robust at the median but tail-biased. Read it as indicative.

**Slow-phase diagnosis.** `fred_docker_backend_replace_phase_duration_seconds{operation="restore",phase}` breaks the re-deploy into per-phase timings (`adopt`, `image_setup`, `volume_setup`, `compose_up`, `verify_startup`). When a restore is slow, query this to see which phase dominates — `adopt` (volume rename), `compose_up`, or `verify_startup` are the usual suspects.

---

## Withdrawal and credit monitoring

Fees are pulled into the provider account by `WithdrawScheduler` on a paid-withdraw cadence of `withdraw_interval` (default 1h). The scheduler also wakes on a separate credit-check cadence, `credit_check_interval` (default `0s` = coupled to `withdraw_interval`); each wake estimates which tenants will deplete before the next paid withdrawal. When `credit_check_interval < withdraw_interval` a **withdraw-cadence guard** is active (`fred_withdraw_guard_active` = 1): credit checks run at the faster cadence, but the paid provider-wide withdrawal stays rate-limited to once per `withdraw_interval` since the last full drain, so faster credit polling no longer forces an extra paid withdrawal every tick (ENG-524). Closed leases settle in full regardless, so a deferred paid withdrawal is a cash-timing choice, not lost fees. `credit_check_interval` must be `≤ withdraw_interval` when set.

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

---

## Graceful shutdown

`providerd` and `docker-backend` both handle SIGINT and SIGTERM. The shutdown order is documented in [ARCHITECTURE.md](ARCHITECTURE.md#graceful-shutdown).

`shutdown_timeout` (default 30s) bounds the in-flight drain. Increase it if you observe `lease_terminal_event_dropped_total` spikes during routine restarts — this means workers were still mid-flight when the actor was forced to exit.

For zero-downtime upgrades, run two providerd instances against different sets of backends and shift traffic, or use the chain's own redundancy (Fred is per-provider; failover is not a Fred concern).

---

## Capacity planning

Per the benchmarks in [PERFORMANCE.md](PERFORMANCE.md), Fred itself sustains 56,000+ events/sec, far above realistic chain event rates. The bottleneck is always the backend (Docker pull, container start, health check) and the chain (block time).

**Practical sizing:**
- Chain ack throughput is the typical limit. With `sub_signer_count = N`, you get up to `N × 50` acks per block (~5s blocks, chain-dependent ≈ 600 leases/min).
- Per docker-backend host, image pull and container start dominate provision latency (10s–60s for typical images).
- Budget memory: ~50MB baseline + ~1KB per active lease (in-memory tracker entries). bbolt stores grow with payload sizes and history retention.

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
