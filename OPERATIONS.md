# Operations Runbook

This document covers day-to-day operation of a Fred deployment: health checks, alert interpretation, common failure modes, recovery procedures, and tuning.

For deployment and initial setup see [DEPLOYMENT.md](DEPLOYMENT.md). For metric definitions see [ARCHITECTURE.md](ARCHITECTURE.md#metrics-prometheus). Sample Grafana dashboards live in the `manifest-deploy` repository.

---

## Health checks

Both `providerd` and `docker-backend` expose `GET /health` that returns 200 when the process can do useful work. Use these for load-balancer health checks and uptime monitoring.

| Endpoint | Checks |
|---|---|
| `providerd /health` | Chain gRPC reachable, all backends reachable, plus token-tracker and placement-store DB writability *if those stores are configured* (`token_tracker_db_path` and `placement_store_db_path`) |
| `docker-backend /health` | Docker daemon reachable |

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
| `fred_docker_backend_die_event_dropped_total` sustained non-zero | Lease actor inbox is wedged | See [Wedged lease actor](#wedged-lease-actor) |
| `fred_docker_backend_lease_actor_stuck_seconds > 900` | Some actor's `handle()` has been running for >15 min | See [Wedged lease actor](#wedged-lease-actor) |
| `fred_docker_backend_lease_actor_panics_total > 0` | Bug — actor handler panicked | Check logs for stack trace, file an issue |
| `fred_docker_backend_lease_terminal_event_dropped_total` rising under clean shutdown | Real data loss pattern | The release store / provision struct may be out of sync with Docker — reconciler will re-detect on next cycle, but root-cause the wedged actor |
| `fred_provisioner_reconciler_panics_total > 0` | Bug — reconciler goroutine panicked | Reconciler keeps running for other leases, but file an issue with the stack trace |
| `fred_background_cleanup_panics_total > 0` | Bug in a cleanup loop | Same — keep running, file issue |
| `fred_api_rate_limit_rejections_total{limiter="tenant"}` spike | Specific tenant exceeded their bucket | Expected if a tenant is bursting; sustained spikes indicate a misbehaving client |
| `fred_payload_leases_awaiting > 0` for >5 min | Tenant created lease with `meta_hash` but never uploaded payload | Tenant-side issue; the lease will eventually expire |
| `fred_reconciler_last_success_timestamp_seconds` stalled | Reconciler is stuck or panicking | Logs + `fred_reconciler_runs_total{outcome="error"}` |
| `fred_watermill_poisoned_messages_total > 0` | A handler exhausted retries on a message | Logs around the topic in question; the poison log identifies the message |
| `fred_docker_backend_retention_refused_total` increasing / `fred_docker_backend_retained_volume_bytes` approaching `fred_docker_backend_disk_pool_bytes` | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |

---

## Out-of-gas tuning

Lease acknowledgments are submitted as Cosmos SDK transactions with a fixed `gas_limit` (default 1,500,000) multiplied by `gas_adjustment` (default 1.2). When the chain rejects with `out of gas`, the broadcast layer retries with `1.5×` more gas, compounding up to `max_gas_limit` (default `0` = uncapped).

**Diagnosing:**
- Spikes in `fred_chain_signer_oog_retries_total{result="retried"}`: retries are working — the gas estimate is tight but eventually succeeds. Consider raising `gas_limit` to skip the retry.
- Spikes in `fred_chain_signer_oog_retries_total{result="exhausted"}` or `fred_provisioner_ack_batch_fee_gas_errors_total`: the cap is too low or the underlying tx genuinely needs more gas (e.g. a large authz batch).

**Tuning:**
1. Look at chain logs for the `gas_used` of failing txs.
2. Set `gas_limit` to `1.2 × p99 gas_used`.
3. If using authz sub-signers (`sub_signer_count > 0`), each lane has its own gas budget; the total chain cost scales with `sub_signer_count`.
4. Set `max_gas_limit` to a safety cap (e.g. `4 × gas_limit`) so a runaway tx doesn't consume an entire fee budget on retries.

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

## Backend at capacity

When SKU resource pools are full, provision requests get HTTP 503 with `insufficient resources`. Fred logs the error and the reconciler retries on the next cycle.

**Symptoms:**
- `fred_backend_insufficient_resources_total{backend="X"}` rising
- `docker-backend /stats` shows allocated == total or close to it
- Active leases stay in `provisioning` state

**Options:**
1. **Add capacity**: spin up another docker-backend on a different host with the same `skus`. Fred round-robins new provisions across them automatically (requires `placement_store_db_path`).
2. **Tighten SKU profiles**: smaller CPU/memory/disk per SKU lets more leases fit.
3. **Tenant quotas**: if one tenant is hogging resources, set `tenant_quota` in `docker-backend.yaml` to cap them.
4. **Force reconciliation**: orphan provisions (lease closed but containers still running) consume budget. The reconciler removes them on its cycle, but you can restart the backend to force an immediate cleanup.

---

## Reclaiming retained volumes under disk pressure

Retained (soft-deleted) volumes count against the disk admission pool until they
are reaped (`fred_docker_backend_retained_volume_bytes` shows the reserved
footprint; `fred_docker_backend_retained_volumes` the count). When retained data
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
live-provision capacity within the single `total_disk_mb` pool. **Invariant:**
size `total_disk_mb` at or below the data filesystem's usable capacity, or
retained+live volumes can exhaust physical disk (tenant ENOSPC) — fred WARNs at
startup if you exceed it.

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
| `placement_store_db_path` | lease→backend mapping for round-robin reads | Read-path misroutes until reconciler rebuilds (next cycle) |
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

---

## Withdrawal and credit monitoring

Fees are pulled into the provider account by `WithdrawScheduler` at `withdraw_interval` (default 1h). Between ticks, the credit checker estimates which tenants will deplete before the next tick and triggers an early withdrawal so we don't miss fees.

**Symptoms of failure:**
- `fred_chain_transactions_total{type="withdraw",outcome="error"}` rising
- Provider balance not increasing despite active leases
- Cross-provider auto-close events not triggering withdrawals → check the watcher is running and seeded

**Common causes:**
- Insufficient gas (see [Out-of-gas tuning](#out-of-gas-tuning))
- Authz authorization expired or revoked when using `sub_signer_count > 0` — the primary key keeps working but sub-signers fail
- Sub-signer balance below `sub_signer_min_balance` and top-up failing — check `sub_signer_top_up_amount` and primary balance

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
