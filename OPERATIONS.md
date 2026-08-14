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
| `fred_payload_persist_failures_total > 0` | A tenant `/update` reached the backend but could not be written to `payloads.db`. That lease is now running a manifest fred has no durable record of, and the next reprovision will revert it to its as-created deployment | Check disk space and permissions on `payload_store_db_path`, then confirm the store is healthy. The tenant received a `500` and can retry — a retry re-applies **and** re-persists. Nothing in fred retries on their behalf, so a lease left in this state stays exposed until the tenant acts |
| `fred_reconciler_last_success_timestamp_seconds` stalled | Reconciler is stuck, panicking, **or running degraded** — a sweep that could not see every backend deliberately does not advance this | Check `fred_reconciler_sweep_complete` first: 0 means degraded, and `fred_reconciler_backend_fetch_total{outcome!="ok"}` names the backend. Otherwise logs + `fred_reconciler_runs_total{outcome="error"}` |
| `fred_reconciler_backend_fetch_total{outcome!="ok"}` sustained for one backend across ≥3 sweeps (~6 min at a 2m interval) | That backend is unreachable from providerd. Its leases are deferred — not re-provisioned, not deprovisioned — while the rest of the fleet reconciles normally | [Backend unreachable during reconciliation](#backend-unreachable-during-reconciliation) |
| `fred_reconciler_sweep_complete == 0` sustained | Same condition, viewed fleet-wide. The action counters describe only the leases fred could positively place, so do not read them as fleet totals while this is 0 | Same |
| `fred_provisioner_reconciler_deferred_leases_total` rising while `fred_reconciler_sweep_complete == 1` | Should be impossible — nothing defers on a complete sweep | Bug; file an issue with the reconcile logs |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown"}` rising | Fred is declining to clean up state for a lease **the chain has no record of**, and will decline again every sweep — this one does not self-heal. Either providerd is pointed at the wrong or a reset chain (check the `pass` label spread: fleet-wide means config, one lease means a phantom), or a provision exists that no lease ever created | Confirm the chain endpoint and provider UUID first. If the chain is right, the resource is genuinely unowned: deprovision it by hand once you have confirmed the tenant is gone |
| `fred_reconciler_cleanup_skips_total{reason="chain_unknown_state"}` rising | The chain reports a lease state this providerd build cannot classify — either the zero `UNSPECIFIED`, or a state added to the ledger after this binary shipped. Cleanup is withheld, which is data-safe but permanent for those leases | **Upgrade fred** to a build whose `manifest-ledger` pin knows the new state. Unlike `chain_unknown` the chain is fine and providerd is behind it, so do not go looking for a phantom provision |
| `fred_reconciler_cleanup_skips_total{reason="chain_error"}` sustained | The per-candidate chain re-check is failing, so cleanup is paused (data-safe). Usually the same cause as any other chain-query failure, or a lookup that blew its 10s budget — that budget exists so a stalled query cannot wedge the sweep, and it reports as an error rather than as evidence | Check `fred_chain_query_duration_seconds{query="get_lease"}` and the node's health; self-heals |
| `fred_reconciler_cleanup_skips_total{reason="chain_live"}` rising steadily | The sweep's lease snapshot is often stale by the time cleanup runs — expected at a low rate, but a high one means sweeps are slow relative to lease churn | Compare `fred_reconciler_duration_seconds` against the reconcile interval; no action if the rate is low |
| `fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}` steady on a removed backend | Expected: a decommissioned backend's placement records are never auto-pruned | [Removing, renaming or pausing a backend](#removing-renaming-or-pausing-a-backend) |
| `fred_watermill_poisoned_messages_total > 0` | A handler exhausted retries on a message | Logs around the topic in question; the poison log identifies the message |
| `fred_docker_backend_retention_refused_total` increasing / `fred_docker_backend_retained_volume_bytes` approaching `fred_docker_backend_disk_pool_bytes` | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |
| `fred_docker_backend_retention_reaping_bytes` > 0 sustained across several sweeps | A `fred-retained-*`/leaked volume the sweep can't destroy — its footprint **is** counted in the admission pool (no over-admit) but pins capacity and likely needs manual reclaim. A rising `..._retention_leaked_total` with `reaping_bytes` flat is instead the self-healing rollback store-error case (no action). | [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
| `sum without (outcome) (increase(fred_docker_backend_retention_sweep_total[3h])) == 0` (with retention enabled) | The periodic retention sweep has stopped iterating entirely — the loop goroutine is gone, the ticker is starved, or the process is wedged. The sum advances on **every** pass regardless of outcome, so a flat sum is absence, not failure. Nothing is being reaped, no interrupted restore is being reconciled, and no orphan record is being pruned | Check the docker-backend process and its logs for `retention cleanup panic`; `fred_background_cleanup_panics_total{component="retention"}` distinguishes a panicking sweep from a dead one |
| `increase(fred_docker_backend_retention_sweep_total{outcome="error"}[6h]) > 0` | At least one sweep stage failed. Two distinct causes land here, so **read the log line before acting**: an unenumerable `retention.db` (the common one — the reaper and orphan pruner reclaim nothing and **every lease close skips volume teardown entirely**, leaving closes `Failed` and retrying, so the provider degrades toward refusing new work), or an unreadable **volume root**, which the orphan stage reports through the same outcome with a perfectly healthy store | **Start with the sweep's log line, not the database.** It prefixes each failure with its stage — `reap expired:` / `retry reaping:` / `list restoring:` are store reads, `reconcile orphans:` can be either (pair it with `retention_orphan_skips_total`: `reason="store_error"` vs `reason="list_error"` separates them exactly). Then fix whichever dependency it names; the parked work resumes on its own. Shares a root cause with the `claims_unreadable` row below |
| `fred_docker_backend_retention_accounting_refresh_failed_total` rising | The retained-disk projection could not be recomputed, so the five retention gauges **and** the admission pool's retained input are frozen at their last values. That is the data-safe direction (a zeroed projection would over-admit), but it means those gauges are stale — do not read them as current while this is rising | Same root cause as the row above: fix the retention store. Until then, treat `retained_volume_bytes` / `retention_reaping_bytes` as last-known-good, not live |
| `fred_docker_backend_volume_quota_clear_failed_total` rising | An XFS volume `Destroy` failed to clear its project block limit — the project-quota table is regrowing (leaked zero-byte entries slow every `xfs_quota` scan) | [Leaked XFS project-quota entries](#leaked-xfs-project-quota-entries) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claims_unreadable"}` > 0 | The retention store could not be read, so no path could establish who owns a volume and **nothing was destroyed** — data-safe, but every close, orphan sweep and reap is now parked, and closing leases stay `Failed` and retry. Same root cause as the `claim_unreadable` and `store_error` skips | Fix `retention.db` health first; the parked work resumes on its own. See the `store_error` row in [Partition collapse triage](#partition-collapse-triage) |
| `fred_docker_backend_volume_destroy_refused_total{reason="claimed"}` sustained | A destroy path keeps meeting a volume another lease owns — normally an in-flight restore that is not converging, since a healthy restore clears its own claim on commit or rollback. Never data loss: the refusal is the guard working | Read with `restore_finalizer_pending_total` and `retention_reaping_leases`; the WARN log names the volume and its owning lease. [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation=~"restore_reconcile\|restore_rollback\|deprovision"}` rising | A `compose down` failed AND the per-container fallback could not finish it, so containers — and the anonymous volumes attached to them — are pinned on this host. Not data loss: on these **blocking** operations fred keeps the lease tracked and its capacity reserved rather than advancing state over live containers, and retries. A sustained rate with `operation="restore_reconcile"` also means the affected tenant cannot retry its restore until the teardown succeeds | [Stuck teardown](#stuck-teardown-docker-backend) |
| `fred_docker_backend_teardown_fallback_total{outcome="failed",operation=~"restore_prelude\|provision_cleanup"}` rising | Same failed teardown, but on an **advisory** operation: the caller discards the error and state advances anyway, so nothing is held open and **nothing will retry**. For `provision_cleanup` that means a possible leak whose capacity has already been returned; for `restore_prelude` it is routinely just a canceled or timed-out restore request with nothing on the host at all. Lower urgency than the blocking row, but the only signal you get | [Stuck teardown](#stuck-teardown-docker-backend) |
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
| `restore_prelude`, `provision_cleanup` | **Advisory** | Discarded the error and advanced anyway. Nothing is held open and **nothing will retry** |

On a **blocking** operation this is not data loss and not an over-admission — the cost is capacity and disk that stay reserved, plus one anonymous volume per surviving container, until the substrate recovers.

On an **advisory** operation the accounting has already moved on. `provision_cleanup` releases the failed provision's pool allocation *before* the teardown runs, so a real leak there is capacity fred believes it has. `restore_prelude` is the opposite: its callers are entered *because* the restore request's context was canceled, and they hand that same dead context to the teardown, so both the `compose down` and the container listing fail with `context canceled` and there is usually **nothing on the host at all** — a canceled or timed-out restore, not a fault.

`outcome="recovered"` is the benign twin — `down` failed but the fallback removed everything it found — with one caveat: on an advisory operation it can be vacuous, recording success after finding zero containers.

**Triage:**

1. Find the lease and the operation from the `operation` label and the warning logs (`compose down failed, falling back to individual removal`, then `failed to remove container`).
2. Check the daemon: `docker ps -a --filter label=fred.lease_uuid=<uuid>`.
   - **Nothing returned** and `operation="restore_prelude"` → this was a canceled restore request, not a leak. Correlate with a `POST /restore` that timed out (providerd's backend client gives up at 30s) or a providerd restart. No action.
   - A container stuck in `Removal In Progress`, or one whose `docker rm -f` hangs, usually means a wedged storage driver or a mount the kernel still holds.
3. Fix the substrate (see [Wedged lease actor](#wedged-lease-actor-docker-backend) for the same class of causes). On a blocking operation fred retries on its own once the daemon can remove containers again. On an advisory one it will not — reap what step 2 found by hand.
4. Restarting the docker-backend is safe and often enough: `recoverState` rebuilds from Docker labels, and the retry paths run again at boot. It also re-adopts a leaked advisory container into `b.provisions`, which is how one becomes visible to the normal orphan path.

**Restore-specific consequence.** With `operation="restore_reconcile"` — and with `restore_rollback`, whose worker arm blocks the same way — the tenant's retention record stays in `restoring` until the teardown succeeds, and a restore cannot be re-requested while it does (the API reports the lease as not restorable). The data itself is safe: a `restoring` record is never reaped and its volumes are excluded from the orphan sweep. But the retention window keeps ageing, so clear the daemon fault well before `retention_max_age` expires for that lease. `restore_prelude` carries none of this — the record is already back to `active` and the tenant can retry immediately.

To confirm nothing is stranded after the fault clears, the counter should stop rising and `docker volume ls -qf dangling=true` should stop growing.

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
1. `df -i <volume-path>` reports **filesystem-wide** inode usage, not the per-tenant quota — for an `ihard` hit it still shows ample free inodes (`IUse%` well below 100%). That is the tell that it's the per-project inode cap, not global exhaustion (global exhaustion would instead fire `FilesystemInodesLow`).
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

## Backend unreachable during reconciliation

A backend that is configured but not answering `GET /provisions` — down, wedged,
partitioned, or with its circuit breaker open — no longer stops reconciliation
for the rest of the fleet. Fred marks it unanswered for that sweep and **defers**
the leases it cannot positively place, then retries on the next cycle.

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
| On a backend that answered | Reconciled normally — this is the whole point of the change |
| On the unreachable backend | Deferred: not acknowledged, re-provisioned, or deprovisioned |
| With no placement record | Deferred while the sweep is incomplete — fred cannot rule out that it lives on the silent backend |
| Orphans on the backends that answered | Deprovisioned normally. A silent backend reports no provisions, so it contributes no orphan candidates of its own and cannot mask anyone else's |
| Orphaned payloads | Cleaned normally — that pass compares the payload store against the chain and reads no backend state at all |
| Placements of leases on the unreachable backend | Not pruned: only that backend's own report can turn "absent from the backend data" into evidence about its records |

Nothing is destroyed and nothing migrates. The cost is latency: affected leases
stop making progress until the backend returns.

**What to do**

1. Identify the backend from the `backend` label and check it directly
   (`GET /health`, `GET /stats`, its own logs).
2. Bring it back. Recovery needs no action on fred's side — the next sweep sees a
   complete fleet, resumes the deferred leases, and prunes any of its placement
   records that turn out to be stale.
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
> ones. Placement pruning asks whether *that record's* backend answered, which is
> a per-record question. Skipped cleanup still costs only a cycle of latency;
> mistaken cleanup still costs a tenant their workload.

---

## Removing, renaming or pausing a backend

**Fred never moves a lease between backends.** A lease is pinned to the backend
holding its data for its lifetime, and a backend absent from the router is
treated as *temporarily unreachable*, never as *gone*.

So when a backend disappears from `providerd.yaml` — removed, renamed, or its
entry commented out during maintenance — leases placed on it stop making
progress rather than migrating:

| Operation | Behavior |
|---|---|
| Provision / re-provision | Refused, retried every reconcile cycle. See log lines below |
| Read (connection details, logs) | `503`, not `404` |
| Restore from that backend's retained data | `503`, not `404` |
| Leases on *other* backends | Unaffected |

Two ERROR log lines cover the two paths, and the reconciler one is the one you
will actually see, since it fires unattended on every cycle:

```
reconcile: refusing to provision, lease is placed on a backend the router does not know   # reconciler
refusing to provision: lease is placed on a backend the router does not know              # event path
```

Both carry `lease_uuid` and an `error` field containing the recorded backend
name (`placement backend not found in router: lease <uuid> is placed on "<name>"`),
so the recorded name can be recovered from the logs even if the config entry is
already gone.

This is deliberate (ENG-635). Substituting a healthy peer would provision a
brand-new **empty volume** while the tenant's real data sits intact on the
absent machine — unattended, on a timer, for every affected lease at once, and
reported to the caller as success. Refusing loses availability; substituting
loses data.

`503` rather than `404` matters for the same reason: a `404` tells a tenant
their deployment no longer exists and invites them to destroy and recreate it,
turning a recoverable outage into real data loss.

**Recovery is to put the backend back under its original `name` and then restart
`providerd`.** The restart is required, not optional: providerd reads its config
and constructs the backend router once at startup and handles no reload signal
(only SIGINT/SIGTERM), so restoring the YAML entry alone leaves the running
process still treating the backend as unknown. After the restart, the next
reconcile cycle resumes those leases with no further action.

Note that the name is the identity key: renaming a backend in config is
equivalent to removing it, and re-adding it under a *different* name will not
reunite it with its leases. If a backend is genuinely gone for good, its leases
are dead — the tenant must create new ones.

Its **placement records outlive it**, deliberately. The pruner deletes a record
only when the named backend answered this sweep, and a decommissioned backend
never will, so its records stay in the index — each one counted under
`fred_reconciler_cleanup_skips_total{pass="placement",reason="backend_silent"}`.
They are a few bytes of bbolt each and they are the only surviving pointer to
where that machine's data was, which is why they are kept rather than reaped by a
process that cannot tell "decommissioned" from "down since Tuesday". Deleting
them is part of the manual decommission, once the data is confirmed migrated or
gone.

> **Ansible caution.** `roles/fred/templates/providerd.yaml.j2` derives backend
> names positionally, so removing a mid-list host renumbers every host below it.
> That produces names which *resolve* — to the wrong machines — and every check
> here passes. Verify rendered names against `backend_index` before applying a
> membership change.

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
- **A give-up under a degraded store still records the footprint.** It used to compute the
  record's volume list through the same ownership table it could not read, and on failure wrote
  no record at all — so the abandoned bytes were counted by nothing (no pool reservation, no
  retained record, no reaping record) and admission over-committed against real disk until an
  operator noticed. The record no longer carries a destroy plan, so there is nothing left for a
  degraded store to prevent it computing. The one residual: if the store cannot be **written**
  either, `retention_leaked_total` plus the `MANUAL CLEANUP REQUIRED` log are the only record.
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
