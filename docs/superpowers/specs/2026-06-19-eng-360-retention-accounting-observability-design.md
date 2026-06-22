# ENG-360 — Retained-volume disk accounting, per-provider cap, and observability

**Status:** Design (for review)
**Date:** 2026-06-19
**Issue:** [ENG-360](https://linear.app/liftedinit/issue/ENG-360) — follow-up to [ENG-325](https://linear.app/liftedinit/issue/ENG-325) (lease-close soft-delete + restore)
**Scope:** fred repo only. The manifest-deploy `fred_retention_max_age` inventory var and the Grafana panel/alert are tracked separately in their own repos.

---

## 1. Background

ENG-325 added a soft-delete grace window: when a lease closes with `retain_on_close: true`, its managed
volumes are renamed into the `fred-retained-` namespace and recorded in a bbolt retention store, then
destroyed by a reaper after `retention_max_age` (default 90 days) — unless the tenant restores first.

ENG-360 was filed by the pre-flight review for enabling `retain_on_close` on the provider envs. It lists
four fred-facing concerns. Investigation (4-way codebase exploration + 2 web-research passes with
adversarial verification) found that two of them are **already done**, one rests on a **false premise**,
and the real work is narrower and more precise than the ticket text implies.

### What was already true before this work

- **`retention_max_age` is genuinely a YAML config key**, not hard-coded. `docker.Config.RetentionMaxAge`
  (`internal/backend/docker/config.go:211`, `yaml:"retention_max_age"`) is defaulted to 90d
  (`config.go:332`), validated `>= 0` (`config.go:489`), and loaded via `DefaultConfig()` →
  `yaml.Unmarshal` overlay (`cmd/docker-backend/main.go:208-224`). It is read at runtime
  (`restore.go`, `info.go:172`). The ticket's "confirm wired vs hard-coded" resolves: **wired.**
- **A per-tenant cap exists**: `max_retained_leases_per_tenant` (`config.go:221`), enforced same-tenant,
  oldest-first by `evictRetentionsToCap` (`restore.go:185`, called from `deprovision.go:175`).
- Both keys are documented in `docker-backend.example.yaml` and `internal/backend/docker/README.md`.
- Duration values must use Go units (`h`/`m`/`s`); `14d` does **not** parse. The example file correctly
  uses `2160h`.

### The corrected premise (the real bug)

The ticket states retained volumes are *"charged against the same `total_disk_mb` pool that gates
provisioning"* → *"provisioning stops."* **This is not what the code does**, and the actual behavior is
worse:

- `ResourcePool` is a **logical SKU-reservation gate**: a live lease reserves its full SKU `DiskMB`
  against the operator-set `total_disk_mb` budget, regardless of bytes actually written
  (`internal/backend/shared/resources.go:92` — `allocatedDisk + profile.DiskMB > totalDisk`).
- On close, `deprovision.go` calls `pool.Release()` for **every** item **regardless of
  `retain_on_close`** (`deprovision.go:111-124`), *before* the soft-delete branch. So a retained
  volume's `DiskMB` is **returned to the free pool** even though the volume still occupies disk.
- The volume is only *renamed*; `RenameVolume` on XFS preserves the project ID and re-applies the
  `bhard` quota (`volume_xfs.go`). Note: `bhard` is a **hard block-limit ceiling, not a reservation** —
  a retained idle volume physically pins only the bytes it actually wrote, capped at the quota.

**Net effect:** the pool **over-commits its own budget**. It can admit new provisions whose combined
SKU reservations exceed `total_disk_mb`, so when `total_disk_mb` is sized to physical capacity the disk
fills and tenants hit **ENOSPC at write time** — a silent failure, strictly worse than the clean
"provisioning stops" the ticket assumed.

There is also **no metric** for the retained footprint (the existing `resource_disk_allocated_ratio`
gauge *drops* on close and is blind to retained volumes), and **no runbook** for reclaiming retained
volumes under disk pressure.

### Why the fix is small and idiomatic

Web research (K8s Released PVs, EBS Recycle Bin, GCS/Azure/S3 soft-delete, ZFS reservations, LVM thin
pools) converges on one principle: **a retained/soft-deleted-but-physically-present tier is never
treated as freed capacity — it keeps counting against the ceiling until a reaper actually reclaims it.**
Because fred's live pool *already* reserves by SKU ceiling, accounting retained volumes the same way is
**symmetric and consistent**, not a new accounting model. The fix is to stop crediting the pool for
retained bytes until the volume is truly destroyed.

---

## 2. Goals / Non-goals

### Goals

1. **Accounting fix (correctness core):** the disk admission gate accounts for retained-volume
   reservations, so over-commit surfaces as a clean provisioning denial, never tenant ENOSPC.
2. **Per-provider cap:** a new `max_retained_disk_mb` config key bounding the aggregate retained
   footprint, with a **refuse-to-retain** breach policy.
3. **Observability:** Prometheus gauges for retained bytes and count, plus a counter for refuse-to-retain
   events, so the disk-pool pressure is attributable and a Grafana panel/alert can be built.
4. **Docs:** an OPERATIONS.md reclaim runbook, ARCHITECTURE.md metric rows, example/README config docs,
   and a CHANGELOG entry.

### Correctness invariant (load-bearing)

The no-ENOSPC guarantee of Goal 1 holds **only if `total_disk_mb <= the data filesystem's usable
capacity`** — i.e. fred is *thick-provisioned* and does not over-commit physical disk. XFS `bhard` is a
hard *block-limit ceiling*, not a physical reservation: a write can still fail `EDQUOT`/`ENOSPC` when
the filesystem is full even if the project is under quota. So `Σ(live + retained hard quotas) <=
total_disk_mb` removes fred's own admission over-commit (the ENG-360 bug), and that translates into a
real no-tenant-ENOSPC guarantee **only when `total_disk_mb` is itself ≤ usable capacity**. "Usable" is
the raw device minus filesystem/metadata/inode and XFS per-AG reservation overhead (and minus any
non-fred consumers sharing the disk — Docker image layers, logs), so operators must leave a few percent
headroom rather than set `total_disk_mb` to the raw device size. Sizing `total_disk_mb` above usable
capacity re-introduces thin-provisioning over-commit and the exact ENOSPC class this work eliminates.
This invariant is stated to operators in the docs and reinforced at startup (§3.1), though the startup
WARN is a coarse guard only (see §3.1 for the exact check performed).

### Non-goals (explicit)

- **Per-tenant retained-bytes quota** — deferred to a follow-up. v1 relies on the existing
  `max_retained_leases_per_tenant` as the per-tenant lever. (See §6.)
- **Measured on-disk usage accounting** (`du`/`xfs_quota report` per volume). We account by **declared
  SKU quota**, consistent with the live pool, FS-agnostic, exec-free, and not tenant-gameable.
- **Changing the SKU-reservation model** (reserve-by-quota, not reserve-by-usage) — out of scope.
- **A manual single-lease reclaim API** (admin endpoint/CLI/gRPC). The runbook uses existing config
  levers + reaper instead. (See §6.)
- **manifest-deploy inventory var** and **Grafana dashboards/alerts** — other repos.

---

## 3. Design

### 3.1 Retained disk accounting (`internal/backend/shared/resources.go`)

Add a single aggregate retained-disk term to `ResourcePool`, **derived from the retention store** (the
single source of truth) — never an independently-mutated counter that can drift from disk reality.

- New field `retainedDiskMB int64` on `ResourcePool`.
- New method `SetRetainedDiskMB(mb int64)` (mutex-guarded).
- Admission gate becomes:
  `p.allocatedDisk + p.retainedDiskMB + profile.DiskMB > p.totalDisk → insufficient disk`.
- `Stats()` (and `ResourceStats`) expose `RetainedDiskMB` so the metric exporter can read it.

`Release()` stays **unchanged** — retained accounting is a separate projection, so we avoid
partial-release complexity in the per-lease allocation map. The live `allocations` map stays purely live.

**Why derive, not duplicate:** `retainedDiskMB` is a cached projection of the bbolt retention store. The
store is already crash-safe and reconciled across restart/partial-reap (`ReapIfExpired`/`DeleteIfActive`
re-record-on-failure). Rebuilding the projection from the store removes any "two counters drift"
failure mode.

**Startup capacity sanity check (defense-in-depth, WARN-only).** At startup, `warnIfOverProvisioned`
calls `syscall.Statfs` on `VolumeDataPath` and **WARNs** if `total_disk_mb` exceeds the filesystem's
**gross total** (`f_blocks × block-size`). This is a *coarse upper-bound guard*, not a usable-capacity
check: `f_blocks` is the post-mkfs total and still INCLUDES root-reserved blocks and non-fred consumers
(Docker image layers, logs, etc.), so the WARN fires only when `total_disk_mb` exceeds even the raw
device size — a late signal. Operators must therefore size `total_disk_mb` below the true usable
capacity themselves; the WARN will NOT fire if `total_disk_mb` is merely above usable but below
gross total. This matches the already-accurate OPERATIONS.md wording. *WARN-only, not refuse-to-start*:
capacity legitimately fluctuates and a hard refusal would turn a benign mis-size into an outage. A
*periodic* statfs admission tripwire is an explicit **non-goal** for v1 — the hard-quota-sum model
plus the startup check is sufficient for a single-node backend, and a live tripwire adds a poller +
reconciliation surface we don't need yet.

### 3.2 Recompute seam (`internal/backend/docker`)

A backend helper recomputes the retained projection from the store and updates the gauges:

```
func (b *Backend) refreshRetentionAccounting(ctx) {
    entries := b.retentionStore.List()                       // active entries
    mb := Σ over active entries: Σ items GetSKUProfile(item.SKU).DiskMB × item.Quantity
    b.pool.SetRetainedDiskMB(mb)
    set fred_docker_backend_retained_volume_bytes = mb << 20
    set fred_docker_backend_retained_leases       = len(active entries)
}
```

Called at every point the retained set changes, alongside the existing `updateResourceMetrics` calls:

- **close + retain** (`deprovision.go`, after the retention record is written),
- **reap** (`restore.go reapExpiredRetentions`),
- **cap-eviction** (`restore.go evictRetentionsToCap`),
- **restore claim / complete / revert** (`restore.go`),
- **startup recovery** (`recover.go`, which already reads the retention store at `recover.go:581`) —
  and, because `recoverState` runs on the reconcile loop (every `reconcile_interval`, default ~5 min),
  this is a **second authoritative recompute** alongside the sweep, bounding any transient projection
  drift to `min(reconcile_interval, sweep_interval)`,
- **the retention sweep tick** (`runRetentionSweep`) so the gauges stay fresh between lease events.

**Active vs restoring:** the projection counts `Status == active` entries only, matching the existing
retention accounting (`evictRetentionsToCap`/`reapExpiredRetentions` already filter to active). When
`ClaimForRestore` flips active→restoring, the bytes leave the retained projection and the restored
lease's `TryAllocate` picks them up as live — net-neutral, no double count, modeling restore's
**adopt** (rename) semantics. *Implementation check:* confirm restore adopts (renames) rather than
copies the retained volume; if it copies (transient 2× on disk), switch the projection to count
`active + restoring`. Covered by a test either way.

**Transition ordering — never under-count.** Level-triggered recompute bounds drift to one sweep tick,
but per-event ordering still matters: the *dangerous* direction is briefly counting bytes in **neither**
`allocatedDisk` nor `retainedDiskMB` (transient under-count → could over-admit), whereas
double-counting only over-denies (safe). So on restore, the restored lease's `TryAllocate` (live) must
run **before** the retained projection is recomputed/dropped; equivalently, recompute retained strictly
after the `active→restoring` flip is committed to bbolt. The periodic sweep recompute is **authoritative**
(any per-event ordering slip self-heals within one tick). A test asserts no interleaving yields an
under-count/over-admit.

### 3.3 Per-provider cap + refuse-to-retain (`config.go`, `deprovision.go`)

- New config key `MaxRetainedDiskMB int64` (`yaml:"max_retained_disk_mb"`), default `0` = unlimited
  (backward-compatible; current behavior preserved when unset).
- `Validate()`:
  - `max_retained_disk_mb >= 0`;
  - if `> 0`: `max_retained_disk_mb <= total_disk_mb` (can't reserve more than the pool) **and**
    `>= the largest single stateful SKU footprint` (so a single SKU-legal lease can never be
    un-retainable by construction);
  - **WARN** (not reject) when `max_retained_disk_mb > 0 && max_retained_leases_per_tenant == 0`: an
    aggregate cap with no per-tenant lever lets one well-funded tenant fill the entire retained pool,
    degrading all others to refuse-to-retain (an availability DoS on the retention feature). The WARN
    nudges the operator to set the per-tenant count cap.
  - **Relationship to `tenant_quota.max_disk_mb` (documented, not enforced):** the per-provider retained
    cap is deliberately *independent* of and may be *smaller* than `tenant_quota.max_disk_mb` (which is a
    per-tenant **aggregate live** cap, not a single-lease size). We do **not** floor the retained cap at
    `tenant_quota.max_disk_mb` — that would force the retained pool to absorb one tenant's entire live
    budget, disproportionate for a single-node box. The field doc-comment must call out the consequence:
    a tenant's max-sized lease can be SKU-legal yet refused retention when the retained cap is smaller.
- **Breach handling at close+retain:** compute the closing lease's reserved bytes
  (`Σ items GetSKUProfile(SKU).DiskMB × Qty`). If `retainedDiskMB + incoming > max_retained_disk_mb`
  (cap `> 0`): **refuse to retain** — destroy the lease's volumes immediately (the existing
  non-retain close path), do **not** write a retention record, emit a WARN log and increment
  `fred_docker_backend_retention_refused_total`.

**Breach policy = refuse-to-retain (the closing lease only).** Rationale (full research in the ENG-360
worktree transcripts):

- **Global FIFO is rejected unconditionally.** In fred's permissionless, mutually-untrusting model it is
  a weaponizable cross-tenant data-destruction primitive (open/close large `retain_on_close` leases to
  flush a victim's in-grace volumes via a legitimate path). All researched multi-tenant quota systems
  (K8s ResourceQuota, Exchange Recoverable Items, EBS Recycle Bin, GCS soft-delete) enforce caps
  per-owner and never destroy another owner's protected data to admit a new item.
- **Refuse-to-retain is the most guarantee-preserving option** — it never destroys *any* in-grace data,
  only declines to *create* a new retention record. It is the simplest correct policy and matches the
  ENG-325 grace-window promise. Same-tenant eviction (the count cap's model) was considered but at a
  *global* cap it collapses into refuse-to-retain whenever the closing tenant has little retained data
  of its own — i.e. exactly the case the global cap exists for — so it adds machinery for no benefit in
  v1.

**Observability of a refusal.** A refused lease writes **no** retention record, so today's
`GetProvision` (`info.go:166`) would *not* report it as retained — it falls through to the diagnostics
fallback (`Status=failed`) or 404, **indistinguishable from any other failed close**. (An earlier draft
wrongly claimed the ENG-329 close-notice already distinguishes this; verified false.)

- **v1 (this PR): operator signal.** The `fred_docker_backend_retention_refused_total` counter + a WARN
  log make every refusal explicit to operators, who are the actors that resize/reclaim capacity.
- **Follow-up (deferred, §6): tenant-facing distinct status.** A tenant who set `retain_on_close: true`
  ideally learns their data was destroyed, not retained. But refuse-to-retain is *semantically a
  successful close* — routing it through the `failed` diagnostics path would mislead, and a proper
  signal requires a new status threaded through the ENG-329 **close-notice** surface (its own area).
  Deferred so v1 stays focused; transparency-about-quota best practice is satisfied for operators now
  and tracked for tenants as a follow-up.

### 3.4 Metrics (`internal/backend/docker/metrics.go`)

House convention is `Namespace="fred"`, `Subsystem="docker_backend"`. No `{backend}` label (single-node;
the subsystem already names the backend; existing metrics carry none) and **no per-tenant label**
(unbounded cardinality in a permissionless model).

| Metric | Type | Unit | Meaning |
|---|---|---|---|
| `fred_docker_backend_retained_volume_bytes` | gauge | bytes | Reserved capacity (SKU quota) currently pinned by retained volumes. Converted MB→bytes at the exporter boundary (Prometheus base-unit convention). Help text notes this is reserved quota, not measured usage. |
| `fred_docker_backend_retained_leases` | gauge | count | Number of active retained leases. (No `_total` suffix — it is a gauge.) |
| `fred_docker_backend_retention_refused_total` | counter | events | Count of close-time refuse-to-retain events due to `max_retained_disk_mb`. |
| `fred_docker_backend_disk_pool_bytes` | gauge | bytes | The admission ceiling (`total_disk_mb << 20`). Denominator so dashboards don't hardcode the pool size. |
| `fred_docker_backend_retained_disk_cap_bytes` | gauge | bytes | The per-provider retained cap (`max_retained_disk_mb << 20`). A registered gauge always exports, so it reads **0 when the cap is unset**; alert queries MUST guard `> 0` before dividing by it. Alert denominator. |

These are exactly what the (separate-repo) Grafana panel + alert consume: the panel computes
`retained_volume_bytes / disk_pool_bytes` to attribute pressure to retained vs live, the alert fires on
`retained_volume_bytes / retained_disk_cap_bytes` (guarded with `retained_disk_cap_bytes > 0` to avoid
a divide-by-zero `+Inf` false positive on providers with no cap configured) approaching 1 and on `retention_refused_total`
increasing — **with no hardcoded constants**. Shipping the limit alongside the state mirrors
`node_filesystem_size_bytes` (next to `avail`) and `kube_resourcequota` (`type=hard` next to
`type=used`); both denominator values are already in memory, so the gauges are near-free.

**Reason-label foresight (no new code now):** `retention_refused_total` ships **label-free** for v1's
single refusal cause. If the §6 per-tenant retained-bytes quota lands later, its refusals MUST be added
as a bounded `reason` label on this same counter (`reason="provider_cap"` / `reason="tenant_quota"`),
**not** a new sibling metric — Prometheus prefers one metric with a low-cardinality breakdown label.

### 3.5 Docs

- **OPERATIONS.md** (root runbook — *not* a new `docs/operations.md`): a new section
  **"## Reclaiming retained volumes under disk pressure"** placed after "Backend at capacity", plus a row
  in the "Common alerts" table pointing at it (the row keys on `retention_refused_total` increasing and
  `retained_volume_bytes` trending toward the pool, with a "First step" link to the section). Follows the
  repo's alert-row→section convention and the SRE trigger→impact→diagnosis→mitigation shape. Reclaim
  levers, ordered **least-destructive-first**: (1) read the new gauges to size the problem; (2) raise
  capacity / lower retention pressure by shortening `retention_max_age` (shrinks the grace window →
  reaper sweeps sooner); (3) set/lower `max_retained_disk_mb` and `max_retained_leases_per_tenant`
  (bounds the tier; future closes refuse-to-retain / evict same-tenant); (4) restart for the boot-eager
  reaper. The section states that `max_retained_disk_mb` directly trades retained-grace capacity against
  live-provision capacity within the one `total_disk_mb` pool, and restates the §2 invariant
  (`total_disk_mb ≤ usable capacity`).
- **ARCHITECTURE.md:** add the five metrics to the docker-backend metrics table (§ ~620-661).
- **docker-backend.example.yaml** + **README.md:** document `max_retained_disk_mb` in the existing
  "SOFT-DELETE & RETENTION" block / config table. Add a **Go-duration units caveat** next to the
  duration keys ("Duration values use Go syntax — `h`/`m`/`s`; days/weeks are not supported. Use `2160h`
  for 90 days, `336h` for 14 days") in the example, the README config-table preamble, and beside the
  OPERATIONS.md "lower `retention_max_age`" instruction. (A custom day-aware unmarshaler is an explicit
  non-goal — it would diverge from `time.ParseDuration` and every other duration key.)
- **CHANGELOG.md `[Unreleased]`:**
  - `### Changed` (placed **first**, labeled) — **Behavior change:** retained volumes now count against
    the disk pool until reaped, closing a disk over-commit / ENOSPC risk. **Operators:** if retention is
    enabled, re-check `total_disk_mb` headroom — effective live capacity now decreases by the retained
    footprint; see the OPERATIONS.md reclaim section. (ENG-360) — one line + cross-link, per Common
    Changelog (detailed guidance lives in the runbook, not the changelog).
  - `### Added` — `max_retained_disk_mb` config key; retained-volume + disk-pool metrics (ENG-360).
  - No feature flag (correct: retention is pre-production / deploy held; a flag would be YAGNI
    machinery needing dual-state testing and later removal).

---

## 4. Testing (TDD)

Pin behavior before implementing. Pair `-race` with `-short` (stress tests OOM under the race detector).

**Pool unit (`resources_test.go`):**
- Admission denies when `allocatedDisk + retainedDiskMB + new.DiskMB > totalDisk`; admits at the boundary.
- `SetRetainedDiskMB` updates the gate; `Stats().RetainedDiskMB` reflects it.

**Accounting integration (`internal/backend/docker`):**
- Close+retain: live disk released from `allocatedDisk`, retained projection rises by the SKU bytes;
  *available* disk for new provisions is unchanged net (no over-commit).
- A provision that would fit only if retained were free is **denied** while a retained volume exists.
- Reap / cap-eviction: retained projection drops; freed disk becomes available.
- Restore: retained projection transfers to live (no double count); failed restore reverts.
- `recoverState`: retained projection rebuilt from the store on startup.

- **Ordering / never under-count:** an interleaving where bytes leave the retained projection before the
  restored lease's `TryAllocate` must not transiently under-count (over-admit); assert double-count
  (over-deny) is the only transient error possible, and the sweep recompute restores the exact value.

**Cap + breach:**
- Close+retain that would breach `max_retained_disk_mb` → volumes destroyed immediately, no retention
  record written, `retention_refused_total` increments, WARN logged.
- `Validate()` rejects `max_retained_disk_mb < 0`, `> total_disk_mb`, or `< largest stateful SKU`; WARNs
  when `max_retained_disk_mb > 0 && max_retained_leases_per_tenant == 0`.

**Startup / config:**
- Startup capacity check WARNs (does not refuse) when `total_disk_mb` exceeds the data filesystem's
  usable bytes (statfs stubbed in test).

**Metrics:** retained gauges reflect the projection after each transition; bytes = MB << 20;
`disk_pool_bytes` = `total_disk_mb << 20`; `retained_disk_cap_bytes` = `max_retained_disk_mb << 20` (0 when the cap is unset).

---

## 5. Files touched

| File | Change |
|---|---|
| `internal/backend/shared/resources.go` | `retainedDiskMB` field, `SetRetainedDiskMB`, admission gate, `Stats`/`ResourceStats` |
| `internal/backend/docker/config.go` | `MaxRetainedDiskMB` key + `Validate` rules (incl. count-cap WARN) |
| `internal/backend/docker/deprovision.go` | refuse-to-retain breach check at close; call `refreshRetentionAccounting` |
| `internal/backend/docker/restore.go` | call `refreshRetentionAccounting` on reap/evict/restore |
| `internal/backend/docker/recover.go` | rebuild retained projection on recover |
| `internal/backend/docker/metrics.go` | 5 new metrics + exporter wiring |
| `internal/backend/docker/backend.go` | `refreshRetentionAccounting` helper; startup statfs WARN |
| `OPERATIONS.md`, `ARCHITECTURE.md`, `docker-backend.example.yaml`, `internal/backend/docker/README.md`, `CHANGELOG.md` | docs |
| `*_test.go` | tests above |

---

## 6. Follow-ups (out of scope for this PR)

- **Per-tenant retained-bytes quota** — a global-only cap lets a well-funded tenant monopolize the
  retained pool, silently degrading other tenants to refuse-to-retain. v1 mitigates with the existing
  per-tenant *count* cap, but only **when it is set (default 0 = unlimited) and SKU sizes are bounded** —
  it is a partial mitigation, not equivalent protection. The startup WARN (§3.3) nudges operators to set
  it. A per-tenant *bytes* quota (same refuse-to-retain enforcement, refusals tagged via the `reason`
  label reserved in §3.4) is the proper fix if monopolization is observed. File as ENG-360 follow-up.
- **`retention_recorded_total` attempts counter** — a companion counter incremented on successful
  retention-record write, so consumers can compute a refused/attempted *rate* rather than just an
  absolute refusal count. Low value for v1 (a non-zero rise in `retention_refused_total` already alerts).
- **Tenant-facing "retention refused — data destroyed" status** — surface a refusal to the *tenant* (not
  just operators) via a new status threaded through the ENG-329 close-notice surface. Deferred from v1
  because refuse-to-retain is semantically a successful close (the `failed` diagnostics path would
  mislead) and the close-notice is a separate area. v1 ships the operator counter + WARN. (§3.3.)
- **Record `SizeMB` on `RetentionEntry`** — v1 derives retained bytes from `Items` + `GetSKUProfile` at
  recompute time. Pinning the bytes on the record at close time would make accounting immune to
  mid-retention SKU-profile edits and cheaper to sum. Optional optimization.
- **Manual single-lease reclaim API** — the runbook uses config levers + reaper. A targeted admin
  reclaim endpoint could be added if operators need finer control.
- **manifest-deploy** `fred_retention_max_age` (+ new `fred_max_retained_disk_mb`) inventory vars, and
  the **Grafana** retained-volume panel/alert — separate repos.
