# Changelog

All notable changes to Fred are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New metric `fred_provisioner_ack_batcher_lane_restarts_total{lane}` counts
  ack-batcher lanes respawned after a recovered panic (see Fixed). Pair with
  `fred_goroutine_panics_total{component="ack_batcher"}` to detect a
  crash-looping lane.

### Changed

### Deprecated

### Removed

### Fixed

- Crash recovery (`recoverState`) no longer resurrects a lease that is mid
  volume-cleanup-retry (`Failed` with `VolumeCleanupAttempts > 0`) back to
  `Ready` from a stale container snapshot. Such a lease has already had its
  containers torn down, so any container a concurrent reconcile still lists for
  it was captured before the removal; merging that stale view after the
  deprovision set `Failed` could reset the retry counter and — once the next
  reconcile GC'd the resulting phantom-`Ready` no-container entry — abandon the
  volume-cleanup retry and leak the lease's pool reservation until process
  restart. The entry is now preserved by pointer across the reconcile map swap,
  matching the existing `Deprovisioning` preserve-case. Fixes the intermittent
  `TestDeprovision_VolumeRetry_ConcurrentRecoverState` CI flake. (ENG-603)
- Ack-batcher lanes now respawn after a recovered panic instead of exiting
  permanently. Previously a single cosmos-SDK marshaling panic on a malformed
  chain RPC response killed the lane for good; at the default single-lane
  configuration that disabled **all** acknowledgment, and the timeout checker
  then wrongly rejected healthy, successfully-provisioned leases. Lanes are
  supervised and restarted; restarts are naturally paced by the batch interval.
  (ENG-589)
- The docker-backend now runs crash recovery and legacy migration under the
  backend lifecycle context instead of the caller's 30s startup context. The
  startup context was canceled the instant `Start` returned, which (1) capped
  the migration health-wait below 30s so a legacy workload that legitimately
  took 30–90s to become healthy failed startup, and (2) fired the `-prev`
  grace-cleanup goroutines' cancellation at ~0s, permanently leaking every
  migration's `-prev` containers. Fast connectivity/capability checks still run
  under the short startup context so an unreachable daemon fails fast. (ENG-592)

### Security

- `GET /logs` and diagnostics log capture now bound the **aggregate** bytes
  buffered across all of a lease's containers (32 MiB per call), not just the
  existing 5 MiB per-container cap. A lease may have up to 1024 containers, so
  without an aggregate budget one authenticated `/logs` request could
  materialize gigabytes and OOM the shared docker-backend host — cross-tenant
  denial of service. Output beyond the budget is truncated with a marker.
  (ENG-590)

## [0.11.0] - 2026-07-22

### Added

- Retention records gained an optional `partition` field (a cooperative
  sub-tenant grouping key within one on-chain tenant) plus the pure
  extraction/validation library for it. Nothing writes a non-empty partition
  yet — behavior and stored bytes are identical to the previous release.
- Retention partitioning: an operator-configured `retention_partition_source`
  plus a `retention_tenant_budgets` aggregator allowlist let one on-chain
  tenant's retention be capped and evicted per end-customer
  (`(tenant, partition)` scope), with per-tenant aggregate caps always
  binding (partitions only sub-divide, never raise). New knob
  `max_retained_disk_mb_per_tenant` (default 0 = unlimited). New metrics:
  `retention_partition_{collapsed_total,stamped_total,evicted_total}`,
  `retention_partitions`, `retention_refused_by_scope_total`,
  `retention_cap_check_failed_total`; existing `retention_refused_total` /
  `retention_evicted_total` keep their exact deployed meanings. Everything
  defaults off; with no config the release is behavior- and byte-identical.

### Changed

- docs: mark the `btrfs` and `zfs` volume backends as experimental and untested,
  and document `xfs` as the only backend validated and used in production. The
  docker backend implements all three `volumeManager` filesystems, but only XFS
  is deployed and exercised in prod (all mainnet/Morpheus backends run XFS with
  `pquota`, and per-volume disk and inode `ihard` quotas are enforced only on
  XFS), so the docker-backend README, `docker-backend.example.yaml`, and
  DEPLOYMENT.md now recommend `xfs` for production and flag btrfs/zfs as
  experimental rather than presenting the three as coequal choices. Docs-only; no
  behavior change. (ENG-564)

### Deprecated

### Removed

### Fixed

- Retention close path now runs the per-tenant count-cap eviction BEFORE the
  retained-disk refusal gate, so a full rolling window rolls instead of
  refusing (destroying) every subsequent close; eviction order is now a total
  order (CreatedAt, then lease UUID). In the rare close where both caps would
  trip, the tenant's oldest record is evicted first and the incoming close may
  then be retained (previously the incoming was destroyed and no eviction ran).

### Security

- manifest: bound the number of ports (and, defense-in-depth, expose entries,
  env vars, and labels) a tenant may declare per service. `flatManifest.Ports`
  was validated per entry but never counted, and every entry becomes a published
  host port plus an iptables DNAT rule (`userland-proxy:false` in prod), so a
  single cheap one-container lease could POST tens of thousands of port entries
  via `/update` (bounded only by the ~1 MB request body) and exhaust the shared
  host ephemeral-port range and netfilter — a host-wide, cross-tenant
  provisioning DoS. `Manifest.validate` now rejects manifests exceeding
  `MaxPorts` (64), `MaxExposePorts` (64), `MaxEnvVars` (256), or `MaxLabels`
  (128) — all far above any legitimate single-container workload — before any
  per-entry work, on both the provision and update paths (`ParsePayload`). The
  number of services in a stack remains bounded separately by
  `ValidateStackAgainstItems` (1:1 with paid lease items). (ENG-547)
- docker: preserve an in-flight lease's admission reservation across the periodic
  state rebuild. `recoverState` rebuilds the resource pool from live containers
  and then replaces it wholesale, but it excludes leases mid-operation
  (`Provisioning`/`Restarting`/`Updating`) from that rebuild — a still-pulling
  provision has no containers yet, and a restart/update is mid-cleanup. The
  full-replace `Reset` therefore dropped those leases' authoritative
  `TryAllocate` reservation for the whole window, so `TryAllocate` briefly saw
  phantom free capacity and could admit leases past physical CPU/memory/disk (and
  past a tenant's quota); once the slow lease re-registered, the pool was left
  over-committed (`allocatedDisk > totalDisk`), and because each volume carries a
  hard XFS quota the summed quotas could exceed physical disk and cause
  cross-tenant `ENOSPC`. Recovery now carries those in-flight reservations
  forward from the live pool — read back from the pool itself rather than
  reconstructed from lease state, so it is correct even before the lease's
  `Items` are populated — via a new `ResourcePool.ResetPreserving`, keyed
  identically so the synchronous reservation survives the rebuild. (ENG-546)
- docker: extend that in-flight reservation preservation to leases mid-close.
  `doDeprovision` removes a lease's containers *before* releasing its pool
  reservation — it defers the release until after the volume-destroy / retention
  work so the footprint is never momentarily uncounted while bytes still persist
  on disk — so a `Deprovisioning` lease can have no containers yet still hold its
  reservation. `recoverState` was dropping it on the periodic rebuild (the same
  phantom-capacity over-admission as above, reached via the close path). Recovery
  now preserves `Deprovisioning` reservations too, counted exactly once (dedup
  against any still-present container), matching the deprovision path's
  deliberate release-after-teardown ordering. (ENG-562)
- docker: extend that preservation to a lease whose close is mid-retry. When
  `doDeprovision` removes a lease's containers but a volume destroy/rename then
  fails (under the retry limit), it keeps the lease `Failed` and — because the
  bytes are still on disk — deliberately does not release its pool reservation.
  `recoverState` was dropping that still-held reservation on the periodic rebuild
  (`Failed` was excluded from preservation): the same over-admission, reached via
  the failed-cleanup path. Recovery now preserves a `Failed` lease's reservation
  only in that volume-cleanup-retry sub-state (`VolumeCleanupAttempts > 0`); a
  genuinely-failed provision, whose reservation was already released, is still
  dropped. (ENG-563)
- docker: `recoverState` now preserves the pool reservation of every tracked
  lease by a single structural rule (the pool is authoritative for a tracked
  lease's footprint), replacing the per-status allowlist grown across ENG-546/
  562/563. Besides subsuming those, it closes three more admission under-counts
  the allowlist missed — a `Ready`→crash→GC'd `Failed` lease, a restore-rollback
  re-quarantine failure, and a deprovision partial-removal failure — all of which
  hold a reservation for bytes still on disk that the allowlist dropped, letting
  `TryAllocate` over-admit past physical disk. Container-derived rebuild is
  retained only to seed the pool on cold start and re-establish a lost key from
  running containers. (ENG-567)
- docker: gate the disk **promote delta** when restoring a lease onto a
  larger-disk SKU tier. `Restore` adopts retained volumes via a pool allocation
  that intentionally skips the global disk-capacity check (the adopted bytes are
  already committed on disk and counted in the retained projection). On a
  *promote* (new SKU `disk_mb` greater than the retained tier's), the extra
  headroom was never checked against free capacity, so a tenant could soft-close
  a small-tier lease and restore it onto a much larger tier — repeatedly — to
  drive committed XFS quota past physical disk and cause cross-tenant `ENOSPC`.
  The adopt path now gates only the growth above the retained footprint
  (`max(0, new − old)` disk); same-tier and demote restores still skip the gate
  as before. (ENG-545)
- docker: XFS tenant volumes now carry a per-volume inode hard limit (`ihard`)
  alongside the block limit, bounding host inode exhaustion from zero-byte-file
  floods. The ceiling is `disk_mb × 1 MiB / min_avg_file_bytes` (new
  `min_avg_file_bytes` knob, default 1024 → 1024 inodes/MiB, floored at 262144
  inodes); a workload whose average file is smaller than `min_avg_file_bytes`
  may now hit `EDQUOT` on inodes. A filesystem-agnostic tar entry-count cap
  backstops crafted-image extraction. (ENG-548)
- deps: bump `golang.org/x/text` to v0.40.0 (from v0.37.0) to resolve
  GO-2026-5970, an infinite-loop-on-invalid-input DoS in `x/text` normalization
  (fixed upstream in v0.39.0). The advisory was published after the v0.10.0 cut
  and flagged the `x/text` version already in the tree, so the CI vuln gate is
  the only thing that changed; this bump keeps it green. Also pulls
  `golang.org/x/sync` to v0.22.0, required by `x/text` v0.40.0.

## [0.10.0] - 2026-07-16

### Added

### Changed

### Deprecated

### Removed

### Fixed

- manifest: a `NONE` health check with trailing arguments (e.g.
  `["NONE", "extra"]`) is now rejected instead of silently accepted. The Go
  validator previously ignored elements after `NONE` while the JSON schema
  (`docs/manifest-schema.json`) rejected them; both now agree that `NONE` takes
  no arguments. The canonical `["NONE"]` form is unaffected.

### Security

- docker: harden stateful-volume bind setup against a symlink escape. A tenant
  could plant a symlink inside its read-write stateful volume (e.g. `data -> /`)
  and, on a later deploy declaring a matching `VOLUME`, have
  `buildStatefulVolumeBinds` follow it — bind-mounting or `chown`-ing an arbitrary
  host path (host `/`, another tenant's volume) into the container with
  root-equivalent access. `sanitizeVolumePath` only validated the VOLUME *string*,
  so the raw `os.MkdirAll`/`os.Chown` traversed the on-disk symlink. Subdirectory
  creation is now confined to the volume root via `os.Root` (mirroring the ENG-430
  tar-extraction hardening), so a VOLUME path that traverses a symlink escaping the
  volume root is rejected instead of followed. The same guard is applied to the
  legacy→stack migration bind path (`migrate.go`). (ENG-539)
- docker: harden the writable-path (`_wp`) scaffolding against an image-seeded
  symlink escape (defense-in-depth follow-up to ENG-539). The `_wp` bind Source is
  mounted read-write into the container, and Docker's `CopyFromContainer` does not
  follow a final-component symlink — so a symlink at the Source would redirect the
  mount outside the volume. `DetectWritablePaths` only yields real directories, so
  this was not exploitable, but the extraction and bind paths now defend themselves
  regardless: `writablePathExtractDir` creates the extraction target via `os.Root`
  (an escaping symlink planted by an earlier path can no longer redirect it), and
  `setupWritablePathBinds` skips any bind whose Source is a symlink or escapes the
  `_wp` root. (ENG-543)

## [0.9.0] - 2026-07-14

### Added

- config: `withdraw_limit` (default `100`) sets how many active leases are settled
  per provider-wide withdrawal transaction (`MsgWithdraw.Limit`). Previously fred
  sent `Limit: 0`, so the chain applied its default of 50; the effective page size
  is now 100 (the chain's `MaxBatchLeaseSize` ceiling), halving the number of
  settlement txs per cycle as a provider's active-lease count grows. The withdrawal
  cycle still pages over the cursor, so this only trades tx count against per-tx gas
  — validated to `1..MaxBatchLeaseSize`. Operators near a full page should confirm
  `gas_limit` (the Simulate-down fallback) covers a 100-lease withdrawal. (ENG-529)
- test: Go native fuzz harnesses for the two tenant-facing custom parsers.
  `FuzzSanitizeAndExtractTar` drives the tar extractor with arbitrary archive
  bytes and byte budgets, asserting it never panics, keeps writes within
  `maxBytes` on disk, and never creates a real path outside the destination.
  `FuzzParsePayload` drives the manifest parser and asserts every payload it
  accepts satisfies the validation invariants (DNS-safe service names, no
  reserved `fred.*`/`traefik.*` label prefixes, bounded/absolute tmpfs mounts) —
  a validation-bypass hunter, not just a crash finder. fred previously had no
  fuzz coverage. (ENG-521)
- providerd: new `credit_check_interval` config knob that decouples the
  credit-check cadence from the paid provider-withdrawal cadence; when set
  shorter than `withdraw_interval`, the paid withdrawal is rate-limited to
  `withdraw_interval` while credit checks keep running frequently. Default
  (`0`) preserves the previous coupled behavior. Adds the
  `fred_withdraw_skipped_by_guard_total` counter and the
  `fred_withdraw_guard_active` gauge. (ENG-524)

### Changed

- scheduler: when `credit_check_interval` is set shorter than
  `withdraw_interval`, the provider-revenue withdrawal is rate-limited to its own
  interval instead of running on every credit-check wake. (ENG-524)

### Deprecated

### Removed

### Fixed

- docker-backend: reject a lease whose total (or per-item) container quantity is
  negative or exceeds a hard cap before reserving any state, closing a pre-admission
  memory-exhaustion DoS. `Provision` pre-sized the per-lease container-ID slice from
  the chain-supplied quantity (`make([]string, 0, totalQuantity)`) inside the lock —
  before SKU validation and the resource-pool admission gate — so an honest
  max-quantity lease (billing caps per-item quantity at 1e9) drove a ~16 GB
  allocation, and a negative value from an overflowed `uint64→int` cast panicked the
  `make`. Total and per-item quantity are now validated against `maxLeaseQuantity`
  (1024) at the top of `Provision` and rejected as a validation error before any
  allocation. (ENG-503)
- docker-backend: closed three retention/restore data-integrity gaps that could
  destroy or tear down live tenant data:
  - the startup orphan-volume reaper no longer destroys a still-open lease's
    volume when its containers were removed out-of-band (e.g. an operator
    prune) — it now skips any volume whose lease still has an active release,
    instead of reaping on a single boot observation. (ENG-505)
  - the orphan-record pruner no longer prunes a give-up-diverged retention
    record whose volume is still on disk under its canonical name; it now
    treats the canonical (not-yet-renamed) form as present, not just the
    `fred-retained-*` name, so a later boot can't reap the intact data.
    (ENG-501)
  - `reconcileRestoring` no longer tears down a running new lease in the
    Updating/Deprovisioning state when a completed restore's record lingered
    past a failed terminal delete — it defers for any live non-Failed
    provision, running the orphaned-restore rollback only for an absent or
    Failed provision. (ENG-512)
- docker-backend: a restore whose best-effort release-record write fails no longer
  strands the restored lease. `doRestore` deleted the retention record
  unconditionally after adopting the volumes, so if the release `Append` failed the
  lease was left with neither record — a later boot's orphan reaper (which keys on
  the release record) could then destroy the still-live tenant data. The retention
  record now acts as the adopted volume's finalizer: it is dropped only once the
  release is durably recorded, and otherwise left `restoring` so the boot reaper
  keeps protecting the volume and `reconcileRestoring` finalizes it once the lease
  reaches Ready. Safe now that `reconcileRestoring` never tears down a running
  lease (ENG-512). (ENG-523)
- scheduler: the withdraw scheduler no longer panics or wedges on a very large
  credit balance. `estimateDepletionTime` converted an unbounded balance /
  burn-rate ratio via `LegacyDec.TruncateInt64()` (panics on int64 overflow)
  and a `time.Duration` multiply (overflowed to a bogus past time); a tenant
  with a large balance and a tiny per-interval burn could trip it. Because the
  estimate ran inside a mutex held without `defer`, a recovered panic left the
  lock held and silently wedged revenue withdrawal and credit-exhaustion
  lease-closing. The horizon is now clamped to a finite bound, and the critical
  section releases its lock via `defer`. (ENG-500)
- provisioner: the reconciler no longer terminates a lease on a transient
  backend circuit-breaker trip. `ErrCircuitOpen` was bucketed with permanent
  errors, so a brief backend outage that opened the breaker permanently
  rejected pending leases and closed active ones on-chain — including
  recoverable leases that would have succeeded on the next tick, and letting
  one tenant's load-induced breaker trip terminate other tenants' leases in the
  same cycle. It is now treated as transient: logged and retried next cycle.
  (ENG-498)

### Security

- docker-backend: bound container log reads to 5 MiB per container. The Docker
  `Tail` option limits lines, not bytes, and a container's stdout/stderr is
  tenant-controlled, so `GET /v1/leases/{uuid}/logs` (and the diagnostics
  capture path) previously buffered an unbounded amount of tenant output in
  memory — a tenant emitting very large output could OOM the provider and take
  down co-located tenants. The demuxed output is now capped with a truncation
  marker. (ENG-499)
- docker-backend: reject tenant-supplied container labels under the reserved
  `traefik.*` prefix during manifest validation (previously only `fred.*` was
  blocked). With ingress enabled, Traefik's Docker provider merges router labels
  from every container into one shared routing table, so a tenant could register
  a `traefik.http.routers.*` rule targeting another tenant's deterministic
  subdomain and intercept its HTTPS traffic. (ENG-497)
- docker-backend: reject tar entries whose declared size would overflow the
  extraction byte budget. `sanitizeAndExtractTar` gated its per-extraction size
  cap with `totalBytes + hdr.Size > maxBytes`, where `hdr.Size` is an
  attacker-controlled `int64` from a tenant image's tar stream. Once an earlier
  entry advanced `totalBytes` above zero, a later entry declaring a
  near-`math.MaxInt64` size wrapped the sum negative and slipped past the gate,
  letting a single entry stream unbounded bytes to host disk and defeat the cap
  — a disk-exhaustion DoS affecting co-located tenants on a shared node. The
  gate now compares against the remaining budget and rejects negative sizes.
  (ENG-520)

## [0.8.0] - 2026-07-09

### Added

- docker-backend: end-to-end integration test proving `Restore()` re-applies the
  correct XFS disk quota (`bhard`) on a SKU promote/demote and refuses an
  over-cap demote — closes the restore→quota-reapply routing gap. (ENG-438)

### Changed

- Provider-wide withdrawal now pages through **all** of a provider's active
  leases within a single cycle, threading the chain's opaque `next_key`
  pagination cursor until it is empty (bounded by `max_withdraw_iterations`).
  Previously fred issued one default-limit withdrawal per tick and discarded
  `has_more`, so a provider with more active leases than the per-transaction
  limit had its tail leases left unsettled until lease close — deferred, uneven,
  and silent revenue collection. A new metric
  `fred_withdraw_incomplete_cycles_total` flags a cycle that hit the iteration
  bound with leases still pending. The provider-withdrawable pre-check query is
  likewise paged and summed across all active leases. Requires manifest-ledger
  v2.3.0 or later, which ships the paginated provider-wide withdrawal — this is a
  coordinated, consensus-breaking chain upgrade. (ENG-475)

### Deprecated

### Removed

### Fixed

### Security

- Release binaries are now built with Go 1.26.5, incorporating the standard-
  library fixes for the `crypto/tls` Encrypted Client Hello privacy leak
  (GO-2026-5856) and the `os` symlink-with-trailing-slash root escape
  (GO-2026-4970). (ENG-415)

## [0.7.0] - 2026-07-03

### Added

- Restore can now target a different SKU tier (promote/demote). A demote is
  refused (HTTP 422) when the retained volume's measured data does not fit the
  new tier's disk cap. New metric `restore_demote_refused_total{backend,reason}`.
  (ENG-438)
- docker-backend: added a regression guard for the retained-disk admission
  accounting invariant — the cached pool projection (`SetRetainedDisk`) must
  equal the value recomputed from the retention store (active + reaping) after
  every self-refreshing retention transition (close, reap, evict, sweep,
  reconcile-restoring, restore rollback, recover). A future transition that
  mutates the retention store but skips `refreshRetentionAccounting` — drifting
  the projection stale-low → over-admit → tenant ENOSPC — now fails a test.
  (ENG-456)

### Changed

### Deprecated

### Removed

### Fixed

- docker-backend (xfs): destroying a volume now clears its XFS project-quota
  limit (`limit -p bhard=0 bsoft=0`) instead of leaving it set, fixing a leak
  where every closed lease left a stale project-quota entry behind. Because every
  `xfs_quota` operation scans the whole project-quota table, the accumulating
  entries degraded provisioning latency cumulatively (and persisted across
  restarts with no self-healing). Introduced once ENG-449/454 made per-volume
  quotas actually apply. The clear is best-effort — a failure is logged and
  counted on the new `volume_quota_clear_failed_total` counter but never fails the
  teardown (a stranded lease is worse than a leaked, observable, zero-byte
  entry). Existing already-leaked entries on long-lived backends need a one-time
  operator cleanup (`limit -p bhard=0` per stale projid); the fix stops all
  further accumulation. (ENG-459)

- docker-backend and k3s-backend: the `GET /retentions` list is now keyset-
  paginated end to end, closing the second un-paginated O(N) cliff left by
  ENG-380. Previously the whole retained-lease list was returned in one response
  capped at 1 MiB (~19.4k records/backend); on overflow the reconciler silently
  marked retentions incomplete, which disabled placement-orphan pruning and
  degraded restore backend-affinity (ENG-333) with no operator-visible error.
  The client now walks pages (complete-or-error, each page bounded), and the
  docker retention store serves each page directly from its ordered bbolt index
  via a cursor seek — O(limit) per request, not a full-bucket scan per page. The
  `/provisions` and `/retentions` handlers now paginate symmetrically at the
  backend. Page size is configurable via `RetentionsPageLimit` (default 1000).
  (ENG-451)

- docker-backend: the daemon now **fails fast at startup** when it lacks
  `CAP_SYS_ADMIN` on a backend that requires it to set quotas (xfs, btrfs),
  instead of rejecting every stateful provision at runtime with an opaque
  `xfs_quota: cannot set limits: Operation not permitted`. The privileged
  quota-set (`quotactl`) needs `CAP_SYS_ADMIN`, but the read-only `report` probe
  in `Validate()` did not — so an under-privileged daemon silently failed to
  enforce per-volume `disk_mb` caps (invisible until the ENG-449 mountpoint fix
  made the code actually reach the privileged call). zfs is exempt (it supports
  `zfs allow` delegation); the noop backend (no stateful SKUs) is unaffected.
  Grant it via `AmbientCapabilities=CAP_SYS_ADMIN` on the docker-backend systemd
  unit (see DEPLOYMENT.md). (ENG-454)
- docker-backend: added a startup **quota backfill** — existing volumes (active
  leases and active retained volumes) have their project quota re-applied
  (re-tag + limit) via the new `EnsureQuota` primitive, so leases provisioned
  before the daemon held `CAP_SYS_ADMIN` get their `disk_mb` enforced without a
  re-provision or data move. Best-effort and idempotent; it never creates a
  volume (a concurrent deprovision cannot be resurrected). New metric
  `volume_quota_backfill_total{outcome}`. (ENG-454)
- docker-backend: `/health` now probes the persistence stores (callback,
  diagnostics, release, retention bbolt) in addition to pinging the Docker
  daemon. Previously a locked/corrupt/read-only store left the backend reporting
  healthy while soft-delete/restore silently failed against it. (ENG-448 / F31)

- docker-backend and k3s-backend: the inbound request-body cap is now
  configurable (`max_request_body_size` /
  `{DOCKER,K3S}_BACKEND_MAX_REQUEST_BODY_SIZE`) and defaults to 2 MiB — larger
  than providerd's 1 MiB cap. Previously both backends hardcoded a 1 MiB cap
  equal to providerd's, so a tenant manifest that just cleared providerd could
  be rejected with an opaque 400 at the backend once providerd re-serialized and
  wrapped it for forwarding. (ENG-448 / F42)

- docker-backend (xfs): per-volume XFS project quotas are now actually applied
  and measurable when `volume_data_path` is a subdirectory of the XFS mount (the
  normal deployment layout, e.g. `/data/fred/volumes` under a `/data/fred`
  mount). `xfs_quota` requires the mount point as its trailing filesystem
  argument, but the manager passed the `volume_data_path` subdirectory, so
  `project -s`/`limit -p` silently no-op'd (the `disk_mb` cap was never
  enforced) and `report -p` failed — `Usage()` returned "project id not found",
  which made the ENG-438 restore demote-fit gate refuse every demote as
  `unmeasurable_read_error`. The containing mount point is now resolved once at
  construction (`resolveMountpoint`) and used for all `xfs_quota` invocations;
  the volume subdirectory is named only in `project -s -p`. btrfs and zfs were
  unaffected. Note: volumes provisioned before this fix stay untagged until
  re-provisioned or re-tagged out of band. (ENG-449)
- docker-backend: the `releases.db` age reaper (`RemoveOlderThan`,
  `releases_max_age` default 90d) no longer deletes a live long-lived lease's
  only manifest-rehydration source. It now retains each lease's most-recent
  active release and its newest entry, pruning only older history, so a lease
  running stably for ≥90d survives a backend restart and stays Restartable
  (previously its record was reaped → `recoverState` left `StackManifest` nil →
  `Restart`/custom-domain reconcile hard-failed). `Append` now derives the next
  release version from `max(existing)+1` so within-key pruning cannot reuse a
  version. (ENG-440)
- **Close-time purge of a containerless lease's release history.** When a deprovision
  reaches `doDeprovision` for a lease with no live container / in-flight op (the
  idempotent no-provision short-circuit), it now still deletes the lease's `releases.db`
  history before returning, instead of short-circuiting before the terminal
  `releaseStore.Delete`. This stops a `lease_closed` event delivered after the container
  was already gone from stranding a stale `status=active` record (an `audit-lease-status`
  false positive) until the 90-day `RemoveOlderThan` TTL. Best-effort and chain-driven;
  the three release-history deletes in `doDeprovision` are consolidated behind one helper.
  Purely cosmetic (no pool/admission/routing impact). (ENG-410)

### Security

- fred-api: the request metric `path` label is now the matched route template
  (e.g. `/v1/leases/{lease_uuid}/status`) rather than the raw URL path, with an
  `unmatched` bucket for unrouted requests. Previously an unauthenticated 404
  path scan minted a new Prometheus series per path (cardinality DoS); the label
  is now bounded to the finite registered-route set. (ENG-448 / F28)

## [0.6.0] - 2026-06-30

### Added

- `fred_docker_backend_restore_total{outcome}` metric: restore re-deploy worker
  outcome counter (`success`/`failure`), incremented on both the success and the
  rollback (`rollbackRestoreAdoption`, panics included) paths so a
  docker-backend-side restore success rate is computable —
  `fred_docker_backend_restore_duration_seconds` is success-only. Both series are
  pre-initialized to `0` so the success-rate query reads `0`, not no-data, before
  the first restore. (ENG-408)
- **Per-tenant retention eviction metric.** New counter
  `fred_docker_backend_retention_evicted_total` increments each time a close-time
  per-tenant cap eviction evicts one of a tenant's own oldest retained leases
  (active→reaping) to honor `max_retained_leases_per_tenant`. Distinct from the global
  `fred_docker_backend_retention_refused_total` (`max_retained_disk_mb`); operators
  can now alert on per-tenant grace loss instead of grepping logs. (ENG-407)
- **Signed releases + SBOMs.** Release artifacts are now signed with keyless
  cosign (Sigstore Fulcio/Rekor — no long-lived keys) and ship a syft SPDX SBOM
  per archive. The signature covers `checksums.txt` (which lists every archive and
  SBOM digest) and the published container image is signed by digest. Verify
  published artifacts with `cosign verify-blob` / `cosign verify` (full commands
  in `.goreleaser.yaml`). (ENG-415)

### Changed

- **Per-transaction gas simulation.** Chain transactions now estimate gas by
  simulating each tx (`gas_adjustment` × simulated `GasUsed`) instead of a fixed
  per-operation value. `gas_limit` is now only the fallback ceiling used when
  `Simulate` is unavailable, and a new `max_gas_limit` (`0` = uncapped) rejects any
  tx whose simulated estimate exceeds it. New config knobs: `gas_adjustment` and
  `max_gas_limit`. (ENG-431)
- Build: Go toolchain bumped to **1.26.4**; building from source now requires
  Go ≥ 1.26.4. (#148)

### Deprecated

### Removed

### Fixed

- **Restore now persists a release record for the new lease.** A successful restore
  brought the adopted stack up but never wrote a `releases.db` record (it set the
  manifest in memory only and deleted the retained record). After a docker-backend
  restart, `recoverState` rehydrates a lease's manifest solely from
  `releaseStore.LatestActive`, so a restored lease came back with a nil manifest and
  `Restart` hard-failed with `ErrInvalidState` ("no stored manifest"). The restore
  success path now appends an `active` release (the retained `StackManifest`
  re-serialized via the same `json.Marshal` → `ParsePayload` round-trip used by
  restart/update), so restored leases survive a restart and stay restartable. The
  fix is best-effort like the provision path — a release-write failure cannot undo
  the already-succeeded restore. (ENG-433)

### Security

- **Tar extraction hardened against symlink-follow TOCTOU.** Tenant image-content
  extraction now uses `os.Root` so path resolution is structurally confined to the
  destination directory, eliminating the symlink-then-write traversal class instead
  of relying on lexical path checks. (ENG-430)
- **Supply-chain / CI hardening.** The `govulncheck` gate no longer blanket-
  suppresses findings: fixable CVEs (`x/crypto`, `x/net`, `spdystream`, and Go
  stdlib via the 1.26.4 bump) were cleared, with only documented unfixable IDs
  allowlisted. Added `gosec` static analysis and `gitleaks` secret scanning to CI,
  and signed releases with SBOMs (see Added). (ENG-415)

## [0.5.0] - 2026-06-26

This release ships **lease-close soft-delete + restore**: a closed or
credit-expired lease's stateful volumes can be held for a grace window and
restored into a fresh lease, instead of being destroyed on close.

> **Upgrade notes (v0.4.0 → v0.5.0)**
>
> - **Restore is opt-in.** The retention/restore data path is gated behind the
>   docker-backend `retain_on_close` key, which defaults to `false`. On a stock
>   deployment nothing is retained and every restore returns `404` — set
>   `retain_on_close: true` (and review `retention_max_age` / `max_retained_disk_mb`)
>   to activate the headline feature.
> - **Disk accounting.** When `retain_on_close` is enabled, retained (soft-deleted)
>   volumes now count against the disk admission pool until reaped — re-check
>   `total_disk_mb` headroom. See the OPERATIONS.md "Reclaiming retained volumes
>   under disk pressure" runbook. (ENG-360)
> - **Writable-path-only volumes are reclaimed at close**, not retained (ENG-406);
>   mixed leases keep their stateful volumes restorable.
> - **Provision placement is now least-loaded (CPU)** with round-robin fallback,
>   using a new optional backend `GET /stats` endpoint (ENG-318). Mixed-version
>   fleets degrade gracefully — a backend without `/stats` is simply excluded.
> - **No new deploy-ordering constraint vs v0.4.0** — the ENG-191 HMAC / ENG-103
>   mTLS wire changes already shipped in 0.4.0 and are not repeated here.

### Added

- **Lease-close soft-delete + restore (headline).** When the docker-backend
  `retain_on_close` key is enabled, a closed lease's managed volumes are
  soft-deleted (renamed into the `fred-retained-` namespace and recorded in a
  bbolt retention store) and held for a grace window instead of being destroyed.
  They can then be restored into a fresh `PENDING` lease via
  `POST /v1/leases/{new_lease_uuid}/restore` with `from_lease_uuid` naming the
  source lease. Restore is same-tenant only (the caller must own both leases),
  pinned to the backend that holds the source data, and must shape-match the
  original service names/quantities. New docker-backend config keys:
  `retain_on_close` (default `false`), `retention_db_path` (default
  `retention.db`), `retention_max_age` (grace window, default 90 days; `0`
  disables reaping), `retention_reap_interval` (reaper cadence, default `1h`),
  and `max_retained_leases_per_tenant` (per-tenant cap, default `0` = unlimited).
  (ENG-325, #114)
- **Queryable retention status.** `GET /v1/leases/{uuid}/status` now reports
  `provision_status: retained` for soft-deleted leases, with
  `retained_until` (RFC3339 grace deadline), `items` (the restore shape: service
  name / SKU / quantity), and a `restore_hint`. The status is served from the
  durable retention record even after the chain prunes the closed lease, under
  strict per-tenant authorization (a cross-tenant query returns `404`). The
  deprovision callback now carries a ground-truth `Retained` flag so `providerd`
  reports `retained` only when volumes were actually held. (ENG-329, #118)
- **Restore latency instrumentation.** New
  `fred_docker_backend_restore_duration_seconds` histogram and
  `fred_docker_backend_replace_phase_duration_seconds{operation,phase}` for the
  shared replace machinery (restart/update/restore). The provisioner's
  `fred_provisioner_provisioning_total` and
  `fred_provisioner_provisioning_duration_seconds` gain an `operation` label
  (`provision`|`restore`) so restore latency stays separable from fresh-provision
  latency. (ENG-357 #123, ENG-358 #124)
- `max_retained_disk_mb` docker-backend config key: per-provider cap on the
  retained-volume tier, with refuse-to-retain on breach (ENG-360, #131).
- Retained-volume metrics: `fred_docker_backend_retained_volume_bytes`,
  `fred_docker_backend_retained_leases`, `fred_docker_backend_retention_refused_total`,
  and `fred_docker_backend_disk_pool_bytes` / `..._retained_disk_cap_bytes`
  denominator gauges (ENG-360, #131).
- `fred_docker_backend_retention_index_reindex_total{trigger}` metric: retention
  in-memory index (re)build count, by trigger (`open`|`manual`) (ENG-385, #137).

### Changed

- **Backend selection is now least-loaded (lowest allocated-CPU ratio) instead
  of round-robin.** When several backends match an SKU, a provision goes to the
  one with the lowest allocated-CPU ratio, queried live from each backend's new
  `GET /stats` endpoint; backends without usable stats are excluded and routing
  falls back to round-robin. Restore requests bypass this and are pinned to the
  backend holding the source lease's retained data (restore backend affinity).
  (ENG-318 #117, ENG-333 #120)
- **Restored leases are acknowledged on-chain inline**, like fresh provisions,
  instead of waiting for the next reconciler pass — cutting restore-to-`ACTIVE`
  time. (ENG-358, #124)
- **Retention list paths use an in-memory tenant/status index** (rebuilt on
  open) for O(subset) lookups instead of full-store scans. (ENG-385, #137)
- **`GET /provisions` (backend) is now paginated** via keyset pagination,
  removing the ~8 MiB response cliff that aborted reconcile on providers with
  many leases; reconciler consumers page through the results. (ENG-380, #136)
- **Writable-path-only volumes are reclaimed (destroyed) at close** instead of
  retained. A volume created solely from writable-path auto-detection (no
  declared `VOLUME`) is unrestorable by construction — its contents are wiped and
  reseeded on every restore — so close now destroys it, leaving no retention
  record, per-tenant slot, `fred-retained-` directory, or retained-disk charge.
  Mixed leases (stateful + writable-path-only) keep their stateful volumes
  restorable. Counted by the new
  `fred_docker_backend_retention_writable_path_reclaimed_total` metric.
  (ENG-406, #138)
- **Behavior change:** Retained (soft-deleted) volumes now count against the
  disk admission pool until they are reaped, closing a disk over-commit / ENOSPC
  risk. **Operators:** if `retain_on_close` is enabled, re-check `total_disk_mb`
  headroom — effective live capacity now decreases by the retained footprint. See
  the OPERATIONS.md "Reclaiming retained volumes under disk pressure" runbook.
  (ENG-360, #131)

### Fixed

- **Retained-disk under-count on broken-store abandon.** A reaping-tombstone
  finalizer (the retention record now outlives the volume until its destruction
  is confirmed) plus make-before-break rollback close the disk-accounting
  under-count across the reap / evict / deprovision-give-up / rollback paths;
  give-up leaks now self-heal. New `fred_docker_backend_retention_leaked_total`
  counter and `fred_docker_backend_retention_reaping_bytes` /
  `fred_docker_backend_retention_reaping_leases` gauges for the reaping tier.
  (ENG-376, #135)
- **Orphaned retention records are pruned** when their volumes vanish
  out-of-band, via a periodic fail-closed reconcile that requires N consecutive
  confirming sweeps before pruning. New `retention_orphan_confirmations`
  docker-backend config key (sweep count, default `3`; `0` is a kill-switch) and
  `fred_docker_backend_retention_orphans_pruned_total` /
  `fred_docker_backend_retention_orphan_skips_total{reason}` metrics.
  (ENG-370, #133)
- **Anonymous Docker volumes are reaped on every teardown** (compose-down and
  container / image-introspection cleanup), stopping a leak that taxed
  compose-up under load. Safe by construction — fred data lives in bind mounts /
  managed directories. (ENG-372, #132)
- **Reconciler placement-prune grace window + deprovision fail-safe.** Replaces
  the SKU-route deprovision fallback (which could deprovision a phantom default
  backend and strand retained volumes) with positive single-backend resolution
  or an all-backends idempotent fallback, and adds a grace window to the
  placement pruner so leases that provisioned during a slow sweep are not pruned.
  (ENG-335, #121)
- **`TimeoutChecker` untracks non-pending leases** instead of retrying a reject
  forever. A lost re-provision callback for an already-`ACTIVE` lease used to
  wedge the lease in-flight (the chain rejects `RejectLeases` for non-`PENDING`
  leases), permanently inflating in-flight counts and skewing capacity/routing;
  terminal reject errors now untrack and hand back to the reconciler.
  (ENG-337, #122)

### Security

- **`callback_insecure_skip_verify` is now gated behind `production_mode`** on
  the docker and k3s backends: when `production_mode: true`, the insecure
  TLS-skip toggle is rejected at startup, closing the backend → Fred callback
  MITM exposure (mirrors the existing `providerd`-side gate). New
  `production_mode` config key (default `false`). (ENG-321, #113)

## [0.4.0] - 2026-06-10

### Security

- **TLS/mTLS for the providerd ↔ docker-backend transport.** The control-plane link between `providerd` and the Docker backend can now be secured with TLS, including mutual TLS with client-identity pinning. (ENG-103, #107)
- **HMAC signatures are now bound to the HTTP method and request URI.** The signing envelope changed from `<timestamp>.<body>` to a four-field canonical string `<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>`, closing a cross-endpoint replay class (e.g. a `POST /provision` body replayed against `POST /deprovision`, or `GET /info/<uuid>` replayed as `GET /logs/<uuid>`). This is a wire-format change with no dual-verify window: old signatures are hard-rejected by the new verifier (fail-closed with `401`). **Deploy order matters** — bring backends down, then Fred, deploy all binaries, bring Fred up first, then the backends. (ENG-191, #88)

### Added

- **TLS configuration knobs for the providerd ↔ docker-backend transport** to enable TLS/mTLS on the control-plane link (see ENG-103 above). (ENG-103, #107)
- **`callback_canonical_path_prefix` configuration field.** Restores the backend → Fred callback leg behind a path-stripping reverse proxy (e.g. Traefik `stripPrefix`) after the ENG-191 URI binding. The verifier prepends this static prefix to `r.URL.RequestURI()` before HMAC verification; the empty default preserves existing direct-call behavior bit-identically. (ENG-198, #91)
- **k3s-backend scaffold (experimental, non-functional).** A `cmd/k3s-backend` binary with full HTTP-contract parity with docker-backend, HMAC-signed inbound + outbound callbacks, a bbolt-persisted callback queue, and a `/health` probe that round-trips against the configured K3s cluster. The provisioner is a stub that returns `202` and posts a `status=failed, error="not implemented"` callback; substantive workload logic is deferred to child issues. Built from source only — **not included in release artifacts** (excluded from goreleaser) until real provisioning lands (ENG-134+). (ENG-133, #86)
- **`scripts/deploy-app.go`** — a focused script that takes `--image` + `--sku` and produces one lease plus an uploaded manifest, printing the lease UUID to stdout. (#92)
- **Apache 2.0 LICENSE** added to the repo root (`Copyright 2026 The Manifest Network`), replacing the README license placeholder. (ENG-176, #96)
- **Recover-time legacy → stack migration.** On Fred startup, the planner scans managed containers and migrates any legacy-named ones to the new service-aware naming convention. Migration is per-lease atomic (stop → rename containers to `-prev` → rename volume directories → `compose up` → verify startup → schedule background `-prev` cleanup → record migration in release store). Required: an active entry in the release store for the lease — see *Troubleshooting* below.
- **`volumeManager.RenameVolume(oldName, newName) error`** on all per-filesystem volume managers (xfs, btrfs, zfs, noop). The xfs implementation preserves project IDs across rename; the btrfs implementation preserves subvolume IDs (metadata-only rename, not copy+delete — verified by `TestIntegration_Docker_BtrfsRenameVolume_PreservesSubvolID`); the zfs implementation uses `zfs rename`.
- **`(*ReleaseStore).RecordMigration(leaseUUID, manifest) error`** for the migration pipeline to durably mark a legacy lease as having been migrated to its wrapped stack form. Idempotent on byte-equal payload.
- **`ProvisionState.StackManifest`** and **`ProvisionState.ServiceContainers`** fields persisted in the lease-state store. These replace the legacy `ProvisionState.Manifest` and the legacy semantic of `ProvisionState.ContainerIDs` indexed by `InstanceIndex` alone.
- **Configuration**: `MigrationReadyTimeout` (per-lease compose-up + verifyStartup bound) and `MigrationGracePeriod` (background `-prev` container retention window for rollback inspection). Reasonable defaults are set.
- **`fred_signer_balance` gauge** — per-signer balance in the configured fee denom, emitted as a `float64` (so large balances cannot be silently dropped — see ENG-252 under *Fixed*), sampled on each `/metrics` scrape via a custom Prometheus collector. Labels: `role` (`provider`|`sub_signer`), bech32 `address`, slice-position `index` (empty for the provider), and `denom` (the bank denom queried, sourced from `cfg.FeeDenom` — defaults to `umfx`). Sampling runs per-address bank queries in parallel under a 5s per-scrape timeout; per-address failures drop only the failing series and increment the new `fred_signer_balance_query_failures_total` counter (labeled by `role`, `address`, `denom` — no `index`). Single-signer mode naturally emits only the provider series, and `DemoteToSingleSigner` is reflected on the next scrape (the collector reads the live pool, no cached state). (ENG-239)
- **`docker-backend --version`** flag prints the build-injected version and exits, matching `providerd --version`. Operators can now query the docker-backend binary's version without a valid config file present (previously the version was only observable in the startup log, which requires a successful boot). (ENG-254)
- **`k3s-backend --version`** flag prints the build-injected version and exits, matching `providerd` and `docker-backend`. Same rationale and behavior as ENG-254 — the version is now queryable without a valid config file present (previously only observable in the startup log, which requires a successful boot). (ENG-255)

### Changed

- **Unified manifest handling on the Compose path.** All provisions, restarts, and updates now flow through Docker Compose end-to-end. The legacy single-service execution path that drove the Docker Engine API directly is removed. Tenants see no API-level behavioural change for new leases. (Plan: `docs/superpowers/plans/2026-05-15-unify-manifest-on-compose.md`)
- **Container naming is now service-aware.** Container names follow the pattern `fred-{lease}-{service}-{idx}` (previously `fred-{lease}-{idx}` for single-service leases). For a 1-service lease with a flat manifest, the service is named `app`, so legacy lease containers under the new code become `fred-{lease}-app-{idx}`. **Breaking change for monitoring tools or scripts that pattern-match container names** — update regexes from `fred-{lease}-\d+` to `fred-{lease}-[a-z][a-z0-9-]*-\d+`.
- **Volume directories follow the same service-aware naming**: `fred-{lease}-{service}-{idx}` under the configured `volume_data_path`. Pre-existing single-service volumes are renamed in-place at startup as part of the auto-migration described below.
- **Compose projects** group every container belonging to a lease under the project name `fred-{lease}`. Operators can inspect a lease's containers with `docker compose -p fred-{lease} ps` and tail logs with `docker compose -p fred-{lease} logs -f`.
- **Restart/Update `provision.Status` is now written exclusively by the lease actor** (ENG-230). The Docker backend's `Restart`/`Update` prelude no longer speculatively flips `Status`/`CallbackURL` outside the actor (and the speculative-write rollback machinery is gone); the actor's state-machine entry actions are the sole writers, firing before the request is acknowledged. **Externally observed behavior is unchanged** — including the HTTP contract: a same-lease concurrent restart/update that loses the race is still rejected with `409 Conflict` (now enforced by the actor rather than a prelude mutex). The internal `fred_docker_backend_lease_failing_race_skipped_total` metric, which only existed to detect the now-closed seam, is removed. (Plan: `docs/superpowers/plans/2026-05-26-eng-230-restart-update-actor-only-write.md`)
- **Custom-domain reconcile resolves only candidate domains** (performance). The reconcile no longer resolves every custom domain on each pass — it resolves only the candidates that could have changed. No behavior change. (ENG-277, #104)
- **Shared backend code lifted from `internal/backend/docker` → `internal/backend/shared`** (internal refactor). The reference-counted work barrier (`workbarrier`) and the tenant-facing manifest parser/validator (`shared/manifest`) moved out of the docker package so the upcoming K3s backend can reuse them. Pure mechanical lift, no behavior change. (#85)
- **Bootstrap provision writes go through a type-enforced actor-sole-writer boundary** (internal hardening). The two bootstrap provision-write paths (recover + provision) now flow through a `recoveredProvision` value type and a `materialize()` typed constructor as the only path into the provision registry. Behavior-preserving structural hardening; field-completeness is guarded by the directive-only `exhaustruct` linter. (ENG-193, #106)
- **`doDeprovision` writes routed through the lease-actor store seam** (internal hardening). Direct `ProvisionState` writes in `doDeprovision` now go through the store seam (`LeaseProvisionStore.Delete` added; initial-mark / partial-failure migrated to `UpdateFn`, success-delete to `store.Delete`). Behavior-preserving. (ENG-232, #105)
- **Deprovision volume-retry writes routed through the store seam** (internal hardening). The volume-retry block in `doDeprovision` — the last live-state writer mutating the provision directly — is split into a short direct span for the docker-private `VolumeCleanupAttempts` counter and `UpdateFn` for the `ProvisionState` writes. Pure structural hardening, no behavior change; covered by a concurrent recoverState/deprovision `-race` test. (ENG-285, #108)

### Deprecated

- **Flat single-service manifest format** (`{"image": "...", "ports": ...}` without a top-level `services` key). The format remains accepted at the wire boundary and is silently auto-wrapped into a 1-service stack manifest under the synthetic service name `app`. Tenants are encouraged to migrate to the explicit stack manifest format (`{"services": {"app": {"image": "...", ...}}}`). The flat format will be removed in a future major release. See [docs/manifest-guide.md](docs/manifest-guide.md) and the deprecation note at the top of [docs/manifest-schema.json](docs/manifest-schema.json).

### Removed

- **`backend.IsStack([]LeaseItem) (bool, error)`** and the associated `ProvisionState.IsStack()` method. Every provision is now stack-shaped by construction; the query had no meaningful counter-case.
- **`ProvisionState.Manifest`** (legacy single-service pointer) and **`ProvisionState.Image`** fields. The canonical workload representation is `Items` + `ServiceImages` (derived from `StackManifest`).
- **`ProvisionSuccessResult.Manifest`** — `StackManifest` is the only surviving manifest field.
- **Legacy single-service `doProvision` / `doRestart` / `doUpdate` / `doReplaceContainers` / `setupVolumeBinds`** function bodies (Task 14). The stack variants, now de-suffixed (`doProvision`, `doRestart`, `doUpdate`, `doReplaceContainers`, `setupVolBinds`), are the only execution paths.

### Fixed

- **Custom-domain HTTP-01 cert issuance is gated on DNS readiness.** Issuance for a custom domain no longer fires before the domain's DNS resolves, avoiding failed/repeated HTTP-01 challenges. (ENG-266, #101)
- **Custom-domain reconcile matches on a normalized `service_name`.** A single-image lease created without `-service-name` carries `service_name=""` on chain, but provision tags the lone item `app`, so an exact-`ServiceName` match (`"app"` vs `""`) silently dropped a `custom_domain` set after deploy (no `-custom` router, no HTTP-01 cert). Both sides are now normalized before matching so a lone unnamed item canonicalizes to `app` on chain and container alike; the rollback is keyed on the item's actual `ServiceName` so legacy `""`-named and multi-service leases are unchanged. (ENG-264, #100)
- **Custom-domain reconcile routed through the lease actor** (TOCTOU + data-race fix). The custom-domain apply + redeploy is serialized through the lease actor against `recoverState`'s struct-swap: the reconciler computes the diff read-only and routes a `ServiceName`-keyed override through the existing restart path; the actor commits `prov.Items` via the success-only `OnSuccess` hook. Deletes the off-actor mutate + CAS rollback. (ENG-278/ENG-231, #103)
- **k3s-backend: per-lease cancellable provisioner context.** Closes the post-unlock race between the stub provisioner's external writes (diagnostic + callback persist) and a concurrent `Deprovision`. Provision creates a per-lease `context.WithCancel`; `Deprovision` cancels it before deleting the map entry; the worker checks `ctx.Err()` at two checkpoints so a `Deprovision` that wins the lock observes cancellation before the next external write. Mirrors docker-backend's `OnExit` pattern. (ENG-189, #89)
- **`fred_signer_balance` gauge no longer drops large balances to an int64 overflow.** The gauge is now emitted via `float64` end-to-end, so balances above the int64 range are reported correctly instead of being silently dropped. (ENG-252, #97)

The change that removes the now-unused route-time `prevStatus` snapshot (replacing it with the lease actor's serially-observed pre-replace state) corrects three latent edges, all in rare races. **Two are `activeProvisions` metric-accuracy corrections (no lease-behavior change); one is a functional lease-behavior delta (the single restart-preflight edge below).** Normal restart/update flows are unaffected. (Plan §9, `docs/superpowers/plans/2026-05-26-eng-230-restart-update-actor-only-write.md`)

- **`activeProvisions` gauge accuracy — correction (a) (metric only):** in the container-death-then-restart-**success** ordering the gauge under-counted. A container death drove the lease `Ready→Failing` (decrementing the gauge); when the queued restart then succeeded, the re-increment was gated on the stale route-time snapshot and skipped, leaving the gauge one short until the next reconcile. It is now keyed on the actor-observed pre-replace state and stays correct.
- **`activeProvisions` gauge accuracy — correction (b) (metric only):** a replace that **recovers from a non-active source** (e.g. a restart-from-`Failed` whose replace fails but whose rollback restores the lease to `Ready`) is now counted. That path previously performed no gauge increment, under-counting the now-`Ready` lease.
- **Restart that fails preflight after a container death no longer reports the lease as `Ready` with dead containers (functional delta — the single lease-behavior change):** a **pre-existing** edge (architect determination): if a restart hit a preflight failure (e.g. an SKU profile removed from config between provision and restart) while the lease had just died and moved to `Failing`, the recovered-vs-failed decision keyed on a stale route-time snapshot and wrongly marked the lease "recovered to `Ready`" (also over-counting the gauge) for up to one reconcile interval. It now keys on the actor-observed source state, so such a lease correctly ends `Failed`. The on-chain callback was `failed` in both the old and new paths, so there was **never a wrong on-chain success** — only a transient wrong API status. This is **not a regression** and is bundled here with the `prevStatus` removal, not buried as an internal refactor.
- **docker-backend**: `DefaultConfig()` no longer pre-populates `SKUProfiles` with the four default tiers. yaml.v3 merges map keys during Unmarshal, so partial operator `sku_profiles:` blocks were silently inheriting defaults — and the bidirectional `sku_mapping`/`sku_profiles` reachability check then rejected the merged config, crash-looping docker-backend on deployments with fewer than four SKUs. `sku_profiles` is now required in YAML (with a clear `Validate` error when missing); see `internal/backend/docker/README.md` for recommended starter profiles. (ENG-238)

### Migration

#### What happens on upgrade

When a Fred instance running the new code starts up against a host that has leases provisioned by the legacy single-service code:

1. Fred's recoverState planner inspects all managed containers and groups legacy-named ones by lease.
2. For each legacy lease, Fred consults the release store. **If the release store has no active entry for the lease, Fred refuses to migrate it and fails startup with the lease UUID in the error message.** This is intentional — the planner refuses to reconstruct a manifest from container inspect alone (it cannot recover tmpfs paths, the resolved User UID, `depends_on` graphs, or `stop_grace_period`).
3. For each migratable lease, Fred runs the atomic pipeline: stop containers → rename to `<name>-prev` → rename volume directories → `compose up` with the wrapped stack manifest → verify startup → schedule background removal of `-prev` containers after `MigrationGracePeriod` → record the wrapped manifest in the release store.
4. Tenants see no API-level change. The lease's leases-state record gains a populated `StackManifest` + `ServiceContainers`.

#### Troubleshooting

##### "lease X has no active release store entry — migration refused"

Fred startup will abort with an error listing the lease UUID. Operator remediation:

- Inspect the release store for the lease (`./fredctl release list <lease-uuid>`).
- If the lease was created **before** the release-store feature was introduced, no automatic migration is possible. Two options:
  1. **Deprovision the lease** (`./fredctl lease deprovision <lease-uuid>`) and have the tenant re-provision. The new provision will be stack-shape from the start.
  2. **Manually add an active release-store entry** for the lease using the current manifest (recover from your provisioning records or the chain's on-chain manifest hash + payload archive).
- Re-start Fred. If the entry exists, migration proceeds.

##### Stuck `-prev` containers and the narrow non-resumable migration window

The migration pipeline has four boundaries; three are crash-resumable, one is not:

- **Boundary 1** (before `Stop + rename-to-prev`): crash-resumable. Next boot re-plans from scratch.
- **Boundary 2** (after `rename-to-prev`, before `compose up`): **NOT resumable.** Containers are stopped and renamed to `<name>-prev`; volumes may be partially renamed. The next boot's planner cannot find the legacy container under its original name (it's `-prev`) and refuses to replan.
  - **Operator remediation:** either (a) manually rename `<name>-prev` containers back to their original names and restart Fred (which will retry the migration cleanly), or (b) deprovision the lease and have the tenant re-provision.
- **Boundary 3** (after `compose up`, before `RecordMigration`): crash-resumable. New stack containers exist alongside `-prev` containers; the release store still has the legacy active entry; the volume rename is already idempotent. Next boot re-plans and reconverges; the operator may see two generations of containers transiently.
- **Boundary 4** (after `RecordMigration`, before background `-prev` removal completes): terminal state. Forward progress is durable. If Fred restarts inside the `MigrationGracePeriod` window, the `-prev` containers linger on disk until manual cleanup (`docker rm <container>`).

##### Orphaned `-prev` containers from pre-upgrade legacy restart attempts

If a pre-upgrade Fred (the legacy single-service code) was interrupted mid-Restart (between `RenameContainer` to `-prev` and the subsequent `RemoveContainer`), the host may carry orphaned `<lease-uuid>-prev` containers from that earlier interrupted Restart attempt. These are unrelated to the recover-time migration `-prev` containers and survive the upgrade. They are operator-cleanup territory:

```
docker ps -a --filter "name=-prev$"
docker rm <container-id>
```

The new code's lease lifecycle does not produce these orphans.

### Configuration

No required configuration changes. New optional knobs:

- `migration_ready_timeout`: per-lease compose-up + verifyStartup bound during the auto-migration. Default: 90 seconds.
- `migration_grace_period`: how long `-prev` legacy containers are retained for operator rollback inspection before background cleanup. Default: 1 minute.

### Acknowledgements

This migration was planned and executed across 16 tasks; the plan and per-task design discussions are in `docs/superpowers/plans/2026-05-15-unify-manifest-on-compose.md`.
