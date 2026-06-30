# Changelog

All notable changes to Fred are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Restore can now target a different SKU tier (promote/demote). A demote is
  refused (HTTP 422) when the retained volume's measured data does not fit the
  new tier's disk cap. New metric `restore_demote_refused_total{backend,reason}`.
  (ENG-438)

### Changed

### Deprecated

### Removed

### Fixed

### Security

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
