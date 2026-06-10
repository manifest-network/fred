# Changelog

All notable changes to Fred are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

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
