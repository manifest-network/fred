# Unify Manifest Handling on the Compose Path

**Status:** Approved
**Date:** 2026-05-15
**Owner:** felix@liftedinit.org

## Problem

The Docker backend maintains two parallel implementations for every lease lifecycle operation: a *legacy* path that drives the Docker Engine API directly (single-service manifests) and a *stack* path that drives Docker Compose (multi-service stack manifests). The split is gated by `backend.IsStack(items)`, which inspects whether lease items carry a `service_name`.

The Compose path is a strict functional superset: anything the legacy path does, Compose can do (a one-service project is a valid Compose project). The duplication imposes recurring costs:

- Roughly 1k+ LOC of parallel implementations across `provision.go`, `restart_update.go`, `deprovision.go`, `recover.go`, `info.go`, and `compose_project.go`.
- Two test suites (`integration_test.go`, `integration_volume_test.go`, etc.) each fork on `isStack`.
- Two volume-bind helpers (`setupVolumeBinds` vs. `setupStackVolBinds`), two allocation-ID schemes, two container-name schemes, two response shapes (`LeaseInfo.Instances` vs. `LeaseInfo.Services`).
- Every new feature has to be considered against both paths; regressions in one path are easy to miss.

## Goal

Collapse the two internal paths into one without breaking existing tenants or destroying their persistent data. After this change the docker backend has a single execution path (Compose) and the codebase contains *no* `isStack` branches. Tenant-visible behavior is preserved: flat manifests and lease items without `service_name` are still accepted on the wire, with deprecation notices.

## Non-goals

- Changing the Compose feature set (no new manifest fields).
- Changing the k3s backend.
- Removing the flat single-service wire format in this PR. (Deprecation is documented; removal deferred to a future major release — the "Sunset hard" plan.)
- Chain-side changes to lease item semantics.

## Scope decisions (locked after migration feedback)

1. **Wire format stays permissive in this PR.** Flat single-service manifests and lease items without `service_name` are still accepted. A deprecation log fires per-lease (rate-limited) when fred normalizes flat input.

2. **Boundary normalization, not internal branching.** Every payload entering fred is normalized to a `*StackManifest` at parse time. Every lease item entering fred is normalized to carry `service_name`. After this normalization step, *no internal code path branches on legacy vs stack* — `backend.IsStack` is deleted and every downstream component sees stack-shaped state only.

3. **Migration strategy: recover-time one-shot recreate.** On fred's first post-upgrade startup, each legacy container (managed container with `fred.lease_uuid` but no `fred.service_name` label) is recreated as a stack-form container in place. The existing volume directory is renamed via the filesystem-specific volume backend so bind paths line up under the new naming convention. Each lease incurs a brief restart during the startup window; no tenant action is required; persistent data is preserved.

## Architecture after the change

### Manifest model

`manifest.Manifest` (the current exported single-service type) is demoted to an unexported `flatManifest` used only inside the manifest package as the JSON-unmarshal target for legacy flat payloads. After parse it is immediately wrapped into a `*StackManifest` and discarded. The exported manifest API exposes `StackManifest` only.

`ParsePayload(data []byte) (*Manifest, *StackManifest, error)` is replaced by `ParsePayload(data []byte) (*StackManifest, error)`. The new contract:

- Stack-format input (`{"services": ...}`) is parsed directly.
- Flat-format input (no `services` key) is parsed into the internal `flatManifest`, wrapped as `{"services": {"app": <flat>}}`, and a deprecation warning is logged with the lease UUID.
- Empty payloads and unparseable JSON return an error.

Synthetic service name: `app`. Centralized as a constant `manifest.DefaultServiceName` so it can be referenced consistently and changed in one place if needed.

### Lease item model

`backend.LeaseItem.ServiceName` remains optional on the wire. A new helper `backend.NormalizeProvisionRequest(req *ProvisionRequest) error` runs at the entry point of Provision / Update:

- If every item has a `service_name`, no change.
- If no item has a `service_name` and there is exactly one item, the item is auto-tagged with `service_name = manifest.DefaultServiceName` ("app"). A deprecation log fires (rate-limited).
- Any other combination (mixed empty/non-empty across items, or multiple items with all empty) is rejected with `ErrInvalidManifest` — these would have been malformed under the legacy contract too.

`backend.IsStack` is **deleted** after all callers stop branching on it.

### Container provisioning

After normalization, all entry points always have a `*StackManifest`. `doProvision`/`doProvisionStack` collapse to a single `doProvision` (the stack body becomes canonical). Same for `doRestart`/`doRestartStack` and `doUpdate`/`doUpdateStack`. `doDeprovision` loses its `isStack` branch.

### Allocation IDs and naming

Allocation IDs and container/volume names are unified on the service-aware form:

| Resource | Format |
|---|---|
| Allocation ID | `{leaseUUID}-{serviceName}-{instanceIndex}` |
| Container name | `fred-{leaseUUID}-{serviceName}-{instanceIndex}` |
| Volume directory | `fred-{leaseUUID}-{serviceName}-{instanceIndex}` |
| Compose project | `fred-{leaseUUID}` |

For migrated legacy leases, `{serviceName}` becomes `app`. Old containers and volumes named `fred-{leaseUUID}-{instanceIndex}` are renamed during the recover-time migration (see below). After migration, no `fred-{uuid}-{idx}` artefacts exist on disk.

### Response shape (`LeaseInfo`)

Both `LeaseInfo.Instances` and `LeaseInfo.Services` continue to populate. `Services` is the primary source of truth (always present, keyed by service name). `Instances` is a flattened convenience view computed by concatenating service instances in deterministic service-name order, preserved for compatibility with existing tooling that reads it.

### Volume binds

`setupVolumeBinds` is deleted. `setupStackVolBinds` is renamed to `setupVolBinds` and becomes the sole entry point.

### Manifest validation

The "depends_on forbidden in single-service" rule disappears (structurally impossible — every input is a stack post-normalization). All stack validators run unconditionally: service-name DNS-label format, 1:1 mapping against (normalized) lease items, depends_on cycle detection.

### Recover-time migration

A new file `internal/backend/docker/migrate.go` owns the legacy → stack migration. On every call to `recoverState`, before the main recover loop, fred scans the managed-container list and dispatches each legacy container (label `fred.lease_uuid` present, `fred.service_name` absent or empty) through the migration pipeline:

1. **Inspect.** Read the existing container's image, env, ports, labels, mounts, health-check, and stop-grace-period from `ContainerInspect`.
2. **Reconstruct.** Read the persisted manifest from the release store; if missing, synthesize a minimal manifest from the inspected container (image + ports + env + healthcheck). Wrap as `{"services": {"app": <manifest>}}` and validate.
3. **Plan.** Compute new artefact names: container `fred-{uuid}-app-{idx}`, volume `fred-{uuid}-app-{idx}`. Note: `{idx}` equals the legacy container's `fred.instance_index` label.
4. **Rename volume directory.** Call `VolumeBackend.RenameVolume(oldName, newName)` for each volume bound to the legacy container. Skipped if the new path already exists and the old does not (idempotent under restart).
5. **Stop and rename old container.** Stop the legacy container with its `stop_grace_period`; rename to `fred-{uuid}-app-{idx}-prev` (mirrors the existing restart-update flow at `backend.go:616`).
6. **Build Compose project.** Use `buildComposeProject` with the wrapped manifest; the project's volume binds resolve to the just-renamed directories.
7. **Compose Up.** Call `composeService.Up`. The new container starts under the stack-style name.
8. **Wait for health.** If the manifest declares a health check, wait until the new container reports `healthy`; otherwise wait until `running`.
9. **Remove `-prev`.** After `migration_grace_period` (config-defaulted to `1m`), force-remove the `-prev` container. The grace period preserves rollback potential if the operator interrupts fred and inspects.
10. **Persist.** Write the wrapped manifest into the release store as a new release entry (idempotent if the same wrapped manifest already exists). Future recover sees the lease as stack-form and skips migration.

Failure handling: any step error aborts startup with a clear log:
```
legacy container migration FAILED: lease <uuid> container <id>: <error>
fred refuses to start with unmigrated legacy containers.
Remediation: investigate the failure cause; re-run fred (migration is idempotent), or
deprovision the lease manually if data loss is acceptable.
```

The fail-fast posture prevents half-migrated state.

### Volume rename per filesystem

`VolumeBackend` (currently in `volume.go`) gains a `RenameVolume(oldName, newName string) error` method, implemented per backend:

- **xfs (`volume_xfs.go`):** `os.Rename` of the volume root directory.
- **btrfs (`volume_btrfs.go`):** plain `os.Rename` on the subvolume root — btrfs supports rename on subvolumes.
- **zfs (`volume_zfs.go`):** `zfs rename <old-dataset> <new-dataset>` via shellout, mirroring the existing zfs commands in this file.

All three are idempotent: if old path doesn't exist and new path does, return nil. If both exist, return error (operator must intervene).

## Affected files

| File | Change |
|---|---|
| `internal/backend/shared/manifest/manifest.go` | Demote `Manifest`→`flatManifest` (unexported); rewrite `ParsePayload` to return `(*StackManifest, error)` with auto-wrap; add `DefaultServiceName` constant. |
| `internal/backend/client.go` | Add `NormalizeProvisionRequest`; delete `IsStack` after all callers gone. |
| `internal/backend/docker/provision.go` | Call `NormalizeProvisionRequest` + `ParsePayload`; delete legacy `doProvision`; rename `doProvisionStack`→`doProvision`. Delete `setupVolumeBinds`; rename `setupStackVolBinds`→`setupVolBinds`. |
| `internal/backend/docker/restart_update.go` | Same pattern: delete legacy `doRestart`, `doUpdate`; rename `doRestartStack`→`doRestart`, `doUpdateStack`→`doUpdate`. |
| `internal/backend/docker/deprovision.go` | Drop `if isStack` branches. |
| `internal/backend/docker/recover.go` | Pre-loop: dispatch legacy containers to migration. Main loop: drop legacy branch (all containers are stack-form after migration). |
| `internal/backend/docker/migrate.go` *(new)* | Owns the legacy→stack migration pipeline. |
| `internal/backend/docker/info.go` | Always populate `Services`; derive `Instances` flattened view. |
| `internal/backend/docker/lifecycle.go` | Only the stack form of container naming (line 1048-1050 simplified). |
| `internal/backend/docker/backend.go` | `prevContainerName` takes a `serviceName` parameter (line 618). |
| `internal/backend/docker/volume.go` | Add `RenameVolume(oldName, newName string) error` to `VolumeBackend` interface. |
| `internal/backend/docker/volume_xfs.go` | Implement `RenameVolume` via `os.Rename`. |
| `internal/backend/docker/volume_btrfs.go` | Implement `RenameVolume` via `os.Rename` (subvolume rename). |
| `internal/backend/docker/volume_zfs.go` | Implement `RenameVolume` via `zfs rename`. |
| `internal/backend/shared/leasesm/*.go` | Drop `IsStack` callers; `ProvisionState` always carries `StackManifest` + `ServiceContainers`. |
| `internal/api/handlers.go` | (No structural change — `NormalizeProvisionRequest` handles it at the backend boundary; the API just passes through.) |
| `docs/manifest-guide.md` | Add deprecation notice on the flat format; keep the section but mark legacy. |
| `docs/manifest-schema.json` | Keep the union for now (flat + stack); document deprecation. |
| `CHANGELOG.md` (new or existing) | Note the deprecation and the auto-migration behavior. |
| `internal/backend/docker/TENANT_MANIFEST.md` | No change. |

## Validation rules summary (post-change)

- Flat-format payloads still accepted; auto-wrapped to `{"services": {"app": ...}}` with a deprecation log.
- Stack-format payloads validated as today.
- Lease items without `service_name`: auto-tagged with `app` if there's exactly one item; otherwise rejected.
- All per-service validators (image registry, port format, env names, labels, tmpfs, user, health_check, stop_grace_period, init, expose) continue to apply per service.
- `depends_on` validation is stack-only — but every payload is a stack now, so the rule is just "the depended-on service must exist in this stack".

## Risks

1. **Recover-time migration is the riskiest piece.** Failure modes include missing release-store entry (mitigated by reconstruction from container inspect), volume rename failure on a specific filesystem (caught and reported), image no longer pullable (logged; lease aborts), Compose network conflict. Each is a fatal startup error with the offending lease identified, so operators never end up half-migrated.
2. **Volume rename atomicity per filesystem.** `os.Rename` is atomic within a filesystem on xfs and btrfs; ZFS `rename` is a metadata operation and atomic. A migration interrupted between rename and container-recreate leaves an orphan directory plus a stopped container; recover-time migration is idempotent and resumes on the next startup.
3. **Brief downtime per legacy lease during startup.** Stateful tenants experience a restart. Operators communicate the upgrade window via the existing release-notes channel.
4. **Container name change for previously single-service leases.** External monitoring tools keyed on `fred-{uuid}-{idx}` break. Release notes call this out.
5. **Boundary-normalization log spam.** Every flat-manifest deploy logs a deprecation. The log is rate-limited per lease UUID (e.g., once per process restart).
6. **Release-store reads of historical entries.** Old flat-format entries are parsed via the wrap path and continue to work. The release store is not migrated proactively; entries get rewritten the next time the lease is updated.

## Out of scope (potential follow-ups)

- The "Sunset hard" release that removes flat-manifest acceptance entirely.
- Deprecating `LeaseInfo.Instances` in favor of `Services` only.
- Migrating the k3s backend to the same model.
- Schema versioning for the manifest payload.
- Operator-facing tooling that pre-wraps legacy manifests for tenants.
