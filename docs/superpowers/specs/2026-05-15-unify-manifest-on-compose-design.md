# Unify Manifest Handling on the Compose Path

**Status:** Draft
**Date:** 2026-05-15
**Owner:** felix@liftedinit.org

## Problem

The Docker backend maintains two parallel implementations for every lease lifecycle operation: a *legacy* path that drives the Docker Engine API directly (single-service manifests) and a *stack* path that drives Docker Compose (multi-service stack manifests). The split is gated by `backend.IsStack(items)`, which inspects whether lease items carry a `service_name`.

The Compose path is a strict functional superset: anything the legacy path does, Compose can do (a one-service project is a valid Compose project). The duplication imposes recurring costs:

- Roughly 1k+ LOC of parallel implementations across `provision.go`, `restart_update.go`, `deprovision.go`, `recover.go`, `info.go`, and `compose_project.go`.
- Two test suites (`integration_test.go`, `integration_volume_test.go`, etc. each fork on `isStack`).
- Two volume-bind helpers (`setupVolumeBinds` vs. `setupStackVolBinds`), two allocation-ID schemes, two container-name schemes, two response shapes (`LeaseInfo.Instances` vs. `LeaseInfo.Services`).
- Every new feature has to be considered against both paths and a regression in one path is easy to miss.

## Goal

Collapse the two paths into one. After this change, **every** lease — including those that today would be "single service" — is internally a one-service Compose project. The wire format is also simplified: tenants always submit a stack-shaped manifest (`{"services": {...}}`) and chain lease items always carry `service_name`.

## Non-goals

- Changing the Compose feature set (no new manifest fields).
- Changing the k3s backend (out of scope; not part of this consolidation).
- Backwards-compatible runtime support for legacy-format leases. The migration model is operator drain + coordinated upgrade.

## Scope decisions (locked)

1. **Both internal and wire format** collapse. Two derived consequences:
   - **Wire (JSON) format:** the manifest API stops accepting flat single-service JSON. All callers must submit `{"services": {...}}`. The auto-detection in `manifest.ParsePayload` is removed.
   - **Chain item semantics:** lease items must always carry `service_name`. The on-chain enforcement is expected to be tightened from "all-or-nothing per lease" to "always required". *This is a chain-side change tracked separately;* the fred-side code is written assuming the chain has been upgraded and fails loudly if a request without `service_name` arrives.

2. **Migration strategy: hard cutover.** Operators are required to deprovision all active leases before upgrading. After upgrade, fred refuses to start if it discovers any legacy-format container (a container with the `fred.lease_uuid` label but no `fred.service_name` label). No in-place migration code is written.

## Architecture after the change

### Manifest model

`manifest.Manifest` (single-service struct) is **deleted**. The only public manifest type is `manifest.StackManifest`. The type is *not* renamed to `Manifest` — keeping the `Stack` qualifier avoids a silent semantic drift where existing `manifest.Manifest` references in unmodified files would type-check but mean something different.

`ParsePayload(data []byte) (*Manifest, *StackManifest, error)` is replaced by `ParseStackPayload(data []byte) (*StackManifest, error)` and rejects any payload missing the `services` key.

### Lease item model

`backend.LeaseItem.ServiceName` becomes a required field. `backend.IsStack` is deleted. Anywhere fred currently branches on `IsStack`, it now assumes stack semantics unconditionally. A validation step at request entry (Provision / Restart / Update) checks every item has a non-empty `service_name` and rejects the request with `ErrInvalidManifest` otherwise.

### Container provisioning

`doProvision` and `doProvisionStack` collapse to a single `doProvision`. `doRestart`/`doRestartStack` collapse to `doRestart`. `doUpdate`/`doUpdateStack` collapse to `doUpdate`. `doDeprovision` loses its `isStack` branch.

The unified implementation is the current stack implementation: `buildComposeProject` → `composeService.Up(...)`. All container creation, network attachment, label emission, and health-aware startup flow through Compose.

### Allocation IDs and names

There is only one format:

| Resource | Format |
|---|---|
| Allocation ID | `{leaseUUID}-{serviceName}-{instanceIndex}` |
| Container name | `fred-{leaseUUID}-{serviceName}-{instanceIndex}` |
| Volume ID | `fred-{leaseUUID}-{serviceName}-{instanceIndex}` |
| Compose project | `fred-{leaseUUID}` |

The legacy `fred-{leaseUUID}-{instanceIndex}` form is removed everywhere (provision.go:429, deprovision.go:155, recover.go:458, backend.go:644, lifecycle.go:1050, etc.).

### Response shape (`LeaseInfo`)

`LeaseInfo.Instances` and `LeaseInfo.Services` both stay; consumers may rely on either. After the change, **every** lease populates `Services` (with at least one entry). `Instances` becomes a flattened convenience view computed by concatenating service instances in service-name order, and is preserved for compatibility with external consumers that already read it.

Optional follow-up (not in this spec's scope): deprecate `Instances` once consumers have migrated.

### Volume binds

`setupVolumeBinds` is deleted. All call sites use `setupStackVolBinds`, which is renamed to `setupVolBinds` (no "stack" qualifier needed once there is one path).

### Recovery

`recover.go` loses its `if c.ServiceName != ""` branch (recover.go:104, recover.go:135-145, recover.go:170-174). All recovered containers must have a `fred.service_name` label. Recovery emits a fatal startup error if any container is missing the label.

The release-store path that parses persisted manifests (recover.go:80-94) uses the new `ParseStackPayload`. Historical releases written before the upgrade will not parse; this is acceptable because (a) the operator drained all leases first, so there are no live leases referencing those releases, and (b) the release store is informational/historical, not load-bearing for recovery beyond active leases.

### Manifest validation

The "depends_on forbidden in single-service" rule is dropped (it becomes structurally impossible when the only manifest type is a stack). All stack validation runs unconditionally: service-name DNS-label validation, 1:1 matching against lease items, depends_on cycle detection.

## Affected files (non-test)

| File | Change |
|---|---|
| `internal/backend/shared/manifest/manifest.go` | Delete the `Manifest` type and its validators; keep `StackManifest` as the only manifest type; replace `ParsePayload` with `ParseStackPayload`. |
| `internal/backend/client.go` | Delete `IsStack`; require `ServiceName` on every `LeaseItem`; tighten `ProvisionRequest`/`UpdateRequest` validation. |
| `internal/backend/docker/provision.go` | Delete `doProvision` (legacy); rename `doProvisionStack`→`doProvision`; collapse branches around lines 126–145, 148–164, 167–199, 209–213, 226–229; delete `setupVolumeBinds`. |
| `internal/backend/docker/restart_update.go` | Delete `doRestart` (legacy) and `doUpdate` (legacy); rename `doRestartStack`/`doUpdateStack` and collapse `isStack` branches (lines 93, 118, 151, 161, 834, 844–848, 853, 915, 972). |
| `internal/backend/docker/deprovision.go` | Delete the `if isStack` branches at lines 67, 80, 110, 143; keep the stack-style cleanup as the only path. |
| `internal/backend/docker/recover.go` | Delete legacy branch (lines 104–145, 168–174); require `ServiceName` on every recovered container; fail-fast on legacy containers. |
| `internal/backend/docker/info.go` | Always populate `LeaseInfo.Services`; also populate the flattened `Instances` view. |
| `internal/backend/docker/lifecycle.go` | Remove the dual container-name branch at line 1048-1050; only the stack form survives. |
| `internal/backend/docker/backend.go` | Remove `prevContainerName` legacy path at line 644; update `prevContainerName` to use the stack form. |
| `internal/backend/docker/compose_project.go` | No structural changes; remains the canonical project builder. |
| `internal/backend/shared/leasesm/*.go` | Drop any `IsStack` callers; `ProvisionState` always carries `StackManifest` and `ServiceContainers`. |
| `internal/api/handlers.go` | Require `service_name` on items in admin/provision-related endpoints; surface a 400 if missing. |
| `cmd/docker-backend/main.go` | No structural change; verify it doesn't fall through legacy code. |
| `docs/manifest-guide.md` | Rewrite: drop single-service format section; mark `services` key as mandatory; update examples; remove "Format auto-detection" paragraph. |
| `docs/manifest-schema.json` | Tighten schema: require `services` at top level; remove the union-type. |
| `internal/backend/docker/TENANT_MANIFEST.md` | Already a stub pointer; no change. |

## Migration & operator runbook

The upgrade is a **drain-then-upgrade** operation. Operators run, in order:

1. Stop accepting new leases (out-of-band, e.g., pause the provider on chain).
2. For each active lease, issue deprovision (chain `MsgRevokeLease` flow) and wait for all containers to be removed.
3. Confirm `docker ps -a --filter label=fred.managed=true` returns nothing.
4. Stop fred.
5. Upgrade fred binary.
6. Start fred. On startup, fred's recover step asserts no legacy containers remain; if any are found, fred exits with a clear error pointing at the offending container IDs.

The release-store database is not migrated. Historical release records may become unreadable by the new code (single-format payloads); fred treats this as benign (log a warning, skip the record). Documenting this trade-off is part of the release notes.

## Validation rules summary (post-change)

- `services` key is required in every manifest payload.
- At least one service is required.
- Every service name is a valid RFC 1123 DNS label.
- Lease items have a 1:1 mapping with manifest services by `service_name`.
- `depends_on` references only services that exist in the same payload; cycles forbidden; max depth 10.
- All existing per-service validators (image registry, port format, env names, labels, tmpfs, user, health_check, stop_grace_period, init, expose) continue to apply per service.

## Risks

1. **Chain-side dependency.** This spec assumes the chain has been upgraded to require `service_name`. If chain work lags, fred-side hard rejection of items missing `service_name` is the only safety net — sufficient but operationally rough.
2. **Operator drain window.** Hard cutover means a forced maintenance window with no live tenant workloads. Communicating this clearly is essential.
3. **External monitoring on container names/labels.** Operators relying on the legacy `fred-{uuid}-{idx}` name pattern in dashboards or alerts will break. Release notes must call this out.
4. **Compose project network creation per lease.** Compose always creates a default project network; today the legacy path does not. Operators will see one new `docker network` per lease. No functional impact, but `docker network ls` becomes noisier.
5. **Recovery fail-fast is unforgiving.** If an operator forgets a single legacy container, fred refuses to start. The error message must list the offending container IDs and the command to remove them.
6. **Release-store historical reads.** Some historical release records will no longer parse. Confirm no code path treats this as fatal.

## Out of scope (potential follow-ups)

- Deprecating `LeaseInfo.Instances` in favor of `Services` only.
- Migrating the k3s backend to the same model.
- Schema versioning for the manifest payload (today there is none).
- Auto-fix tooling that wraps legacy manifests in `{"services": {...}}` for operators preparing to upgrade.
