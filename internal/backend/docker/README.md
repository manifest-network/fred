# Docker Backend

The Docker backend provisions tenant containers with optional managed persistent volumes. It receives provision requests from Fred, manages the full container lifecycle (pull, create, start, verify, deprovision), enforces SKU-based resource limits, and reports results via HMAC-signed callbacks.

For stateful production workloads, deploy the native `docker-backend` binary
under systemd on the Docker host with XFS project quotas. Tenant containers use
managed persistent volumes; the backend manager runs on the host. The locally
built backend container image supports stateless development only. See the
[deployment guide](../../../DEPLOYMENT.md#stateful-workloads-disk_mb--0-skus)
for filesystem, mountpoint, capability, and storage-identity requirements.

Image admission requires Docker Engine **28.1+ (API 1.49+)**. Runtime construction
probes the daemon and negotiates this prerequisite before exposing admission or
execution capabilities, so unsupported APIs fail startup before storage
initialization or recovery. Image admission resolves
multi-platform indexes to a single immutable manifest before container creation.

The `imageexec` package owns this boundary. Guarded admission produces an opaque
`imageexec.Image` containing the checked identity and copied metadata. Helpers,
user resolution, image-content seeding, and workload creation require that type;
a raw reference or cached ID cannot authorize creation. Compose compilation
requires one admitted image for every service and produces an opaque
`imageexec.PreparedProject`. Execution receives that sealed plan, with no mutable
project accessor. Both executors accept only values from their own admitter.
The retained SDK views expose neither raw `ContainerCreate` nor generic Compose
`Up`; the constructors capture those methods inside the typed executors.

## Configuration Reference

All fields are set in the backend's YAML config block. Defaults come from `DefaultConfig()`.

### Core

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| Name | `name` | string | `"docker"` | Backend identifier |
| ListenAddr | `listen_addr` | string | `":9001"` | HTTP server listen address |
| DockerHost | `docker_host` | string | `"unix:///var/run/docker.sock"` | Docker daemon socket path or URL |
| HostAddress | `host_address` | string | *(required)* | External IP/hostname for port mappings. Must be a valid IP or hostname, not a URL |
| HostBindIP | `host_bind_ip` | string | `"0.0.0.0"` | IP address to bind container ports to |
| LogLevel | `log_level` | string | `"info"` | Log verbosity: `debug`, `info`, `warn`, `error`. Not set in `DefaultConfig()`; defaults to `"info"` at startup via `cmp.Or` |
| ProductionMode | `production_mode` | bool | `false` | Tightens startup checks beyond basic validation. When true, `Validate` rejects dev-only insecure toggles — currently `callback_insecure_skip_verify`. Mirrors providerd's `production_mode` |
| MaxRequestBodySize | `max_request_body_size` | int64 | `2097152` (2 MiB) | Caps inbound HTTP request body size (bytes). Falls back to `DefaultMaxRequestBodySize` (2 MiB) when unset or non-positive. Also settable via env `DOCKER_BACKEND_MAX_REQUEST_BODY_SIZE` (ENG-448) |

### TLS & mTLS (ENG-103)

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| TLSCertFile | `tls_cert_file` | string | *(empty)* | Server certificate. When set together with `tls_key_file`, the listener serves HTTPS; otherwise plaintext HTTP (default). Loaded once at startup — rotation requires a restart (ENG-294) |
| TLSKeyFile | `tls_key_file` | string | *(empty)* | Server private key. Must be set together with `tls_cert_file` |
| TLSClientCAFile | `tls_client_ca_file` | string | *(empty)* | Enables mutual TLS: the listener requires and verifies a client certificate signed by this CA. Requires `tls_cert_file` + `tls_key_file` |
| TLSClientAllowedNames | `tls_client_allowed_names` | []string | *(empty)* | Optionally pins the mTLS client identity — the presented cert's CommonName or a DNS SAN must be in this list. Empty accepts any cert signed by `tls_client_ca_file`. Requires `tls_client_ca_file` |

### Resources

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| TotalCPUCores | `total_cpu_cores` | float64 | `8.0` | Total CPU cores in the resource pool; must be finite and positive |
| TotalMemoryMB | `total_memory_mb` | int64 | `16384` | Total memory available (MB) |
| TotalDiskMB | `total_disk_mb` | int64 | `102400` | Total physical disk admission pool (MB): durable SKU disk plus pinned scratch for live diskless instances and retained/reaping durable footprints |

### SKU Management

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| SKUMapping | `sku_mapping` | map[string]string | *(empty)* | Maps on-chain SKU UUIDs to profile names |
| SKUProfiles | `sku_profiles` | map[string]SKUProfile | *(required, non-empty)* | Maps profile names to resource limits. CPU must be finite and positive, memory positive, and disk non-negative. Operator-declared; no defaults |

`sku_profiles` is required and authoritative when a new generation is admitted — `DefaultConfig()` deliberately does not seed it, because yaml.v3 merges map keys during Unmarshal and a partial operator config would silently inherit defaults (see ENG-238). Validate rejects an empty map with `"at least one SKU profile is required"`. The admitted CPU, memory, and durable disk values are then pinned in durable operation/release authority; a `disk_mb: 0` Docker row additionally pins the current `container_tmpfs_size_mb` as its mutually exclusive scratch allowance. Restart, update, recovery, and close do not continuously reprice that generation from later configuration edits.

Recommended starter profiles (copy these into your config if you want the previous four-tier shape):

| Profile | CPU Cores | Memory MB | Disk MB |
|---|---|---|---|
| `docker-micro` | 0.25 | 256 | 512 |
| `docker-small` | 0.5 | 512 | 1024 |
| `docker-medium` | 1.0 | 1024 | 2048 |
| `docker-large` | 2.0 | 2048 | 4096 |

SKU resolution: the backend first checks `SKUMapping` for a UUID-to-name translation, then looks up the name in `SKUProfiles`. This allows on-chain UUIDs to map to human-readable profile names.

### Image Security

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| AllowedRegistries | `allowed_registries` | []string | `["docker.io", "ghcr.io"]` | Registries from which images may be pulled |

Images are validated before pull. The registry is extracted from the image reference (e.g., `ghcr.io/org/app:v1` -> `ghcr.io`). Bare names like `nginx` resolve to `docker.io`.

### Callbacks

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| CallbackSecret | `callback_secret` | string | *(required, min 32 bytes)* | Bidirectional HMAC-SHA256 key; must match this backend's providerd `backends[].hmac_secret` and be unique across backends |
| CallbackInsecureSkipVerify | `callback_insecure_skip_verify` | bool | `false` | Skip TLS verification for callbacks (dev only) |
| CallbackDBPath | `callback_db_path` | string | `"callbacks.db"` | Path to the identity-bound bbolt operation-state, maintenance-intent, non-expiring close-finalizer, and callback-outbox journals; back it up with `releases.db`, `retention.db`, both storage-identity markers, and the matching substrate |
| CallbackMaxAge | `callback_max_age` | duration | `24h` | Maximum age of typed lifecycle observations; Pending and terminal operation rows, maintenance/close intents, and exact operation/maintenance completions never expire; pre-identity v0.13 callback rows must be drained before upgrade and are never runtime queue entries; must be positive |

Provision and restore requests carry two callback endpoints. The Docker backend
persists the exact operation completion URL as `fred.callback_url` and the
typed observation endpoint as `fred.lifecycle_callback_url`. Initial
provision/restore success or failure uses the exact URL. Restart, update, and
custom-domain completion uses the lifecycle URL but remains an exact,
non-coalescible maintenance delivery; autonomous container-death and deprovision
events are coalescible lifecycle observations.
Every new provision or restore requires exactly one canonical UUIDv4
`operation_id` in the completion URL, including restore from a legacy retained
source. A tokenless request is rejected with `400` before its operation intent,
restore source claim, or substrate mutation. The lifecycle field may be omitted
at request entry: the backend derives its exact typed pair before admission;
an explicitly supplied field must match that derivation byte-for-byte.
On upgrade, containers lacking the newer lifecycle label are recovered by
replacing only `operation_id` with `lifecycle_id` in a typed exact URL while
preserving unrelated query fields; an operationless legacy URL stays tokenless.
That compatibility preserves existing workloads and their lifecycle/maintenance
routes, not new tokenless provision/restore admission. Tokenless means no typed
callback identity, not unauthenticated; all callback and request HMAC checks
still apply.

Every outbound callback is persisted under its own delivery UUID before the
first attempt. Exact operation/maintenance completions and lifecycle events for
one lease therefore remain independently replayable and FIFO ordered;
successful delivery removes only that event. Lifecycle coalescing and age
cleanup never remove an exact completion.
For provision and restore, the same transaction that enqueues the exact callback
transitions the immutable operation row from Pending to Succeeded or Failed.
Delivery removes only the FIFO event; the terminal operation row remains as
exact retry and restore-recovery authority until a later authorized lease
transition atomically supersedes or retires it.
Recovery receives a sealed `OperationRecoveryState`: only
`OperationIntentClaim` can settle Pending work, while `OperationSucceeded` and
`OperationFailed` are typed, non-resolvable terminal evidence. Restore planning
is a closed type switch; nil/absence cannot construct a rollback plan.
Because maintenance completions share the lease's stable lifecycle URL and do
not carry `maintenance_id` on the wire, Docker refuses the next restart, update,
or custom-domain replacement for that lease while a prior exact maintenance
completion is queued. Successful synchronous callback delivery precisely
removes the row and reopens admission. The resulting `409 Conflict` is
lease-local and retryable; other leases remain available.
When recovery records maintenance Success followed by runtime Failed, both
rows are maintenance-derived exact deliveries; removing only the Success head
does not release the fence.
The legacy v0.13 lease-keyed queue must be empty before upgrade: current startup
and health refuse a nonempty old bucket, and the current sender never invents a
storage identity for an old row. See `DEPLOYMENT.md` for the stopped cutover;
rolling either direction across this callback-store boundary is unsupported.

Provision/restore admission also commits an immutable per-lease Pending operation row,
including the exact resolved CPU/memory/durable-disk/scratch profiles, before its first
substrate mutation. That snapshot moves unchanged into the live provision and
successful release. Terminal settlement changes only its typed state and records
the exact callback atomically; a restore handback requires the matching Failed
row and treats absence as invalid authority. Restart/update/custom-domain replacement similarly commits a
separately typed maintenance intent before appending its exact target release or
touching Docker. A store-assigned canonical UUIDv4 identifies that intent, target
release, and every target container. The cancelable pre-append admission and
append-started authority are distinct opaque capabilities; advancing to the
latter invalidates every stale cancellation token before `releases.db` can be
mutated. Deprovision uses the close variant of `callback_lease_mutation_heads`
described under
[Durable close finalization](#durable-close-finalization). None of these causal
intent classes is governed by `callback_max_age`.

### Protected launches and failed replacements

Every managed launch owns its complete volume preparation and container start
sequence. It reserves the attested physical directories, excludes namespace
changes, retires only the exact prior runtime's writers, and validates the
complete mount graph before Docker starts a replacement. Bind aliases share the
same reservation. A foreign writer or a layout where one container could replace
another container's pending bind source refuses the launch.

Before Docker receives a launch, `callbacks.db` records its exact attempt and
physical volumes. Only successful, synchronous completion of all issued Docker
calls and storage attestation can clear this record. A timeout or lost response
leaves an unresolved launch across restart. Empty inventory does not clear it;
subsequent launches and create/destroy/rename operations in that lease's
volume namespace remain blocked. This also prevents replacing an inode to evade
an outstanding request. Preserve the journal and directories when diagnosing
`physical volume has an unsettled Docker launch`; deleting a row or recreating
the volume path is not a supported repair.

Before pulling a replacement image or retiring its source, maintenance captures
the source's immutable image content/platform, effective Docker configuration,
network settings, and physical volume identities. A failure before replacement
dispatch can preserve an intact, healthy source without recreating it. If the
replacement's Docker calls completed but startup verification fails, its logs
are persisted before cleanup and the exact captured source may be recreated.
Image-derived writable paths are reseeded from that source image while retained
application data stays in its original volumes. Compensation neither
resolves a mutable image tag nor rebuilds policy from current configuration. A
verified source returns the lease to Ready while the maintenance callback still
reports failure. An empty or positively verified incomplete source cohort permits
repairing a Failed lease, but cannot authorize compensation. Foreign or divergent
survivors refuse replacement. Unknown Docker effects retain pending work;
an activated target release is never rolled back. Recreating the old application
does not reverse database migrations or other writes it made to retained data.

Image inspection helpers have their own durable ownership records in
`callbacks.db`. Their exact random name, image identity, and attempt precede
Create; cleanup uses the backend lifetime even when the inspection caller was
canceled. Startup and periodic recovery retry exact helper removal, including
a late container from a lost Create response. Those uncertain receipts do not
expire when a workload closes. Helper cleanup never treats an unrelated
container or a replacement daemon as the owned helper.

### Diagnostics

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| DiagnosticsDBPath | `diagnostics_db_path` | string | `"diagnostics.db"` | Path to the recreateable bbolt failure-diagnostics database; it carries no lifecycle authority |
| DiagnosticsMaxAge | `diagnostics_max_age` | duration | `168h` | Maximum age of persisted diagnostic entries before cleanup (7 days) |

Failure capture is keyed by the exact provision/restore or maintenance attempt.
Cleanup persists the original cause and bounded container logs before removing
failed targets; a diagnostics write failure retains cleanup for retry. Close
carries the exact interrupted attempt into that same capture path. Only current
terminal authority publishes a capture to the lease view, so delayed cleanup of
an older attempt cannot replace a newer failure.

`GET /provisions/{lease_uuid}` and `GET /logs/{lease_uuid}` retain the persisted
failure after container removal or backend restart. When compensation restored a Ready source, live log keys remain unchanged and
the failed attempt is included under `failed/<service>/<instance>` keys. Those
entries are tied to that compensated release and disappear from the active view
after a later deployment. Logs share a 32 MiB aggregate content budget, with
bounded marker and encoding overhead. Log capture is bounded to 32 MiB per attempt;
unavailable or truncated capture is recorded
explicitly. The default retention is seven days. Recreating `diagnostics.db`
while stopped loses its existing captures; it does not manufacture lifecycle or
cleanup authority.

### Releases

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| ReleasesDBPath | `releases_db_path` | string | `"releases.db"` | Path to the identity-bound authoritative release-history database; the file is required even when empty |
| ReleasesMaxAge | `releases_max_age` | duration | `2160h` | Maximum age of persisted release entries before cleanup (90 days) |

Every new release persists immutable ordered `Items` and the canonical resource
profile snapshot used for admission. A stack-form v0.13 row has neither field:
recovery derives the complete service/SKU/domain/index cohort from exact Docker
labels, checks it against the active manifest, and freezes `Items` plus every
then-current SKU profile under an exact whole-release compare-and-swap. A
service-name-less pre-stack cohort is unsupported and startup refuses it before
any substrate mutation. A transitional items-only row is backfilled only under
an exact version-and-items compare-and-swap. Keep the deployed v0.13 SKU
mapping, numeric profiles, and `container_tmpfs_size_mb` unchanged through the
first successful upgraded startup. Because v0.13 did not persist desired
quantity in its release row, the mandatory provider-side placement preflight in
`DEPLOYMENT.md` must still compare the exposed frozen `Items` with the
height-pinned chain workload before providerd starts.

Each encoded per-lease history is capped at 32 MiB. Before a provision or
restore mutates substrate, the backend proves that the exact terminal release
can fit after deterministic compaction. Compaction always preserves the
index-latest row and most recent active row.
It removes expired disposable audit entries first, then the oldest fresh
disposable entries, so capacity pressure may shorten audit history before
`releases_max_age` but cannot discard recovery or rollback-cleanup authority. If
protected authority alone exceeds the cap, admission fails before mutation.

### Authoritative store lineage

Before the first normal start of a genuinely new empty backend, run
`docker-backend -config docker-backend.yaml -initialize-storage-identity new`
once. Normal startup is verification-only and never creates, repairs, or binds a
missing marker or authoritative journal. Existing v0.13 state must use the
stopped-and-drained `adopt` workflow in `DEPLOYMENT.md`.

Before the cutover backup or `adopt`, run the upgraded binary without starting
the service:

```bash
docker-backend -config docker-backend.yaml \
  -preflight-storage-identity-adoption
```

Require exit status zero and the sole stdout line
`ready_for_v0_13_storage_identity_adoption`. This proof is read-only and leaves
both markers absent. It reports unresolved v0.13 restoring records and exact
interrupted deprovision shapes rather than synthesizing the operation authority
those records lack; follow the isolated-v0.13 repair procedure in
`DEPLOYMENT.md`, then stop, preflight, and back up again.
Both this preflight and `-initialize-storage-identity {new|adopt}` accept
`-storage-identity-operation-timeout` (default `10m`). The deadline is shared
across context-aware Docker and filesystem-control-plane probes but is
cooperative: a blocking local open, bbolt operation, or fsync cannot be
interrupted mid-syscall. A timeout is failure, not proof of rollback; keep the
lineage stopped and rerun the same mode against unchanged input.
An active legacy release with neither containers nor a retention finalizer is a
different unresolved-close refusal: never replay deprovision against it, because
that can purge the release while stranding remaining tenant volumes. Restore a
complete pre-close snapshot or follow Deployment's chain-proven manual data
decision procedure.

The supported v0.13 upgrade shape is an ordinary stack-form cohort with a
complete service label and coherent tokenless callback identity. Any pre-stack
container, `-prev` rollback remnant, authorityless migration generation, or
other partial topology fails the read-only preflight and normal startup before
mutation. This release intentionally has no in-place converter or inference
path for those shapes; restore a known-good supported snapshot or use a separate
proof-bearing repair procedure.

Storage-identity initialization binds the physical parent directories of both
markers and all three authoritative journals (`callbacks.db`, `releases.db`,
and `retention.db`) before it inspects fresh/adoption evidence. Later file
inspection, creation, and publication is relative to those retained directory
descriptors. Renaming, unmounting, or recreating a parent at the same pathname
therefore aborts sealing instead of redirecting authority into replacement
storage.

At runtime, marker/substrate verification and the three journals share one
backend-lifetime terminal authority latch. Identity drift, an outcome-unknown
bbolt commit, or another terminal substrate/store proof failure records its
first cause before canceling the backend; every sibling journal and callback
delivery then refuses with that cause. Journal writes hold the shared gate
through commit and postcheck, so withdrawal waits for an admitted write and
blocks the next one. A readable `releases.db`, for example, cannot advance after
`callbacks.db` has withdrawn the common lineage. Preserve the marker pair, all
three journals, Docker substrate, and managed volumes as one evidence/backup
set.

`TerminalStorageAuthorityFailure` publishes that first runtime cause to the
binary's main loop. The listener closes, HTTP and backend workers drain under
their fixed shutdown bounds, and the process exits status 1 even after a clean
drain so its supervisor launches a fresh `Start`. A persistent fault therefore
crash-loops closed. A failure found during `Start` exits 1 before listener bind;
there is no running server to drain. Library consumers embedding `Backend` must
also consume `TerminalStorageAuthorityFailure`, stop serving, drain, and replace
the entire Backend instance; continuing with the latched instance is unsupported.

An active Release is the durable runtime commit record. Current releases carry
an all-or-nothing typed authority that binds a valid UUIDv4 operation lineage to
the exact tenant, canonical provider UUID, emitted/effective item topology,
pinned resource profiles, manifest, and callback pair. A fully inspected
callback-bearing v0.13 cohort is instead CAS-fenced with a disjoint tokenless
`LegacyRuntimeAuthority`; the authorityless v0.13 row is accepted only as
pre-backfill upgrade input and must be frozen before any operation can remove
its last witness. Restart and Update treat a requested callback base as pending
until the replacement is Ready and that Release activates; failure and rollback
keep the previous active route. Either committed authority class can rebuild a
conservative Failed projection and full allocation with zero survivors, without
minting a replacement Release or guessing from mutable configuration.
An active callbackless pre-label cohort cannot be assigned provider callback
authority safely and is rejected by the mandatory stopped adoption preflight.
Callbackless historical cleanup/close evidence remains readable, but never
authorizes zero-survivor recovery or maintenance.

Runtime callback URLs contain causal capabilities. Treat `releases.db` and every
stopped-process copy or backup as sensitive together with `callbacks.db` and
`retention.db`; do not paste raw Release rows into logs or tickets.

Every bbolt database path uses a no-follow final-component open and requires a
single-link regular file with exact mode `0600`. The three authoritative
journals are additionally storage-identity-bound and continuously re-attested.
`diagnostics.db` carries no authority and may be recreated while stopped; it has
the same open-time shape/mode checks but no continuous lineage re-attestation.

### Soft-delete & Retention

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| RetainOnClose | `retain_on_close` | bool | `false` | When true, managed volumes are renamed into a `fred-retained-` namespace on lease close/expire instead of being destroyed. Tenants can restore data into a new lease via `POST /v1/leases/{new_lease_uuid}/restore`. |
| RetentionDBPath | `retention_db_path` | string | `"retention.db"` | Path to the identity-bound authoritative retention database; always required even when `retain_on_close` is false |
| RetentionMaxAge | `retention_max_age` | duration | `2160h` (90 days) | How long retained volumes are kept before the grace reaper destroys them. When `> 0` it also gates restore eligibility — a retained record older than this is no longer restorable. Set to `0` to disable age-based reaping **and** the age gate: retained volumes are then kept **indefinitely** and stay restorable until evicted, unless a per-tenant cap (`max_retained_leases_per_tenant > 0`) is configured to evict them. |
| RetentionReapInterval | `retention_reap_interval` | duration | `1h` | Cadence of the background retention sweep, which destroys expired retained volumes and reconciles in-flight restores. If set to `0` it falls back to `retention_max_age`, then to a hard-coded `1h`. The sweep still runs (to reconcile restores) when `retain_on_close` is set even with `retention_max_age: 0`. |
| MaxRetainedLeasesPerTenant | `max_retained_leases_per_tenant` | int | `0` (unlimited) | Maximum number of retained leases kept per tenant. When a soft-delete would exceed the cap, the tenant's oldest retained lease(s) are **evicted (hard-deleted)** at close time — oldest-first until `cap-1` remain (so a single close can drop multiple old leases). Never touches other tenants and never evicts a record being restored. `0` means no cap. |
| RetentionOrphanConfirmations | `retention_orphan_confirmations` | int | `3` | Number of consecutive retention sweeps a soft-deleted record must be observed with **all** its retained volumes missing before the record is pruned (ENG-370). Catches records orphaned when their backing volumes vanish out-of-band (host/docker churn, `docker volume prune`, data-root reset) so they don't linger for the full grace window. Fail-safe: a sweep that cannot enumerate volumes, or finds the volume root absent/unreadable, skips rather than pruning. This is a **sweep count**, not a duration — the effective confirmation window is `N × retention_reap_interval` (≈3h at the 1h default). `0` disables orphan pruning entirely (kill-switch). |
| MaxRetainedDiskMB | `max_retained_disk_mb` | int64 | `0` (unlimited) | Per-provider cap on the aggregate retained-volume disk footprint (MB) across all tenants. When retaining a closing lease would exceed this cap, the lease is destroyed immediately instead of retained (existing in-grace data is never evicted). `0` means no cap. When set, must be ≤ `total_disk_mb` **and** ≥ the largest stateful SKU's `disk_mb` (a smaller cap would make an otherwise-legal lease impossible to retain). |

> **Writable-path-only reclaim (ENG-406):** even with `retain_on_close: true`, a closing lease's volumes that hold only ephemeral `_wp/` writable-path scaffolding (no declared-`VOLUME` durable data) are **destroyed (reclaimed)** at close instead of retained — restore reseeds `_wp` from the image regardless, so retaining them preserves nothing restorable. The detector is conservative toward RETAIN (it never destroys a stateful volume). Counted by `fred_docker_backend_retention_writable_path_reclaimed_total`.

> **Duration syntax:** `retention_max_age` and `retention_reap_interval` use Go duration syntax — valid units are `h`, `m`, `s` (e.g. `2160h` for 90 days, `336h` for 14 days). The units `d` (days) and `w` (weeks) are **not** valid and will fail config validation.

### Tenant Quotas

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| TenantQuota | `tenant_quota` | object | *(none)* | Per-tenant resource limits (optional) |
| TenantQuota.MaxCPUCores | `tenant_quota.max_cpu_cores` | float64 | - | Maximum CPU cores per tenant |
| TenantQuota.MaxMemoryMB | `tenant_quota.max_memory_mb` | int64 | - | Maximum memory per tenant (MB) |
| TenantQuota.MaxDiskMB | `tenant_quota.max_disk_mb` | int64 | - | Maximum physical disk admission per tenant (MB), including pinned scratch for live diskless instances |

When `tenant_quota` is configured, no single tenant can consume more than the specified limits, even if the resource pool has capacity available. Quota values must be positive, CPU must be finite, and no quota may exceed the corresponding total pool capacity. YAML `.nan`, `.inf`, and `-.inf` values are rejected.

### Timeouts

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| ImagePullTimeout | `image_pull_timeout` | duration | `5m` | Timeout for pulling images |
| StorageAttestationTimeout | `storage_attestation_timeout` | duration | `30s` (also when zero) | Full construction-time managed-volume proof budget; post-recovery inventory proof uses `max(2m, storage_attestation_timeout)`. Negative values are rejected. |
| ContainerCreateTimeout | `container_create_timeout` | duration | `30s` | Timeout for creating containers |
| ContainerStartTimeout | `container_start_timeout` | duration | `30s` | Timeout for starting containers. Interrupted provision/restore recovery uses the durable `provision_timeout` horizon instead. |
| ProvisionTimeout | `provision_timeout` | duration | `10m` | Maximum time for the entire provisioning operation. Validated as positive — must be `> 0`. |
| ReconcileInterval | `reconcile_interval` | duration | `5m` | How often to reconcile state with Docker |
| StartupVerifyDuration | `startup_verify_duration` | duration | `5s` | Grace period after start before verifying containers are still running |
| ContainerStopTimeout | `container_stop_timeout` | duration | `30s` | Grace period before SIGKILL when stopping containers |

Configuration loading starts from `DefaultConfig`, so omitted YAML keys receive
the values above. For programmatic configs, zero
`container_stop_timeout` selects its documented default; negative values are
rejected. `startup_verify_duration: 0` likewise selects 5s rather than disabling
verification. Image pull, container create/start, provision, and reconcile
durations must remain positive. Durations use Go syntax (`h`, `m`, `s`); `d` and
`w` are not valid units.

### Container Hardening

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| NetworkIsolation | `network_isolation` | *bool | `true` | Per-tenant Docker network isolation |
| ContainerReadonlyRootfs | `container_readonly_rootfs` | *bool | `true` | Read-only root filesystem |
| ContainerPidsLimit | `container_pids_limit` | *int64 | `256` | Maximum PIDs per container |
| ContainerTmpfsSizeMB | `container_tmpfs_size_mb` | int | `64` | Size (MB) for each `/tmp`/`/run`/image-`VOLUME` tmpfs and, separately, the conservative on-disk scratch allowance pinned for every `disk_mb: 0` instance. Tenant-declared tmpfs uses the same per-mount ceiling; memory tmpfs is not charged as disk |

### Ingress (Traefik Integration)

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| Ingress.Enabled | `ingress.enabled` | bool | `false` | Enable reverse proxy label generation |
| Ingress.WildcardDomain | `ingress.wildcard_domain` | string | *(required when enabled)* | Base domain for tenant subdomains (e.g., `apps.example.com`) |
| Ingress.Entrypoint | `ingress.entrypoint` | string | *(required when enabled)* | Traefik entrypoint name (e.g., `websecure`) |
| Ingress.CustomDomainCertResolver | `ingress.custom_domain_cert_resolver` | string | `"http01"` | Traefik certresolver name used for per-tenant custom domains (HTTP-01 by default) |
| Ingress.CustomDomainMiddlewares | `ingress.custom_domain_middlewares` | string[] | `["security-headers@file"]` | Traefik middleware references applied to the secondary custom-domain router |
| Ingress.CustomDomainDNSResolvers | `ingress.custom_domain_dns_resolvers` | string[] | `["1.1.1.1:53","8.8.8.8:53","9.9.9.9:53"]` | Public DNS servers (`host:port`) fred queries to confirm a tenant custom domain resolves to this host before emitting its HTTP-01 router (ENG-266) |
| Ingress.CustomDomainDNSQuorum | `ingress.custom_domain_dns_quorum` | int | `0` (majority) | How many resolvers must independently see the domain at this host before the readiness gate opens. `0` = majority; clamped to `[1, len(resolvers)]` |
| Ingress.CustomDomainDNSCheckDisabled | `ingress.custom_domain_dns_check_disabled` | bool | `false` | Turns OFF the custom-domain DNS readiness gate, emitting the custom-domain router immediately (ENG-266) |

When enabled, containers with routable TCP ports receive Traefik Docker labels for automatic HTTPS routing. Each container gets a unique subdomain under `wildcard_domain` derived from lease UUID and service metadata (guaranteed ≤63 chars per RFC 1035). Port selection: explicit manifest `ingress` hint > 80 > 8080 > lowest TCP port. Requires `network_isolation` to be enabled — Traefik routes traffic via the per-tenant Docker network.

Routers are generated with `tls=true` but no `certresolver`. The wildcard certificate for `wildcard_domain` must be provisioned at the Traefik level — typically via a DNS-01 ACME resolver with `domains` set in Traefik's static config, or via a default certificate in `tls.stores`. Fred does not drive per-domain ACME challenges.

Example:
```yaml
ingress:
  enabled: true
  wildcard_domain: "apps.example.com"
  entrypoint: "websecure"
```

### Volume Management

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| VolumeDataPath | `volume_data_path` | string | *(empty)* | Host directory for managed durable and writable-path scratch volumes. Required when any SKU has `disk_mb > 0`; optional on an all-diskless host |
| VolumeMountPath | `volume_mount_path` | string | *(empty)* | Operator-declared active mount containing `volume_data_path`. Required and runtime-verified whenever `volume_data_path` is set, including an all-diskless managed-scratch deployment |
| VolumeFilesystem | `volume_filesystem` | string | *(auto-detected)* | Filesystem type: `btrfs`, `xfs`, or `zfs`. Auto-detected from `volume_data_path` if empty. **Only `xfs` is validated in production** — see [Supported Filesystems](#supported-filesystems) |
| MinAvgFileBytes | `min_avg_file_bytes` | int64 | 1024 | Smallest avg file size before the per-volume inode ceiling binds. On XFS, derives the `ihard` quota (disk_mb × 1 MiB / min_avg_file_bytes, floored at 262144 inodes); the same value also caps tar-extraction entry count during writable-path image seeding on any backend. |

When any SKU profile has `disk_mb > 0`, the backend manages quota-enforced host
directories that are bind-mounted into containers at their Dockerfile `VOLUME`
paths. A diskless image may also receive a managed directory only for
auto-detected non-`VOLUME` writable paths under a read-only rootfs. That scratch
directory is quota-capped at the generation's pinned `container_tmpfs_size_mb`
and is non-retainable. If no volume root is configured, scratch creation/seeding
is best-effort and skipped, while the conservative pool reservation remains.

#### Supported Filesystems

| Filesystem | Mechanism | Requirements | Production status |
|---|---|---|---|
| **xfs** | Project quotas | `pquota` mount option, `xfs_quota` binary; daemon `CAP_SYS_ADMIN` | **Validated — use this in production** |
| **btrfs** | Subvolumes with qgroup quotas | `btrfs quota enable` on the filesystem; daemon `CAP_SYS_ADMIN` | Experimental — automated tests, not production-validated or deployed |
| **zfs** | Child datasets with quota property | Parent dataset exists, `zfs` binary (exempt from `CAP_SYS_ADMIN` via `zfs allow` delegation) | Experimental — automated tests, not production-validated or deployed |

> **Production support:** **`xfs` is the only filesystem validated and used in production.** All mainnet and Morpheus backends run XFS with `pquota`, and per-volume disk *and* inode (`ihard`) quotas are exercised only on XFS. The **`btrfs`** and **`zfs`** backends have automated coverage but are **not production-validated and not used in any deployment** — treat them as experimental. In particular, the inode-exhaustion backstops that XFS enforces via kernel project quotas have no equivalent production evidence on btrfs/zfs. Use `xfs` for any production deployment.

> **Capability requirement (xfs/btrfs):** setting a volume's block-quota limit is a privileged operation, so the docker-backend must hold `CAP_SYS_ADMIN` on an xfs or btrfs backend. The daemon **fails fast at startup** if it lacks it (`internal/backend/docker/capability.go`) rather than provisioning with silently-unenforced disk caps. The startup backfill that re-tags pre-existing tenant-owned volumes additionally needs `CAP_FOWNER`. Grant them ambiently — `AmbientCapabilities=CAP_SYS_ADMIN CAP_FOWNER` on the systemd unit; a plain `setcap cap_sys_admin+ep` on the binary does **not** propagate to the exec'd `xfs_quota`/`btrfs` child processes. `zfs` is exempt (`zfs allow` delegation) and `noop` is unaffected. See [DEPLOYMENT.md](../../../DEPLOYMENT.md#xfs-good-for-large-fleets) and its [systemd section](../../../DEPLOYMENT.md#process-management-systemd) for the full setup.

Startup quota backfill uses each generation's immutable effective authority:
durable `disk_mb` for stateful volumes and pinned scratch for a physically
present diskless writable-path volume. Mutable config is consulted only for a
true v0.13 row whose entire profile snapshot is absent, then the result is
persisted once. Scratch remains excluded from retention caps; an exact scratch
name conservatively present in a retention row stays physically counted and
re-capped only until it is destroyed.

#### Stateful vs Ephemeral Containers

| SKU `disk_mb` | Behavior | Image VOLUME paths |
|---|---|---|
| `> 0` (stateful) | Quota-enforced host directory created per container | Bind-mounted from host directory |
| `0` (ephemeral) | Pins one scratch allowance per instance; creates a quota-enforced host directory only when a writable path is detected and managed volumes are configured | Overridden with tmpfs (prevents anonymous volumes); never bind-mounted as durable data |

All containers have a readonly root filesystem by default (configurable via
`container_readonly_rootfs`). Stateful containers write durable data to
bind-mounted volumes. Ephemeral containers use memory tmpfs for `VOLUME`, `/tmp`,
`/run`, and tenant tmpfs mounts; only auto-detected writable-path scaffolding may
use the separate host scratch described above.

**Example stateful SKU:**

```yaml
volume_data_path: "/var/lib/fred/volumes"
volume_mount_path: "/var/lib/fred/volumes"
# volume_filesystem: "xfs"  # optional, auto-detected (xfs is the only production-validated backend)

sku_profiles:
  docker-redis:
    cpu_cores: 0.5
    memory_mb: 512
    disk_mb: 2048
```

When provisioning `redis:latest` on this SKU:
1. Image inspected — discovers `VOLUME /data`
2. Host directory created: `/var/lib/fred/volumes/fred-<lease>-0/` with 2048 MB quota
3. Subdirectory `data/` bind-mounted to container `/data`
4. Redis writes to `/data` — quota enforced by kernel
5. On deprovision: an XFS delete authority is synced first, the host directory
   is removed in place, block and inode usage are proved zero, and all
   project-quota limits are cleared (`bhard=0 bsoft=0 ihard=0 isoft=0`) before
   the authority is removed

> **XFS quota-table hygiene and crash recovery (ENG-459/ENG-632):** before
> removing any final-volume bytes, `Destroy` creates an empty, project-zero,
> parent-synced
> `.fred-xfs-delete-<project-id>-<managed-volume>` sibling. The final name stays
> in place while deletion progresses. After its absence is synced, Fred proves
> both block and inode usage are zero, strictly clears all four limits, then
> removes and syncs the authority. An open-but-unlinked file therefore blocks
> completion. Any failure retains the authority, fail-stops the current backend
> instance, and blocks recreation under the same final name. The daemon closes
> its listener, drains, and exits status 1 so its supervisor launches a fresh
> `Start`; an unrepaired fault crash-loops closed. Recovery must complete before
> readiness; this is not a best-effort current-generation cleanup. Entries
> leaked by pre-ENG-459 daemons
> without a typed authority still require the one-time manual operator cleanup.

### SKU Profile Fields

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| CPUCores | `cpu_cores` | float64 | — | CPU cores allocated to each container |
| MemoryMB | `memory_mb` | int64 | — | Memory in MB allocated to each container |
| DiskMB | `disk_mb` | int64 | `0` | Durable, retainable disk budget in MB. When `> 0`, a quota-enforced host directory is bind-mounted to image `VOLUME` paths (requires `volume_data_path`). When `0`, image `VOLUME` paths are overridden with tmpfs and the backend pins `container_tmpfs_size_mb` separately as non-retainable scratch admission |

## Tenant Manifest Reference

See [Manifest Guide](../../../docs/manifest-guide.md) for the full tenant-facing manifest specification (image, ports, env, health check, tmpfs). A formal [JSON Schema](../../../docs/manifest-schema.json) is also available.

## Soft-delete & Restore

When `retain_on_close: true` is set, the backend performs a **soft-delete** instead of a hard destroy at lease close or auto-expire time:

1. Canonical volumes that are **writable-path-only** — they hold only the ephemeral `_wp/` scaffolding (a read-only-rootfs writable-path mount) and no declared-`VOLUME` durable data — are **destroyed (reclaimed)**, not retained. Restore reseeds `_wp` from the image anyway (the ENG-367 wipe-contract), so retaining such a volume preserves nothing restorable and only pollutes the retention record, a per-tenant slot, the retained-disk budget, and the volume root. The detector (`isWritablePathOnly`) is conservative toward RETAIN — it destroys only *provably* `_wp`-only volumes, never a stateful one (ENG-406). If uncertainty nevertheless leaves an exact diskless scratch name in a retention row, its pinned allowance stays in physical pool accounting and quota backfill until destruction; that fail-closed exception is not retention entitlement. Observable via `fred_docker_backend_retention_writable_path_reclaimed_total`.
2. The remaining managed volumes for the lease are **renamed** from `fred-<lease_uuid>-…` into a `fred-retained-<lease_uuid>-…` namespace and kept on disk.
3. The original containers and resource-pool allocations are still released (the running workload is stopped; resources are freed for new leases).
4. Fred publishes a `retained` status event to any connected tenant WebSocket so the tenant knows their data may be recoverable.
5. Retained volumes are held for up to `retention_max_age` (default 90 days). The grace reaper runs every `retention_reap_interval` (default 1h) and destroys expired retained volumes.
6. If a retained lease's backing volumes disappear **out-of-band** (host/docker churn, `docker volume prune`, a data-root reset on redeploy) while its record survives, the periodic sweep prunes the now-orphaned record after it is observed fully volume-less for `retention_orphan_confirmations` consecutive sweeps (default 3). This keeps dead records from accumulating for the full grace window. The prune is fail-safe — a sweep that errors listing volumes, or finds the volume root absent/unreadable, skips entirely rather than risk pruning a record whose volumes are merely transiently unavailable. Observable via `fred_docker_backend_retention_orphans_pruned_total` and `fred_docker_backend_retention_orphan_skips_total{reason}`.

### Restore flow

To restore data from a closed lease into a new lease:

1. Open a **fresh lease on the same provider** by requesting the **same service names and quantities** as the original closed lease. The new lease UUID (`new_lease_uuid`) will be in `PENDING` state.
2. Call `POST /v1/leases/{new_lease_uuid}/restore` with body `{"from_lease_uuid": "<original_closed_lease_uuid>"}`. Fred validates the request and delegates to the backend.
3. The backend renames the retained volumes into the new lease's namespace (the synchronous **adopt** phase) and re-deploys the **retained manifest** (the exact deployment that was running at close time) onto them. The new lease becomes active with the same data. To change the image or configuration after restore, use the normal update path once the lease is active.

Restore-specific re-deploy behavior worth knowing:

- **Image must already be present on the node.** Restore reuses the replace machinery and starts by inspecting the local image. If it was garbage-collected since close, restore fails with an image-inspect error; pre-pull it before restoring. Classic images and independently addressable platform manifests require no registry access. A legacy containerd index whose selected platform lacks an independent image-store record needs a one-time pull by that exact manifest digest, even when its layers are cached. If the registry is unavailable, this preparation fails before creating containers.
- **Image and configuration are fixed.** Restore deploys strictly from the retained `StackManifest` and items; the request carries no manifest. The new lease's requested service names and quantities must shape-match the retained set exactly (otherwise the restore is rejected with a validation error).
- **The SKU tier may change (promote/demote).** Only the item *shape* must match (service names + quantities); the SKU's resource (disk) tier **may** differ from the source lease. A **promote** (same-or-larger `disk_mb` tier) is admitted only when its aggregate growth above the retained footprint fits disk capacity, then the larger cap is applied. A **demote** (smaller `disk_mb` tier) is allowed only if the retained volume's **measured** data fits the new tier's `disk_mb` cap — the backend runs `checkDemoteFit` before adopting (restoring durable stateful data into an ephemeral `disk_mb=0` tier is always refused). The conservative exact-name exception above may restore scratch only into another diskless row, after measuring it against that destination's pinned scratch allowance. A refused demote returns HTTP `422` with body `{"code":"demote_exceeds_tier"}` (`backend.ErrDemoteDataExceedsTier`) and is counted by `fred_docker_backend_restore_demote_refused_total{backend,reason}` (`reason` ∈ `measured_exceeds`, `unmeasurable_read_error`, `unmeasurable_backend`, `ephemeral_tier`); it is **not** counted by `restore_total`.
- **Containers are recreated, ownership is not rewritten.** Restore does not force-recreate beyond the normal replace, and the volume chown is non-recursive (it sets ownership on the VOLUME mount point only), so existing files keep their on-disk ownership.

### Limitations

- **Best-effort and capacity-bounded**: retention is not a guarantee. When a per-tenant cap (`max_retained_leases_per_tenant > 0`) is configured, a soft-delete may evict that tenant's oldest retained lease(s) — independent of age — to make room for the newer one. Always restore within the grace window.
- **Same-backend-node only**: a restore can only run on the backend node that physically holds the retained volumes (the rename is local; nothing is copied between nodes). In single-backend deployments this is always satisfied. In multi-node deployments restore routing is automatic: the reconciler queries each backend's `GET /retentions` and records each retained lease's backend in the placement store, so a restore is routed to the node holding the source data (ENG-333). Restore returns `404` if no backend still holds that lease's retained data.
- **Not a backup**: retained data is a single copy on the node's local disk (RAID-backed by the operator). It provides a grace window against accidental lease closure, not protection against node-level data loss. Operators should run separate backup procedures for production data.

### Failure handling & crash recovery

Restore is crash-safe and self-healing. A retention record carries one of three persisted statuses — `active` (awaiting restore or reap), `restoring` (a restore is in flight), and `reaping` (volumes are pending physical destruction). Adoption renames volumes and applies the destination filesystem quota; rollback must reverse both mutations safely:

The `reaping` status is a finalizer tombstone (ENG-376): when a retained record
is grace-expired or cap-evicted, the record is **not** deleted at the
active→reaping transition. It outlives its volumes and is deleted only after
every volume is confirmed destroyed. The bytes therefore remain counted while
the record is no longer restore-claimable. A record that cannot be reclaimed
stays `reaping` (observable via
`fred_docker_backend_retention_reaping_leases` /
`fred_docker_backend_retention_reaping_bytes`), and a failed destroy increments
`fred_docker_backend_retention_leaked_total`. A historical v0.13 give-up
tombstone remains readable and converges through the same finalizer; current
close failure keeps its non-expiring close intent instead of creating one.

A tombstone scopes cleanup to one exact lease namespace; it does not carry a
caller-authored destroy list. A restore cannot acquire that lease as its
destination: the completed close that produced the retention row installed a
permanent mutation head, so operation admission fails before the source can move
to `restoring` (ENG-659). The inverse ordering is also excluded because a pending
restore operation prevents close admission. The finalizer still re-checks live
ownership at destroy time, so a re-provisioned lease is never reclaimed, and an
unreadable retention store means nothing is destroyed that pass. Those holds are
counted by `fred_docker_backend_retention_reap_skips_total{reason}` and are
deliberately **not** counted as leaks—the footprint keeps counting while the
record stays `reaping`.

- **Restore failure before commit:** the new lease's compose project is torn down
  and adopted volumes are **re-quarantined**. The backend proves/reapplies the
  immutable source quota and transitions the exact operation row to Failed while
  atomically enqueueing its callback before it
  pre-counts the retained footprint, CAS-reactivates the source, and releases
  destination allocations. After actor acceptance, physical rollback parks the
  row `restoring` until the actor has durably recorded Failed and enqueued its
  callback; the periodic sweep completes the handback from that terminal row.
  Successful callback delivery does not remove it, and absence fails closed.
  Any uncertainty remains `restoring` and live-counted.
- **Committed restore followed by failure:** an exact active destination Release
  is the durable commit marker. It is retained even if the destination is now
  Failed or absent; a matching Pending operation is transitioned to Succeeded.
  An exact Succeeded operation plus the immutable source finalizer reconstructs
  a missing active Release; Failed plus an exact committed Release is
  contradictory authority and fails closed. With zero survivors, recovery
  publishes a conservative Failed destination, keeps its
  exact allocation, and retains the source finalizer as tenant/provider identity
  across repeated restarts. Once no Pending or contradictory Failed restore
  operation remains (an authorized successor may already have retired Succeeded
  history), a plain
  Restart may repair the destination; reaching Ready lets reconciliation consume
  that row. Close first commits a complete close intent and then deletes it
  before teardown.
  This is a post-commit runtime failure, never source rollback.
- **Destination mutation fence:** while the source row remains `restoring`, new
  Provision and Restore operations for its destination are rejected. Before
  commit, maintenance is rejected too. After an exact active Release proves
  commit and no Pending or contradictory Failed restore operation remains, a
  plain Restart may recover a
  committed Failed destination. Update and custom-domain redeploys remain fenced
  until that Restart reaches Ready and finalizer reconciliation consumes the
  row. The volatile provision map alone cannot release that fence.
- **Crash mid-restore**: on the next startup (and on every periodic sweep) the
  backend reconciles dangling `restoring` records — recognizing exact committed
  Releases and rolling back only pre-commit generations. The restoring record is
  itself the exact ownership/finalizer authority; no unattributed-volume reaper
  infers destructive permission. The complete decision and mutation hold the destination lease's
  command fence and typed exclusive actor-quiescence claim, so queued messages,
  handlers, workers, terminal handoff, and actor replacement cannot race a stale
  failed snapshot to Ready. A
  backend-local recovery-snapshot fence also serializes Restore's durable
  admission and rollback handback with inventory-to-projection/pool publication,
  so an absent-to-transient-to-absent restore cannot be resurrected from stale
  Docker inventory. A record is written before the rename, so a crash in the
  narrow window between the two is repaired by re-quarantine rather than data
  loss.

## Provisioning Lifecycle

1. **Synchronous validation** -- the `Provision` method validates the request before returning:
   - Checks for duplicate lease (returns `ErrAlreadyProvisioned` unless existing provision is failed)
   - Resolves all SKUs to profiles via `SKUMapping` + `SKUProfiles`
   - Parses the JSON manifest and validates image, ports, labels, and health check
   - Validates the image against `AllowedRegistries`
   - Freezes immutable resource profiles and allocates all instances from the
     resource pool (rolls back on failure). A diskless instance reserves its
     pinned scratch allowance before image inspection, whether or not a host
     scratch directory is later required

2. **Asynchronous provisioning** -- runs in a goroutine tracked by a `WaitGroup`:
   - Pulls the image (once, shared across all containers in the lease)
   - Inspects the image to discover Dockerfile `VOLUME` declarations
   - Creates/ensures the per-tenant network (if `NetworkIsolation` is enabled)
   - For each item in the lease (supports multi-SKU), for each unit (supports multi-unit):
     - For stateful SKUs (`disk_mb > 0`): creates a quota-enforced host directory and bind-mounts image VOLUME paths into it
     - For ephemeral SKUs (`disk_mb == 0`): overrides image `VOLUME` paths with
       tmpfs; when managed volumes are configured and a non-`VOLUME` writable
       path is detected, creates a quota-capped scratch bind instead
     - Creates a container with the appropriate SKU profile, hardening settings, and labels
     - Starts the container
   - Verifies startup (see [Startup Verification](#startup-verification) for the two paths)

3. **Callback** -- on success or failure, sends an HMAC-signed callback to the URL provided in the provision request.

Multi-unit leases create multiple containers from the same manifest. Multi-SKU leases create containers with different resource profiles per SKU. Instance indices are 0-based across all items.

The entire async operation is bounded by `ProvisionTimeout` and is canceled on backend shutdown.

### Stack Provisioning

When lease items carry `service_name` fields (and the payload is a [stack manifest](../../../docs/manifest-guide.md#stack-manifest)), the backend provisions a multi-service stack:

1. **Synchronous validation** — same as single-container, plus:
   - Detects stack vs single mode via `IsStack(items)`
   - Validates 1:1 mapping between manifest service names and lease item service names
   - Proves that quantity expansion produces unique Compose keys (for example,
     `web` quantity 2 cannot coexist with an unscaled `web-0`); Compose PS output
     is attributed through that exact key map rather than numeric-prefix parsing
   - Validates each per-service manifest independently

2. **Asynchronous provisioning** — Docker Compose-based deployment:
   - Each service's image is pulled and inspected independently (pre-flight, before Compose)
   - Volumes are pre-created for stateful services (`disk_mb > 0` with image
     `VOLUME`s) and for detected writable-path scratch when available
     - Resource allocation ID: `{leaseUUID}-{serviceName}-{instanceIndex}`
     - Volume ID: `fred-{leaseUUID}-{serviceName}-{instanceIndex}`
   - A Compose project is built in-memory from the stack manifest via `buildComposeProject`
   - Service startup ordering is controlled by `depends_on` declarations in the manifest (supports `service_started` and `service_healthy` conditions with cycle detection)
   - `compose.Up` atomically creates, starts, and network-attaches all service containers
   - `compose.PS` discovers the resulting container IDs per service
   - Startup verification runs per-service, each using its own health check config
   - Restart/update uses `compose.Up` with the updated project; on failure, the previous manifest is rebuilt and rolled back via another `compose.Up`
   - Deprovision uses `compose.Down` for atomic cleanup, with fallback to individual container removal

3. **Callback** — single callback for the entire stack (success only when all services are healthy/running).

## Container Hardening

Every container is created with the following security measures:

| Feature | Implementation | Notes |
|---|---|---|
| Drop all capabilities | `CapDrop: ["ALL"]` | No Linux capabilities granted |
| No new privileges | `SecurityOpt: ["no-new-privileges:true"]` | Prevents privilege escalation via setuid/setgid |
| Read-only root filesystem | `ReadonlyRootfs: true` | Configurable via `container_readonly_rootfs` |
| Tmpfs for `/tmp` and `/run` | `Tmpfs: {"/tmp": "size=64M", "/run": "size=64M"}` | Only when readonly rootfs is enabled; size from `container_tmpfs_size_mb`. Tenants may request up to 4 additional tmpfs mounts via manifest, for a maximum of 6 total (384MB at default size). **Note:** On cgroup v1, tmpfs memory is not counted against the container's cgroup memory limit. On cgroup v2 (default on modern systems), it is. |
| PID limit | `PidsLimit: 256` | Configurable via `container_pids_limit` |
| Memory (no swap) | `MemorySwap == Memory` | Prevents swap usage entirely |
| Restart policy disabled | `RestartPolicyDisabled` | Failed containers stay dead for crash detection |
| Network isolation | Per-tenant bridge network | Configurable via `network_isolation` |

## Startup Verification

After all containers in a lease are started, the backend verifies they are ready before sending a success callback. The verification path depends on whether the manifest declares an active health check.

### No health check (fixed-wait path)

When the manifest has no `health_check` (or sets `Test[0]` to `"NONE"`), the backend waits for `StartupVerifyDuration` (default 5s) and then inspects each container. If any container has exited during this window, the entire provision is marked as failed and cleaned up.

This catches containers that crash immediately on startup due to bad configuration, read-only filesystem errors, missing dependencies, or similar issues -- before a success callback is sent and the lease is acknowledged as active on chain.

Note: the runtime uses `cmp.Or` to fall back to 5s when the value is zero, so setting `startup_verify_duration: 0` does not disable verification -- it uses the 5s default.

### With health check (health-aware path)

When the manifest declares an active health check (`health_check` with `Test[0]` of `"CMD"` or `"CMD-SHELL"`), the backend polls every 2s until all containers report `healthy`. The behavior on each poll:

- **`healthy`** -- container passes, removed from the pending set.
- **`unhealthy`** -- provision fails immediately with an error.
- **Container exited** -- provision fails immediately (caught before checking health status).
- **`starting`** -- keep polling.

The polling is bounded by the existing `ProvisionTimeout` context (default 10m). If the timeout fires before all containers are healthy, the provision fails. Operators must ensure `ProvisionTimeout` is compatible with their health check timing (start period + interval * retries).

A health check defined in the Dockerfile but not in the manifest does **not** trigger the health-aware path -- the manifest is the contract.

## Re-provisioning

When a provision has `status=failed` (e.g., a container crashed and was detected by the reconciler), a new `Provision` call for the same lease UUID is allowed. The re-provision flow:

1. The existing `FailCount` is carried over from the failed provision record.
2. Resource allocations are released and old containers are removed. Managed volumes are **kept** — stateful data persists across re-provisions.
3. A new provision record is created with `FailCount` preserved.
4. The full provisioning flow runs again (image pull, image inspect, volume setup via idempotent Create, container create/start, startup verification). Existing volumes are reused with quota updated; only new volumes are created.
5. On failure, `FailCount` is incremented. The `FailCount` is also persisted in the `fred.fail_count` container label. Only newly created volumes are cleaned up; reused volumes are preserved.

## Lease State Machine

**One concept: the lease actor is the scope of atomicity for its messages and its workers.** Everything else falls out of that invariant:

- **Typed registry atomicity** — the actor registry (`b.actors`) is guarded by
  a mutex. The closed `ActorCommandMessage` set may resolve-or-create through
  `routeToLease`; the closed `ActorObservationMessage` set must instead carry an
  exact provision pointer plus deep immutable generation snapshot.
  `routeActorObservation` revalidates that claim, the recovery reservation,
  actor resolution, and enqueue under the same registry critical section. Its
  bound validator runs again immediately before serial actor handling, closing
  the enqueue-to-handle window. Delayed die/cohort observations therefore cannot
  materialize an actor after rollback or target a replacement actor, and callers
  never retain an actor pointer.
- **Worker ownership** — every worker goroutine (provision, restart, update, diag) is spawned by the actor and tracked by its per-actor `workers` barrier (a channel-signaled reference counter; see `work_barrier.go`). Normal actor exit waits for `workers.Zero()` before registry deletion and inbox drain. The wait is bounded: a stuck worker aborts a preempting state transition (so deprovision cannot tear down underneath it), while shutdown eventually returns `ErrShutdownDrainTimeout`, leaves dependencies open, and makes the process exit non-zero. The barrier's channel-based wait means a wedged worker adds no leaked waiter on top of itself.
- **Typed recovery quiescence** — restore reconciliation must acquire an opaque `QuiescenceClaim` from the exact registry actor before reading mutable recovery inputs. One activity count overlaps queued/handling messages, worker execution, and worker-to-terminal-message handoff. The claim holds admission and activity gates and pins the actor against retirement/replacement until release; routing refuses without blocking while it is held. A missing claim defers that lease. Recovery never composes racy inbox-depth and worker-idleness snapshots into authority.
- **Provision capacity ownership** — fresh provisions and retries both require
  an operation-bound `ProvisionAdmission`. The pool reserves the conservative
  predecessor/candidate envelope before actor dispatch or teardown; the actor
  consumes it into one worker's `ProvisionResourceExecution`. Rejection aborts
  an unconsumed admission, matching terminal evidence finalizes accounting, and
  ambiguity keeps the reservation for durable recovery. An absent active release
  never bypasses admission, and a downgrade never exposes predecessor capacity
  before cleanup. Recovery uses the same envelope calculation.
- **Drain-with-handle** — on exit, any message in the inbox is processed via `handle()` (not just closed-and-dropped). Terminal events delivered during the shutdown window still drive their SM transition. Silent drops are gone.
- **Non-blocking routing** — both command and exact-generation observation
  routers use a non-blocking inbox send under the registry mutex. A wedged actor
  cannot stall the event loop. Observation refusal—including generation drift,
  a recovery reservation, shutdown, or a full inbox—is retried from current
  state by reconciliation. Container-death refusal increments
  `die_event_dropped_total`; cohort-divergence refusal is logged.

Every lease is owned by a per-lease actor goroutine with a bounded inbox (16 messages). All transitions flow through a state machine, one per actor, which serializes transitions and owns the side effects (callback emission, diagnostics persistence, gauge updates). The SM's initial state is the lease's current `Status` at actor creation — new leases start in `Provisioning`, recovered leases start in whatever state they were in.

```mermaid
stateDiagram-v2
    Provisioning --> Ready: ProvisionCompleted
    Provisioning --> Failed: ProvisionErrored
    Provisioning --> Deprovisioning: DeprovisionRequested

    Ready --> Failing: ContainerDied [guard]
    Ready --> Failed: CohortDiverged [durable release mismatch]
    Ready --> Deprovisioning: DeprovisionRequested
    Ready --> Restarting: RestartRequested
    Ready --> Updating: UpdateRequested

    Failing --> Failed: DiagGathered
    Failing --> Deprovisioning: DeprovisionRequested

    Failed --> Provisioning: ProvisionRequested
    Failed --> Restarting: RestartRequested
    Failed --> Updating: UpdateRequested
    Failed --> Deprovisioning: DeprovisionRequested

    Restarting --> Ready: ReplaceCompleted
    Restarting --> Ready: ReplaceRecovered
    Restarting --> Failed: ReplaceFailed
    Restarting --> Deprovisioning: DeprovisionRequested

    Updating --> Ready: ReplaceCompleted
    Updating --> Ready: ReplaceRecovered
    Updating --> Failed: ReplaceFailed
    Updating --> Deprovisioning: DeprovisionRequested

    Deprovisioning --> [*]
```

The edges above are the complete set of allowed transitions; any event not listed against a source state is either ignored (see below) or rejected as an invalid trigger. The authoritative source is `internal/backend/shared/leasesm/lease_sm.go`.

### Key behaviors

- **`Ready → Failing` guard.** The `ContainerDied` trigger fires only if a Docker `Inspect` confirms the container actually exited. Die events can be duplicated or stale; the guard filters them.
- **Preemption via `OnExit` cancellation + `workers.Zero()`.** `Failing`, `Provisioning`, `Restarting`, and `Updating` each own one async worker goroutine (diag gather, provision, or replace). Every transition out of these states calls the worker's `CancelFunc` via `OnExit`, then `a.waitForWorkers()` selects on the per-actor `workers.Zero()` channel until the goroutine has returned and its terminal `sendTerminal` has landed in the inbox. If that bounded wait expires, the state transition fails and `Deprovision` refuses substrate teardown; a retry can proceed once the worker actually drains. This preserves the rule that a canceled worker cannot recreate containers after close.
- **Durable cohort divergence.** Recovery compares the exact service/SKU/index/domain/image set observed from Docker with the active release's durable `Items`. A mismatch is a typed `Ready → Failed` transition (or a directly materialized cold-start failure), not a fabricated container-death event. Supported stack-form v0.13 rows receive ordered items and canonical resource profiles under compare-and-swap before ordinary recovery; transitional items-only rows are version-and-items CAS-backfilled.
- **Exact-key command fence.** A zero-value-ready, ref-counted keyed mutex serializes mutation admission, complete restore reconciliation, and teardown for one lease. Unrelated leases never share a stripe, idle entries are removed, and durable journals—not the mutex—remain crash-recovery authority.
- **Construction-bound recovery exclusion.** Background operation,
  maintenance, and close convergence can receive a short-lived recovery scope
  only from the backend's fixed coordinator. That coordinator holds both the
  exact command-fence key and an actor-registry/quiescence reservation across
  classification or cleanup. Live close instead presents an actor-owned scope
  minted only after its transition has drained the worker. Copies are revoked
  when either callback returns, so a durable claim or stale actor reference is
  not independently destructive authority.
- **Defense-in-depth `Ignore` on `Deprovisioning`.** Cancellation is best-effort: a goroutine can race past the cancel signal and fire its completion event anyway. `Deprovisioning` ignores every such event (`DiagGathered`, `ProvisionCompleted`, `ProvisionErrored`, `ReplaceCompleted`, `ReplaceRecovered`, `ReplaceFailed`) so the race is structurally safe.
- **One terminal decision per transition.** Provision, failure, and replace callback decisions live in SM entry actions (`onEnterReadyFromProvision`, `onEnterFailedFromDiag`, `onEnterFailedFromProvision`, `onEnterReadyFromReplaceCompleted`, `onEnterReadyFromReplaceRecovered`, `onEnterFailedFromReplace`), never in workers. Deprovision settlement is instead owned by the durable close finalizer: its precise resolution atomically removes the close capability and enqueues one lifecycle result. Worker preemption, ignored late events, and the durable claim together prevent a second logical settlement; HTTP redelivery of the one outbox entry remains expected.
- **Three `Replace*` events for two terminal states.** `ReplaceCompleted` means restart/update succeeded (→ `Ready`, Success callback). `ReplaceRecovered` means it failed but rollback restored a working lease (→ `Ready`, Failed callback with rollback suffix). `ReplaceFailed` means both the operation and the rollback failed (→ `Failed`, Failed callback).
- **Non-blocking routing, reconciler backstop.** Command routing and
  `routeActorObservation` are non-blocking. Container-event and reconcile
  observations carry the exact provision-generation snapshot that produced
  them. Admission atomically rejects an already-stale claim, and the bound actor
  message discards one that becomes stale while queued, instead of creating or
  targeting the wrong actor.
  Container-death refusal is counted in `die_event_dropped_total`,
  cohort-divergence refusal is logged, and the reconciler re-detects both from
  current state. One wedged actor cannot stall die-event delivery for other
  leases.

### Observability

- `fred_docker_backend_lease_sm_transitions_total{from,to,event}` — every transition.
- `fred_docker_backend_lease_actors_created_total` — cumulative actor count; should track distinct leases (recycled UUIDs after Deprovision produce a fresh actor, so this counter grows faster than the live-actor count).
- `fred_docker_backend_lease_actor_stuck_seconds` — age of the oldest in-flight actor handler. Alert threshold should exceed the longest legitimate operation (Deprovision can hold an actor for minutes during container/volume cleanup).
- `fred_docker_backend_lease_actor_inbox_depth` — histogram of per-actor inbox depth; p99 near 0 is healthy.
- `fred_docker_backend_lease_actor_panics_total` — counts panics recovered inside actor handlers. Any non-zero is a bug; the actor survives and keeps processing, but the message that panicked did not drive its transition.
- `fred_docker_backend_lease_terminal_event_dropped_total{event}` — worker terminal sends refused because the actor had exited (pathological `waitForWorkers` timeout). Should be zero in normal operation.
- `fred_docker_backend_die_event_dropped_total{source}` — container-death
  observations refused because their exact generation was stale, recovery held
  the actor key, the backend was shutting down, or the current actor's inbox was
  unavailable. `source` is `event_loop` or `reconcile`. The reconciler
  re-detects current failures; sustained growth flags churn, recovery contention,
  a wedged actor, or chronic burst.
- `fred_docker_backend_pending_close_intents` and `fred_docker_backend_oldest_close_intent_age_seconds` — unlabeled aggregate count and oldest age for the non-expiring destructive-close journal. A brief non-zero value is normal while a close runs; sustained age means a finalizer dependency is unavailable. Use the lease-scoped recovery log to identify the row without introducing an unbounded lease label.
- `fred_docker_backend_operation_intent_recovery_timeout_exhaustions_total{reason="provision_timeout"}` — exact provision/restore intents classified past their durable admission deadline. Both kinds share this configured horizon. Cleanup remains periodic and retryable; there is no container-start recovery timer.
- `fred_docker_backend_operation_intent_recovery_cleanup_retries_total` — Deferred exact operation cleanup (`provision`/`restore`); intent and reservation remain for periodic retry.
- `fred_docker_backend_terminal_substrate_pending_containers` — Last late-container count for permanent `closed`/`failed_operation` receipts; nonzero withholds this backend’s pool capacity/readiness until strict absence.
- `fred_docker_backend_terminal_substrate_cleanup_retries_total` — Transient late-container cleanup retries; daemon stays alive and exact terminal receipts remain.
- `fred_docker_backend_unaccounted_managed_volumes` — Attested managed volumes absent from current live, admitted-operation, and all retention projections; diagnostic only, never deletion or admission authority.
- `fred_docker_backend_unaccounted_managed_volume_observation_failures_total` — Failed diagnostic inventory/footprint observations; last unaccounted-volume gauge is retained, not reset to zero.
- `fred_docker_backend_reconciliation_total{outcome}` and `fred_docker_backend_reconciliation_last_success_timestamp_seconds` — the runtime signal for `recoverState`, including maintenance-WAL convergence. A valid but semantically indeterminate maintenance row can make a pass report `outcome="error"` and leave last-success stale while `/health` remains green and `callback_store_errors_total` remains unchanged; those latter signals validate structural store access, not every substrate classification. During startup the equivalent failure exits before the periodic loop starts and appears as `failed to recover state` with the lease-scoped nested error.
- `fred_docker_backend_retention_sweep_total{outcome}` — one increment per periodic retention-sweep pass, `success` or `error`. The sum across outcomes is a liveness heartbeat (it advances every tick regardless of result); `{outcome="error"}` means a sweep stage failed — usually an unenumerable retention store, but the orphan stage reports a failed volume-root enumeration here too, so the joined stage error is what identifies the actual failing dependency. Every stage runs on every pass and the stage errors are joined, so the log line names all of them rather than only the first.
- `fred_docker_backend_retention_accounting_refresh_failed_total` — the retained-disk projection could not be recomputed and the previous value was kept. Safe (a zeroed projection would over-admit) but it means the five retention gauges and the pool's retained input are stale while this rises.

## State Recovery

On startup and at each backend `ReconcileInterval`, `recoverState` rebuilds
in-memory state from Docker. Direct in-process callers may also invoke
`RefreshState`. In the standard separate-process deployment, providerd's HTTP
backend client's `RefreshState` is intentionally a no-op: a providerd reconcile
sweep reads the last backend projection and does not force an extra Docker
recovery pass. Runtime WAL retry cadence is therefore the docker-backend's own
`reconcile_interval` (default `5m`), not providerd's sweep interval.

The bounds are nested and aggregate where cardinality matters:

- `New` uses `storage_attestation_timeout` (default `30s`) for the complete
  construction-time substrate/storage-identity attestation. Large fleets can
  widen that aggregate without changing individual Docker call limits.
  Library callers that need a caller-owned construction deadline use
  `NewWithContext`, which adds no fallback deadline; pass a finite context. The
  context is not retained after construction.
- `Start` shares the shorter of its caller context and 30 seconds across initial
  identity/connectivity reads, then uses the backend lifecycle context with one
  finite overall crash-recovery budget. Production derives it as the saturating
  sum of every sequential phase's local maximum (51m10s with defaults), reserving
  the shared operation-classification and cleanup phase even if every earlier
  phase consumes its cap. Transitional operations are deferred to periodic
  sweeps, not waited out during startup.
- Within that overall budget, interrupted-volume recovery uses a fixed
  two-minute child deadline. Its complete clean-inventory proof uses
  `max(2m, storage_attestation_timeout)`; `container_stop_timeout` cannot
  inflate either deadline.
- Retention reconciliation, quota reconciliation, and retention reap each use one aggregate
  `max(2m, container_stop_timeout)` budget. State rebuild retains its 30-minute
  cap; operation recovery receives the larger of that ordinary phase budget and
  its configured provision/read/cleanup sum; the final identity proof receives
  one Docker read budget. A longer configured stop grace is therefore honored
  for one container without multiplying that grace per lease.
- Ordinary recovery Docker list/inspect boundaries are capped at 30 seconds.
  Cold-start diagnostics share one 30-second context across every failed lease,
  and orphan-network cleanup shares one across the whole network set.
- Post-mutation storage verification receives its own bounded 30-second read
  context, even after effect cancellation or shutdown. This prevents an expired
  effect context from manufacturing a failed identity proof. The original
  effect error is preserved; successful attestation alone never declares the
  effect successful. An independently failed proof still fails closed.

A timeout fails that recovery boundary closed (and preserves exact operation,
maintenance, and close authority) instead of wedging a Docker/CLI boundary.
Local filesystem deadlines are cooperative: one blocking kernel call cannot be
forcibly interrupted, and a very large recursive top-level removal can cross
the nominal budget. Recovery checks cancellation between top-level entries and
phases and makes no further mutations afterward.
These are internal safety bounds, not YAML keys; zero-valued internal fields
select these production defaults.

`recoverState` proceeds in this order:

1. **Classify maintenance WALs lease by lease** -- decode the complete maintenance-intent journal, then re-read and classify each exact row under that lease's command fence. A live actor that owns the same `maintenance_id` remains the serial owner. Otherwise recovery joins the exact source/target Release with a fresh bounded strict Docker inventory; it never relies on one fleet-wide point-in-time snapshot and never reruns Compose. An indeterminate row returns before ordinary projection can reinterpret a mixed source/target cohort. Partial-target cleanup revalidates and removes immutable Docker IDs individually, so an error on a later sibling may leave safe idempotent progress while the WAL remains for the next pass; reusable names are never cleanup authority.
2. **List managed containers** -- filters by `fred.managed=true` label for the ordinary projection.
3. **Stabilize lifecycle authority** -- recovery holds the exclusive side of a backend-local snapshot guard from ordinary managed-container inventory through close/restore durable-authority reads and matching provision/pool publication. Live Deprovision and Restore paths hold the shared side only for authority capture and durable handoffs, not destructive substrate work. Load every close intent before ordinary callback-label or release-cohort validation. Containers owned by a close are deliberately excluded from those exact-cohort checks because teardown may already have removed some or all siblings. Provision validation remains available, but its short accepted-intent-to-projection handoff can wait for the current snapshot publication; Restore admission or final rollback may wait at its corresponding handoff.
4. **Validate ordinary callback and release cohorts** -- every sibling must carry one coherent callback pair. Current releases must carry complete matching typed runtime authority. A complete callback-bearing stack-form v0.13 cohort is CAS-fenced with a distinct `LegacyRuntimeAuthority` that freezes its canonical principal and tokenless pair without manufacturing an operation ID; a pre-stack or callbackless cohort is rejected because provider callback authority cannot be minted safely. Either authority's exact `Items` and resource profiles must match the observed service/SKU/index/domain/image set; a mismatch fails the lease closed. With no survivors, it reconstructs the exact identity, callback route, topology, and conservative allocation as Failed. A still-unbackfilled v0.13 stack row is derived only from a complete, identity-consistent, dense cohort and exact active manifest, then receives the whole-release and runtime-authority backfills before any operation may erase its last container. The first and every subsequent restart, update, or custom-domain replacement stays tokenless; its independent UUIDv4 `maintenance_id` is exact replacement journal/cohort identity, not provider callback authority. Only a later genuine provision or restore rotates callback authority to typed. A pending operation intent owns its own generation transition and resource snapshot and is classified separately during startup.
5. **Group by lease UUID** -- ordinary containers are grouped into provision records. The highest `FailCount` across containers in a lease is used (handles partial re-provisions).
6. **Rebuild close owners** -- a full close publishes a conservative `deprovisioning` projection and full resource reservation solely from its immutable journal snapshot, even with zero survivors. Cleanup-only closes reserve their durable topology without publishing a tenant provision.
7. **Rebuild restore destination authority** -- a source `restoring` finalizer
   supplies exact destination operation ID, items, manifest, profiles, callback
   pair, and allocation. Without
   a matching active Release it remains pre-commit rollback authority and does
   not create a restartable projection. An exact matching Release is committed
   authority: retain the Release, recover zero survivors as a conservative
   Failed destination with its allocation held, and let the later startup
   operation phase transition a matching Pending row to Succeeded. Retain
   the source finalizer as identity until a
   successful plain Restart reaches Ready and reconciliation consumes it, or
   close persists a full intent and takes ownership. Update and custom-domain
   redeploys stay fenced until finalizer consumption.
8. **Detect ready-to-failed transitions** -- if a provision was in-memory as `ready` but Docker shows a container as exited/dead, or if its exact durable cohort diverges, the typed state-machine transition marks it `failed`, increments `FailCount`, and emits the lifecycle callback.
9. **Cold-start FailCount correction** -- provisions recovered as `failed` with no prior in-memory state have their `FailCount` incremented by 1. The label value was written at creation time (before the crash), so the increment accounts for the observed failure.
10. **Preserve in-flight provisions** -- Pending operation state and its resource reservation survive an inventory rebuild. A Pending provision remains excluded from ordinary inference and is settled by the later startup operation phase. Succeeded/Failed operation rows are durable decisions, not in-flight work.
11. **Reset resource accounting** -- allocations are rebuilt atomically from operation-intent, active-release, restore-finalizer, and close snapshots while reservations for every still-tracked lease are preserved. Docker uses durable `disk_mb` or its mutually exclusive pinned scratch allowance; mutable configuration is used only while explicitly upgrading supported v0.13 evidence.
12. **Resume admitted closes** -- after conservative projections and reservations are visible and the recovery guard is released, retry every close under its per-lease command fence after re-reading the durable journal. Transient failures retain the journal and durable execution generation for the next level-triggered pass.
13. **Orphaned network cleanup** -- if `NetworkIsolation` is enabled, removes any managed networks whose tenant has no active provisions and no connected containers.

During `Start`, the next phase lets Pending provision and restore operations
exclusively classify and settle their exact substrate; only afterward may
restore/retention finalizers reconcile, followed by quota backfill,
and retention reaping. Unattributed managed volumes are preserved for explicit
operator attribution; there is no inference-driven cleanup phase or separate preflight
owner that can consume an operation's empty destination. Periodic `recoverState`
retries operations, maintenance, close convergence, and exact late-container
cleanup. Provision and restore intents share one absolute recovery horizon derived
from durable admission time and the configured `provision_timeout`. Exact-empty
or transitional cohorts before that deadline remain Pending and are observed
again by the periodic sweep; they do not block startup. A future admission
timestamp after clock rollback is capped to one fresh observation window.
There is no second `container_start_timeout` recovery algorithm.

A terminal sibling or exhausted horizon enters exact failed-operation cleanup.
An inspection error, pre-effect cancellation, or ordinary removal failure
preserves the intent, reservation, and substrate for retry. An interrupted
restore additionally requires the exact source/destination/callback/topology
proof and typed actor-quiescence capability before rollback. A committed
destination Release always wins and is never rolled back. Failed settlement
atomically records the terminal operation outcome and callback before source
handback; the durable Failed state drives any handback retry.

`fred_docker_backend_operation_intent_recovery_timeout_exhaustions_total{reason="provision_timeout"}`
counts expired provision/restore classifications;
`fred_docker_backend_operation_intent_recovery_cleanup_retries_total{kind}`
counts deferred operation cleanup. Permanent closed/failed receipts also exclude
late substrate from ordinary projection. If those compact receipts cannot account
for a survivor's resources, the pool withholds new capacity and Health returns
unready until a strict later inventory proves absence. The daemon and cleanup
loops continue running. This is distinct from verified storage-identity drift,
which remains a terminal safety failure.

The hold is backend-wide: any operation needing a new or replacement allocation
from this backend's pool is refused, not just work for the late container's lease.
It does not gate other backend instances. Successful close can
replace failed-operation history with a stronger permanent closed receipt; an
existing failed-family hold still remembers the observed callback cohort until
strict absence. This compact observation scope grants no deletion authority and
cannot release another receipt family's hold.

### Durable close finalization

Deprovision never begins destructive work from volatile provision state alone.
It first commits an opaque UUIDv4 close capability as the lease's tagged head in
`callback_lease_mutation_heads`, with an immutable snapshot of:

- lease, backend/storage, tenant, and provider identity;
- ordered items and the complete manifest;
- exact operation/lifecycle callback pair and the close-time retention policy;
- exact per-SKU CPU, memory, durable disk, and scratch profiles used by the lease;
- selected release version plus SHA-256 digest.

Validation bounds aggregate quantities and target count, rejects partial
callback pairs or divergent manifest topology, and verifies the row on every
mutation through an opaque digest-bearing claim. The release fence prevents a
delayed close from deleting a newer deployment.
If release history is already absent, retirement is idempotently complete: the
close row itself carries the bounded cleanup snapshot and prevents any newer
operation from creating a release for that lease.

Close recovery and any retained/reaping record produced from that close use the
captured resource profiles, never the current `sku_profiles`. Resizing or
removing a SKU therefore cannot undercount an already-owned footprint. Retention
rows written by older versions remain readable and fall back to current config;
startup fails closed if such a row references an unavailable SKU.

Close admission and operation preemption are one bbolt transaction. If a
provision/restore intent exists, that transaction enqueues its exact failed
operation callback before removing it and publishing the close. Operation
intent admission checks the close journal, while the deprovisioning actor (or an
absent cleanup-only projection) rejects maintenance. A later worker therefore
cannot start for the same lease while teardown owns it.

The normal completion order is mandatory:

1. Advance the durable execution generation, then run the construction-bound
   container, retention, and volume workflow. Callers cannot choose targets or
   inject a finalizer.
2. Delete release history only if its selected generation still matches the
   stored version/digest fence. An already-absent key is an idempotent retired
   state, including after history-store loss; a changed history is a conflict and
   remains intact. Cleanup authority stays in the close row, not in the release
   key.
3. In one `callbacks.db` transaction, enqueue the terminal lifecycle callback
   and replace the precise close row with a permanent closed-UUID receipt.
   Callbackless cleanup still writes that receipt. Then send a non-blocking,
   exact-lease commit wake to the tracked callback replay loop; the mailbox
   coalesces repeats for that lease without discarding other lease identities.
   The durable row, not the wake, owns delivery.
4. Delete the volatile provision projection only after steps 2 and 3 commit.

A crash at any earlier boundary leaves the close row as the retry owner. The
callback sender may fail after step 3 without reopening teardown: no HTTP runs in
the lease actor or startup recovery, the durable outbox owns delivery, and its
periodic 30-second sweep backs up the immediate wake. There is no attempt-count
terminalization. A plain error or ambiguous effect preserves the exact Started
generation; restart performs read-only strict classification. Only executor-
minted `Destroyed` or `Retained` evidence can cross terminal settlement, while
`Incomplete` evidence can authorize another durable generation.
When a complete Release or exact container cohort supplies tenant/provider
identity, that pair remains in the sealed close authority and every late-
container cleanup must match it. Only a true orphan with no principal witness
uses the weaker sealed authority: exact retired lease UUID plus reserved
`fred.*` managed labels under the attested backend/storage identity. Tenant and
provider are always both present or both absent; a mixed representation fails
decode. Closed receipts never expire, but recovery point-looks up only UUIDs in
the live inventory, so each sweep is O(live substrate), not O(close history).
The log `durable close recovery remains pending` includes the lease UUID and
durable execution generation. Corrupt rows or simultaneous close/operation rows make
callback-store health fail closed. A terminal operation row is history rather
than active mutation authority, so close admission atomically retires it instead
of leaving both rows. Pending but valid close work is reported in logs rather
than an unbounded lease-labeled metric.

Startup never destroys a managed volume merely because no current container or
row names it. Absence from several projections is not destruction authority: a
late Docker effect, incomplete legacy observation, or unavailable journal can
all produce the same view. A valid but unattributed `fred-*` volume is therefore
preserved for explicit operator attribution. Exact operation, close, and
retention finalizers remain the only automatic volume destroyers.

The `fred-` and `fred-retained-` prefixes are reserved storage authority. Do
not place backups, scratch files, symlinks, or non-directory artifacts with
either prefix inside `volume_data_path`: startup deliberately rejects a member
whose exact managed grammar and quota substrate cannot be attested rather than
hiding unaccounted bytes. A v0.13 active Release is conservatively treated as
claiming its whole parsed lease namespace because its backfilled quantity came
from the surviving dense container prefix and cannot prove that a higher
original instance never existed; the mandatory chain preflight resolves that
mismatch. Current typed releases remain exact-name claims.

> **Exact authority, not inferred garbage collection.** A volume's *name* does
> not establish ownership: while a restore is in flight the original lease's
> data wears the new lease's canonical name. Every automatic destroy is reached
> from an exact durable operation, close, or retention finalizer and is checked
> against the live/retention ownership projection. Unknown managed volumes are
> never promoted into another lease and never selected by a global orphan scan.

## Callback Protocol

Callbacks notify Fred of provisioning results.

### Signing

Each callback carries an `X-Fred-Signature` header in the format:

```
t=<unix-timestamp>,sha256=<hex-encoded-hmac>
```

The HMAC-SHA256 is computed over the canonical string
`<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>` using the
configured `CallbackSecret`. Binding the method and URI prevents
cross-endpoint replay of captured signatures; hashing the body keeps the
canonical string binary-safe. See `internal/hmacauth` for the reference
implementation.

### Error Message Sanitization

Callback error messages use hardcoded, deterministic strings and never include container logs or runtime-specific data. This prevents secrets, API keys, or other sensitive data from being permanently recorded on-chain as rejection reasons.

Full diagnostics (exit codes, OOM status, container logs) are available via the HMAC-authenticated `GET /provisions/{lease_uuid}` and `GET /logs/{lease_uuid}` endpoints.

### Payload

```json
{
  "lease_uuid": "...",
  "status": "deprovisioned",
  "backend": "docker",
  "retained": true
}
```

`lease_uuid` and `status` are always present (`status` is one of `success`, `failed`, or `deprovisioned`). `error`, `backend`, and `retained` are all `omitempty`, so an empty/false value is omitted from the JSON entirely (as with `error` in the example above):

- `error` — the failure reason on a `failed` callback; omitted when empty.
- `backend` — the backend name; omitted by pre-upgrade senders.
- `retained` — only meaningful on a `deprovisioned` callback: `true` when the backend actually soft-deleted (retained) the lease's volumes, and omitted (read as `false`) otherwise. Best-effort ground truth; the queryable `/retentions` status is the durable backstop.

### Retry Strategy

- **3 attempts** with backoff delays of 0s, 1s, 5s.
- One **2m15s total delivery deadline** covers all attempts and their backoff;
  attempts do not receive independent timeouts. The HTTP client deliberately has
  no competing client-wide timeout, so the request context is the sole deadline.
- Fred's callback application budget is 2m. A fresh first attempt therefore
  normally leaves 15s for Fred to serialize a retryable 503; later attempts use
  only the shared deadline's remainder.
- Retries abort immediately if the backend is shutting down (`stopCtx` is canceled).
- A 2xx response is considered success; any other status triggers a retry.
- Operation, maintenance, and lifecycle completion paths atomically enqueue the
  durable result and send a non-blocking commit wake naming the exact affected
  lease. The mailbox coalesces repeats for one lease without losing other lease
  identities, and a stronger handoff transfers work after a canceled drainer
  releases ownership. Only the tracked loop performs HTTP retry I/O, so a slow
  callback cannot extend a lease actor, API handler, or startup-recovery critical
  section. The 30s sweep discovers pre-start rows and retries dormant failed heads.
- Semantic publication is construction-bound to the exact callback and release
  journals. Operation and maintenance status comes from sealed terminal proofs;
  substrate classification receives only a release-scoped generation proof.
  An autonomous runtime failure can publish only after the exact journal pair
  upgrades that proof to a phase-qualified observation permit while no Pending,
  maintenance, close, or closed head exists. Publication re-attests both facts.
  Callback URLs, backend lineage, status, and retained state are therefore not
  caller-selected. The callback sender owns only authenticated transport, replay,
  and precise removal after 2xx.
- Exhaustion or cancellation keeps the same durable FIFO head for a later replay.
  One slow callback can occupy one replay worker and its per-lease FIFO lock for
  up to 2m15s, but it cannot block another lease or backend node.

This reverse callback budget is independent of providerd's
`backends[].timeout`. That setting bounds Fred-to-backend requests; changing it
does not shorten or extend backend-to-Fred callback delivery.

## HTTP API

All authenticated endpoints require an `X-Fred-Signature` HMAC-SHA256 header (see [Signing](#signing)). Request bodies are limited to 2 MiB by default (`DefaultMaxRequestBodySize`), configurable via `max_request_body_size` (env `DOCKER_BACKEND_MAX_REQUEST_BODY_SIZE`). All JSON responses use `Content-Type: application/json`. Errors return `{"error": "message"}`.

### `POST /provision` (authenticated)

Starts async container provisioning. Pre-flight validation (SKU, manifest, image allowlist, resources) is synchronous; the actual container lifecycle runs in a background goroutine with results delivered via callback.

**Request (single-container):**

```json
{
  "lease_uuid": "abc-123",
  "tenant": "manifest1...",
  "provider_uuid": "prov-1",
  "items": [
    { "sku": "docker-small", "quantity": 2 }
  ],
  "callback_url": "https://fred-host/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
  "lifecycle_callback_url": "https://fred-host/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
  "payload": "<base64-encoded manifest JSON>"
}
```

**Request (stack):**

```json
{
  "lease_uuid": "abc-123",
  "tenant": "manifest1...",
  "provider_uuid": "prov-1",
  "items": [
    { "sku": "docker-small", "quantity": 1, "service_name": "web" },
    { "sku": "docker-medium", "quantity": 1, "service_name": "db" }
  ],
  "callback_url": "https://fred-host/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
  "lifecycle_callback_url": "https://fred-host/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
  "payload": "<base64-encoded stack manifest JSON>"
}
```

**Response (`202 Accepted`):**

```json
{
  "provision_id": "abc-123"
}
```

**Errors:** `400` (validation), `409` (already provisioned), `503` (insufficient resources). A capacity refusal carries `{"error":"...","code":"insufficient_resources"}` so, under the configured transport's trust boundary, providerd can classify the matching write-ahead attempt as clearable; an uncoded 503 remains ambiguous for compatibility. Backend responses are not HMAC-authenticated, so the code is a protocol-contract signal rather than cryptographic proof of authorship.

### `POST /deprovision` (authenticated)

Removes all containers and managed volumes for a lease and releases resources.
Admission is write-ahead: the durable close intent commits before teardown and
is an idempotency/finalizer owner across retries and restarts. A truly
nonexistent lease with no projection, release, or substrate authority returns
success; an absent projection with a fenced release enters cleanup-only recovery
instead of treating container absence as proof that cleanup finished.

**Request:**

```json
{
  "lease_uuid": "abc-123"
}
```

**Response (`200`):**

```json
{
  "status": "ok"
}
```

### `GET /info/{lease_uuid}` (authenticated)

Returns connection details for a running lease. Only available when the provision status is `ready` — returns `404` otherwise.

**Response (`200`, single-container):**

```json
{
  "host": "192.168.1.100",
  "instances": [
    {
      "instance_index": 0,
      "container_id": "abcdefghijkl",
      "image": "nginx:latest",
      "status": "running",
      "ports": {
        "80/tcp": { "host_ip": "0.0.0.0", "host_port": "32768" }
      }
    }
  ]
}
```

**Response (`200`, stack):**

For stack provisions, instances are grouped by service name under a `"services"` map. Each service value is an object with an `"instances"` key:

```json
{
  "host": "192.168.1.100",
  "services": {
    "web": {
      "instances": [
        {
          "instance_index": 0,
          "container_id": "abcdefghijkl",
          "image": "ghcr.io/myorg/webapp:v2.1.0",
          "status": "running",
          "ports": {
            "8080/tcp": { "host_ip": "0.0.0.0", "host_port": "32768" }
          }
        }
      ]
    },
    "db": {
      "instances": [
        {
          "instance_index": 0,
          "container_id": "mnopqrstuvwx",
          "image": "postgres:16",
          "status": "running",
          "ports": {
            "5432/tcp": { "host_ip": "0.0.0.0", "host_port": "32769" }
          }
        }
      ]
    }
  }
}
```

### `GET /logs/{lease_uuid}` (authenticated)

Returns container stdout/stderr using `"serviceName/instanceIndex"` keys
(e.g., `"web/0"`, `"db/0"`). Works for any provision status (provisioning,
ready, or failed). After successful compensation, captured replacement logs
appear alongside live logs under `failed/<service>/<instance>` keys. Logs share
a 32 MiB aggregate content budget, with bounded marker and encoding overhead.

**Query parameters:** `tail` — number of lines (default 100, max 10000).

**Response (`200`, stack):**

```json
{
  "web/0": "2025-01-15T10:00:00Z Listening on :8080\n...",
  "db/0": "2025-01-15T10:00:00Z database system is ready to accept connections\n..."
}
```

If log retrieval fails for a specific instance, its value is the placeholder `<log unavailable>` (the underlying error is logged operator-side, never returned).

### `GET /provisions/{lease_uuid}` (authenticated)

Returns a single provision record. This is the primary endpoint for retrieving full failure diagnostics after a sanitized callback.

For newly persisted failure diagnostics, `lifecycle_generation` remains the
same non-secret observation before and after this endpoint falls back to
`diagnostics.db`. Pre-upgrade rows omit the field and remain readable as
unknown. This historical value is read-model metadata only: the recreateable
diagnostics store carries no lifecycle authority, diagnostics-only rows never
enter `GET /provisions` inventory, and no settlement or repair decision may
rely on the singular fallback.

**Response (`200`):**

```json
{
  "lease_uuid": "abc-123",
  "provider_uuid": "prov-1",
  "status": "failed",
  "created_at": "2025-01-15T10:00:00Z",
  "fail_count": 2,
  "reason": "ContainerExited",
  "message": "container exited unexpectedly"
}
```

`status` is one of: `provisioning`, `ready`, `failing`, `failed`, `unknown`, `restarting`, `updating`, `deprovisioning`. `failing` marks the brief window between container-death detection and the Failed callback being emitted; a concurrent Deprovision arriving in this window transitions the lease straight to `deprovisioning` without ever reaching `failed`, preventing a stale Failed callback. On failure, `reason` (a stable machine code, e.g. `ContainerExited`) and `message` (a curated human summary) are present; the verbose diagnostics (exit codes, OOM status, container logs) stay operator-side in the diagnostics store and structured logs — they are never included in this response (ENG-508).

### `GET /provisions` (authenticated)

Returns provision records. `GET /provisions` is keyset-paginated. Query params: `limit` (max page size) and `continue` (a lease UUID — the `continue` cursor returned by the previous page). The JSON response carries a top-level `continue` field set to the last record's lease UUID, omitted once the list is exhausted. An invalid `limit` or a non-UUID `continue` returns 400, as does a `continue` cursor supplied without a positive `limit`. A `limit` above the server maximum (5000) is coerced down to it rather than rejected. With no params it returns the full list unpaginated (back-compat). One or more `lease_uuid` query params return just those records. (ENG-380)

**Response (`200`):**

```json
{
  "provisions": [
    {
      "lease_uuid": "abc-123",
      "provider_uuid": "prov-1",
      "status": "ready",
      "created_at": "2025-01-15T10:00:00Z",
      "fail_count": 0
    },
    {
      "lease_uuid": "def-456",
      "provider_uuid": "prov-1",
      "status": "failed",
      "created_at": "2025-01-15T10:05:00Z",
      "fail_count": 3,
      "reason": "ContainerExited",
      "message": "container exited unexpectedly"
    }
  ],
  "continue": "def-456"
}
```

The `continue` field is present only on a full page with more records remaining; it is omitted once the list is exhausted.

### `POST /restart` (authenticated)

Restarts a lease's containers in place (same image, same configuration). Async — the result is delivered via callback. Returns `202` (`{"status": "restarting"}`), `404` if not provisioned, `409` for an invalid state.

### `POST /update` (authenticated)

Re-deploys a lease with a new manifest (image/config change). The `payload` field carries the new base64-encoded manifest. Async — result via callback. On failure the previous manifest is rolled back. Returns `202` (`{"status": "updating"}`), `400` (validation), `404`, `409`.

### `POST /restore` (authenticated)

Restores a closed lease's retained volumes into a fresh lease. Body carries `from_lease_uuid` (the original closed lease), the exact operation `callback_url`, and the typed `lifecycle_callback_url`. Async — result via callback. Returns `202` (`{"status": "restoring"}`). `422` is **overloaded**: a **bare** `422` (no `code`) means no retained data exists (`ErrNotRetained`), while `422` with body `{"code":"demote_exceeds_tier"}` means the retained data exceeds the requested smaller SKU tier (`ErrDemoteDataExceedsTier`, see [Restore flow](#restore-flow)). Also `409` for invalid state / already provisioned, `400` (validation), and `503` with `code="insufficient_resources"` for a synchronous capacity refusal. Under the configured transport trust boundary the coded response makes the exact attempt clearable; it is not an HMAC-authenticated backend response. See [Soft-delete & Restore](#soft-delete--restore).

The completion URL must carry a canonical UUIDv4 `operation_id`; a tokenless
request cannot create an operation intent or reserve the retained source.
Omitting `lifecycle_callback_url` derives the matching typed route, as for
provision. Existing legacy retained data does not relax new-target admission.

### `GET /retentions` (authenticated)

Lists this backend's retained (soft-deleted) leases. Used by the reconciler to route restores to the node physically holding each lease's retained volumes (ENG-333).

`GET /retentions` is keyset-paginated, mirroring [`GET /provisions`](#get-provisions-authenticated): it accepts `limit` (max page size) and `continue` (a lease UUID — the cursor returned by the previous page) query params and returns a top-level `continue` field set to the last record's lease UUID, omitted once the list is exhausted. Fred's client pages at `RetentionsPageLimit` (default 1000), fail-closing each page body at 1 MiB (ENG-451). With no params it returns the full list unpaginated (back-compat).

**Response (`200`):** `{"retentions": [ ... ], "continue": "<lease-uuid>"}` (`retentions` serialized as `[]` when empty; `continue` omitted once the list is exhausted).

### `GET /releases/{lease_uuid}` (authenticated)

Returns the persisted release (deployment) history for a lease. Entries are
retained for up to `releases_max_age` (default 90 days), but disposable audit
rows may be compacted sooner to keep the encoded per-lease history within
32 MiB. The client accepts up to 48 MiB because response projection can add a
default failure reason that was absent from the stored representation.

### `POST /reconcile_custom_domain` (authenticated)

Reconciles a lease's custom-domain ingress labels to match the supplied items. Body carries `lease_uuid` and `items`. Returns `204 No Content`; `404` if not provisioned, `409` for an invalid state.

### `GET /health` (unauthenticated)

Docker daemon reachability check. Also probes the callback, diagnostics, release, and retention bbolt stores — a locked, corrupt, or read-only store surfaces as unhealthy instead of the backend reporting healthy while soft-delete/restore silently fail (ENG-448).

Late containers covered by permanent closed/failed receipts also make this
backend unready while their resource footprint cannot be accounted. The pool
reports zero available capacity and withholds routable load statistics, but the
daemon remains running and retries exact cleanup. This clears only after strict
inventory proves absence. The diagnostic `unaccounted_managed_volumes` gauge
does not itself gate health or
authorize cleanup; a sustained value calls for operator attribution.

**Response (`200`):**

```json
{
  "status": "healthy"
}
```

Returns `503` if the Docker daemon is unreachable **or** any of those stores is unhealthy.

### `GET /stats` (unauthenticated)

Resource pool usage.

Returns `503` while an accounting hold makes the known resource ledger
incomplete. The in-process load-statistics path refuses the same snapshot, so
multi-backend routing can select a healthy peer with usable statistics. Known
allocations remain visible through `/metrics`; they are not inflated to model
an unknown footprint. If no candidate has usable statistics, the router's
existing fallback still applies, but the held pool refuses new allocations.

Disk values are effective physical admission values. A stateful instance
contributes its durable `disk_mb`; a diskless instance contributes its immutable
`scratch_disk_mb` pinned from `container_tmpfs_size_mb`, even if no managed
writable-path directory was ultimately needed. Retained caps remain durable-data
policy; an exact scratch-volume name retained conservatively remains in physical
pool accounting until it is destroyed.

**Response (`200`):**

```json
{
  "total_cpu_cores": 8.0,
  "total_memory_mb": 16384,
  "total_disk_mb": 102400,
  "allocated_cpu_cores": 2.5,
  "allocated_memory_mb": 4096,
  "allocated_disk_mb": 10240,
  "available_cpu_cores": 5.5,
  "available_memory_mb": 12288,
  "available_disk_mb": 92160,
  "active_containers": 5
}
```

### `GET /metrics` (unauthenticated)

Prometheus metrics in exposition format. Served by `promhttp.Handler()`.

## Resource Pool

The resource pool tracks CPU, memory, and effective physical disk allocations.

- **Allocation IDs** are per-instance: `<lease-uuid>-<instance-index>` for single-container leases (e.g., `abc123-0`, `abc123-1`), or `<lease-uuid>-<service-name>-<instance-index>` for stack leases (e.g., `abc123-web-0`, `abc123-db-0`).
- **TryAllocate** atomically checks capacity and reserves resources for a SKU. Durable workflows use the exact profile already captured by their intent/release/restore-finalizer/close snapshot, so accounting and substrate limits cannot resolve different values. Docker effective disk is `disk_mb + scratch_disk_mb`, with validation making the two mutually exclusive. On insufficient resources, it returns an error and the caller rolls back any partial allocations.
- **Release** is idempotent -- releasing a non-existent allocation is a no-op.
- **Stats** returns total, allocated, and available CPU/memory/effective disk.
- **Reset** atomically validates and replaces all allocations from durable intent, active-release, and close snapshots during recovery. Invalid or overflowing input returns an error without changing the previous projection.

## Tenant Network Isolation

When `network_isolation` is enabled (default), each tenant's containers are placed in a dedicated Docker bridge network. This provides:

- **Same-tenant communication**: containers on the same tenant bridge can reach each other directly.
- **Cross-tenant isolation**: Docker's `DOCKER-ISOLATION` iptables chains DROP forwarded traffic between different bridge networks. Containers from different tenants cannot communicate directly.
- **Outbound internet**: containers can reach the internet (required for port bindings).
- **Port bindings**: inbound traffic to published ports works normally. Cross-tenant communication is only possible through public-facing endpoints (published ports on the host).

> **Prerequisite**: Docker must have iptables enabled (the default). If the daemon runs with `--iptables=false`, cross-tenant isolation is lost. Fred logs daemon warnings at startup to help detect this.

> **Why not `Internal: true`?** Docker's `Internal` network flag prevents port publishing entirely ([moby#36174](https://github.com/moby/moby/issues/36174)), which would make tenant services unreachable.

### Network lifecycle

- **Naming**: `fred-tenant-<hex(sha256(tenant)[:8])>` -- first 8 bytes of the SHA-256 hash, hex-encoded to 16 characters. Deterministic, derived from the tenant address.
- **Creation**: `EnsureTenantNetwork` creates the network on first use, or returns the existing one.
- **Removal**: `RemoveTenantNetworkIfEmpty` removes the network when no containers are connected. Called during deprovision.
- **Orphan cleanup**: during state recovery, managed networks with no active provisions and no connected containers are removed.
- Networks carry `fred.managed=true` and `fred.tenant` labels.

## Container Labels

All managed containers and networks carry labels in the `fred.*` namespace.

| Label | Value | Description |
|---|---|---|
| `fred.managed` | `"true"` | Marks the container/network as managed by Fred |
| `fred.lease_uuid` | lease UUID | Associates the container with a lease |
| `fred.tenant` | tenant address | Tenant that owns the container/network |
| `fred.provider_uuid` | provider UUID | Provider that fulfills the lease |
| `fred.sku` | SKU identifier | SKU profile used for resource limits |
| `fred.created_at` | RFC 3339 timestamp | When the container was created |
| `fred.instance_index` | integer string | 0-based index within a multi-unit lease |
| `fred.fail_count` | integer string | Number of provision failures for this lease at creation time |
| `fred.callback_url` | URL string | Exact completion URL with an operation capability for new provision/restore; inherited v0.13 lineage remains tokenless |
| `fred.lifecycle_callback_url` | URL string | Paired endpoint for later maintenance, runtime-failure, and deprovision observations; typed for new provision/restore, tokenless for inherited v0.13 lineage; persisted across backend restarts |
| `fred.service_name` | service name string | Service name within a stack (stack provisions only) |
| `fred.backend_name` | backend name string | Name of the backend managing the container; set on every managed container |
| `fred.fqdn` | FQDN string | Assigned ingress FQDN; set on the ingress / custom-domain path |
| `fred.custom_domain` | domain string | Tenant custom domain; set on the custom-domain path |
| `fred.image_reference` | image reference string | Original manifest image reference, preserved for release comparisons while execution uses an immutable image ID |
| `fred.image_id` | `sha256:` image ID | Binds `fred.image_reference` to Docker's actual image ID and the container's configured image; partial or inconsistent bindings fail inventory validation |

Manifest and image labels may not use the `fred.*`, `traefik.*`, or
`com.docker.compose.*` namespaces, matched case-insensitively. Image metadata is
checked before creating workloads or inspection helpers, and execution uses
the inspected immutable image ID. This prevents inherited labels from becoming
ingress or container-lifecycle instructions. Images built with Compose may carry
reserved labels automatically; rebuild them without orchestration metadata
(for example, with `docker build`) before provisioning or replacing workloads.
Existing containers are not rewritten by this admission check.

## Bandwidth Limiting

Network bandwidth limiting is an operational concern handled outside of the docker-backend process. Operators can use Linux `tc` (traffic control) to rate-limit container network traffic on the host.

### Identifying container interfaces

Each Docker container gets a veth pair. The host-side interface can be found by inspecting the container's network namespace:

```bash
# Get the container's PID
PID=$(docker inspect --format '{{.State.Pid}}' <container_id>)

# Get the veth peer index from inside the container's namespace
PEER_IDX=$(nsenter -t $PID -n ip link show eth0 | grep -oP '(?<=@if)\d+')

# Find the host-side veth interface by index
HOST_VETH=$(ip link | grep "^${PEER_IDX}:" | awk '{print $2}' | tr -d ':@')
```

### Applying rate limits with tc

Use `tc` to set ingress and egress limits on the host-side veth interface:

```bash
# Egress (container → network): limit to 10 Mbit/s with 32KB burst
tc qdisc add dev $HOST_VETH root tbf rate 10mbit burst 32kbit latency 50ms

# Ingress (network → container): use an IFB (intermediate functional block) device
modprobe ifb
ip link set dev ifb0 up
tc qdisc add dev $HOST_VETH ingress
tc filter add dev $HOST_VETH parent ffff: protocol ip u32 match u32 0 0 \
    action mirred egress redirect dev ifb0
tc qdisc add dev ifb0 root tbf rate 10mbit burst 32kbit latency 50ms
```

### Automation

For production use, integrate `tc` rules into a container lifecycle hook or a script triggered by Docker events (`docker events --filter event=start`). The docker-backend does not manage bandwidth limits directly to keep the provisioning path simple and avoid requiring `CAP_NET_ADMIN`.
