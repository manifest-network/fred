# Docker Backend

The Docker backend provisions ephemeral containers for tenant workloads. It receives provision requests from Fred, manages the full container lifecycle (pull, create, start, verify, deprovision), enforces SKU-based resource limits, and reports results via HMAC-signed callbacks.

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

### Resources

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| TotalCPUCores | `total_cpu_cores` | float64 | `8.0` | Total CPU cores in the resource pool |
| TotalMemoryMB | `total_memory_mb` | int64 | `16384` | Total memory available (MB) |
| TotalDiskMB | `total_disk_mb` | int64 | `102400` | Total disk space available (MB) |

### SKU Management

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| SKUMapping | `sku_mapping` | map[string]string | *(empty)* | Maps on-chain SKU UUIDs to profile names |
| SKUProfiles | `sku_profiles` | map[string]SKUProfile | *(see below)* | Maps profile names to resource limits |

Default SKU profiles:

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
| CallbackSecret | `callback_secret` | string | *(required, min 32 chars)* | HMAC-SHA256 secret for signing callbacks |
| CallbackInsecureSkipVerify | `callback_insecure_skip_verify` | bool | `false` | Skip TLS verification for callbacks (dev only) |
| CallbackDBPath | `callback_db_path` | string | `"callbacks.db"` | Path to bbolt database for persisting pending callbacks |
| CallbackMaxAge | `callback_max_age` | duration | `24h` | Maximum age of persisted callback entries before cleanup |

### Tenant Quotas

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| TenantQuota | `tenant_quota` | object | *(none)* | Per-tenant resource limits (optional) |
| TenantQuota.MaxCPUCores | `tenant_quota.max_cpu_cores` | float64 | - | Maximum CPU cores per tenant |
| TenantQuota.MaxMemoryMB | `tenant_quota.max_memory_mb` | int64 | - | Maximum memory per tenant (MB) |
| TenantQuota.MaxDiskMB | `tenant_quota.max_disk_mb` | int64 | - | Maximum disk per tenant (MB) |

When `tenant_quota` is configured, no single tenant can consume more than the specified limits, even if the resource pool has capacity available. Quota values cannot exceed the total pool capacity.

### Timeouts

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| ImagePullTimeout | `image_pull_timeout` | duration | `5m` | Timeout for pulling images |
| ContainerCreateTimeout | `container_create_timeout` | duration | `30s` | Timeout for creating containers |
| ContainerStartTimeout | `container_start_timeout` | duration | `30s` | Timeout for starting containers |
| ProvisionTimeout | `provision_timeout` | duration | `10m` | Maximum time for the entire provisioning operation |
| ReconcileInterval | `reconcile_interval` | duration | `5m` | How often to reconcile state with Docker |
| StartupVerifyDuration | `startup_verify_duration` | duration | `5s` | Grace period after start before verifying containers are still running |

### Container Hardening

| Field | YAML Key | Type | Default | Description |
|---|---|---|---|---|
| NetworkIsolation | `network_isolation` | *bool | `true` | Per-tenant Docker network isolation |
| ContainerReadonlyRootfs | `container_readonly_rootfs` | *bool | `true` | Read-only root filesystem |
| ContainerPidsLimit | `container_pids_limit` | *int64 | `256` | Maximum PIDs per container |
| ContainerTmpfsSizeMB | `container_tmpfs_size_mb` | int | `64` | Tmpfs size (MB) for `/tmp` and `/run` when readonly rootfs is enabled |
| ContainerDiskQuota | `container_disk_quota` | *bool | `true` | Enable Docker overlay2 storage driver disk quotas |

#### Disk Quota Requirements

The `container_disk_quota` option enables per-container disk quotas using Docker's `--storage-opt size=` feature. **This requires specific filesystem configuration:**

- Docker must use the `overlay2` storage driver
- The underlying filesystem must be **XFS**
- XFS must be mounted with the `pquota` (project quota) option

To check if your system supports disk quotas:

```bash
# Check storage driver (should show "overlay2")
docker info | grep "Storage Driver"

# Check if /var/lib/docker is on XFS with pquota
mount | grep "$(df /var/lib/docker --output=source | tail -1)"
# Should include "pquota" in mount options
```

If your system doesn't meet these requirements, you'll see errors like:

```
--storage-opt is supported only for overlay over xfs with 'pquota' mount option
```

**To disable disk quotas**, set `container_disk_quota: false` in your configuration. Containers will still have CPU and memory limits enforced via cgroups, but won't have storage limits at the Docker level.

## Provisioning Lifecycle

1. **Synchronous validation** -- the `Provision` method validates the request before returning:
   - Checks for duplicate lease (returns `ErrAlreadyProvisioned` unless existing provision is failed)
   - Resolves all SKUs to profiles via `SKUMapping` + `SKUProfiles`
   - Parses the JSON manifest and validates image, ports, labels, and health check
   - Validates the image against `AllowedRegistries`
   - Allocates resources for all instances from the resource pool (rolls back on failure)

2. **Asynchronous provisioning** -- runs in a goroutine tracked by a `WaitGroup`:
   - Pulls the image (once, shared across all containers in the lease)
   - Creates/ensures the per-tenant network (if `NetworkIsolation` is enabled)
   - For each item in the lease (supports multi-SKU), for each unit (supports multi-unit):
     - Creates a container with the appropriate SKU profile, hardening settings, and labels
     - Starts the container
   - Verifies startup (see [Startup Verification](#startup-verification) for the two paths)

3. **Callback** -- on success or failure, sends an HMAC-signed callback to the URL provided in the provision request.

Multi-unit leases create multiple containers from the same manifest. Multi-SKU leases create containers with different resource profiles per SKU. Instance indices are 0-based across all items.

The entire async operation is bounded by `ProvisionTimeout` and is canceled on backend shutdown.

## Container Hardening

Every container is created with the following security measures:

| Feature | Implementation | Notes |
|---|---|---|
| Drop all capabilities | `CapDrop: ["ALL"]` | No Linux capabilities granted |
| No new privileges | `SecurityOpt: ["no-new-privileges:true"]` | Prevents privilege escalation via setuid/setgid |
| Read-only root filesystem | `ReadonlyRootfs: true` | Configurable via `container_readonly_rootfs` |
| Tmpfs for `/tmp` and `/run` | `Tmpfs: {"/tmp": "size=64M", "/run": "size=64M"}` | Only when readonly rootfs is enabled; size from `container_tmpfs_size_mb`. Tenants may request up to 4 additional tmpfs mounts via manifest, for a maximum of 6 total (384MB at default size). |
| PID limit | `PidsLimit: 256` | Configurable via `container_pids_limit` |
| Memory (no swap) | `MemorySwap == Memory` | Prevents swap usage entirely |
| Restart policy disabled | `RestartPolicyDisabled` | Failed containers stay dead for crash detection |
| Network isolation | Per-tenant internal bridge | Configurable via `network_isolation` |

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
2. Old containers are removed and their resource allocations are released.
3. A new provision record is created with `FailCount` preserved.
4. The full provisioning flow runs again (image pull, container create/start, startup verification).
5. On failure, `FailCount` is incremented. The `FailCount` is also persisted in the `fred.fail_count` container label.

## State Recovery

On startup, at each `ReconcileInterval`, and on every reconciler cycle (via `RefreshState`), `recoverState` rebuilds in-memory state from Docker:

1. **List managed containers** -- filters by `fred.managed=true` label.
2. **Group by lease UUID** -- containers are grouped into provision records. The highest `FailCount` across containers in a lease is used (handles partial re-provisions).
3. **Detect ready-to-failed transitions** -- if a provision was in-memory as `ready` but Docker shows the container as exited/dead, the provision is marked `failed`, its `FailCount` is incremented, and a failure callback is sent.
4. **Cold-start FailCount correction** -- provisions recovered as `failed` with no prior in-memory state have their `FailCount` incremented by 1. The label value was written at creation time (before the crash), so the increment accounts for the observed failure.
5. **Preserve in-flight provisions** -- provisions with `status=provisioning` that have no containers yet are kept to avoid dropping active async work.
6. **Reset resource pool** -- `pool.Reset()` clears all allocations and rebuilds them from the recovered containers' SKU profiles.
7. **Orphaned network cleanup** -- if `NetworkIsolation` is enabled, removes any managed networks whose tenant has no active provisions and no connected containers.

## Callback Protocol

Callbacks notify Fred of provisioning results.

### Signing

Each callback carries an `X-Fred-Signature` header in the format:

```
t=<unix-timestamp>,sha256=<hex-encoded-hmac>
```

The HMAC-SHA256 is computed over `<timestamp>.<body>` using the configured `CallbackSecret`.

### Payload

```json
{
  "lease_uuid": "...",
  "status": "success" | "failed",
  "error": ""
}
```

### Retry Strategy

- **3 attempts** with backoff delays of 0s, 1s, 5s.
- Each attempt has a 10s HTTP timeout.
- Retries abort immediately if the backend is shutting down (`stopCtx` is canceled).
- A 2xx response is considered success; any other status triggers a retry.

## Resource Pool

The resource pool tracks CPU, memory, and disk allocations.

- **Allocation IDs** are per-instance: `<lease-uuid>-<instance-index>` (e.g., `abc123-0`, `abc123-1`).
- **TryAllocate** atomically checks capacity and reserves resources for a SKU. On insufficient resources, returns an error and the caller rolls back any partial allocations.
- **Release** is idempotent -- releasing a non-existent allocation is a no-op.
- **Stats** returns total, allocated, and available CPU/memory/disk.
- **Reset** clears all allocations and rebuilds from a provided list. Used during state recovery to synchronize with Docker's actual state.

## Tenant Network Isolation

When `network_isolation` is enabled (default), each tenant's containers are placed in a dedicated Docker bridge network.

- **Naming**: `fred-tenant-<hex(sha256(tenant)[:8])>` -- first 8 bytes of the SHA-256 hash, hex-encoded to 16 characters. Deterministic, derived from the tenant address.
- **Network type**: internal bridge (`Internal: true`), no external connectivity.
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

User-provided labels in the manifest are also applied, but may not use the `fred.*` prefix.

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
