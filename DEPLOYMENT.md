# Deployment Guide

This document covers production deployment of Fred: host requirements, filesystem setup for stateful workloads, process management, TLS topology, multi-host setups, backup/restore, and upgrade procedures.

For day-to-day operation see [OPERATIONS.md](OPERATIONS.md). For the configuration reference see [README.md](README.md#configuration). Ansible roles and Grafana dashboards live in the `manifest-deploy` repository.

---

## Components and topology

A production deployment has three independently runnable processes:

| Binary | Where it runs | What it does |
|---|---|---|
| `providerd` | Provider control host (1) | Subscribes to chain events, owns ADR-036 / HMAC auth, calls backends, batches lease acknowledgments |
| `docker-backend` | Each Docker host (1..N) | Provisions containers, manages volumes, sends signed callbacks |
| `k3s-backend` | Experimental — not for production | Kubernetes backend scaffold (ENG-133): boots and serves the backend contract but provisions nothing (every provision is reported as failed). Full K8s provisioning lands in ENG-134+. |
| `manifestd` | Chain node (external) | Cosmos SDK chain that emits lease events |

Common topology:

```
                    Manifest Chain (external)
                            │
                            │ gRPC / WebSocket
                            ▼
                  ┌──────────────────────┐
                  │      providerd       │ ← TLS-terminated tenant API
                  │  (1 instance per     │   :8080
                  │   provider)          │
                  └──────┬───────────────┘
                         │ HTTPS + HMAC
              ┌──────────┼──────────────┐
              ▼          ▼              ▼
       ┌────────────┐  ┌────────────┐  ┌────────────┐
       │ docker-    │  │ docker-    │  │ docker-    │
       │ backend-1  │  │ backend-2  │  │ backend-N  │
       │            │  │            │  │            │
       │ + Docker   │  │ + Docker   │  │ + Docker   │
       │ + Traefik  │  │ + Traefik  │  │ + Traefik  │
       └────────────┘  └────────────┘  └────────────┘
```

Each docker-backend host typically also runs Traefik (see [Ingress](#ingress-via-traefik)).

---

## Host requirements

### `providerd` host

| Requirement | Recommendation |
|---|---|
| OS | Linux (any distribution); systemd recommended |
| CPU / RAM | 2 cores, 2 GB RAM is plenty for thousands of leases |
| Disk | A few hundred MB for bbolt files; SSD recommended |
| Network | Reachable from each docker-backend (HMAC callbacks); reachable from tenants (TLS-terminated) |
| Outbound | gRPC + WebSocket to the chain; HTTPS to each docker-backend |

### `docker-backend` host

| Requirement | Recommendation |
|---|---|
| OS | Linux. cgroup v2 strongly recommended — under cgroup v2, tmpfs memory is counted against the container's memory limit; under v1 it is not, which makes the per-container memory budget less precise |
| Docker | iptables must be enabled (the default). `--iptables=false` disables cross-tenant network isolation; the docker-backend logs a daemon-warning at startup if it detects this |
| CPU / RAM | Sized for the SKU pool you advertise; budget 10–20% overhead for the daemon |
| Disk | Image cache + per-tenant volumes (see [Stateful workloads](#stateful-workloads-disk_mb--0-skus)) |
| Network | Reachable from `providerd`; outbound reachability to image registries |

---

## Filesystem setup

### Stateless workloads (`disk_mb: 0` SKUs only)

No managed-volume filesystem is required. Containers run with read-only rootfs
and tmpfs at `/tmp` and `/run`; image `VOLUME` paths are also overridden with
tmpfs to prevent accidental anonymous volumes.

Docker nevertheless reserves one conservative physical scratch allowance per
diskless instance. Its size is the positive `container_tmpfs_size_mb` value
frozen when that generation is admitted (64 MB by default), and it counts
against `total_disk_mb`, tenant disk quota, `/stats`, and recovery even if image
inspection ultimately finds no writable path. This is accounting authority, not
durable storage or retention entitlement. If `volume_data_path` is configured,
auto-detected non-`VOLUME` writable paths may use a quota-enforced host scratch
directory; if it is omitted, creation/seeding is best-effort and the workload
still starts without that bind. `/tmp`, `/run`, image `VOLUME` overrides, and
tenant-declared tmpfs remain memory-backed.

### Stateful workloads (`disk_mb > 0` SKUs)

The docker-backend places each container's data on a quota-enforced host directory. Three filesystems are implemented, but **`xfs` is the only backend validated and used in production.** All mainnet and Morpheus backends run XFS with `pquota`, and per-volume disk *and* inode (`ihard`) quotas are exercised only on XFS. **Use `xfs` for production deployments** (see below). The `btrfs` and `zfs` backends are implemented but **untested and not used in any deployment** — treat them as experimental.

#### xfs (good for large fleets)

**This is the recommended, production-validated backend.**

```bash
mkfs.xfs /dev/nvme1n1
mkdir -p /var/lib/fred/volumes
# pquota is required for project quotas
mount -o pquota /dev/nvme1n1 /var/lib/fred/volumes

# Persist
echo "/dev/nvme1n1 /var/lib/fred/volumes xfs pquota 0 0" >> /etc/fstab

# Install xfs_quota
apt-get install -y xfsprogs   # Debian/Ubuntu
dnf install -y xfsprogs        # Fedora/RHEL
```

Each container gets a directory with an xfs project quota.

> **The docker-backend must run with `CAP_SYS_ADMIN` to enforce XFS project
> quotas, and additionally with `CAP_FOWNER` for the startup quota backfill
> (see below).** Setting the block limit (`xfs_quota limit -p`,
> i.e. `quotactl(Q_XSETQLIM)`) is privileged and needs `CAP_SYS_ADMIN`; the
> `report` read is not, so a missing capability is invisible until the first
> stateful provision fails with
> `xfs_quota: cannot set limits: Operation not permitted`. The backend
> **fails fast at startup** if it lacks `CAP_SYS_ADMIN` rather than running with
> silently-unenforced per-volume disk caps. See the systemd section below for how
> to grant it. (`xfs_quota` requires a real mount point, but the backend resolves
> the XFS mount that *contains* `volume_data_path` automatically — so
> `volume_data_path` may be the mount itself, as above, or a subdirectory of it,
> e.g. `/data/fred/volumes` under a `/data` mount.)
>
> **`CAP_FOWNER` is additionally required for the startup quota backfill.**
> Tagging a directory with its project ID (`xfs_quota project -s`, i.e.
> `FS_IOC_FSSETXATTR`) is permitted for the inode's owner, so a *fresh* provision
> — which tags the still-empty, daemon-owned volume before chown'ing its subdirs
> to the tenant — needs only `CAP_SYS_ADMIN`. The startup backfill that heals
> volumes provisioned before the capability was granted, however, re-tags
> directories that have already been chown'd to the tenant UID; setting a project
> ID on an inode the daemon does not own requires `CAP_FOWNER`. By design the
> daemon does **not** fail fast on a missing `CAP_FOWNER` — a fresh backend with no
> pre-existing untagged volumes doesn't need it, so the startup guard gates only on
> `CAP_SYS_ADMIN`. The backfill is best-effort instead: if it can't re-tag a volume
> it logs `quota backfill: failed to re-apply quota` and increments
> `volume_quota_backfill_total{outcome="failed"}`, leaving that one volume
> unenforced. To heal such volumes, grant `CAP_FOWNER` and restart the backend (or
> re-provision them).

#### Experimental backends (untested — not for production)

> **Not production-ready.** The `btrfs` and `zfs` backends are implemented but
> have **no production coverage** — no mainnet/Morpheus backend runs them, and the
> per-volume inode (`ihard`) exhaustion protection enforced on XFS has no tested
> equivalent here. They are documented for completeness only; use `xfs` for any
> production deployment.

#### btrfs (experimental)

```bash
# Create a btrfs filesystem on a dedicated disk/partition
mkfs.btrfs /dev/nvme1n1
mkdir -p /var/lib/fred/volumes
mount -t btrfs /dev/nvme1n1 /var/lib/fred/volumes

# Enable quotas (one-time, per filesystem)
btrfs quota enable /var/lib/fred/volumes

# Persist in /etc/fstab
echo "/dev/nvme1n1 /var/lib/fred/volumes btrfs defaults 0 0" >> /etc/fstab
```

Each container gets a btrfs subvolume with a qgroup quota.

#### zfs (experimental)

```bash
zpool create fredpool /dev/nvme1n1
zfs create fredpool/volumes
zfs set mountpoint=/var/lib/fred/volumes fredpool/volumes
```

Each container gets a child dataset with the `quota` property set.

#### Set the config

In `docker-backend.yaml`:

```yaml
volume_data_path: "/var/lib/fred/volumes"
volume_mount_path: "/var/lib/fred/volumes"
# volume_filesystem auto-detects from the mount; set explicitly to override:
# volume_filesystem: "xfs"   # xfs is the only production-validated backend
```

The docker-backend refuses to start if any SKU has `disk_mb > 0` and
`volume_data_path` is unset. Whenever `volume_data_path` is set—even on an
all-diskless host—`volume_mount_path` is required and must be the active mount
containing it.

---

## Configuration files

Two files, both validated at startup. The daemon refuses to start with any required field missing or invalid.

| File | Mounted at | Owner |
|---|---|---|
| `config.yaml` | `providerd` | Provider operator |
| `docker-backend.yaml` | each `docker-backend` | Provider operator |

Reference: [config.example.yaml](config.example.yaml), [docker-backend.example.yaml](docker-backend.example.yaml).

### Required field checklist

**`config.yaml`** (providerd):

- `provider_uuid` — your registered provider UUID
- `provider_address` — chain address for management messages
- `keyring_dir` + `key_name` — Cosmos keyring with the provider's signing key
- `callback_base_url` — URL where backends reach providerd (e.g. `https://fred.example.com:8443`)
- `backends` — at least one entry with `name`, `url`, a unique
  `hmac_secret` of at least 32 bytes, and either `skus` or `default: true`.
  Production rejects a missing or duplicate per-backend secret; the top-level
  `callback_secret` compatibility fallback is for non-production deployments
  only
- `placement_store_db_path` — required durable placement database; providerd refuses to start without it because supported deployments always use multiple backends
- `production_mode: true` — required on providerd and every bundled backend in production (forces replay protection, blocks SSRF and insecure TLS settings, requires peer-verified `https://` backend URLs using the configured private CA or system roots, and requires an `https://` `callback_base_url` whose peer the backend verifies)
- `token_tracker_db_path` — required when `production_mode: true`

**Environment variables** (providerd):

- `FRED_KEYRING_PASSPHRASE` — **required** when `keyring_backend` is `file` (the default); the signer pool hard-fails at startup without it. Not needed for `keyring_backend: test` or `os`. Pass it via a systemd `EnvironmentFile=` (see [Process management](#process-management-systemd)), not `Environment=`, so it isn't exposed via `systemctl show`.
- `FRED_MNEMONIC` — first-boot only; **required** when `sub_signer_count > 0` so the signer pool can derive its sub-signer keys. Without it the pool runs single-signer (logged as a warning, no hard fail). It can be unset on later boots once the sub-keys persist in the keyring.

**`docker-backend.yaml`**:

- `host_address` — public IP or hostname tenants will use to reach containers
- `callback_secret` — must match only this backend's corresponding
  `providerd` `backends[].hmac_secret` and must not be reused by another backend
- `callback_max_age` — must be positive (default `24h`) in both Docker and k3s
  production backends. It bounds legacy callbacks and typed lifecycle
  observations. Exact operation/maintenance completions, operation and
  maintenance intents, and Docker's close intents do not expire: they may be the
  only evidence that can settle a durable placement attempt, classify a partial
  replacement, or safely resume partial teardown. An undeliverable exact head
  blocks that lease until delivery or operator repair; a close row remains until
  its release fence is retired and its lifecycle result is durably enqueued.
  Zero and negative values are rejected
- `sku_mapping` — maps on-chain SKU UUIDs to local profile names (provisioning fails otherwise)
- `sku_profiles` CPU values and `total_cpu_cores` must be finite and positive;
  YAML `.nan`, `.inf`, and `-.inf` are rejected. Memory totals/profile values
  must be positive, while a profile's `disk_mb` may be zero for stateless work
- SKU configuration resolves new generations and the one-time v0.13 migration;
  it is not a live repricing mechanism. Admitted generations persist immutable
  CPU/memory/durable-disk/scratch profiles through operation, release, close,
  and retention authority. For every Docker `disk_mb: 0` row, the positive
  `container_tmpfs_size_mb` value is pinned as conservative scratch disk and is
  included in global/tenant disk admission
- `container_tmpfs_size_mb` — non-negative; `0` or omission selects `64`.
  It controls the memory tmpfs ceiling and the separately accounted on-disk
  scratch allowance for diskless Docker generations
- `volume_data_path` — required when any SKU has `disk_mb > 0`
- `volume_mount_path` — required whenever `volume_data_path` is configured; must name the
  actual active mount that contains `volume_data_path`, not merely a directory
  on the same root filesystem. Storage-lineage initialization and runtime
  re-attestation fail closed if this mount or the pinned data-root inode changes

**Environment variables** (docker-backend / k3s-backend) — each overrides the corresponding YAML field:

- `DOCKER_BACKEND_ADDR` / `K3S_BACKEND_ADDR` — override `listen_addr`
- `DOCKER_BACKEND_CALLBACK_SECRET` / `K3S_BACKEND_CALLBACK_SECRET` — override `callback_secret` (lets you keep the secret out of the on-disk YAML)
- `DOCKER_BACKEND_HOST_ADDRESS` / `K3S_BACKEND_HOST_ADDRESS` — override `host_address`
- `DOCKER_BACKEND_MAX_REQUEST_BODY_SIZE` / `K3S_BACKEND_MAX_REQUEST_BODY_SIZE` — override the 2 MiB request-body cap (a non-positive value is ignored)
- `DOCKER_HOST` (docker-backend) — Docker daemon endpoint, per the standard Docker convention
- `KUBECONFIG` (k3s-backend) — path to the kubeconfig

**Deployment-automation prerequisite.** Before rolling this release, verify the
rendered configuration on every host, not only the source template. Production
environments must render `production_mode: true`; every Docker host with
`volume_data_path` must also render the actual `volume_mount_path`; and disk
capacity/tenant quotas must include one `container_tmpfs_size_mb` scratch charge
per possible diskless instance. For a v0.13 cutover, freeze that value with the
SKU profiles until each backend completes its first upgraded startup. Render one
unique secret `K_i` into each backend's `callback_secret` and the matching
provider `backends[].hmac_secret`; remove the provider's top-level
`callback_secret` in production. Update the
external monitoring rules to page on placement write failures, missing placement
inventory, stale/erroring Docker reconciliation, aged close intents, and
sustained restore-finalizer failures using the signals in
[OPERATIONS.md](OPERATIONS.md#common-alerts-and-what-they-mean). A structurally
valid but semantically unresolved maintenance WAL need not make `/health` fail or
increment `callback_store_errors_total`; the Docker reconciliation error and
last-success metrics are its runtime signal. These are prerequisites in the
deployment repository; this source-tree upgrade does not rewrite external
manifests or alert rules.

### Docker startup and recovery budgets

Docker-backend uses fixed, nested safety budgets; they are not YAML tuning
knobs. The production constructor allows 30 seconds for the initial
Docker/storage-lineage attestation. `Start` shares at most 30 seconds across its
initial identity, ping, and capability reads, then uses a backend-lifecycle
30-minute aggregate budget for crash convergence and legacy migration. Each
subsequent startup phase receives one aggregate budget of
`max(2m, container_stop_timeout)`. Recovery Docker list/inspect calls are capped
at 30 seconds, and cold-start diagnostic collection plus orphan-network cleanup
each share one such aggregate budget rather than receiving a fresh timeout per
container/network.

Consequently, a wedged Docker API fails startup or defers a best-effort phase
instead of blocking process lifetime, while a large fleet cannot multiply a
per-object timeout without bound. An overall recovery deadline is a typed startup
failure: systemd may restart the backend, and the next launch resumes from the
same operation, maintenance, and close intents and substrate evidence.

For Go embedders, `docker.New` installs the finite 30-second construction bound;
`docker.NewWithContext` adds no fallback, so pass a finite caller deadline. It
does not retain that context after construction. Internal zero-valued recovery budgets
select the production defaults. In YAML, `container_stop_timeout`,
`migration_ready_timeout`, and `migration_grace_period` likewise use zero as
their documented default sentinel and reject negative values; other required
operation durations keep their existing positive-only validation. Go duration
syntax has no `d` or `w` unit.

---

## Secret rotation

Each production backend has its own bidirectional HMAC secret `K_i`, configured
as that backend's `callback_secret` and the matching provider
`backends[].hmac_secret`. Never reuse one backend's key for another: key
separation prevents a compromised backend from authenticating requests or
callbacks as a peer. Rotation requires a coordinated restart:

1. Drain operations and callback replay, remove provider ingress, then stop the
   single `providerd` and the backend(s) whose keys will change.
2. Generate a unique secret of at least 32 bytes for every changed backend.
   Write `K_i` to that backend's `callback_secret` first.
3. Write the same `K_i` only to the corresponding provider
   `backends[].hmac_secret`. Production must not retain the top-level
   `callback_secret` fallback.
4. Start and verify the backends first, then start `providerd` and verify a test
   provision. Identity-bearing current-schema callback rows need no rewrite:
   the upgraded sender preserves their captured storage lineage and signs them
   with the backend's current key when it delivers them. This does not permit a
   v0.13 cutover with pending legacy rows; that outbox must be drained before
   storage-identity initialization.

There is no built-in support for two-secret rotation (active + previous), so
do not attempt a rolling mixed-key phase; secret rotation includes a brief
`providerd` outage. Do not overlap two
`providerd` instances for the same provider and backend fleet: the placement
database is a single-writer bbolt file, while the lifecycle-operation registry
is process-local and has no cross-process coordinator. A second instance with a
different database is therefore not a safe active-active workaround.

The Cosmos keyring's signing key is not rotatable — it's the provider's chain identity.

---

## Process management (systemd)

A minimal `providerd.service`:

```ini
[Unit]
Description=Fred provider daemon
Documentation=https://github.com/manifest-network/fred
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=fred
Group=fred
# Secrets (file chmod 0600, owned by fred): FRED_KEYRING_PASSPHRASE — required for
# the default keyring_backend: file — plus FRED_MNEMONIC on first boot when
# sub_signer_count > 0. EnvironmentFile= keeps them out of 'systemctl show' (unlike Environment=).
EnvironmentFile=/etc/fred/providerd.env
ExecStart=/usr/local/bin/providerd -c /etc/fred/config.yaml
Restart=on-failure
RestartSec=5s
TimeoutStopSec=45s
LimitNOFILE=65536

# Hardening (the daemon does not need root)
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
# ReadWritePaths must include the directories holding any *_db_path values
# from your config (token_tracker_db_path, payload_store_db_path,
# placement_store_db_path). Adjust this line to match.
ReadWritePaths=/var/lib/fred

[Install]
WantedBy=multi-user.target
```

`docker-backend.service` is the same shape with three differences:
- It needs Docker socket access. Either add `SupplementaryGroups=docker` to the unit (so the service user inherits the `docker` group), add the service user to the `docker` group out of band, or run as root. Note this makes the docker-backend effectively host-root-equivalent regardless of `User=` (access to a rootful Docker socket can launch a privileged container) — so unlike `providerd`, its minimal-capability hardening is partly cosmetic. The real lever for de-privileging it is rootless Docker.
- **On XFS hosts it needs `CAP_SYS_ADMIN` and `CAP_FOWNER`** to set per-volume project quotas (see the xfs section above): `CAP_SYS_ADMIN` to set the block limit, and `CAP_FOWNER` so the startup backfill can re-tag tenant-owned volume directories with their project ID. Add `AmbientCapabilities=CAP_SYS_ADMIN CAP_FOWNER` and include both in `CapabilityBoundingSet` — ambient capabilities are compatible with `NoNewPrivileges=true`. Scope this to `docker-backend` only; `providerd` does not need it. (btrfs `subvolume`/`qgroup` and zfs `create`/`set` are likewise privileged; zfs alternatively supports `zfs allow` delegation.) Note: a plain `setcap cap_sys_admin,cap_fowner+ep` on the binary is **not** sufficient — the daemon shells out to `xfs_quota`/`btrfs`, and a file-capability grant does not propagate to those child processes (it clears the ambient set). Use `AmbientCapabilities` (or run as root); the daemon refuses to start without `CAP_SYS_ADMIN`.
- `ReadWritePaths` should cover the directories holding `callback_db_path`, `diagnostics_db_path`, `releases_db_path`, `retention_db_path`, and `volume_data_path`. The authoritative retention database is required even when `retain_on_close` is false.

`TimeoutStopSec` should comfortably exceed the graceful-drain window so systemd
doesn't SIGKILL mid-shutdown. For `providerd` this window is `shutdown_timeout`
from your config (default 30s). Docker-backend has two sequential bounds: a
fixed 30s HTTP-server shutdown followed by a fixed 90s backend-worker drain.
Configure its unit comfortably above the combined two minutes so the binary can
report a typed drain failure and exit non-zero rather than being killed first;
neither Docker bound is configurable.

---

## TLS topology

There are three TLS surfaces, each independently configurable.

### 1. Tenant API (providerd → tenant)

Set `tls_cert_file` and `tls_key_file` in `config.yaml`. Both must be set or neither.

In production, the common pattern is to terminate TLS at a reverse proxy (nginx, Caddy, Traefik) and run providerd with HTTP only on a private interface:

```nginx
server {
    listen 443 ssl http2;
    server_name fred.example.com;

    ssl_certificate     /etc/letsencrypt/live/fred.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/fred.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        # WebSocket /events endpoint
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 3600s;
    }
}
```

Configure `trusted_proxies` in `config.yaml` to include the proxy's IP CIDR so per-IP rate limiting uses the real client address from `X-Forwarded-For`. Untrusted `X-Forwarded-For` headers are ignored. Without this, every request appears to originate from the proxy, collapsing all clients into a single per-IP bucket — effectively a global `rate_limit_rps` cap shared across restore/update/restart/`/data`. Raise `rate_limit_rps`/`rate_limit_burst` or set `trusted_proxies` for high-throughput direct-API workloads.

### 2. providerd → backend (HMAC-authenticated)

HMAC authenticates each providerd request, but it does not authenticate the backend's response header or body. Callback HMAC authenticates the reverse hop, but cannot keep its causal tokens confidential on plaintext transport. Plain HTTP is therefore development-only. Provider `production_mode: true` requires every `backends[].url` and `callback_base_url` to use `https://` and verifies backend peers using `tls_ca_file` (a private CA is valid) or system roots; enable production mode on each bundled backend too so it cannot disable callback peer verification. Configure backend-native TLS/mTLS or trusted TLS-terminating proxies before production.

Keep the two certificate-verification paths distinct. In the reference
`manifest-deploy` topology, providerd connects to each backend's private IP and
verifies its IP-SAN leaf against the internal private CA while presenting an
mTLS client certificate. Backend callbacks instead connect to the public
`callback_base_url`; Traefik terminates that TLS connection and the backend
verifies Traefik's publicly trusted certificate with Go's system roots. A
private/self-signed CA on the first hop is still verification—it is an explicit
trust anchor, not `InsecureSkipVerify`. Likewise,
`callback_insecure_skip_verify: false` performs verification even if backend
`production_mode` was accidentally omitted; backend production mode is the
startup guard that prevents a later configuration change from turning that
verification off. Providerd's `production_mode` does not set the separate
backend process's guard.

**Recommended: native TLS/mTLS (ENG-103).** The docker-backend can serve TLS directly, and providerd can verify it (and optionally present a client certificate for mutual TLS). Outside production mode the transport stays HTTP until configured; production mode refuses that plaintext default. When enabled, TLS 1.3 is pinned as the minimum version (`MinVersion`; no maximum is set).

On the **docker-backend** (server), in `docker-backend.yaml`:

```yaml
# Server TLS (both must be set, or neither): serves HTTPS on listen_addr.
tls_cert_file: "/etc/fred/backend.crt"
tls_key_file:  "/etc/fred/backend.key"
# Mutual TLS (optional): require and verify a client certificate signed by
# this CA. Requires the server tls_cert_file/tls_key_file above.
tls_client_ca_file: "/etc/fred/client-ca.crt"
# Pin the client identity (optional): the client cert's CommonName or a DNS
# SAN must appear here. Use whenever tls_client_ca_file is not dedicated
# solely to providerd. Requires tls_client_ca_file.
tls_client_allowed_names:
  - "providerd.fred.example.com"
```

On **providerd** (client), per `backends[]` entry in `config.yaml`:

```yaml
backends:
  - name: docker-1
    url: "https://10.0.0.1:9001"     # https:// to use TLS
    tls_ca_file: "/etc/fred/backend-ca.crt"        # CA that signed the backend's server cert
    tls_client_cert_file: "/etc/fred/providerd.crt" # client cert for mTLS (set with key)
    tls_client_key_file:  "/etc/fred/providerd.key" # client key for mTLS (set with cert)
    # tls_skip_verify: true   # insecure; rejected when production_mode is true
```

Empty client TLS fields fall back to Go defaults (system root CAs, no client certificate). `tls_skip_verify` disables server-certificate verification and is for testing only — it is rejected when `production_mode: true`, as is an `http://` backend URL.

**Alternative: TLS-terminating proxy.** Instead of native TLS, you can run docker-backend behind a TLS-terminating reverse proxy and use `https://` in `backends[].url`. This predates native TLS support and remains a valid option, e.g. where you already operate a proxy fleet.

### 3. providerd → chain (gRPC)

Set `grpc_tls_enabled: true` to enable TLS for the chain gRPC endpoint. `grpc_tls_ca_file` overrides the system CA bundle. `grpc_tls_skip_verify` is for testing only and is blocked when `production_mode: true`.

---

## Ingress (via Traefik)

When `ingress.enabled: true` in `docker-backend.yaml`, the backend emits Traefik labels for each container with a routable port. The Traefik integration:

- Generates a unique subdomain per container under `wildcard_domain` (e.g. `<lease-prefix>.apps.example.com`)
- Emits `tls=true` routers that use Traefik's default certificate or a `tls.stores` default
- **Does not drive ACME challenges** — the wildcard cert for `wildcard_domain` must be provisioned at the Traefik level (typically via a DNS-01 ACME resolver with `domains` set in Traefik's static config)
- Requires `network_isolation: true` (the default), since Traefik routes via the per-tenant Docker network

A minimal Traefik setup is outside this doc's scope; the `manifest-deploy` repo has a working Ansible role.

---

## Multi-host setup with load-based routing

To distribute new provisions across multiple Docker hosts:

```yaml
# config.yaml
production_mode: true
callback_base_url: "https://fred.example.com"
token_tracker_db_path: "/var/lib/fred/tokens.db"

backends:
  - name: docker-1
    url: "https://10.0.0.1:9001"
    hmac_secret: "docker-1-unique-secret-change-me-0123456789"
    tls_ca_file: "/etc/fred/backend-ca.crt"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"
    default: true
  - name: docker-2
    url: "https://10.0.0.2:9001"
    hmac_secret: "docker-2-unique-secret-change-me-0123456789"
    tls_ca_file: "/etc/fred/backend-ca.crt"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"

placement_store_db_path: "/var/lib/fred/placements.db"
```

Identical `skus` lists on `docker-1` and `docker-2` make them a matching pool for those SKUs. Fred routes each new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats. `placement_store_db_path` is required so direct read, restart, and update operations reach the backend holding each lease, and restore uses the same durable state. The prepared database is bound to the configured `provider_uuid`; providerd rejects a store prepared for another provider. Normal startup opens only an existing prepared file and never creates, initializes, or migrates it.

To add a genuinely new host, configure its empty Docker/backend data paths and,
before the first normal server start, seal them once with
`docker-backend -config /etc/fred/docker-backend.yaml -initialize-storage-identity new`.
Normal startup is verification-only: it will not create the marker pair or the
authoritative callback/release/retention journals. Then start `docker-backend`,
add it with the same `skus` list to `config.yaml`, and restart `providerd`; it has
no configuration reload signal. Every topology membership change requires
identity-bearing responses
from the complete proposed fleet before the topology is committed, and a new
complete inventory before its admission baseline becomes healthy. By contrast,
an unchanged topology with an established baseline tolerates a transiently down
node: healthy answering nodes may continue receiving genuinely recordless
`PENDING` work, while work tied to the silent node remains pinned.

To drain a host, remove its SKUs so it no longer receives new provisions,
restart `providerd`, and keep the backend entry configured, reachable, and under
the same stable name until every placement and retention on it is gone. Let a
complete reconciliation sweep record that the backend's raw `/provisions` and
`/retentions` inventories are both concretely empty before removing the entry.
Removal requires that latest topology-bound empty-inventory evidence and no
durable placement or lifecycle reference to the name; an outage or a filtered
projection is not a drain proof. Existing leases may close naturally or be
deliberately migrated through the chain lifecycle; deleting or renaming the
backend entry does not migrate them. Backend names are immutable storage
identities: once a drained name is removed, restoring that same storage under
its original name is allowed, but the topology must complete a fresh full-fleet
inventory before admitting new work. Never bind replacement storage to a
historical name; give it a new unique name.

---

## Backups

The bbolt files are persistent state. Some can be rebuilt from the chain or the
docker daemon, but placement records also contain unresolved attempts and
conflict candidates that are not fully reconstructible. Backups speed recovery
and preserve that safety evidence.

| File | Backup priority | Restore behavior |
|---|---|---|
| `<docker>/releases.db` | High — immutable active topology/resource authority, including pinned scratch, is not reconstructible | Lost on disk failure. A matching active destination Release is also a restore commit marker: losing it can turn committed destination ownership into unresolved source-finalizer state. A pending close remains resumable from its complete non-expiring `callbacks.db` snapshot and treats an absent release key as already retired; the close blocks any newer operation for that lease. Current writers cap each encoded lease history at 32 MiB and compact only disposable audit rows |
| `<docker>/retention.db` | High — retained ownership, restore generation/finalizer, destination operation ID/callback pair/manifest/items/profiles, volume names, and source resource profiles are not reconstructible | Lost on disk failure → retained volumes can no longer be safely restored or reaped (orphaned on disk), and a committed or rolled-back restore cannot be classified or observed safely. The empty identity-bound file is always present and required, including when `retain_on_close` is false |
| `<docker>/diagnostics.db` | Medium — failure diagnostics for past 7 days, but no lifecycle authority | May be recreated after loss while the backend is stopped; only historical diagnostics are lost. Open/create still refuses a symlink, hard link, non-regular file, or mode other than exact `0600`, but the file is not identity-bound or continuously re-attested |
| `<docker>/callbacks.db` | Critical while an operation, maintenance, or close intent or exact completion is pending — write-ahead provision/restore and replacement intents, immutable resource/target authority, non-expiring destructive-close finalizers, durable exact/lifecycle deliveries, and per-lease FIFO evidence. Causal/close intents and exact operation/maintenance completions do not age out; legacy and lifecycle observations are retained up to `callback_max_age` | Accepted intent, partial-replacement/close authority, immutable sizing, and queued callback evidence are not recreated. Normal startup refuses a missing file instead of rebuilding its schema. Losing a maintenance row can make an exact replacement cohort unclassifiable; losing a close row after teardown starts can turn an intentional zero-survivor cohort into unexplained release divergence. Restore this file with the matching `releases.db`, `retention.db`, marker pair, and substrate |
| Backend storage-lineage seal | Critical — the marker pair plus every identity-bound authoritative store bind a backend name to one substrate generation | Docker's set is `callbacks.db`, `releases.db`, `retention.db`, both markers, and the substrate; k3s uses `callbacks.db`, `releases.db`, both markers, and the cluster. Every authoritative database must remain an unsymlinked, single-link regular file with exact mode `0600`; startup and runtime re-attestation fail closed on drift. Restore the complete matching set. One missing, corrupt, foreign, cross-kind, or path/inode-replaced member intentionally prevents startup. Never copy markers onto replacement storage or rerun initialization to repair a committed seal. Whenever Docker has `volume_data_path`, the primary is `volume_data_path/.fred-backend-storage-identity.json` and the anchor is `callback_db_path.storage-identity-anchor.json`; Docker without a managed volume root and k3s keep both adjacent to `callback_db_path`. If all paths share one mount, the set detects partial deletion/torn initialization but is not an independent backup—protect and snapshot the whole mount |
| `placement_store_db_path` | Critical — provider binding, unresolved attempts, ordinary and rejected-positive (`untrusted_positive`) quarantine, immutable backend-name/storage pins, topology history, and the durable inventory baseline are non-derivable safety authority | Restore the exact file only while `providerd` is stopped. It must be an unsymlinked, single-link regular file with exact mode `0600`. Normal startup never creates, initializes, or migrates an absent/empty/unprepared replacement, and rejects a file bound to another provider. The fresh initializer is only for a genuinely new provider with zero total chain lease history; it is never recovery for a lost database |
| `payload_store_db_path` | Low — pending tenant manifests, which tenants can re-upload | Restore only while `providerd` is stopped as an unsymlinked, single-link regular file with exact mode `0600`; otherwise tenants must re-upload pending payloads |
| `token_tracker_db_path` | None — replay protection has 30s window anyway | Empties on restart, acceptable. bbolt creates a missing file with mode `0600`, but this short-lived cache is not lineage/path identity-bound like placement or payload authority; replace it only while providerd is stopped |

Ordinary file-copy backups must be taken with the owning daemon stopped (bbolt's
file lock refuses a second bbolt open). Snapshot each backend's marker pair,
complete authoritative-store set, and substrate as one unit so a restore cannot
mix generations or times. For zero-downtime backups, use an atomic
filesystem-level snapshot (LVM, ZFS, btrfs) — bbolt files are crash-consistent.
Restoring a complete matching snapshot intentionally preserves the same lineage,
so fence the original backend before the restored copy starts.

`placement_store_db_path` is specifically not hot-swappable. Never copy over,
unlink, rename, rotate, or restore that pathname while `providerd` is running;
the process binds the exact private single-link inode it opened and permanently
withdraws placement authority if the path stops naming it or its mode/link count
drifts. A live backup must be an atomic
filesystem snapshot, not pathname replacement. Restore only while stopped. A
stopped restore may naturally create a new inode: the next strict open validates
the provider-bound authority and binds that inode before using it.

### Initializing a genuinely fresh placement authority

Normal `providerd` startup deliberately cannot create `placement_store_db_path`.
Only a genuinely new provider with zero total chain lease history may initialize
it offline with `placement-preflight`. This is never an inventory-based recovery
path for a lost database: once the provider has any chain lease history,
including terminal history, restore the exact placement backup.

Before running the initializer:

1. Initialize/seal every backend storage identity, and keep every configured
   backend reachable. Each complete `/provisions` and `/retentions` response
   must carry the same canonical identity for that backend, identities must be
   unique across names, and both inventories must be concretely empty.
2. Stop `providerd` and fence tenant API plus chain-facing lease-mutation ingress.
   Keep each backend running because the initializer must authenticate its live
   `/provisions` and `/retentions` responses, but first prove it empty and drain
   every in-flight mutation and callback/outbox delivery so those workers are
   idle. Keep this boundary in force through database publication and the first
   `providerd` startup; inventory responders remain available, but no authorized
   path may create work while the proof is collected.
3. Verify the configured `placement_store_db_path` is an absolute, clean path
   whose parent directory already exists, and that the database itself does not
   exist—not even as an empty file. Relative paths are rejected so the offline
   tool and the service cannot resolve the same configuration against different
   working directories. The confirmation binds the resolved parent's physical
   device/inode identity; keep that directory mounted and in place between the
   print and initialize invocations. The initializer is no-overwrite and refuses
   every existing path.
4. Independently write down the exact expected backend-name roster. Do not copy
   it programmatically from the configuration during the cutover; the separate
   `-expected-backends` JSON value is an operator cross-check against the fleet
   the configuration and live inventory actually prove.
5. Ensure the signer-free, all-state chain query for the configured
   `provider_uuid` can complete at one positive pinned height over
   certificate-verified gRPC TLS. The provider must have zero total leases;
   `PENDING`, `ACTIVE`, terminal, unknown, and future-state history all block
   initialization. An intentionally local
   development chain may use plaintext or skip verification only with the exact
   `-confirm-insecure-chain 'I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT'`
   operator attestation. This is never an automatic downgrade: it is required
   even when a shared configuration template sets `production_mode: true`, and
   the tool rejects a stale attestation when certificate verification is active.

Then run the exact high-friction acknowledgement:

```bash
fresh_confirmation="$(
  placement-preflight \
    -config /etc/fred/config.yaml \
    -print-fresh-confirmation \
    -expected-backends '["docker-1","docker-2"]'
)"

placement-preflight \
  -config /etc/fred/config.yaml \
  -initialize-fresh \
  -expected-backends '["docker-1","docker-2"]' \
  -confirm-quiesced "$fresh_confirmation"
```

Supply the independently recorded, sorted roster to both commands. The print
mode loads and validates only the local configuration and roster, resolves and
binds the target parent's physical device/inode identity, renders the exact
Go-canonical acknowledgement (including safe string escaping), and exits
without a backend/chain request or mutation. Review its target path, parent
identity, provider UUID, and roster before passing that exact output to the
initializer; a missing or mismatched acknowledgement—including a parent that
was renamed, unmounted, or recreated at the same pathname—fails before any
backend probe. In the rendered
statement, “all backends are quiesced” means their authoritative inventories are
empty, mutation work is drained, and callback/outbox delivery is idle—not that
the inventory-serving backend processes are stopped.

The tool reuses the configured HMAC and peer-verified TLS settings, verifies
that the invocation's parent identity matches the printed acknowledgement,
collects the complete empty fleet, and takes the pinned zero-history chain
snapshot. It then reopens and re-attests that proof-bound physical parent before
constructing a deadline-bound, one-shot capability whose evidence age is capped
at two minutes even when `-proof-timeout` is larger. The capability is bound to
the resolved target path and parent device/inode, provider UUID, independently
supplied exact roster, configured topology, and unique storage IDs. It creates
and verifies the candidate through the retained parent descriptor, then
publishes it with descriptor-relative `renameat2(RENAME_NOREPLACE)`: the final
entry is never overwritten and the candidate never has a second crash-visible
name. It establishes the empty topology's admission baseline. It
prints the bound `fresh_target`, `fresh_provider`, and
`expected_backend_roster`, then prints `INITIALIZED_FOR_CUTOVER` only after reopening and verifying
the candidate authority, publishing and syncing it, reopening the published
inode read-only to repeat the physical and exact semantic postconditions, and rendering all
diagnostic lines; the verdict is the final line of one output write. Treat the
initializer as successful only when it exits zero and that final line begins
exactly `INITIALIZED_FOR_CUTOVER:`. Any missing,
nonempty, malformed, identity-inconsistent, or timed-out response leaves the
destination absent.
The earlier of `-proof-timeout` and the two-minute package limit is the
capability's publication deadline. File
open/fsync/close calls are not themselves interruptible, but cancellation or
expiry observed before the no-replace rename leaves the destination absent. If
it is observed after the rename, the command returns `INITIALIZED:` because the
target now exists. A parent-path identity mismatch at any boundary fails closed;
recreating the old directory name does not retarget the retained capability.
Keep the offline command under process/service supervision for blocking
filesystem operations and output.

If the command exits with an `INITIALIZED:` error, publication succeeded before
a later durability check or complete-verdict write failed. Do not rerun the
initializer and do not assume the path is absent. Keep `providerd` stopped and
immediately inspect the current authority read-only with
`placement-repair -config /etc/fred/config.yaml -classify`, then investigate storage
health and preserve the file before deciding how to proceed.

There is no v0.13-compatible in-place rollback for this new schema. Before any
upgraded side effect, stop the process and archive the newly initialized file if
you must abandon the attempt, then repeat the offline workflow. Once upgraded
side effects begin, keep and back up that exact provider-bound database and
forward-fix; never point v0.13 at it or discard it in favor of a fresh file.

---

## Upgrades

Fred releases are tagged on GitHub with binaries via `goreleaser`. The release process is:

1. Tag a release on GitHub. `goreleaser` publishes `providerd`,
   `docker-backend`, the offline `placement-preflight`
   inspector/preparer/initializer, and the offline `placement-repair` tool as
   binary archives, plus a single Docker
   image — `ghcr.io/manifest-network/fred` — which contains `providerd` plus
   both placement tools. The image still starts `providerd` by default; invoke
   a tool with `--entrypoint /placement-preflight` or
   `--entrypoint /placement-repair`. There is no published `docker-backend` (or
   `k3s-backend`) image; run the `docker-backend` binary from its archive, or
   build its image locally (see [Docker images](#docker-images)).
2. Pull the new binary or image to your hosts.
3. Roll the backend binaries one at a time when the release's backend protocol
   is backward-compatible, then stop the single `providerd` instance and start
   the upgraded binary. Never overlap the old and new `providerd` processes for
   the same provider and backend fleet.

`providerd` upgrades are stop/start and include a brief outage. Active-active or
rolling `providerd` instances for one provider/backend fleet are unsupported:
bbolt enforces one writer when the instances share databases, and separate
databases would also split the process-local lifecycle-operation registry. The
startup sequence (chain reconnect, inventory projection, reconciliation) usually
takes seconds; keep the callback path available again as soon as the new process
starts.

### Upgrading from v0.13.0

The v0.13 placement database is migrated only by the mandatory offline
`placement-preflight -prepare` step below. Normal upgraded startup is read-only
with respect to schema: it refuses an unprepared file rather than performing a
first-open migration. Preparation binds the database to the exact configured
provider UUID and observed backend storage identities, then atomically assigns
unambiguous v0.13.0 `{backend,set_at}` owners typed record revisions and an
explicit ID-empty legacy lifecycle binding. Fred does not mint a bearer
capability that the existing backend never received; the binding becomes typed
only after a later exact operation distributes and confirms the paired
lifecycle URL. Ambiguous, malformed, or otherwise unusable records remain
fail-closed.

Historical raw backend-name rows use v0.13's exact first-byte grammar during
the one-time migration. Printable configured names such as `null`, `true`,
`123`, or `[]` remain raw names rather than being reinterpreted as JSON scalar
values; object-form rows retain their original `set_at` timestamp.

Lifecycle adoption runs before record-revision migration and
requires a database-wide first-open epoch: both
`placement_lifecycle_capabilities` and `placement_metadata` must be absent at
transaction entry, no placement row may already carry a revision, and the
individual owner must be revision-zero and confirmed. Once either new bucket
has existed, no missing row is backfilled—even if an unsupported downgrade
later writes a revision-zero placement—because Fred cannot prove it did not
replace formerly typed authority. The epoch proof is database-wide: one
nonzero-revision row, or one JSON-object-shaped row whose revision header cannot
be decoded, disqualifies legacy adoption for every otherwise valid revision-zero
row because Fred cannot prove the whole file is pristine v0.13.0 state. A
semantically invalid but decodable revision-zero row is quarantined lease-locally
and does not disqualify another clean owner. The successful offline preparation
creates the new schema boundary, so repairing a disqualifying row afterward
cannot rerun adoption. A placement whose capability is missing or corrupt is
quarantined for lifecycle callbacks on that lease rather than downgraded to
tokenless authority; it does not prevent unrelated leases from loading. This is
an in-place migration of the exact v0.13.0 placement database, not an inventory
backfill. Preserve `placement_store_db_path`: inventory cannot reconstruct
unresolved attempts, conflicts, historical negative evidence, or the immutable
backend-name/storage bindings. Normal upgraded startup refuses an absent,
empty, or unprepared replacement database rather than self-binding whatever
same-name endpoints current configuration happens to resolve.

The per-backend HMAC migration makes this a stopped provider/fleet cutover; do
not roll a mixed-key fleet while the old providerd remains online. Quiesce and
drain operations, including every legacy callback outbox to zero—pending
pre-identity rows are not carried across the storage-identity seal—then remove
provider ingress and stop the old providerd and all backends. Install the
upgraded binaries without starting them, preserve the deployed v0.13 SKU and
scratch configuration, and set each backend's existing `callback_secret` to a
new unique value `K_i` of at least 32 bytes.

Before taking the cutover backup or publishing any storage-identity state, run
the dedicated read-only proof against every stopped Docker backend:

```bash
docker-backend -config /etc/fred/docker-backend.yaml \
  -preflight-storage-identity-adoption
```

Require exit status zero and stdout containing only the exact checked verdict
`ready_for_v0_13_storage_identity_adoption` followed by a newline. All startup
and failure diagnostics go to stderr, and a short or failed stdout write makes
the command fail. This mode acquires the same
descriptor-bound journal parents, Docker `SystemID`, complete strict container
inventory, release/retention evidence, and managed-volume inventory as the
initializer, but it requires both marker entries to remain absent and has no
marker/store mutation or recovery capability. It is mutually exclusive with
`-initialize-storage-identity`. A success is a point-in-time proof, not a
reservation: keep the old processes stopped, take the placement plus complete
backend substrate/control-state backups only after every backend passes, and
let `adopt` repeat the proof at its own publication boundary.

`stats.in_flight_provisions == 0` and a drained callback outbox are necessary
but not sufficient. A v0.13 restore can leave a durable `status:"restoring"`
row after a teardown, re-quarantine, or finalization failure even when no
operation remains in memory, and v0.13 `/retentions` does not expose that
status. The preflight reports the exact source, destination, and generation as
`ErrLegacyRestoringRetention`; the upgraded binary cannot safely reconstruct
the missing destination items/profile, typed operation ID, or exact callback
pair. Do not edit the row or synthesize those facts. Instead, restore/restart
the complete matching v0.13 lineage in isolation, let its retention reconciler
commit or roll back the restore, drain callbacks again, stop it, rerun the
read-only preflight, and take a fresh backup after it passes. A v0.13 `reaping`
tombstone is different: names already destroyed by a partial reap may be absent
and remain resumable, and a give-up tombstone can have an empty name list while
canonical and retained volumes remain. The proof permits absent stored names
only for `reaping` and derives that status's surviving footprint from the exact
`fred-{original_lease_uuid}-` and `fred-retained-{original_lease_uuid}-`
namespaces. Every managed volume outside those prefixes must still have strict
live-container or retention evidence. Every legacy retention SKU must also
resolve through the preserved v0.13 `sku_mapping` and `sku_profiles`; restore a
removed mapping/profile before retrying rather than sealing a journal that
cannot be accounted on first upgraded startup. Likewise, a positive v0.13 SKU
`disk_mb` reservation does not prove that a container has a bind: v0.13 created
a host volume only when image-volume or writable-path discovery required one. Release
and container topology prove a legitimate bindless cohort; the reverse
inventory check still refuses any unexplained managed volume.

The preflight also rejects an interrupted v0.13 deprovision when the journals
contain an active release, no managed container cohort, and a semantically
matching `active` or `reaping` retention for the same lease. This is a safe,
recognizable crash window after the retention/volume transition but before
release purge; it is not permission for the upgraded binary to edit across
journals. Restart the complete matching v0.13 lineage in isolation and replay
the exact close/deprovision event or request until the stale active release is
purged while the retention is preserved. Then drain callbacks, stop it, rerun
the read-only preflight, and take a fresh backup. A zero-container active release
with a present but divergent retention remains an unclassified inconsistency and
is rejected without mutation.

A distinct `ErrV013UnresolvedClose` refusal means an active release has no
managed container cohort and no retention finalizer. The report includes the
count of volumes still present in that lease's exact canonical/retained
namespace. This shape is compatible with a v0.13 crash after container teardown
but before retention or release finalization, whether or not volumes remain.
Do **not** replay v0.13 deprovision: its absent-provision path can purge the
release while leaving tenant volumes stranded with no durable owner. Restore a
complete matching pre-close snapshot and recover that v0.13 lineage in
isolation. If no such snapshot exists, require height-pinned chain and provider
inventory proof that the lease is terminal, then make and record an explicit
manual decision about the surviving data and journal authority. Stop the
lineage, rerun the read-only preflight, and take a fresh backup only after that
decision is complete.

`ErrV013InterruptedMigration` is a separate migration-generation refusal. An
exact pre-`RecordMigration` shape has a complete dense stopped `-prev` cohort
plus a complete semantically equal stack generation whose v0.13 writer omitted
backend, provider, and callback authority. v0.13 restart does not repair that
generation. Keep a full stopped backup, record the bounded sorted
`immutable_stack_container_ids` JSON array, and re-inspect each full ID while
stopped. Require its lease, exact `app` name/index, image/SKU, and omitted
backend/provider/callback labels to still match the saved diagnostic and
backup; a mismatch aborts cleanup. Stop those exact IDs if necessary, remove
them explicitly by ID without force, and leave every `-prev` container, bind,
and volume untouched. Rerun the read-only Docker
preflight while still stopped; after it passes, take a fresh complete backup,
seal with `adopt`, and start the upgraded backend. Its current migration path
then resumes from the proven rollback-only cohort. A pure rollback-only cohort
whose old bind source is absent but exact service-aware volume exists is also
resumable: the proof admits only that exact old-name/new-name transition and
reverse inventory must still explain the new volume.

An exact post-`RecordMigration` shape has a committed authorityless stack plus
a partial or complete `-prev` cleanup cohort. A partial rollback set cannot
prove desired quantity or callback authority, so neither old-binary restart nor
manual deletion is a repair. Restore a known-good complete pre-migration
snapshot or use a dedicated proof-bearing repair tool; this release ships no
general repair command for that class. A stack-only authorityless generation
has the same missing authority and fails closed. Do not infer quantity, copy a
callback from another generation, relabel containers, or seal either shape.

The release-journal inspector also closes v0.13's append-before-Compose window.
It atomically normalizes the exact flat-active then semantically equal
stack-active pair written by `RecordMigration`, and semantically identical
post-active `deploying` retries, while it binds the stopped store. A deploying
manifest that differs in environment, command, or any other parsed field is
ambiguous because Docker may run either generation; restore the matching v0.13
snapshot and settle or roll back that operation before retrying. Unrecognized
multiple-active histories remain unchanged and are rejected.

An `ErrV013OrphanRollbackRemnant` refusal is narrower: the stopped Docker
lineage contains only one exact dense v0.13 `-prev` rollback cohort for one
lease. Every member has a unique immutable ID and dense index, the configured
backend label, one canonical provider UUID, and byte-equal tenant, SKU, image,
effective custom domain, and callback pair; every Docker state is explicitly
non-running (`created`, `exited`, or `dead`). No active release, retention, or
managed-volume authority exists. The diagnostic emits the backend, provider,
and a bounded, sorted JSON array named `immutable_container_ids`. Record the
whole diagnostic, those IDs, and the Docker daemon SystemID before changing
anything. The preflight derives them while descriptor-pinning this journal set
and managed-volume root and re-attesting that SystemID; v0.13 had no separate
storage-UUID container label to invent. Never substitute container names, parse
the line with shell `eval`, or delete the remnants merely because the local
journals are empty. This local Docker classification is necessary but not
sufficient. Keep every process stopped, take a complete lineage backup, and
obtain the separate chain/placement proof:

```bash
placement-preflight \
  -config /etc/fred/config.yaml \
  -prove-terminal-orphan 550e8400-e29b-41d4-a716-446655440000 \
  -expected-backend docker-1
```

Use the exact lease UUID and backend name printed by the Docker diagnostic and
configured in the stopped provider. Its successful verdict must also print the
same provider UUID as the Docker diagnostic; a different configured/chain
provider is not authority for this cohort. The mode is mutually exclusive with
inspection preparation and fresh initialization. It opens and holds the stopped
placement database's shared lock first, re-attests its physical file, requires
the complete database to be pristine v0.13 schema, and queries a complete
signer-free all-state provider snapshot pinned to one positive chain height. The
target must be present in that snapshot and terminal; absence from the provider
query is not evidence. It makes no backend HTTP call and has no database,
Docker, keyring, or transaction mutation capability.

Proceed only after exit status zero and the single exact stdout line beginning
`TERMINAL_ORPHAN_PROVED:` reports the same lease, backend, and provider plus
`placement=absent`. That verdict is still not sufficient by itself: retain the
original Docker classification and require the final Docker adoption preflight
to pass after cleanup. A distinct residual-placement error means the target is
terminal but still has the expected v0.13 owner. Do not hand-edit the database
or delete the containers. Restart the complete matching v0.13 backend fleet and
providerd in isolation, with normal tenant ingress fenced, let its orphan pruner
observe the target absent from the backend provision and retention inventories
and retire the row, then drain, stop, back up, and rerun both proofs. A foreign,
revisioned, malformed, blocking, or chain-absent result grants no cleanup
authority; restore or investigate the matching lineage instead.

After both positive facts are recorded, inspect every emitted immutable ID
again while stopped (for example, `docker container inspect -- "$id"`) and
verify its full ID, exact lease/backend/provider/tenant/SKU/image/domain and
callback labels, dense name/index, explicit `created`/`exited`/`dead` state, and
mount evidence still match the saved diagnostic and backup on the same recorded
Docker SystemID. Remove each exact ID explicitly without force
(`docker container rm -- "$id"`); a state change or failed comparison aborts
cleanup. Then rerun `-prove-terminal-orphan` and every Docker backend's
`-preflight-storage-identity-adoption`. Only when the terminal proof still
reports the same provider and every Docker preflight now emits its normal
checked success verdict may you take a fresh complete cutover backup and
continue with storage-identity sealing.

After the backup, seal each existing v0.13 storage lineage exactly once before
its first normal upgraded start:

```bash
docker-backend -config /etc/fred/docker-backend.yaml \
  -initialize-storage-identity adopt
```

Use `-initialize-storage-identity new` for either a genuinely new backend or a
provably empty, drained v0.13 lineage. A genuinely fresh Docker lineage requires
all three authoritative paths (`callbacks.db`, `releases.db`, and
`retention.db`) to be absent. An existing v0.13 lineage requires all three files
to exist and decode successfully; `retention.db` is required even if
`retain_on_close` was false. A partial set is neither fresh nor adoptable and is
rejected rather than completed. The empty-v0.13 proof requires no managed
containers, active releases, retained records, or managed volume directories
and a fully drained legacy callback outbox. `new` also refuses operation,
maintenance, or close intents, or evidence of an already-upgraded callback
store.
`adopt` rejects an empty lineage with guidance to use `new`; otherwise it
requires strict and complete v0.13 container/retention/volume evidence beneath
the configured active `volume_mount_path`, a drained v0.13 callback store, valid
release history, and the strongest local cross-check v0.13 persisted between
every live workload and its active release. That local proof requires exact
lease/tenant/provider identity, coherent callback pairs, manifest service and
image equality, one SKU and effective custom domain per service, unique dense
instance indexes, and complete reverse volume ownership. A v0.13 release did
not persist requested `Items`, so local Docker evidence alone cannot prove that
the highest requested index is not missing. The initializer does not claim it
can. First upgraded startup freezes the observed cohort and then exposes its
exact `Items`; while providerd remains stopped, the mandatory placement
preflight below compares SKU, quantity, service, and nonempty effective domain
against the height-pinned chain items. That later result—not backend startup or
storage sealing by itself—is the cutover authorization. On a mismatch, stop the
backends and restore the complete pre-cutover backend backup; do not prepare or
start providerd with the locally inferred row. The Docker daemon `SystemID`
must remain the same before and after inspection.

Stopped storage-lineage inspection is deliberately bounded independently for
each authoritative database: at most 100,000 logical records, 256 MiB total
key-plus-value bytes, and 32 MiB for any one key or value. These are cutover
collection limits; ordinary runtime release validation streams all lease rows
and retains only the 32 MiB per-record ceiling, so a healthy large fleet does
not become unreadable merely because its aggregate release history exceeds
256 MiB. If a stopped v0.13 lineage exceeds a cutover ceiling, keep it on the
matching predecessor, drain completed work, and let that version's supported
age cleanup prune disposable records before retrying. Do not hand-edit bbolt,
delete a journal, or remove a load-bearing row to force adoption.

Initialization is a crash-resumable lineage transaction. Before inspecting
fresh/existing evidence, it binds the physical parent directory of both markers
and every authoritative journal (Docker callbacks/releases/retention; k3s
callbacks/releases). All inspection, creation, and publication then uses those
retained descriptors, and every parent pathname must continue to name the same
device/inode. It first publishes a pending anchor containing the generated UUID
and immutable fresh/existing profile. Only after that anchor is durable can its
opaque pending capability create or bind an authoritative store; a bare UUID
has no such authority. Copies share one revocation state, and the coordinator
withdraws them on every exit and before the first committed-marker publication,
so a retained hook copy cannot extend the preparation phase. The initializer
then creates or binds every authoritative
store with that UUID and its distinct store kind, revalidates the substrate and
cross-store evidence, publishes the primary marker, and finally commits the
anchor. A parent rename, unmount, or same-path directory recreation is rejected
instead of redirecting the transaction. If interrupted, rerun the same command
against the unchanged stopped input; the pending anchor forces the same profile
and identity. Do not switch modes, delete one output, or restore only part of
the set. Once both markers are committed, initialization is verification-only
and will never recreate, rebind, or repair a store.

The experimental k3s scaffold supports only
`-initialize-storage-identity new`, binds the seal to its cluster UID, and uses
the same all-absent or all-existing rule for `callbacks.db` and `releases.db`.
Its existing-profile proof requires a drained callback outbox and no active
release; it does not adopt live workloads.

Normal startup loads a typed verified-storage capability from the marker pair,
verifies the entire authoritative set before recovery or cleanup, and opens the
stores without file-creation or lineage-repair authority. One missing, empty, unbound,
foreign, cross-kind, symlinked, hard-linked, malformed, mode other than exact
`0600`, or replaced path/inode is a hard failure—restore the complete seal and
substrate instead of rerunning
initialization. The diagnostics store is not authoritative and may be recreated
while stopped, but its opener still requires an unsymlinked, single-link regular
file with exact mode `0600`; unlike the authoritative set, it is not bound to the
storage UUID and is not re-attested at every transaction.
Every authority read/write and background callback/release cleanup re-attests
the store binding and inode; request/substrate boundaries separately re-attest
the marker and daemon/cluster identity. The first terminal authoritative-store
or substrate failure is a backend-lifetime latch: it cancels the backend and is
returned by every sibling journal and callback-delivery boundary. Replacing a
file after startup, or receiving an outcome-unknown bbolt `Commit` error,
therefore fail-stops the whole backend authority rather than allowing another
journal to continue from a partially trusted lineage.

After configuring every backend key, set the same `K_i` only on that backend's
matching providerd `backends[].hmac_secret` and remove the provider's top-level
`callback_secret`. Start and verify all upgraded backends while keeping
providerd stopped. The startup storage identity must match the exact durable
backend name-to-storage-ID binding used by placement preparation. The mandatory
drain means no pre-identity v0.13 callback row crosses this boundary; if one is
restored afterward, current callback-store startup fails and health stays red
until the old outbox is explicitly drained. If a legacy row is introduced into
an already-open store, the sender leaves it queued rather than assigning the
mounted lineage. Identity-bearing current-schema rows
created after sealing retain their recorded identity and are signed with the
current `K_i` at delivery. With the
placement authority still stopped, run `placement-repair -classify` and require
`pristine_v0_13` before the first preparation; use the same command to resolve
any later ambiguous preparation outcome. Then run the read-only preflight and
mandatory preparation below. Start the upgraded providerd only after those
checks produce the required safe verdicts.

The marker detects replacement, not a simultaneously running full clone. A
snapshot that includes both markers and complete Docker data or cluster state
intentionally retains the same UUID. Fence the original node or cluster before
starting the restored clone, and never copy markers onto replacement storage.

Keep the exact deployed v0.13 `sku_mapping`, numeric `sku_profiles`, and
`container_tmpfs_size_mb` through the first successful upgraded Docker-backend
startup. Do not resize/remove a profile or change the scratch allowance in the
cutover deployment: startup migration uses that configuration to freeze legacy
live generations into ordered release items and canonical resource snapshots.
For each legacy `disk_mb: 0` instance it pins the cutover
`container_tmpfs_size_mb` as scratch once; later configuration changes do not
reprice it. After startup succeeds, back up the resulting
`releases.db`, `retention.db`, `callbacks.db`, and marker pair with the matching
substrate before making later profile changes.

v0.13.0 allowed an explicit
`callback_max_age: 0`; before restarting each upgraded backend, replace that
value with a positive duration because current Docker and k3s binaries reject
zero at startup. If the key is omitted, the `24h` default remains in effect and
no config edit is needed. A v0.13.0 backend can accept the new additive request
fields, but it does not persist the separate typed
`lifecycle_callback_url`; leaving one in the fleet would keep suppressing later
restart/update completion, runtime-failure, deprovisioned, and retained
observations after the exact provision/restore operation expires. The same
ordering installs the new
two-minute-fifteen-second callback delivery deadline before providerd begins
using synchronous application with its two-minute budget; an old ten-second
sender must not be the durability owner during that cutover. The new sender
shares that one deadline across all delivery attempts and backoff, leaving the
durable FIFO head for the 30-second replay loop when the budget ends. Slow HTTP
failures and black-holed requests therefore cannot block one lease for three
consecutive full application budgets. Operation, maintenance, and lifecycle
completion paths never perform that HTTP chain inline: after atomically queueing
their result they send a non-blocking, coalescing wake to the tracked replay
loop. The periodic 30-second sweep is the fallback if the in-memory wake is lost,
already pending, or sent before the loop starts.

At the initial stopped boundary, wait for `stats.in_flight_provisions` to reach
zero and verify the single v0.13.0 providerd shutdown reports no remaining
provision operations before running the backend adoption preflight described
above. Take the stopped bbolt/substrate backup only after that preflight passes.
After the upgraded,
per-backend-keyed backends are running and inventory-ready, keep providerd
stopped and its placement file offline. First classify that exact file; a normal
first cutover must report `pristine_v0_13`. Then run the read-only
`placement-preflight` inspection from the new release against the exact
configuration intended for the cutover, followed by mandatory preparation with
a new backup destination:

```bash
placement-repair -config /etc/fred/config.yaml -classify
placement-preflight -config /etc/fred/config.yaml
placement-preflight -config /etc/fred/config.yaml \
  -prepare -backup /var/backups/fred/placements-v0.13-pre-identity.db \
  -attest-drained 'I attest provider ingress, delayed backend requests/effects, and callback replay are stopped and drained'
```

Require exit status zero and the exact mode-specific final verdict from each
preflight command: `READY_TO_PREPARE:` for the default read-only inspection and
`PREPARED_FOR_CUTOVER:` for `-prepare`. Do not accept a generic `PASS` substring
or any token found in error text. The command renders top-level errors as one
prefixed, JSON-quoted physical line, including remote response bodies, so
newlines and terminal controls cannot manufacture a verdict.

Supply the same `PROVIDER_*` environment overrides as the service when the
configuration depends on them. Default mode opens the existing
`placement_store_db_path` read-only, runs bbolt's full physical page/freelist
consistency checker, and holds its shared bbolt lock while it
uses each backend's configured `hmac_secret`, HTTP timeout, and TLS/mTLS settings
to fetch every page of `/provisions` and `/retentions` concurrently from every
configured backend. Every page must carry one stable storage ID and the two
inventory endpoints must agree. Under that same lock, the tool uses a
signer-free gRPC client to fetch the configured provider's complete all-state
lease index at one pinned positive block height; it has no keyring or transaction
surface. Certificate-verified gRPC TLS is the deployment default because this
membership is provider-binding authority. An intentionally local development
chain may use the exact `-confirm-insecure-chain` operator attestation documented
above, including with a shared `production_mode: true` template; the tool rejects
plaintext or skip-verify evidence without that phrase and rejects the phrase when
certificate verification is active. Default mode never
creates a database or bucket and never starts a write transaction. `-prepare`
first binds the backup parent's physical device/inode, before any remote
evidence. It then reacquires the stopped file exclusively, repeats the proof and
physical check, creates, fsyncs, byte-verifies, and physically validates the
no-overwrite backup through that retained directory descriptor, and atomically
migrates lifecycle/revision metadata
and pins the provider UUID plus exact active name-to-storage-ID map while
leaving the admission baseline empty. A running providerd prevents either mode
from acquiring its lock; an unreachable or incomplete backend, chain-query
failure, expiration of `-proof-timeout` (two minutes by default), or any
discrepancy below exits nonzero without printing the applicable cutover
verdict. That timeout bounds
remote inventory/chain proof and cancellable validation only. File open, exact
backup copy, fsync, bbolt commit, close, and stdout are blocking storage/process
operations; supervise the offline command with the deployment service/process
manager and never assume those operations were safely canceled by
`-proof-timeout`. Proof freshness is nevertheless checked immediately before
and after the backup and at transaction commit admission. A descriptor-relative
`renameat2(RENAME_NOREPLACE)` is the backup publication boundary: it cannot
overwrite an existing entry and leaves no second crash-visible name. The tool
retains the published inode identity and rechecks both that inode and its parent
device/inode before migration, after mutation, and before its final verdict. Any
later validation, sync, close, or temporary-file cleanup error belonging to
backup publication, or any later
proof/capability failure before migration commit, is `BACKUP PUBLISHED`. No
preparation mutation committed; preserve the artifact, rerun the default read-only preflight, and
choose a new `-backup` path. A bbolt `Commit` error is instead `OUTCOME UNKNOWN`:
pages or a meta page may already be visible, so preserve both databases and
classify the live file without retrying or restoring blindly. Run:

```bash
placement-repair -config /etc/fred/config.yaml -classify
```

The classifier opens only the existing stopped file read-only, verifies that
the path continues to name the same inode, and emits one bounded JSON report;
it never creates or replaces the database and never prints `PASS`. It reports
the exact recognized bucket set, physical-check status, canonical
provider/topology, sorted storage bindings, topology/baseline/inventory epochs,
counts, and up to 128 lexicographically sorted per-lease state/revision,
owner-or-attempt/lifecycle verdicts, and the always-present
`untrusted_positive` quarantine flag (plus an omitted-row count). Each expected,
observed, known, storage-binding, and empty-inventory collection is capped at 64
entries with its own omitted count; each row carries at most four conflict
owners plus an omitted count, and overlong rendered identities are counted but
not emitted. Raw metadata, placement, and lifecycle values are rejected before
decoding above 4 MiB, 1 MiB, and 256 KiB respectively, and the final encoded
report (including its newline) is hard-limited to 1 MiB. It validates
the placement, storage-identity, and lifecycle relationships without exposing
operation or lifecycle IDs, callback URLs, tokens, secrets, payloads, tenant
data, or raw row values. Exit status is zero only for
`pristine_v0_13` (the preparation commit did not become visible) or
`prepared_current` (one complete current authority is visible).
`mixed_or_incomplete` and `corrupt` reports are emitted but exit nonzero; keep
the daemon stopped and preserve both files for investigation. Missing files,
permission failures, a live-writer lock, or a path/inode change are
environmental errors and do not become a misleading schema verdict.
`-classify` is local read-only inspection and rejects an explicitly supplied
`-timeout`; supervise blocking filesystem I/O externally. If cancellation is
observed before reporting, the command emits no stale safe report.

A successful commit
is synced and physically checked on the open handle, then closed, reopened
read-only, and physically and semantically reverified before
`PREPARED_FOR_CUTOVER`; any failure
after the definite commit is `PREPARED:`. Do not mistake blocking storage work
for a safely canceled operation:

The exact `-attest-drained` statement is mandatory. Current backends do not
expose an administrative generation/epoch that could let this tool remotely
fence delayed work, so this is an operator-established causal boundary, not a
claim that point-in-time inventory proves quiescence. After the proof is
collected, the preparer mints a short-lived opaque capability bound to its exact
exclusive session and proof cancellation scope, provider UUID, canonical live database path, topology and
storage/inventory digest, height-pinned chain membership, and canonical backup
destination. It expires no later than `-proof-timeout`. The target and expiry are
rechecked before backup, immediately before mutation, and inside the bbolt write
transaction; changing any bound fact or allowing the capability to expire leaves
the published backup intact and refuses the migration.
The capability is consumed immediately before exact-backup creation. It also
binds the backup parent's physical identity, so renaming, unmounting, or
recreating that directory cannot redirect publication after remote evidence was
collected. Copies share the same one-shot state. Any backup or later failure
therefore requires a new complete proof, drain attestation, capability, and
no-overwrite backup path; only failures before that admission boundary may
reuse the still-live proof.

1. Every surviving backend-reported lease has exactly one unambiguous confirmed
   placement row, and every confirmed placement intended to survive is present
   in the backend inventory.
2. Each row names the exact backend that reported the lease and is still a
   revision-zero v0.13.0 record. A missing row, backend mismatch, attempt,
   conflict, malformed record, or already-revisioned record is not eligible for
   automatic legacy adoption. The revision-epoch check is global: one
   nonzero-revision row, or one `{`-prefixed row whose revision header cannot be
   decoded, invalidates the first-open provenance proof for every owner in the
   file, not just that lease. Other malformed revision-zero rows remain
   lease-local but are still ineligible themselves.
3. Every placement or backend-inventory survivor—including a retention-only
   lease—appears in the pinned all-state chain snapshot for the exact configured
   provider. Each provision inventory preserves its backend-reported provider
   UUID; every non-empty reported value must also equal the configured provider.
   Empty provider values remain valid evidence for old v0.13 backend records,
   but cannot substitute for chain membership. For every live provision, the
   complete backend-reported `Items` must also equal the pinned chain workload
   in SKU, quantity, and normalized service name. Item order is not authority;
   a single historical unnamed service normalizes to `app`. A nonempty observed
   effective custom domain must equal the chain value, while an empty observed
   domain may represent v0.13's documented DNS-readiness deferral. This exact
   quantity check is what detects a locally dense but truncated Docker cohort;
   terminal chain-only history remains membership evidence and is not forced
   through current live-workload quantity/service validation.
4. Both `placement_lifecycle_capabilities` and `placement_metadata` are absent.
   Their joint first creation, combined with the absence of any revisioned row
   or JSON-object-shaped row whose revision header cannot be decoded, is the
   one-time provenance proof that permits Fred to adopt revision-zero owners as
   ID-empty legacy bindings before assigning revisions.

Abort the cutover on any discrepancy. If either new bucket already exists, any
placement row is revisioned, or any JSON-object-shaped row has an undecodable
revision header, the file has already crossed (or cannot prove it has not
crossed) the first-open boundary; neither preparation nor upgraded startup will
backfill missing bindings. Stop and restore the known-good v0.13 snapshot, then
repeat the preflight. Do not attempt
to repair coverage from passive inventory or proceed with an empty replacement
database.

Only exit status zero together with the exact `PREPARED_FOR_CUTOVER:` verdict
from `-prepare` authorizes the provider cutover. Preparation
ends in-place old-binary rollback: reverting to v0.13 requires restoring the
emitted backup, and that backup must never be restored after upgraded side
effects begin. Normal upgraded startup opens only an existing fully prepared
database and performs no schema/bootstrap write.

`PREPARED_FOR_CUTOVER` is rendered last and the complete verdict is issued in one write after
the prepared database closes. If the command instead exits with a `PREPARED:`
error, the migration transaction already succeeded before a later sync, close,
or verdict-reporting failure. Do not blindly rerun `-prepare`, do not infer that
the v0.13 rows remain, and do not start `providerd`. Immediately run
`placement-repair -config /etc/fred/config.yaml -classify` against the stopped file,
preserve both it and the mandatory backup, and resolve storage health and the
actual durable state before choosing forward progress or a safe restore.

After preparation, replace v0.13.0 and start exactly one new providerd. Do not
run the two providerd versions concurrently. Existing stack-form v0.13 tenant
containers whose complete authority passes the checks above are not restarted
or moved merely for this upgrade. The older service-name-less Docker cohort is
the explicit exception: on its first upgraded backend start Fred stops and
renames the original generation to exact `-prev` names, renames any managed
volume parent to the service-aware name, and recreates the `app` service through
Compose. Plan bounded per-lease downtime for that migration; a proven
rollback-only cohort resumes the same idempotent sequence. Keep provider and
tenant ingress fenced until backend startup and the chain/workload placement
proof succeed. Provider startup reconciliation then validates backend inventory
against the migrated placement index before it enables new lifecycle side
effects. It cannot reconstruct a missing legacy lifecycle binding.

The upgraded backends still expose HMAC-authenticated legacy mutation paths.
Those paths do not carry the new storage identity, so fencing the stopped v0.13
provider and replacing its global key with the unique `K_i` set are security
boundaries, not mere housekeeping. After the new provider is healthy, revoke
its predecessor's mTLS credential too; do not leave a bootable old provider
with any current per-backend key or transport credential.

Keep tenant API ingress out of the load balancer until all of these hold from
the same new process:

1. `GET /readyz` returns 200 **and its top-level `status` is `healthy`**. HTTP
   200 alone is insufficient because a remote dependency failure is `degraded`
   and deliberately also maps to 200.
2. The response reports `checks.placement_inventory.status=healthy`, plus
   healthy chain and configured-backend checks.
3. `fred_reconciler_sweep_complete == 1` after that health sample.

The callback path itself must remain reachable while this cutover gate is
closed. This first complete projection is mandatory for a v0.13.0 placement
database (and for any new or topology-changed database) before degraded
admission is safe. Once established, `placement_inventory` is a durable baseline
bound to the configured backend identities and survives provider restarts and
transient inventory outages.

After bootstrap, `fred_reconciler_sweep_complete` is only a last-sweep
observation. During a partial sweep, the reconciler may place genuinely new
recordless `PENDING` work only on its typed intersection of nodes that answered
both inventories. It defers recordless `ACTIVE` recovery and work tied to a
silent owner, attempt, or conflict. The tenant event path has no per-sweep
witness: it requires the same durable topology baseline, live-routes within that
topology using backend stats, and records its exact write-ahead attempt before
dispatch. Inventory silence never clears an attempt or conflict in either path.

The callback protocol can preserve state inherited from v0.13.0, but this does
not make a mixed-version rollout supported. During the stopped cutover, upgrade
every backend before providerd while all old processes remain fenced. Each new
backend requires a drained legacy callback outbox and sealed storage identity,
then recovers the operationless `callback_url` already embedded in a v0.13
workload, derives its tokenless lifecycle route, and reports its generation
class in internal provision inventory without exposing the URL. Mandatory
offline preparation migrates the matching confirmed placement as an explicitly
legacy owner before the new provider starts. Stack-form workloads keep reporting
lifecycle observations without replacement; the deliberate service-name-less
Docker migration recreates its containers while preserving the exact tokenless
callback pair on the replacement generation. An absent or rebuilt placement
database is never a recovery path for those workloads.

Do not interpret that installation order as permission to roll the fleet. A
v0.13.0 backend does not understand `lifecycle_callback_url`; if paired with a
new provider it reuses the now-expired operation URL for later restart, failure,
and teardown observations. Stop and drain the old fleet, upgrade and restart all
backends with the new per-backend keys, verify that they recovered their
existing provisions, complete offline placement preparation, and only then
start the new providerd.

New exact completion URLs add a canonical UUIDv4 `operation_id`; current
lifecycle URLs add a separately typed `lifecycle_id`. Backends must continue
treating either complete URL as opaque and sign the complete request URI when
posting it. The exact operation registry is intentionally process-local, but a
callback after provider restart can reacquire its Registry lease and settle
through the exact durable placement Attempt. If positive inventory carrying the
exact paired typed generation already promoted that Attempt, the durable
lifecycle generation remains
the exact recovery capability. Only a nonmatching old operation is acknowledged
and ignored. Confirmed lifecycle capability bindings likewise remain durable
across provider restarts. Tokenless
v0.13.0 callbacks remain a status-only compatibility path only for owners
explicitly migrated as legacy. When no matching confirmed placement owner
remains, either kind narrows to teardown-only authority: runtime observations
are ignored, maintenance cannot reissue it, and only exact terminal
deprovision can durably consume it before the best-effort retained notice. If
the placement is already absent, that consume deletes the capability at the
at-most-once boundary; a duplicate is therefore missing rather than retired.
If a placement or newer attempt still exists, retirement remains durable until
its later settlement or placement deletion can safely prune it. Active
teardown-only capabilities have no fixed TTL because backend retry horizons are
configurable; they remain as outstanding authority until exact terminal
consumption or authoritative conflict cleanup. In particular, if reconciliation
removes an orphan's placement after the chain is terminal and every backend
reports the lease absent, no backend may remain to send that terminal callback;
the teardown-only record can then persist indefinitely. This is deliberate
fail-closed residual authority: it cannot publish runtime success or failure,
be reissued for maintenance, or mutate placement or chain state, and passive
inventory cannot rotate it. Do not delete it merely because inventory is absent;
it preserves authorization for a delayed exact deprovision observation.
Complete backend inventory and
level-triggered reconciliation recover only from positive backend evidence;
absence never clears an ambiguous pre-restart attempt or conflict.

#### Inspecting placement repair targets

With `providerd` stopped, use `placement-repair` itself to discover the exact
durable facts needed by either repair mode:

```bash
placement-repair -config /etc/fred/config.yaml -list
placement-repair -config /etc/fred/config.yaml -inspect \
  -lease 018f47a2-8b1c-7def-8123-456789abcdef
```

These modes open the existing bbolt file read-only under a shared offline lock,
perform no backend requests, and emit JSON containing the durable topology plus
the exact lease, state, owner, attempt backend, operation ID and kind, restore
source UUID or provision payload hash when present, immutable request tenant,
provider UUID and ordered backend items, revision, conflict candidate set, and
an always-present `untrusted_positive` boolean. `true` identifies a quarantine
created from positive membership in a rejected inventory response; it is not an
authoritative owner. They cannot produce a mutation capability. A later
mutating command must reopen the stopped database exclusively and independently
match the exact current row; copying an old inspection result cannot bypass that
check. Keep the inspection output with the incident record.

#### Repairing one causally drained placement attempt

`placement-repair` is a narrow last resort for an exact provision or restore
request that returned ambiguously, whose callback can no longer arrive, and
whose backend effect operators have independently proved cannot occur. Do not
use it merely because a lease is absent from inventory: absence is only a
point-in-time cross-check and cannot prove that a delayed request will not
commit later.

First obtain the exact `lease_uuid`, attempted `backend`, and `operation_id`
from the immediate ambiguous provision/restore WARN or ERROR log. Current logs
emit all three fields together. Correlate that operation with the backend's
durable callback queue and transport logs; if the exact UUID cannot be
recovered, do not guess and do not repair the row. Then:

1. Remove tenant and chain-event ingress, stop `providerd`, and leave it stopped.
   The tool opens the existing placement database exclusively and refuses to
   run if the daemon or another writer holds it.
2. Drain or cancel every delayed request/effect path to the attempted backend
   and drain callback replay for that exact operation. This causal drain is an
   operator proof; no Fred inventory endpoint can establish it automatically.
3. Keep every backend from the stopped provider configuration reachable. Run
   the default dry-run with the same config and environment overrides:

   ```bash
   placement-repair \
     -config /etc/fred/config.yaml \
     -lease 018f47a2-8b1c-7def-8123-456789abcdef \
     -backend backend-a \
     -operation-id 550e8400-e29b-41d4-a716-446655440000
   ```

   Dry-run is byte-for-byte nonmutating. It requires the canonical current
   typed attempt and exact durable/configured topology, then fetches complete
   fresh `/provisions` and `/retentions` inventories from every configured
   backend. An attempt-only target must be absent everywhere. If the attempt
   preserves a confirmed owner, the only allowed positive target is one active
   provision on that same backend whose explicit lifecycle generation exactly
   matches the preserved pre-attempt authority: the same typed UUID, or an
   explicit legacy observation for a preserved legacy owner. The attempted
   generation, an unknown/unusable/mismatched generation, a report from another
   backend, any retention, or a silent/incomplete/extra backend refuses the
   repair. Any allowed preserved provision must also report the exact stopped
   store provider UUID and, when the durable attempt contains a tenant, that
   exact tenant. The output prints the tuple-bound confirmation value.
4. Only after independently completing step 2, repeat the same command with
   the exact values printed by dry-run. Choose a new backup pathname on
   protected storage; `placement-repair` refuses to overwrite any existing
   directory entry:

   ```bash
   placement-repair \
     -config /etc/fred/config.yaml \
     -lease 018f47a2-8b1c-7def-8123-456789abcdef \
     -backend backend-a \
     -operation-id 550e8400-e29b-41d4-a716-446655440000 \
     -apply \
     -backup /var/backups/fred/placements-before-attempt-repair.db \
     -confirm 'refuse-attempt:018f47a2-8b1c-7def-8123-456789abcdef:backend-a:550e8400-e29b-41d4-a716-446655440000' \
     -attest-drained 'I attest all delayed backend requests/effects and callback replay are drained'
   ```

The mutating path repeats the complete backend probe and bbolt physical
consistency check, then publishes, fsyncs, byte-verifies, and physically checks a
byte-for-byte copy of the exclusively locked database. While holding mutation
admission after that backup, it probes the complete fleet again and requires
both the target verdict and full inventory digest to match the initial evidence
before its single exact refusal transaction. Backup publication is mandatory and
no-overwrite; failure leaves the repair transaction unattempted. The tool
refuses a stale/newer operation or any tuple mismatch, preserves an existing
confirmed owner, verifies the exact attempt is gone, and syncs and closes the
database, reopens it read-only, and rechecks physical and exact semantic
postconditions before printing `PASS`.

The repair `-timeout` is an inventory-evidence freshness deadline through
mutation admission, not a claim that Go can cancel blocking backup I/O. The tool
binds attempt evidence to that exact cancellation scope and deadline, checks it
immediately before and after backup publication, again at mutation admission,
and from inside the bbolt transaction. Evidence cannot be transplanted to a new
context or reused after expiry. The descriptor-relative no-replace rename is
the publication boundary. The tool retains and re-attests the exact published
inode and bound parent, SHA-256 bytes, length, exact `0600` mode, and single-link
status before mutation, after mutation, and before `PASS`; a parent
rename/recreation, backup-entry substitution, in-place content change, mode
change, or hard-link alias cannot be mistaken for a valid rollback image. An
error after publication (including later validation,
sync, close, or cleanup of the backup itself, or a later final-proof/pre-commit
failure) is `BACKUP PUBLISHED` and means no repair mutation committed;
preserve that artifact, rerun the dry-run/proof, and supply a different new
`-backup` path.

#### Resolving one causally drained placement conflict

An ordinary durable conflict means at least two backends have positively
reported the same lease. A rejected inventory response can instead create an
`untrusted_positive` quarantine with only one candidate: Fred preserves the raw
membership fact while refusing to trust that endpoint's ownership payload. A
sole candidate of that exact kind may self-resolve when a later **complete**,
identity-valid projection positively reports the same lease from the same
backend. A partial projection, silence, a different reporter, more than one
candidate, unknown ownership, or an ordinary conflict never self-resolves.
Reconciliation intentionally retains the union in all those operator-only
cases: later point-in-time absence cannot prove that a delayed request, effect,
or callback on an absent candidate has been eliminated.

1. Stop ingress and `providerd`, then use `-inspect` to record the exact revision
   and complete candidate set. All candidates must still belong to the durable
   and configured topology; unknown-owner legacy/corrupt conflicts are not
   eligible for this repair.
2. Probe and drain every candidate's delayed request/effect paths and callback
   replay. This causal proof is mandatory even when fresh inventory currently
   shows only one owner.
3. Run a dry-run selecting the backend that actually holds the lease:

   ```bash
   placement-repair \
     -config /etc/fred/config.yaml \
     -resolve-conflict \
     -lease 018f47a2-8b1c-7def-8123-456789abcdef \
     -backend backend-a
   ```

   The tool holds the database exclusively while collecting complete fresh
   `/provisions` and `/retentions` inventories from every configured backend. It
   requires the target to occur exactly once on the selected backend and nowhere
   else, rejects malformed/noncanonical/duplicate inventories, and rechecks the
   exact revision, topology identity, and sorted durable candidate set. The
   resulting opaque, expiring repair plan is the only conflict-mutation
   capability. Its printed confirmation digest binds the lease, selected
   backend and immutable storage identity, provider, authoritative tenant when
   available, active-versus-retained evidence kind, lifecycle kind and ID,
   topology identity, revision, complete candidate set, and digest of the full
   fleet inventory.
4. Only after step 2, repeat the printed command with `-apply`, a new protected
   `-backup /var/backups/fred/placements-before-conflict-repair.db` path, the
   exact `-confirm` value, and:

   ```text
   -attest-drained 'I attest all delayed backend requests/effects and callback replay are drained'
   ```

An active provision must report the exact stopped-store provider UUID. When a
durable attempt snapshot supplies the tenant, the active observation must also
match that tenant exactly. Only identity-authoritative active evidence with an
explicit typed generation receives that exact lifecycle authority; an explicit
legacy generation remains tokenless legacy. An active provision with
unknown/unusable or identity-incomplete evidence is restored as routing affinity
with lifecycle authority quarantined. Retained inventory now carries provider
and tenant where the backend can prove them; older or third-party retained
evidence without both identities can still repair routing, but never lifecycle
authority. A mismatching nonempty provider or tenant fails closed. Retention is
therefore never promoted into runtime callback authority.

After all proof gates pass, the tool physically validates the source and uses
the already-bound backup-parent capability to publish and fsync the mandatory
byte-exact, no-overwrite pre-mutation backup. It retains that exact published
inode and verifies its parent, inode, SHA-256 bytes, length, exact `0600` mode,
and single-link status immediately before mutation, then—while
holding placement mutation admission—re-probes every backend and requires every
plan-bound fact and the complete inventory digest to remain identical. Context
cancellation or plan expiry is checked again inside the bbolt transaction. The
tool repeats that complete backup check after the transaction and before the
verdict, then syncs, closes, reopens read-only, and physically and semantically
verifies the repaired database before printing `PASS`.

For either repair mode, keep `providerd` stopped until the outcome is resolved.
`BACKUP PUBLISHED` means the destination inode crossed the atomic no-overwrite
publication boundary but no repair mutation committed. A later durability,
content, physical-consistency, parent, or inode check may have failed, so
preserve it but do not assume it is a usable rollback image until read-only
inspection succeeds; repeat the proof with a new path.
If the live mutation committed before a later complete backup recheck or
verdict failed, the result is instead `COMMITTED:`. `OUTCOME UNKNOWN`
means bbolt `Commit` returned an error and the repair may or may not be visible;
preserve both files, do not retry or restore, and inspect the live database.
An ordinary error without any of those prefixes or `COMMITTED:` means the tool
did not observe a successful repair transaction. An error beginning with
`COMMITTED:` is categorically different: the mutation transaction succeeded, but
post-commit verification, sync, close, reopen verification, or `PASS` reporting
failed, so retrying could target already-changed authority.
Do not retry and do not assume the old row remains. Immediately run the
read-only inspection while the daemon remains stopped, for example:

```bash
placement-repair -config /etc/fred/config.yaml -inspect \
  -lease 018f47a2-8b1c-7def-8123-456789abcdef
```

Retain both the live file and the tool-created backup until that inspection and
storage-health investigation establish the actual durable result. Restore only
from a proven safe snapshot under the rollback rules below.

> **Rollback boundary:** the stopped v0.13.0 backup is the only supported direct
> downgrade point. Preparation itself writes the new schema, provider binding,
> and backend identity pins; v0.13 therefore requires restoring the emitted
> backup even before the new daemon starts. Once the new providerd starts,
> startup reconciliation may persist new placement facts or issue side effects
> before tenant traffic is accepted. Never open a
> placement database touched by the new version with v0.13.0; the old reader does
> not understand unresolved attempts, conflict quarantine, or operation IDs and a
> later old-version write can silently discard those safety facts. To roll back,
> stop the new process and restore the pre-upgrade backup only if you can also
> prove no chain or backend lifecycle state changed after that snapshot. Otherwise
> forward-fix.

The backend storage-lineage seal has its own rollback boundary. Roll Docker's
marker pair, `callbacks.db`, `releases.db`, `retention.db`, and substrate back as
one stopped snapshot; for k3s the corresponding set is its marker pair,
`callbacks.db`, `releases.db`, and cluster state. A current binary rejects an
individually missing, foreign, cross-kind, or path-replaced store, but it cannot
distinguish a complete matching snapshot from the original lineage. Fence the
original before starting a restored copy. Even files that each carry the same
UUID must not be assembled from different snapshot times: the binding proves
lineage and role, not cross-file temporal consistency. Diagnostics may be
restored or recreated independently because they hold no lifecycle authority.

The docker backend's callback database also crosses a schema rollback boundary. Before
the first provision/restore substrate mutation, new binaries write an immutable
per-lease intent below `pending_callback_operation_intents`; terminal handling
atomically replaces it with the exact callback delivery. Before restart, update,
or custom-domain replacement, they write a separate intent below
`pending_callback_maintenance_intents`. Its UUIDv4 and request snapshot remain
immutable while a typed phase advances from cancelable pre-append authority to
append-started authority and then binds the exact source/target Release pair and
every target container. Exact terminal classification atomically replaces it
with a non-coalescible maintenance completion. New binaries also write a
non-expiring deprovision finalizer below `pending_callback_close_intents` before
the first destructive side effect. Its identity, full close snapshot,
active-release fence, and legacy rollback container IDs remain immutable, while
its typed `cleanup_attempts` progress advances durably. Admission atomically
converts any preempted
operation or maintenance intent into its exact failed completion, and later
operation/maintenance admission is blocked while the close exists. Close recovery runs before ordinary
exact-cohort validation, persists cleanup-attempt progress, and retires the
release fence before atomically enqueueing the lifecycle result and deleting the
close row. Only then may its volatile projection disappear.

New binaries also write explicitly typed deliveries with a durable monotonic sequence to a
per-lease nested bucket below `pending_callbacks_v2`. Exact operation and
maintenance completions remain FIFO; newer lifecycle observations coalesce only
older typed lifecycle observations and cannot pass or delete an older exact or
legacy entry for that lease.
An undelivered exact maintenance completion is also a lease-local admission
fence: the next restart, update, or custom-domain replacement returns retryable
409 until a synchronous 2xx precisely removes that delivery. This prevents a
terminal result on the stable lifecycle route from appearing after a newer
maintenance start; it does not pause other leases.
The queue retries during runtime as well as at startup. Corruption below an
identifiable lease bucket stops that lease without blocking unrelated lease
replay, but remains visible through unhealthy backend status and is never
silently deleted by replay or TTL cleanup. v0.13.0 intentionally ignores the v2
bucket. Rolling only the backend binary back does not create a replay loop, but
v2 callbacks remain undelivered until the new binary is restored. Preserve the
current `callbacks.db` and matching `releases.db` if they must survive for later
forward recovery; restoring the pre-upgrade snapshot discards accepted
operation/maintenance intents, close finalizers, and callbacks recorded after
that snapshot. v0.13 understands none of the new intent buckets or v2
deliveries. A pending operation intent may describe substrate work whose
callback was never enqueued; a pending maintenance intent may identify a target
generation that is partial or already committed; a pending close may own
teardown that is already partial; and a pending exact completion may be Fred's
only settlement evidence. Any one is a reason to forward-fix rather than
downgrade.

Treat `retention.db` and `releases.db` as the same restore-ownership boundary.
A `restoring` source row fences Provision, Restore, Update, and custom-domain
redeploy of its destination; before commit and exact restore-intent settlement it
fences plain Restart too. An exactly matching active destination Release proves
that the restore already committed—even if containers subsequently failed or
disappeared. In the zero-survivor case the source row deliberately remains as
tenant/provider identity while the destination recovers Failed with its exact
allocation. Once the exact restore intent is settled, a plain
identity-preserving Restart may repair it; reaching Ready lets reconciliation
consume the row and admit ordinary maintenance. Alternatively, close transfers
it to a full close intent before teardown. Restoring only one file can turn a
committed runtime failure into apparent pre-commit rollback or erase the only
source identity. Roll them back only with the matching callback store, managed
volumes, containers, and storage identity.

Schema compatibility is store-specific; the bbolt files carry no global schema
version or automatic downgrade guard. In particular, the placement store
retains an undecodable placement or lifecycle-capability entry as per-lease
unusable, fail-closed safety evidence rather than dropping it during cleanup or
making the whole provider unstartable. A confirmed revisioned placement with a
missing capability is isolated the same way and passive inventory cannot mint a
replacement token; a later exact typed operation may safely establish new
authority. Do not assume an older Fred can safely read a file after a newer
version has written it. Follow the release-specific notes and take a
stopped-process backup before upgrading.

> **Upgrading an XFS backend from before ENG-454/ENG-459:** it may carry orphaned XFS project-quota table entries left by volumes destroyed under the older build. New leaks are prevented going forward, but pre-existing entries are not swept automatically — `xfs_quota report -p` is filesystem-global, so Fred cannot distinguish its own orphaned entries from live foreign project limits. Clear each stale project ID with a one-time manual `xfs_quota -x -c 'limit -p bhard=0 bsoft=0 ihard=0 isoft=0 <projid>' <mount>` (matching what `Destroy` does).
>
> **Downgrading after ENG-548:** once a volume has been provisioned with an inode limit (`ihard`), rolling back to a pre-ENG-548 binary — whose `Destroy` clears only `bhard`/`bsoft` — leaves `ihard` in place, silently reintroducing the ENG-459 stuck-entry leak. `fred_docker_backend_volume_quota_clear_failed_total` will **not** fire in this case, since the old binary's partial clear still reports success, so the alert-driven runbook above won't catch the regression. The remedy is the same ENG-459 operator sweep, using the four-limit clear command above.

---

## Docker images

`goreleaser` publishes only the `providerd` image (`ghcr.io/manifest-network/fred`, built from `Dockerfile.goreleaser`). The `docker-backend` and `k3s-backend` images are **not** published — build them locally from the multi-stage `Dockerfile` below, or run `docker-backend` directly from its released binary archive.

The repo ships a multi-stage `Dockerfile` with three named targets:

```bash
# providerd
docker build --target providerd -t fred-providerd .

# docker-backend
docker build --target docker-backend -t fred-docker-backend .

# k3s-backend (experimental scaffold — see below; not for production)
docker build --target k3s-backend -t fred-k3s-backend .
```

The `docker-backend` stage is intentionally last in the `Dockerfile`, so a target-less `docker build .` defaults to producing the `docker-backend` image. `k3s-backend` is an experimental, non-functional scaffold (ENG-133) — it boots and serves the backend contract but provisions nothing (every provision is reported as failed); it is not for production. Full Kubernetes provisioning lands in ENG-134+.

All three run as the `nonroot` user (UID 65532) on a `gcr.io/distroless/static-debian12` base.

**`docker-backend`** — needs the Docker socket and a writable `/data` volume (which holds `callbacks.db`, `diagnostics.db`, `releases.db`, and the always-required `retention.db`; the image declares `WORKDIR /data` and `VOLUME /data`). Use a named Docker volume so permissions are handled automatically; for a host bind mount, the directory must be owned by UID 65532. Use `--group-add` with the host Docker group ID so the nonroot user can talk to the socket:

```bash
DOCKER_GID=$(getent group docker | cut -d: -f3)

# One time for a genuinely new, empty backend. Keep this same volume and Docker
# substrate for the normal start below.
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd)/docker-backend.yaml:/data/docker-backend.yaml:ro \
  -v db-data:/data \
  --group-add ${DOCKER_GID} \
  fred-docker-backend --config /data/docker-backend.yaml \
    --initialize-storage-identity new

docker run -d --name docker-backend \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd)/docker-backend.yaml:/data/docker-backend.yaml:ro \
  -v db-data:/data \
  --group-add ${DOCKER_GID} \
  -p 9001:9001 \
  fred-docker-backend --config /data/docker-backend.yaml
```

**`providerd`** — has no built-in volume; mount the config and (if needed) keyring directory wherever you like:

```bash
docker run -d --name providerd \
  -v $(pwd)/config.yaml:/config.yaml:ro \
  -v $(pwd)/keyring:/keyring:ro \
  -p 8080:8080 \
  fred-providerd --config /config.yaml
```

Configure `keyring_dir: /keyring` in `config.yaml`. Set
`placement_store_db_path` and mount its writable host directory; providerd will
not start without it. Do the same for any configured `token_tracker_db_path` or
`payload_store_db_path`; the providerd image does not declare a default data
volume.
