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

No special setup required. Containers run with read-only rootfs and tmpfs at `/tmp` and `/run`. The image's `VOLUME` paths are also overridden with tmpfs to prevent accidental anonymous volumes.

### Stateful workloads (`disk_mb > 0` SKUs)

The docker-backend places each container's data on a quota-enforced host directory. Pick one of three filesystems for `volume_data_path`:

#### btrfs (recommended for ease of use)

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

#### xfs (good for large fleets)

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

#### zfs (best for snapshots/backups)

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
# volume_filesystem auto-detects from the mount; set explicitly to override:
# volume_filesystem: "btrfs"
```

The docker-backend refuses to start if any SKU has `disk_mb > 0` and `volume_data_path` is unset.

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
- `callback_secret` — shared HMAC secret (32+ chars; same value used by every backend)
- `backends` — at least one entry with `name`, `url`, and either `skus` or `default: true`
- `placement_store_db_path` — strongly recommended when multiple backends share `skus`. It is not startup-enforced: if unset, providerd logs a warning and read operations (connection details, logs) fall back to a fan-out across matching backends that may route incorrectly
- `production_mode: true` — strongly recommended in production (forces replay protection, blocks SSRF, blocks `grpc_tls_skip_verify`)
- `token_tracker_db_path` — required when `production_mode: true`

**Environment variables** (providerd):

- `FRED_KEYRING_PASSPHRASE` — **required** when `keyring_backend` is `file` (the default); the signer pool hard-fails at startup without it. Not needed for `keyring_backend: test` or `os`. Pass it via a systemd `EnvironmentFile=` (see [Process management](#process-management-systemd)), not `Environment=`, so it isn't exposed via `systemctl show`.
- `FRED_MNEMONIC` — first-boot only; **required** when `sub_signer_count > 0` so the signer pool can derive its sub-signer keys. Without it the pool runs single-signer (logged as a warning, no hard fail). It can be unset on later boots once the sub-keys persist in the keyring.

**`docker-backend.yaml`**:

- `host_address` — public IP or hostname tenants will use to reach containers
- `callback_secret` — must match `providerd`'s value
- `sku_mapping` — maps on-chain SKU UUIDs to local profile names (provisioning fails otherwise)
- `volume_data_path` — required when any SKU has `disk_mb > 0`

**Environment variables** (docker-backend / k3s-backend) — each overrides the corresponding YAML field:

- `DOCKER_BACKEND_ADDR` / `K3S_BACKEND_ADDR` — override `listen_addr`
- `DOCKER_BACKEND_CALLBACK_SECRET` / `K3S_BACKEND_CALLBACK_SECRET` — override `callback_secret` (lets you keep the secret out of the on-disk YAML)
- `DOCKER_BACKEND_HOST_ADDRESS` / `K3S_BACKEND_HOST_ADDRESS` — override `host_address`
- `DOCKER_BACKEND_MAX_REQUEST_BODY_SIZE` / `K3S_BACKEND_MAX_REQUEST_BODY_SIZE` — override the 2 MiB request-body cap (a non-positive value is ignored)
- `DOCKER_HOST` (docker-backend) — Docker daemon endpoint, per the standard Docker convention
- `KUBECONFIG` (k3s-backend) — path to the kubeconfig

---

## Secret rotation

The HMAC `callback_secret` is shared between `providerd` and every backend. Because it is verified on both directions (Fred → backend and backend → Fred), rotation requires coordinated restart:

1. Stop traffic (or accept brief 401s during rotation).
2. Update `callback_secret` in `config.yaml` and **every** `docker-backend.yaml`.
3. Restart **every `docker-backend`** first, then `providerd` last. Rationale: with a fleet of backends, the rolling restart is the longer operation; finishing with `providerd` minimizes the total window where the old and new secrets are both in play. During the rotation window, in-flight requests in both directions will see 401s — the backend's persistent callback queue and Watermill's redelivery on the providerd side recover automatically once both ends are on the new secret.
4. Verify with a test provision.

There is no built-in support for two-secret rotation (active + previous). If you need zero-downtime rotation, deploy a second `providerd` with the new secret on a separate hostname and migrate traffic.

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
- `ReadWritePaths` should cover the directories holding `callback_db_path`, `diagnostics_db_path`, `releases_db_path`, `volume_data_path`, and — when retention is enabled — `retention_db_path`.

`TimeoutStopSec` should comfortably exceed the graceful-drain window so systemd doesn't SIGKILL mid-shutdown. For `providerd` this window is `shutdown_timeout` from your config (default 30s); the `docker-backend` uses a fixed 30s drain (not configurable).

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

The HMAC signature on this hop always provides authentication and integrity, so on a trusted private network plain HTTP is acceptable. For confidentiality (and across untrusted networks), enable transport TLS — both directions are independently configurable.

**Recommended: native TLS/mTLS (ENG-103).** The docker-backend can serve TLS directly, and providerd can verify it (and optionally present a client certificate for mutual TLS). TLS is plaintext-by-default — the transport stays HTTP until you configure it — and when enabled, TLS 1.3 is pinned as the minimum version (`MinVersion`; no maximum is set).

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

Empty client TLS fields fall back to Go defaults (system root CAs, no client certificate). `tls_skip_verify` disables server-certificate verification and is for testing only — it is rejected when `production_mode: true`.

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
backends:
  - name: docker-1
    url: "http://10.0.0.1:9001"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"
    default: true
  - name: docker-2
    url: "http://10.0.0.2:9001"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"

placement_store_db_path: "/var/lib/fred/placements.db"
```

Identical `skus` lists on `docker-1` and `docker-2` make them a matching pool for those SKUs. Fred routes each new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats. `placement_store_db_path` should be set so that read operations (connection details, logs) route directly to the backend holding each lease; it is not startup-enforced — without it, providerd logs a warning and reads fall back to a fan-out across matching backends.

To add a host: spin up another `docker-backend` with the same `skus` list, add it to `config.yaml`, and reload `providerd`. New provisions immediately distribute across the new host. To drain: remove its entry from `skus` (so it no longer receives new provisions) and let existing leases close naturally, or migrate them by deprovisioning + reprovisioning on the chain side.

---

## Backups

The bbolt files are the persistent state. None of them is a single point of truth — most can be rebuilt from the chain or the docker daemon — but warm backups speed up disaster recovery.

| File | Backup priority | Restore behavior |
|---|---|---|
| `<docker>/releases.db` | High — release history is not reconstructible | Lost on disk failure |
| `<docker>/retention.db` | High — index of soft-deleted leases (tenant, manifest, retained volume names); not reconstructible from chain or daemon | Lost on disk failure → retained volumes can no longer be restored *or* reaped (orphaned on disk). Only present when `retain_on_close` is enabled |
| `<docker>/diagnostics.db` | Medium — failure diagnostics for past 7 days | Lost on disk failure |
| `<docker>/callbacks.db` | Low — pending callbacks, ephemeral | Bbolt rebuilds; some callbacks lost |
| `placement_store_db_path` | Low — rebuilt by reconciler from backend `ListProvisions` on startup | Brief misroute window during rebuild |
| `payload_store_db_path` | Low — pending payloads, tenant can re-upload | Tenants must re-upload pending payloads |
| `token_tracker_db_path` | None — replay protection has 30s window anyway | Empties on restart, acceptable |

Backups must be taken with the daemon stopped (bbolt's file lock will refuse concurrent open). For zero-downtime backups, use filesystem-level snapshots (LVM, ZFS, btrfs) — bbolt files are crash-consistent.

---

## Upgrades

Fred releases are tagged on GitHub with binaries via `goreleaser`. The release process is:

1. Tag a release on GitHub. `goreleaser` publishes the `providerd` and `docker-backend` binary archives plus a single Docker image — `ghcr.io/manifest-network/fred` — which contains **only** `providerd`. There is no published `docker-backend` (or `k3s-backend`) image; run the `docker-backend` binary from its archive, or build its image locally (see [Docker images](#docker-images)).
2. Pull the new binary or image to your hosts.
3. Restart `providerd` and each `docker-backend`. Order does not matter — the HMAC handshake is symmetric.

Fred does not currently support graceful in-place upgrades. The startup sequence (chain reconnect, reconciliation, accept callbacks) takes seconds; the brief downtime is generally acceptable. For zero-downtime, use the multi-instance pattern under [Secret rotation](#secret-rotation).

Schema migrations: the bbolt files carry no explicit schema version, and Fred does **not** run schema migrations on them. An entry a newer build cannot decode is dropped (not migrated) the next time that store's age-based cleanup runs. Rolling back to an older Fred after a newer version has written is likewise not guaranteed. **Take a backup before upgrading.**

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

**`docker-backend`** — needs the Docker socket and a writable `/data` volume (which holds `callbacks.db`, `diagnostics.db`, `releases.db`; the image declares `WORKDIR /data` and `VOLUME /data`). Use a named Docker volume so permissions are handled automatically; for a host bind mount, the directory must be owned by UID 65532. Use `--group-add` with the host Docker group ID so the nonroot user can talk to the socket:

```bash
DOCKER_GID=$(getent group docker | cut -d: -f3)

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

Configure `keyring_dir: /keyring` in `config.yaml`. If you set `token_tracker_db_path`, `payload_store_db_path`, or `placement_store_db_path`, also mount a writable host directory (the providerd image does not declare a default data volume).
