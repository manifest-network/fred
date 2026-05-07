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
| Disk | Image cache + per-tenant volumes (see [Stateful workloads](#stateful-workloads)) |
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
- `placement_store_db_path` — required only when multiple backends share `skus`
- `production_mode: true` — strongly recommended in production (forces replay protection, blocks SSRF, blocks `grpc_tls_skip_verify`)
- `token_tracker_db_path` — required when `production_mode: true`

**`docker-backend.yaml`**:

- `host_address` — public IP or hostname tenants will use to reach containers
- `callback_secret` — must match `providerd`'s value
- `sku_mapping` — maps on-chain SKU UUIDs to local profile names (provisioning fails otherwise)
- `volume_data_path` — required when any SKU has `disk_mb > 0`

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

`docker-backend.service` is the same shape with two differences:
- It needs Docker socket access. Either add `SupplementaryGroups=docker` to the unit (so the service user inherits the `docker` group), add the service user to the `docker` group out of band, or run as root.
- `ReadWritePaths` should cover the directories holding `callback_db_path`, `diagnostics_db_path`, `releases_db_path`, and `volume_data_path`.

`TimeoutStopSec` should comfortably exceed `shutdown_timeout` from your config (default 30s) so systemd doesn't SIGKILL during graceful drain.

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

Configure `trusted_proxies` in `config.yaml` to include the proxy's IP CIDR so per-IP rate limiting uses the real client address from `X-Forwarded-For`. Untrusted `X-Forwarded-For` headers are ignored.

### 2. providerd → backend (HMAC-authenticated)

If your backends are on private networks, plain HTTP between providerd and docker-backend is acceptable; the HMAC signature provides authentication and integrity. For backends across untrusted networks, run docker-backend behind TLS-terminating proxies and use `https://` in the `backends[].url`.

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

## Multi-host setup with round-robin

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

Identical `skus` lists on `docker-1` and `docker-2` make them a round-robin pool. `placement_store_db_path` is **required** so that read operations (connection details, logs) hit the backend that actually holds each lease.

To add a host: spin up another `docker-backend` with the same `skus` list, add it to `config.yaml`, and reload `providerd`. New provisions immediately distribute across the new host. To drain: remove its entry from `skus` (so it no longer receives new provisions) and let existing leases close naturally, or migrate them by deprovisioning + reprovisioning on the chain side.

---

## Backups

The bbolt files are the persistent state. None of them is a single point of truth — most can be rebuilt from the chain or the docker daemon — but warm backups speed up disaster recovery.

| File | Backup priority | Restore behavior |
|---|---|---|
| `<docker>/releases.db` | High — release history is not reconstructible | Lost on disk failure |
| `<docker>/diagnostics.db` | Medium — failure diagnostics for past 7 days | Lost on disk failure |
| `<docker>/callbacks.db` | Low — pending callbacks, ephemeral | Bbolt rebuilds; some callbacks lost |
| `placement_store_db_path` | Low — rebuilt by reconciler from backend `ListProvisions` on startup | Brief misroute window during rebuild |
| `payload_store_db_path` | Low — pending payloads, tenant can re-upload | Tenants must re-upload pending payloads |
| `token_tracker_db_path` | None — replay protection has 30s window anyway | Empties on restart, acceptable |

Backups must be taken with the daemon stopped (bbolt's file lock will refuse concurrent open). For zero-downtime backups, use filesystem-level snapshots (LVM, ZFS, btrfs) — bbolt files are crash-consistent.

---

## Upgrades

Fred releases are tagged on GitHub with binaries via `goreleaser`. The release process is:

1. Tag a release on GitHub. CI builds binaries and Docker images.
2. Pull the new binary or image to your hosts.
3. Restart `providerd` and each `docker-backend`. Order does not matter — the HMAC handshake is symmetric.

Fred does not currently support graceful in-place upgrades. The startup sequence (chain reconnect, reconciliation, accept callbacks) takes seconds; the brief downtime is generally acceptable. For zero-downtime, use the multi-instance pattern under [Secret rotation](#secret-rotation).

Schema migrations: bbolt schemas are versioned implicitly; Fred handles forward migrations transparently. Backward compatibility (rolling back to an older Fred) is not guaranteed once a newer version has written to the bbolt files.

---

## Docker images

The repo ships a multi-stage `Dockerfile` with two named targets:

```bash
# providerd
docker build --target providerd -t fred-providerd .

# docker-backend
docker build --target docker-backend -t fred-docker-backend .
```

Both run as the `nonroot` user (UID 65532) on a `gcr.io/distroless/static-debian12` base.

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
