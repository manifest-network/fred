# Docker Backend — Tenant Manifest Reference

> For provider/operator configuration (resource pools, SKU profiles, security hardening, etc.), see [README.md](README.md).

The manifest is a JSON document submitted by tenants as the lease payload. It describes the container to run. Resource limits (CPU, memory, disk) are determined by the SKU, not the manifest.

## Full Schema

```json
{
  "image": "nginx:latest",
  "ports": {
    "80/tcp": {},
    "8443/tcp": { "host_port": 9443 }
  },
  "env": {
    "APP_PORT": "8080",
    "DATABASE_URL": "postgres://db:5432/myapp"
  },
  "command": ["/usr/bin/myapp"],
  "args": ["--config", "/etc/myapp.yaml"],
  "labels": {
    "app": "myapp",
    "version": "1.2.3"
  },
  "health_check": {
    "test": ["CMD", "/usr/bin/myapp", "healthcheck"],
    "interval": "30s",
    "timeout": "5s",
    "retries": 3,
    "start_period": "10s"
  },
  "tmpfs": ["/var/cache/nginx", "/var/log/nginx"]
}
```

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `image` | string | **yes** | Container image reference (e.g., `nginx:latest`, `ghcr.io/org/app:v1`). Must be from an operator-approved registry. |
| `ports` | object | no | Port mappings. Keys are `"port/protocol"` (e.g., `"80/tcp"`, `"53/udp"`). Port must be 1-65535, protocol must be `tcp` or `udp`. |
| `env` | object | no | Environment variables as key-value string pairs. Some names are blocked (see below). |
| `command` | string[] | no | Overrides the container entrypoint. |
| `args` | string[] | no | Arguments passed to the command. |
| `labels` | object | no | Custom container labels. Cannot use the `fred.*` prefix (reserved). |
| `health_check` | object | no | Container health check configuration (see below). |
| `tmpfs` | string[] | no | Additional writable tmpfs mount paths (see below). |

## Ports

Each port entry maps a container port to the host. The key format is `"port/protocol"`:

```json
{
  "80/tcp": {},
  "8080/tcp": { "host_port": 9080 }
}
```

- **Without `host_port`** (or `host_port: 0`): Docker assigns a random available host port.
- **With `host_port`**: Binds to the specified host port. May fail if the port is already in use.

Use `GET /info/{lease_uuid}` after provisioning to discover the assigned host ports.

## Environment Variables

Tenants may set arbitrary environment variables with the following restrictions:

| Blocked Name/Prefix | Reason |
|---|---|
| `PATH` | Prevents command hijacking |
| `LD_*` | Dynamic linker variables (`LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, etc.) |
| `FRED_*` | Reserved for backend-internal use |
| `DOCKER_*` | Docker runtime variables |

Matching is case-insensitive. Variable names cannot be empty or contain `=` or null bytes.

## Health Check

Configures Docker's built-in container health checking. When present, the backend waits for all containers to report `healthy` before sending a success callback.

```json
{
  "test": ["CMD", "/usr/bin/myapp", "healthcheck"],
  "interval": "30s",
  "timeout": "5s",
  "retries": 3,
  "start_period": "10s"
}
```

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `test` | string[] | **yes** | — | Health check command (see test types below) |
| `interval` | duration | no | Docker default | Time between checks (e.g., `"30s"`, `"1m"`) |
| `timeout` | duration | no | Docker default | Timeout per check |
| `retries` | int | no | Docker default | Failures before marking `unhealthy` |
| `start_period` | duration | no | Docker default | Grace period during startup |

**Test types** (first element of `test`):

| Type | Example | Description |
|---|---|---|
| `CMD` | `["CMD", "/bin/check", "--flag"]` | Runs the command directly (exec form) |
| `CMD-SHELL` | `["CMD-SHELL", "curl -f http://localhost/health"]` | Runs via `/bin/sh -c` (shell form) |
| `NONE` | `["NONE"]` | Disables health checking |

Durations accept Go duration strings (`"30s"`, `"1m30s"`, `"500ms"`) or integer nanoseconds.

**Note:** A health check defined in the Dockerfile but not in the manifest does not trigger health-aware startup verification — the manifest is the contract.

## Tmpfs Mounts

When the operator enables read-only root filesystem (default), `/tmp` and `/run` are always mounted as writable tmpfs. Tenants can request up to 4 additional tmpfs mounts for application-specific writable directories:

```json
{
  "tmpfs": ["/var/cache/nginx", "/var/log/nginx"]
}
```

**Constraints:**

- Maximum 4 additional mounts (6 total including `/tmp` and `/run`)
- Paths must be absolute
- Each mount uses the operator-configured size limit (default 64 MB)
- Duplicate paths are rejected
- The following paths are blocked:

| Path | Reason |
|---|---|
| `/tmp` | Managed automatically by the backend |
| `/run` | Managed automatically by the backend |
| `/proc` | Sensitive kernel filesystem |
| `/sys` | Sensitive kernel filesystem |
| `/dev` | Sensitive kernel filesystem |
| `/` | Cannot mount root |

Subdirectories of blocked paths (e.g., `/proc/self`, `/sys/fs/cgroup`) are also rejected.

## Image Registry Allowlist

The container image must be from an operator-approved registry. The registry is extracted from the image reference using Docker's standard normalization:

| Image | Resolved Registry |
|---|---|
| `nginx` | `docker.io` |
| `nginx:latest` | `docker.io` |
| `myorg/myapp:v1` | `docker.io` |
| `ghcr.io/org/app:v1` | `ghcr.io` |
| `registry.example.com:5000/img` | `registry.example.com:5000` |

If the resolved registry is not in the operator's allowlist, the provision is rejected.

## Minimal Example

The simplest valid manifest:

```json
{
  "image": "nginx:latest"
}
```

## Web Application Example

```json
{
  "image": "ghcr.io/myorg/webapp:v2.1.0",
  "ports": {
    "8080/tcp": {}
  },
  "env": {
    "APP_ENV": "production",
    "LOG_LEVEL": "info"
  },
  "health_check": {
    "test": ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"],
    "interval": "15s",
    "timeout": "3s",
    "retries": 3,
    "start_period": "30s"
  },
  "tmpfs": ["/var/cache/app"]
}
```

## Redis Example

Requires a stateful SKU with `disk_mb > 0` (configured by the operator). The image's `VOLUME /data` is automatically discovered and bind-mounted to a quota-enforced host directory. Data persists across container restarts and re-provisions but is destroyed on deprovision.

```json
{
  "image": "redis:7",
  "ports": {
    "6379/tcp": {}
  },
  "health_check": {
    "test": ["CMD", "redis-cli", "ping"],
    "interval": "10s",
    "timeout": "3s",
    "retries": 3,
    "start_period": "5s"
  }
}
```

## PostgreSQL Example

Requires a stateful SKU with `disk_mb > 0`. The image's VOLUME paths (`/var/lib/postgresql/data`) are automatically discovered and bind-mounted to a quota-enforced host directory.

```json
{
  "image": "postgres:16",
  "ports": {
    "5432/tcp": {}
  },
  "env": {
    "POSTGRES_PASSWORD": "changeme",
    "POSTGRES_DB": "myapp"
  },
  "health_check": {
    "test": ["CMD-SHELL", "pg_isready -U postgres"],
    "interval": "10s",
    "timeout": "5s",
    "retries": 5,
    "start_period": "30s"
  }
}
```
