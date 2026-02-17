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
  "tmpfs": ["/var/cache/nginx", "/var/log/nginx"],
  "user": "nginx",
  "stop_grace_period": "10s",
  "init": true,
  "expose": ["3000", "9090"],
  "depends_on": {
    "db": { "condition": "service_healthy" }
  }
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
| `user` | string | no | Override container runtime user (see below). |
| `stop_grace_period` | duration | no | Time after SIGTERM before SIGKILL (1s–120s, see below). |
| `init` | bool | no | Run an init process (tini) as PID 1 (see below). |
| `expose` | string[] | no | Document inter-service ports without host binding (see below). |
| `depends_on` | object | no | Service startup dependencies — **stack-only** (see below). |

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

## User Override

The `user` field overrides the container's runtime user. This is primarily useful for images like PostgreSQL whose entrypoint starts as root and attempts to `chown` data directories — since `CAP_CHOWN` is dropped by default (see [Container Hardening](README.md#container-hardening)), the container must run directly as the target user instead.

When `user` is set, the backend:
1. Resolves the user specification to a numeric UID/GID by inspecting the image's `/etc/passwd`
2. Pre-chowns volume subdirectories to the resolved UID:GID before the container starts
3. Sets `container.Config.User` so the process runs as the specified user from the start

**Accepted formats** (same as Docker's `USER` directive):

| Format | Example | Description |
|---|---|---|
| `uid` | `"999"` | Numeric user ID |
| `uid:gid` | `"999:999"` | Numeric user and group IDs |
| `username` | `"postgres"` | Username resolved from the image's `/etc/passwd` |
| `username:group` | `"postgres:postgres"` | User and group resolved from the image |

**Constraints:**
- Must not contain whitespace
- User part cannot be empty
- Group part (after `:`) cannot be empty if the colon is present

If neither the manifest nor the Dockerfile's `USER` directive specifies a user, the container runs as root (UID 0).

## Stop Grace Period

The `stop_grace_period` field specifies how long to wait after sending SIGTERM before sending SIGKILL when stopping the container. This gives the application time to shut down gracefully (flush buffers, close connections, etc.).

```json
{
  "stop_grace_period": "30s"
}
```

**Constraints:**

- Minimum: `1s`
- Maximum: `120s`
- Accepts Go duration strings (`"10s"`, `"1m30s"`)

If not set, Docker's default (10s) is used.

## Init Process

The `init` field runs an init process (tini) as PID 1 inside the container. This is useful for:

- **Zombie reaping**: Prevents zombie processes from accumulating when the main process spawns children that exit.
- **Signal forwarding**: Ensures SIGTERM is properly forwarded to the main process.

```json
{
  "init": true
}
```

Set to `true` to enable, `false` to explicitly disable. If not set, Docker's default behavior applies.

## Expose

The `expose` field documents inter-service ports without creating host port bindings. This is purely documentary — it does not affect networking or security. Unlike `ports`, exposed ports are only accessible to other services on the same network (when network isolation is enabled).

```json
{
  "expose": ["3000", "9090"]
}
```

**Constraints:**

- Values must be valid port numbers (1–65535) as strings
- No duplicate ports

**`expose` vs `ports`**: Use `ports` when external access is needed (creates host bindings). Use `expose` to document ports that other services in the stack connect to internally.

## Depends On (Stack Only)

The `depends_on` field declares startup dependencies between services in a stack manifest. It controls the order in which Docker Compose starts services. This field is **only allowed in stack manifests** — single-container manifests reject it.

```json
{
  "depends_on": {
    "db": { "condition": "service_healthy" },
    "cache": { "condition": "service_started" }
  }
}
```

Keys are service names that this service depends on. Each value specifies a condition:

| Condition | Description |
|---|---|
| `service_started` | Wait until the dependency has started |
| `service_healthy` | Wait until the dependency reports healthy (requires the dependency to have a `health_check`) |

**Validation rules:**

- Referenced services must exist in the stack
- A service cannot depend on itself
- `service_healthy` requires the referenced service to have an active `health_check`
- Circular dependencies are rejected (cycle detection with max depth 10)

**Fan-out behavior:** When a dependency has `quantity > 1`, the depending service waits for all instances. For example, if `web` depends on `db` and `db` has quantity 2, then `web` depends on both `db-0` and `db-1`.

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

Requires a stateful SKU with `disk_mb > 0`. The image's VOLUME paths (`/var/lib/postgresql/data`) are automatically discovered and bind-mounted to a quota-enforced host directory. The `user` field ensures the container runs as `postgres` from the start, bypassing the default entrypoint's `chown` calls that would fail without `CAP_CHOWN`.

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
  "user": "postgres",
  "health_check": {
    "test": ["CMD-SHELL", "pg_isready -U postgres"],
    "interval": "10s",
    "timeout": "5s",
    "retries": 5,
    "start_period": "30s"
  }
}
```

## Stack Manifests

A stack manifest deploys multiple services as a single lease. Instead of a flat `DockerManifest`, the payload wraps per-service manifests under a `"services"` key:

```json
{
  "services": {
    "web": {
      "image": "ghcr.io/myorg/webapp:v2.1.0",
      "ports": { "8080/tcp": {} },
      "env": { "DATABASE_URL": "postgres://db:5432/myapp" },
      "depends_on": {
        "db": { "condition": "service_healthy" }
      },
      "health_check": {
        "test": ["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"],
        "interval": "15s",
        "timeout": "3s",
        "retries": 3,
        "start_period": "30s"
      }
    },
    "db": {
      "image": "postgres:16",
      "ports": { "5432/tcp": {} },
      "env": { "POSTGRES_PASSWORD": "changeme", "POSTGRES_DB": "myapp" },
      "user": "postgres",
      "health_check": {
        "test": ["CMD-SHELL", "pg_isready -U postgres"],
        "interval": "10s",
        "timeout": "5s",
        "retries": 5,
        "start_period": "30s"
      }
    }
  }
}
```

### Auto-detection

The backend auto-detects the manifest format: if the JSON payload contains a `"services"` key, it is parsed as a stack manifest. Otherwise, it is parsed as a single-container manifest. You cannot mix the two formats.

### Service Name Constraints

Service names (the keys under `"services"`) must:

- Match the regex `[a-z0-9]([a-z0-9-]*[a-z0-9])?` (DNS label safe)
- Be at most 63 characters long
- Not be empty

### 1:1 Service Mapping

Every service name in the manifest must correspond to exactly one lease item with a matching `service_name`, and vice versa. The backend rejects payloads where:

- A manifest service has no corresponding lease item
- A lease item has no corresponding manifest service

### Per-service Schema

Each value under `"services"` uses the same `DockerManifest` schema described above (image, ports, env, command, args, labels, health_check, tmpfs, user). All validation rules apply per-service.

### Inter-service DNS

When network isolation is enabled, all services in a stack share the same tenant network. Each service's containers are created with the service name as a network alias, allowing services to reach each other by name (e.g., the `web` service can connect to `db:5432`).
