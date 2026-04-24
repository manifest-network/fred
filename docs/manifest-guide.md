# Fred Deployment Manifest Guide

Fred accepts deployment manifests as JSON payloads that describe how containers should be provisioned. There are two formats: **single-service** manifests for standalone containers, and **stack** manifests for multi-service deployments.

## Manifest Formats

### Single-Service Manifest

A flat JSON object with `image` as the only required field. Used when a lease has a single item with no `service_name`.

```json
{
  "image": "nginx:1.25",
  "ports": {
    "80/tcp": { "host_port": 8080 }
  },
  "env": {
    "NGINX_WORKERS": "4"
  },
  "health_check": {
    "test": ["CMD", "curl", "-f", "http://localhost/"],
    "interval": "10s",
    "timeout": "5s",
    "retries": 3,
    "start_period": "30s"
  }
}
```

### Stack Manifest

A JSON object with a top-level `services` key containing a map of service names to manifest objects. Used when lease items have `service_name` fields. The `depends_on` field is **only** allowed within a stack.

```json
{
  "services": {
    "web": {
      "image": "nginx:1.25",
      "ports": {
        "80/tcp": { "host_port": 8080 }
      },
      "depends_on": {
        "api": { "condition": "service_healthy" }
      }
    },
    "api": {
      "image": "myorg/api:v2.1",
      "ports": {
        "3000/tcp": {}
      },
      "env": {
        "DATABASE_URL": "postgres://db:5432/app"
      },
      "health_check": {
        "test": ["CMD-SHELL", "curl -f http://localhost:3000/healthz"],
        "interval": "15s",
        "timeout": "5s",
        "retries": 3,
        "start_period": "10s"
      },
      "depends_on": {
        "db": { "condition": "service_healthy" }
      }
    },
    "db": {
      "image": "postgres:16",
      "user": "999:999",
      "env": {
        "POSTGRES_PASSWORD": "secret",
        "POSTGRES_DB": "app"
      },
      "health_check": {
        "test": ["CMD", "pg_isready", "-U", "postgres"],
        "interval": "10s",
        "timeout": "5s",
        "retries": 5,
        "start_period": "30s"
      },
      "tmpfs": ["/var/run/postgresql"]
    }
  }
}
```

**Format auto-detection:** Fred inspects the payload for a top-level `"services"` key. If present, it is parsed as a stack manifest; otherwise as a single-service manifest.

## Field Reference

### DockerManifest Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `image` | string | **Yes** | — | Container image to run (e.g., `nginx:latest`). |
| `ports` | object | No | `{}` | Port mappings. Keys: `"port/protocol"`. Values: `PortConfig`. |
| `env` | object | No | `{}` | Environment variables (string→string map). |
| `command` | string[] | No | `[]` | Overrides the container entrypoint. |
| `args` | string[] | No | `[]` | Arguments passed to the command. |
| `labels` | object | No | `{}` | Custom container labels (string→string map). |
| `health_check` | object | No | `null` | Health check configuration. See [Health Check](#health-check). |
| `tmpfs` | string[] | No | `[]` | Additional tmpfs mount paths. See [Tmpfs Mounts](#tmpfs-mounts). |
| `user` | string | No | `""` | Container runtime user (`"uid"`, `"uid:gid"`, `"name"`, `"name:group"`). |
| `depends_on` | object | No | `{}` | Startup dependencies. **Stack manifests only.** See [depends_on](#depends_on). |
| `stop_grace_period` | duration | No | `null` | Time after SIGTERM before SIGKILL. Range: `1s`–`120s`. |
| `init` | boolean | No | `null` | Run tini as PID 1 for zombie reaping and signal forwarding. |
| `expose` | string[] | No | `[]` | Inter-service ports (no host binding). Values are port number strings. |

### StackManifest Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `services` | object | **Yes** | Map of service name → DockerManifest. At least one service required. |

### PortConfig Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `host_port` | integer | No | `0` | Fixed host port (0 = auto-assign). Range: 0–65535. |
| `ingress` | boolean | No | `false` | Mark this port as the preferred ingress route. TCP only; at most one port per manifest may set this. Overrides the default `80 > 8080 > lowest TCP` preference. |

### HealthCheckConfig Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `test` | string[] | **Yes** | — | Command array. First element: `CMD`, `CMD-SHELL`, or `NONE`. |
| `interval` | duration | No | — | Time between checks. |
| `timeout` | duration | No | — | Per-check timeout. |
| `retries` | integer | No | `0` | Failures before marking unhealthy. Must be ≥ 0. |
| `start_period` | duration | No | — | Initial grace period. |

### DependsOnCondition Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `condition` | string | **Yes** | `"service_started"` or `"service_healthy"`. |

## Validation Rules

### Image

- **Required.** Cannot be empty.
- Must be from a registry in the operator's `allowed_registries` list. The registry is extracted using Docker's standard normalization:

| Image | Resolved Registry |
|---|---|
| `nginx` | `docker.io` |
| `nginx:latest` | `docker.io` |
| `myorg/myapp:v1` | `docker.io` |
| `ghcr.io/org/app:v1` | `ghcr.io` |
| `registry.example.com:5000/img` | `registry.example.com:5000` |

### Ports

Port map keys must be in `"port/protocol"` format:

| Example | Valid | Reason |
|---|---|---|
| `"80/tcp"` | Yes | Standard HTTP |
| `"53/udp"` | Yes | DNS over UDP |
| `"443/tcp"` | Yes | HTTPS |
| `"80"` | **No** | Missing protocol |
| `"0/tcp"` | **No** | Port must be 1–65535 |
| `"70000/tcp"` | **No** | Port exceeds 65535 |
| `"80/http"` | **No** | Protocol must be `tcp` or `udp` |

> **Note:** The Go runtime normalizes protocol case (e.g., `"80/TCP"` is accepted), but the canonical form is lowercase. Always use lowercase `tcp` or `udp` in manifests.

Host port (`host_port`) must be 0–65535. A value of 0 (or omitted) means Docker auto-assigns. Use `GET /info/{lease_uuid}` after provisioning to discover assigned host ports.

### Environment Variables

Env var names are validated for security:

- Cannot be empty.
- Cannot contain `=` or NUL (`\x00`).
- The following are **blocked** (case-insensitive):

| Blocked | Rationale |
|---|---|
| `PATH` | Prevents command hijacking |
| `LD_*` (e.g., `LD_PRELOAD`, `LD_LIBRARY_PATH`) | Dynamic linker injection |
| `FRED_*` | Reserved for backend internals |
| `DOCKER_*` | Docker runtime variables |

```json
// Valid
{ "env": { "DATABASE_URL": "postgres://...", "APP_PORT": "3000" } }

// Invalid — blocked names
{ "env": { "PATH": "/usr/bin", "LD_PRELOAD": "/tmp/evil.so" } }
{ "env": { "FRED_TOKEN": "abc", "DOCKER_HOST": "tcp://..." } }
```

### Labels

- Keys must **not** start with `fred.` — this prefix is reserved for backend-managed labels.

```json
// Valid
{ "labels": { "app": "myapp", "version": "1.0" } }

// Invalid
{ "labels": { "fred.lease": "abc-123" } }
```

### Health Check

The `test` array's first element determines the check type:

| Type | Behavior | Example |
|---|---|---|
| `CMD` | Executes the remaining elements as a command with arguments | `["CMD", "pg_isready", "-U", "postgres"]` |
| `CMD-SHELL` | Passes the second element to `/bin/sh -c` | `["CMD-SHELL", "curl -f http://localhost:3000/healthz"]` |
| `NONE` | Disables health checking | `["NONE"]` |

Rules:
- `test` is required when `health_check` is present.
- `CMD` and `CMD-SHELL` require at least one argument after the type prefix.
- `retries` must be ≥ 0.
- Duration fields (`interval`, `timeout`, `start_period`) accept Go duration strings (`"30s"`, `"1m30s"`, `"500ms"`) or integer nanoseconds.
- When a health check is present, the backend waits for all containers to report `healthy` before sending a success callback.
- A health check defined in the Dockerfile but **not** in the manifest does not trigger health-aware startup verification — the manifest is the contract.

```json
// CMD — exec form
{
  "health_check": {
    "test": ["CMD", "redis-cli", "ping"],
    "interval": "10s",
    "retries": 3
  }
}

// CMD-SHELL — shell form
{
  "health_check": {
    "test": ["CMD-SHELL", "curl -f http://localhost/ || exit 1"],
    "interval": "30s",
    "timeout": "10s",
    "retries": 5,
    "start_period": "1m"
  }
}

// NONE — disable
{
  "health_check": {
    "test": ["NONE"]
  }
}
```

### Tmpfs Mounts

Containers run with a read-only root filesystem by default (operator-configurable via the docker-backend `container_readonly_rootfs` setting). `/tmp` and `/run` are mounted as tmpfs automatically. Use the `tmpfs` field for additional writable directories.

**Rules:**
- Maximum **4** additional mounts.
- Paths must be **absolute** (start with `/`).
- Paths are normalized (trailing slashes, `..` components resolved).
- No duplicate paths (after normalization).
- Cannot mount `/` (root filesystem).

**Blocked paths:**

| Path | Reason | Sub-paths |
|---|---|---|
| `/tmp` | Automatically mounted by backend | Sub-paths allowed (e.g., `/tmp/cache`) |
| `/run` | Automatically mounted by backend | Sub-paths allowed (e.g., `/run/mysqld`) |
| `/proc` | Kernel filesystem | Sub-paths **also** blocked |
| `/sys` | Kernel filesystem | Sub-paths **also** blocked |
| `/dev` | Kernel filesystem | Sub-paths **also** blocked |

```json
// Valid
{ "tmpfs": ["/var/cache/nginx", "/var/log/nginx"] }
{ "tmpfs": ["/run/mysqld"] }

// Invalid — exceeds max 4
{ "tmpfs": ["/a", "/b", "/c", "/d", "/e"] }

// Invalid — blocked paths
{ "tmpfs": ["/tmp"] }
{ "tmpfs": ["/proc/something"] }
{ "tmpfs": ["/sys/fs/cgroup"] }

// Invalid — relative path
{ "tmpfs": ["var/cache"] }
```

Each tmpfs mount uses the operator-configured size limit (default 64MB). Combined with the automatic `/tmp` and `/run` mounts, the maximum total tmpfs memory is 6 × 64MB = 384MB.

### User

Overrides the container's runtime user. Useful for images like `postgres` whose entrypoint tries to chown directories (requires running as the target user directly since `CAP_CHOWN` is dropped).

**Formats** (same as Docker's `USER` directive):

| Format | Example | Description |
|---|---|---|
| `uid` | `"999"` | Numeric user ID |
| `uid:gid` | `"999:999"` | Numeric user and group IDs |
| `username` | `"postgres"` | Username resolved from the image's `/etc/passwd` |
| `username:group` | `"postgres:postgres"` | User and group resolved from the image |

**Rules:**
- No whitespace allowed.
- User part (before `:`) cannot be empty.
- If `:` is present, the group part (after `:`) cannot be empty.

**Backend behavior:** When `user` is set, the backend resolves the specification to a numeric UID/GID by inspecting the image's `/etc/passwd`, pre-chowns volume subdirectories to the resolved UID:GID, and sets the container to run as that user from start. When `user` is not set but the image declares `VOLUME` paths, the backend auto-detects the volume directory ownership from the image and runs as the detected UID:GID (this handles images like `postgres` and `mongo` that pre-chown their data directories during build). If no user can be determined from any source, the container runs as root (UID 0).

```json
// Valid
{ "user": "1000" }
{ "user": "1000:1000" }
{ "user": "nginx" }
{ "user": "postgres:postgres" }

// Invalid
{ "user": "1000:" }
{ "user": ":1000" }
{ "user": "my user" }
```

### stop_grace_period

Time to wait after sending SIGTERM before sending SIGKILL on container stop.

- Minimum: **1s**
- Maximum: **120s**
- Accepts Go duration strings or integer nanoseconds.

If not set, Docker's default (10s) is used.

```json
{ "stop_grace_period": "10s" }
{ "stop_grace_period": "2m" }

// Invalid
{ "stop_grace_period": "500ms" }
{ "stop_grace_period": "121s" }
```

### Init

Runs an init process (tini) as PID 1 inside the container for zombie reaping and signal forwarding. Set to `true` to enable, `false` to explicitly disable. If not set, Docker's default behavior applies.

### Expose

Documents inter-service ports without creating host bindings. Unlike `ports`, exposed ports are only accessible to other services on the same network (when network isolation is enabled). Use `ports` when external access is needed; use `expose` to document internal service ports.

- Values are port number strings, range 1–65535.
- No duplicates.

```json
{ "expose": ["3000", "8080"] }

// Invalid
{ "expose": ["0"] }
{ "expose": ["3000", "3000"] }
```

### Unknown Fields

Unknown fields are **rejected** at parse time. This prevents accidental misuse such as passing a stack manifest where a single-service manifest is expected, or typos in field names.

```json
// Invalid — unknown field
{ "image": "nginx", "volumes": ["/data"] }
```

## Stack-Specific Topics

### Service Names

Service names must be valid RFC 1123 DNS labels:

- Pattern: `[a-z0-9]([a-z0-9-]*[a-z0-9])?`
- Length: 1–63 characters
- Lowercase letters, digits, and hyphens only
- Must start and end with a letter or digit

| Name | Valid | Reason |
|---|---|---|
| `web` | Yes | |
| `my-db` | Yes | |
| `a` | Yes | Single character |
| `a1b2` | Yes | Mixed alphanumeric |
| `Web` | **No** | Uppercase |
| `my_db` | **No** | Underscore |
| `-web` | **No** | Leading hyphen |
| `web-` | **No** | Trailing hyphen |
| `my.svc` | **No** | Dot |
| 64× `a` | **No** | Exceeds 63 characters |

### Service Name ↔ Lease Item Matching

Stack manifest service names must match on-chain lease items **exactly 1:1**:

- Every service in the manifest must have a corresponding lease item with the same `service_name`.
- Every lease item must have a corresponding service in the manifest.
- No duplicates in either direction.

This ensures the operator provisions exactly the resources the tenant is paying for.

### depends_on

Declares startup ordering between services in a stack.

**Rules:**
- `depends_on` is **forbidden** in single-service manifests.
- Referenced services must exist in the same stack.
- Self-references are not allowed.
- `condition` is required and must be one of:
  - `"service_started"` — wait for the dependency container to start.
  - `"service_healthy"` — wait for the dependency to pass its health check. **Requires** the referenced service to have an active `health_check` (not `NONE`).
- The dependency graph must be acyclic (no circular dependencies).

**Fan-out behavior:** When a dependency has `quantity > 1`, the depending service waits for all instances. For example, if `web` depends on `db` and `db` has quantity 2, then `web` depends on both `db-0` and `db-1`.

### Inter-Service DNS

When network isolation is enabled, all services in a stack share the same tenant network. Each service's containers are created with the service name as a network alias, allowing services to reach each other by name (e.g., the `web` service can connect to `db:5432`).

### Cycle Detection

Fred performs DFS-based cycle detection on the `depends_on` graph with a maximum depth of **10**. This catches:

- Direct cycles: A → B → A
- Transitive cycles: A → B → C → A
- Excessively deep (but acyclic) chains that exceed depth 10

Diamond dependencies (A → B, A → C, B → D, C → D) are valid and not considered cycles.

```json
// Valid — diamond, not a cycle
{
  "services": {
    "a": { "image": "img", "depends_on": { "b": {"condition":"service_started"}, "c": {"condition":"service_started"} }},
    "b": { "image": "img", "depends_on": { "d": {"condition":"service_started"} }},
    "c": { "image": "img", "depends_on": { "d": {"condition":"service_started"} }},
    "d": { "image": "img" }
  }
}

// Invalid — cycle: web → db → web
{
  "services": {
    "web": { "image": "nginx", "depends_on": { "db": {"condition":"service_started"} }},
    "db":  { "image": "postgres", "depends_on": { "web": {"condition":"service_started"} }}
  }
}
```

## Payload Constraints

- **Maximum size:** 1 MiB (1,048,576 bytes) by default, enforced by the API's `MaxRequestBodySize` limit. Configurable by the operator.
- **Content type:** Raw JSON. For the `/data` endpoint the manifest bytes are the raw HTTP body; for `/update` the manifest is base64-encoded inside a JSON wrapper (`{"payload": "<base64>"}`).
- **Hash verification (deploy):** The SHA-256 hash of the payload body must match the `meta_hash` recorded on-chain in the lease.
- **Empty payloads** are rejected.
- **Unknown JSON fields** in the manifest are rejected.

## API Endpoints

### POST /v1/leases/{uuid}/data — Deploy

Uploads the initial deployment manifest for a pending lease. The raw HTTP body is the manifest JSON.

**Requirements:**
- Lease must be in `PENDING` state.
- Lease must have a `meta_hash` set.
- SHA-256 hash of the body must match the lease's `meta_hash`.
- Bearer token authentication (ADR-036 signature).
- Returns `202 Accepted` on success.

### POST /v1/leases/{uuid}/update — Update

Submits a new manifest for an already-provisioned lease. The body is a JSON object:

```json
{
  "payload": "<base64-encoded manifest>"
}
```

**Requirements:**
- Lease must be in `ACTIVE` state.
- Bearer token authentication with replay protection.
- Returns `202 Accepted` on success.

## Duration Format

Duration fields accept two formats:

1. **Go duration strings** — human-readable: `"30s"`, `"1m30s"`, `"500ms"`, `"2h"`, `"100us"`
2. **Integer nanoseconds** — e.g., `30000000000` for 30 seconds

Go duration strings support these units: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`.

## Examples

### Minimal

```json
{
  "image": "nginx:latest"
}
```

### Web Application

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

### Redis (Stateful)

Requires a stateful SKU with `disk_mb > 0`. The image's `VOLUME /data` is automatically discovered and bind-mounted to a quota-enforced host directory. Data persists across container restarts and re-provisions but is destroyed on deprovision.

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

### PostgreSQL (User Override + Stateful)

Requires a stateful SKU with `disk_mb > 0`. The backend auto-detects the volume ownership for `postgres` images, but setting `user` explicitly is recommended for clarity and to ensure correct behavior across image versions.

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

### Stack (Web + Database)

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

## Common Mistakes

| Mistake | Error | Fix |
|---|---|---|
| Missing protocol in port key | `must be in format 'port/protocol'` | Use `"80/tcp"` not `"80"` |
| Using `depends_on` in single manifest | `depends_on is only allowed in stack manifests` | Wrap in `{"services": {...}}` |
| `service_healthy` without health check | `requires ... to have a health_check` | Add `health_check` to the dependency |
| `NONE` health check with `service_healthy` | Same as above | Use `CMD` or `CMD-SHELL` instead |
| Setting `PATH` env var | `variable "PATH" is not allowed` | Use a different variable name or set PATH in the Dockerfile |
| Using `fred.*` label prefix | `labels cannot use reserved prefix 'fred.'` | Choose a different prefix |
| More than 4 tmpfs mounts | `too many mounts (N), maximum is 4` | Consolidate mount points |
| Tmpfs on `/tmp` or `/run` | `path "/tmp" is managed by the backend` | These are auto-mounted; use sub-paths if needed |
| Tmpfs under `/proc`, `/sys`, `/dev` | `path ... is under sensitive path ...` | These kernel filesystems cannot be masked |
| Service name with uppercase | `must match [a-z0-9]...` | Use lowercase DNS labels |
| Service name > 63 chars | `exceeds 63 characters` | Shorten the name |
| Manifest services don't match lease items | `has no matching lease item` | Ensure 1:1 mapping between services and lease items |
| Typo in field name | `unknown field "volumez"` | Check spelling; unknown fields are rejected |
| `stop_grace_period` too short | `must be at least 1s` | Use a value ≥ 1s |
| `stop_grace_period` too long | `must be at most 120s` | Use a value ≤ 120s |
| Empty health check test | `test is required` | Provide at least the type prefix |
| Negative retries | `retries cannot be negative` | Use 0 or a positive integer |
| Cycle in depends_on | `cycle detected involving service ...` | Remove circular dependency |
| Dependency chain too deep | `dependency chain exceeds maximum depth of 10` | Flatten your dependency graph |
