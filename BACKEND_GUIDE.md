# Backend Implementation Guide

This guide explains how to implement a backend for Fred. A backend is an HTTP service that provisions and manages resources on behalf of Fred.

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│                            FRED                                 │
│                                                                │
│  Lease: {items: [{sku: "docker-nginx", quantity: 1}], ...}     │
│              │                                                 │
│              ▼                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Router                              │   │
│  │                                                         │   │
│  │   SKU == "a1b2c3d4-...-1234567890ab" ?                   │   │
│  │         YES → use Docker Backend                        │   │
│  │                                                         │   │
│  │   SKU == "c3d4e5f6-...-4567890123de" ?                  │   │
│  │         YES → use GPU Backend                           │   │
│  │                                                         │   │
│  │   No match? → use Default Backend                       │   │
│  └──────────────────────────┬──────────────────────────────┘   │
│                             │                                  │
│                             │ HTTP Client                      │
└─────────────────────────────│──────────────────────────────────┘
                              │
                              │ POST /provision
                              │ {items: [...], ...}
                              ▼
                ┌───────────────────────────────┐
                │       Your Backend            │
                │       (HTTP server)           │
                │                               │
                │  Receives full SKU, decides:  │
                │  "docker-nginx" → nginx:latest│
                │  "docker-redis" → redis:7     │
                └───────────────────────────────┘
```

## Two Levels of SKU Handling

### Level 1: Fred Routes by SKU UUID

Fred's router matches exact SKU UUIDs to backends. On-chain SKUs are always UUIDv7. This is configured in Fred's `config.yaml`:

```yaml
backends:
  - name: docker-1
    url: "http://docker-backend-1:9001"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"
    default: true

  # Round-robin: same skus as docker-1, provisions distributed 50/50
  - name: docker-2
    url: "http://docker-backend-2:9001"
    skus:
      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
      - "b2c3d4e5-f6a7-8901-bcde-2345678901bc"

# Required for round-robin — tracks which backend serves each lease
placement_store_db_path: "/var/lib/fred/placements.db"
```

Fred does NOT interpret the SKU — it only uses exact UUID matching to decide which backend receives the request.

**Round-robin:** Multiple backends can share the same `skus` list. When this happens, Fred distributes new provisions across them using round-robin and records a placement (lease->backend) so that subsequent read operations (connection details, logs, diagnostics) reach the correct machine. This requires `placement_store_db_path` to be configured.

### Level 2: Backend Interprets Full SKU

Your backend receives the full SKU and decides what to do with it. This is entirely up to you:

| SKU | Backend Interpretation |
|-----|----------------------|
| `docker-nginx` | Create nginx:latest container |
| `docker-redis` | Create redis:7-alpine container |
| `k8s-small` | Create deployment with 1 CPU, 512MB (illustrative — see k3s note below) |
| `k8s-large` | Create deployment with 4 CPU, 4GB (illustrative — see k3s note below) |
| `gpu-a100` | Allocate A100 GPU node |

**Note:** The `mock-backend` included with Fred ignores the SKU entirely - it provisions the same fake resource regardless of SKU. This is intentional for testing purposes.

**Note on `k8s-*` / k3s:** The `k8s-small`/`k8s-large` rows above are illustrative only. The bundled `k3s-backend` is an **experimental, non-functional scaffold (ENG-133)**: it serves the full HTTP contract, but its provisioner returns `status=failed, error="not implemented"` and returns `ErrNotProvisioned` for info/logs/restart/update. It is **not for production use**; real Kubernetes provisioning lands in ENG-134+.

## Inbound Authentication

**Your backend MUST verify Fred's signature on every inbound contract request.** Fred signs every request it sends to a backend with the same `X-Fred-Signature` HMAC scheme used for callbacks (see Callback Protocol → HMAC Signature). A backend that does not verify inbound signatures accepts unauthenticated provision/deprovision/update commands from anyone who can reach it.

Both reference backends (`internal/backend/docker`, `cmd/k3s-backend`) wrap **all** contract routes in HMAC verification middleware and respond `401 Unauthorized` to any request with a missing or invalid `X-Fred-Signature`.

The verifier uses the **same canonical string** documented for callbacks, computed over the request's method, request-URI, and body:

```
<timestamp>\n<METHOD>\n<request-URI>\n<hex(sha256(body))>
```

Read the body, then verify before dispatching to the handler. Backends inside this repository can call `hmacauth.VerifyRequest(secret, r, body, sig, 5*time.Minute)`; external backends should re-derive the canonical string using the standalone sample in the Callback Protocol section (the computation is symmetric — sender and verifier hash identical bytes).

**Unauthenticated endpoints:** only the operational endpoints `GET /health`, `GET /stats`, and `GET /metrics` are exempt. Every other (contract) endpoint below must be authenticated.

## HTTP API Specification

Your backend must implement these HTTP endpoints:

### POST /provision

Start provisioning a resource asynchronously.

**Request:**
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "tenant": "manifest1abc...",
  "provider_uuid": "01234567-89ab-cdef-0123-456789abcdef",
  "items": [
    {"sku": "docker-nginx", "quantity": 1, "service_name": "web", "custom_domain": "app.example.com"},
    {"sku": "docker-redis", "quantity": 2, "service_name": "cache"}
  ],
  "callback_url": "http://fred:8080/callbacks/provision",
  "payload": "base64-encoded-bytes",
  "payload_hash": "sha256-hex-string"
}
```

**Item fields:**
- `sku` - The full SKU (your backend interprets it; see Two Levels of SKU Handling)
- `quantity` - Number of instances to provision for this item
- `service_name` (omitempty) - Service name in a stack lease. **Fred forwards this verbatim from the chain**, so a legacy single-item lease arrives with an **empty** `service_name`. Your backend must treat an empty `service_name` as the synthetic default `"app"` itself — the in-repo backends do this at handler entry via `backend.NormalizeProvisionRequest` (which also rejects mixed-presence and multi-unnamed item sets). The same applies to items in `/update` and `/reconcile_custom_domain`.
- `custom_domain` (omitempty) - Optional FQDN the tenant assigned to this item. When non-empty (and the service has a routable HTTP port), the backend should route `Host(<custom_domain>)` to this item's instances (see `POST /reconcile_custom_domain`)

**Response:** `202 Accepted`
```json
{
  "provision_id": "your-internal-id"
}
```

**Behavior:**
1. Validate the request
2. Store the `callback_url` for this `lease_uuid`
3. Return 202 immediately (do NOT block on provisioning)
4. Start provisioning in a background goroutine
5. When complete, POST to the `callback_url` (see Callback Protocol below)

**Error Responses:**
- `400 Bad Request` - Invalid request body
- `409 Conflict` - Lease already provisioned
- `503 Service Unavailable` - Insufficient resources

### GET /info/{lease_uuid}

Get connection details for a provisioned resource.

**Response:** `200 OK`
```json
{
  "host": "192.168.1.100",
  "protocol": "tcp",
  "ports": {
    "80/tcp": {
      "host_ip": "0.0.0.0",
      "host_port": "32768"
    },
    "443/tcp": {
      "host_ip": "0.0.0.0",
      "host_port": "32769"
    }
  },
  "instances": [
    {
      "instance_index": 0,
      "container_id": "abc123def456",
      "image": "nginx:latest",
      "status": "running",
      "ports": {
        "80/tcp": {"host_ip": "0.0.0.0", "host_port": "32768"}
      }
    }
  ]
}
```

The response format is flexible - return whatever fields are relevant to your resource type. Fred passes this directly to tenants. The `ports` field maps container ports (e.g., `80/tcp`) to host bindings. Per-instance details (`container_id`, `image`, `status`, `fqdn`, `ports`) are nested under `instances[]`, not at the top level.

**Known fields** that Fred extracts into the structured `ConnectionResponse`:
- Top level: `host`, `fqdn`, `protocol`, `ports`, `instances`, `services`, `metadata`
- Per `instances[]` entry: `instance_index`, `container_id`, `image`, `status`, `fqdn`, `ports`

When ingress is enabled, instances may include an `fqdn` field. If no top-level `fqdn` is present in the response, Fred propagates the first instance's `fqdn` to `connection.fqdn` automatically. The same propagation applies per-service in stack leases.

**Error Responses:**
- `404 Not Found` - Lease not provisioned or not ready yet

### POST /deprovision

Release resources for a lease. **Must be idempotent** - calling multiple times should not error.

**Request:**
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response:** `200 OK`
```json
{
  "status": "ok"
}
```

**Behavior:**
- Stop/delete the resource
- Clean up any associated state
- Return 200 even if the resource doesn't exist (idempotent)

### GET /provisions

List all currently provisioned resources. Used by Fred for reconciliation.

**Response:** `200 OK`
```json
{
  "provisions": [
    {
      "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
      "provider_uuid": "01234567-89ab-cdef-0123-456789abcdef",
      "status": "ready",
      "created_at": "2024-01-15T10:30:00Z",
      "fail_count": 0,
      "last_error": "",
      "image": "nginx:latest",
      "sku": "docker-nginx",
      "quantity": 1,
      "items": [
        {"sku": "docker-nginx", "quantity": 1, "service_name": "web"}
      ],
      "service_images": {"web": "nginx:latest"}
    }
  ]
}
```

**Fields:**
- `fail_count` - Number of provision failures for this lease
- `last_error` (omitempty) - Last diagnostic error message
- `image` / `sku` (omitempty) - Image and SKU for non-stack (single-service) leases
- `quantity` - Total expected container count across all items
- `items` (omitempty) - Per-service items for stack leases
- `service_images` (omitempty) - Map of service name → image for stack leases

**Status Values:**
- `provisioning` - Resource is being created
- `ready` - Resource is available
- `failing` - Container death detected; Failed callback pending (transient, brief)
- `failed` - Provisioning failed
- `unknown` - Status could not be determined
- `restarting` - Containers are being restarted
- `updating` - New manifest is being deployed
- `deprovisioning` - Containers are being removed

### GET /provisions/{lease_uuid}

Get provision diagnostics for a specific lease. Used by fred to serve `GET /v1/leases/{lease_uuid}/provision` to tenants. Falls back to persisted diagnostics (bbolt) when the provision is no longer in memory.

**Response:** `200 OK`
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "provider_uuid": "01234567-89ab-cdef-0123-456789abcdef",
  "status": "failed",
  "fail_count": 3,
  "last_error": "container exited with code 1 (OOM killed)",
  "created_at": "2024-01-15T10:30:00Z"
}
```

**Fields:**
- `status` - Provision status: `provisioning`, `ready`, `failing`, `failed`, `unknown`, `restarting`, `updating`, or `deprovisioning`. A backend that implements soft-delete/retention (see `/restore` below) also returns `retained` for a closed lease whose data is retained, alongside `retained_until` (RFC3339) and `items` (the restore shape)
- `fail_count` - Number of provision failures
- `last_error` - Full diagnostic error message (exit codes, OOM, truncated logs)

**Error Responses:**
- `404 Not Found` - Lease not provisioned (or diagnostics expired)

### GET /logs/{lease_uuid}

Get container logs for a specific lease. Used by fred to serve `GET /v1/leases/{lease_uuid}/logs` to tenants. Falls back to persisted logs when containers no longer exist.

**Query Parameters:**
- `tail` - Number of log lines per container (default: 100)

**Response:** `200 OK`
```json
{
  "0": "2024-01-15 10:30:00 Starting nginx...\nListening on port 80\n",
  "1": "2024-01-15 10:30:00 Redis ready\n"
}
```

**Fields:**
- Keys are container instance indices (`"0"`, `"1"`, ...), values are log output strings

**Error Responses:**
- `404 Not Found` - Lease not provisioned (or logs expired)

### POST /restart

Restart containers for a lease without changing the manifest. Stops existing containers, recreates them with the same configuration, and sends a callback on completion. Volumes are preserved across restarts.

**Request:**
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "callback_url": "http://fred:8080/callbacks/provision"
}
```

**Response:** `202 Accepted`
```json
{
  "status": "restarting"
}
```

**Behavior:**
1. Validate the lease exists and is in a restartable state (`ready` or `failed`)
2. Return 202 immediately
3. Stop and rename existing containers (kept for rollback) in a background goroutine
4. Recreate containers with the same manifest and configuration
5. Run startup verification (health checks or startup delay)
6. On success: remove old containers and POST success callback. On failure: rollback to old containers, restore `ready` status, and POST failure callback

**Error Responses:**
- `404 Not Found` - Lease not provisioned
- `409 Conflict` - Invalid state for restart (e.g., already restarting, updating, or provisioning)

### POST /update

Deploy a new manifest for a lease, replacing containers with a new image/configuration. Pulls the new image, stops old containers (kept for rollback), creates new ones, and sends a callback on completion. On failure, rolls back to the previous containers. Volumes are preserved.

**Request:**
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "callback_url": "http://fred:8080/callbacks/provision",
  "payload": "base64-encoded-manifest",
  "payload_hash": "sha256-hex-string"
}
```

**Response:** `202 Accepted`
```json
{
  "status": "updating"
}
```

**Behavior:**
1. Validate the lease exists and is in an updatable state (`ready` or `failed`)
2. Parse and validate the new manifest
3. Return 202 immediately
4. Pull the new image in a background goroutine
5. Stop and rename old containers (kept for rollback)
6. Create and start new containers from the updated manifest
7. Run startup verification
8. On success: remove old containers and POST success callback. On failure: rollback to old containers, mark status as `failed` (the desired update was not achieved even though old containers may be restored), and POST failure callback

**Error Responses:**
- `400 Bad Request` - Invalid manifest or validation error
- `404 Not Found` - Lease not provisioned
- `409 Conflict` - Invalid state for update (e.g., currently restarting or provisioning)

### POST /restore (optional — retention support)

Restore a soft-deleted lease's retained data into a **new** lease (async, callback on completion). Only meaningful for backends that implement soft-delete on close (`retain_on_close`). A backend without retention support must still serve this route and return `422`.

**Request:**
```json
{
  "lease_uuid": "<new-lease-uuid>",
  "from_lease_uuid": "<original-retained-lease-uuid>",
  "tenant": "manifest1abc...",
  "provider_uuid": "01234567-89ab-cdef-0123-456789abcdef",
  "items": [{"sku": "docker-redis", "quantity": 1, "service_name": "app"}],
  "callback_url": "http://fred:8080/callbacks/provision"
}
```

`lease_uuid` is the new lease; `from_lease_uuid` is the original retained lease. `items` must shape-match (service name → summed quantity) the retained set.

**Response:** `202 Accepted`
```json
{
  "status": "restoring"
}
```

**Behavior:**
1. Validate the retained record exists and is owned by `tenant`; re-deploy strictly from the **retained manifest** captured at close time (the request carries no manifest)
2. Adopt the retained volumes into the new lease's namespace, then bring up the stack and POST a callback
3. On failure, re-quarantine the volumes (data preserved) and POST a failure callback

**Error Responses:**
- `400 Bad Request` - Missing required fields or items/manifest validation error
- `409 Conflict` - Invalid state for restore (bare body), or already provisioned (body `code: "already_provisioned"`)
- `422 Unprocessable Entity` - No retained data for `from_lease_uuid` (also the correct response for backends without retention support)
- `503 Service Unavailable` - Insufficient resources

### GET /retentions (optional — retention support)

List the leases whose data this backend currently retains. Fred's reconciler polls this on every backend each tick to keep restore routing affinity (route a restore to the backend holding the source data). Backends without retention return an empty list.

**Response:** `200 OK`
```json
{
  "retentions": [
    {"lease_uuid": "550e8400-e29b-41d4-a716-446655440000"}
  ]
}
```

Always return `{"retentions": []}` (never `null`) when nothing is retained.

### POST /reconcile_custom_domain

Reconcile the custom-domain routing for a lease's items. Fred calls this on **every active lease on every reconcile tick**, so it must be cheap and idempotent: when nothing has changed, do no work and return success. The request carries the current desired item set; the backend ensures each item's `custom_domain` routing (e.g. a secondary Traefik router for `Host(<custom_domain>)`) matches.

**Request:**
```json
{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    {"sku": "docker-nginx", "quantity": 1, "service_name": "web", "custom_domain": "app.example.com"},
    {"sku": "docker-redis", "quantity": 2, "service_name": "cache"}
  ]
}
```

**Response:** `204 No Content`

**Behavior:**
- Compare each item's desired `custom_domain` to the current routing and apply any difference
- Make no changes (and still return `204`) when the desired state already matches — this is the common case every tick
- A backend that does not support custom domains (or has ingress disabled) should return `204` as a no-op, **not** `404`. Returning `404` on every tick pollutes Fred's reconciler error metrics and logs

**Error Responses:**
- `404 Not Found` - Lease not provisioned
- `409 Conflict` - Invalid state for reconcile (e.g., currently restarting, updating, or provisioning)

### GET /releases/{lease_uuid}

Get the release (deployment) history for a lease. Each provision, update, or restart creates a release entry. Restarts reuse the existing manifest and image but still record a new release entry for operational tracking.

**Response:** `200 OK`
```json
[
  {
    "version": 1,
    "image": "nginx:1.24",
    "status": "superseded",
    "created_at": "2024-01-15T10:30:00Z",
    "manifest": "base64-encoded-manifest"
  },
  {
    "version": 2,
    "image": "nginx:1.25",
    "status": "active",
    "created_at": "2024-01-16T14:00:00Z",
    "manifest": "base64-encoded-manifest"
  }
]
```

**Fields:**
- `version` - Monotonically increasing version number (starting at 1)
- `image` - Container image used in this release
- `status` - Release status: `deploying`, `active`, `superseded`, or `failed`
- `error` - Error message (only present for failed releases)
- `manifest` - The raw manifest payload used for this release

**Error Responses:**
- `404 Not Found` - Lease not provisioned

### GET /health

Simple health check endpoint. This endpoint is **not** authenticated (see Inbound Authentication above).

**Response:** `200 OK`
```json
{
  "status": "healthy"
}
```

Return 200 if your backend can accept requests. Fred uses this for health monitoring.

### RefreshState (Backend interface method, optional for in-process backends)

`RefreshState` exists on Fred's `Backend` interface, but it is **not** a required HTTP route for HTTP backends. Fred's HTTP client uses a no-op implementation for remote backends because those servers are expected to maintain their own state.

In-process backends (such as the Docker backend) should implement `RefreshState` to sync in-memory state from the underlying infrastructure before reconciliation calls `ListProvisions`.

If your HTTP backend keeps an internal cache and you want an external trigger, you may optionally expose your own `/refresh-state` route. Fred itself does not call that route.

### GET /stats (Optional)

Return resource capacity and usage statistics. Useful for UI display and monitoring.

**Response:** `200 OK`
```json
{
  "total_cpu_cores": 8.0,
  "total_memory_mb": 16384,
  "total_disk_mb": 102400,
  "allocated_cpu_cores": 4.5,
  "allocated_memory_mb": 4608,
  "allocated_disk_mb": 9216,
  "available_cpu_cores": 3.5,
  "available_memory_mb": 11776,
  "available_disk_mb": 93184,
  "active_containers": 4
}
```

This endpoint is optional but recommended for production backends. The Docker backend implements it; the mock backend intentionally omits it to stay minimal.

## Callback Protocol

When provisioning completes (success or failure), POST to the `callback_url` from the provision request.

### Request Format

```http
POST {callback_url}
Content-Type: application/json
X-Fred-Signature: t=<unix-timestamp>,sha256=<hex-encoded-hmac>

{
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "error": "",
  "backend": "my-backend"
}
```

**Note:** The timestamp must be the current Unix time when sending the request. Callbacks with timestamps older than 5 minutes or more than 1 minute in the future are rejected.

**Fields:**
- `status`: One of `"success"`, `"failed"`, or `"deprovisioned"`. Use `"deprovisioned"` when the backend has autonomously torn down a lease (e.g. after a failed provision rollback) so Fred records the lease as deprovisioned without firing failure-callback side effects.
- `error`: Error message if status is `"failed"`, empty otherwise
- `backend` (omitempty): The backend's configured name. Lets Fred label metrics per-backend without a placement lookup. Empty from pre-upgrade senders.

### HMAC Signature with Replay Protection

Fred verifies callbacks using HMAC-SHA256 with timestamp-based replay protection. Callbacks older than 5 minutes are rejected.

**Signature format:** `t=<unix-timestamp>,sha256=<hex-encoded-hmac>`

The HMAC is computed over a four-field canonical string that binds the timestamp, HTTP method, request URI, and a hash of the body:

```
<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>
```

Binding the method and URI prevents cross-endpoint replay: a signature captured on (e.g.) `POST /callbacks/provision` cannot be replayed against any other endpoint. The body is included as a SHA-256 hash, so arbitrary bytes (including `\n`, NUL, or invalid UTF-8) cannot influence the canonical string.

```go
import (
    "bytes"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "time"
)

// computeSignature signs the given request shape. method and uri must
// match what the verifier sees on the wire — typically req.Method and
// req.URL.RequestURI() after the *http.Request is built.
func computeSignature(secret, method, uri string, body []byte) string {
    timestamp := time.Now().Unix()
    bodyHash := sha256.Sum256(body) // sha256.Sum256(nil) is stable
    signed := fmt.Sprintf("%d\n%s\n%s\n%x", timestamp, method, uri, bodyHash[:])

    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(signed))
    sig := hex.EncodeToString(mac.Sum(nil))

    return fmt.Sprintf("t=%d,sha256=%s", timestamp, sig)
}

// Usage — build the *http.Request first, then sign using its method
// and request-URI so sender and verifier hash identical bytes:
body, _ := json.Marshal(callbackPayload)
req, _ := http.NewRequest(http.MethodPost, callbackURL, bytes.NewReader(body))
sig := computeSignature(os.Getenv("CALLBACK_SECRET"), req.Method, req.URL.RequestURI(), body)
req.Header.Set("X-Fred-Signature", sig)
```

The `CALLBACK_SECRET` must match Fred's `callback_secret` configuration. Backends that live inside this repository can import `internal/hmacauth` and call `hmacauth.SignRequest(secret, req, body)` instead of computing the canonical string by hand; Go's `internal/` rule makes that helper unavailable to external backends, which should use the standalone sample above.

### Security Notes

- **Replay protection**: Callbacks older than 5 minutes are rejected
- **Cross-endpoint binding**: Signature is bound to HTTP method + request URI; a captured signature cannot be replayed against a different endpoint
- **Clock skew tolerance**: Timestamps up to 1 minute in the future are accepted
- **Binary-safe body**: Body is hashed (SHA-256), so the canonical string is unaffected by embedded `\n`, NUL, or non-UTF-8 bytes

## State Management

### In-Memory Pattern (Recommended for Starting)

Follow the in-memory state pattern from `cmd/mock-backend/main.go` (a minimal, callback-only example — see Reference Implementation):

```go
type MyBackend struct {
    provisions map[string]*provision
    mu         sync.RWMutex
}

type provision struct {
    LeaseUUID    string
    Status       string    // "provisioning", "ready", "failed"
    CreatedAt    time.Time
    // ... your resource-specific fields
}
```

### Callback URL Storage

Store callback URLs per lease to handle concurrent provisions:

```go
type BackendServer struct {
    backend        *MyBackend
    callbackURLs   map[string]string  // lease_uuid -> callback_url
    callbackURLsMu sync.Mutex
}
```

### State Recovery on Startup

For production use, recover state from your actual resources:

```go
func (b *DockerBackend) recoverState(ctx context.Context) error {
    // List all containers with fred.managed=true label
    containers, err := b.docker.ContainerList(ctx, types.ContainerListOptions{
        Filters: filters.NewArgs(filters.Arg("label", "fred.managed=true")),
    })

    // Rebuild in-memory state from container labels
    for _, c := range containers {
        b.provisions[c.Labels["fred.lease_uuid"]] = &provision{
            LeaseUUID: c.Labels["fred.lease_uuid"],
            Status:    "ready",
            // ...
        }
    }
    return nil
}
```

## Reconciliation Support

Fred periodically calls `GET /provisions` to detect:

1. **Orphans**: Resources that exist but have no corresponding lease (lease was closed while backend was down)
2. **Missing**: Leases that should be provisioned but aren't

Your `ListProvisions` must return ALL resources you're managing, so Fred can reconcile correctly.

## Example: Minimal Backend Structure

```go
package main

import (
    "encoding/json"
    "net/http"
    "sync"
)

type Backend struct {
    provisions   map[string]*provision
    callbackURLs map[string]string
    mu           sync.RWMutex
}

func main() {
    b := &Backend{
        provisions:   make(map[string]*provision),
        callbackURLs: make(map[string]string),
    }

    mux := http.NewServeMux()

    // Contract routes — all wrapped in inbound HMAC verification.
    auth := hmacAuthMiddleware(callbackSecret) // 401s missing/invalid X-Fred-Signature
    mux.Handle("POST /provision", auth(http.HandlerFunc(b.handleProvision)))
    mux.Handle("POST /deprovision", auth(http.HandlerFunc(b.handleDeprovision)))
    mux.Handle("GET /info/{lease_uuid}", auth(http.HandlerFunc(b.handleGetInfo)))
    mux.Handle("GET /logs/{lease_uuid}", auth(http.HandlerFunc(b.handleGetLogs)))
    mux.Handle("GET /provisions/{lease_uuid}", auth(http.HandlerFunc(b.handleGetProvision)))
    mux.Handle("GET /provisions", auth(http.HandlerFunc(b.handleListProvisions)))
    mux.Handle("POST /restart", auth(http.HandlerFunc(b.handleRestart)))
    mux.Handle("POST /update", auth(http.HandlerFunc(b.handleUpdate)))
    mux.Handle("POST /reconcile_custom_domain", auth(http.HandlerFunc(b.handleReconcileCustomDomain)))
    mux.Handle("GET /releases/{lease_uuid}", auth(http.HandlerFunc(b.handleGetReleases)))

    // Operational routes — no auth (monitoring, health checks).
    mux.HandleFunc("GET /health", b.handleHealth)

    http.ListenAndServe(":9001", mux)
}

func (b *Backend) handleProvision(w http.ResponseWriter, r *http.Request) {
    var req ProvisionRequest
    json.NewDecoder(r.Body).Decode(&req)

    b.mu.Lock()
    // Store callback URL
    b.callbackURLs[req.LeaseUUID] = req.CallbackURL
    // Create provision record
    b.provisions[req.LeaseUUID] = &provision{
        LeaseUUID: req.LeaseUUID,
        Status:    "provisioning",
    }
    b.mu.Unlock()

    // Async provisioning
    go b.provisionResource(req)

    w.WriteHeader(http.StatusAccepted)
    json.NewEncoder(w).Encode(map[string]string{"provision_id": req.LeaseUUID})
}
```

## Reference Implementation

**For a full-contract reference, read the Docker backend at `internal/backend/docker` (with `cmd/docker-backend`).** It implements all 10 contract routes plus `/health`, verifies inbound HMAC signatures, supports TLS/mTLS, and exercises the complete lifecycle (provision, info, logs, restart, update, releases, custom-domain reconciliation, deprovision, reconciliation).

`cmd/mock-backend/main.go` is a **minimal, callback-only example**: it implements 6 of the 10 contract routes (provision, info, deprovision, provisions, provisions/{lease_uuid}, logs) plus `/health` — it lacks restart, update, reconcile_custom_domain, and releases — **does NOT verify inbound authentication**, and ignores the SKU. It is useful for seeing the in-memory state pattern and callback-sending mechanics, but is **not** a complete contract reference — do not model a production backend on it. Key sections:

| Function | Description |
|----------|-------------|
| `MockBackendServer` struct | Server setup with callback URL tracking |
| `handleProvision` | Provision handler with async goroutine |
| `handleGetInfo` | GetInfo handler |
| `handleDeprovision` | Deprovision handler (idempotent) |
| `handleListProvisions` | ListProvisions for reconciliation |
| `sendCallback`, `computeSignature` | Callback sending with HMAC signature |

## Configuration

The production backends (docker-backend, k3s-backend) are configured via a YAML file (the Docker backend reads `docker-backend.yaml`; see `internal/backend/docker/config.go`); the mock backend is the exception — it is configured via `MOCK_BACKEND_*` environment variables, not YAML. A backend needs at minimum a listen address, an HMAC `callback_secret` that matches Fred's `callback_secret`, and a name/host for logging and connection info:

```yaml
listen_addr: ":9001"             # Listen address
callback_secret: "32-char-min"   # HMAC secret (must match Fred's config)
name: "my-backend"               # For logging/metrics
host_address: "192.168.1.100"    # For connection info
```

The Docker backend takes a `--config` flag (path to the YAML file, default `docker-backend.yaml`) and a `--version` flag that prints the build-injected version and exits 0 without loading config or connecting to Docker.

### TLS / mTLS on the providerd → backend transport (ENG-103)

TLS on the providerd → backend HTTP transport is **optional**; the transport defaults to **plaintext** when unconfigured. When enabled, TLS 1.3 is pinned as the minimum version. TLS is configured on both sides:

**Server side** (the backend's YAML, e.g. `docker-backend.yaml`):

```yaml
tls_cert_file: "/path/to/server-cert.pem"   # enable server TLS (with tls_key_file)
tls_key_file:  "/path/to/server-key.pem"
tls_client_ca_file: "/path/to/client-ca.pem" # require + verify client certs => mTLS
tls_client_allowed_names:                     # pin allowed client identities
  - "providerd"
```

Setting `tls_cert_file` + `tls_key_file` turns on server TLS. Adding `tls_client_ca_file` turns on mutual TLS: the listener requires and verifies a client certificate signed by that CA. `tls_client_allowed_names` optionally pins the client's identity — the presented certificate's CommonName or a DNS SAN must be in this list. **Without it, any certificate signed by the client CA is accepted**, so set it whenever the client CA is not dedicated solely to providerd. The server config is built via `tlsconfig.ServerConfig`.

**Client side** (providerd, per-backend under `backends[]` in Fred's `config.yaml`):

```yaml
backends:
  - name: my-backend
    url: "https://my-backend:9001"
    tls_ca_file: "/path/to/server-ca.pem"          # CA that signed the backend's server cert
    tls_client_cert_file: "/path/to/client-cert.pem" # client cert for mTLS (set with key)
    tls_client_key_file: "/path/to/client-key.pem"   # client key for mTLS (set with cert)
    tls_skip_verify: false                           # DEV ONLY; rejected in production_mode
```

The client config is built via `tlsconfig.ClientConfig`. Client-identity pinning is enforced with `tls.Config.VerifyConnection` (so it applies even on resumed TLS sessions), matching the client certificate's CommonName / DNS-SAN against `tls_client_allowed_names`.

## Testing Your Backend

**Note:** `/health` is unauthenticated, so the health check below works as-is. The contract endpoints (`/provision`, `/provisions`, `/info`, `/deprovision`, ...) require a valid `X-Fred-Signature` header (see Inbound Authentication) and will return `401` to the plain `curl` commands below. To exercise them against an auth-enforcing backend, sign the request with the canonical string (or temporarily run the backend with auth disabled in a dev-only build).

### 1. Health Check
```bash
curl http://localhost:9001/health
```

### 2. Provision
```bash
curl -X POST http://localhost:9001/provision \
  -H "Content-Type: application/json" \
  -d '{
    "lease_uuid": "test-lease-1",
    "tenant": "manifest1test",
    "provider_uuid": "test-provider",
    "items": [{"sku": "docker-nginx", "quantity": 1}],
    "callback_url": "http://localhost:8080/callbacks/provision"
  }'
```

### 3. Check Provisions
```bash
curl http://localhost:9001/provisions
```

### 4. Get Info
```bash
curl http://localhost:9001/info/test-lease-1
```

### 5. Deprovision
```bash
curl -X POST http://localhost:9001/deprovision \
  -H "Content-Type: application/json" \
  -d '{"lease_uuid": "test-lease-1"}'
```

## Checklist

Before deploying your backend:

- [ ] All 10 contract HTTP endpoints implemented (`/provision`, `/deprovision`, `/info/{lease_uuid}`, `/logs/{lease_uuid}`, `/provisions/{lease_uuid}`, `/provisions`, `/restart`, `/update`, `/reconcile_custom_domain`, `/releases/{lease_uuid}`) plus `/health`
- [ ] **Inbound `X-Fred-Signature` verified on all contract endpoints** (401 on missing/invalid; only `/health`, `/stats`, `/metrics` are exempt)
- [ ] Provision returns 202 and works asynchronously
- [ ] Callbacks signed with HMAC-SHA256 with timestamp
- [ ] Deprovision is idempotent
- [ ] ListProvisions returns all managed resources
- [ ] `/reconcile_custom_domain` is idempotent and returns 204 on no-change (never 404 just because custom domains are unsupported — that pollutes Fred's reconciler every tick)
- [ ] State protected with mutex for concurrent access
- [ ] Callback URLs stored per-lease (not globally)
- [ ] Health endpoint returns 200 when operational
- [ ] Graceful shutdown (finish in-flight provisions)
- [ ] (Optional) `/stats` endpoint for resource monitoring

**Note:** `/restart`, `/update`, `/reconcile_custom_domain`, and `/releases/{lease_uuid}` are all part of the contract, not optional add-ons. `/reconcile_custom_domain` is called on **every reconcile tick** for each active lease (`internal/provisioner/reconciler.go`), so it must be cheap and idempotent — a backend with no work should return success, not 404 (e.g. the k3s scaffold returns `nil` from `ReconcileCustomDomain`). `/restart`, `/update`, and `/releases/{lease_uuid}` are invoked **on demand** when a tenant calls the corresponding API operation (`RestartLease`/`UpdateLease`/`GetLeaseReleases` in `internal/api/handlers.go`); they must still be implemented, but returning 404 when a lease isn't provisioned is correct for these.

## Further reading

- [SECURITY.md](SECURITY.md) — full security model including HMAC details and replay protection
- [OPERATIONS.md](OPERATIONS.md) — operator-side runbook (alert interpretation, callback debugging)
- [docs/manifest-guide.md](docs/manifest-guide.md) — tenant manifest schema (if your backend interprets manifests)
- [internal/backend/docker/README.md](internal/backend/docker/README.md) — the bundled Docker backend, the full-contract reference implementation
- [CONTRIBUTING.md § Adding a backend operation](CONTRIBUTING.md#adding-a-backend-operation) — if you're contributing a backend operation upstream rather than building a separate backend
