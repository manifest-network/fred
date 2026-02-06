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
│  │   "docker-nginx" starts with "docker-" ?                │   │
│  │         YES → use Docker Backend                        │   │
│  │                                                         │   │
│  │   "gpu-a100" starts with "gpu-" ?                       │   │
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

### Level 1: Fred Routes by SKU Prefix

Fred's router matches SKU prefixes to backends. This is configured in Fred's `config.yaml`:

```yaml
backends:
  - name: docker
    url: "http://docker-backend:9001"
    sku_prefix: "docker-"      # Matches: docker-nginx, docker-redis, docker-*
    default: true

  - name: kubernetes
    url: "http://k8s-backend:9000"
    sku_prefix: "k8s-"         # Matches: k8s-small, k8s-large, k8s-*

  - name: gpu
    url: "http://gpu-backend:9000"
    sku_prefix: "gpu-"         # Matches: gpu-a100, gpu-h100, gpu-*
```

Fred does NOT interpret the full SKU - it only uses the prefix to decide which backend receives the request.

### Level 2: Backend Interprets Full SKU

Your backend receives the full SKU and decides what to do with it. This is entirely up to you:

| SKU | Backend Interpretation |
|-----|----------------------|
| `docker-nginx` | Create nginx:latest container |
| `docker-redis` | Create redis:7-alpine container |
| `k8s-small` | Create deployment with 1 CPU, 512MB |
| `k8s-large` | Create deployment with 4 CPU, 4GB |
| `gpu-a100` | Allocate A100 GPU node |

**Note:** The `mock-backend` included with Fred ignores the SKU entirely - it provisions the same fake resource regardless of SKU. This is intentional for testing purposes.

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
    {"sku": "docker-nginx", "quantity": 1},
    {"sku": "docker-redis", "quantity": 2}
  ],
  "callback_url": "http://fred:8080/callbacks/provision",
  "payload": "base64-encoded-bytes",
  "payload_hash": "sha256-hex-string"
}
```

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
  "protocol": "tcp",
  "container_id": "abc123def456",
  "image": "nginx:latest",
  "status": "running"
}
```

The response format is flexible - return whatever fields are relevant to your resource type. Fred passes this directly to tenants. The `ports` field maps container ports (e.g., `80/tcp`) to host bindings.

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
      "created_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

**Status Values:**
- `provisioning` - Resource is being created
- `ready` - Resource is available
- `failed` - Provisioning failed

### GET /provisions/{lease_uuid}

Get provision diagnostics for a specific lease. Used by fred to serve `GET /v1/leases/{uuid}/provision` to tenants. Falls back to persisted diagnostics (bbolt) when the provision is no longer in memory.

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
- `status` - Provision status: `provisioning`, `ready`, or `failed`
- `fail_count` - Number of provision failures
- `last_error` - Full diagnostic error message (exit codes, OOM, truncated logs)

**Error Responses:**
- `404 Not Found` - Lease not provisioned (or diagnostics expired)

### GET /logs/{lease_uuid}

Get container logs for a specific lease. Used by fred to serve `GET /v1/leases/{uuid}/logs` to tenants. Falls back to persisted logs when containers no longer exist.

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

### GET /health

Simple health check endpoint.

**Response:** `200 OK`

Return 200 if your backend can accept requests. Fred uses this for health monitoring.

### POST /refresh-state (Optional)

Synchronize in-memory provision state with the underlying infrastructure. Fred's HTTP client does **not** call this endpoint — it is a no-op for HTTP backends because the backend server is expected to maintain its own state. This endpoint exists in the `Backend` interface (`RefreshState`) for in-process backends (like the Docker backend) that need to re-read container/VM state before the reconciler calls `ListProvisions`.

If your backend maintains an in-memory cache of provisions, you may choose to expose this endpoint so an external trigger can force a state refresh. Otherwise, you can safely ignore it.

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

This endpoint is optional but recommended for production backends.

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
  "error": ""
}
```

**Note:** The timestamp must be the current Unix time when sending the request. Callbacks with timestamps older than 5 minutes or more than 1 minute in the future are rejected.

**Fields:**
- `status`: Either `"success"` or `"failed"`
- `error`: Error message if status is `"failed"`, empty otherwise

### HMAC Signature with Replay Protection

Fred verifies callbacks using HMAC-SHA256 with timestamp-based replay protection (following the Stripe pattern). Callbacks older than 5 minutes are rejected.

**Signature format:** `t=<unix-timestamp>,sha256=<hex-encoded-hmac>`

The HMAC is computed over `<timestamp>.<body>` to bind the timestamp to the signature:

```go
import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "time"
)

func computeSignature(body []byte, secret string) string {
    timestamp := time.Now().Unix()
    signedPayload := fmt.Sprintf("%d.%s", timestamp, body)

    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(signedPayload))
    sig := hex.EncodeToString(mac.Sum(nil))

    return fmt.Sprintf("t=%d,sha256=%s", timestamp, sig)
}

// Usage:
body, _ := json.Marshal(callbackPayload)
signature := computeSignature(body, os.Getenv("CALLBACK_SECRET"))
req.Header.Set("X-Fred-Signature", signature)
```

The `CALLBACK_SECRET` must match Fred's `callback_secret` configuration.

### Security Notes

- **Replay protection**: Callbacks older than 5 minutes are rejected
- **Clock skew tolerance**: Timestamps up to 1 minute in the future are accepted
- **Industry standard**: Follows the same pattern as Stripe, GitHub, and Slack webhooks

## State Management

### In-Memory Pattern (Recommended for Starting)

Follow the pattern from `cmd/mock-backend/main.go` (the HTTP reference implementation):

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
    mux.HandleFunc("POST /provision", b.handleProvision)
    mux.HandleFunc("GET /info/{lease_uuid}", b.handleGetInfo)
    mux.HandleFunc("GET /provisions/{lease_uuid}", b.handleGetProvision)
    mux.HandleFunc("GET /logs/{lease_uuid}", b.handleGetLogs)
    mux.HandleFunc("POST /deprovision", b.handleDeprovision)
    mux.HandleFunc("GET /provisions", b.handleListProvisions)
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

See `cmd/mock-backend/main.go` for a complete working example. Key sections:

| Function | Description |
|----------|-------------|
| `MockBackendServer` struct | Server setup with callback URL tracking |
| `handleProvision` | Provision handler with async goroutine |
| `handleGetInfo` | GetInfo handler |
| `handleDeprovision` | Deprovision handler (idempotent) |
| `handleListProvisions` | ListProvisions for reconciliation |
| `sendCallback`, `computeSignature` | Callback sending with HMAC signature |

## Configuration

Your backend should accept configuration via environment variables:

```bash
# Required
MY_BACKEND_ADDR=":9001"                      # Listen address
MY_BACKEND_CALLBACK_SECRET="32-char-min"     # HMAC secret (must match Fred's config)

# Recommended
MY_BACKEND_NAME="my-backend"                 # For logging/metrics
MY_BACKEND_HOST_ADDRESS="192.168.1.100"      # For connection info

# Optional
MY_BACKEND_TLS_CERT="/path/to/cert.pem"      # Enable HTTPS
MY_BACKEND_TLS_KEY="/path/to/key.pem"
```

## Testing Your Backend

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

- [ ] All 7 required HTTP endpoints implemented (`/provision`, `/info/{uuid}`, `/provisions/{uuid}`, `/logs/{uuid}`, `/deprovision`, `/provisions`, `/health`)
- [ ] Provision returns 202 and works asynchronously
- [ ] Callbacks signed with HMAC-SHA256 with timestamp
- [ ] Deprovision is idempotent
- [ ] ListProvisions returns all managed resources
- [ ] State protected with mutex for concurrent access
- [ ] Callback URLs stored per-lease (not globally)
- [ ] Health endpoint returns 200 when operational
- [ ] Graceful shutdown (finish in-flight provisions)
- [ ] (Optional) `/stats` endpoint for resource monitoring
