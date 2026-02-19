# Fred Architecture

This document describes the architectural decisions and design patterns used in fred, the Manifest Provider Daemon.

## Overview

Fred is a **lease lifecycle manager** that bridges Manifest Network's on-chain billing system with pluggable backend provisioning systems. It uses an **event-driven architecture** with **level-triggered reconciliation** for crash recovery.

## Design Principles

1. **Chain is Source of Truth**: Lease state always comes from the blockchain
2. **Backend is Source of Truth for Provisions**: What's actually running is determined by querying backends
3. **Eventual Consistency**: Events trigger actions, reconciliation fixes drift
4. **Fail-Safe**: When in doubt, query current state rather than trusting cached data
5. **Idempotent Operations**: All operations can be safely retried

## Key Architectural Decisions

### Why Watermill for Event Routing?

We evaluated several approaches for internal event routing:

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Akash Provider** | Battle-tested | Tightly coupled to K8s | Too much adaptation |
| **Temporal** | Durable workflows | Heavy operational overhead | Overkill |
| **100% custom** | Full control | Reinvent routing, retries | Too much work |
| **Watermill** | Event routing, middleware, testable | Additional dependency | **Selected** |

Watermill provides:
- **Router**: Routes messages to handlers (like HTTP router but for events)
- **Middleware**: Built-in retries, panic recovery, logging
- **Testing**: In-memory GoChannel makes unit testing straightforward
- **Flexibility**: Can swap transports (channels, Kafka, Redis) without code changes

### Why Level-Triggered Reconciliation?

We use **level-triggered** (state-based) rather than **edge-triggered** (event-based) reconciliation:

```
Edge-triggered (what we DON'T do):
  "What events did I miss?" → Requires durable event queue, replay logic

Level-triggered (what we DO):
  "What is the current state?" → Query chain + backends, compare, act
```

**Benefits:**
- No need for durable event storage
- Simpler crash recovery (just query current state)
- Handles any inconsistency, not just missed events
- Self-healing: periodic reconciliation fixes drift from any cause

**Trade-off:**
- More chain/backend queries
- Slightly higher latency for crash recovery vs. event replay

### Why Not Open Service Broker API?

OSB API assumes the caller initiates provisioning:

```
OSB: Platform ──API call──> Broker ──> Provision
```

Manifest Network requires chain events to trigger provisioning:

```
Manifest: Tenant ──tx──> Chain ──event──> Fred ──> Provision
```

The tenant shouldn't need to call Fred directly - provisioning should happen automatically when a lease is created on-chain.

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FRED                                         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Event Subscriber                                │   │
│  │  WebSocket ──parse──> LeaseEvent ──fan-out──> Multiple consumers    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│           │                                      │                          │
│           │                                      │                          │
│           ▼                                      ▼                          │
│  ┌─────────────────┐                   ┌─────────────────┐                 │
│  │  Event Bridge   │                   │    Watcher      │                 │
│  │  (our provider) │                   │ (cross-provider)│                 │
│  └────────┬────────┘                   └────────┬────────┘                 │
│           │                                      │                          │
│           ▼                                      ▼                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      Watermill Router                                │   │
│  │                                                                     │   │
│  │  Middleware: [PoisonQueue, Retry, Recoverer]                           │   │
│  │                                                                     │   │
│  │  Topics → Handlers (HandlerSet):                                    │   │
│  │  ─────────────────────────────────────────────────────────────      │   │
│  │  events.lease.created       →  handleLeaseCreated                   │   │
│  │  events.lease.closed        →  handleLeaseClosed                    │   │
│  │  events.lease.expired       →  handleLeaseExpired                   │   │
│  │  events.payload.received    →  handlePayloadReceived                │   │
│  │  events.backend.callback    →  handleBackendCallback                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                   │                                         │
│                                   ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Provision Manager                                │   │
│  │                                                                     │   │
│  │  Coordinator that wires together:                                   │   │
│  │                                                                     │   │
│  │  ┌──────────────────┐  ┌─────────────────┐  ┌──────────────────┐  │   │
│  │  │   Orchestrator   │  │  InFlightTracker │  │   AckBatcher     │  │   │
│  │  │  Routes to       │  │  (interface)     │  │  Batches chain   │  │   │
│  │  │  backends,       │  │  Ephemeral map,  │  │  ack txns for    │  │   │
│  │  │  starts          │  │  recovered via   │  │  efficiency      │  │   │
│  │  │  provisioning    │  │  reconciliation  │  │                  │  │   │
│  │  └──────────────────┘  └─────────────────┘  └──────────────────┘  │   │
│  │                                                                     │   │
│  │  ┌──────────────────┐  ┌─────────────────┐                        │   │
│  │  │  TimeoutChecker  │  │   PayloadStore   │                        │   │
│  │  │  Rejects leases  │  │  Temp storage    │                        │   │
│  │  │  with expired    │  │  for tenant      │                        │   │
│  │  │  callbacks       │  │  payloads (bbolt)│                        │   │
│  │  └──────────────────┘  └─────────────────┘                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Reconciler (independent component)                                 │   │
│  │  Level-triggered state comparison: chain vs backends                │   │
│  │  Calls RefreshState on each backend before reading provisions       │   │
│  │  Runs on startup + periodically, uses worker pool                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dependency Injection

All components are wired via interfaces, not concrete types. This enables testing
each component in isolation with mocks and allows swapping implementations.

```
Manager (coordinator)
├── ChainClient          interface → chain.Client
├── BackendRouter        *backend.Router (passed to Orchestrator via interface)
├── InFlightTracker      interface → inFlightMap (sync.RWMutex-protected map)
├── PlacementStore       interface → placement.Store (bbolt + cache, optional)
├── Orchestrator         struct    → uses BackendRouter + InFlightTracker + PlacementStore
├── HandlerSet           struct    → uses Orchestrator + Tracker + AckBatcher
├── AckBatcher           struct    → uses ChainClient
├── TimeoutChecker       struct    → uses InFlightTracker + LeaseRejecter
└── PayloadStore         struct    → bbolt-backed (optional)

Reconciler (independent)
├── ReconcilerChainClient  interface → chain.Client
├── BackendRouter          *backend.Router
├── PlacementStore         interface → placement.Store (syncs on startup)
└── ReconcilerTracker      interface → Manager (extends InFlightTracker)

API Handlers
├── PlacementLookup        interface → placement.Store (read-only, optional)
└── BackendRouter          *backend.Router
```

Key interfaces defined where they're consumed:

| Interface | Defined in | Used by |
|-----------|-----------|---------|
| `ChainClient` | `provisioner/manager.go` | Manager, HandlerSet, AckBatcher |
| `ReconcilerChainClient` | `provisioner/reconciler.go` | Reconciler |
| `BackendRouter` | `provisioner/interfaces.go` | Orchestrator |
| `InFlightTracker` | `provisioner/tracker.go` | Orchestrator, HandlerSet, TimeoutChecker |
| `ReconcilerTracker` | `provisioner/tracker.go` | Reconciler |
| `PlacementStore` | `provisioner/interfaces.go` | Orchestrator, Reconciler |
| `PlacementLookup` | `api/handlers.go` | API read handlers |
| `LeaseRejecter` | `provisioner/interfaces.go` | TimeoutChecker |
| `Acknowledger` | `provisioner/ack_batcher.go` | HandlerSet (lease acknowledgement) |
| `CallbackPublisher` | `api/server.go` | API callback handler |
| `StatusChecker` | `api/server.go` | API status handler |

## Event Flow

### Lease Creation (No Payload)

```
1. Tenant creates lease on chain
2. Chain emits lease_created event
3. Event Subscriber receives via WebSocket
4. Event Bridge publishes to Watermill topic
5. handleLeaseCreated:
   a. Check if lease already in-flight (idempotency)
   b. Route to backend by SKU (round-robin if multiple backends match)
   c. Call backend POST /provision with callback URL
   d. Track as in-flight, record placement (lease→backend)
6. Backend provisions resource asynchronously
7. Backend calls POST /callbacks/provision (API server):
   a. Verify HMAC signature (CallbackAuthenticator)
   b. Parse and validate callback payload
   c. Publish to Watermill topic (events.backend.callback)
8. handleBackendCallback (Watermill handler):
   a. If success: acknowledge lease on chain
   b. If failed: reject lease on chain
   c. Remove from in-flight tracking after chain ack succeeds
```

### Lease Creation (With Payload)

```
1. Tenant creates lease with meta_hash on chain
2. Chain emits lease_created event
3. handleLeaseCreated sees meta_hash, waits for payload
4. Tenant POSTs payload to /v1/leases/{uuid}/data
5. Fred validates SHA-256(payload) == lease.meta_hash
6. Fred stores payload, publishes to Watermill
7. handlePayloadReceived:
   a. Retrieve stored payload
   b. Call backend POST /provision with payload
   c. Continue as above
```

### Lease Closure

```
1. Tenant closes lease (or credit depleted)
2. Chain emits lease_closed event
3. handleLeaseClosed:
   a. Route to backend by SKU
   b. Call backend POST /deprovision
   c. Backend cleans up resources (idempotent)
```

## Concurrency Model

### Goroutine Management

All long-running goroutines are:
1. Tracked via `sync.WaitGroup`
2. Wrapped with panic recovery via `safeGo()`
3. Cancellable via context

```go
// Uses sync.WaitGroup.Go (Go 1.25+) for cleaner goroutine management.
func safeGo(wg *sync.WaitGroup, errChan chan<- error, component string, fn func() error) {
    wg.Go(func() {
        defer func() {
            if r := recover(); r != nil {
                errChan <- fmt.Errorf("%s panic: %v", component, r)
            }
        }()
        if err := fn(); err != nil && !errors.Is(err, context.Canceled) {
            errChan <- fmt.Errorf("%s error: %w", component, err)
        }
    })
}
```

### Startup Sequence

The startup order is critical to avoid race conditions:

```
1. Start API server (wait for it to be listening)
   └─ Must be ready before reconciliation triggers callbacks
2. Start provision manager (wait for Watermill handlers to be subscribed)
   └─ Must be ready before callbacks arrive from backends
3. Perform initial withdrawal
4. Perform startup reconciliation
   └─ May provision leases, triggering backend callbacks
5. Start remaining components in parallel:
   - Event subscriber
   - Event bridge
   - Lease watcher
   - Withdrawal scheduler
   - Periodic reconciler
```

**Why this order matters:** Startup reconciliation detects unprovisioned leases and sends provision requests to backends. Backends respond with callbacks to Fred's API. If the API server isn't listening yet, callbacks fail with "connection refused". If the provision manager's Watermill handlers aren't subscribed yet, callbacks fail with "No subscribers to send message".

### State Protection

- **In-flight map**: Protected by `sync.RWMutex`
- **Reconciler flag**: Uses `atomic.Bool` to prevent concurrent reconciliation
- **Event subscriber channels**: Lock-free via atomic closed flag + WaitGroup

### Graceful Shutdown

```
1. Receive SIGINT/SIGTERM
2. Wait for in-flight provisions to drain (with timeout)
   └─ API server stays running to receive backend callbacks
3. Stop API server (stop accepting new requests)
4. Cancel context (signals all components)
5. Stop withdrawal scheduler (wait for in-flight tx)
6. Close event subscriber
7. Wait for all goroutines (with timeout)
8. Close provision manager (cleanup Watermill + stores)
```

## Backend Integration

### Router Design

The backend router matches leases to backends by exact SKU UUID. When multiple backends share the same SKU list, `RouteRoundRobin` distributes new provisions across them using an atomic counter. A placement store (bbolt) records which backend serves each lease so that read operations always reach the correct machine.

```go
type Router struct {
    backends       []backendEntry
    backendsByName map[string]Backend
    defaultBackend Backend
    counter        atomic.Uint64  // round-robin counter
}

func (r *Router) Route(sku string) Backend       // first match (deterministic)
func (r *Router) RouteAll(sku string) []Backend   // all matching backends
func (r *Router) RouteRoundRobin(sku string) Backend // round-robin across matches
```

**Routing strategies:**
- `Route` — returns the first matching backend (used for deprovision fallback and read-path when no placement exists)
- `RouteRoundRobin` — distributes across all matching backends (used for new provisions)
- **Placement lookup** — maps `lease_uuid → backend_name` for read-path routing (connection, logs, diagnostics)

When a single backend matches a SKU, all three strategies behave identically.

### Circuit Breaker

Each backend client uses sony/gobreaker for circuit breaker protection:

```
States: Closed → Open → Half-Open → Closed
        (healthy) (failing) (testing) (recovered)
```

When a backend is unhealthy, requests fail fast with `ErrCircuitOpen` rather than waiting for timeouts.

**What counts as a failure:**
- Network errors (connection refused, timeout)
- HTTP 5xx errors (server errors)

**What does NOT count as a failure (exempted via `IsSuccessful`):**
- `ErrNotProvisioned` (HTTP 404) — valid "lease not found" from read endpoints
- `ErrValidation` (HTTP 400) — permanent client error, won't succeed on retry
- `ErrAlreadyProvisioned` (HTTP 409 from Provision) — idempotent duplicate
- `ErrInvalidState` (HTTP 409 from Restart/Update) — wrong lease state for operation
- `ErrInsufficientResources` (HTTP 503 from Provision) — backend at capacity, not unhealthy

This ensures that expected business conditions don't trip the circuit breaker and block backend operations.

## Data Flow

### Placement Store

Tracks which backend serves each lease (bbolt + in-memory cache):
- Written when provisioning starts (after `RouteRoundRobin` picks a backend)
- Read on every tenant API call (connection, logs, diagnostics) to route to the correct backend
- Deleted on deprovision or lease closure
- Rebuilt on startup: the reconciler calls `SetBatch` with placements from all backends' `ListProvisions`
- Optional — only needed when multiple backends share the same SKU list

### Payload Store

Tenant payloads are stored temporarily in bbolt (an embedded key-value store):
- Written when payload uploaded
- Read when provisioning starts
- Deleted after successful provision or TTL expiry
- Uses write batching for efficiency under load

### Token Tracker

Used tokens are tracked in bbolt to prevent replay attacks:
- Token signature stored as key
- Expiry time stored as value
- Periodic cleanup removes expired entries
- Survives restarts (persistent)

## Observability

### Metrics (Prometheus)

Key metrics exposed at `/metrics`:
- `fred_api_requests_total` - API request count by method/path/status
- `fred_chain_transactions_total` - Chain transactions by type/outcome
- `fred_backend_requests_total` - Backend request count by backend/operation/status
- `fred_provisioner_in_flight_provisions` - Current in-flight provisions gauge
- `fred_reconciler_duration_seconds` - Reconciliation timing histogram
- `fred_backend_circuit_breaker_state` - Backend circuit breaker states (0=closed, 1=half-open, 2=open)

### Logging (slog)

Structured logging with consistent fields:
- `lease_uuid` - Always included for lease-related operations
- `tenant` - Included when known
- `backend` - Included for backend operations
- `error` - Included on failures

## Testing Strategy

### Unit Tests

- Mock interfaces for chain client, backends
- Table-driven tests for validation logic
- In-memory Watermill for handler tests

### Integration Tests

- Mock backend for end-to-end flow testing
- Simulated delays and failures
- Callback authentication testing

### Test Fixtures

Common test data in `internal/testutil/fixtures.go`:
- Valid/invalid auth tokens
- Lease objects in various states
- Signed messages for ADR-036 testing

## Security Model

### Tenant Authentication

```
1. Tenant creates auth token with:
   - tenant address
   - lease UUID
   - timestamp
   - public key
   - ADR-036 signature

2. Fred validates:
   - Signature matches message
   - Public key derives to tenant address
   - Timestamp not expired (max 30 seconds old)
   - Timestamp not too far in future (max 10 seconds clock skew)
   - Token not previously used (replay protection)
   - Lease belongs to tenant and this provider
```

### Callback Authentication

Callbacks use HMAC-SHA256 with timestamp-based replay protection (following the Stripe pattern):

```
1. Backend computes timestamp and signed payload: "<timestamp>.<body>"
2. Backend computes HMAC-SHA256(signed_payload, shared_secret)
3. Backend sends X-Fred-Signature: t=<timestamp>,sha256=<hex>
4. Fred parses timestamp and signature
5. Fred rejects if timestamp > 5 minutes old (replay protection)
6. Fred rejects if timestamp > 1 minute in future (clock skew limit)
7. Fred recomputes signature and compares (constant-time)
```

### Defense in Depth

- Input validation at API boundary
- Rate limiting per-IP and per-tenant
- Request size limits
- TLS for transport security
- Generic error messages to clients
- Security headers on all responses
