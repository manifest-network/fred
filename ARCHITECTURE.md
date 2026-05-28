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
│  │  events.lease.created       →  HandleLeaseCreated                   │   │
│  │  events.lease.closed        →  HandleLeaseClosed                    │   │
│  │  events.lease.expired       →  HandleLeaseExpired                   │   │
│  │  events.payload.received    →  HandlePayloadReceived                │   │
│  │  events.backend.callback    →  HandleBackendCallback                │   │
│  │  events.lease.event         →  (fan-out to WebSocket subscribers)   │   │
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
│  │  │  Routes to       │  │  (interface)     │  │  N parallel      │  │   │
│  │  │  backends,       │  │  Ephemeral map,  │  │  lanes via authz │  │   │
│  │  │  starts          │  │  recovered via   │  │  sub-signers     │  │   │
│  │  │  provisioning    │  │  reconciliation  │  │  (round-robin)   │  │   │
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
├── ChainClient          interface → chain.Client (backed by SignerPool)
│   └── SignerPool       primary + N sub-signers for authz parallel signing
├── BackendRouter        interface → *backend.Router (passed to Orchestrator)
├── InFlightTracker      interface → DefaultInFlightTracker (sync.RWMutex-protected map)
├── PlacementStore       interface → placement.Store (bbolt + cache, optional)
├── Orchestrator         struct    → uses BackendRouter + InFlightTracker + PlacementStore
├── HandlerSet           struct    → uses Orchestrator + Tracker + AckBatcher
├── AckBatcher           struct    → N parallel ackLane workers, round-robin dispatch
├── TimeoutChecker       struct    → uses InFlightTracker + LeaseRejecter
└── PayloadStore         struct    → bbolt-backed (optional)

Reconciler (independent)
├── ReconcilerChainClient  interface → chain.Client
├── Acknowledger           interface → AckBatcher (routes acks through parallel lanes)
├── BackendRouter          interface → *backend.Router
├── PlacementStore         interface → placement.Store (syncs on startup)
└── ReconcilerTracker      interface → Manager (extends InFlightTracker)

API Handlers
├── PlacementLookup        interface → placement.Store (read-only, optional)
└── BackendRouter          *backend.Router (concrete; only provisioner uses interface)
```

Key interfaces defined where they're consumed:

| Interface | Defined in | Used by |
|-----------|-----------|---------|
| `ChainClient` | `provisioner/topics.go` | Manager, HandlerSet, AckBatcher |
| `ReconcilerChainClient` | `provisioner/reconciler.go` | Reconciler |
| `BackendRouter` | `provisioner/interfaces.go` | Orchestrator |
| `InFlightTracker` | `provisioner/tracker.go` | Orchestrator, HandlerSet, TimeoutChecker |
| `ReconcilerTracker` | `provisioner/tracker.go` | Reconciler |
| `PlacementStore` | `provisioner/interfaces.go` | Orchestrator, Reconciler |
| `PlacementLookup` | `api/handlers.go` | API read handlers |
| `LeaseRejecter` | `provisioner/interfaces.go` | TimeoutChecker |
| `Acknowledger` | `provisioner/ack_batcher.go` | HandlerSet, Reconciler (lease acknowledgement via parallel lanes) |
| `CallbackPublisher` | `api/server.go` | API callback handler |
| `StatusChecker` | `api/server.go` | API status handler |

## State Reference

Two state machines run side-by-side: the chain owns the lease's billing state, and Fred (with the backend) owns the provision's runtime state. Both appear in API responses, metrics, and reconciliation logic.

### Chain Lease State

Authoritative on-chain field. The chain emits events when this changes; Fred reacts.

| State | Description | Source |
|---|---|---|
| `PENDING` | Lease created on-chain. Awaiting provisioning (and a payload upload if `meta_hash` is set). | `LEASE_STATE_PENDING` |
| `ACTIVE` | Provisioned and acknowledged. Tenant can use the resource and is being billed. | `LEASE_STATE_ACTIVE` |
| `CLOSED` | Tenant-closed or auto-closed (credit depletion, expiry). Resources should be deprovisioned. | `LEASE_STATE_CLOSED` |
| `EXPIRED` | Lease's time bound elapsed. Same handling as CLOSED. | `LEASE_STATE_EXPIRED` |

### Provision Status

Fred-internal runtime status of the provisioned resource. Surfaced via `GET /v1/leases/{uuid}/provision`, the docker-backend's `GET /provisions/{uuid}`, and the lease state machine in `internal/backend/docker/lease_sm.go`.

| Status | Description | Terminal? |
|---|---|---|
| `provisioning` | Backend has accepted the request and is creating the resource. | No |
| `ready` | Resource is healthy and serving traffic. | No |
| `failing` | Container death detected; diagnostics being gathered. Brief transient — collapses to `failed` (or `deprovisioning` if a Deprovision arrives in this window). | No |
| `failed` | Provisioning or runtime failure. May be re-provisioned, restarted, updated, or deprovisioned. | Yes (until next request) |
| `restarting` | Containers are being recreated with the same manifest. | No |
| `updating` | New manifest is being deployed (containers replaced). | No |
| `deprovisioning` | Containers/volumes are being removed. | Yes (transient before actor exits) |
| `unknown` | Reserved safety state — should never appear in normal flow. | — |

The full transition matrix lives in [internal/backend/docker/README.md](internal/backend/docker/README.md#lease-state-machine). The reconciler matrix in [README.md](README.md#reconciliation) shows how chain state × provision status maps to corrective actions.

## Event Flow

### Lease Creation (No Payload)

```
1. Tenant creates lease on chain
2. Chain emits lease_created event
3. Event Subscriber receives via WebSocket
4. Event Bridge publishes to Watermill topic
5. HandleLeaseCreated:
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
   a. GetInFlight from in-flight tracker for lease (non-destructive)
   b. If success: acknowledge lease on chain via AckBatcher
   c. If failed + PENDING: reject lease on chain, then untrack
   d. If failed + ACTIVE: untrack and defer to reconciler (retry/reject by FailCount)
```

### Lease Creation (With Payload)

```
1. Tenant creates lease with meta_hash on chain
2. Chain emits lease_created event
3. HandleLeaseCreated sees meta_hash, waits for payload
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
3. HandleLeaseClosed:
   a. Clean up stored payload (if any)
   b. Fetch lease from chain for SKU routing hint
   c. Route to backend by SKU, call backend POST /deprovision
   d. Backend cleans up resources (idempotent)
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
4. Close event broker (send clean close frames to WebSocket clients)
5. Cancel context (signals all components)
6. Stop withdrawal scheduler (wait for in-flight tx)
7. Close event subscriber
8. Wait for all goroutines (with timeout)
9. Close provision manager (cleanup Watermill + stores)
10. If timed out: additional 2s grace period for lingering components
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

### Lease Actor Model (Docker backend)

The Docker backend replaces lock-heavy mutation of shared provision state with a **single-writer actor per lease**. Each active lease owns a dedicated goroutine — a `leaseActor` — that serializes all state-mutating operations for that lease. This is the central concurrency primitive of the Docker backend.

**Structure:**

```go
type leaseActor struct {
    leaseUUID string
    sm        *leaseSM                 // stateless.StateMachine wrapper
    inbox     chan leaseMessage        // buffered — caller enqueues, actor dequeues
    ...
}

type leaseMessage interface {
    isLeaseMessage()
    doneChan() chan struct{}          // closed after message is processed
    onPanic(err error)                 // unblocks caller if a handler panics
}
```

Messages are value types. The full set: `containerDiedMsg`, `deprovisionMsg`, `diagGatheredMsg`, `provisionRequestedMsg`, `provisionCompletedMsg`, `provisionErroredMsg`, `restartRequestedMsg`, `updateRequestedMsg`, `replaceCompletedMsg`, `replaceRecoveredMsg`, `replaceFailedMsg`. Each implements `leaseMessage`.

Synchronous callers signal back through one of two channels:
- **`reply chan error`** (deprovisionMsg only) — receives the operation outcome.
- **`ack chan error`** (provisionRequested / restartRequested / updateRequested) — one-shot accept/reject. The actor sends `Fire`'s result so the caller knows whether the SM accepted the transition before returning; the actual work then runs asynchronously and reports back via the corresponding completed/errored/replace-outcome message.

Fire-and-forget messages (containerDied, diagGathered, the three terminal "completed/errored/replace-outcome" messages) use neither channel.

**State machine:**

Built on [`qmuntal/stateless`](https://github.com/qmuntal/stateless), states come from `backend.ProvisionStatus`. All eight statuses are configured (`Provisioning, Ready, Failing, Failed, Restarting, Updating, Deprovisioning, Unknown`) so `Fire` never hits an unconfigured state; `Unknown` exists as a safety state and is not part of normal flow.

```
           ┌──────────────────┐
           │   Provisioning   │ ──evProvisionCompleted──► Ready
           └──────────────────┘ ──evProvisionErrored────► Failed
                   │                   ──evDeprovisionRequested──► Deprovisioning
                   ▼
  Ready ──evContainerDied[guard]──► Failing ──evDiagGathered──► Failed
    │                                 │
    ├──evRestartRequested──► Restarting ──evReplaceCompleted──► Ready
    │                               ├──evReplaceRecovered──► Ready
    │                               └──evReplaceFailed────► Failed
    ├──evUpdateRequested──► Updating (same shape as Restarting)
    └──evDeprovisionRequested──► Deprovisioning ──evContainersRemoved──► (actor exits)

  Failed  ──evProvisionRequested──► Provisioning   (retry from Failed)
  Failed  ──evRestartRequested────► Restarting     (Restart over a Failed lease)
  Failed  ──evUpdateRequested─────► Updating       (Update over a Failed lease)
  Failing ──evProvisionRequested──► Provisioning   (retry from Failing; OnExit
  Failing ──evRestartRequested────► Restarting     cancels the diag goroutine,
  Failing ──evUpdateRequested─────► Updating       removing the wedge)
```

All states are configured up front with explicit `Permit`/`Ignore`/`OnEntry`/`OnExit` rules. Triggers that don't match a permit become **explicit Ignore** (no-op, no error) rather than unhandled-trigger errors.

**Cancel-on-exit for stale callbacks:**

`Failing`, `Provisioning`, `Restarting`, and `Updating` each run an async goroutine (diagnostics gathering, container provisioning, or atomic replace). On `OnExit`, the actor cancels the goroutine's context and waits for it to finish. This is the **structural suppression** that prevents a stale `Failed` or `Success` callback from being emitted after the lease has moved on (see `onExitFailing`, `onExitProvisioning`).

**Why this shape:**

- **No held locks during slow I/O** — all Docker calls happen outside any shared mutex; linearization is enforced by the inbox.
- **Deterministic preemption** — a `Deprovision` that arrives mid-provisioning cancels the in-flight work via `OnExit` and transitions cleanly to `Deprovisioning`.
- **Blast-radius-contained panics** — each message is wrapped in `recover()`. The actor survives, other leases are unaffected, and the panicking caller is unblocked via `onPanic`.
- **Observable transitions** — every SM transition is counted in `lease_sm_transitions_total{source,destination,trigger}`.

**Registry & lifecycle:**

- `Backend.actors` is a `map[leaseUUID]*leaseActor`; `actorForLocked` resolves-or-creates under a short mutex.
- An actor self-removes from the registry after `Deprovisioning` completes (via a deferred `removeFromRegistry`).
- `errActorTerminated` is returned when a new message arrives at an actor whose `Deprovisioning` has just completed but whose registry cleanup hasn't yet fired — the caller rolls back and retries, getting a fresh actor.

**Inbox delivery and backpressure:**

The inbox is buffered. Three distinct delivery paths cover the cases that actually arise:

- **`routeToLease`** — production fast path for fire-and-forget messages from container-event and reconcile sites. Resolves-or-creates the actor and enqueues atomically under the registry mutex; the enqueue itself is non-blocking (no timeout — full inbox = immediate refusal). Refusals are counted in `die_event_dropped_total`. The reconciler re-detects any missed transition on its next cycle, so drops degrade the realtime event path but do not lose data.
- **`routeToLeaseBlocking`** — wraps `routeToLease` with ctx-bounded retry for caller-facing API paths (Provision/Deprovision/Restart/Update) that need backpressure-with-retry rather than fast refusal.
- **`sendTerminal`** — used by in-flight worker goroutines to deliver terminal SM events whose physical work has already happened on the host (containers swapped, removed, etc.). Bounded by `terminalSendTimeout` (10s) and refuses on `hasExited`, `isExiting`, or send timeout. Refusals are counted in `lease_terminal_event_dropped_total`; recovery falls to the next reconcile cycle.

A bare `send()` method exists for tests only — production code never holds an actor pointer directly.

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

All metrics use the `fred_` namespace and are exposed at `/metrics`. The docker-backend exposes its own set under `fred_docker_backend_*` at the docker-backend's own `/metrics` endpoint.

#### Fred (`/metrics` on the providerd HTTP server)

**API:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_api_requests_total` | counter | `method, path, status` | API request count |
| `fred_api_request_duration_seconds` | histogram | `method, path, status` | Request latency |
| `fred_api_rate_limit_rejections_total` | counter | `limiter` | Rate limit rejections (`global`, `tenant`) |
| `fred_api_non_in_flight_callbacks_total` | counter | `backend, status` | Callbacks for leases not tracked in-flight (restart/update completions, late delivery, intentional deprovision) |

**Provisioner:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_provisioner_in_flight_provisions` | gauge | — | Current in-flight provisions |
| `fred_provisioner_provisioning_total` | counter | `outcome, backend` | Provisioning operations by outcome/backend |
| `fred_provisioner_provisioning_duration_seconds` | histogram | `backend` | Provisioning latency |
| `fred_provisioner_callback_timeouts_total` | counter | — | Backend callback timeouts |
| `fred_provisioner_ack_batch_fee_gas_errors_total` | counter | `lane` | Ack-batch failures classified as insufficient-fee or out-of-gas — sustained non-zero indicates `gas_limit`/`max_gas_limit`/fee misconfiguration |
| `fred_provisioner_ack_batch_individual_fallbacks_total` | counter | `lane` | Ack-batch failures that fell back to per-lease retries |
| `fred_provisioner_reconciler_inflight_skips_total` | counter | — | Ready leases the reconciler skipped because the main flow owns them |
| `fred_provisioner_reconciler_panics_total` | counter | `stage` | Panics recovered in reconciler goroutines (`process_lease`, `process_orphan`, `fetch_provisions`) — any non-zero is a latent bug |

**Reconciler:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_reconciler_runs_total` | counter | `outcome` | Reconciliation runs by outcome |
| `fred_reconciler_duration_seconds` | histogram | — | Run timing |
| `fred_reconciler_actions_total` | counter | `action` | Actions taken (`provisioned`, `acknowledged`, `deprovisioned`, `anomaly`, `lease_error`) |
| `fred_reconciler_last_success_timestamp_seconds` | gauge | — | Unix timestamp of last successful run |
| `fred_reconciler_conflicts_total` | counter | — | Reconciler conflicts (lease already in-flight) |

**Backend:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_backend_requests_total` | counter | `backend, operation, status` | Backend request count |
| `fred_backend_request_duration_seconds` | histogram | `backend, operation, status` | Backend request latency |
| `fred_backend_circuit_breaker_state` | gauge | `backend` | Circuit breaker state (0=closed, 1=half-open, 2=open) |
| `fred_backend_healthy` | gauge | `backend` | Backend health (1=healthy, 0=unhealthy) |
| `fred_backend_insufficient_resources_total` | counter | `backend` | Capacity 503s |

**Chain:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_chain_transactions_total` | counter | `type, outcome` | Chain transactions (`acknowledge`, `reject`, `withdraw`, `close`) |
| `fred_chain_query_duration_seconds` | histogram | `query` | Chain query latency |
| `fred_chain_signer_oog_retries_total` | counter | `result` | Out-of-gas retry decisions at the broadcast layer (`retried`, `exhausted`) |

**Payload:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_payload_uploads_total` | counter | `outcome` | Uploads (`success`, `invalid_auth`, `hash_mismatch`, `conflict`, `error`) |
| `fred_payload_stored_count` | gauge | — | Payloads currently stored |
| `fred_payload_size_bytes` | histogram | — | Upload size distribution |
| `fred_payload_leases_awaiting` | gauge | — | Leases waiting for payload upload |

**Signer pool:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_signer_pool_size` | gauge | — | Total signers (primary + sub-signers) |
| `fred_signer_pool_lane_count` | gauge | — | Active batcher lanes |
| `fred_signer_balance` | gauge | `role, address, index, denom` | Per-signer balance in the configured fee denom, sampled on each `/metrics` scrape via a custom collector with a 5s per-scrape timeout and parallel per-address bank queries. `role` ∈ `provider`, `sub_signer`. `address` is the bech32 verbatim from the live signer pool (so `DemoteToSingleSigner` is reflected on the next scrape). `index` is the slice position (`0..N-1`) for `sub_signer`; empty for `provider`. `denom` is the bank denom queried, sourced from `cfg.FeeDenom` (default `umfx`). Single-signer mode naturally emits only the `provider` series. Per-address query failures drop that one series and bump the failures counter below; other addresses on the same scrape still emit. |
| `fred_signer_balance_query_failures_total` | counter | `role, address, denom` | Per-address signer balance query failures during scrape sampling (no `index` label — index is gauge-only). Each `slog.Warn` failure on a scrape increments this once. |

**Watermill / events:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_watermill_messages_total` | counter | `topic, outcome` | Watermill messages processed |
| `fred_watermill_poisoned_messages_total` | counter | — | Messages sent to poison queue |
| `fred_events_dropped_total` | counter | `event_type` | Events dropped due to full subscriber channels |
| `fred_messages_malformed_total` | counter | `topic` | Unparseable messages |

**Background goroutine health:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_background_cleanup_panics_total` | counter | `component` | Panics in cleanup loops (token tracker, callback store, etc.) — any non-zero is a bug |
| `fred_background_goroutine_panics_total` | counter | `component` | Panics in long-lived background goroutines |

#### Docker backend (`/metrics` on the docker-backend HTTP server)

All docker-backend metrics live under `fred_docker_backend_*`.

**Provisioning & resources:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_provisions_total` | counter | `outcome` | Provision attempts |
| `fred_docker_backend_deprovisions_total` | counter | — | Deprovision operations |
| `fred_docker_backend_active_provisions` | gauge | — | Active provisions |
| `fred_docker_backend_provision_duration_seconds` | histogram | — | End-to-end provision time |
| `fred_docker_backend_image_pull_duration_seconds` | histogram | — | Image pull duration |
| `fred_docker_backend_container_create_duration_seconds` | histogram | — | Container create duration |
| `fred_docker_backend_resource_cpu_allocated_ratio` | gauge | — | Allocated/total CPU |
| `fred_docker_backend_resource_memory_allocated_ratio` | gauge | — | Allocated/total memory |
| `fred_docker_backend_resource_disk_allocated_ratio` | gauge | — | Allocated/total disk |

**Callbacks:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_callback_delivery_total` | counter | `outcome` | Callback delivery attempts |
| `fred_docker_backend_callback_store_errors_total` | counter | — | bbolt persistence failures for callbacks |

**Reconciliation:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_reconciliation_total` | counter | `outcome` | Reconciliation runs |
| `fred_docker_backend_reconciliation_last_success_timestamp_seconds` | gauge | — | Unix timestamp of last successful reconciliation |
| `fred_docker_backend_idempotent_ops_total` | counter | `op, reason` | Docker operations skipped because the daemon reported the work was already done. Spikes on `remove`/`in_progress` suggest reconciler/event races; spikes on `create`/`already_exists` suggest crash-replay |
| `fred_docker_backend_container_removal_wait_failures_total` | counter | — | RemoveContainer calls where the "in progress" wait did not confirm NotFound before timeout |

**Lease actor / state machine:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_lease_sm_transitions_total` | counter | `from, to, event` | State-machine transitions |
| `fred_docker_backend_lease_actors_created_total` | counter | — | Cumulative actors created since startup |
| `fred_docker_backend_lease_actor_stuck_seconds` | gauge | — | Age of oldest in-flight actor `handle()` call. Alert threshold should exceed the longest legitimate operation (Deprovision can hold the actor for minutes during cleanup) |
| `fred_docker_backend_lease_actor_inbox_depth` | histogram | — | Per-actor inbox depth (cap is 16). Healthy: p99 near 0 |
| `fred_docker_backend_lease_actor_panics_total` | counter | — | Panics recovered in actor handlers — any non-zero is a bug |
| `fred_docker_backend_lease_terminal_event_dropped_total` | counter | `event` | Terminal SM events `sendTerminal` refused to deliver (actor exited, mid-exit, or inbox wedged). Sustained non-zero under clean shutdown indicates a real data-loss pattern |
| `fred_docker_backend_die_event_dropped_total` | counter | `source` | Container-death signals `routeToLease` could not deliver (`event_loop`, `reconcile`). Reconciler re-detects on next cycle, so this is not data loss but flags a wedged actor or chronic burst |
| `fred_docker_backend_lease_failing_race_skipped_total` | counter | — | `onEnterFailing` bails due to concurrent Restart/Update |
| `fred_docker_backend_lease_worker_panics_total` | counter | `worker_type` | Panics in lease worker goroutines (provision/replace/diag) — any non-zero is a latent bug |

### Logging (slog)

Structured logging with consistent fields:
- `lease_uuid` - Always included for lease-related operations
- `tenant` - Included when known
- `backend` - Included for backend operations
- `error` - Included on failures

## Testing Strategy

The full developer-facing test reference (commands, prerequisites, conventions) lives in [CONTRIBUTING.md § Testing](CONTRIBUTING.md#testing). This section covers the testing *philosophy*.

### Layers

| Layer | Where | What it covers | How to run |
|---|---|---|---|
| Unit | `_test.go` next to each file | Single function / type behavior. Mock interfaces for chain, backends, etc. | `make test` |
| Race | Same files, `-race -short` | Concurrency invariants — actor messages, in-flight tracker, signer pool. Stress tests skip via `testing.Short()` because they OOM under `-race`. | `go test -race -short ./...` |
| Integration (provisioner) | `_test.go` with no build tag | Full event flow with the in-memory mock backend. Watermill GoChannel transport. | `make test` |
| Integration (Docker) | `_test.go` with `//go:build integration` | Real Docker daemon, real container lifecycle. | `make test-integration` (requires Docker) |
| Integration (volumes) | Same, gated on root | btrfs/xfs/zfs quota enforcement. | `sudo make test-integration-volume` |
| Stress | `manager_stress_test.go` | 10K–1M event burst tests, sustained-load tests. | See [PERFORMANCE.md](PERFORMANCE.md#running-benchmarks); 500K/1M gated by `STRESS_TEST_LARGE=1` |
| Bench | `*_bench_test.go` | Throughput and latency profiling. | `go test -bench=.` |

### Test fixtures

Common test data lives in `internal/testutil/fixtures.go`:

- `NewTestKeyPair(seed)` — deterministic secp256k1 keypair from a string seed
- `CreateTestToken` / `CreateTestPayloadToken` — signed ADR-036 bearer tokens (see [tenant-quickstart.md](docs/tenant-quickstart.md) for the canonical token shape)
- `CreateExpiredToken`, `CreateFutureToken` — variants for negative testing

Chain-client mocks live in `internal/chain/chaintest/` and are deliberately separated from package `chain` so they do not link into the production `providerd` binary.

### Failure-mode coverage

Race-detector runs and integration tests catch most concurrency bugs. The patterns to match when adding a feature:

- **Multiple-message races**: cover the case where two messages arrive at a lease actor in the same tick (see `lease_actor_test.go::TestLeaseActor_DeprovisionDuringProvisioning`).
- **Reconciler vs event-loop races**: cover cases where the reconciler and the realtime event path observe the same transition (see `recover_state_test.go`).
- **Backend timeout/circuit-breaker**: cover cases where the backend client fails fast on a circuit-open state.
- **bbolt I/O failures**: cover cases where the writer goroutine fails — the package must not silently lose data.

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

Callbacks (backend → Fred) and inbound requests (Fred → backend) use
HMAC-SHA256 with a four-field canonical string that binds the timestamp,
HTTP method, request URI, and a hash of the body:

```
1. Sender constructs the canonical string:
     "<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>"
2. Sender computes HMAC-SHA256(canonical_string, shared_secret)
3. Sender sends X-Fred-Signature: t=<timestamp>,sha256=<hex>
4. Verifier extracts r.Method and r.URL.RequestURI() from the request
5. Verifier reads the body and hashes it (sha256.Sum256)
6. Verifier rejects if timestamp > 5 minutes old (replay protection)
7. Verifier rejects if timestamp > 1 minute in future (clock skew limit)
8. Verifier recomputes the canonical string + HMAC and compares (constant-time)
```

Binding the method and request URI prevents cross-endpoint replay
(a captured `POST /provision` signature cannot be replayed against
`POST /deprovision`, nor a `GET /info/<id>` against `GET /logs/<id>`).
Including the body as a SHA-256 hash rather than as a literal string
keeps the canonical string bounded in length and binary-safe.

### Defense in Depth

- Input validation at API boundary
- Rate limiting per-IP and per-tenant
- Request size limits
- TLS for transport security
- Generic error messages to clients
- Security headers on all responses
