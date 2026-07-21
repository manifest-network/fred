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

Fred-internal runtime status of the provisioned resource. Surfaced via `GET /v1/leases/{uuid}/provision`, the docker-backend's `GET /provisions/{uuid}`, and the lease state machine in `internal/backend/shared/leasesm/lease_sm.go`.

| Status | Description | Terminal? |
|---|---|---|
| `provisioning` | Backend has accepted the request and is creating the resource. | No |
| `ready` | Resource is healthy and serving traffic. | No |
| `failing` | Container death detected; diagnostics being gathered. Brief transient — collapses to `failed` (or `deprovisioning` if a Deprovision arrives in this window). | No |
| `failed` | Provisioning or runtime failure. May be re-provisioned, restarted, updated, or deprovisioned. | Yes (until next request) |
| `restarting` | Containers are being recreated with the same manifest. Also the status a **restore** rides on — it reuses the restart machinery (`evRestoreRequested → Restarting`). | No |
| `updating` | New manifest is being deployed (containers replaced). | No |
| `deprovisioning` | Containers/volumes are being removed. | Yes (transient before actor exits) |
| `retained` | The lease was closed/expired and its data soft-deleted (volumes renamed into the `fred-retained-` namespace), restorable within the grace window. Derived from the backend's retention record (ENG-329), not an in-memory provision state; surfaced by the queryable status API. | Yes |
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
   b. Route to backend by SKU (least-loaded matching backend if several match)
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
   d. Backend cleans up resources (idempotent). With `retain_on_close`, the
      backend soft-deletes volumes instead of destroying them and reports
      `retained: true` on its deprovisioned callback.
```

The lease's **placement record survives close** (only the payload is cleaned up). This is deliberate: it keeps the closed lease's backend resolvable so a later restore can be routed to the node that holds its retained data (restore affinity, ENG-333). A `PENDING` lease that is *rejected* (never provisioned) deletes its placement eagerly, since there is no retention to protect.

### Lease Restore

Restore adopts a soft-deleted lease's retained data into a new lease (see the [docker backend restore flow](internal/backend/docker/README.md#soft-delete--restore)):

```
1. Tenant opens a fresh PENDING lease matching the closed lease's shape
2. Tenant POSTs /v1/leases/{new}/restore with from_lease_uuid = {closed}
3. RestoreLease:
   a. Resolve the backend that holds the SOURCE lease's retained data, via the
      source lease's surviving placement (restore is same-backend, ENG-333)
   b. Track the new lease in-flight as a restore BEFORE calling the backend
   c. Call backend POST /restore; optimistically record the new lease's
      placement on the source backend
4. Backend adopts the retained volumes and re-deploys the retained manifest async
5. Backend POSTs the success callback:
   a. Because the lease was tracked in-flight (step 3b), the callback is
      acknowledged INLINE on-chain (ENG-358) — no ~reconciler-interval wait
```

The new lease **may target a different SKU tier** than the source (promote/demote, ENG-438). Only the item *shape* must match (service names + quantities); the SKU/disk tier may differ. A **promote** (same-or-larger disk tier) is always allowed and the new `disk_mb` cap is applied. A **demote** (smaller disk tier) is allowed only if the retained volume's *measured* data fits the new tier's `disk_mb` cap — the backend runs `checkDemoteFit` before adopting. A refused demote returns HTTP 422; on the backend→fred hop the body carries `code=demote_exceeds_tier` (`ErrDemoteDataExceedsTier`), which the fred-api boundary forwards to the tenant as a 422 (whose own `code` field is the numeric status) — distinct from a *bare* 422 (`ErrNotRetained`, no retained data), which fred maps to 404.

Reconciler interplay (level-triggered backstop):

- **Inline ack, reconciler backstop.** Inline acknowledgement (ENG-358) is the fast path; if no restore tracker is wired, the restore still converges because the reconciler acks a `PENDING` + `ready` lease. The reconciler *skips* acking a lease the in-flight tracker already owns (counted by `reconciler_inflight_skips_total`), avoiding a double-ack.
- **Restore affinity sync (ENG-333).** Each reconcile tick fans out `GET /retentions` to every backend and syncs the `lease → backend` map into the placement store, so retained leases stay routable to their source node across restarts.
- **Placement prune grace + deprovision fail-safe (ENG-335).** The reconciler will not prune a placement set younger than a grace window (`2 × reconcile_interval`, measured from sweep start), so a lease that provisioned during a slow sweep is not mis-pruned. When a lease exhausts its re-provision attempts and is closed, the reconciler eagerly calls `backend.Deprovision` on its backend instead of waiting for the next orphan-cleanup cycle (logged at WARN, non-fatal).

### Retention partitioning (aggregator sub-tenancy)

Retention records carry an optional cooperative `partition` — a sub-tenant grouping key an allowlisted (aggregator) tenant declares for its own end-customers via the operator-configured `retention_partition_source`. Partitions only sub-divide the tenant's own budget: the per-tenant aggregate caps always run over the whole record set (a tenant declaring N partitions has the same total budget as N=1), the `""` default bucket is never sub-capped (so any collapsed/legacy/undeclared record behaves exactly as before partitioning existed), count caps evict oldest-first within the tenant, disk caps refuse the incoming close only, and every uncertainty collapses toward "keep". The partition is advisory grouping metadata — never a security boundary (isolation stays keyed on the on-chain tenant) and never load-bearing for restore/reap correctness.

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

The backend router matches leases to backends by exact SKU UUID. When multiple backends share the same SKU list, Fred routes each new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats. A placement store (bbolt) records which backend serves each lease so that read operations always reach the correct machine.

```go
type Router struct {
    backends       []backendEntry
    backendsByName map[string]Backend
    defaultBackend Backend
    counter        atomic.Uint64  // round-robin counter
}

func (r *Router) Route(sku string) Backend                 // first match (deterministic)
func (r *Router) RouteAll(sku string) []Backend             // all matching backends
func (r *Router) RouteForProvision(ctx, sku, inFlight) Backend // least-loaded across matches
func (r *Router) RouteRoundRobin(sku string) Backend       // round-robin across matches
```

**Routing strategies:**
- `Route` — returns the first matching backend (used for deprovision fallback and read-path when no placement exists)
- `RouteForProvision` — routes a new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats
- `RouteRoundRobin` — distributes across all matching backends via an atomic counter (the tie-break and no-stats fallback for `RouteForProvision`)
- **Placement lookup** — maps `lease_uuid → backend_name` for read-path routing (connection, logs, diagnostics)

When a single backend matches a SKU, all strategies behave identically.

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

The Docker backend replaces lock-heavy mutation of shared provision state with a **single-writer actor per lease**. Each active lease owns a dedicated goroutine — a `leasesm.LeaseActor` — that serializes all state-mutating operations for that lease. This is the central concurrency primitive of the Docker backend. The SM/actor machinery lives in the shared, substrate-agnostic package `internal/backend/shared/leasesm` (see [SM is shared across backends](#k3s-backend-experimental)).

**Structure:**

The actor holds **no substrate/backend pointer** — all reach-back into substrate state goes through closures supplied in `cfg` (`lease_actor.go:261`):

```go
type LeaseActor struct {
    leaseUUID        string
    cfg              LeaseActorConfig  // substrate-agnostic closures (incl. ProvisionStore)
    inbox            chan LeaseMessage // buffered — caller enqueues, actor dequeues
    sm               *leaseSM          // stateless.StateMachine wrapper
    pendingDeathInfo *InstanceState
    diagCancel       context.CancelFunc // canceled on exit from Failing
    workCancel       context.CancelFunc // canceled on exit from Provisioning/Restarting/Updating
    replaceWasActive bool
    ...
}

type LeaseMessage interface {
    isLeaseMessage()
    doneChan() chan struct{}          // closed after message is processed
    onPanic(err error)                 // unblocks caller if a handler panics
}
```

Messages are value types. Caller-facing messages are exported: `ContainerDiedMsg`, `DeprovisionMsg`, `ProvisionRequestedMsg`, `RestartRequestedMsg`, `UpdateRequestedMsg` (so substrate packages can construct and route them). The internal terminal/handoff messages remain unexported: `diagGatheredMsg`, `provisionCompletedMsg`, `provisionErroredMsg`, `replaceCompletedMsg`, `replaceRecoveredMsg`, `replaceFailedMsg`. Each implements `LeaseMessage`.

Synchronous callers signal back through one of two exported channels:
- **`Reply chan error`** (`DeprovisionMsg` only) — receives the operation outcome.
- **`Ack chan error`** (`ProvisionRequestedMsg` / `RestartRequestedMsg` / `UpdateRequestedMsg`) — one-shot accept/reject. The actor sends `Fire`'s result so the caller knows whether the SM accepted the transition before returning; the actual work then runs asynchronously and reports back via the corresponding completed/errored/replace-outcome message.

Fire-and-forget messages (`ContainerDiedMsg`, `diagGatheredMsg`, the three terminal "completed/errored/replace-outcome" messages) use neither channel.

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

**The `LeaseProvisionStore` seam (single-writer substrate):**

The live provision records are owned by the Docker backend, not the actor: `Backend.provisions` is a `map[string]*provision` guarded by `provisionsMu` (`backend.go:72`). The actor never touches that map directly. Instead it mutates provision state **only** through `leasesm.LeaseProvisionStore` — a `backendProvisionStore` adapter (`leasesm_adapters.go:98-140`) wired as `b.provisionStore` (`backend.go:171`, `562`) and reached via the actor's `cfg`. Every SM entry/exit action reads and writes through `cfg.ProvisionStore.Get(...)` and `cfg.ProvisionStore.UpdateFn(...)` (`lease_sm.go:329`, `388`, `458`, `533`); the closure-style `UpdateFn` runs a compound multi-field update inside one `provisionsMu.Lock`, so atomicity is preserved without one method per transition.

This mutex-guarded shared map plus the per-lease serialization goroutine is a **deliberate idiomatic Go hybrid** — the single-writer substrate the actor model is built on. It is **not tech debt and not an unfinished migration**: the map stays shared because recovery and enumeration are inherently cross-lease, while the per-lease ordering guarantee comes from the inbox, and the store adapter takes the same mutex as the direct accessors (`recover.go`, `deprovision.go`, startup mutators) so cross-path atomicity holds.

**Registry & lifecycle:**

- `Backend.actors` is a `map[string]*leasesm.LeaseActor`; `actorForLocked` resolves-or-creates under a short mutex.
- An actor self-removes from the registry after `Deprovisioning` completes (via a deferred `removeFromRegistry`).
- `errActorTerminated` is returned when a new message arrives at an actor whose `Deprovisioning` has just completed but whose registry cleanup hasn't yet fired — the caller rolls back and retries, getting a fresh actor.

**Inbox delivery and backpressure:**

The inbox is buffered. Three distinct delivery paths cover the cases that actually arise:

- **`routeToLease`** — production fast path for fire-and-forget messages from container-event and reconcile sites. Resolves-or-creates the actor and enqueues atomically under the registry mutex; the enqueue itself is non-blocking (no timeout — full inbox = immediate refusal). Refusals are counted in `die_event_dropped_total`. The reconciler re-detects any missed transition on its next cycle, so drops degrade the realtime event path but do not lose data.
- **`routeToLeaseBlocking`** — wraps `routeToLease` with ctx-bounded retry for caller-facing API paths (Provision/Deprovision/Restart/Update) that need backpressure-with-retry rather than fast refusal.
- **`sendTerminal`** — used by in-flight worker goroutines to deliver terminal SM events whose physical work has already happened on the host (containers swapped, removed, etc.). Bounded by `terminalSendTimeout` (10s) and refuses on `hasExited`, `isExiting`, or send timeout. Refusals are counted in `lease_terminal_event_dropped_total`; recovery falls to the next reconcile cycle.

A bare `send()` method exists for tests only — production code never holds an actor pointer directly.

### K3s backend (experimental)

`internal/backend/k3s` is an **experimental, non-functional scaffold (ENG-133)**. It boots, serves the full backend contract over HTTP, and wires up config/metrics/health, but its provisioner is a stub: every accepted provision flips to `failed` and posts a `status=failed, error="not implemented"` callback (`internal/backend/k3s/provision_stub.go:15`). Real Pod/Deployment provisioning lands in ENG-134+.

The SM/actor machinery (`internal/backend/shared/leasesm`) is **shared across backends** and substrate-agnostic — the actor holds no backend pointer and reaches all substrate state through `cfg` closures (`LeaseProvisionStore`, `InstanceInspector`, `DiagnosticsGatherer`), so the same single-writer model applies to any backend that supplies those seams.

## Data Flow

### Placement Store

Tracks which backend serves each lease (bbolt + in-memory cache):
- Written when provisioning starts (after `RouteForProvision` picks a backend)
- Read on every tenant API call (connection, logs, diagnostics) to route to the correct backend
- Deleted only by the reconciler (`cleanupOrphanedPlacements`, ENG-333); survives lease close so a later restore can route to the source node. Deleted eagerly only when a never-provisioned `PENDING` lease is rejected (no retention to protect)
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
| `fred_api_requests_total` | counter | `method, path, status` | API request count. The `path` label is the matched-route TEMPLATE (e.g. `/v1/leases/{lease_uuid}/status`), with a single `unmatched` bucket for requests matching no route — bounding `path` to the finite set of registered routes + 1 (closes an unauthenticated path-scan cardinality vector, ENG-448/F28) |
| `fred_api_request_duration_seconds` | histogram | `method, path, status` | Request latency |
| `fred_api_rate_limit_rejections_total` | counter | `limiter` | Rate limit rejections. `limiter="global"` = the single per-IP limiter shared across all routes (no route/path dimension); `limiter="tenant"` = per-tenant limiter |
| `fred_api_non_in_flight_callbacks_total` | counter | `backend, status` | Callbacks for leases not tracked in-flight (restart/update completions, late delivery, intentional deprovision) |

**Provisioner:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_provisioner_in_flight_provisions` | gauge | — | Current in-flight provisions |
| `fred_provisioner_provisioning_total` | counter | `outcome, backend, operation` | Provisioning operations by outcome/backend. `operation` ∈ `provision`/`restore` separates fresh provisions from restores (ENG-358) |
| `fred_provisioner_provisioning_duration_seconds` | histogram | `backend, operation` | Provisioning latency. `operation` ∈ `provision`/`restore` |
| `fred_provisioner_callback_timeouts_total` | counter | — | Backend callback timeouts |
| `fred_provisioner_ack_batch_fee_gas_errors_total` | counter | `lane` | Ack-batch failures classified as insufficient-fee or out-of-gas — sustained non-zero indicates `gas_limit`/`max_gas_limit`/fee misconfiguration |
| `fred_provisioner_ack_batch_individual_fallbacks_total` | counter | `lane` | Ack-batch failures that fell back to per-lease retries |
| `fred_provisioner_reconciler_inflight_skips_total` | counter | — | Ready leases the reconciler skipped because the main flow owns them |
| `fred_provisioner_reconciler_panics_total` | counter | `stage` | Panics recovered in reconciler goroutines (`process_lease`, `process_orphan`, `fetch_provisions`, `fetch_retentions`) — any non-zero is a latent bug |

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
| `fred_backend_allocated_cpu_ratio` | gauge | `backend` | Allocated-CPU ratio observed by the router at provision time (allocated/total). Per-backend router-decision signal, event-sampled on multi-candidate routing; not intended for cross-backend aggregation — use the backends' own `/stats` component gauges for fleet views (ENG-318) |
| `fred_backend_routing_fallback_total` | counter | — | Provision-routing decisions that fell back to round-robin (no usable backend load stats) |

**Chain:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_chain_transactions_total` | counter | `type, outcome` | Chain transactions (`acknowledge`, `reject`, `withdraw`, `close`) |
| `fred_chain_query_duration_seconds` | histogram | `query` | Chain query latency |
| `fred_chain_signer_oog_retries_total` | counter | `result` | Out-of-gas retry decisions at the broadcast layer (`retried`, `exhausted`) |
| `fred_chain_gas_simulation_total` | counter | `result` | Per-tx gas-simulation outcomes: `simulated` (Simulate succeeded), `fallback` (Simulate unavailable → used the `gas_limit` ceiling), `refused` (simulated estimate exceeded `max_gas_limit`, rejected before broadcast) (ENG-431) |
| `fred_chain_gas_simulated` | histogram | — | Declared gas magnitude per broadcast (`gas_adjustment` × simulated `GasUsed`, or the fallback ceiling) (ENG-431) |

**Withdraw:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_withdraw_incomplete_cycles_total` | counter | — | Provider-wide withdrawal cycles that hit the iteration bound (`max_withdraw_iterations`) with the pagination cursor still non-empty — the provider was not fully drained in that cycle (deferred to the next cycle, not fund loss) (ENG-475) |
| `fred_withdraw_skipped_by_guard_total` | counter | — | Scheduler wakes that skipped the paid withdrawal because the withdraw-cadence guard had not elapsed since the last full drain; only increments when `guard_active`=1 (ENG-524) |
| `fred_withdraw_guard_active` | gauge | — | 1 when the withdraw-cadence guard is active (`credit_check_interval < withdraw_interval`), else 0 (ENG-524) |

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
| `fred_docker_backend_restore_duration_seconds` | histogram | — | Restore re-deploy worker duration (success only); measures the async re-deploy and excludes the synchronous adopt prelude (tracked separately under `replace_phase_duration_seconds{phase=adopt}`). Buckets mirror `provision_duration_seconds` for an indicative restore-vs-fresh-provision overlay (provision is success+failure, restore success-only) |
| `fred_docker_backend_restore_total` | counter | `outcome` | Restore re-deploy worker attempts by `outcome` ∈ `success`/`failure`. Unlike the success-only `restore_duration_seconds`, it also counts the failure path (`rollbackRestoreAdoption`, panics included), so a docker-backend restore success rate is computable. Worker-scoped like `restore_duration_seconds` and `provisions_total`: a restore that fails in the synchronous adopt prelude (claim/rename/route/ack) before the worker spawns surfaces as the synchronous `Restore()` error and is counted by neither outcome here |
| `fred_docker_backend_replace_phase_duration_seconds` | histogram | `operation, phase` | Per-phase duration of the shared replace machinery. `operation` ∈ `restart`/`update`/`restore`; `phase` ∈ `adopt` (restore-only volume rename), `image_setup`, `volume_setup` (incl. VOLUME-subdir chown), `compose_up`, `verify_startup` |
| `fred_docker_backend_resource_cpu_allocated_ratio` | gauge | — | Allocated/total CPU |
| `fred_docker_backend_resource_memory_allocated_ratio` | gauge | — | Allocated/total memory |
| `fred_docker_backend_resource_disk_allocated_ratio` | gauge | — | Allocated/total disk |
| `fred_docker_backend_restore_demote_refused_total` | counter | `backend, reason` | Restores refused by the demote fit-gate (`checkDemoteFit`) because the retained data does not fit the requested smaller SKU tier. `reason` ∈ `measured_exceeds`, `unmeasurable_read_error`, `unmeasurable_backend`, `ephemeral_tier`. Synchronous-prelude refusals — NOT counted by `restore_total` (worker-scoped); surfaced to the tenant as HTTP 422 — the `demote_exceeds_tier` string discriminator rides only the backend→fred hop (ENG-438) |
| `fred_docker_backend_volume_quota_backfill_total` | counter | `outcome` | Startup quota-backfill per-volume re-application (re-tag + re-limit) attempts, `outcome` ∈ `applied`/`failed`; re-applies `disk_mb` enforcement to volumes provisioned before the daemon held the quota capability, without a re-provision (ENG-454) |
| `fred_docker_backend_volume_quota_clear_failed_total` | counter | — | XFS project-quota clear failures on volume Destroy; a rising rate means the project-quota table is regrowing (leaked zero-byte entry) and needs one-time manual operator cleanup (ENG-459) |

**Retention:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_retained_volume_bytes` | gauge | — | Reserved disk capacity (SKU quota) pinned by retained/soft-deleted volumes, in bytes |
| `fred_docker_backend_retained_leases` | gauge | — | Number of active retained (soft-deleted) leases |
| `fred_docker_backend_retention_refused_total` | counter | — | Close-time refuse-to-retain events due to the max_retained_disk_mb cap |
| `fred_docker_backend_retention_evicted_total` | counter | — | Close-time per-tenant cap evictions (max_retained_leases_per_tenant); a tenant's oldest retained lease evicted from the active set (marked reaping) to make room |
| `fred_docker_backend_retention_partition_collapsed_total` | counter | `reason` | Aggregator partition declarations collapsed to the default (whole-tenant) bucket at close, by reason (counted per close attempt — retries re-count). `reason` ∈ `no_input`, `divergent`, `invalid`, `over_limit`, `store_error`. A collapse never blocks a close and never destroys data — the record simply files in the tenant's default bucket |
| `fred_docker_backend_retention_partition_stamped_total` | counter | — | Close attempts that resolved a non-empty retention partition; the adoption/typo detector — a source configured and a tenant allowlisted but this flat at 0 means the configured key never matches what the integrator emits |
| `fred_docker_backend_retention_partition_evicted_total` | counter | — | Close-time per-partition (L2) sub-cap evictions — an aggregator's own noisy-customer containment, NOT a provider capacity signal. `..._retention_evicted_total` keeps its per-tenant (L1) meaning; the two are never conflated |
| `fred_docker_backend_retention_partitions` | gauge | — | Distinct non-empty (tenant, partition) buckets across active+restoring retained records. May legitimately exceed the sum of budgeted `max_partitions` while an over-limit shrink drains (labels age out with their records) |
| `fred_docker_backend_retention_refused_by_scope_total` | counter | `scope` | Close-time refuse-to-retain events by the tripped cap scope. `scope` ∈ `global` (L0 `max_retained_disk_mb`), `tenant` (L1 per-tenant aggregate disk), `partition` (L2 per-partition disk sub-cap). The bare `..._retention_refused_total` keeps its deployed L0-global-only meaning, so this is the scoped superset |
| `fred_docker_backend_retention_cap_check_failed_total` | counter | `check` | Retention cap checks that failed OPEN on a store read error (fail-open is data-safe — never destroy on uncertainty — but a sustained rate means the quota gates are silently off). `check` ∈ `evict`, `breach`, `bound`, `refuse_get` |
| `fred_docker_backend_disk_pool_bytes` | gauge | — | Total disk admission pool (total_disk_mb) in bytes |
| `fred_docker_backend_retained_disk_cap_bytes` | gauge | — | Per-provider retained-volume cap (max_retained_disk_mb) in bytes; 0 when unset |
| `fred_docker_backend_retention_reaping_bytes` | gauge | — | Reserved disk footprint of reaping (pending-destroy) retained records, in bytes |
| `fred_docker_backend_retention_reaping_leases` | gauge | — | Number of retained records stuck in the reaping (pending-destroy) state |
| `fred_docker_backend_retention_leaked_total` | counter | — | Retained-volume leak events (failed destroy / give-up / uncommitted revert) — see ENG-376 |
| `fred_docker_backend_retention_orphans_pruned_total` | counter | — | Total retention records pruned due to confirmed-absent backing volumes |
| `fred_docker_backend_retention_orphan_skips_total` | counter | `reason` | Orphan-reconcile skips by reason (sweep-level bailouts + per-record raced prune attempts). `reason` ∈ `list_error`, `root_unverifiable`, `raced`, `disabled`, `store_error` |
| `fred_docker_backend_retention_writable_path_reclaimed_total` | counter | — | Total writable-path-only volumes destroyed (reclaimed) at close instead of retained |
| `fred_docker_backend_retention_index_reindex_total` | counter | `trigger` | Count of retention in-memory index (re)builds, by trigger (`open`\|`manual`) |
| `fred_docker_backend_restore_finalizer_pending_total` | counter | — | Restore-finalizer kept-pending events: a successful restore whose active-release write failed, so the retention record stays `restoring` to keep protecting the adopted volume; reconcile re-increments each sweep, so a sustained rate means the release store is failing (mirrors `retention_leaked_total`'s observable-backstop role, ENG-523) |

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
| `fred_docker_backend_lease_worker_panics_total` | counter | `worker_type` | Panics in lease worker goroutines (provision/replace/diag) — any non-zero is a latent bug |

#### k3s backend (`/metrics` on the k3s-backend HTTP server)

The k3s backend is an experimental, non-functional scaffold (ENG-133), but its binary still serves `GET /metrics` (default `:9002`) and increments three counters under `fred_k3s_backend_*`, mirroring the docker-backend names.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_k3s_backend_provisions_total` | counter | `outcome` | Provision requests received by the k3s backend (`outcome` ∈ `accepted`/`rejected`) |
| `fred_k3s_backend_callback_delivery_total` | counter | `outcome` | Callback delivery outcomes, one per overall delivery after retries (`outcome` ∈ `success`/`failure`) |
| `fred_k3s_backend_callback_store_errors_total` | counter | — | bbolt errors persisting pending callbacks in the k3s backend |

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
