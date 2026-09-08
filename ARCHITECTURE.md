# Fred Architecture

This document describes the architectural decisions and design patterns used in fred, the Manifest Provider Daemon.

## Overview

Fred is a **lease lifecycle manager** that bridges Manifest Network's on-chain billing system with pluggable backend provisioning systems. It uses an **event-driven architecture** with **level-triggered reconciliation** for crash recovery.

## Design Principles

1. **Chain Authority is Positive**: A supported, identity-matching chain state
   authorizes lease-lifecycle decisions; a nil, failed, or unknown read does not
   authorize cleanup
2. **Backend Evidence is Positive**: Physical observations originate at the
   backends; membership can confirm an effect, while silence or absence alone
   cannot authorize replacement or destruction
3. **Effects Need Durable Causality**: Attempts, journals, receipts, placement,
   and callback outboxes preserve what may have crossed a side-effect boundary
4. **Eventual Consistency**: Events trigger actions; reconciliation rejoins
   current observations with those durable facts to repair drift
5. **Fail-Safe**: Unavailable, stale, contradictory, or incomplete authority
   defers or quarantines work rather than guessing
6. **Idempotent Operations**: Retry the same logical operation with its original
   durable identity; a newly generated identity represents new work and may
   conflict

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
Edge-triggered chain recovery (what we DON'T do):
  "What chain events did I miss?" → Requires durable chain-event queue/replay

Level-triggered (what we DO):
  "What is the current state?" → Query chain + backends, compare, act
```

**Benefits:**
- No need for durable chain-event storage; accepted backend effects still use
  durable attempts, mutation journals, receipts, and callback outboxes
- Crash recovery recomputes decisions from current positive evidence joined
  with durable causal facts instead of replaying chain events
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
│  │  events.lease.event         →  (fan-out to WebSocket subscribers)   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Backend callback HTTP ──verified/synchronous──> CallbackService           │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Provision Manager                                │   │
│  │                                                                     │   │
│  │  Composition root that wires together:                              │   │
│  │                                                                     │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │   │
│  │  │    Placement     │  │RuntimeController │  │   AckBatcher     │  │   │
│  │  │    execution     │  │  Observe/drain   │  │  N parallel      │  │   │
│  │  │  Store + Registry│  │  only; no claim  │  │  lanes via authz │  │   │
│  │  │  + one router    │  │  or settlement   │  │  sub-signers     │  │   │
│  │  │  bound once      │  │  authority       │  │  (round-robin)   │  │   │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │   │
│  │                                                                     │   │
│  │  Purpose applications own complete provision, restore, callback,   │   │
│  │  timeout, maintenance, and reconciliation lifecycle sequences.      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Reconciler (independent component)                                 │   │
│  │  Evidence join: chain + backend inventory + durable placement       │   │
│  │  Typed sweep binds inventory/store/operation causal boundaries      │   │
│  │  Exact re-read mints action capability; action derives its target   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Dependency Injection

Components consume narrow interfaces where substitutability helps; the
composition root deliberately owns concrete single-authority registries and
routers. This keeps tests isolated without abstracting types that have only one
valid implementation.

```
Manager (coordinator)
├── ChainClient            interface → chain.Client (backed by SignerPool)
│   └── SignerPool         primary + N sub-signers for authz parallel signing
├── placement.Store        concrete  → required durable placement authority
├── operation.Registry     concrete  → created and owned privately by placement.Store
├── OperationCoordinator   concrete  → inseparable Store + private Registry aggregate
├── ExecutionCoordinator   concrete  → atomically bound aggregate + router + callback factory + provider control plane
├── purpose coordinators   concrete  → complete lifecycle application workflows
├── ProvisionOrchestrator  struct    → thin HandlerEventCoordinator factory
├── HandlerSet             struct    → Watermill/callback transport adapters
├── RuntimeController      value     → Manager's observe/drain-only Registry facet
├── AckBatcher             struct    → N parallel ackLane workers, round-robin dispatch
├── TimeoutChecker         struct    → uses a purpose-bound TimeoutCoordinator
└── PayloadStore           struct    → bbolt-backed (optional)

Reconciler (independent)
├── ReconcilerChainClient       interface → chain.Client
├── ProviderControlPlane        interface → one Manager-owned chain adapter + its jointly constructed AckBatcher
├── ReconcilerPayloads          interface → Manager (payload reads only)
└── ReconciliationCoordinator   concrete  → exact Store/Registry/router/inventory aggregate
    ├── ReconciliationSweep          → Store fence + Registry boundary + inventory session
    ├── ProjectedReconciliationSweep → one projected snapshot; mints action capabilities
    └── Observed*Action              → exact re-read + lease claim + derived backend target

API Handlers
├── PlacementLookup          interface → placement.Store (read-only, optional)
├── BackendRouter            *backend.Router (read requests only)
├── maintenance.Service      application adapter → MaintenanceApplication
└── restore.Service          application adapter → RestoreCoordinator
```

Key boundaries are defined where they are consumed:

| Boundary | Defined in | Used by |
|-----------|-----------|---------|
| `ChainClient` | `provisioner/topics.go` | Manager, HandlerSet, AckBatcher |
| `ReconcilerChainClient` | `provisioner/reconciler.go` | Reconciler |
| private `backendRouter` | `provisioner/interfaces.go` | Trusted composition into `placement.ExecutionCoordinator`; never exposed as application mutation authority |
| `operation.Registry` and `operation.RuntimeController` | `provisioner/operation/registry.go`, `runtime_controller.go` | The placement aggregate consumes settlement authority once; Manager retains only status/drain operations |
| `placement.OperationCoordinator` / `ExecutionCoordinator` | `provisioner/placement/operation_coordinator.go`, `backend_execution.go`, `provider_control_plane.go` | Composition root; atomically binds one Store-owned Registry, callback factory, backend runtime, and provider control plane. A failed binding publishes no partial execution authority and can be retried |
| `placement.ProvisionCoordinator`, `RestoreCoordinator`, `AuthenticatedCallbackCoordinator`, `TimeoutCoordinator`, `MaintenanceApplication` | `provisioner/placement/` | Purpose-specific application boundaries; each owns its full claim/admit/dispatch/settle sequence |
| `placement.ReconciliationSweep`, `ProjectedReconciliationSweep`, and observed action capabilities | `provisioner/placement/reconciliation_sweep.go` | Reconciler only; prevent cross-sweep or caller-selected target splicing |
| `CallbackApplication` | `provisioner/callback_service.go` | HandlerSet transport adapter |
| `maintenance.Service` / `restore.Service` | `provisioner/maintenance/service.go`, `provisioner/restore/service.go` | HTTP result adapters retaining only their high-level application capability |
| `PlacementLookup` | `api/handlers.go` | API read handlers |
| `LeaseRejecter` | `provisioner/interfaces.go` | TimeoutChecker |
| `Acknowledger` | `provisioner/ack_batcher.go` | HandlerSet, Reconciler (lease acknowledgement via parallel lanes) |
| `CallbackPublisher` | `api/server.go` | API callback handler |
| `StatusChecker` | `api/server.go` | API status handler |

## State Reference

Two state machines run side-by-side: the chain owns the lease's billing state, and Fred (with the backend) owns the provision's runtime state. Both shape API responses and reconciliation; Fred exports provision-state metrics, while chain state remains an input rather than a Fred-owned metric.

### Chain Lease State

Authoritative on-chain field. The chain emits events when this changes; Fred reacts.

| State | Description | Source |
|---|---|---|
| `PENDING` | Lease created on-chain. Awaiting provisioning (and a payload upload if `meta_hash` is set). | `LEASE_STATE_PENDING` |
| `ACTIVE` | Provisioned and acknowledged. Tenant can use the resource and is being billed. | `LEASE_STATE_ACTIVE` |
| `CLOSED` | Tenant-closed or auto-closed (credit depletion, expiry). Resources should be deprovisioned. | `LEASE_STATE_CLOSED` |
| `REJECTED` | Provisioning was definitively rejected. Resources and any backend residue should be cleaned up. | `LEASE_STATE_REJECTED` |
| `EXPIRED` | Lease's time bound elapsed. Same handling as CLOSED. | `LEASE_STATE_EXPIRED` |
| `UNSPECIFIED` or an unrecognized future value | Not actionable evidence. Reconciliation fails closed: it preserves backend state and retries a later authoritative read. | `LEASE_STATE_UNSPECIFIED` or unknown enum value |

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
   a. Convert the event's lease/tenant identity into an opaque application request
   b. `ProvisionCoordinator.ExecuteCurrentLease` takes the lifecycle claim and
      performs a bounded authoritative chain re-read and payload validation
   c. Derive the backend from its construction-bound runtime (least-loaded
      matching backend if several match)
   d. Mint the typed operation/callback pair and durably record the exact attempt
   e. Call backend POST /provision inside the bound execution coordinator
   f. Confirm the placement on acceptance, clear only the attempt on a typed
      refusal, or retain the attempt when the outcome is ambiguous; the handler
      receives only a closed, observation-only result
6. Backend provisions resource asynchronously
7. Backend calls POST /callbacks/provision (API server):
   a. Verify HMAC signature (CallbackAuthenticator)
   b. Parse exactly one authenticated `operation_id` or `lifecycle_id` query
      capability and overwrite any body-supplied identity
   c. Admit the callback into Manager's synchronous typed application path
8. AuthenticatedCallbackCoordinator:
   a. Match and claim an exact operation capability, or authorize a later
      observation against the durable current lease/backend lifecycle capability
   b. If success: acknowledge lease on chain via AckBatcher
   c. If failed + PENDING: reject lease on chain, then finish the claim
   d. If failed + ACTIVE: finish and defer recovery to reconciliation
   e. Return a closed consequence result to CallbackService, which publishes the
      callback-derived status before returning 200. Retryable
      application failure or the dedicated deadline returns 503, so the
      backend retains this lease's durable FIFO head
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
   b. Submit an opaque payload-provision request to the same
      `ProvisionCoordinator.ExecuteCurrentLease` application boundary
   c. The coordinator re-reads and validates the lease, payload, route, durable
      attempt, dispatch, and settlement exactly as above
```

### Lease Closure

```
1. Tenant closes lease (or credit depleted)
2. Chain emits lease_closed event
3. HandleLeaseClosed:
   a. Clean up stored payload (if any)
   b. Submit only the lease UUID to the construction-bound deprovision application
   c. The coordinator derives every positive backend candidate from durable
      confirmed, attempted, or conflicting placement and process-local work,
      acquires the exact claims, and calls POST /deprovision for each candidate.
      If ownership is unresolved it sweeps every configured backend; a missing
      named candidate or any failed call preserves retry authority
   d. Backend cleans up resources (idempotent). With `retain_on_close`, the
      backend soft-deletes volumes instead of destroying them and reports
      `retained: true` on its deprovisioned callback.
```

The lease's **confirmed placement survives close** (only the payload is cleaned up). This is deliberate: it keeps the closed lease's backend resolvable so a later restore can be routed to the node that holds its retained data (restore affinity, ENG-333). A failed re-provision of an `ACTIVE` lease clears only its matching attempt and preserves its confirmed backend. After a `PENDING` operation is successfully rejected on chain, Fred conditionally deletes the entire placement record only when every backend named by the current record belongs to that failed operation; an unusable, foreign-owned, or concurrently revised record is preserved for reconciliation or operator repair.

### Lease Restore

Restore adopts a soft-deleted lease's retained data into a new lease (see the [docker backend restore flow](internal/backend/docker/README.md#soft-delete--restore)):

```
1. Tenant opens a fresh PENDING lease matching the closed lease's shape
2. Tenant POSTs /v1/leases/{new}/restore with from_lease_uuid = {closed}
3. `restore.Service` submits only authenticated source/target/tenant identity to
   `RestoreCoordinator.ExecuteApplication`:
   a. The coordinator verifies source ownership before acquiring ordered source
      and target lifecycle claims, then re-reads the target as a
      tenant/provider-owned PENDING lease
   b. It initiates a typed restore operation, atomically reserves the source's
      exact confirmed placement, and durably writes the absent target's attempt
      on that same backend (restore is same-backend, ENG-333)
   c. It derives the exact backend and HMAC-covered callback pair internally
   d. It calls backend POST /restore and owns settlement: exact positive evidence
      confirms, a transport-trusted contract refusal clears, and every ambiguous
      outcome retains the attempt. No claim, route, or terminal choice returns to
      the service
4. Backend adopts the retained volumes and re-deploys the retained manifest async
5. Backend POSTs the success callback:
   a. Because the lease was tracked in-flight (step 3b), the callback is
      acknowledged INLINE on-chain (ENG-358) — no ~reconciler-interval wait
```

The new lease **may target a different SKU tier** than the source (promote/demote, ENG-438). Only the item *shape* must match (service names + quantities); the SKU/disk tier may differ. A **promote** (same-or-larger disk tier) is admitted only when the aggregate growth above the retained footprint fits disk capacity, and the new `disk_mb` cap is applied. A **demote** (smaller disk tier) is allowed only if the retained volume's *measured* data fits the new tier's `disk_mb` cap — the backend runs `checkDemoteFit` before adopting. A refused demote returns HTTP 422; on the backend→fred hop the body carries `code=demote_exceeds_tier` (`ErrDemoteDataExceedsTier`), which the fred-api boundary forwards to the tenant as a 422 (whose own `code` field is the numeric status) — distinct from a *bare* 422 (`ErrNotRetained`, no retained data), which fred maps to 404. If a restore fails after changing a physical quota, rollback first proves every volume still fits and reapplies the immutable source cap; any uncertainty leaves the source `restoring` and its destination allocation counted.

The `restoring` row is also the destination's durable ownership finalizer. While
it exists, the Docker backend rejects a new Provision or Restore generation.
Before commit, every maintenance path remains fenced. Once an exact active
Release proves commit and no Pending or contradictory Failed restore operation
remains (Succeeded history may later be atomically retired by an authorized
successor), a plain,
identity-preserving Restart may recover the Failed destination. Update and
custom-domain redeploys remain fenced until that Restart reaches Ready and
finalizer reconciliation consumes the row. A failure before actor
acceptance tears down/re-quarantines, restores source quota, retires only an
uncommitted destination generation, records the exact Failed outcome and
callback atomically, then
CAS-hands the source back. After actor acceptance, physical rollback deliberately
stops short of source handback: the actor must first durably record the Failed
operation outcome and enqueue its callback, after which the level-triggered
retention sweep performs the same make-before-break handback from that terminal
row. Absence is invalid authority and fails closed. This ordering prevents
volatile actor loss or successful callback delivery from erasing the settlement
decision. An exact active destination
Release changes the verdict: it is the durable restore commit marker, so rollback
must not delete it or return the data to the source. Recovery first transitions
any matching Pending operation to Succeeded and—with zero surviving containers—
reconstructs a conservative Failed destination plus its exact allocation. It
retains the source row as durable tenant/provider identity across repeated
restarts until a successful plain Restart reaches Ready and reconciliation
consumes it, or close transfers that authority into its own complete intent.
An exact Succeeded operation outcome is equivalent irreversible evidence when
the earlier Release append did not complete: together with the source
finalizer's immutable destination snapshot it reconstructs the missing active
Release. Conversely, a Failed outcome beside an exact committed Release is
contradictory authority and fails closed.
The missing cohort is a post-commit runtime failure.

Reconciler interplay (level-triggered backstop):

- **Inline ack, reconciler backstop.** Inline acknowledgement (ENG-358) is the fast path. Restore requires both durable placement authority and the typed operation registry; production composition cannot construct the service without either. Once accepted, the reconciler remains the level-triggered backstop for a lost callback and acks a `PENDING` + `ready` lease. It *skips* a lease the operation registry still owns (counted by `reconciler_inflight_skips_total`), avoiding a double-ack.
- **Restore affinity sync (ENG-333).** Each reconcile tick fans out `GET /retentions` to every backend and syncs the `lease → backend` map into the placement store, so retained leases stay routable to their source node across restarts.
  A trusted retained positive from the already confirmed owner preserves that
  exact affinity during an unrelated backend outage. Partial evidence cannot
  establish a new owner or clear an attempt/conflict; a contradictory or rejected
  reporter remains quarantined. Successful chain reads with no lease are a
  separate `chain_unknown` observation: cleanup is withheld, but this is not a
  transient chain failure that invalidates an otherwise complete sweep.
- **Write-ahead placement (ENG-632).** Placement stores two independent facts: `Backend` is confirmed ownership and `Attempt` is an unresolved outbound call. Every provision, re-provision, and restore persists `Attempt` before contacting a backend. The attempt binds a typed operation kind, operation UUID, exact operation/lifecycle callback destinations, and either the provision payload fingerprint or restore source UUID; incomplete or malformed combinations decode unusably. A failed prewrite makes zero backend calls; a transport/5xx ambiguity—including an unvalidated HTTP 503—retains the exact typed attempt and blocks substitution. On a later sweep Fred reconstructs that same operation ID and request and redelivers it only to the pinned attempted backend. The persisted callback pair is reused even if `callback_base_url` changed, and an updated ACTIVE payload is bound by its attempt fingerprint rather than the immutable create-time `MetaHash`. Package-owned `backend.Invoke*` choke points recognize only the exact identity-bound HTTP transport type; decorators and custom backends cannot acquire causal authority through interface or method embedding. That transport returns a zero-invalid outcome (`accepted`, typed `refused`, `not dispatched`, or `ambiguous`) minted at the exact response/control-flow branch. Placement never derives settlement or chain-rejection authority with `errors.Is` over an arbitrary backend error tree, and restore never derives a tenant-facing permanent state verdict from one. The compatibility `backend.Backend` adapter can express success, but maps every non-nil return to ambiguity. Thus only the bundled identity-bound HTTP client can supply refusal/no-dispatch evidence; raw errors remain diagnostics. An accepted response promotes the attempt; a transport-minted contract refusal or a positively unsent fresh call clears it; every ambiguous result retains it for another sweep. Recovery is stricter: `not dispatched` preserves the existing generation for exact redelivery, and only a typed refusal clears it. Missing payload bytes are a retriable loss of local authority, never permission to send a payloadless request or terminate a live lease. If the target is positively terminal, Fred instead claims the exact attempt, deprovisions every distinct attempted/confirmed candidate, and promotes conservative closed-lease affinity only after every call succeeds; any unavailable backend or ambiguous result retains the attempt. Inventory silence never clears it because a delayed remote call can commit after an absence response; the remaining settlement paths are the exact authenticated callback, a positive report from the attempted backend carrying the exact paired typed lifecycle generation, or explicit operator repair. The inventory collector seals backend membership, storage identity, lifecycle generation, tenant, and provider as one immutable per-lease row; projection derives lifecycle and maintenance authority from that row and has no parallel caller-supplied identity map. The `insufficient_resources` response code establishes protocol conformance, not cryptographic authorship: Fred HMAC-signs backend-bound requests but backend response bodies are not separately signed. `production_mode: true` therefore requires certificate-verified HTTPS to every backend; plaintext remains development-only. Ordinary proxy HTML, foreign JSON, code-less legacy responses, and unknown codes stay ambiguous. A positive report from another backend is accumulated with every prior owner/attempt into a durable quarantine instead of moving affinity. The first complete `/provisions` + `/retentions` projection atomically persists a baseline bound to the sorted set of immutable backend storage identities. That baseline survives process restarts and transient outages while the topology is unchanged; adding, removing, or renaming an identity invalidates admission until a complete projection safely establishes the new topology. A historical identity may rejoin only as the same storage identity; replacement storage receives a new name. During a later partial sweep, a genuinely recordless `PENDING` lease may target only a backend that answered both inventories, while recordless `ACTIVE` recovery, confirmed work on a silent owner, and ambiguous/conflicting work remain deferred. A live chain lease positively reported in retention is also deferred lease-locally: ordinary provision cannot overwrite data that requires the restore path. One failed node therefore does not globally pause the reconciler's healthy-node admission. The tenant event path has no per-sweep witness: it is fenced by the durable baseline and immutable topology, live-routes by backend stats, and persists its exact attempt before dispatch. Exact callback URLs carry an HMAC-covered UUIDv4 operation ID, and callback/timeout settlement claims serialize terminal work so an older response cannot settle a replacement operation. Each successful exact operation atomically promotes a separately typed lifecycle ID bound to the authoritative backend; that capability survives provider restart and ordinarily authorizes only status observations, rotates on a newer exact success, and retires on deprovision. Without a matching confirmed placement owner it narrows to teardown-only authority: success/failure is a 200 no-op, maintenance cannot reissue it, and only its exact deprovision observation may atomically retire it and publish retained status. Retirement commits before the best-effort push, so a process crash can lose that event but cannot resurrect authority; queryable retention remains the backstop. Existing v0.13 owners migrate explicitly as tokenless legacy rather than receiving a capability their backend never saw. An authenticated exact success continues to chain acknowledgement even if initial placement confirmation fails, so a timeout cannot reject the now-live lease. A transient placement-store error then returns non-2xx without finishing the operation, retaining the backend's exact outbox evidence; retry completes placement directly or through durable recovery after timeout observes `ACTIVE` and retires only the volatile operation. Likewise, if a failure callback lands its chain rejection before placement refusal fails, retry observes the terminal lease and consumes the exact attempt without issuing another rejection. Permanent semantic contradictions still finish volatile settlement while preserving the write-ahead record for exact paired-generation inventory or operator repair.
  Placement and lifecycle-capability rows carry explicit row schemas and reject
  case aliases, duplicate keys, unknown fields, future schemas, and trailing
  values recursively. Runtime Open and health verification require that exact
  structural boundary, so a future or malformed authority row cannot be
  silently ignored after a downgrade. Only the stopped v0.13 preparation path
  accepts the historical raw/backend-object placement grammar. A structurally
  current placement whose cross-field authority is contradictory remains
  unusable evidence for that lease and requires exact recovery or operator
  repair.
  Rejected inventory payloads do not become ownership authority, but neither may
  their positive membership disappear into apparent absence. Fred persists those
  candidates as `untrusted_positive` quarantine across restart. Only a sole
  candidate observed again on the same backend by a later complete,
  identity-valid projection can self-resolve; every other quarantine requires
  causal operator proof and offline repair.
  The placement Store privately creates a Registry and consumes its one-shot
  settlement authority into one exact `OperationCoordinator`; production never
  receives either raw authority. `BindBackendRuntime` atomically joins that
  aggregate to one backend runtime, the Store-owned callback factory, and one
  broad `ProviderControlPlane`. Validation failure publishes no intermediate
  coordinator, so a backend-only or chain-only object cannot mint an executable
  facet. Manager constructs the concrete chain adapter and its `AckBatcher`
  together, preventing reads/writes from chain A from being paired with
  acknowledgement authority from chain B. Provision, restore, maintenance,
  callback, timeout, deprovision, and reconciliation then receive only narrow
  purpose-specific application facets; callers cannot independently substitute
  a Registry, Store, router, callback origin, chain reader/writer, backend target,
  or terminal verdict. Manager retains an `operation.RuntimeController`, whose
  observe/drain surface cannot mutate work.
  Reconciliation additionally binds each collector session, Store fence, and
  Registry boundary in one `ReconciliationSweep`; only its one-shot projected
  form can mint exact-chain-re-read action capabilities. Redelivery and terminal
  teardown use disjoint claim types, and the terminal claim deliberately exposes
  no refusal transition.
  The raw session, fence, and projector are package-private, so production code
  cannot bypass that joined boundary. Chain inventory is collected first; it
  cannot reveal backend ownership. `BeginSweep` then persists a pending marker
  immediately before backend inventory reads. Each successful endpoint read is
  returned only as an opaque, exact-sweep, one-shot receipt after its positive
  lease observations have installed Store-owned barriers. Provision and
  retention receipts for one backend are consumed together; identity,
  refresh, and cross-endpoint checks decide the closed authoritative/untrusted
  variant inside the sweep. An unmatched receipt can only be rejected as
  untrusted, and outstanding receipts make sealing impossible. A successful
  semantic projection clears the exact marker atomically; an orderly, sealed,
  zero-positive `End` may also clear it. If a
  restart inherits the marker, a lost positive might name a different owner, a
  retained copy, or unusable identity evidence. Partial inventory then remains
  observational but cannot mint fresh provision, restore, maintenance,
  chain-write, prune, or backend-cleanup authority until a complete fleet
  projection resolves the uncertainty. Exact durable callback, attempt, and
  maintenance-command recovery remains available because it replays recorded
  authority rather than inferring new authority from absence.
  Projected actions are also bound to the Store inventory epoch. Beginning a
  newer sweep invalidates an older, not-yet-claimed action; an action that
  already holds its lease claim is captured as in-flight by the newer sweep and
  causally excluded until release. Ordinary concurrency therefore does not
  impose that fleet-wide pause: a fenced trusted
  provision is discharged when the current durable `Backend`/`Attempt` and
  lifecycle generation already represent the same fact. Retention, untrusted
  identity, a novel reporter, or a contradictory generation/principal is not
  interchangeable and keeps the marker recovery-required.
- **Placement prune grace + deprovision fail-safe (ENG-335).** The reconciler will not prune a placement set younger than a grace window (`2 × reconcile_interval`, measured from sweep start), so a lease that provisioned during a slow sweep is not mis-pruned. When a lease exhausts its re-provision attempts and is closed, the reconciler eagerly calls `backend.Deprovision` on its backend instead of waiting for the next orphan-cleanup cycle (logged at WARN, non-fatal).
- **Destroy only on a positive fact (ENG-654).** The passes that delete durable state never infer "finished" from absence. A lease missing from the sweep's `chainLeases` proves nothing on its own: that map is built from two non-atomic queries filtered to `PENDING`/`ACTIVE`, so absence covers terminal, never-known, and just-created alike. Orphan deprovision and orphaned-payload cleanup therefore re-read the lease (`GetLease`) per candidate and act only on a positively reported `CLOSED`/`REJECTED`/`EXPIRED`; a query error, an `UNSPECIFIED` state, or a chain with no record of the lease all keep the state and bump `fred_reconciler_cleanup_skips_total`. Exact callback and timeout settlement, attempt recovery, and restore-source authorization follow the same rule: a typed not-found or nil point-read retains durable evidence and returns a retryable/unavailable result instead of consuming an operation, deprovisioning a candidate, or dispatching restore from backend state alone. A retained restore source must be a positive, identity-matching `CLOSED` chain lease. The last of those is not a corner case — `x/billing` never deletes a lease, so "no record" means a phantom provision, a wrong or reset chain, or a lagging RPC node, and treating it as terminal would let one bad endpoint deprovision the fleet or authorize work from the wrong history. Placement pruning asks a different question, per record rather than per sweep: it prunes only what the record's **own** backend accounted for on both `/provisions` and `/retentions`. Between them these replace the fleet-wide completeness gate ENG-356 left on all three passes, under which one silent machine paused cleanup — and stranded admission capacity — for every healthy one.

The ENG-632 attempt record also binds an immutable tenant, provider, and full ordered backend-item snapshot. Mutable chain fields such as `CustomDomain` authorize current liveness but cannot rewrite an already-dispatched exact request across a restart.

### Retention partitioning (aggregator sub-tenancy)

Retention records carry an optional cooperative `partition` — a sub-tenant grouping key an allowlisted (aggregator) tenant declares for its own end-customers via the operator-configured `retention_partition_source`. Partitions only sub-divide the tenant's own budget: the per-tenant aggregate caps always run over the whole record set (a tenant declaring N partitions has the same total budget as N=1), the `""` default bucket is never sub-capped (so any collapsed/legacy/undeclared record behaves exactly as before partitioning existed), count caps evict oldest-first (a per-partition sub-cap within that partition, the per-tenant aggregate across all the tenant's records), disk caps refuse the incoming close only, and every uncertainty collapses toward "keep". The partition is advisory grouping metadata — never a security boundary (isolation stays keyed on the on-chain tenant) and never load-bearing for restore/reap correctness.

## Concurrency Model

### Goroutine Management

All long-running goroutines are tracked, panic-contained, and cancellable.
Top-level daemon components use `safeGo()`; component-owned child loops use the
same shape locally and are joined by their owner before its stores close.

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
   ├─ Ack batcher lanes start FIRST, before the handlers that call Acknowledge()
   └─ Must be ready before callbacks arrive from backends
3. Launch maintenance recovery in the background
   ├─ Durable per-lease claims were already rehydrated during construction
   └─ Its immediate retry must not let one unavailable backend delay fleet startup
4. Perform initial withdrawal
5. Perform startup reconciliation
   └─ May provision leases, triggering backend callbacks
6. Start remaining components in parallel:
   - Event subscriber
   - Event bridge
   - Lease watcher
   - Withdrawal scheduler
   - Periodic reconciler
```

**Why this order matters:** Startup reconciliation detects unprovisioned leases and sends provision requests to backends. Backends respond with callbacks to Fred's API. If the API server isn't listening yet, callbacks fail with "connection refused". If the provision manager has not started, its callback admission gate is closed and its acknowledgment lanes are unavailable, so it deliberately returns 503 and the backend retains the durable delivery.

Startup reconciliation cannot hold the remainder of startup indefinitely on a
stalled chain node. The complete paginated `PENDING` and `ACTIVE` inventories
run concurrently with independent 30-second contexts. If either fails, the
sweep stays incomplete, startup continues, and the periodic reconciler retries
both from a fresh snapshot.

The placement pruner and callback-timeout checker are independently bounded
background domains. Placement cleanup has one 10-second aggregate pass budget,
bounded workers, stable ordering, and a rotating cursor; forty stalled exact
reads therefore cost one budget rather than forty per-record timeouts, and later
records receive the first worker slots on the next pass. Timeout settlement uses
the same aggregate-budget/worker/cursor shape. Each candidate joins the exact
Registry and durable placement generation before its chain call, preserves that
generation on cancellation, error, or panic, and cannot settle a replacement.
`Manager.Close` cancels and joins the timeout loop before closing its dependent
runtime authorities.

Docker-backend startup is independently bounded. The convenience constructor
allows 30 seconds for initial substrate/storage-identity attestation;
`NewWithContext` adds no fallback, so its callers must supply a finite
construction deadline explicitly. `Start` uses the
shorter of its caller context and a 30-second bound for the initial Docker
identity/connectivity reads, then a finite backend-lifecycle aggregate budget
for crash convergence. The production default is the saturating sum of every
sequential phase's local maximum (51m10s with default settings), rather than a
single phase's limit. Inside it, interrupted-volume recovery and its
clean-inventory proof each receive a fixed two-minute filesystem-only child
deadline; container stop grace cannot inflate them. Restore preflight, retention
reconciliation, quota reconciliation, orphan collection, and retention reap
each receive one aggregate budget of `max(2m, container_stop_timeout)`. State
rebuild retains its 30-minute cap; operation recovery receives the larger of the
ordinary phase budget and its configured provision/read/cleanup sum; the final
identity proof receives one Docker read budget. Each recovery list/inspect
Docker call receives its own 30-second child budget, while the complete
cold-start diagnostic scan and complete orphan-network scan each share one
separate 30-second aggregate budget. Per-call recovery bounds can therefore
accumulate with fleet size, but the finite `Start` parent remains the hard
aggregate ceiling. Its sum reserves a fresh operation-recovery window even when
every preceding phase consumes its cap; one wedged daemon call cannot block
indefinitely.

The separate Docker storage-identity preflight and initializer share one
`-storage-identity-operation-timeout` deadline (default `10m`) across
context-aware Docker and filesystem-control-plane probes. It is cooperative,
not a hard wall-clock interrupt for blocking local open, bbolt, or fsync work.
Deadline expiry makes the command unsuccessful but is not proof of rollback:
initialization may retain its crash-resumable pending state or finish marker
publication before a later cancellation check. Operators keep the lineage
stopped and rerun the same mode against unchanged input.

**Ack batcher ordering:** `Manager.Start` launches the batcher's lanes before opening synchronous callback admission and before `wmRouter.Run` starts chain/payload handlers. The reconciler is the other caller of `Acknowledge()`; its first ack happens in step 4, after the `Running()` gate in step 2. The batcher runs on the manager's own lifecycle context (`m.stopCtx`, rooted at `context.Background()`), *not* on the ctx passed to `Start`. That keeps the lanes' lifetime exactly what it was before ENG-723, when `NewManager` started them on a bare `context.Background()`: `Close()` is what ends them. Direct callbacks have a second manager-owned cancellation context; `Close()` closes admission, cancels admitted application, waits for it to drain, and only then stops the batcher. Constructing a `Manager` starts no goroutines.

### State Protection

- **In-flight map**: Protected by `sync.RWMutex`
- **Reconciler flag**: Uses `atomic.Bool` to prevent concurrent reconciliation
- **Event subscriber channels**: Lock-free via atomic closed flag + WaitGroup

### Graceful Shutdown

```
1. Receive SIGINT/SIGTERM
2. Close ordinary lifecycle admission, then wait for operations and held
   lease-action claims to drain (with timeout)
   └─ API stays running; authenticated callbacks may still settle durable work
      and any callback admitted after a zero observation is drained by HTTP and
      manager shutdown
3. Stop API server (stop accepting new requests)
4. Close event broker (send clean close frames to WebSocket clients)
5. Cancel context (signals all components)
6. Stop withdrawal scheduler (wait for in-flight tx)
7. Close event subscriber
8. Wait for all goroutines (with timeout)
9. Close provision manager (close callback admission, cancel/drain admitted
   application, then clean up Watermill + stores)
10. If timed out: additional 2s grace period for lingering components
```

That sequence and the configurable `shutdown_timeout` belong to `providerd`.
Docker-backend has a separate fail-closed boundary. A terminal runtime storage-
authority withdrawal publishes its first cause to the main loop, which closes
the listener, allows up to 30 seconds for HTTP shutdown, cancels backend work,
and waits up to 90 seconds for backend-owned goroutines. It exits status 1 even
when that drain succeeds so a supervisor must launch a fresh `Start` to recover
the retained evidence. A drain timeout also exits 1 and leaves the Docker client
and durable stores open until process death so a still-running worker cannot use
closed dependencies. A failure during `Start` happens before listener bind and
exits 1 immediately; there is no running server or worker set to drain.

## Backend Integration

### Router Design

The backend router matches leases to backends by exact SKU UUID. When multiple backends share the same SKU list, Fred routes each new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats. A placement store (bbolt) records which backend serves each lease so that read operations always reach the correct machine.

```go
type Router struct {
    backends       []backendEntry
    backendsByName map[string]Backend
    defaultBackend Backend
    counter        atomic.Uint64  // tie-break and no-stats fallback rotation
}

func (r *Router) Route(sku string) Backend                 // first match (deterministic)
func (r *Router) RouteAll(sku string) []Backend             // all matching backends
func (r *Router) RouteForProvision(ctx, sku, inFlight) Backend // least-loaded across matches
```

**Routing strategies:**
- `Route` — returns the first matching backend (used for deprovision fallback and read-path when no placement exists)
- `RouteForProvision` — routes a new provision to the least-loaded matching backend — the SKU-matching backend reporting the lowest allocated-CPU ratio from its `/stats` endpoint (ENG-318). Ties break by fewest in-flight provisions, then by a round-robin counter; round-robin is also the fallback when no matching backend exposes usable load stats
- **Placement lookup** — stores a confirmed backend plus an optional unresolved attempt, or a durable quarantine containing every known conflicting owner. Only a confirmed backend pins mutating/provision routing. Read-only provision discovery safely queries every configured confirmed, attempted, or conflicting candidate before SKU fan-out; an unresolved all-miss returns 503 rather than a false 404. Attempts and conflicts gate destructive reconciliation and positively target deprovision.

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
- `ErrAlreadyProvisioned` (HTTP 409 from Provision) — exempt from circuit-breaker
  failure counting, but not durable ownership proof because the client cannot
  distinguish a configured-endpoint conflict from an intermediary-generated 409
- `ErrInvalidState` (HTTP 409 from Restart/Update) — wrong lease state for operation
- `ErrInsufficientResources` (HTTP 503 from Provision) — treated as a capacity signal for circuit-breaker health, but not as a settlement verdict because an intermediary may have emitted an unvalidated 503 after backend acceptance. Its `ErrCapacityRefused` subtype requires the declared envelope plus `code="insufficient_resources"`; this is a contract verdict under transport trust, not an authenticated response.

This ensures that expected business conditions don't trip the circuit breaker and block backend operations.

### Lease Actor Model (Docker backend)

The Docker backend uses a **single-writer actor per lease** for its live runtime
projection. A `leasesm.LeaseActor` serializes commands, observations, state
transitions, worker ownership, and terminal handoff for that lease. Durable
operation, maintenance, close, release, and retention state remains in the
corresponding journal; the actor consumes store-issued capabilities rather than
becoming another authority database. The substrate-agnostic state-machine and
actor code lives in `internal/backend/shared/leasesm` (see
[SM is shared across backends](#k3s-backend-experimental)).

**Structure:**

The actor holds **no substrate/backend pointer**. Construction validates every
required interface and fixed handler before the actor is installed in the
registry or its goroutine starts. Commands carry only their journal-issued
authority; they cannot supply a callback, physical function, or terminal status
at dispatch time:

```go
type LeaseActor struct {
    leaseUUID string
    cfg       LeaseActorConfig // immutable interfaces and construction-bound handlers
    inbox     chan leaseMessage
    sm        *leaseSM
    workers   *workbarrier.Barrier
    activity  int64            // includes admission, handling, workers, and handoff
    ...
}
```

Concrete inbound message structs are private. Substrate packages can obtain them
only from operation-specific constructors such as `NewProvisionCommand`,
`NewRestartCommand`, and `NewContainerDiedObservation`; those constructors
validate the complete input and return opaque `ActorCommand`,
`ActorObservation`, or `RecoveryCommand` values. Their private envelopes are
one-shot even when copied. A command therefore cannot be relabelled as an
observation, a recovery correction cannot create a new actor, a stale
observation cannot gain actor-creation authority, and callers cannot assemble a
partly valid message with a struct literal.

Synchronous constructors return an `ActorReply` whose result channel is
receive-only. The private message retains the only sending end, so a caller
cannot acknowledge its own request or splice one command's reply channel into
another. Observations are fire-and-forget and expose no caller-controlled
completion channel. Tests synchronize through observable state or the same
typed quiescence capability used by recovery. Worker terminal/handoff messages
and their success/failure projections
are likewise private or opaque and can be constructed only from the matching
operation or maintenance proof.

**State machine:**

Built on [`qmuntal/stateless`](https://github.com/qmuntal/stateless), actor states
come from `backend.ProvisionStatus`, plus a private `Reserved` control state that
separates resource reservation from worker acceptance. All public provision
statuses are configured. `Retained` is a query-time projection from the durable
retention journal rather than an actor state; `Unknown` is fail-closed and not
part of normal flow.

```
  Reserved ──evProvisionRequested──► Provisioning
           └─evRestoreRequested────► Restarting

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

All states are configured up front with typed trigger payloads and explicit
`Permit`/`Ignore`/`OnEntry`/`OnExit` rules. Only deliberately stale terminal or
observation events are ignored. An invalid caller command is refused instead of
being treated as idempotent success.

**Cancel-on-exit for stale callbacks:**

`Failing`, `Provisioning`, `Restarting`, and `Updating` each own an async
goroutine. On exit, the actor cancels and joins that worker through a bounded
barrier before a conflicting transition may commit. The activity count overlaps
message-to-worker and worker-to-terminal handoff, so a typed quiescence claim
cannot observe a false idle gap. This structurally suppresses stale terminal
callbacks after the lease has moved on.

**Why this shape:**

- **Invalid authority is unconstructable** — exported constructors return
  opaque one-shot commands/observations, receive-only replies, and exact journal
  proofs. Raw callback routes, caller-selected physical handlers, and
  caller-selected terminal statuses do not cross the actor boundary.
- **No fleet-wide lock during slow I/O** — ordinary actor work is linearized by
  the inbox, so unrelated leases do not contend. Recovery deliberately holds one
  exact lease's command and actor-quiescence capability across classification or
  cleanup I/O; that is the exclusion proof which prevents a live worker from
  recreating substrate while recovery tears it down.
- **Deterministic preemption** — a `Deprovision` that arrives mid-provisioning cancels the in-flight work via `OnExit` and transitions cleanly to `Deprovisioning`.
- **Blast-radius-contained panics** — each message is wrapped in `recover()`. The actor survives, other leases are unaffected, and the panicking caller is unblocked via `onPanic`.
- **Observable transitions** — every SM transition is counted in `lease_sm_transitions_total{source,destination,trigger}`.

**The `LeaseProvisionStore` seam (single-writer substrate):**

The live provision records are owned by the Docker backend, not the actor:
`Backend.provisions` is a `map[string]*provision` guarded by `provisionsMu`.
The actor reaches it only through `leasesm.LeaseProvisionStore`. The read side
exposes scalar status/existence and exact runtime-generation classification,
not a mutable pointer or broad snapshot. The actor package applies compound
projection changes through `UpdateFn` under the same mutex, and readiness
metrics change in that critical section. Destructive close progress is not kept
in this volatile projection; it belongs to the non-expiring close journal.

This mutex-guarded cross-lease projection plus per-lease serial actors is an
idiomatic Go hybrid: enumeration and recovery need a fleet view, while command
ordering and worker ownership need a lease-local owner. The map is a projection,
not causal settlement authority; every crash-sensitive decision comes from a
durable typed journal fact.

**Registry & lifecycle:**

- `Backend.actors` is a `map[string]*leasesm.LeaseActor`; `actorForLocked`
  resolves-or-creates under a short mutex only after the typed router has proved
  whether the input is a command or a current-generation observation.
- Actor retirement first closes external admission, drains already accepted
  worker-terminal messages, joins its worker barrier, then calls the fixed
  `OnTerminated` registry remover. Installation and goroutine start are ordered
  by `NewLeaseActor`, so even an already-cancelled backend cannot leak a
  half-installed actor.
- `errActorTerminated` is returned when a command was accepted before final
  retirement but can no longer run; the caller preserves or rolls back its
  durable authority as appropriate and a retry resolves a fresh actor.

**Inbox delivery and backpressure:**

The inbox is buffered. The external capability type determines whether actor
creation is legal: `ActorCommand` may resolve or create an actor,
`ActorObservation` carries an exact durable runtime-generation proof, and
`RecoveryCommand` may target only an already reserved/quiescent actor. The
delivery paths are disjoint:

- **`routeToLease`** — accepts only `ActorCommand`, resolves-or-creates
  the actor, and enqueues atomically under the registry mutex. The enqueue is
  non-blocking (no timeout — full inbox = immediate refusal).
- **`routeToLeaseBlocking`** — wraps command-only `routeToLease` with
  context-bounded retry for caller-facing Provision, Deprovision, Restart,
  Update, and Restore paths that need backpressure-with-retry.
- **`routeActorObservation`** — accepts only `ActorObservation`. Under
  the same actor-registry critical section it checks the recovery reservation,
  re-attests the store-issued release/projection proof, and only then resolves
  the current actor and enqueues. The actor repeats the classification immediately
  before serial handling and silently discards a generation that changed while
  queued. A delayed die/cohort event therefore cannot create an actor after
  rollback or target a replacement generation. Refused
  container-death observations are counted in `die_event_dropped_total`;
  cohort-divergence refusal is logged, and reconciliation re-detects both from
  current state.
- **Maintenance recovery admission** — accepts only `RecoveryCommand` while an
  exclusive actor-recovery claim owns the registry key and the exact actor is
  quiescent. It cannot resolve or create an actor.
- **`sendTerminal`** — used by in-flight worker goroutines to deliver terminal SM events whose physical work has already happened on the host (containers swapped, removed, etc.). Bounded by `terminalSendTimeout` (10s) and refuses on `hasExited`, `isExiting`, or send timeout. Refusals are counted in `lease_terminal_event_dropped_total`; recovery falls to the next reconcile cycle.

Docker mutation admission, restore reconciliation, and teardown share a
zero-value-ready, ref-counted mutex registry keyed by lease UUID. Unrelated
leases never contend, entries disappear after the last holder or waiter
releases them, and the fence carries no durable lifecycle authority. Update and
Restart hold it from their release-history append through the actor's definitive
accept/reject response; caller cancellation after enqueue cannot expose an
unowned "latest" release to a retry. The accepted actor state then excludes
every competing appender for the worker's lifetime. Restore reconciliation
holds the same lease key across its complete decision and mutation. Before it
reads mutable recovery inputs, it must also acquire an opaque typed
`leasesm.QuiescenceClaim` from the exact registry actor. The actor's unified
activity count spans accepted and queued messages, handler execution, workers,
and the worker-to-terminal-message handoff; the claim holds its admission and
activity gates until release and pins that actor against retirement or
replacement. If the claim is unavailable, reconciliation defers the lease.
This capability, not separate point-in-time inbox or worker-idleness snapshots,
proves that a stale Failed snapshot cannot tear down a destination while actor
work can still make it Ready. A
delayed successful-restore finalizer uses the fence while it snapshots the Ready
generation and records it, so maintenance settlement cannot activate a stale
restore row. A durable restore source finalizer is an additional admission
predicate under that same fence: Provision and Restore always refuse the
destination. A plain Restart additionally requires an exact committed Release
and no Pending or contradictory Failed restore operation; an authorized
successor may already have retired Succeeded history. Update and custom-domain redeploys refuse while the
finalizer remains; after committed failure, a successful plain Restart reaches
Ready so finalizer reconciliation can consume it under the same fence.

Background operation, maintenance, and close recovery enter through one
construction-bound `RecoveryCoordinator`. Its backend adapter atomically joins
the lease command fence with an actor-registry reservation and an exact
`QuiescenceClaim`; failure to acquire either is a lease-local deferral. The
coordinator mints a callback-lifetime `LeaseRecoveryScope`, holds the exclusion
until the callback returns or panics, and then revokes every copy. Live close
takes the complementary route: only the lease actor can mint `ActorCloseScope`,
and it does so after the deprovision transition has canceled and joined its
worker. The coordinator accepts that scope only for the currently registered
actor and its construction lineage. Consequently a durable journal claim alone,
a stale actor pointer, and a point-in-time worker count are each insufficient to
authorize physical recovery.

### Backend operation, maintenance, and close-intent journals

Providerd's placement `Attempt` proves which backend may have received a call;
it cannot prove whether that backend crossed its own asynchronous mutation
boundary. The bundled backends therefore keep a second, deliberately local
write-ahead record in `callbacks.db`. Provision and restore admission write an
immutable per-lease operation intent—including the exact callback pair, storage
identity, provider/tenant identity, items, resolved CPU/memory/durable-disk/scratch profiles,
manifest, and restore generation—by synchronous bbolt commit before the first
substrate side effect. A different
operation cannot replace it while it is Pending or its exact callback is queued;
an exact retry observes the existing or already completed record and performs no
second mutation. Recovery reads a sealed `OperationRecoveryState`: its only
concrete forms are the resolver-capable, opaque concrete `OperationIntentClaim`
and the non-resolvable `OperationSucceeded` / `OperationFailed` outcomes. The
pending claim's private fields make its zero value invalid without introducing a
nil or typed-nil interface state. External packages cannot manufacture another
state or use terminal evidence as a pending mutation capability, so restore
planning must handle the complete state set explicitly.

`shared.OperationSettlement` is the only production facade for operation
admission, recovery, active-Release publication, and terminal settlement. It is
constructed from one exact open `CallbackStore`/`ReleaseStore` pair and mints
opaque candidates and proofs tied to those store instances. The persistence
methods on `CallbackStore` and the raw operation-Release writers on
`ReleaseStore` are private implementation details; a backend cannot admit work
through one callback journal and later settle it against another release
journal, even when both files claim the same storage lineage. Reopening either
file invalidates every in-memory capability and recovery must mint replacements
from the reopened coordinator.

Durable `Started` and physical execution are joined by
`internal/backend/shared/substratemutation`, not by convention. Each settlement
owns one `Protocol[Subject]`. At backend construction, `NewExecutor` binds that
protocol exactly once to a narrow substrate facade, one workflow, one strict
classifier, and the storage-lineage authorizer; no request or recovery caller
can replace any of them. The journal's exact not-started-to-started CAS returns
the immutable subject that mints a one-shot `LiveExecution`. Restart recovery
can mint only the disjoint `RecoveryExecution` after re-reading the same durable
Started row, and it may classify but cannot replay live work.

Inside the bound facade, `Runner.Step` is the only route to a tenant substrate
effect and `Runner.Prepare` is reserved for auxiliary work such as pulling an
image. The runner becomes inert when its single execution returns. Before the
first Step, failure is a definitive refusal; after any Step is entered, any
error, panic, cancellation, or storage post-attestation failure is ambiguous.
Only a wholly successful workflow followed by the construction-bound exhaustive
classifier can return sealed Ready/Absent/Retained/Destroyed evidence. Terminal
settlement accepts that execution-bound evidence rather than booleans, IDs, or
caller-selected status. Raw Docker, Compose, and volume writers exist only as
constructor inputs and cannot be recovered from `Backend` by an operation
handler.

For Docker, that resolved snapshot distinguishes durable, retainable `disk_mb`
from a mutually exclusive ephemeral `scratch_disk_mb`. Every diskless instance
conservatively pins the current positive `container_tmpfs_size_mb` as scratch
before image inspection, even when the eventual image needs no managed
writable-path volume. Pool admission, tenant quota, stats, recovery, and live
close accounting use their sum as effective physical disk. Scratch does not make
a lease stateful or retainable; writable-path-only volumes are reclaimed at
close. If conservative classification leaves an exact scratch-volume name in a
retention row, its pinned allowance remains in physical pool accounting and
quota backfill until that name is destroyed; this is fail-closed accounting, not
retention entitlement. K3s legitimately persists zero scratch.

That resolved profile snapshot moves unchanged from operation intent to the
live provision and successful release, then into any close/retention authority.
Recovery uses the intent snapshot during the write-ahead window and the active
release snapshot afterward; mutable SKU configuration cannot reprice an
admitted generation. Supported v0.13 live rows are normalized by authority
backfill before ordinary recovery. A service-name-less pre-stack cohort is
rejected before mutation; an already stack-form cohort is checked against its
active manifest and exact
dense Docker labels, then its ordered items and canonical profiles are frozen by
a whole-release compare-and-swap. Transitional items-only active releases are
compare-and-swap backfilled against their exact version and items. Local Docker
evidence cannot prove that v0.13's requested highest instance index was not
already missing, because the old release row omitted desired items. The provider
therefore remains stopped until the placement preflight exactly compares the
backend's frozen workload with the height-pinned chain items. Operators must
keep the deployed v0.13 SKU mapping, numeric profiles, and Docker
`container_tmpfs_size_mb` unchanged through that first successful upgraded
startup.

Restore's source finalizer separately freezes the destination items, manifest,
profiles, source generation, typed operation ID, and exact operation/lifecycle
callback pair. It
remains the destination's identity and lifecycle authority across actor
settlement, release-store failure, and zero-survivor restarts. Before a matching
active Release exists, rollback performs physical/quota cleanup, transitions the
exact operation row to Failed while atomically enqueueing its callback, and then
hands the source back. If handback must be retried, that exact terminal outcome
is the durable decision; an absent operation row is corrupt or missing authority
and fails closed. Once an active Release exactly matches those immutable fields,
it is durable commit evidence: recovery transitions a matching Pending operation
to Succeeded and retains the Release. Zero survivors recover as a
conservative Failed destination with its exact allocation, never as a restore
rollback; the source finalizer remains as exact tenant/provider identity until a
successful plain Restart reaches Ready and reconciliation consumes it, or
Deprovision first transfers ownership to a fully persisted close intent and only
then deletes it. Update and custom-domain redeploys remain fenced before that
consumption because they would create topology not represented by the lingering
finalizer.
An already-Succeeded operation can also reconstruct a missing active Release
from that immutable finalizer; an exact Failed outcome plus a committed Release
is a contradiction, not rollback permission.

Terminal work atomically transitions that precise operation row from Pending to
Succeeded or Failed and enqueues the corresponding callback in the same database
transaction. This closes both crash windows: the backend never has a terminal
substrate decision without a durable callback, and callback delivery cannot erase
the terminal decision needed by an exact retry or restore handback. Successful
delivery removes only the FIFO delivery row; the terminal operation row remains.
It is removed or superseded only atomically by a later authorized lease
transition, never by delivery or age cleanup. Operation rows and queued exact
completions do not expire by age.

Bundled backends split callback semantics from transport by construction.
`shared.CallbackPublisher` is bound to the exact callback/release journal pair
and exposes only proof-specific operation and maintenance terminal methods plus
a Failed-only runtime-observation method. Substrate classification first mints
a release-only `RuntimeGenerationProof` for one exact active Release (store
instance, version, digest, and operation ID); that proof cannot publish a
callback. `AuthorizeRuntimeObservationContext` then acquires the exact journal pair's
per-lease gate and can mint a `RuntimeObservationPermit` only when the aggregate
mutation head is absent, is the matching terminal-Succeeded operation, or is a
terminal-Failed successor whose settlement sealed this exact active Release as
its predecessor. The last case preserves legitimate observations from a
surviving runtime after a replacement refusal, including an adopted v0.13
runtime; an initial Failed operation has no predecessor authority. Pending
operations and every maintenance, close, or closed phase have no representable
publication capability. Publication re-attests both the Release and that sealed
phase while holding the same gate used by operation settlement and the separate,
pair-bound Release-backfill authority. Callback route, backend lineage, status,
and retained state are never supplied by the actor. `shared.CallbackSender`
cannot enqueue or settle anything. It only
replays durable heads, performs HMAC HTTP delivery/retries, and precisely removes
an unchanged row after 2xx. Construction requires an opaque storage attestor
that binds the exact open callback-store instance to the verifier's immutable
storage ID and backend-lifetime authority gate. The attestor owns the finite
verification budget and is used before publication, replay discovery, and every
HTTP attempt; closing or reopening the store revokes it. A bare verification
closure therefore cannot be cross-wired into callback transport.

Retention mutation follows the same candidate-to-proof shape. Active selectors
return opaque, complete-row candidates rather than mutable `RetentionEntry`
values. `BeginReaping` and restore claim consume those candidates and return
state-specific `ReapingRetentionProof` or `RestoringRetentionProof` values;
delete, rollback, and close settlement accept only the matching proof and
re-attest its canonical row under the journal lock. A UUID, a detached query
DTO, or a proof from a reopened/cross-lineage store therefore cannot authorize
a destructive transition.

Docker replacement commands use the maintenance variant of the sealed
per-lease aggregate in `callback_lease_mutation_heads`, rather than overloading
the operation variant or a coalescible lifecycle observation. Admission
claims the exact active source Release, allocates a canonical UUIDv4
`maintenance_id`, and commits it before appending the deploying target or
touching Docker. The same ID is immutable in the intent, exact target Release,
and every target container. Source and target claims include store-assigned
release versions plus immutable SHA-256 fences; target admission preserves the
source tenant, canonical provider, and typed operation/lifecycle identity. A
trusted maintenance request may move the callback base without rotating that
identity, and the new route becomes authoritative only if the exact target
activates.

The request authority is minted by—and usable only with—the exact open callback
journal instance whose verified storage lineage it carries; another journal or
a reopen of the same file cannot restamp it. Admission is only a replay
classification. A newly created intent additionally carries a distinct opaque
first-dispatch/cancel capability; existing and completed replay classifications
carry no mutation-capability type. After release-capacity proof,
`StartMaintenanceAppend` consumes that capability, durably advances the row,
and returns a different append capability before the independent release
database can create a target. A crash in the resulting no-target window is a
recoverable interrupted operation through a store-issued durable claim, while
a copied stale dispatch cannot delete the only recovery index after a release
append.

Successful replacement first activates that exact target Release and only then
atomically converts the maintenance intent into a non-expiring,
non-coalescible lifecycle-route completion. Failed replacement records failure
against that exact target while rollback leaves the source Release and its
callback route active. A transient callback-store failure leaves the intent for
level-triggered recovery; it never changes an already-active target back to
failure. Because the lifecycle route is stable across replacements, admission
for the next maintenance generation refuses while any prior exact maintenance
completion for that lease remains in the durable FIFO. A synchronous 2xx and
precise removal of that delivery release the fence. This lease-local
availability tradeoff prevents an older terminal result from being published
after a newer restart/update start; it does not block maintenance for other
leases. Cold and periodic recovery never rerun an uncertain replacement: they
classify the exact MaintenanceID-bearing Release and container cohort, settle an
active target as success, activate a complete deploying cohort, fail exact
absence, and remove only inspected immutable target container IDs for a partial
cohort. Runtime readiness is tri-state: observed terminal unready evidence may
fail a generation, while an inspect error, timeout, or still-starting workload
is indeterminate and preserves the intent for another bounded sweep. A target
whose Release is already active is durable maintenance success even when its
runtime has subsequently disappeared; that case atomically replaces the intent
with two FIFO facts—maintenance Success followed by lifecycle Failed—and uses
one idempotent actor transition to install the target authority directly in
Failed. Both rows are classified as exact maintenance-derived barriers, so
delivering only the Success head cannot admit a newer generation ahead of the
paired runtime failure. If the per-lease callback FIFO is busy, recovery changes neither the
intent nor ordinary inventory and retries level-triggered. Unreadable,
divergent, removal-ambiguous, or callback-lock-busy evidence therefore preserves
the intent and keeps recovery fail closed.

Before Docker tears down a restore destination, it first persists the complete
close intent described below. It then validates the lingering source finalizer's
tenant/provider, item shape, manifest topology, resource profiles, and source
generation and CAS-deletes that row. Failure returns before teardown; success
leaves the close intent as the sole durable cleanup and identity owner.

Historical v0.13 release histories can contain the duplicate-Active shape
written by `RecordMigration`; the decoder retains a narrowly scoped read-only
normalization for that deployed wire history. This release does not write a
migration marker or attempt a live single-container-to-Compose conversion.
Only an already stack-form v0.13 cohort can be adopted: strict inventory and
the active manifest must prove its complete dense topology before exact items,
resource profiles, and tokenless runtime authority are backfilled. Pre-stack,
`-prev`, or authorityless migration remnants stop startup before mutation.

Docker deprovision has an analogous close-tagged write-ahead finalizer in the
non-expiring `callback_lease_mutation_heads` aggregate. Before the first
container, volume, retention, or release mutation, close
admission commits an opaque UUIDv4 close capability and an immutable snapshot of
the backend/storage identity, tenant/provider, ordered items, manifest, exact
callback pair, retention policy, the exact per-SKU CPU/memory/durable-disk/scratch profiles,
and selected-release version plus SHA-256 fence. The release
fence prevents a delayed close from deleting a newer deployment generation;
the physical executor re-attests each container against that exact close subject
before mutation. Quantities, topology, resource profiles, identities, callback
classes, and durable digests are validated on every decode, so malformed
authority remains preserved and unusable rather than being partially
interpreted. An already-absent release key is an idempotent retired state: the
close row itself contains the cleanup topology and blocks any newer operation
for that lease, so loss of release-history metadata cannot redirect substrate
cleanup.

Beginning a close and preempting accepted provision/restore or maintenance work
share one bbolt transaction: the earlier intent becomes its exact failed
completion, then the close row becomes the sole destructive authority. Before
that transaction, Docker settles an exact target already committed Active as
maintenance success, so close cannot rewrite success as preemption failure.
Conversely, operation and maintenance admission each check the other intent
classes and the close bucket and refuse late or overlapping work for the lease.
The aggregate head has one transactional writer whose input is a sealed sum of
phase-specific transitions. Each variant carries concrete typed predecessor,
successor, and receipt values, so an action/payload mismatch, nullable replay
fence, or phase skip cannot be assembled and handed to the writer.
Immediately before each physical execution, the close journal advances a
monotonic `CloseExecutionGeneration` and returns an opaque one-shot subject for
that exact row. A retry requires either an independently recovered Started row
or executor-attested `Incomplete`/pre-effect refusal; ambiguous live effects
cannot mint an immediate retry. No retry count can convert uncertainty into
success, failure, or cleanup authority.
`CloseExecutionPending` deliberately does not implement the terminal interface.
Only executor-minted `CloseExecutionDestroyed` and `CloseExecutionRetained`
values can enter `CompleteClose`, which re-attests both the exact close
generation and the matching retention/release facts before finalization.

Recovery, live Deprovision admission/settlement, and Restore's operation-intent
to `restoring` admission bridge and rollback handback share a backend-local
recovery-snapshot guard. Recovery holds the exclusive side from managed-container
inventory through durable journal reads and the matching provision/pool
publication. Live paths hold the shared side only across their durable
authority capture and projection/accounting handoffs; destructive Docker and
volume operations hold neither side. This prevents a close, or a restore
admitted and completely rolled back during inventory collection, from
disappearing before an older snapshot is published. Provision validation
remains available, but its short accepted-intent-to-projection handoff can wait
for snapshot publication; Restore admission may wait at its corresponding
handoff. Recovered
closes resume afterward under the per-lease command fence and a fresh journal
read. Partial or zero survivors are expected after teardown starts, so a full
close rebuilds a
conservative `deprovisioning` projection and resource reservation from its
immutable snapshot instead of attempting to reconstruct authority from whatever
Docker still reports or repricing it from mutable configuration. Retained and
reaping records created by this version inherit the same exact resource
snapshot. Pre-upgrade retention rows have no such field and use the current SKU
configuration; startup refuses admission if one of those legacy rows references
an unavailable SKU. A cleanup-only close is used when the volatile projection is
already absent but a fenced release still authorizes substrate cleanup: it
publishes no tenant-visible provision, never retains data, and remains the
non-expiring retry owner rather than giving up without a safe tombstone. Its
terminal receipt has an explicit sealed authority kind. A principal-bound
receipt retains the complete tenant/provider pair frozen from typed Release or
exact substrate authority and requires both labels to match before cleanup. A
true-orphan receipt is permitted only when neither principal witness exists; it
authorizes cleanup through the provider-authenticated close, reserved `fred.*`
managed labels, exact retired lease UUID, and the attested backend/storage pair.
Half-present principal authority is invalid on admission and decode.

Successful finalization has one required order: retire `releases.db` under the
exact fence; atomically enqueue the lifecycle result and replace the close row
with a permanent closed-UUID receipt in `callbacks.db`; only then delete the
volatile provision projection. The receipt rejects UUID reuse indefinitely.
The callback aggregate reserves one permanent UUID slot on the first operation,
maintenance, or close admission for a lease. The slot and aggregate head commit
in one bbolt transaction, so a close that has started destructive work can
always replace its already-reserved head with the closed receipt without a late
allocation failure. Slots never age out, even while a live lease temporarily has
no aggregate head. A fixed 100,000-slot limit per backend storage lineage bounds
the aggregate identity set and head scan. A separate 100,000-entry counter on
that aggregate is shared by operation and maintenance receipt reservations.
Each admission reserves its receipt before substrate mutation; settlement turns
the reservation into its permanent replay row without a second capacity
decision. Canceling an unstarted maintenance intent releases its unused
reservation, and successful close releases both receipt classes behind the
stronger lease-wide tombstone. At either limit, only work needing new capacity
is definitively refused before side effects; already-reserved transitions
continue. These are safety ceilings, not operating targets, and neither claims
that the stopped inspector's independently bounded logical-row budget equals
the number of lease UUIDs or receipts.
Each recovery sweep batch-resolves only UUIDs in live managed inventory, so the
check remains O(live substrate), excludes a late Create before projection, and
target-removes it on that or the next sweep under the receipt's sealed cleanup
authority. Callback
delivery itself remains asynchronous and replayable. This is a global durable
sender invariant, not a close-only exception: operation, maintenance, and
lifecycle completion paths persist under the per-lease FIFO lock, then send only
a non-blocking commit wake naming that exact lease to the tracked outbox loop.
The mailbox coalesces repeats for one lease without discarding the identities of
other leases; a stronger handoff wake transfers work when a canceled drainer
releases ownership. The durable row is the authority, while the periodic
30-second sweep discovers startup work and retries dormant failed heads. Current outbox rows
and compact terminal receipts carry a required wire version and reject duplicate,
unknown, or future-schema fields recursively. The versionless v0.13 decoder is
confined to the stopped upgrade inspector: current runtime requires
`pending_callbacks` to be empty and never decodes or replays those rows. An
unknown top-level `callbacks.db` bucket refuses current startup so a downgrade
cannot write through future authority. No callback network
I/O runs inside a lease actor, API handler, or startup recovery. A failure at any
earlier step keeps the close row and conservative capacity owner for the next
level-triggered recovery pass; no restart has to infer whether teardown happened.

Release histories and retention entries have the same explicit current-row
boundary: each is wrapped in a `schema_version: 1` envelope and rejects unknown,
case-aliased, duplicate, future, or trailing data recursively. Their exact
versionless v0.13 wire shapes are accepted only by the stopped
`InitializationProfileExisting` adoption transaction, which validates every row
before rewriting any row or publishing the storage binding. A versionless
identity-bound shape from an interrupted upgrade of an earlier development build
is handled only while replaying that same pending marker transaction. Ordinary
Open, Check, Verify, and runtime reads require the versioned form; they never
reinterpret a versionless authority row as legacy data. Unknown top-level release
or retention buckets likewise refuse startup.

Docker recovery deliberately uses lease-isolated evidence rather than one
fleet-wide substrate snapshot. Under the recovery mutex it first decodes the
complete maintenance-intent journal, then classifies each row under that lease's
command fence from its exact Release and a fresh bounded strict Docker
inventory. A live actor that owns the same `maintenance_id` remains the serial
owner; otherwise an unclassifiable row fails that recovery pass before ordinary
projection can interpret a mixed source/target cohort. Recovery then takes its
ordinary managed-container snapshot, loads close authority before validating
ordinary callback labels or release cohorts, and excludes close-owned cohorts
whose disappearance is intentional. During startup, operation-intent preflight
and settlement run later, after ordinary projection and restore authority have
been reconstructed before quota reconciliation or any exact finalizer can consume evidence. An exact
ready container set reconstructs the active release and Succeeded operation;
exact absence produces an interrupted-operation failure. An identity-exact but
incomplete or terminal provision cohort is failed only after its candidate
containers are removed; an identity-exact partial, failed, paused, or other
non-terminal restore cohort enters its destination-fenced rollback. Partial or
mixed callback identities, unavailable SKUs, unreadable retention state, and
other identity, topology, or read uncertainty preserve the Pending row and fail
startup closed. A pre-existing terminal row is an immutable recovery decision;
without a committed Release, its absence where a restore finalizer names that
operation is invalid. The k3s scaffold
creates no cluster objects, so its only valid operation-intent recovery is the
deterministic `not implemented` failure.

For Docker, an already-active Release with durable runtime authority is itself
exact recovery evidence even when containers are exited or entirely absent.
Current releases carry an immutable nested typed authority that binds the same
valid operation UUID to tenant, canonical provider UUID, and the current
operation/lifecycle callback pair. A fully inspected callback-bearing v0.13
cohort instead gets a separately typed `LegacyRuntimeAuthority`: it freezes the
exact principal and tokenless callback pair without inventing an operation
capability the old provider never issued. Both forms also bind the exact
manifest, emitted items, and resource profiles. Invalid, mixed, or partial
forms are unrepresentable through their constructors and rejected on decode.
Recovery therefore transitions
a matching Pending operation to Succeeded, or reconstructs either authority class as
a conservative Failed projection and reservation without appending a new
Release.
Resource accounting consumes only complete cohorts reconstructed from an exact
intent, Release, close claim, or restore finalizer; a container-observed prefix
is never capacity authority. The pool replaces owners represented by those
durable cohorts and conservatively preserves every existing owner omitted from
the recovery publication. Consequently an incomplete observation can leak
reserved capacity until the exact terminal close/rollback path releases it, but
cannot expose phantom capacity or over-admit after a restart. This fail-closed
leak is intentional: operator-visible over-counting is safer than reconstructing
absence into permission to reuse tenant capacity.
An active callbackless pre-label cohort cannot be assigned provider callback
authority safely and is rejected by the mandatory stopped adoption preflight.
Callbackless historical cleanup/close evidence remains readable, but never
authorizes zero-survivor recovery or maintenance.

Restart, update, and custom-domain replacement preserve that authority class.
Their separate UUIDv4 `maintenance_id` remains the exact write-ahead and
container-cohort identity even for a tokenless v0.13 release. A callback-base
move is pending substrate state and becomes authoritative only when the target
Release activates; failure or rollback leaves the prior active route intact.
The first and every subsequent restart, update, or custom-domain replacement of
that lineage remains legacy and tokenless; `maintenance_id` is replacement WAL
and cohort identity, not provider callback authority. Only a later genuine
provision or restore operation can rotate it to operation-scoped typed callback
authority.

This is an anti-corruption boundary, not a tokenless mode in current lifecycle
code. Stopped adoption converts validated v0.13 evidence into the distinct,
zero-invalid `LegacyRuntimeAuthority` variant; authenticated callback ingress
recognizes an absent query capability only when it matches that migrated owner.
Current operation constructors reject missing IDs, and no ordinary provision,
restore, reconciliation, or maintenance path can mint tokenless authority.
Current identities are also distinct types rather than contextual strings:
`operationid.ID` is the neutral provision/restore identity shared across the
provider/backend wire and durable layers, `lifecycle.ID` is observation authority
and requires an explicit checked conversion from an operation identity, and
`maintenanceid.ID` identifies a caller-issued restart/update command. Their
representations are private, their zero values are invalid, and construction
either validates one canonical UUIDv4 spelling or returns cryptographic-random
generation failure to the caller. Operation and lifecycle UUIDs are also causal
capabilities: their typed values implement `slog.LogValuer` and `fmt.Formatter`
with domain-separated, non-reversible fingerprints. Generic structured logging,
error wrapping, and formatting therefore cannot reveal the canonical token;
only explicit wire/persistence conversion can. Callback URLs remain sensitive
at their string boundary and are never diagnostic values.

Tenant restart/update admission has a provider-side write-ahead boundary in the
already-required placement database. The API accepts exactly one canonical
UUIDv4 `Idempotency-Key` and converts it to an opaque `maintenanceid.ID` before
acquiring mutation authority. One atomic `BeginMaintenanceCommand` transaction
requires the exact current placement revision and matching confirmed backend,
storage identity, lifecycle generation, provider, tenant, kind, and payload
fingerprint. A pending-head index is a derived uniqueness constraint: one lease
can have at most one pending command, and every placement/lifecycle write choke
point fences that lease while the head exists. Startup rehydrates the same
`operation.Registry` lease claim before API or reconciliation composition, with
no network calls; bounded recovery later re-authorizes stored facts and retries
only the exact request. An unavailable backend therefore delays its pinned
leases without becoming a fleet-wide startup gate.

Callers never construct the routing half of that command. The placement store
mints a distinct prepared-command capability from its current placement,
storage identity, lifecycle generation, and durable tenant/provider runtime
principal; `BeginMaintenanceCommand` accepts only that capability, while
recovery accepts only the distinct pending-claim type. Exact provision/restore
promotion records the principal directly. For a migrated v0.13 owner, one
complete identity-bearing provision/retention projection may establish it only
when the reported tenant/provider, configured backend storage identity, and
legacy lifecycle observation all agree. Partial inventory cannot create or
change a principal. Once established it survives provider restart, so a later
outage of an unrelated backend does not disable maintenance on the available
confirmed owner.

Update acceptance is a three-part boundary: the backend must accept (including
an exact idempotent replay), the exact payload must be durable in the payload
store, and only then may the provider replace Pending with a compact terminal
receipt and release the lease claim. Transport uncertainty retains Pending.
Authoritative chain evidence that the lease ended or its authority was revoked
instead writes a terminal cancellation, preventing an immortal command from
deadlocking close. Terminal provider receipts omit payload bytes, retain the
SHA-256 fingerprint, and remain exact for as long as placement or lifecycle
authority for the lease exists. Provider and backend therefore share the same
lifetime command-identity boundary: neither side can age out a UUID and mistake
an exact late retry for new asynchronous work. Both histories use an explicit
ceiling of 1,024 compact receipts per live lease and safely refuse the next
command before side effects. The transaction that removes the lease's final
placement or lifecycle authority also reclaims provider receipts atomically;
startup and periodic command recovery do not scan lifetime history. A retained
full-store sweep exists only for explicit repair/upgrade. Pending commands never
expire. The backend's store-assigned completion sequence also
classifies an older completed update as superseded, preventing a recovered
request from authorizing that update's stale payload to overwrite a later one.
Successful close removes the backend receipt set under the permanent
closed-lease fence.

Callback-store health validates every delivery and intent bucket and the
invariant that one lease cannot simultaneously own operation, maintenance, and
close rows. A terminal Succeeded/Failed operation row is history rather than
active mutation authority; admission of an authorized successor atomically
retires it instead of leaving simultaneous rows. That health check is
structural: a valid maintenance intent
whose substrate outcome is still indeterminate remains healthy evidence rather
than corrupt storage. Likewise, `callback_store_errors_total` covers instrumented
callback persistence/store failures and fail-closed operation-intent startup
recovery; it is not a generic counter for every semantic maintenance-recovery
refusal. A periodic maintenance refusal logs the lease, increments
`reconciliation_total{outcome="error"}`, and leaves
`reconciliation_last_success_timestamp_seconds` stale; the same refusal during
startup prevents the backend from starting. `/health` can remain green during a
periodic semantic refusal. Close-finalizer retries instead log the lease and
durable execution generation. There is intentionally no pending-intent gauge:
a short-lived operation or maintenance intent is normal, and a lease-labeled
close gauge would have unbounded cardinality.

### Authoritative backend-store lineage

The storage-identity marker pair and the backend's authority-bearing bbolt files
form one lineage seal. Docker always requires `callbacks.db`, `releases.db`, and
`retention.db`; the retention store exists and is bound even when
`retain_on_close` is false, because absence must never be confused with an
empty authoritative retention inventory. K3s requires `callbacks.db` and
`releases.db`. Each store carries the canonical backend storage UUID and a
distinct store-kind tag, so a database copied from another backend generation
or substituted at a different configured role fails closed. The diagnostics
database is deliberately outside this set: it contains bounded operator-facing
history, grants no mutation or lifecycle authority, and may be recreated while
the backend is stopped. Its generic opener still rejects a symlink, hard link,
non-regular file, or mode other than exact `0600`, but diagnostics is not bound
to the storage UUID and is not re-attested at every transaction.

Initialization is the only code path allowed to create or bind authoritative
stores. Before reading fresh/existing evidence, the initializer retains typed
directory capabilities for both marker parents and every authoritative journal
parent. They bind the physical device/inode and make all later inspection,
creation, and publication descriptor-relative; a rename, unmount, or same-path
directory recreation cannot redirect the transaction. A pending anchor first
records both the chosen storage UUID and whether the input is a wholly fresh set
or a complete existing v0.13 set. Only after that anchor is durable does the
marker coordinator mint the opaque `PendingStorage` capability accepted by
authoritative-store preparation; a parsed or caller-supplied UUID cannot bind a
store. Every capability copy shares one revocation state. The coordinator
revokes it on every return path and explicitly before the first committed-marker
publication, so an escaped hook copy cannot create or bind another store after
the pending preparation phase. The initializer then creates/binds and
validates every required store, rechecks every parent, the substrate, and
cross-store evidence, publishes the primary marker, and finally commits the
anchor. It returns `VerifiedStorage` only after final committed-marker and
read-only store verification. This ordering is crash-resumable and idempotent:
a rerun must use the
persisted profile and exact identity, while a committed pair is
verification-only and never repairs a missing or unbound store. Fresh input
means every required store is absent; existing input means every required store
is present. A mixed set is not guessed or completed. Existing adoption
additionally requires a stopped backend, a drained legacy callback outbox,
valid release/retention rows, and substrate evidence that agrees with the active
releases.

That substrate evidence is concrete rather than name-based. Every managed name
first becomes one canonical typed component and must then be an actual Btrfs
subvolume, a real directory on the configured XFS root with a regular no-follow
nonzero project-ID marker, or the exact depth-one ZFS child mounted at its exact
managed path. ZFS proof inventory unions child datasets with directory entries,
so an unmounted or externally mounted child cannot disappear as absence.
Container evidence separately proves the label-derived volume and exact
target-derived subtree, rejecting a symlink at any component. XFS startup quota
reconciliation establishes the live kernel project tag and limits before
readiness; the marker alone is durable project-ID authority, not proof that a
previous tagging command completed.

The XFS allocator's ownership domain is the containing filesystem, not the
managed directory. Project IDs and dquots are filesystem-global, while Fred
scans only its configured root and allocates from the full nonzero 32-bit
namespace. Sole project-ID allocation on that mount is therefore a deployment
invariant: separate Fred roots or a foreign allocator can collide even when
their paths do not. A dedicated XFS mount is the current construction-safe
choice. Coordinated disjoint ranges require a follow-up allocator feature; no
range configuration exists in this release.

XFS therefore creates under a typed, parent-synced hidden stage whose name
carries the nonzero project ID and final managed name. Only after its marker,
project tag, and limits are durable does a descriptor-rooted no-replace rename
publish the bind-ready final name. A stage recovered on normal sealed startup is
cleanup-only because its requested quota was not persisted. Its parent-synced
typed name authorizes clearing the encoded dquot and deleting an empty stage or
one whose sole no-follow regular marker is at most ten bytes; that marker may be
empty, partial, or zero-filled after a crash. This weaker proof can never
publish: publication still requires a complete parsed marker equal to the ID in
the stage name. Runtime post-durability errors attempt exact compensation; the
external quota clear uses a detached cleanup context capped at 30 seconds and
by any earlier aggregate parent deadline. Any cleanup failure or outcome
ambiguity preserves the stage, latches the current backend instance, and
cancels further substrate work. The daemon closes its listener, drains, and
exits status 1 so its supervisor launches a fresh `Start`, which must recover
the retained authority before serving again; a persistent fault crash-loops
closed. The same process does not retry it. The backend never publishes
recovered stage evidence.

XFS destruction uses a separate typed, parent-synced authority named
`.fred-xfs-delete-<project-id>-<managed-volume>`. It is an empty sibling that is
normalized to project ID zero and synced before the final volume is changed; the
final name remains in place while its contents are removed. This lets a restart
recognize the exact deletion even if the tenant volume's marker was already
unlinked. Recovery re-normalizes and re-attests the authority before touching
the final tree, requires the final name's absence to be parent-synced, and then
proves both block and inode usage for the encoded project ID are zero. An
open-but-unlinked file therefore keeps the operation pending. Only after that
proof does recovery clear all block and inode limits, remove the delete
authority, and sync the parent again. Any failure preserves the authority and
rejects creation of the same final name. A runtime failure takes the graceful
nonzero latch path; a failure found by `Start` exits 1 before binding a listener.
The supervisor's fresh `Start` must resume the exact cleanup before readiness.

ZFS instead retains an exact unmounted child and normal sealed startup remounts
and re-attests it without destruction. Any ambiguous recovery preserves evidence
and prevents readiness. Read-only preflight and initialization reject both
create and delete forms rather than mutate the lineage being proved. Btrfs has
no private stage; its already-published subvolume is classified by operation
recovery and the fatal quota gate. Unattributed Btrfs subvolumes are preserved;
only an exact durable operation, close, or retention finalizer can destroy them.

Normal startup loads the committed marker pair as a typed verified-storage
capability, verifies the complete authoritative set before recovery or cleanup,
and opens each store without file-creation or lineage-repair authority. The open
requires exact mode `0600` and records the exact single-link regular-file inode
without following the final path component. Every later authority read/write,
including initial and periodic callback/release cleanup, re-attests the store
binding, configured path, and inode; request/substrate boundaries independently
re-attest the marker pair and Docker daemon or cluster identity. Deletion,
rename, replacement, a symlink, a hard-link alias, permission drift away from
exact `0600`, a foreign identity, or a cross-kind database latches identity
drift and fail-stops the backend rather than silently creating a new authority.
The shared write primitive also distinguishes a
definitely rolled-back pre-commit rejection from a bbolt `Commit` error. The
latter may already be durable, so it latches mutation-outcome ambiguity instead
of retrying against an assumed rollback. Both cases publish the first terminal
cause into one backend-lifetime authority latch. Every sibling journal consults
that latch at its transaction boundaries. Authoritative writes hold the shared
gate through bbolt `Commit` and post-commit path proof: withdrawal waits for an
already admitted write to finish, becomes visible before another write can
enter, and therefore linearizes commit-before-withdrawal versus refusal.
Callback delivery consults the same latch before sending, so one failed store
or substrate proof withdraws the whole lineage rather than letting independent
journals advance inconsistently.

This fence detects incomplete and mixed restores, not a simultaneously running
clone of one complete stopped snapshot. A snapshot containing the matching
marker pair, all authoritative stores, and the substrate intentionally remains
the same lineage. Operators must fence the original before starting the restored
copy and must roll the whole set back together; there is no supported recovery
that combines individually matching files from different points in time.

The production deployment envelope verified read-only for the ENG-632 cutover
on 2026-09-02 is `docker-backend` on XFS. Btrfs and ZFS retain automated coverage
but are not deployed or production-validated, and K3s is the non-functional
scaffold described below. This point-in-time fact scopes the rollout proof; it
does not replace the mandatory pre-cutover re-inventory in `DEPLOYMENT.md`.

### K3s backend (experimental)

`internal/backend/k3s` is an **experimental, non-functional scaffold (ENG-133)**. It boots, serves the full backend contract over HTTP, and wires up config/metrics/health, but its provisioner is a stub: every accepted provision flips to `failed` and posts a `status=failed, error="not implemented"` callback (`internal/backend/k3s/provision_stub.go:15`). Real Pod/Deployment provisioning lands in ENG-134+.

The SM/actor machinery (`internal/backend/shared/leasesm`) is **shared across backends** and substrate-agnostic — the actor holds no backend pointer and receives narrow interfaces plus fixed operation handlers at construction (`LeaseProvisionStore`, `InstanceInspector`, `DiagnosticsGatherer`, and the journal-claim executors). A caller can submit only opaque commands minted by validating constructors; it cannot attach a different physical function at dispatch time. The same single-writer projection model therefore applies to any backend that supplies those seams.

## Data Flow

### Placement Store

Tracks which backend serves each lease (bbolt + in-memory cache):
- The database is authority for exactly one canonical provider UUID. Production
  startup supplies the configured UUID to a strict open and rejects a database
  prepared for another provider before using any placement fact. That open
  never creates the file, initializes buckets, or migrates schema: v0.13
  adoption and genuinely fresh initialization are explicit offline workflows.
  Legacy preparation installs provider authority only after a read-only
  three-source proof: exact stopped-database ownership, complete identity-bearing
  backend inventory (including each provision's reported provider), and exact
  membership in a signer-free all-state provider query pinned to one block
  height. Retention-only survivors must be present in that chain snapshot too.
  Because membership mints durable provider authority, the offline tool defaults
  to certificate-verified gRPC TLS. Only an exact operator attestation for an
  intentionally local development chain can consume plaintext or skip-verify
  evidence, even when a shared template sets `production_mode: true`; verified
  TLS rejects that stale override. Before collecting any remote proof, mutating
  preflight/repair modes bind the mandatory backup target's physical parent
  device/inode. Backup I/O remains descriptor-relative, publication is an
  atomic `renameat2(RENAME_NOREPLACE)`, and the exact published inode plus parent,
  SHA-256 bytes, length, `0600` mode, and single-link status are re-attested
  before and after mutation and before a success verdict. A
  pre-commit failure after publication is `BACKUP PUBLISHED`; a later failure
  after a definite mutation is `PREPARED:` or `COMMITTED:`; an indeterminate
  bbolt commit is `OUTCOME UNKNOWN`
- Retains a descriptor for the exact regular-file inode opened at
  `placement_store_db_path` and re-attests that the configured pathname still
  resolves to it at every authority-bearing read/write boundary. Initial open
  and every re-attestation require an unsymlinked, single-link regular file with
  exact mode `0600`; a path, permission, or link-count mismatch permanently
  withdraws authority from that process. So does a bbolt `Commit`
  error: pages may already be visible, and retrying from an assumed rollback
  would be unsafe. Pre-commit transaction-construction failures remain definitely
  uncommitted and retryable. The database is therefore not hot-swappable; live
  replacement, unlink, or rename cannot be repaired by restoring the pathname
- Writes an operation-scoped attempt after `RouteForProvision` picks a backend
  but before the backend call; promotes it to confirmed ownership only from an
  exact accepted/positive result
- Persists the current typed lifecycle callback capability in a sibling bucket,
  promotes it atomically with positive ownership, and retains it long enough to
  authorize and then retire delayed teardown observations
- Read by direct-routed tenant operations such as connection details, logs,
  release history, restart, update, and restore. Provision/status discovery keeps
  its bounded fan-out behavior
- Deleted normally by the reconciler (`cleanupOrphanedPlacements`, ENG-333); survives lease close so a later restore can route to the source node. After a `PENDING` provisioning operation is rejected, the callback may delete the whole record eagerly, but only when its revision and every named backend still belong to that failed operation. An `ACTIVE` re-provision failure clears only its attempt
- Refreshed from backend `/provisions` and `/retentions` inventories inside an
  existing prepared authority. Inventory cannot reconstruct a lost authority.
  A positive
  report confirms attempted ownership only with the exact paired typed
  lifecycle generation; contradictory positives are
  unioned with every prior owner/attempt into durable conflict quarantine.
  A response rejected for contradictory provision/retention membership,
  missing or inconsistent endpoint storage identity, or conflict with a durable
  storage-identity pin still contributes its raw positive membership to a
  distinct `untrusted_positive` quarantine. A sole candidate of that exact kind
  may self-resolve only from a later complete, identity-valid matching positive
  from the same backend. Partial inventory, silence, a different or second
  reporter, unknown ownership, and ordinary conflicts cannot resolve it.
  Inventory absence never clears an attempt or operator-only quarantine, even
  after every backend answers
- Stores the immutable backend-identity history and the topology-bound admission
  baseline. A later partial sweep attenuates that baseline to a typed scope for
  recordless `PENDING` reconciliation on nodes that answered both inventories.
  An unchanged topology therefore tolerates a transiently silent node without
  globally pausing healthy-node admission. A membership change instead requires
  identity-bearing evidence from the complete proposed fleet and invalidates the
  old baseline until a complete inventory commits
- Persists, with every complete projection, the topology-bound set of backends
  whose raw `/provisions` and `/retentions` responses were both concretely empty.
  Removing a name requires that latest complete raw evidence plus the absence of
  every placement and lifecycle reference; projection filtering and backend
  silence cannot manufacture a drain proof
- Required in every deployment for write-ahead safety, restore ownership,
  restart recovery, and correct routing. It is backup-critical rather than a
  derived cache: restore it after loss, only while `providerd` is stopped. A live
  backup must use an atomic filesystem snapshot rather than replacing or copying
  over its pathname. The explicit fresh initializer exists only for a genuinely
  new provider after a complete chain query proves zero total lease history, an
  independently supplied exact fleet roster matches configuration, and every
  configured backend proves complete empty inventory while mutation ingress is
  fenced and the live inventory-serving backends are drained and idle. Its
  print-time acknowledgement includes the target parent's physical device/inode;
  initialization reopens that exact directory, uses descriptor-relative I/O,
  and publishes with `renameat2(RENAME_NOREPLACE)`. A renamed or recreated
  parent therefore invalidates the proof rather than redirecting the new
  authority

### Payload Store

Tenant payloads are stored temporarily in bbolt (an embedded key-value store):
- Written when payload uploaded
- Read when provisioning starts
- Deleted after successful provision or TTL expiry
- Uses write batching for efficiency under load
- Creates a missing database descriptor-relatively with exclusive `0600`
  permissions; an existing database must be an unsymlinked, single-link regular
  file with that exact mode
- Retains the physical parent and database inode and re-attests parent, path,
  mode, and link count before and after every read or batched write. Drift
  permanently withdraws payload-store authority for that process

### Token Tracker

Used tokens are tracked in bbolt to prevent replay attacks:
- Token signature stored as key
- Expiry time stored as value
- Periodic cleanup removes expired entries
- Survives restarts (persistent)

## Observability

### Metrics (Prometheus)

All metrics use the `fred_` namespace and are exposed at `/metrics`. The docker-backend exposes its own set under `fred_docker_backend_*` at the docker-backend's own `/metrics` endpoint.

**Each binary exports its own section below and nothing else** — the tables here are a contract, not a catalogue. Collectors are created with `promauto`, which registers on the default registerer at *package init*, so a binary exports everything its dependency closure declares whether or not it has a call site. That is how every docker-backend came to export 21 providerd-only collectors at a permanent 0 (ENG-712), and why `internal/metrics` is providerd's alone: the backend `cmd` packages assert their surface in a test, and a `depguard` rule keeps the import from coming back. The `fred_background_*` panic counters below are the one family every binary emits; they live in `internal/metrics/background`, and being label-carrying they cost a binary that never writes them nothing.

#### Fred (`/metrics` on the providerd HTTP server)

**API:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_api_requests_total` | counter | `method, path, status` | API request count. The `path` label is the matched-route TEMPLATE (e.g. `/v1/leases/{lease_uuid}/status`), with a single `unmatched` bucket for requests matching no route — bounding `path` to the finite set of registered routes + 1 (closes an unauthenticated path-scan cardinality vector, ENG-448/F28) |
| `fred_api_request_duration_seconds` | histogram | `method, path, status` | Request latency |
| `fred_api_rate_limit_rejections_total` | counter | `limiter` | Rate limit rejections. `limiter="global"` = the single per-IP limiter shared across all routes (no route/path dimension); `limiter="tenant"` = per-tenant limiter |
| `fred_api_non_in_flight_callbacks_total` | counter | `backend, status` | Callbacks received at ingress outside exact in-flight operation settlement, including observations later dropped by lifecycle policy |

**Provisioner:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_provisioner_in_flight_provisions` | gauge | — | Current in-flight provisions |
| `fred_provisioner_provisioning_total` | counter | `outcome, backend, operation` | Provisioning operations by outcome/backend. `operation` ∈ `provision`/`restore` separates fresh provisions from restores (ENG-358) |
| `fred_provisioner_provisioning_duration_seconds` | histogram | `backend, operation` | Provisioning latency. `operation` ∈ `provision`/`restore` |
| `fred_provisioner_callback_timeouts_total` | counter | — | Backend callback timeouts |
| `fred_provisioner_callback_settlement_claim_wait_timeouts_total` | counter | — | Callback handlers that exhausted the bounded wait for another terminal actor's exact in-flight operation claim. Any increase indicates a stuck or unusually slow settlement actor |
| `fred_provisioner_callback_placement_semantic_conflicts_total` | counter | — | Authenticated success-callback settlement attempts that encountered a permanent semantic placement verdict and continued toward chain acknowledgement while preserving the durable record. Retries may increment the counter more than once |
| `fred_provisioner_callback_deprovision_owned_success_total` | counter | — | Provision-success callbacks observed while close/deprovision owned that exact operation ID. Fred consumes the callback without acknowledging the closing lease; any increase identifies a provision/close overlap |
| `fred_provisioner_lifecycle_callback_outcomes_total` | counter | `outcome, verdict, status` | Authenticated callbacks routed to lifecycle policy, classified exactly once by bounded application outcome, authorization verdict, and callback status |
| `fred_provisioner_lifecycle_event_sink_panics_total` | counter | `event` | Panics recovered from best-effort lifecycle event sinks before backend dispatch, while recording a restore refusal, or after terminal callback settlement. `event` is bounded to `provision_starting`, `restore_restarting`, `restore_refused`, or `callback`. Recovery deliberately lets backend dispatch or callback settlement continue |
| `fred_provisioner_backend_invocation_panics_total` | counter | `operation` | Panics recovered at providerd's construction-bound backend execution boundary. `operation` is bounded to `provision`, `restore`, `deprovision`, `restart`, `update`, `get_provision`, or `reconcile_custom_domain`; mutation panics remain ambiguous because the side effect may already have occurred |
| `fred_provisioner_ack_batch_fee_gas_errors_total` | counter | `lane` | Ack-batch failures classified as insufficient-fee or out-of-gas — sustained non-zero indicates `gas_limit`/`max_gas_limit`/fee misconfiguration |
| `fred_provisioner_ack_batch_individual_fallbacks_total` | counter | `lane` | Ack-batch failures that fell back to per-lease retries |
| `fred_provisioner_reconciler_inflight_skips_total` | counter | — | Ready leases the reconciler skipped because the main flow owns them |
| `fred_provisioner_reconciler_panics_total` | counter | `stage` | Panics recovered in reconciler goroutines (`process_lease`, `process_orphan`, `fetch_provisions`, `fetch_retentions`, `check_placement_marker`, `placement_cleanup`) — any non-zero is a latent bug; placement-cleanup recovery preserves the exact candidate while unrelated worker lanes continue |
| `fred_placement_write_failures_total` | counter | — | Failed durable placement mutations or sync verification. Any increase is actionable: a definitely pre-commit failure blocks the backend call and may be retried, while an outcome-unknown bbolt `Commit` error permanently withdraws this process's placement authority for offline classification |

**Reconciler:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_reconciler_runs_total` | counter | `outcome` | Reconciliation runs by outcome (`success`, `partial`, `degraded`, `error`) — one per sweep, most severe wins |
| `fred_reconciler_duration_seconds` | histogram | — | Run timing |
| `fred_reconciler_actions_total` | counter | `action` | Actions taken (`provisioned`, `acknowledged`, `deprovisioned`, `anomaly`, `lease_error`) |
| `fred_reconciler_last_success_timestamp_seconds` | gauge | — | Unix timestamp of last **clean, complete** run — a degraded sweep does not advance it |
| `fred_reconciler_conflicts_total` | counter | — | Reconciler conflicts (lease already in-flight) |
| `fred_reconciler_backend_fetch_total` | counter | `backend`, `outcome` | Per-backend provision-list attempts (`ok`, `error`, `circuit_open`). The signal that a single backend is unreachable, which no longer breaks the fleet-wide sweep |
| `fred_reconciler_sweep_complete` | gauge | — | 0 while a sweep is in progress or after an incomplete/error sweep; 1 only after the most recently completed full-fleet inventory was durably projected. This is an observability signal, not a global admission gate; a matching durable topology baseline survives a 0 |
| `fred_provisioner_reconciler_deferred_leases_total` | counter | — | Leases skipped because ownership or lifecycle evidence was not safe to act on: a backend was silent, placement was ambiguous/unresolved, or an operation or placement change crossed the inventory boundary. It can increase during a complete sweep under ordinary concurrent lease activity |
| `fred_reconciler_cleanup_skips_total` | counter | `pass`, `reason` | Destructive cleanup withheld for lack of positive evidence. `pass`: `orphan`, `payload`, `placement`. `reason`: `chain_live`, `chain_unknown`, `chain_unknown_state`, `chain_error`, `backend_silent`, `attempt_pending`. Every value is a deliberate fail-safe refusal to delete; `attempt_pending` means an ambiguity remains while exact same-operation redelivery to the pinned backend, its callback, exact paired-generation inventory, a contract-conforming refusal, or operator repair may settle it. `chain_unknown` (no record — check the endpoint) and `chain_unknown_state` (fred is older than the chain — upgrade it) are the two absence/state reasons that do not self-heal |

**Backend:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_backend_requests_total` | counter | `backend, operation, status` | Backend request count |
| `fred_backend_request_duration_seconds` | histogram | `backend, operation, status` | Backend request latency |
| `fred_backend_circuit_breaker_state` | gauge | `backend` | Circuit breaker state (0=closed, 1=half-open, 2=open) |
| `fred_backend_healthy` | gauge | `backend` | Backend health (1=healthy, 0=unhealthy). Written **only** from inside the `/health` and `/readyz` handlers, so it is exactly as fresh as whatever polls them; with no prober it latches at its last value rather than going absent |
| `fred_health_check_healthy` | gauge | `check` | Health of a non-backend dependency as observed by the health handler — `chain`, `token_tracker`, `placement_store`, `placement_inventory`, `payload_store` (1=healthy, 0=unhealthy). `placement_store=0` also covers sticky runtime path/inode withdrawal or an outcome-unknown commit; `placement_inventory` is the topology-bound admission baseline and is always present. Backends are excluded because `fred_backend_healthy` already carries a per-backend label this one cannot express. Same freshness caveat |
| `fred_backend_insufficient_resources_total` | counter | `backend`, `verdict` | Capacity 503s split into `coded_refusal` (contract-conforming; exact attempt is clearable) and `ambiguous` (legacy/code-less/unknown-code; attempt retained) |
| `fred_backend_malformed_error_body_total` | counter | `backend`, `operation` | Client-error responses whose body was not the declared JSON error envelope |
| `fred_backend_allocated_cpu_ratio` | gauge | `backend` | Allocated-CPU ratio observed by the router at provision time (allocated/total). Per-backend router-decision signal, event-sampled on multi-candidate routing; not intended for cross-backend aggregation — use the backends' own `/stats` component gauges for fleet views (ENG-318) |
| `fred_backend_routing_fallback_total` | counter | — | Provision-routing decisions that fell back to round-robin (no usable backend load stats) |
| `fred_backend_health_probe_panics_total` | counter | — | Panics recovered inside a per-backend health-probe goroutine. Always a bug: the probe is an HTTP call that should return errors. The recover exists because these probes run on their own goroutines, where net/http's per-connection panic recovery does not reach them |

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
| `fred_withdraw_credit_check_zero_deferred_total` | counter | — | Credit checks that read an empty balance but deferred lease closure pending the `credit_check_zero_grace_period` window (aggregate, no tenant label); a sustained or rising rate suggests a chronically lagging chain node or a mistuned grace period (ENG-591) |

**Payload:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_payload_uploads_total` | counter | `outcome` | Uploads (`success`, `invalid_auth`, `hash_mismatch`, `conflict`, `error`) |
| `fred_payload_stored_count` | gauge | — | Payloads currently stored |
| `fred_payload_size_bytes` | histogram | — | Upload size distribution |
| `fred_payload_leases_awaiting` | gauge | — | Leases waiting for payload upload |
| `fred_payload_persist_failures_total` | counter | `operation` | Payloads applied to a backend but not persisted — each one is a lease whose running deployment has no durable record (ENG-619) |

**Signer pool:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_signer_pool_size` | gauge | — | Total signers (primary + sub-signers) |
| `fred_signer_pool_lane_count` | gauge | — | Active batcher lanes. Falls to 1 when the pool is demoted to single-signer, which since ENG-688 happens only on a *positive* fact — the grant queries succeeded, reported the grants missing, and creating them failed. A grant query that merely fails no longer demotes; watch `fred_signer_grant_check_total` for that. |
| `fred_signer_grant_check_total` | counter | `outcome` | Sub-signer authz grant sweeps, incremented exactly once per pass of the sub-signer maintenance loop, so `sum without (outcome) (increase(...))` is a loop-liveness heartbeat and `{outcome="error"}` is the failure signal. This is the only signal for a `providerd` that could not verify its grants — that state deliberately leaves the pool intact, so no gauge moves. |
| `fred_signer_balance` | gauge | `role, address, index, denom` | Per-signer balance in the configured fee denom, sampled on each `/metrics` scrape via a custom collector with a 5s per-scrape timeout and parallel per-address bank queries. `role` ∈ `provider`, `sub_signer`. `address` is the bech32 verbatim from the live signer pool (so `DemoteToSingleSigner` is reflected on the next scrape). `index` is the slice position (`0..N-1`) for `sub_signer`; empty for `provider`. `denom` is the bank denom queried, sourced from `cfg.FeeDenom` (default `umfx`). Single-signer mode naturally emits only the `provider` series. Per-address query failures drop that one series and bump the failures counter below; other addresses on the same scrape still emit. |
| `fred_signer_balance_query_failures_total` | counter | `role, address, denom` | Per-address signer balance query failures during scrape sampling (no `index` label — index is gauge-only). Each `slog.Warn` failure on a scrape increments this once. |

**Watermill / events:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_watermill_messages_total` | counter | `topic, outcome` | Watermill messages processed |
| `fred_watermill_poisoned_messages_total` | counter | — | Messages sent to poison queue |
| `fred_events_dropped_total` | counter | `event_type` | Events dropped due to full subscriber channels |
| `fred_messages_malformed_total` | counter | `topic` | Unparseable messages |

**Background goroutine health** — these shared metrics are emitted by the fred binaries that run the corresponding background loops, which is why both live in `internal/metrics/background`. Both are `CounterVec`s, so **a healthy process exports no series at all**: absence is the normal state, and a series appearing at all is the event. `component` names the loop that panicked and, with the scrape's `job`, the process:

| Metric | Emitted by | `component` values |
|---|---|---|
| `fred_background_cleanup_panics_total` | providerd | `token` |
| | docker-backend | `callback`, `diagnostics`, `releases`, `retention` |
| | k3s-backend | `callback`, `diagnostics`, `releases` (no retention store — retention is docker-only, ENG-325) |
| `fred_background_goroutine_panics_total` | providerd | `payload_writer`, `ack_batcher`, `withdraw_scheduler`, `timeout_checker_sweep`, `timeout_checker_candidate` |
| | docker-backend, k3s-backend | `callback_replay` |

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_background_cleanup_panics_total` | counter | `component` | Panics in cleanup loops (token tracker, callback store, retention sweep, etc.) — any non-zero is a bug |
| `fred_background_goroutine_panics_total` | counter | `component` | Panics in long-lived background goroutines |

#### Docker backend (`/metrics` on the docker-backend HTTP server)

All docker-backend metrics live under `fred_docker_backend_*`, and that endpoint carries nothing else beyond the shared `fred_background_*` counters above — `cmd/docker-backend/metrics_surface_test.go` asserts it.

**Provisioning & resources:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_provisions_total` | counter | `outcome` | Provision attempts |
| `fred_docker_backend_deprovisions_total` | counter | — | Deprovision operations |
| `fred_docker_backend_active_provisions` | gauge | — | Active provisions |
| `fred_docker_backend_provision_duration_seconds` | histogram | — | End-to-end provision time |
| `fred_docker_backend_operation_intent_recovery_timeout_exhaustions_total` | counter | `reason` | Exact provision intents classified past their durable admission deadline (`reason="provision_timeout"`). Cleanup uncertainty retains the intent and reservation for periodic retry; there is no separate container-start recovery timer |
| `fred_docker_backend_operation_intent_recovery_cleanup_retries_total` | counter | kind | Deferred exact operation cleanup (`provision`/`restore`); intent and reservation remain for periodic retry |
| `fred_docker_backend_terminal_substrate_pending_containers` | gauge | receipt | Last late-container count for permanent `closed`/`failed_operation` receipts; nonzero withholds this backend’s pool capacity/readiness until strict absence |
| `fred_docker_backend_terminal_substrate_cleanup_retries_total` | counter | receipt | Transient late-container cleanup retries; daemon stays alive and exact terminal receipts remain |
| `fred_docker_backend_unaccounted_managed_volumes` | gauge | — | Attested managed volumes absent from current live, admitted-operation, and all retention projections; diagnostic only, never deletion or admission authority |
| `fred_docker_backend_unaccounted_managed_volume_observation_failures_total` | counter | — | Failed diagnostic inventory/footprint observations; last unaccounted-volume gauge is retained, not reset to zero |
| `fred_docker_backend_image_pull_duration_seconds` | histogram | — | Image pull duration |
| `fred_docker_backend_restore_duration_seconds` | histogram | — | Restore re-deploy worker duration (success only); measures the async re-deploy and excludes the synchronous adopt prelude (tracked separately under `replace_phase_duration_seconds{phase=adopt}`). Buckets mirror `provision_duration_seconds` for an indicative restore-vs-fresh-provision overlay (provision is success+failure, restore success-only) |
| `fred_docker_backend_restore_total` | counter | `outcome` | Restore re-deploy worker attempts by `outcome` ∈ `success`/`failure`. Unlike the success-only `restore_duration_seconds`, it also counts the failure path (`rollbackRestoreAdoption`, panics included), so a docker-backend restore success rate is computable. Worker-scoped like `restore_duration_seconds` and `provisions_total`: a restore that fails in the synchronous adopt prelude (claim/rename/route/ack) before the worker spawns surfaces as the synchronous `Restore()` error and is counted by neither outcome here |
| `fred_docker_backend_replace_phase_duration_seconds` | histogram | `operation, phase` | Per-phase duration of the shared replace machinery. `operation` ∈ `restart`/`update`/`restore`; `phase` ∈ `adopt` (restore-only volume rename), `image_setup`, `volume_setup` (incl. VOLUME-subdir chown), `compose_up`, `verify_startup` |
| `fred_docker_backend_resource_cpu_allocated_ratio` | gauge | — | Allocated/total CPU |
| `fred_docker_backend_resource_memory_allocated_ratio` | gauge | — | Allocated/total memory |
| `fred_docker_backend_resource_disk_allocated_ratio` | gauge | — | Allocated/total physical disk. Docker includes durable `disk_mb` plus the pinned scratch allowance for every live diskless instance, even if no managed scratch directory was ultimately needed |
| `fred_docker_backend_restore_demote_refused_total` | counter | `backend, reason` | Restores refused by the demote fit-gate (`checkDemoteFit`) because the retained data does not fit the requested smaller SKU tier. `reason` ∈ `measured_exceeds`, `unmeasurable_read_error`, `unmeasurable_backend`, `ephemeral_tier`. Synchronous-prelude refusals — NOT counted by `restore_total` (worker-scoped); surfaced to the tenant as HTTP 422 — the `demote_exceeds_tier` string discriminator rides only the backend→fred hop (ENG-438) |
| `fred_docker_backend_volume_quota_backfill_total` | counter | `outcome` | Startup quota-reconciliation per-volume re-application (re-tag + re-limit) attempts, `outcome` ∈ `applied`/`failed`; re-applies the immutable effective quota (`disk_mb` for stateful volumes or pinned scratch for a present diskless writable-path volume) without a re-provision. The complete inventory is attempted, then any failed application, inventory error, or durable-profile error fails startup/readiness before the normal metrics endpoint binds (ENG-454) |
| `fred_docker_backend_volume_quota_clear_failed_total` | counter | — | Failed XFS quota-clear commands during interrupted-create compensation or typed deletion; preceding block/inode proof failures are not counted. Typed authority is retained and the current backend instance fail-stops for recovery by a fresh `Start`; only historical already-absent/no-authority leaks need classified one-time manual cleanup (ENG-459/ENG-632) |

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
| `fred_docker_backend_retention_partitions` | gauge | — | Distinct non-empty (tenant, partition) buckets across active+restoring retained records. May legitimately exceed the sum of budgeted `max_partitions` while an over-limit shrink drains (partition values age out with their records) |
| `fred_docker_backend_retention_refused_by_scope_total` | counter | `scope` | Close-time refuse-to-retain events by the tripped cap scope. `scope` ∈ `global` (L0 `max_retained_disk_mb`), `tenant` (L1 per-tenant aggregate disk), `partition` (L2 per-partition disk sub-cap). The bare `..._retention_refused_total` keeps its deployed L0-global-only meaning, so this is the scoped superset |
| `fred_docker_backend_retention_cap_check_failed_total` | counter | `check` | Retention cap checks that failed OPEN on a store read error (fail-open is data-safe — never destroy on uncertainty — but a sustained rate means the quota gates are silently off). `check` ∈ `evict`, `breach`, `bound`, `refuse_get` |
| `fred_docker_backend_disk_pool_bytes` | gauge | — | Total disk admission pool (total_disk_mb) in bytes |
| `fred_docker_backend_retained_disk_cap_bytes` | gauge | — | Per-provider retained-volume cap (max_retained_disk_mb) in bytes; 0 when unset |
| `fred_docker_backend_retention_reaping_bytes` | gauge | — | Reserved disk footprint of reaping (pending-destroy) retained records, in bytes |
| `fred_docker_backend_retention_reaping_leases` | gauge | — | Number of retained records stuck in the reaping (pending-destroy) state |
| `fred_docker_backend_retention_leaked_total` | counter | — | Retained-volume leak events (failed destroy or uncommitted restore revert) — see ENG-376 |
| `fred_docker_backend_retention_orphans_pruned_total` | counter | — | Total retention records pruned due to confirmed-absent backing volumes |
| `fred_docker_backend_retention_orphan_skips_total` | counter | `reason` | Orphan-reconcile skips by reason (sweep-level bailouts + per-record raced prune attempts). `reason` ∈ `list_error`, `root_unverifiable`, `raced`, `disabled`, `store_error` |
| `fred_docker_backend_retention_reap_skips_total` | counter | `reason` | Reaping-finalizer attempts deferred without dropping their exact tombstone, counted per reap attempt (not per volume). `reason` ∈ `owner_claimed` (the namespace belongs to a live provision: nothing to unblock, and it clears when that lease is next closed cleanly, ENG-658), `claim_unreadable` (retention-store or volume-root authority could not be read ⇒ nothing destroyed this pass, fail-safe — the ticketing signal). Restore adoption cannot overlap a reaping destination: both require mutually exclusive per-lease mutation heads, so the collision is rejected at admission (ENG-659). The record is left reaping in both reachable skip cases, so its footprint keeps counting |
| `fred_docker_backend_volume_destroy_refused_total` | counter | `site`, `reason` | Managed-volume destroys refused by the ownership choke point, counted **per volume** (not per attempt — deliberately not summable with `retention_reap_skips_total`). Every authorized destroy in the docker backend routes through one primitive, so this is the single place a "we nearly destroyed another lease's data" event surfaces, whichever exact operation asked. `site` ∈ `deprovision_destroy`, `deprovision_reclaim`, `retention_refused`, `reaping`. `reason` ∈ `claimed` (another lease owns those bytes — the guard working; how it clears depends on who owns them, which the accompanying WARN names: a restore's claim clears when that restore commits or rolls back, a live provision's only when that lease is next closed. Raised either by the up-front owner table or by the destroy-time re-check under the volume-name lock, which catches a claim published after the table was resolved, ENG-681), `claims_unreadable` (the retention store could not be read ⇒ ownership unprovable ⇒ nothing destroyed, fail-safe — the ticketing signal), `no_destroyer` (the volume manager offers no destroy capability; unreachable in production, since startup refuses such a manager). There is deliberately no site for unattributed-volume garbage collection: a name plus inventory absence is not destructive authority, so such volumes are preserved for explicit operator attribution |
| `fred_docker_backend_retention_sweep_total` | counter | `outcome` | Periodic retention-sweep passes, **exactly one increment per pass**. `outcome` ∈ `success` (every stage completed), `error` (at least one stage failed). Because the sum across outcomes advances on every tick regardless of result, `sum without (outcome) (increase(...[N])) == 0` is a liveness heartbeat for the sweep goroutine, and `{outcome="error"}` is the sweep-stage failure signal. It does **not** identify the failing dependency: a degraded retention store is the common cause, but the orphan stage also reports a failed volume-root enumeration here. The joined error in the sweep's log line is the discriminator — it prefixes each failure with its stage (`reap expired:` / `retry reaping:` / `list restoring:` / `reconcile orphans:`). Both series are pre-initialised to 0, so "never failed" reads as 0 rather than no-data. Deliberately **not** paired with a last-success gauge: a gauge that only advances on a fully clean pass freezes under benign sustained errors, which is why the deployed rules refuse to alert on the equivalent reconciler gauge (ENG-680) |
| `fred_docker_backend_retention_accounting_refresh_failed_total` | counter | — | Retained-disk accounting refreshes that could not recompute from the store and therefore **kept the last value**. Keeping it is the safe direction (a zeroed projection would over-admit and risk ENOSPC) but it is silent: all five retention gauges *and* the pool's retained input hold plausible numbers for as long as the store is degraded, which is indistinguishable from a healthy provider. This is the "the gauges you are reading are stale" signal. Fires from every retention transition (close, restore, recover, boot, sweep), so its rate is lease-churn, **not** summable with `retention_sweep_total` (ENG-680) |
| `fred_docker_backend_retention_writable_path_reclaimed_total` | counter | — | Total writable-path-only volumes destroyed (reclaimed) at close instead of retained |
| `fred_docker_backend_retention_index_reindex_total` | counter | `trigger` | Count of retention in-memory index (re)builds, by trigger (`open`\|`manual`) |
| `fred_docker_backend_restore_finalizer_pending_total` | counter | — | Restore-finalizer kept-pending events: increments when a successful restore cannot durably commit or verify its exact active Release, so the retention record stays `restoring` to protect adopted data. Reconciliation retries without incrementing; alert on increases and use the retained row plus WARN log to track convergence (ENG-523) |

**Callbacks:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_callback_delivery_total` | counter | `outcome` | Callback delivery attempts |
| `fred_docker_backend_callback_store_errors_total` | counter | — | Instrumented failures reading or writing durable callback evidence, including fail-closed operation-intent startup recovery. It is not a generic semantic maintenance-recovery signal: a valid indeterminate WAL may leave this unchanged. Pending close retries are logged separately with lease UUID and durable cleanup-attempt count |
| `fred_docker_backend_pending_close_intents` | gauge | — | Aggregate count of non-expiring destructive-close finalizers awaiting settlement. It deliberately has no lease label |
| `fred_docker_backend_oldest_close_intent_age_seconds` | gauge | — | Age of the oldest pending close finalizer; zero when none are pending. Alert on sustained age, then use the lease-scoped recovery log to identify the row |
| `fred_docker_backend_lease_mutation_uuid_slots` | gauge | — | Monotonic number of permanent lease-UUID slots reserved in this backend storage lineage. Closing or settling a lease does not reduce it |
| `fred_docker_backend_lease_mutation_uuid_slot_limit` | gauge | — | Fixed hard ceiling for permanent lease-UUID slots. Alert on the used/limit ratio before new UUID admission reaches the definitive-refusal boundary |
| `fred_docker_backend_callback_receipt_reservations` | gauge | — | Durable operation and maintenance receipt reservations currently consumed; successful close can reclaim them behind the stronger closed-lease fence |
| `fred_docker_backend_callback_receipt_reservation_limit` | gauge | — | Fixed hard ceiling shared by operation and maintenance receipt reservations. Alert before command admission reaches definitive refusal |

**Reconciliation:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_docker_backend_reconciliation_total` | counter | `outcome` | Docker `recoverState` runs; `outcome="error"` is the runtime signal for a failed maintenance-WAL classification as well as other recovery failures |
| `fred_docker_backend_reconciliation_last_success_timestamp_seconds` | gauge | — | Unix timestamp of last successful Docker recovery pass; staleness beyond the configured backend reconcile interval signals that level-triggered convergence is not completing |
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
| `fred_docker_backend_die_event_dropped_total` | counter | `source` | Container-death signals the exact-generation observation router could not deliver (`event_loop`, `reconcile`) because the generation changed, recovery held the actor key, the backend was stopping, or the inbox was unavailable. Reconciler re-detects current failures on its next cycle; sustained growth flags churn, recovery contention, a wedged actor, or chronic burst |
| `fred_docker_backend_lease_worker_panics_total` | counter | `worker_type` | Panics in lease worker goroutines (provision/replace/diag) — any non-zero is a latent bug |

#### k3s backend (`/metrics` on the k3s-backend HTTP server)

The k3s backend is an experimental, non-functional scaffold (ENG-133), but its binary still serves `GET /metrics` (default `:9002`) and increments three counters under `fred_k3s_backend_*`, mirroring the docker-backend names.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fred_k3s_backend_provisions_total` | counter | `outcome` | Provision requests received by the k3s backend (`outcome` ∈ `accepted`/`rejected`) |
| `fred_k3s_backend_callback_delivery_total` | counter | `outcome` | Callback delivery outcomes, one per overall delivery after retries (`outcome` ∈ `success`/`failure`) |
| `fred_k3s_backend_callback_store_errors_total` | counter | — | Failures reading or writing durable callback evidence in the k3s backend, including interrupted-operation recovery |

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
| Race | Same files, `-race -short` | Concurrency invariants — actor messages, operation registry, signer pool. Stress tests skip via `testing.Short()` because they OOM under `-race`. | `go test -race -short ./...` |
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
2. Sender computes HMAC-SHA256(canonical_string, backend_secret)
3. Sender sends X-Fred-Signature: t=<timestamp>,sha256=<hex>
4. Verifier extracts r.Method and r.URL.RequestURI() from the request
5. Verifier reads the body and hashes it (sha256.Sum256)
6. Verifier rejects if timestamp > 5 minutes old (same-endpoint replay bound)
7. Verifier rejects if timestamp > 1 minute in future (clock skew limit)
8. Verifier recomputes the canonical string + HMAC and compares (constant-time)
```

Binding the method and request URI prevents cross-endpoint replay
(a captured `POST /provision` signature cannot be replayed against
`POST /deprovision`, nor a `GET /info/<id>` against `GET /logs/<id>`).
Including the body as a SHA-256 hash rather than as a literal string
keeps the canonical string bounded in length and binary-safe.

Production uses one distinct bidirectional key per backend. Provider config
maps `backends[].hmac_secret=K_i`; the corresponding backend config maps its
existing `callback_secret=K_i`. On callback ingress, the HMAC-covered canonical
`backend_storage_id` selects the key associated with the placement store's
prepared immutable storage lineage. That value remains untrusted until HMAC
verification and is then checked against durable callback authority. Pairwise
key uniqueness prevents a compromised backend from authenticating commands or
callbacks for another backend. The legacy fleet-wide top-level key is permitted
only outside production.

The timestamp is not a nonce: the exact request can be replayed against the
same method and URI inside the five-minute window. This is required for durable
callback retry. Typed operation/lifecycle settlement and idempotent handlers
make duplicates safe; method/URI binding prevents moving the signature to a
different endpoint or operation.

### Defense in Depth

- Input validation at API boundary
- Rate limiting per-IP and per-tenant
- Request size limits
- TLS for transport security
- Generic error messages to clients
- Security headers on all responses
