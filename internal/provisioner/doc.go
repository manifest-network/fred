// Package provisioner implements lease lifecycle application services and their
// Watermill runtime composition.
//
// # Architecture
//
// Manager owns process lifetime and wires narrow consumer-owned ports. HandlerSet
// adapts internal messages to opaque application inputs; handlers do not own
// lifecycle policy. Chain and payload messages run through Watermill.
// Authenticated backend callbacks are applied synchronously so the backend's
// durable per-lease FIFO is preserved through settlement and event delivery.
// The transport-facing orchestrator, callback, restore, and maintenance services
// retain only purpose-specific application capabilities and closed result types;
// they cannot choose a backend, manufacture a callback route, or select a durable
// settlement outcome.
//
// The operation.Registry is the only process-local source of lifecycle operation
// state. It issues typed OperationID values and opaque initiation, lease, token,
// and settlement capabilities, enforcing Preparing -> Calling -> Active ordering
// without exposing raw identifiers as mutation authority. The placement Store
// privately constructs the Registry and consumes its one-shot settlement
// authority into OperationCoordinator. Construction atomically binds that
// aggregate to exactly one backend runtime and one provider control plane through
// ExecutionCoordinator; no backend-only intermediate is published.
// Purpose-specific placement coordinators then own complete provision, restore,
// maintenance, callback, timeout, and reconciliation sequences. After
// composition, Manager retains operation.RuntimeController, which can observe and drain work
// but cannot claim, initiate, route, dispatch, or settle it.
//
// The placement store is the durable authority for write-ahead attempts,
// confirmed owners, conflict quarantine, and inventory revisions. Consumer ports
// deliberately expose only the transitions each service is permitted to make.
// During the originating process, settlement joins the process-local capability
// with the durable operation-scoped record. After a restart, the exact durable
// operation identity is independently sufficient to reacquire a lease claim and
// finish settlement; volatile registry loss must not discard causal evidence.
//
// # Event Topics
//
// The provisioner uses these Watermill topics:
//
//	TopicLeaseCreated     - New lease needs provisioning
//	TopicLeaseClosed      - Lease closed, deprovision resources
//	TopicLeaseExpired     - Lease expired, deprovision resources
//	TopicPayloadReceived  - Tenant uploaded payload, start provisioning
//	TopicLeaseEvent       - Real-time lease status events for WebSocket delivery
//
// TopicBackendCallback remains only for the legacy message-shaped adapter;
// production callbacks are not published through Watermill.
//
// # Reconciler
//
// Reconciliation is a level-triggered evidence join, not a distributed FSM. It
// collects read-only chain, provision, and retention inventory; projects the
// positive ownership evidence atomically into durable placement state; evaluates
// a pure decision table; and dispatches only actions authorized by that evidence.
// The first complete inventory establishes a durable baseline bound to the
// configured immutable backend identities. That baseline survives restart and
// transient incomplete sweeps. Each later sweep attenuates it to a typed scope
// containing only backends that answered both inventories; only genuinely new
// recordless PENDING reconciliation may use that scope. Recordless ACTIVE work,
// work pinned to a silent owner, attempts, and conflicts remain deferred.
//
// The tenant event path has no per-sweep witness. It requires the same durable
// topology baseline, live-routes within the configured topology, and persists an
// exact write-ahead attempt before backend dispatch.
//
// Each inventory pass uses one ReconciliationSweep that binds a Store fence, an
// operation.ReconciliationBoundary, and an inventory session. Projection is a
// one-shot transition to ProjectedReconciliationSweep. Only that value can mint
// an ObservedReconciliationAction or ObservedOrphanAction, after a bounded exact
// chain read under a lease claim. Live and orphan mutation methods derive the
// lease and backend from those opaque capabilities, so callers cannot splice
// observations, revisions, claims, or targets from different sweeps.
//
// The existing stateless FSM dependency is used inside backend-local per-lease
// actors. It is deliberately not used as a fleet-wide distributed state machine:
// backend inventory and chain state are observations that must be rejoined after
// process loss, not edge events that can be replayed reliably.
//
// Tokenless v0.13 compatibility exists only at explicit migration and callback
// boundaries. Offline adoption may preserve an already-distributed tokenless
// route as LegacyRuntimeAuthority, and authenticated callback ingress may observe
// it only for the matching migrated owner. New provision/restore authority uses
// distinct typed operation and lifecycle UUIDv4 identities, while every new
// maintenance command has its own typed UUIDv4 identity. Ordinary runtime code
// never mints tokenless authority; maintenance on a legacy owner can only
// preserve the already-adopted legacy authority class.
//
// # PayloadStore
//
// The PayloadStore provides lease-lifetime storage for tenant deployment payloads:
//   - Stores payloads uploaded via API and replacements accepted by update
//   - Retains the current payload so reconciliation can safely re-provision
//   - Uses write batching for efficiency under load
//   - Reconciler-driven cleanup when leases are no longer active
//   - Survives restarts (persistent bbolt storage)
//
// # Crash Recovery
//
// Crash recovery never treats process-local registry loss or a missing callback
// as proof that a backend was not contacted. Rather than replaying missed events,
// the reconciler combines positive inventory with durable attempts and ownership:
//
//	Chain: PENDING lease exists
//	Placement: unresolved backend attempt exists
//	Backend: attempted backend reports the lease and exact paired generation
//	Action: confirm ownership, then continue settlement
//
// When positive inventory or a callback has not settled the attempt, a later
// sweep reconstructs the exact operation ID and request and redelivers only to
// the pinned attempted backend. Acceptance or idempotent recognition promotes
// it; a contract-conforming refusal clears it; every ambiguous response retains
// it for another sweep. The attempt itself binds the typed kind, exact callback
// pair, immutable tenant/provider/ordered item snapshot, and provision payload
// fingerprint or restore source, so callback-base, CustomDomain, and payload
// updates across a restart cannot silently rewrite the request. Missing
// payload bytes defer rather than downgrading the call or terminating the lease.
// A positively terminal target follows a separate claimed path: every exact
// attempted/confirmed backend is deprovisioned, and only all-success promotes
// conservative closed-lease affinity; ambiguity retains the attempt.
//
// An older observed generation preserves both its current authority and the
// newer durable attempt. A callback after Registry loss may reacquire the lease
// and settle through that exact attempt; inventory never invents its ID. A nil
// chain point-read is not terminal proof: callback settlement stays retryable
// and preserves the operation, attempt, and payload until the chain positively
// reports a supported live or terminal state.
// A positive report from another backend is unioned with every existing owner
// and attempt into durable conflict quarantine. Complete or partial inventory
// silence never clears an ambiguous attempt or conflict: an old request can
// commit after the list response. Exact same-operation redelivery, the exact
// callback, matching paired-generation inventory, a contract-conforming
// refusal trusted under the configured backend transport, or explicit operator
// proof may settle an attempt. This makes timeout, panic, transport failure, generic
// 5xx, callback loss, and restart conservative by construction.
package provisioner
