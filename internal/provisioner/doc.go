// Package provisioner implements lease lifecycle application services and their
// Watermill runtime composition.
//
// # Architecture
//
// Manager owns process lifetime and wires narrow consumer-owned ports. HandlerSet
// adapts internal messages to application inputs; handlers do not own lifecycle
// policy. Chain and payload messages run through Watermill. Authenticated backend
// callbacks are applied synchronously so the backend's durable per-lease FIFO is
// preserved through settlement and event delivery. ProvisionOrchestrator owns
// provision admission and backend dispatch, CallbackService owns callback
// settlement policy, and the restore package owns atomic source/target restore
// admission and dispatch.
//
// The operation.Registry is the only process-local source of lifecycle operation
// state. It issues typed OperationID values and opaque initiation, lease, token,
// and settlement capabilities, enforcing Preparing -> Calling -> Active ordering
// without exposing raw identifiers as mutation authority. Manager owns this
// registry directly; lifecycle consumers receive narrow capability ports.
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
// The existing stateless FSM dependency is used inside backend-local per-lease
// actors. It is deliberately not used as a fleet-wide distributed state machine:
// backend inventory and chain state are observations that must be rejoined after
// process loss, not edge events that can be replayed reliably.
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
