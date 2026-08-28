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
// without exposing raw identifiers as mutation authority. The legacy
// InFlightTracker surface is a temporary compatibility adapter over Registry and
// does not own duplicate lifecycle state.
//
// The placement store is the durable authority for write-ahead attempts,
// confirmed owners, conflict quarantine, and inventory revisions. Consumer ports
// deliberately expose only the transitions each service is permitted to make.
// A process-local capability and a durable operation-scoped record must agree
// before an external result can settle a lifecycle operation.
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
//	Backend: attempted backend reports the lease
//	Action: confirm ownership, then continue settlement
//
// A positive report from another backend is unioned with every existing owner
// and attempt into durable conflict quarantine. Complete or partial inventory
// silence never clears an ambiguous attempt or conflict: an old request can
// commit after the list response. Only a contract-conforming synchronous
// refusal trusted under the configured backend transport, or explicit operator
// proof, may clear it. This makes timeout, panic, transport failure, generic
// 5xx, callback loss, and restart conservative by construction.
package provisioner
