// Package provisioner implements the lease provisioning lifecycle using
// Watermill for event-driven processing.
//
// # Overview
//
// The provisioner bridges chain events to backend operations:
//
//	Chain Event → Watermill Topic → Handler → Backend Call → Chain Tx
//
// # Manager
//
// The Manager is the central component that:
//   - Subscribes to Watermill topics for lease events
//   - Tracks in-flight provisions to prevent duplicates
//   - Routes to appropriate backends via the backend.Router
//   - Batches lease acknowledgments for efficiency
//   - Handles backend callbacks and chain transactions
//
// # Event Topics
//
// The provisioner uses these Watermill topics:
//
//	TopicLeaseCreated     - New lease needs provisioning
//	TopicLeaseClosed      - Lease closed, deprovision resources
//	TopicLeaseExpired     - Lease expired, deprovision resources
//	TopicPayloadReceived  - Tenant uploaded payload, start provisioning
//	TopicBackendCallback  - Backend reported provisioning result
//	TopicLeaseEvent       - Real-time lease status events for WebSocket delivery
//
// # Reconciler
//
// The Reconciler provides level-triggered state reconciliation:
//   - Runs on startup to recover from crashes
//   - Runs periodically to fix any drift
//   - Compares chain state vs backend state
//   - Takes corrective action (provision, deprovision, acknowledge)
//
// # PayloadStore
//
// The PayloadStore provides temporary storage for tenant deployment payloads:
//   - Stores payloads uploaded via API until provisioning starts
//   - Uses write batching for efficiency under load
//   - Reconciler-driven cleanup when leases are no longer active
//   - Survives restarts (persistent bbolt storage)
//
// # Crash Recovery
//
// The provisioner uses level-triggered reconciliation for crash recovery.
// Rather than replaying missed events, it queries current state and acts:
//
//	Chain: PENDING lease exists
//	Backend: No provision found
//	Action: Start provisioning
//
// This approach is simpler and handles any inconsistency, not just missed events.
package provisioner
