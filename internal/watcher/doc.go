// Package watcher detects cross-provider credit depletion and triggers our
// own withdrawal in response.
//
// # Why this exists
//
// Tenants commonly run leases on multiple providers from a shared credit
// balance. When a different provider's withdrawal exhausts a tenant's credit,
// the chain emits a lease_auto_closed event for every lease that ran out of
// budget — including ours. Without intervention, we would only learn about
// closure on the next scheduled withdrawal cycle and miss any pending fees.
//
// The Watcher subscribes to the chain event stream alongside the main event
// bridge and, when a lease_auto_closed event arrives for a tenant we share,
// invokes the WithdrawTrigger to bring an immediate withdrawal forward.
//
// # Tenant set
//
// The set of tenants we share with is seeded at startup from
// GetActiveLeasesByProvider. Membership decreases as our leases close,
// expire, or are auto-closed. New leases acknowledged after startup are not
// added to the live set — they are picked up on the next restart. This is
// intentional: the watcher is a best-effort fast-path for cross-provider
// events; the periodic withdrawal scheduler is the authoritative backstop.
//
// # Fan-out
//
// The watcher receives events through its own buffered channel from the
// EventSubscriber's fan-out, independently of the provisioner's event
// bridge. A slow watcher cannot block lease provisioning.
package watcher
