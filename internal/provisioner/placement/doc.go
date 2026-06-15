// Package placement implements a bbolt-backed lease→backend mapping with an
// in-memory cache. It is the routing primitive that makes round-robin
// backends safe for read operations.
//
// # Why this exists
//
// When multiple backends share the same `skus` list, the provisioner uses
// RouteForProvision to distribute new provisions across them. But subsequent
// read operations (GET /connection, /logs, /provision diagnostics) need to
// reach the specific backend that holds the lease's state. Round-robin'ing
// reads would produce 404s on N-1 of N backends.
//
// The placement store records `lease_uuid → backend_name` at provision time
// and is consulted on every read-path call.
//
// # Concurrency
//
// Reads are served entirely from the in-memory cache (RWMutex-protected) and
// never block on bbolt. Writes go to bbolt first, then update the cache.
// This keeps the read path fast (it's on every authenticated tenant call)
// while preserving durability.
//
// # Recovery
//
// On startup the reconciler calls SetBatch with placements derived from
// every backend's ListProvisions response. This rebuilds the cache from
// authoritative state and corrects any drift introduced by manual operator
// intervention or cross-backend lease moves.
//
// # Optional
//
// The placement store is optional. It is only required when multiple
// backends share the same SKU list (round-robin setups). For single-backend
// or per-SKU-dedicated-backend deployments it can be omitted entirely.
package placement
