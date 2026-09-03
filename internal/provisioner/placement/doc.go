// Package placement implements Fred's durable multi-backend placement
// authority. It persists confirmed owners, operation-scoped write-ahead
// attempts, and quarantined ownership conflicts in bbolt, with an in-memory
// read projection.
//
// # Why this exists
//
// When multiple backends share the same `skus` list, the provisioner uses
// RouteForProvision to distribute new provisions across them. But subsequent
// read operations (GET /connection, /logs, /provision diagnostics) need to
// reach the specific backend that holds the lease's state. Round-robin'ing
// reads would produce 404s on N-1 of N backends.
//
// A confirmed `lease_uuid → backend_name` record routes reads and retained-data
// restore to the machine that owns the workload. Before provision or restore
// can contact a backend, an attempt carrying the same typed operation identity
// as its callback URL and an immutable snapshot of tenant, provider, and exact
// ordered backend items is written durably. An ambiguous synchronous result
// keeps that evidence so no retry can silently choose a second backend or
// rebuild a different request from later mutable chain state.
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
// Backend names are immutable storage identities. The Store durably records both
// the current topology and every historically adopted name, so removing a
// referenced identity is rejected. A temporarily absent name may return only as
// the same storage identity; replacement storage must receive a new name.
//
// The first complete `/provisions` and `/retentions` projection establishes an
// AdmissionBaseline bound to the current topology. It survives Store reopen and
// transient incomplete sweeps. A topology change invalidates it until another
// complete projection commits. A later sweep can attenuate the baseline to an
// AdmissionScope containing exactly the backends that answered both inventories;
// only a genuinely recordless PENDING reconciliation may use that scope.
//
// ProjectInventory applies positive observations at a causal fence. An exact
// positive confirms its attempted owner only when an active upgraded backend
// reports the same paired typed lifecycle generation. Older/unknown generations
// preserve the attempt; retention-only evidence carries no lifecycle authority.
// A contradictory positive is unioned with all owners and attempts into durable
// conflict quarantine. Inventory
// silence, complete or partial, never clears an attempt or conflict because an
// old request may commit after the list response.
//
// # Required authority
//
// Every supported deployment uses multiple backends, so production startup
// requires a writable placement database. Production consumers receive narrow
// typed capability ports for provision, callback, restore, or reconciliation
// work.
package placement
