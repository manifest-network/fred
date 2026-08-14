// Package payload implements bbolt-backed temporary storage for tenant
// deployment payloads.
//
// # Lifecycle
//
// A payload enters the store when a tenant uploads it via POST /v1/leases/{uuid}/data.
// It is REPLACED when the tenant updates the deployment via
// POST /v1/leases/{uuid}/update (Put). It is removed when:
//
//   - Provisioning starts and Pop succeeds (normal path), or
//   - The lease is closed/expired (reconciler-driven cleanup), or
//   - The TTL expires (background cleanup loop)
//
// The store is persistent (bbolt) so payloads survive restarts. This matters
// because a tenant might upload a payload, then Fred restarts before the
// provisioner picks it up — and because the reprovision path replays from here,
// so whatever is stored is what the tenant's lease comes back as.
//
// # Recorded hashes
//
// Each payload's own SHA-256 is written beside it, in a separate payload_hashes
// bucket, inside the same transaction as the payload. Readers verify a stored
// payload against that recorded hash rather than against the lease's on-chain
// MetaHash, because MetaHash is set once at lease creation and immutable, while
// a tenant update legitimately replaces the manifest (ENG-619). Verifying an
// updated payload against MetaHash would read a successful update as corruption.
//
// The hash lives outside the payload record deliberately: a checksum stored
// inside the data it describes rots along with it, so the split is what makes
// bit-rot detectable at all. It is an integrity reference and not a
// tamper-evidence one — anything that can rewrite the payload can rewrite the
// hash — so the on-chain MetaHash remains the cross-trust-boundary commitment.
// A payload with no recorded hash predates this bucket and falls back to
// MetaHash.
//
// # Write batching
//
// All mutating operations (Store, Put, Pop, Delete) are routed through a single
// writer goroutine that batches writes into bbolt transactions. This reduces
// lock contention under load — a single bbolt Update is far cheaper than N
// independent updates because bbolt serializes write transactions.
//
//   - DefaultBatchSize: 50 operations per transaction
//   - DefaultFlushInterval: 50ms maximum delay before a partial batch flushes
//
// Reads (Get, Has) bypass the batcher and run as bbolt View transactions
// directly — they don't contend with each other and are a simple key fetch.
//
// # Backpressure
//
// The write channel is buffered. Once full, mutating callers block until
// space frees up. Under sustained extreme load this surfaces as upstream
// latency rather than memory growth or silent drops.
//
// # Optional
//
// The store is optional at the provisioner level: if `payload_store_db_path`
// is unset, payloads are kept only in memory. This is fine for development
// but loses uploaded payloads on restart.
package payload
