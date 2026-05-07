// Package payload implements bbolt-backed temporary storage for tenant
// deployment payloads.
//
// # Lifecycle
//
// A payload enters the store when a tenant uploads it via POST /v1/leases/{uuid}/data.
// It is removed when:
//
//   - Provisioning starts and Pop succeeds (normal path), or
//   - The lease is closed/expired (reconciler-driven cleanup), or
//   - The TTL expires (background cleanup loop)
//
// The store is persistent (bbolt) so payloads survive restarts. This matters
// because a tenant might upload a payload, then Fred restarts before the
// provisioner picks it up.
//
// # Write batching
//
// All mutating operations (Store, Pop, Delete) are routed through a single
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
