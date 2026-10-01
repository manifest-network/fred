// Package api provides the HTTP API server for fred.
//
// The API serves three main purposes:
//
//  1. Tenant Access: Authenticated endpoints for tenants to retrieve connection
//     details and upload deployment payloads for their leases.
//
//  2. Backend Callbacks: Endpoint for backends to report provisioning results
//     with HMAC-SHA256 authentication.
//
//  3. Observability: Health check and Prometheus metrics endpoints.
//
// # Authentication
//
// Tenant endpoints use ADR-036 signature-based authentication. Tenants create
// a bearer token containing their address, the lease UUID, a timestamp, their
// public key, and a signature over the message. The server validates:
//   - The signature matches the message content
//   - The public key derives to the tenant address
//   - The timestamp is at most 30 seconds in the past and at most 10 seconds
//     in the future (clock-skew tolerance)
//   - Replay protection (TokenTracker): required for connection, restart, and
//     update — where a replayed token would re-leak sensitive data or re-run a
//     mutating operation. Idempotent reads (status, provision, logs, releases,
//     events) skip this check. The data upload endpoint skips it too and
//     relies on its own idempotency (409 on a duplicate upload for the lease).
//
// The token tracker uses fail-closed semantics: if the database is unavailable,
// requests are rejected with 503 Service Unavailable rather than proceeding
// without replay protection. Since token lifetime is short (30 seconds), clients
// can safely retry with a fresh token.
//
// Backend callbacks use HMAC-SHA256 authentication. Production selects a
// distinct per-backend key by the callback's HMAC-covered immutable storage
// identity; providerd maps it from backends[].hmac_secret and the corresponding
// backend maps the same value from callback_secret. A fleet-wide top-level
// callback_secret remains available only as a non-production compatibility
// mode.
//
// # Rate Limiting
//
// Tenant and observability routes use:
//   - Per-IP rate limiting (via RateLimiter)
//   - Per-tenant rate limiting for authenticated endpoints (via TenantRateLimiter)
//
// Both use token bucket algorithms with configurable RPS and burst sizes.
// POST /callbacks/provision instead has independent, fixed 100 RPS / 200 burst
// buckets for ingress IPs and verified backend storage identities. A valid HMAC
// may bypass an exhausted ingress bucket, but still spends its storage budget.
// This isolates backend completion from tenant traffic sharing the same NAT.
// The shared wire decoder bounds every callback to 1 MiB, 256 structural tokens,
// and 16 nesting levels before JSON decoding. Exact signed retries remain
// authenticated and budgeted; there is no callback replay cache.
//
// # Endpoints
//
//	GET  /health                                - Health check with chain connectivity
//	GET  /metrics                               - Prometheus metrics
//	GET  /workloads?lease_uuid=<u>...           - Bulk workload metadata lookup (unauthenticated)
//	GET  /v1/leases/{lease_uuid}/connection     - Get connection details (authenticated)
//	GET  /v1/leases/{lease_uuid}/status         - Get provisioning status (authenticated)
//	GET  /v1/leases/{lease_uuid}/provision      - Get provision diagnostics (authenticated)
//	GET  /v1/leases/{lease_uuid}/logs           - Get container logs (authenticated)
//	GET  /v1/leases/{lease_uuid}/releases       - Get release history (authenticated)
//	GET  /v1/leases/{lease_uuid}/events         - Stream lease events via WebSocket (authenticated)
//	POST /v1/leases/{lease_uuid}/data           - Upload deployment payload (authenticated)
//	POST /v1/leases/{lease_uuid}/restart        - Restart a provisioned lease (authenticated)
//	POST /v1/leases/{lease_uuid}/update         - Update a provisioned lease (authenticated)
//	POST /callbacks/provision                   - Backend provisioning callback (HMAC auth)
package api
