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
//   - The timestamp is within 30 seconds (prevents replay)
//   - The token hasn't been used before (replay protection via TokenTracker)
//
// The token tracker uses fail-closed semantics: if the database is unavailable,
// requests are rejected with 503 Service Unavailable rather than proceeding
// without replay protection. Since token lifetime is short (30 seconds), clients
// can safely retry with a fresh token.
//
// Backend callbacks use HMAC-SHA256 authentication with a shared secret
// configured via callback_secret.
//
// # Rate Limiting
//
// The server implements two layers of rate limiting:
//   - Per-IP rate limiting for all requests (via RateLimiter)
//   - Per-tenant rate limiting for authenticated endpoints (via TenantRateLimiter)
//
// Both use token bucket algorithms with configurable RPS and burst sizes.
//
// # Endpoints
//
//	GET  /health                          - Health check with chain connectivity
//	GET  /metrics                         - Prometheus metrics
//	GET  /v1/leases/{uuid}/connection     - Get connection details (authenticated)
//	GET  /v1/leases/{uuid}/status         - Get provisioning status (authenticated)
//	GET  /v1/leases/{uuid}/provision      - Get provision diagnostics (authenticated)
//	GET  /v1/leases/{uuid}/logs           - Get container logs (authenticated)
//	POST /v1/leases/{uuid}/data           - Upload deployment payload (authenticated)
//	POST /callbacks/provision             - Backend provisioning callback (HMAC auth)
package api
