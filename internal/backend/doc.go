// Package backend provides the interface and client for communicating with
// provisioning backends.
//
// Fred supports multiple backends, each responsible for provisioning a specific
// type of resource (e.g., Kubernetes deployments, GPU allocations, VMs). The
// Router directs leases to the appropriate backend based on exact SKU UUID matching.
//
// # Backend Interface
//
// Any backend must implement the Backend interface, which defines eight operations:
//
//   - Provision: Start async provisioning, backend calls callback URL when done
//   - GetInfo: Retrieve connection details for a provisioned resource
//   - GetProvision: Retrieve provision diagnostics (status, error, fail count)
//   - GetLogs: Retrieve container logs for a provision
//   - Deprovision: Clean up resources (must be idempotent)
//   - ListProvisions: Return all provisioned resources (for reconciliation)
//   - Health: Report whether the backend can accept requests
//   - Name: Return the backend's configured name
//
// # HTTPClient
//
// The HTTPClient implements the Backend interface for HTTP-based backends.
// It includes:
//   - Circuit breaker (sony/gobreaker) for fault tolerance
//   - Configurable timeouts
//   - Prometheus metrics for all operations
//
// # Router
//
// The Router matches leases to backends using exact SKU UUIDs:
//
//	backends:
//	  - name: docker-1
//	    skus:
//	      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
//	  - name: default
//	    default: true          # Fallback for unmatched SKUs
//
// # Callback Protocol
//
// After async provisioning completes, backends call fred's callback URL:
//
//	POST {callback_url}
//	X-Fred-Signature: t=<unix-timestamp>,sha256=<hmac-sha256-hex>
//	Content-Type: application/json
//
//	{"lease_uuid": "...", "status": "success"|"failed", "error": "..."}
//
// The HMAC is computed over "<timestamp>.<body>" to bind the timestamp to
// the signature. Callbacks older than 5 minutes are rejected (replay protection).
// Timestamps up to 1 minute in the future are accepted (clock skew tolerance).
package backend
