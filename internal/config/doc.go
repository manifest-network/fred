// Package config handles configuration loading and validation for fred.
//
// # Configuration Sources
//
// Configuration is loaded from multiple sources with the following precedence
// (highest to lowest):
//
//  1. Environment variables (PROVIDER_ prefix)
//  2. Configuration file (YAML)
//  3. Default values
//
// # Environment Variables
//
// All configuration options can be set via environment variables with the
// PROVIDER_ prefix. Nested options use underscores:
//
//	PROVIDER_CHAIN_ID=manifest-1
//	PROVIDER_GRPC_ENDPOINT=localhost:9090
//	PROVIDER_PROVIDER_UUID=01234567-89ab-cdef-0123-456789abcdef
//
// # Validation
//
// The Validate method performs comprehensive validation:
//
//   - Required fields must be non-empty
//   - UUIDs must be valid format
//   - URLs must be absolute http/https URLs
//   - Durations must be positive
//   - Backend names must be unique
//   - Callback secret must be at least 32 characters
//   - TLS cert and key must both be specified or neither
//   - gas_adjustment must be in [1.0, 3.0]; max_gas_limit, if set, must be ≥ gas_limit
//
// The daemon will fail to start with a clear error message if validation fails.
//
// # Production Mode
//
// Setting production_mode=true tightens startup checks beyond basic validation:
//
//   - token_tracker_db_path must be configured (replay protection cannot be
//     silently disabled)
//   - grpc_tls_skip_verify must be false when grpc_tls_enabled is true
//   - callback_base_url and backend URLs are run through an SSRF check that
//     rejects IP literals for loopback, link-local, and unspecified addresses
//     as well as the hostname "localhost"
//
// # CORS
//
// cors_origins defaults to ["*"]. This is safe because cookie-bearing
// credentials are disabled on the API — tenants must send their bearer token
// in the Authorization header, so there is no ambient-authority risk from
// allowing all origins. Set an explicit list to restrict, or set an empty
// list to disable CORS middleware entirely.
//
// # Parallel Signing
//
// sub_signer_count > 0 enables authz-based parallel signing for lease
// acknowledgments. Each sub-signer holds a granted authz authorization and
// gets topped up from the primary key on its own schedule (see
// sub_signer_min_balance, sub_signer_top_up_amount,
// sub_signer_fund_check_interval).
//
// # Backend Configuration
//
// Multiple backends can be configured with SKU-based routing:
//
//	backends:
//	  - name: docker-1
//	    url: "http://docker-backend:9000"
//	    timeout: 30s
//	    skus:
//	      - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
//	    default: true
//
// At least one backend with default: true should be configured as a fallback.
package config
