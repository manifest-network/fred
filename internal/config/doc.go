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
//
// The daemon will fail to start with a clear error message if validation fails.
//
// # Backend Configuration
//
// Multiple backends can be configured with SKU-based routing:
//
//	backends:
//	  - name: kubernetes
//	    url: "http://k8s-backend:9000"
//	    timeout: 30s
//	    sku_prefix: "k8s-"
//	    default: true
//
// At least one backend with default: true should be configured as a fallback.
package config
