// Package chain provides clients for interacting with the Manifest blockchain.
//
// # Client
//
// The Client provides gRPC-based access to chain queries and transactions:
//
//   - Query leases by UUID, provider, or state; query credit accounts and
//     provider withdrawable balances
//   - Submit transactions: acknowledge / reject / close leases, withdraw
//     funds by provider
//   - Wait for transaction confirmation with retry logic
//   - Ping for health checks
//
// All query operations support context cancellation and include Prometheus metrics.
//
// # EventSubscriber
//
// The EventSubscriber connects to CometBFT's WebSocket interface to receive
// real-time lease events:
//
//   - lease_created: New lease for this provider
//   - lease_closed: Lease was closed by tenant
//   - lease_expired: Lease expired (time limit)
//   - lease_auto_closed: Lease auto-closed due to credit exhaustion
//
// The subscriber uses a fan-out pattern where multiple consumers can subscribe
// independently. Each consumer gets its own buffered channel.
//
// # Signer
//
// The Signer handles transaction signing using the Cosmos SDK keyring:
//
//   - Loads keys from file, OS keychain, or test backend
//   - Signs transactions with proper chain ID and account info
//   - Provides the provider's bech32 address
//
// # Error Handling
//
// The package defines ChainTxError for wrapping Cosmos SDK transaction errors.
// This enables error inspection with errors.Is() for specific error codes
// like insufficient funds or invalid sequence.
//
// # Reconnection
//
// The EventSubscriber implements automatic reconnection with exponential backoff:
//
//	Initial: 1 second
//	Max: 60 seconds
//	Multiplier: 2x
//
// The Client uses retry logic for transient gRPC errors (Unavailable, Unknown).
package chain
