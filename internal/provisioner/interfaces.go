package provisioner

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
)

// BackendRouter defines the interface for routing requests to backends.
// This abstracts the backend.Router for testability.
type BackendRouter interface {
	// Route returns the appropriate backend for the given SKU.
	Route(sku string) backend.Backend

	// GetBackendByName returns a backend by its name. Returns nil if not found.
	GetBackendByName(name string) backend.Backend

	// Backends returns all unique backends for operations like reconciliation.
	Backends() []backend.Backend
}

// Compile-time check that backend.Router implements BackendRouter.
var _ BackendRouter = (*backend.Router)(nil)

// LeaseRejecter defines the interface for rejecting leases on chain.
// This is used by the TimeoutChecker to reject timed-out leases.
type LeaseRejecter interface {
	// RejectLeases rejects the given leases with the specified reason.
	// Returns the number of leases rejected, transaction hashes, and any error.
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
