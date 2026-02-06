package provisioner

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// BackendRouter defines the interface for routing requests to backends.
// This abstracts the backend.Router for testability.
type BackendRouter interface {
	// Route returns the appropriate backend for the given SKU.
	Route(sku string) backend.Backend

	// RouteRoundRobin distributes requests across all backends matching
	// the SKU using round-robin selection. Falls back to the default backend.
	RouteRoundRobin(sku string) backend.Backend

	// GetBackendByName returns a backend by its name. Returns nil if not found.
	GetBackendByName(name string) backend.Backend

	// Backends returns all unique backends for operations like reconciliation.
	Backends() []backend.Backend
}

// Compile-time check that backend.Router implements BackendRouter.
var _ BackendRouter = (*backend.Router)(nil)

// PlacementStore records which backend is serving each lease so that
// read operations reach the correct backend after round-robin provisioning.
type PlacementStore interface {
	Get(leaseUUID string) string
	Set(leaseUUID, backendName string) error
	Delete(leaseUUID string)
	SetBatch(placements map[string]string) error
	Healthy() error
	Close() error
}

// Compile-time check that placement.Store implements PlacementStore.
var _ PlacementStore = (*placement.Store)(nil)

// LeaseRejecter defines the interface for rejecting leases on chain.
// This is used by the TimeoutChecker to reject timed-out leases.
type LeaseRejecter interface {
	// RejectLeases rejects the given leases with the specified reason.
	// Returns the number of leases rejected, transaction hashes, and any error.
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
