package provisioner

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// backendRouter is the trusted composition port used while constructing the
// purpose-specific placement facets. It is deliberately private because its
// methods return broad mutation-capable backend clients.
type backendRouter interface {
	// Route returns the appropriate backend for the given SKU.
	Route(sku string) backend.Backend

	// RouteForProvision selects the least-loaded backend matching the SKU for a
	// new provision, falling back to round-robin when no candidate exposes usable
	// load stats. inFlightByBackend is a per-backend in-flight provision count
	// used to spread concurrent provisions; it may be nil.
	RouteForProvision(ctx context.Context, sku string, inFlightByBackend map[string]int) backend.Backend

	// RouteForProvisionAmong applies the same provision routing policy while
	// treating eligibleNames as a hard boundary. It returns nil when neither an
	// eligible SKU match nor the eligible default backend exists.
	RouteForProvisionAmong(ctx context.Context, sku string, eligibleNames map[string]struct{}, inFlightByBackend map[string]int) backend.Backend

	// GetBackendByName returns a backend by its name. Returns nil if not found.
	GetBackendByName(name string) backend.Backend

	// HasBackend reports topology membership without exposing a backend client.
	HasBackend(name string) bool

	// Backends returns all unique backends for operations like reconciliation.
	Backends() []backend.Backend
}

// Compile-time check that backend.Router implements backendRouter.
var _ backendRouter = (*backend.Router)(nil)

// PlacementView is the read-only placement projection shared by routing and
// lifecycle consumers. Holding a view never authorizes a placement mutation.
type PlacementView interface {
	Lookup(leaseUUID string) placement.Placement
	List() map[string]placement.Placement
}

// Compile-time check for the concrete durable store.
var _ PlacementView = (*placement.Store)(nil)

// LeaseRejecter defines the interface for rejecting leases on chain.
// This is used by the TimeoutChecker to reject timed-out leases.
type LeaseRejecter interface {
	// RejectLeases rejects the given leases with the specified reason.
	// Returns the number of leases rejected, transaction hashes, and any error.
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
