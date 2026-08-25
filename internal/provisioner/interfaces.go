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

	// RouteForProvision selects the least-loaded backend matching the SKU for a
	// new provision, falling back to round-robin when no candidate exposes usable
	// load stats. inFlightByBackend is a per-backend in-flight provision count
	// used to spread concurrent provisions; it may be nil.
	RouteForProvision(ctx context.Context, sku string, inFlightByBackend map[string]int) backend.Backend

	// GetBackendByName returns a backend by its name. Returns nil if not found.
	GetBackendByName(name string) backend.Backend

	// Backends returns all unique backends for operations like reconciliation.
	Backends() []backend.Backend
}

// Compile-time check that backend.Router implements BackendRouter.
var _ BackendRouter = (*backend.Router)(nil)

// PlacementStore records which backend is serving each lease so that
// read operations reach the correct backend after provision routing.
type PlacementStore interface {
	Lookup(leaseUUID string) placement.Placement
	List() map[string]placement.Placement
	SnapshotRevision() uint64
	SetAttempting(leaseUUID, backendName string) error
	Confirm(leaseUUID, backendName string) error
	ConfirmAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	ClearAttempt(leaseUUID, backendName string) error
	ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	Delete(leaseUUID string) error
	DeleteIfRevision(leaseUUID string, revision uint64) (bool, error)
	SetBatchIfNotNewer(placements map[string]string, maxRevision uint64) error
	SetConflictsIfNotNewer(conflicts map[string][]string, maxRevision uint64) error
	ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error
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
