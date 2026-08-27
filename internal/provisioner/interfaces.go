package provisioner

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
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

	// RouteForProvisionAmong applies the same provision routing policy while
	// treating eligibleNames as a hard boundary. It returns nil when neither an
	// eligible SKU match nor the eligible default backend exists.
	RouteForProvisionAmong(ctx context.Context, sku string, eligibleNames map[string]struct{}, inFlightByBackend map[string]int) backend.Backend

	// GetBackendByName returns a backend by its name. Returns nil if not found.
	GetBackendByName(name string) backend.Backend

	// Backends returns all unique backends for operations like reconciliation.
	Backends() []backend.Backend
}

// Compile-time check that backend.Router implements BackendRouter.
var _ BackendRouter = (*backend.Router)(nil)

// PlacementView is the read-only placement projection shared by routing and
// lifecycle consumers. Holding a view never authorizes a placement mutation.
type PlacementView interface {
	Lookup(leaseUUID string) placement.Placement
	List() map[string]placement.Placement
}

// Compile-time check for the concrete durable store.
var _ PlacementView = (*placement.Store)(nil)

// ProvisionOperations is the process-local lifecycle authority needed by the
// event-driven provision coordinator. Its opaque capabilities keep the
// prepare/call/settle transitions exact while allowing callers and tests to
// supply the narrow port instead of the Registry implementation.
type ProvisionOperations interface {
	CountsByBackend() map[string]int
	TryInitiateClaimed(operation.LeaseClaim, operation.TrackSpec) operation.InitiationResult
	BeginCall(operation.Initiation) bool
	Activate(operation.Initiation) operation.InitiationCompletion
	AbortInitiation(operation.Initiation) operation.InitiationCompletion
	Lookup(string) (operation.Record, bool)
	TryClaimDeprovision(string, operation.OperationID) operation.SettlementResult
	ReleaseSettlement(operation.SettlementClaim) bool
	FinishSettlement(operation.SettlementClaim) bool
	TryClaimLeaseNow(string) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
}

// ReconcilerOperations is the process-local lifecycle authority needed by the
// level-triggered reconciler. Snapshot-scoped lease claims and phase-aware
// initiation are deliberately exposed together because a reconciled backend
// side effect must hold both halves of that causal boundary.
type ReconcilerOperations interface {
	Contains(string) bool
	Snapshot() operation.TrackerSnapshot
	LeaseUUIDs() []string
	TryClaimLease(string, operation.TrackerSnapshot) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
	Lookup(string) (operation.Record, bool)
	CountsByBackend() map[string]int
	TryInitiateClaimed(operation.LeaseClaim, operation.TrackSpec) operation.InitiationResult
	BeginCall(operation.Initiation) bool
	Activate(operation.Initiation) operation.InitiationCompletion
	AbortInitiation(operation.Initiation) operation.InitiationCompletion
}

var _ ProvisionOperations = (*operation.Registry)(nil)
var _ ReconcilerOperations = (*operation.Registry)(nil)

// ProvisionPlacement is the exact placement capability needed to initiate a
// provision. It cannot project inventory, change readiness, settle callbacks,
// or prune records.
type ProvisionPlacement interface {
	PlacementView
	CurrentAdmissionBaseline() placement.AdmissionBaseline
	ScopeAdmission(placement.AdmissionBaseline, []string) (placement.AdmissionScope, error)
	BeginNewAttempt(placement.AdmissionScope, string, string, operation.OperationID) (placement.AttemptToken, bool, error)
	BeginOwnedAttempt(placement.AdmissionBaseline, placement.RecordRevision, string, operation.OperationID) (placement.AttemptToken, bool, error)
	ConfirmAttempt(placement.AttemptToken) (bool, error)
	RefuseAttempt(placement.AttemptToken) (bool, error)
}

// ReconcilerPlacement is the inventory and repair authority owned by
// reconciliation. It includes scoped attempt admission for repair work while
// excluding callback settlement by operation identity.
type ReconcilerPlacement interface {
	PlacementView
	ConfigureBackendTopology([]string) error
	CurrentAdmissionBaseline() placement.AdmissionBaseline
	ScopeAdmission(placement.AdmissionBaseline, []string) (placement.AdmissionScope, error)
	BeginInventorySession() placement.InventoryFence
	EndInventorySession(placement.InventoryFence)
	ProjectInventory(placement.InventoryFence, placement.InventoryProjection) (placement.ProjectionResult, error)
	BeginNewAttempt(placement.AdmissionScope, string, string, operation.OperationID) (placement.AttemptToken, bool, error)
	BeginOwnedAttempt(placement.AdmissionBaseline, placement.RecordRevision, string, operation.OperationID) (placement.AttemptToken, bool, error)
	ConfirmAttempt(placement.AttemptToken) (bool, error)
	RefuseAttempt(placement.AttemptToken) (bool, error)
	DeleteRecord(placement.RecordRevision) (bool, error)
}

// PlacementAuthorityStore is the composition-root aggregate implemented by
// the durable store. Consumers receive one of the narrower ports above (or
// CallbackPlacement), so unrelated authority is unavailable by construction.
type PlacementAuthorityStore interface {
	ProvisionPlacement
	ReconcilerPlacement
	ConfirmOperation(leaseUUID, backendName string, operationID operation.OperationID) (bool, error)
	RefuseOperation(leaseUUID, backendName string, operationID operation.OperationID) (bool, error)
}

var _ ProvisionPlacement = (*placement.Store)(nil)
var _ ReconcilerPlacement = (*placement.Store)(nil)
var _ PlacementAuthorityStore = (*placement.Store)(nil)

// LeaseRejecter defines the interface for rejecting leases on chain.
// This is used by the TimeoutChecker to reject timed-out leases.
type LeaseRejecter interface {
	// RejectLeases rejects the given leases with the specified reason.
	// Returns the number of leases rejected, transaction hashes, and any error.
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}
